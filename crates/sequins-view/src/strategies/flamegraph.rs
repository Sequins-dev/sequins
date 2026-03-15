//! FlamegraphStrategy — entity-level incremental flamegraph updates.
//!
//! Consumes a SeqlStream from a query like:
//!   `samples last {window} | <- stacks as stacks | <- frames as frames`
//!
//! The stream carries three table streams:
//!   - Primary (table=None):        sample rows (`stack_id`, `value`, `time_unix_nano`)
//!   - Auxiliary "stacks":          junction rows (`stack_id`, `frame_id`, `position`)
//!   - Auxiliary "frames":          frame metadata (`frame_id`, `function_name`, …)
//!
//! The strategy joins these internally and emits entity-level deltas for each
//! unique call-stack path (keyed by `{root_frame_id}/{...}/{leaf_frame_id}`).
//!
//! # Expiry
//! On each Heartbeat, samples older than `watermark_ns - retention_ns` are
//! expired: their value contributions are subtracted from all nodes they
//! contributed to, and zero-value nodes are removed.

use crate::delta::ViewDelta;
use crate::strategy::{ViewDeltaStream, ViewStrategy};
use arrow::array::{Array, Int64Array, StringArray, StringViewArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::frame::batch_to_ipc;
use sequins_query::SeqlStream;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ── Schemas ───────────────────────────────────────────────────────────────────

fn descriptor_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("path_key", DataType::Utf8, false),
        Field::new("frame_id", DataType::UInt64, false),
        Field::new("function_name", DataType::Utf8, false),
        Field::new("system_name", DataType::Utf8, true),
        Field::new("filename", DataType::Utf8, true),
        Field::new("line", DataType::Int64, true),
        Field::new("depth", DataType::UInt32, false),
        Field::new("parent_path_key", DataType::Utf8, true),
    ]))
}

fn data_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("total_value", DataType::Int64, false),
        Field::new("self_value", DataType::Int64, false),
    ]))
}

// ── Internal state types ──────────────────────────────────────────────────────

/// Immutable metadata for a flamegraph node.
#[derive(Clone)]
struct FrameInfo {
    function_name: String,
    system_name: Option<String>,
    filename: Option<String>,
    line: Option<i64>,
}

/// Mutable state for an aggregated flamegraph node.
/// Only the values that change after creation are kept here;
/// immutable metadata (frame_id, depth, parent) is sent once via EntityCreated.
struct NodeState {
    total_value: i64,
    self_value: i64,
}

/// Per-sample contribution record for expiry tracking.
struct SampleContribution {
    /// All path keys touched by this sample (root → leaf order).
    path_keys: Vec<String>,
    /// The sample's value (contributed to total_value of all nodes, self_value of leaf).
    value: i64,
}

/// All mutable state owned by `FlamegraphStrategy::transform`.
/// A raw sample row buffered until its stack_id is resolvable.
struct PendingSample {
    stack_id: u64,
    value: i64,
    time_unix_nano: i64,
}

struct FlamegraphState {
    /// stack_id → [(frame_id, position)] — junction table built from auxiliary "stacks" stream
    stacks: HashMap<u64, Vec<(u64, u32)>>,
    /// frame_id → FrameInfo — built from auxiliary "frames" stream
    frames: HashMap<u64, FrameInfo>,
    /// path_key → NodeState — aggregated nodes
    nodes: HashMap<String, NodeState>,
    /// time_unix_nano → contributions — for expiry tracking
    sample_contributions: BTreeMap<i64, Vec<SampleContribution>>,
    /// Nanosecond retention window (e.g. 3_600_000_000_000 for 1 hour)
    retention_ns: u64,
    /// Samples that arrived before their stack was known; drained after each stacks/frames batch.
    pending_samples: Vec<PendingSample>,
}

impl FlamegraphState {
    fn new(retention_ns: u64) -> Self {
        Self {
            stacks: HashMap::new(),
            frames: HashMap::new(),
            nodes: HashMap::new(),
            sample_contributions: BTreeMap::new(),
            retention_ns,
            pending_samples: Vec::new(),
        }
    }

    /// Accumulate new junction rows into the stacks reference table, then drain pending samples.
    fn process_stacks(&mut self, batch: &RecordBatch) -> Vec<ViewDelta> {
        for row in 0..batch.num_rows() {
            let stack_id = match col_u64(batch, "stack_id", row) {
                Some(v) => v,
                None => continue,
            };
            let frame_id = match col_u64(batch, "frame_id", row) {
                Some(v) => v,
                None => continue,
            };
            let position = match col_u32(batch, "position", row) {
                Some(v) => v,
                None => continue,
            };
            self.stacks
                .entry(stack_id)
                .or_default()
                .push((frame_id, position));
        }
        // Keep each stack sorted by position (ascending = leaf-first)
        for frames in self.stacks.values_mut() {
            frames.sort_by_key(|&(_, pos)| pos);
        }
        self.drain_pending()
    }

    /// Accumulate new frame metadata rows into the frames reference table, then drain pending samples.
    fn process_frames(&mut self, batch: &RecordBatch) -> Vec<ViewDelta> {
        for row in 0..batch.num_rows() {
            let frame_id = match col_u64(batch, "frame_id", row) {
                Some(v) => v,
                None => continue,
            };
            let function_name = col_str_view(batch, "function_name", row)
                .unwrap_or_else(|| "<unknown>".to_string());
            let system_name = col_str_view(batch, "system_name", row);
            let filename = col_str_view(batch, "filename", row);
            let line = col_i64(batch, "line", row);

            self.frames.insert(
                frame_id,
                FrameInfo {
                    function_name,
                    system_name,
                    filename,
                    line,
                },
            );
        }
        self.drain_pending()
    }

    /// Returns true if both the stack and all its frames are known.
    fn can_resolve_sample(&self, stack_id: u64) -> bool {
        match self.stacks.get(&stack_id) {
            Some(frames) => frames.iter().all(|(fid, _)| self.frames.contains_key(fid)),
            None => false,
        }
    }

    /// Drain buffered samples whose stack and all frames are now known. Returns entity deltas.
    fn drain_pending(&mut self) -> Vec<ViewDelta> {
        if self.pending_samples.is_empty() {
            return vec![];
        }
        let pending = std::mem::take(&mut self.pending_samples);
        let mut deltas = Vec::new();
        for s in pending {
            if self.can_resolve_sample(s.stack_id) {
                deltas.extend(self.process_one_sample(s.stack_id, s.value, s.time_unix_nano));
            } else {
                // Still unresolvable — put back in buffer
                self.pending_samples.push(s);
            }
        }
        deltas
    }

    /// Process a batch of sample rows and emit entity deltas.
    /// Samples whose stack or frames are not yet known are buffered in `pending_samples`.
    fn process_samples(&mut self, batch: &RecordBatch) -> Vec<ViewDelta> {
        let mut deltas = Vec::new();

        for row in 0..batch.num_rows() {
            let stack_id = match col_u64(batch, "stack_id", row) {
                Some(v) => v,
                None => continue,
            };
            let value = match col_i64(batch, "value", row) {
                Some(v) => v,
                None => continue,
            };
            let time_unix_nano = match col_i64(batch, "time_unix_nano", row) {
                Some(v) => v,
                None => continue,
            };

            if self.can_resolve_sample(stack_id) {
                deltas.extend(self.process_one_sample(stack_id, value, time_unix_nano));
            } else {
                // Stack or frames not yet available (Phase 1 sends primary before auxiliary).
                // Buffer the sample; drain_pending() will process it when both arrive.
                self.pending_samples.push(PendingSample {
                    stack_id,
                    value,
                    time_unix_nano,
                });
            }
        }
        deltas
    }

    /// Process a single sample that has a known stack_id.
    fn process_one_sample(
        &mut self,
        stack_id: u64,
        value: i64,
        time_unix_nano: i64,
    ) -> Vec<ViewDelta> {
        let mut deltas = Vec::new();

        // Resolve stack_id → frames (leaf-first order from stacks table)
        let stack_frames = match self.stacks.get(&stack_id) {
            Some(frames) if !frames.is_empty() => frames.clone(),
            _ => return deltas, // Unknown stack — skip
        };

        // Convert from leaf-first (pprof/OTLP position=0 is leaf) to root-first
        // by reversing. stack_frames is sorted ascending by position,
        // so reversing gives us root-first order.
        let root_first: Vec<(u64, u32)> = stack_frames.iter().copied().rev().collect();

        // Build path keys for each depth level (root → leaf)
        let mut path_keys: Vec<String> = Vec::with_capacity(root_first.len());
        let mut current_path = String::new();

        for (i, &(frame_id, _)) in root_first.iter().enumerate() {
            if i == 0 {
                current_path = frame_id.to_string();
            } else {
                current_path = format!("{}/{}", current_path, frame_id);
            }
            path_keys.push(current_path.clone());
        }

        if path_keys.is_empty() {
            return deltas;
        }

        let leaf_path_key = path_keys.last().unwrap().clone();

        // Create/update each node along the path
        for (depth, (&(frame_id, _), path_key)) in
            root_first.iter().zip(path_keys.iter()).enumerate()
        {
            let is_leaf = path_key == &leaf_path_key;
            let parent_path_key = if depth == 0 {
                None
            } else {
                path_keys.get(depth - 1).cloned()
            };

            if let Some(node) = self.nodes.get_mut(path_key) {
                // Existing node: increment values
                node.total_value += value;
                if is_leaf {
                    node.self_value += value;
                }
                // Emit EntityDataReplaced
                let data_ipc = batch_to_ipc(&make_data_batch(node.total_value, node.self_value));
                deltas.push(ViewDelta::EntityDataReplaced {
                    key: path_key.clone(),
                    data_ipc,
                });
            } else {
                // New node: create it
                let frame_info = self.frames.get(&frame_id).cloned().unwrap_or(FrameInfo {
                    function_name: format!("frame_{}", frame_id),
                    system_name: None,
                    filename: None,
                    line: None,
                });

                let total_value = value;
                let self_value = if is_leaf { value } else { 0 };

                self.nodes.insert(
                    path_key.clone(),
                    NodeState {
                        total_value,
                        self_value,
                    },
                );

                let descriptor_ipc = batch_to_ipc(&make_descriptor_batch(
                    path_key,
                    frame_id,
                    &frame_info.function_name,
                    frame_info.system_name.as_deref(),
                    frame_info.filename.as_deref(),
                    frame_info.line,
                    depth as u32,
                    parent_path_key.as_deref(),
                ));
                let data_ipc = batch_to_ipc(&make_data_batch(total_value, self_value));

                deltas.push(ViewDelta::EntityCreated {
                    key: path_key.clone(),
                    descriptor_ipc,
                    data_ipc,
                });
            }
        }

        // Track this sample's contributions for later expiry
        self.sample_contributions
            .entry(time_unix_nano)
            .or_default()
            .push(SampleContribution { path_keys, value });

        deltas
    }

    /// Expire samples older than `watermark_ns - retention_ns` and emit deltas.
    fn expire(&mut self, watermark_ns: u64) -> Vec<ViewDelta> {
        let mut deltas = Vec::new();
        let threshold = watermark_ns.saturating_sub(self.retention_ns) as i64;

        // Collect all timestamps older than the threshold
        let expired_timestamps: Vec<i64> = self
            .sample_contributions
            .range(..threshold)
            .map(|(&ts, _)| ts)
            .collect();

        for ts in expired_timestamps {
            let contributions = match self.sample_contributions.remove(&ts) {
                Some(c) => c,
                None => continue,
            };

            for contrib in contributions {
                let leaf_path_key = match contrib.path_keys.last() {
                    Some(k) => k.clone(),
                    None => continue,
                };

                for path_key in contrib.path_keys.iter() {
                    let is_leaf = path_key == &leaf_path_key;

                    if let Some(node) = self.nodes.get_mut(path_key) {
                        node.total_value -= contrib.value;
                        if is_leaf {
                            node.self_value -= contrib.value;
                        }

                        if node.total_value <= 0 {
                            // Node is now empty — remove it
                            self.nodes.remove(path_key);
                            deltas.push(ViewDelta::EntityRemoved {
                                key: path_key.clone(),
                            });
                        } else {
                            let data_ipc =
                                batch_to_ipc(&make_data_batch(node.total_value, node.self_value));
                            deltas.push(ViewDelta::EntityDataReplaced {
                                key: path_key.clone(),
                                data_ipc,
                            });
                        }
                    }
                }
            }
        }

        deltas
    }
}

// ── Strategy ──────────────────────────────────────────────────────────────────

/// Incrementally builds and maintains a flamegraph as entity-level deltas.
///
/// Expected query format:
/// ```text
/// samples last {window} | <- stacks as stacks | <- frames as frames
/// ```
///
/// The `retention_ns` must match the query's time window (e.g.
/// `3_600_000_000_000` for `last 1h`).
pub struct FlamegraphStrategy {
    retention_ns: u64,
}

impl FlamegraphStrategy {
    /// Create a new flamegraph strategy.
    ///
    /// `retention_ns` — nanosecond retention window matching the query's time range.
    pub fn new(retention_ns: u64) -> Self {
        Self { retention_ns }
    }
}

#[async_trait]
impl ViewStrategy for FlamegraphStrategy {
    async fn transform(&self, mut stream: SeqlStream) -> ViewDeltaStream {
        let retention_ns = self.retention_ns;

        Box::pin(stream! {
            let mut state = FlamegraphState::new(retention_ns);
            let mut ready_sent = false;

            while let Some(result) = stream.next().await {
                let fd = match result {
                    Ok(fd) => fd,
                    Err(e) => {
                        yield ViewDelta::Error { message: e.to_string() };
                        return;
                    }
                };

                let metadata = match decode_metadata(&fd.app_metadata) {
                    Some(m) => m,
                    None => continue,
                };

                match metadata {
                    SeqlMetadata::Data { table } | SeqlMetadata::Append { table, .. } => {
                        if fd.data_body.is_empty() {
                            continue;
                        }
                        let batch = match sequins_query::frame::ipc_to_batch(&fd.data_body) {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::warn!("FlamegraphStrategy: failed to decode batch: {e}");
                                continue;
                            }
                        };

                        let deltas = match table.as_deref() {
                            None => state.process_samples(&batch),
                            Some("stacks") => state.process_stacks(&batch),
                            Some("frames") => state.process_frames(&batch),
                            Some(other) => {
                                tracing::debug!("FlamegraphStrategy: ignoring auxiliary table '{other}'");
                                vec![]
                            }
                        };

                        for delta in deltas {
                            yield delta;
                        }
                    }
                    SeqlMetadata::Heartbeat { watermark_ns } => {
                        let expiry_deltas = state.expire(watermark_ns);
                        for delta in expiry_deltas {
                            yield delta;
                        }
                        #[allow(unused_assignments)]
                        if !ready_sent {
                            ready_sent = true;
                            yield ViewDelta::Ready;
                        }
                        yield ViewDelta::Heartbeat { watermark_ns };
                    }
                    SeqlMetadata::Complete { .. } => {
                        yield ViewDelta::Ready;
                        return;
                    }
                    SeqlMetadata::Warning { code, message } => {
                        yield ViewDelta::Warning { code, message };
                    }
                    SeqlMetadata::Schema { .. }
                    | SeqlMetadata::Update { .. }
                    | SeqlMetadata::Expire { .. }
                    | SeqlMetadata::Replace { .. } => {}
                }
            }
        })
    }
}

// ── RecordBatch builders ──────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn make_descriptor_batch(
    path_key: &str,
    frame_id: u64,
    function_name: &str,
    system_name: Option<&str>,
    filename: Option<&str>,
    line: Option<i64>,
    depth: u32,
    parent_path_key: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        descriptor_schema(),
        vec![
            Arc::new(StringArray::from(vec![path_key])),
            Arc::new(UInt64Array::from(vec![frame_id])),
            Arc::new(StringArray::from(vec![function_name])),
            Arc::new(StringArray::from(vec![system_name])),
            Arc::new(StringArray::from(vec![filename])),
            Arc::new(Int64Array::from(vec![line])),
            Arc::new(UInt32Array::from(vec![depth])),
            Arc::new(StringArray::from(vec![parent_path_key])),
        ],
    )
    .expect("descriptor batch construction failed")
}

fn make_data_batch(total_value: i64, self_value: i64) -> RecordBatch {
    RecordBatch::try_new(
        data_schema(),
        vec![
            Arc::new(Int64Array::from(vec![total_value])),
            Arc::new(Int64Array::from(vec![self_value])),
        ],
    )
    .expect("data batch construction failed")
}

// ── Column helpers ────────────────────────────────────────────────────────────

fn col_u64(batch: &RecordBatch, name: &str, row: usize) -> Option<u64> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<UInt64Array>()
        .map(|a| a.value(row))
}

fn col_u32(batch: &RecordBatch, name: &str, row: usize) -> Option<u32> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<UInt32Array>()
        .map(|a| a.value(row))
}

fn col_i64(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    col.as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| a.value(row))
}

fn col_str_view(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }
    // Accepts both Utf8View (from OTLP ingest) and Utf8 (from constructed batches)
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        Some(arr.value(row).to_string())
    } else {
        col.as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| arr.value(row).to_string())
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, StringViewArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_frames_batch(frame_ids: &[u64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("frame_id", DataType::UInt64, false),
            Field::new("function_name", DataType::Utf8View, false),
            Field::new("system_name", DataType::Utf8View, true),
            Field::new("filename", DataType::Utf8View, true),
            Field::new("line", DataType::Int64, true),
            Field::new("column", DataType::Int64, true),
            Field::new("mapping_id", DataType::UInt64, true),
            Field::new("inline", DataType::Boolean, false),
        ]));
        let n = frame_ids.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(frame_ids.to_vec())),
                Arc::new(StringViewArray::from(
                    names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(StringViewArray::from(vec![None::<&str>; n])),
                Arc::new(StringViewArray::from(vec![None::<&str>; n])),
                Arc::new(Int64Array::from(vec![None::<i64>; n])),
                Arc::new(Int64Array::from(vec![None::<i64>; n])),
                Arc::new(UInt64Array::from(vec![None::<u64>; n])),
                Arc::new(BooleanArray::from(vec![false; n])),
            ],
        )
        .unwrap()
    }

    fn make_stacks_batch(stack_ids: &[u64], frame_ids: &[u64], positions: &[u32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("stack_id", DataType::UInt64, false),
            Field::new("frame_id", DataType::UInt64, false),
            Field::new("position", DataType::UInt32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(stack_ids.to_vec())),
                Arc::new(UInt64Array::from(frame_ids.to_vec())),
                Arc::new(UInt32Array::from(positions.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_samples_batch(stack_ids: &[u64], values: &[i64], times: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("profile_id", DataType::Utf8View, false),
            Field::new("stack_id", DataType::UInt64, false),
            Field::new("service", DataType::Utf8View, false),
            Field::new("time_unix_nano", DataType::Int64, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            Field::new("value_type", DataType::Utf8View, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let n = stack_ids.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec!["p1"; n])),
                Arc::new(UInt64Array::from(stack_ids.to_vec())),
                Arc::new(StringViewArray::from(vec!["api"; n])),
                Arc::new(Int64Array::from(times.to_vec())),
                Arc::new(UInt32Array::from(vec![1u32; n])),
                Arc::new(UInt32Array::from(vec![1u32; n])),
                Arc::new(StringViewArray::from(vec!["cpu"; n])),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn entity_created_for_new_path() {
        let mut state = FlamegraphState::new(3_600_000_000_000);

        // Register frames: frame 10 (root), frame 11 (leaf)
        let frames = make_frames_batch(&[10, 11], &["main", "handle_request"]);
        state.process_frames(&frames);

        // Register stack 1: position 0=leaf(11), position 1=root(10)
        let stacks = make_stacks_batch(&[1, 1], &[11, 10], &[0, 1]);
        state.process_stacks(&stacks);

        // Process one sample with stack_id=1, value=1000
        let samples = make_samples_batch(&[1], &[1000], &[1_000_000_000]);
        let deltas = state.process_samples(&samples);

        // Should emit EntityCreated for each depth (root + leaf = 2 nodes)
        let created: Vec<&ViewDelta> = deltas
            .iter()
            .filter(|d| matches!(d, ViewDelta::EntityCreated { .. }))
            .collect();
        assert_eq!(
            created.len(),
            2,
            "Expected 2 EntityCreated deltas (one per depth)"
        );
    }

    #[test]
    fn entity_data_replaced_on_second_sample() {
        let mut state = FlamegraphState::new(3_600_000_000_000);

        let frames = make_frames_batch(&[10, 11], &["main", "handle_request"]);
        state.process_frames(&frames);

        let stacks = make_stacks_batch(&[1, 1], &[11, 10], &[0, 1]);
        state.process_stacks(&stacks);

        // First sample creates nodes
        let s1 = make_samples_batch(&[1], &[1000], &[1_000_000_000]);
        state.process_samples(&s1);

        // Second sample should update existing nodes
        let s2 = make_samples_batch(&[1], &[500], &[2_000_000_000]);
        let deltas = state.process_samples(&s2);

        let replaced: Vec<&ViewDelta> = deltas
            .iter()
            .filter(|d| matches!(d, ViewDelta::EntityDataReplaced { .. }))
            .collect();
        assert_eq!(
            replaced.len(),
            2,
            "Expected 2 EntityDataReplaced deltas (one per depth)"
        );
    }

    #[test]
    fn expiry_removes_zero_value_nodes() {
        let mut state = FlamegraphState::new(3_600_000_000_000); // 1h retention

        let frames = make_frames_batch(&[10], &["main"]);
        state.process_frames(&frames);

        // Single-frame stack (root = leaf)
        let stacks = make_stacks_batch(&[1], &[10], &[0]);
        state.process_stacks(&stacks);

        // Sample at t=1000 (1 microsecond)
        let samples = make_samples_batch(&[1], &[100], &[1000]);
        state.process_samples(&samples);

        assert_eq!(state.nodes.len(), 1);

        // Expire: watermark = 1h + 2µs → threshold = 2µs > sample time 1µs
        let watermark = 3_600_000_000_000u64 + 2000;
        let expire_deltas = state.expire(watermark);

        // Node should be removed (total_value → 0)
        assert_eq!(state.nodes.len(), 0);
        let removed: Vec<&ViewDelta> = expire_deltas
            .iter()
            .filter(|d| matches!(d, ViewDelta::EntityRemoved { .. }))
            .collect();
        assert_eq!(removed.len(), 1);
    }

    #[test]
    fn partial_expiry_decrements_but_keeps_node() {
        let mut state = FlamegraphState::new(3_600_000_000_000);

        let frames = make_frames_batch(&[10], &["main"]);
        state.process_frames(&frames);

        let stacks = make_stacks_batch(&[1], &[10], &[0]);
        state.process_stacks(&stacks);

        // Two samples at different times
        let s1 = make_samples_batch(&[1], &[100], &[1000]);
        state.process_samples(&s1);
        let s2 = make_samples_batch(&[1], &[200], &[4_000_000_000_000]); // 4h — well within window
        state.process_samples(&s2);

        // Expire only the first sample (threshold falls between them)
        let watermark = 3_600_000_000_000u64 + 2000;
        let expire_deltas = state.expire(watermark);

        // Node should still exist with value 200
        assert_eq!(state.nodes.len(), 1);
        let node_key = "10";
        let node = &state.nodes[node_key];
        assert_eq!(node.total_value, 200);

        let replaced: Vec<&ViewDelta> = expire_deltas
            .iter()
            .filter(|d| matches!(d, ViewDelta::EntityDataReplaced { .. }))
            .collect();
        assert_eq!(replaced.len(), 1);
    }

    #[test]
    fn auxiliary_stacks_frames_emit_no_deltas() {
        let mut state = FlamegraphState::new(3_600_000_000_000);

        let frames = make_frames_batch(&[10], &["main"]);
        // process_frames returns nothing
        state.process_frames(&frames);
        assert_eq!(state.frames.len(), 1);

        let stacks = make_stacks_batch(&[1], &[10], &[0]);
        // process_stacks returns nothing
        state.process_stacks(&stacks);
        assert_eq!(state.stacks.len(), 1);
    }
}
