use crate::batch_chain::{compaction_loop, BatchChain, BatchMeta};
use crate::config::HotTierConfig;
use crate::error::{HotTierError, Result};
use arrow::array::{Array, BooleanArray, StringViewArray, UInt32Array, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use dashmap::DashSet;
use sequins_types::arrow_schema;
use sequins_types::models::InstrumentationScope;
use sequins_types::SignalType;
use std::collections::HashMap;
use std::sync::Arc;

/// Compact resource ID — content-addressed FNV-1a hash of the attribute set.
///
/// Using the hash directly as the ID means:
/// - Same resource attributes → same ID (deterministic, cross-restart stable)
/// - No sequential counter to persist
/// - No race conditions in concurrent registration
pub type ResourceId = u32;

/// Compact scope ID — content-addressed FNV-1a hash of (name, version, attributes).
pub type ScopeId = u32;

/// Stable FNV-1a 32-bit hash.
///
/// Unlike `DefaultHasher`, this uses a fixed seed so the result is deterministic
/// across process runs and different CPU architectures.
#[inline]
pub(crate) fn fnv1a_32(data: &[u8]) -> u32 {
    const OFFSET: u32 = 2166136261;
    const PRIME: u32 = 16777619;
    let mut hash = OFFSET;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Hot tier (in-memory) storage backed by 16 lock-free `BatchChain`s.
///
/// Each signal type has its own chain that accumulates `RecordBatch`es
/// as data arrives.  Background compaction tasks merge small batches
/// and (eventually) flush completed ones to the cold tier.
///
/// Resources and scopes are deduplicated via `DashSet` content-addressed
/// caches so that only one row per unique entity is ever pushed to the chain.
///
/// Each chain is stored as `Arc<BatchChain>` so that it can be handed to
/// DataFusion as an `Arc<dyn TableProvider>` without any additional wrapping.
/// `Arc<BatchChain>` derefs to `BatchChain`, so all `push` / `row_count`
/// call-sites remain unchanged.
pub struct HotTier {
    // -----------------------------------------------------------------------
    // 16 BatchChains — one per signal table
    // -----------------------------------------------------------------------
    pub spans: Arc<BatchChain>,
    pub span_links: Arc<BatchChain>,
    pub span_events: Arc<BatchChain>,
    /// Trace-level rollup (no `SignalType::Traces` variant yet).
    pub traces: Arc<BatchChain>,
    pub logs: Arc<BatchChain>,
    pub metrics: Arc<BatchChain>,
    pub datapoints: Arc<BatchChain>,
    pub histogram_datapoints: Arc<BatchChain>,
    pub exponential_histogram_datapoints: Arc<BatchChain>,
    pub profiles: Arc<BatchChain>,
    pub samples: Arc<BatchChain>,
    pub stacks: Arc<BatchChain>,
    pub frames: Arc<BatchChain>,
    pub mappings: Arc<BatchChain>,
    pub resources: Arc<BatchChain>,
    pub scopes: Arc<BatchChain>,

    // -----------------------------------------------------------------------
    // Content-addressed dedup sets — hash IS the ID, so just track presence.
    // -----------------------------------------------------------------------
    known_resources: DashSet<u32>,
    known_scopes: DashSet<u32>,
    /// Metric IDs seen since startup — prevents duplicate rows in the metrics metadata table.
    known_metrics: DashSet<[u8; 16]>,
    known_stacks: DashSet<u64>,
    known_frames: DashSet<u64>,
    known_mappings: DashSet<u64>,
}

impl HotTier {
    /// Create a new hot tier and spawn background compaction tasks for each chain.
    pub fn new(config: HotTierConfig) -> Self {
        let target_rows = config.max_entries;

        let make_chain =
            |schema: arrow::datatypes::SchemaRef, name: &'static str| -> Arc<BatchChain> {
                let (chain, rx) = BatchChain::new(schema.clone());
                let head = chain.head_arc();
                tokio::spawn(compaction_loop::<
                    fn(Arc<RecordBatch>, BatchMeta) -> std::future::Ready<()>,
                    std::future::Ready<()>,
                >(
                    head, schema, rx, target_rows, name.to_string(), None
                ));
                Arc::new(chain)
            };

        // Use span_schema for traces until a dedicated trace_schema exists.
        let trace_schema = arrow_schema::span_schema();

        Self {
            spans: make_chain(arrow_schema::span_schema(), "spans"),
            span_links: make_chain(arrow_schema::span_links_schema(), "span_links"),
            span_events: make_chain(arrow_schema::span_events_schema(), "span_events"),
            traces: make_chain(trace_schema, "traces"),
            logs: make_chain(arrow_schema::log_schema(), "logs"),
            metrics: make_chain(arrow_schema::metric_schema(), "metrics"),
            datapoints: make_chain(arrow_schema::series_data_point_schema(), "datapoints"),
            histogram_datapoints: make_chain(
                arrow_schema::histogram_series_data_point_schema(),
                "histogram_datapoints",
            ),
            exponential_histogram_datapoints: make_chain(
                arrow_schema::exp_histogram_data_point_schema(),
                "exp_histogram_datapoints",
            ),
            profiles: make_chain(arrow_schema::profile_schema(), "profiles"),
            samples: make_chain(arrow_schema::profile_samples_schema(), "samples"),
            stacks: make_chain(arrow_schema::profile_stacks_schema(), "stacks"),
            frames: make_chain(arrow_schema::profile_frames_schema(), "frames"),
            mappings: make_chain(arrow_schema::profile_mappings_schema(), "mappings"),
            resources: make_chain(arrow_schema::resource_schema(), "resources"),
            scopes: make_chain(arrow_schema::scope_schema(), "scopes"),
            known_resources: DashSet::new(),
            known_scopes: DashSet::new(),
            known_metrics: DashSet::new(),
            known_stacks: DashSet::new(),
            known_frames: DashSet::new(),
            known_mappings: DashSet::new(),
        }
    }

    /// Returns `true` if this metric ID is newly seen and has been recorded.
    ///
    /// Like `register_resource` / `register_scope`, this is idempotent:
    /// concurrent inserts of the same ID are safely collapsed by `DashSet`.
    /// The caller is responsible for building and pushing the batch row when
    /// this returns `true`.
    pub fn is_new_metric(&self, id: [u8; 16]) -> bool {
        self.known_metrics.insert(id)
    }

    // -----------------------------------------------------------------------
    // Batch-level dedup filters for content-addressed profile tables
    // -----------------------------------------------------------------------

    /// Filter a RecordBatch to rows whose `id_column` value is newly seen.
    ///
    /// Returns `None` if all rows were already known (avoids empty batch allocation).
    /// The DashSet is updated atomically for each row — concurrent calls are safe.
    fn filter_new_rows(
        &self,
        batch: &RecordBatch,
        id_column: &str,
        known_set: &DashSet<u64>,
    ) -> Option<RecordBatch> {
        let col_idx = batch.schema().index_of(id_column).ok()?;
        let ids = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()?;

        let mut mask = vec![false; batch.num_rows()];
        let mut any_new = false;
        for (i, flag) in mask.iter_mut().enumerate() {
            if !ids.is_null(i) {
                let id = ids.value(i);
                if known_set.insert(id) {
                    *flag = true;
                    any_new = true;
                }
            }
        }

        if !any_new {
            return None;
        }

        let boolean_mask = BooleanArray::from(mask);
        filter_record_batch(batch, &boolean_mask).ok()
    }

    /// Filter a stacks junction batch to rows whose `stack_id` is newly seen.
    ///
    /// Because the stacks table is a junction table (multiple rows per stack_id),
    /// we do a two-pass approach: first collect genuinely new stack_ids, then keep
    /// ALL rows matching those IDs.
    pub fn filter_new_stacks(&self, batch: &RecordBatch) -> Option<RecordBatch> {
        let col_idx = batch.schema().index_of("stack_id").ok()?;
        let ids = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()?;

        let mut new_ids = std::collections::HashSet::new();
        for i in 0..ids.len() {
            if !ids.is_null(i) {
                let id = ids.value(i);
                if self.known_stacks.insert(id) {
                    new_ids.insert(id);
                }
            }
        }

        if new_ids.is_empty() {
            return None;
        }

        let mask: Vec<bool> = (0..ids.len())
            .map(|i| !ids.is_null(i) && new_ids.contains(&ids.value(i)))
            .collect();

        let boolean_mask = BooleanArray::from(mask);
        filter_record_batch(batch, &boolean_mask).ok()
    }

    /// Filter a frames batch to rows whose `frame_id` is newly seen.
    pub fn filter_new_frames(&self, batch: &RecordBatch) -> Option<RecordBatch> {
        self.filter_new_rows(batch, "frame_id", &self.known_frames)
    }

    /// Filter a mappings batch to rows whose `mapping_id` is newly seen.
    pub fn filter_new_mappings(&self, batch: &RecordBatch) -> Option<RecordBatch> {
        self.filter_new_rows(batch, "mapping_id", &self.known_mappings)
    }

    // -----------------------------------------------------------------------
    // BatchChain access by SignalType
    // -----------------------------------------------------------------------

    /// Return a reference to the `BatchChain` for the given signal type.
    ///
    /// Because the chains are stored as `Arc<BatchChain>`, this derefs
    /// transparently — callers that need the raw chain for `push` / `row_count`
    /// are unchanged.
    pub fn chain(&self, signal: &SignalType) -> &BatchChain {
        match signal {
            SignalType::Spans => &self.spans,
            SignalType::SpanLinks => &self.span_links,
            SignalType::SpanEvents => &self.span_events,
            SignalType::Logs => &self.logs,
            SignalType::Metrics => &self.datapoints,
            SignalType::MetricsMetadata => &self.metrics,
            SignalType::Histograms => &self.histogram_datapoints,
            SignalType::ExpHistograms => &self.exponential_histogram_datapoints,
            SignalType::ProfilesMetadata => &self.profiles,
            SignalType::ProfileSamples => &self.samples,
            SignalType::ProfileStacks => &self.stacks,
            SignalType::ProfileFrames => &self.frames,
            SignalType::ProfileMappings => &self.mappings,
            SignalType::Resources => &self.resources,
            SignalType::Scopes => &self.scopes,
        }
    }

    /// Return a cloned `Arc<BatchChain>` for the given signal type.
    ///
    /// This is used by the DataFusion registration layer to hand the chain
    /// directly to a `SessionContext` as an `Arc<dyn TableProvider>` without
    /// any wrapping struct.
    pub fn chain_arc(&self, signal: &SignalType) -> Arc<BatchChain> {
        match signal {
            SignalType::Spans => Arc::clone(&self.spans),
            SignalType::SpanLinks => Arc::clone(&self.span_links),
            SignalType::SpanEvents => Arc::clone(&self.span_events),
            SignalType::Logs => Arc::clone(&self.logs),
            SignalType::Metrics => Arc::clone(&self.datapoints),
            SignalType::MetricsMetadata => Arc::clone(&self.metrics),
            SignalType::Histograms => Arc::clone(&self.histogram_datapoints),
            SignalType::ExpHistograms => Arc::clone(&self.exponential_histogram_datapoints),
            SignalType::ProfilesMetadata => Arc::clone(&self.profiles),
            SignalType::ProfileSamples => Arc::clone(&self.samples),
            SignalType::ProfileStacks => Arc::clone(&self.stacks),
            SignalType::ProfileFrames => Arc::clone(&self.frames),
            SignalType::ProfileMappings => Arc::clone(&self.mappings),
            SignalType::Resources => Arc::clone(&self.resources),
            SignalType::Scopes => Arc::clone(&self.scopes),
        }
    }

    // -----------------------------------------------------------------------
    // Resource / scope registration
    // -----------------------------------------------------------------------

    /// Register a resource and get its content-addressed ID.
    ///
    /// The ID is an FNV-1a hash of the sorted key=value attribute pairs, making it
    /// deterministic across process restarts and multiple nodes.  Concurrent
    /// registrations for the same attributes are idempotent — the first writer wins
    /// and subsequent calls return the same ID without a data race.
    pub fn register_resource(&self, attributes: &HashMap<String, String>) -> Result<ResourceId> {
        let id = resource_hash(attributes);

        // DashSet::insert returns true only when the value is newly inserted,
        // so at most one row per unique ID is ever pushed to the chain —
        // concurrent insertions of the same ID are safely collapsed here.
        if !self.known_resources.insert(id) {
            return Ok(id);
        }

        let service_name = attributes
            .get("service.name")
            .map(|s| s.as_str())
            .unwrap_or("");
        let attrs_json = attrs_to_json(attributes);
        let schema = arrow_schema::resource_schema();
        let batch = build_resource_batch(id, service_name, &attrs_json, &schema)
            .map_err(HotTierError::Storage)?;
        self.resources.push(
            Arc::new(batch),
            BatchMeta {
                min_timestamp: 0,
                max_timestamp: 0,
                row_count: 1,
            },
        );

        Ok(id)
    }

    /// Register a scope and get its content-addressed ID.
    ///
    /// The ID is an FNV-1a hash of (name, version, sorted attribute pairs),
    /// making it deterministic across restarts and multiple nodes.
    pub fn register_scope(&self, scope: &InstrumentationScope) -> Result<ScopeId> {
        let id = scope_hash(scope);

        if !self.known_scopes.insert(id) {
            return Ok(id);
        }

        let attrs_json = serde_json::to_string(&scope.attributes).unwrap_or_default();
        let schema = arrow_schema::scope_schema();
        let batch = build_scope_batch(id, &scope.name, &scope.version, &attrs_json, &schema)
            .map_err(HotTierError::Storage)?;
        self.scopes.push(
            Arc::new(batch),
            BatchMeta {
                min_timestamp: 0,
                max_timestamp: 0,
                row_count: 1,
            },
        );

        Ok(id)
    }

    /// Return current storage statistics.
    ///
    /// Counts rows in the relevant BatchChains (spans, logs, metrics metadata,
    /// profiles metadata). This is O(n) in chain length but chains are short.
    pub fn stats(&self) -> StorageStats {
        StorageStats {
            span_count: self.spans.row_count(),
            log_count: self.logs.row_count(),
            metric_count: self.metrics.row_count(),
            profile_count: self.profiles.row_count(),
        }
    }

    /// Clear all chains (no-op: BatchChains don't support in-place clearing;
    /// tests that relied on clear() should create a fresh HotTier).
    pub fn clear(&self) {
        // No-op: BatchChains don't support in-place clearing.
    }
}

// -----------------------------------------------------------------------
// Hashing helpers
// -----------------------------------------------------------------------

/// Compute a deterministic FNV-1a hash over a sorted `key=value` attribute map.
fn resource_hash(attributes: &HashMap<String, String>) -> u32 {
    let mut keys: Vec<&String> = attributes.keys().collect();
    keys.sort();
    let mut content = String::new();
    for key in keys {
        if let Some(value) = attributes.get(key) {
            content.push_str(key);
            content.push('\0');
            content.push_str(value);
            content.push('\n');
        }
    }
    fnv1a_32(content.as_bytes())
}

/// Compute a deterministic FNV-1a hash over scope (name, version, sorted attrs).
fn scope_hash(scope: &InstrumentationScope) -> u32 {
    let mut content = String::new();
    content.push_str(&scope.name);
    content.push('\0');
    content.push_str(&scope.version);
    content.push('\0');
    let mut attr_keys: Vec<&String> = scope.attributes.keys().collect();
    attr_keys.sort();
    for key in attr_keys {
        content.push_str(key);
        content.push('\0');
        let val_str = match scope.attributes.get(key) {
            Some(v) => serde_json::to_string(v).unwrap_or_default(),
            None => String::new(),
        };
        content.push_str(&val_str);
        content.push('\n');
    }
    fnv1a_32(content.as_bytes())
}

/// Serialize a string-map to a JSON string.
fn attrs_to_json(attrs: &HashMap<String, String>) -> String {
    serde_json::to_string(attrs).unwrap_or_else(|_| "{}".to_string())
}

// -----------------------------------------------------------------------
// RecordBatch builders for resources and scopes
// -----------------------------------------------------------------------

/// Build a single-row `RecordBatch` for the resources chain.
fn build_resource_batch(
    id: u32,
    service_name: &str,
    attrs_json: &str,
    schema: &arrow::datatypes::SchemaRef,
) -> std::result::Result<RecordBatch, String> {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![id])),
            Arc::new(StringViewArray::from(vec![service_name])),
            Arc::new(StringViewArray::from(vec![attrs_json])),
        ],
    )
    .map_err(|e| format!("build_resource_batch: {e}"))
}

/// Build a single-row `RecordBatch` for the scopes chain.
fn build_scope_batch(
    id: u32,
    name: &str,
    version: &str,
    attrs_json: &str,
    schema: &arrow::datatypes::SchemaRef,
) -> std::result::Result<RecordBatch, String> {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(vec![id])),
            Arc::new(StringViewArray::from(vec![name])),
            Arc::new(StringViewArray::from(vec![version])),
            Arc::new(StringViewArray::from(vec![attrs_json])),
        ],
    )
    .map_err(|e| format!("build_scope_batch: {e}"))
}

// -----------------------------------------------------------------------
// Public statistics types
// -----------------------------------------------------------------------

/// Statistics about evicted entries (legacy; eviction is a no-op for BatchChains).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvictionStats {
    pub spans_evicted: usize,
    pub logs_evicted: usize,
    pub metrics_evicted: usize,
    pub metric_data_points_evicted: usize,
    pub histogram_data_points_evicted: usize,
    pub profiles_evicted: usize,
}

impl EvictionStats {
    pub fn total(&self) -> usize {
        self.spans_evicted
            + self.logs_evicted
            + self.metrics_evicted
            + self.metric_data_points_evicted
            + self.histogram_data_points_evicted
            + self.profiles_evicted
    }
}

/// Current storage statistics
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStats {
    pub span_count: usize,
    pub log_count: usize,
    pub metric_count: usize,
    pub profile_count: usize,
}

impl StorageStats {
    pub fn total(&self) -> usize {
        self.span_count + self.log_count + self.metric_count + self.profile_count
    }
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_types::models::Duration;

    fn create_test_config() -> HotTierConfig {
        HotTierConfig {
            max_age: Duration::from_minutes(5),
            max_entries: 1000,
        }
    }

    #[tokio::test]
    async fn test_hot_tier_new() {
        let config = create_test_config();
        let hot_tier = HotTier::new(config);
        let stats = hot_tier.stats();
        assert_eq!(stats.total(), 0);
    }

    #[tokio::test]
    async fn test_register_resource_dedup() {
        let hot_tier = HotTier::new(create_test_config());
        let mut attrs = HashMap::new();
        attrs.insert("service.name".to_string(), "my-service".to_string());

        let id1 = hot_tier.register_resource(&attrs).unwrap();
        let id2 = hot_tier.register_resource(&attrs).unwrap();
        assert_eq!(id1, id2, "same attributes should yield same resource ID");
    }

    #[tokio::test]
    async fn test_register_resource_different_attrs() {
        let hot_tier = HotTier::new(create_test_config());
        let mut attrs1 = HashMap::new();
        attrs1.insert("service.name".to_string(), "service-a".to_string());
        let mut attrs2 = HashMap::new();
        attrs2.insert("service.name".to_string(), "service-b".to_string());

        let id1 = hot_tier.register_resource(&attrs1).unwrap();
        let id2 = hot_tier.register_resource(&attrs2).unwrap();
        assert_ne!(id1, id2, "different attributes should yield different IDs");
    }

    #[tokio::test]
    async fn test_register_scope_dedup() {
        let hot_tier = HotTier::new(create_test_config());
        let scope = InstrumentationScope::simple("my.lib".to_string(), "1.0".to_string());

        let id1 = hot_tier.register_scope(&scope).unwrap();
        let id2 = hot_tier.register_scope(&scope).unwrap();
        assert_eq!(id1, id2, "same scope should yield same scope ID");
    }

    #[tokio::test]
    async fn test_register_scope_different_versions() {
        let hot_tier = HotTier::new(create_test_config());
        let scope_v1 = InstrumentationScope::simple("my.lib".to_string(), "1.0".to_string());
        let scope_v2 = InstrumentationScope::simple("my.lib".to_string(), "2.0".to_string());

        let id1 = hot_tier.register_scope(&scope_v1).unwrap();
        let id2 = hot_tier.register_scope(&scope_v2).unwrap();
        assert_ne!(
            id1, id2,
            "different versions should yield different scope IDs"
        );
    }

    #[tokio::test]
    async fn test_chain_returns_correct_schema() {
        let hot_tier = HotTier::new(create_test_config());
        let span_schema = arrow_schema::span_schema();
        let chain_schema = hot_tier.chain(&SignalType::Spans).schema();
        assert_eq!(chain_schema.fields().len(), span_schema.fields().len());
    }

    #[test]
    fn test_fnv1a_32_deterministic() {
        let h1 = fnv1a_32(b"hello world");
        let h2 = fnv1a_32(b"hello world");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_fnv1a_32_differs_for_different_input() {
        let h1 = fnv1a_32(b"hello");
        let h2 = fnv1a_32(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_resource_hash_order_independent() {
        let mut attrs1 = HashMap::new();
        attrs1.insert("a".to_string(), "1".to_string());
        attrs1.insert("b".to_string(), "2".to_string());

        let mut attrs2 = HashMap::new();
        attrs2.insert("b".to_string(), "2".to_string());
        attrs2.insert("a".to_string(), "1".to_string());

        assert_eq!(resource_hash(&attrs1), resource_hash(&attrs2));
    }

    #[test]
    fn test_eviction_stats_total() {
        let stats = EvictionStats {
            spans_evicted: 1,
            logs_evicted: 2,
            metrics_evicted: 3,
            metric_data_points_evicted: 4,
            histogram_data_points_evicted: 5,
            profiles_evicted: 6,
        };
        assert_eq!(stats.total(), 21);
    }

    #[test]
    fn test_storage_stats_total() {
        let stats = StorageStats {
            span_count: 10,
            log_count: 20,
            metric_count: 5,
            profile_count: 3,
        };
        assert_eq!(stats.total(), 38);
    }

    /// Verify that every SignalType variant maps to a BatchChain whose schema
    /// matches the canonical schema from `SignalType::schema()`.
    ///
    /// This catches regressions where the hot-tier field order or type deviates
    /// from the signal-type schema (e.g. the Metrics→datapoints / MetricsMetadata→metrics
    /// mapping which is easy to get backwards).
    #[tokio::test]
    async fn test_chain_mapping_for_all_signal_types() {
        let hot_tier = HotTier::new(create_test_config());

        for signal in sequins_types::SignalType::all() {
            let chain_schema = hot_tier.chain(signal).schema();
            let type_schema = signal.schema();
            assert_eq!(
                chain_schema.fields().len(),
                type_schema.fields().len(),
                "{:?}: chain schema field count ({}) != type schema field count ({})",
                signal,
                chain_schema.fields().len(),
                type_schema.fields().len()
            );
            assert_eq!(
                chain_schema, type_schema,
                "{:?}: chain schema does not match SignalType::schema()",
                signal
            );
        }
    }
}
