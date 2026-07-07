//! Per-tier compaction: merge multiple small `.vortex` files per partition into one.
//!
//! Each hot-tier flush writes a small Vortex file.  Over time a single date
//! partition accumulates many files (one per flush).  Compaction reads all of
//! them, concatenates their rows, and rewrites as a single merged file, which
//! reduces file-count overhead for both DataFusion listing and Vortex pruning.
//!
//! # Shared-dataset safety
//!
//! Cold is a single dataset shared by the whole cluster, so compaction must be
//! safe even if two nodes run it concurrently and must never lose data:
//!
//! - **Only raw flush files are merged** (`compacted-*` outputs are excluded), so
//!   the input set for a partition is stable across retries. The merged file's
//!   name is derived deterministically from that input set
//!   (`compacted-{hash}.vortex`), so a re-run — or a second node — writes the
//!   *same* bytes to the *same* path (idempotent, no duplication).
//! - **All-or-nothing read**: if any input can't be read, the partition is
//!   skipped entirely — a file whose rows we couldn't recover is never deleted.
//! - **Durability-gated delete**: inputs are deleted only after the merged file
//!   is confirmed present and non-empty in object storage.
//! - **Recent files are skipped** so compaction never races a concurrent flush
//!   still writing to the same partition.

use super::cold_tier::ColdTier;
use crate::error::{Error, Result};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use sequins_arrow_schema::SignalType;
use std::collections::HashMap;
#[allow(unused_imports)]
use vortex::VortexSessionDefault;

/// A raw flush file is only eligible for compaction once it is at least this old
/// (nanoseconds), so compaction never races a concurrent flush still writing the
/// partition.
const COMPACTION_MIN_AGE_NS: i64 = 60_000_000_000; // 60s

impl ColdTier {
    /// Compact the small raw `.vortex` flush files in each partition of `signal`
    /// into one merged file per partition.
    ///
    /// Safe to run concurrently on multiple nodes over the shared dataset and
    /// idempotent on retry (see the module docs). Returns the net number of files
    /// removed (inputs deleted minus merged outputs written).
    pub async fn compact_signal(&self, signal: SignalType, schema: SchemaRef) -> Result<usize> {
        let base_path = crate::store_base_path(&self.config.uri);

        // The prefix used by write_signal_batch for this signal.
        let signal_prefix = signal_cold_path(signal);
        let full_prefix_str = format!("{}/{}", base_path.trim_end_matches('/'), signal_prefix);
        let prefix = ObjectPath::from(full_prefix_str.as_str());

        let cutoff_ns = {
            let now = sequins_types::models::Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get timestamp: {}", e)))?;
            now.as_nanos() - COMPACTION_MIN_AGE_NS
        };

        // Collect eligible **raw** .vortex files, grouped by partition directory.
        // `compacted-*` outputs are excluded so the input set is stable across
        // retries; too-recent files are excluded so we don't race a live flush.
        let mut by_partition: HashMap<String, Vec<ObjectPath>> = HashMap::new();
        let mut list_stream = self.store.list(Some(&prefix));
        while let Some(meta) = list_stream.next().await {
            let meta =
                meta.map_err(|e| Error::Storage(format!("Failed to list objects: {}", e)))?;
            let loc = meta.location.to_string();
            if !loc.ends_with(".vortex") {
                continue;
            }
            let filename = loc.rsplit('/').next().unwrap_or(&loc);
            if filename.starts_with("compacted-") {
                continue; // already a compaction output — leave it be
            }
            // Filename is `{ts_nanos}-{node_id}-{seq}.vortex`; skip if too recent.
            match filename
                .strip_suffix(".vortex")
                .and_then(|s| s.split('-').next())
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(ts) if ts < cutoff_ns => {}
                _ => continue, // unparseable or too recent → not eligible
            }
            let partition = loc
                .rfind('/')
                .map(|i| loc[..i].to_string())
                .unwrap_or_default();
            by_partition
                .entry(partition)
                .or_default()
                .push(meta.location);
        }

        let mut files_removed = 0usize;
        for (partition, mut paths) in by_partition {
            if paths.len() < 2 {
                continue; // nothing to compact
            }
            // Deterministic order so the derived output name is stable.
            paths.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));

            // Read every input; all-or-nothing so we never delete rows we failed
            // to recover.
            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut read_ok = true;
            for path in &paths {
                match self.read_vortex_file(path, &schema).await {
                    Ok(mut bs) => batches.append(&mut bs),
                    Err(e) => {
                        tracing::warn!(path = %path, error = %e, "compaction: input unreadable; skipping partition");
                        read_ok = false;
                        break;
                    }
                }
            }
            if !read_ok || batches.is_empty() {
                continue;
            }

            let merged = concat_batches(&schema, &batches)
                .map_err(|e| Error::Storage(format!("Failed to concat batches: {}", e)))?;
            if merged.num_rows() == 0 {
                continue;
            }

            // Deterministic output name derived from the (sorted) input set: a
            // retry or a second node writes the same bytes to the same path.
            let merged_path = format!("{}/compacted-{}.vortex", partition, hash_inputs(&paths));
            self.write_record_batch_at(merged, schema.clone(), &merged_path, None)
                .await?;

            // Durability gate: confirm the merged file is present and non-empty
            // before deleting any input.
            match self
                .store
                .head(&ObjectPath::from(merged_path.as_str()))
                .await
            {
                Ok(meta) if meta.size > 0 => {}
                _ => {
                    tracing::warn!(path = %merged_path, "compaction: merged output not durable; keeping inputs");
                    continue;
                }
            }

            for path in &paths {
                if let Err(e) = self.store.delete(path).await {
                    // A concurrent compactor may have deleted it already — tolerate.
                    tracing::warn!(path = %path, error = %e, "compaction: input delete failed");
                    continue;
                }
                files_removed += 1;
            }
            // N inputs replaced by 1 output: net removed = N - 1.
            files_removed = files_removed.saturating_sub(1);
        }

        Ok(files_removed)
    }

    /// Read all record batches from a single Vortex file.
    async fn read_vortex_file(
        &self,
        path: &ObjectPath,
        schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        use vortex::file::OpenOptionsSessionExt;
        use vortex::session::VortexSession;
        use vortex::VortexSessionDefault;
        let session = VortexSession::default();
        let vxf = session
            .open_options()
            .open_object_store(&self.store, path.as_ref())
            .await
            .map_err(|e| Error::Storage(format!("open {}: {}", path, e)))?;
        let scan = vxf
            .scan()
            .map_err(|e| Error::Storage(format!("scan {}: {}", path, e)))?;
        let mut stream = Box::pin(
            scan.into_record_batch_stream(schema.clone())
                .map_err(|e| Error::Storage(format!("stream {}: {}", path, e)))?,
        );
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            out.push(batch.map_err(|e| Error::Storage(format!("read batch {}: {}", path, e)))?);
        }
        Ok(out)
    }
}

/// FNV-1a/64 (hex) over the sorted input basenames — a stable, content-derived
/// suffix so the same input set always yields the same merged filename.
fn hash_inputs(paths: &[ObjectPath]) -> String {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut h = OFFSET;
    for p in paths {
        let name = p.as_ref().rsplit('/').next().unwrap_or(p.as_ref());
        for &b in name.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(PRIME);
        }
        h ^= b'\n' as u64;
        h = h.wrapping_mul(PRIME);
    }
    format!("{h:016x}")
}

/// Map a `SignalType` to its cold-tier path prefix (the same paths used by `write_signal`).
fn signal_cold_path(signal: SignalType) -> &'static str {
    match signal {
        SignalType::Spans => "spans",
        SignalType::Logs => "logs",
        SignalType::SpanLinks => "spans/links",
        SignalType::SpanEvents => "spans/events",
        SignalType::MetricsMetadata => "metrics/metadata",
        SignalType::Metrics => "metrics/data",
        SignalType::Histograms => "metrics/histograms",
        SignalType::ExpHistograms => "metrics/exp_histograms",
        SignalType::ProfilesMetadata => "profiles/metadata",
        SignalType::ProfileSamples => "profiles/samples",
        SignalType::ProfileStacks => "profiles/stacks",
        SignalType::ProfileFrames => "profiles/frames",
        SignalType::ProfileMappings => "profiles/mappings",
        SignalType::Resources => "resources",
        SignalType::Scopes => "scopes",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::create_test_cold_tier;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    #[tokio::test]
    async fn compact_merges_old_raw_files_preserves_rows_and_skips_recent() {
        let (ct, _tmp) = create_test_cold_tier().await;
        let base = crate::store_base_path(&ct.config.uri)
            .trim_end_matches('/')
            .to_string();
        let dir = format!("{base}/logs/year=2020/month=01/day=01");

        // Two OLD raw files (ts far below now-60s) that should be compacted...
        ct.write_record_batch_at(
            batch(vec![1, 2]),
            test_schema(),
            &format!("{dir}/1000000000-a-0.vortex"),
            None,
        )
        .await
        .unwrap();
        ct.write_record_batch_at(
            batch(vec![3, 4, 5]),
            test_schema(),
            &format!("{dir}/1000000001-a-1.vortex"),
            None,
        )
        .await
        .unwrap();
        // ...plus a RECENT file that must be left untouched (could still be flushing).
        let recent_ts = sequins_types::models::Timestamp::now().unwrap().as_nanos();
        ct.write_record_batch_at(
            batch(vec![9]),
            test_schema(),
            &format!("{dir}/{recent_ts}-a-2.vortex"),
            None,
        )
        .await
        .unwrap();

        let removed = ct
            .compact_signal(SignalType::Logs, test_schema())
            .await
            .unwrap();
        assert_eq!(
            removed, 1,
            "two old raw files -> one merged (net removed 1)"
        );

        // Inspect the resulting partition.
        let prefix = ObjectPath::from(format!("{base}/logs").as_str());
        let mut s = ct.store.list(Some(&prefix));
        let mut compacted = Vec::new();
        let mut raw_old = 0;
        let mut recent = 0;
        while let Some(m) = s.next().await {
            let loc = m.unwrap().location;
            let name = loc.as_ref().rsplit('/').next().unwrap().to_string();
            if name.starts_with("compacted-") {
                compacted.push(loc);
            } else if name.starts_with("1000000000") || name.starts_with("1000000001") {
                raw_old += 1;
            } else {
                recent += 1;
            }
        }
        assert_eq!(compacted.len(), 1, "exactly one compacted output");
        assert_eq!(raw_old, 0, "old raw inputs deleted after merge");
        assert_eq!(recent, 1, "recent file skipped (never a compaction input)");

        // The merged file must contain every row from the two old inputs.
        let rows: usize = ct
            .read_vortex_file(&compacted[0], &test_schema())
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 5, "merged file preserves all rows (2 + 3)");
    }

    #[tokio::test]
    async fn compact_is_idempotent_on_rerun() {
        // A second run over the same partition (now holding one compacted file and
        // no eligible raw inputs) is a no-op — it must not touch the merged data.
        let (ct, _tmp) = create_test_cold_tier().await;
        let base = crate::store_base_path(&ct.config.uri)
            .trim_end_matches('/')
            .to_string();
        let dir = format!("{base}/logs/year=2020/month=01/day=01");
        ct.write_record_batch_at(
            batch(vec![1, 2]),
            test_schema(),
            &format!("{dir}/1000000000-a-0.vortex"),
            None,
        )
        .await
        .unwrap();
        ct.write_record_batch_at(
            batch(vec![3]),
            test_schema(),
            &format!("{dir}/1000000001-a-1.vortex"),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            ct.compact_signal(SignalType::Logs, test_schema())
                .await
                .unwrap(),
            1
        );
        // Second run: only a compacted-* file remains → nothing eligible → no-op.
        assert_eq!(
            ct.compact_signal(SignalType::Logs, test_schema())
                .await
                .unwrap(),
            0
        );
    }

    #[test]
    fn hash_inputs_is_stable() {
        let a = ObjectPath::from("logs/y/1000-a-0.vortex");
        let b = ObjectPath::from("logs/y/2000-a-1.vortex");
        assert_eq!(
            hash_inputs(&[a.clone(), b.clone()]),
            hash_inputs(&[a, b]),
            "same input set must hash identically (deterministic output name)"
        );
    }
}
