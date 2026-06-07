//! Per-tier compaction: merge multiple small `.vortex` files per partition into one.
//!
//! Each hot-tier flush writes a small Vortex file.  Over time a single date
//! partition accumulates many files (one per flush).  Compaction reads all of
//! them, concatenates their rows, and rewrites as a single merged file, which
//! reduces file-count overhead for both DataFusion listing and Vortex pruning.

use super::cold_tier::ColdTier;
use crate::error::{Error, Result};
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use sequins_types::SignalType;
use std::collections::HashMap;
#[allow(unused_imports)]
use vortex::VortexSessionDefault;

impl ColdTier {
    /// Compact all `.vortex` files under `signal_prefix/` that share a partition
    /// directory (same `year=/month=/day=/hour=` prefix).
    ///
    /// For each partition that contains ≥ 2 files, the files are read,
    /// concatenated, and rewritten as a single merged file.  The originals
    /// are then deleted.
    ///
    /// Returns the number of files that were removed (originals minus merged).
    pub async fn compact_signal(&self, signal: SignalType, schema: SchemaRef) -> Result<usize> {
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);

        // The prefix used by write_signal_batch for this signal.
        let signal_prefix = signal_cold_path(signal);
        let full_prefix_str = format!("{}/{}", base_path.trim_end_matches('/'), signal_prefix);
        let prefix = ObjectPath::from(full_prefix_str.as_str());

        // Collect all .vortex files and group by partition directory.
        let mut by_partition: HashMap<String, Vec<ObjectPath>> = HashMap::new();
        let mut list_stream = self.store.list(Some(&prefix));
        while let Some(meta) = list_stream.next().await {
            let meta =
                meta.map_err(|e| Error::Storage(format!("Failed to list objects: {}", e)))?;
            let loc = meta.location.to_string();
            if !loc.ends_with(".vortex") {
                continue;
            }
            // Partition key = everything up to (but not including) the filename.
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
        for (partition_key, paths) in &by_partition {
            if paths.len() < 2 {
                continue; // nothing to compact
            }

            // Read all files in this partition into RecordBatches.
            let mut batches: Vec<RecordBatch> = Vec::new();
            for path in paths {
                let path_str = path.to_string();
                use vortex::file::OpenOptionsSessionExt;
                use vortex::session::VortexSession;
                use vortex::VortexSessionDefault;
                let session = VortexSession::default();
                let vxf = match session
                    .open_options()
                    .open_object_store(&self.store, path_str.as_str())
                    .await
                {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let scan = match vxf.scan() {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let mut stream = match scan.into_record_batch_stream(schema.clone()) {
                    Ok(s) => Box::pin(s),
                    Err(_) => continue,
                };
                while let Some(batch) = stream.next().await {
                    if let Ok(b) = batch {
                        batches.push(b);
                    }
                }
            }

            if batches.is_empty() {
                continue;
            }

            // Concatenate all batches into one.
            let merged = concat_batches(&schema, &batches)
                .map_err(|e| Error::Storage(format!("Failed to concat batches: {}", e)))?;

            if merged.num_rows() == 0 {
                continue;
            }

            // Write the merged batch to a new file in the same partition.
            let now_ns = crate::helpers::generate_partition_path(signal_prefix, {
                &sequins_types::models::Timestamp::now()
                    .map_err(|e| Error::Storage(format!("Failed to get timestamp: {}", e)))?
            });
            let merged_path_str = format!("{}/{}", base_path.trim_end_matches('/'), now_ns);
            self.write_record_batch(
                merged,
                schema.clone(),
                &merged_path_str,
                None, // companion index not regenerated during compaction
            )
            .await?;

            // Delete the original files.
            for path in paths {
                self.store
                    .delete(path)
                    .await
                    .map_err(|e| Error::Storage(format!("Failed to delete {}: {}", path, e)))?;
                files_removed += 1;
            }
            // We replaced N files with 1, net removal = N - 1.
            files_removed = files_removed.saturating_sub(1);
            let _ = partition_key; // suppress unused warning
        }

        Ok(files_removed)
    }
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
