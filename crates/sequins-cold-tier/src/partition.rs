//! Partition path generation and cleanup utilities

use super::cold_tier::ColdTier;
use crate::error::{Error, Result};
use object_store::ObjectStoreExt;
use sequins_types::models::Timestamp;

impl ColdTier {
    pub(crate) fn generate_metric_partition_path(
        _metric_name: &str,
        timestamp: &Timestamp,
    ) -> String {
        use chrono::{DateTime, Timelike, Utc};

        let dt = DateTime::<Utc>::from_timestamp(timestamp.as_secs(), 0).unwrap_or_else(Utc::now);

        // Use 2-hour buckets (0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22)
        let hour_bucket = (dt.hour() / 2) * 2;

        format!(
            "metrics/data/{}/{:02}/{}.vortex",
            dt.format("%Y-%m-%d"),
            hour_bucket,
            timestamp.as_nanos()
        )
    }

    /// Generate partition path for histogram data points.
    pub(crate) fn generate_histogram_partition_path(
        _metric_name: &str,
        timestamp: &Timestamp,
    ) -> String {
        use chrono::{DateTime, Timelike, Utc};

        let dt = DateTime::<Utc>::from_timestamp(timestamp.as_secs(), 0).unwrap_or_else(Utc::now);

        // Use 2-hour buckets (0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22)
        let hour_bucket = (dt.hour() / 2) * 2;

        format!(
            "metrics/histograms/{}/{:02}/{}.vortex",
            dt.format("%Y-%m-%d"),
            hour_bucket,
            timestamp.as_nanos()
        )
    }

    /// Delete all `.vortex` files under `telemetry_type/` that are older than `retention_period`.
    ///
    /// Uses `ObjectStore::list` rather than `std::fs::read_dir` so it works on any
    /// object store backend (local filesystem, S3, GCS, Azure Blob Storage).
    ///
    /// Files are identified by their nanosecond-timestamp filename: every Vortex file
    /// written by `write_signal_batch` is named `{timestamp_nanos}.vortex`.
    pub async fn cleanup_old_files(
        &self,
        telemetry_type: &str,
        retention_period: sequins_types::Duration,
    ) -> Result<usize> {
        use futures::StreamExt;
        use object_store::path::Path as ObjectPath;

        let cutoff_nanos = {
            let now = Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;
            (now - retention_period).as_nanos()
        };

        // Build the full prefix including the base path, since the LocalFileSystem store
        // is rooted at `/` and uses absolute paths (matching write_signal_batch behaviour).
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let prefix_str = format!("{}/{}", base_path.trim_end_matches('/'), telemetry_type);
        let prefix = ObjectPath::from(prefix_str.as_str());
        let mut list_stream = self.store.list(Some(&prefix));
        let mut to_delete: Vec<ObjectPath> = Vec::new();

        while let Some(meta) = list_stream.next().await {
            let meta =
                meta.map_err(|e| Error::Storage(format!("Failed to list objects: {}", e)))?;

            let location = meta.location.to_string();
            if !location.ends_with(".vortex") {
                continue;
            }

            // Filename is the last path component; strip `.vortex` to get the nanos timestamp.
            if let Some(filename) = location.split('/').next_back() {
                if let Some(ts_str) = filename.strip_suffix(".vortex") {
                    if let Ok(file_nanos) = ts_str.parse::<i64>() {
                        if file_nanos < cutoff_nanos {
                            to_delete.push(meta.location);
                        }
                    }
                }
            }
        }

        let deleted_count = to_delete.len();
        for path in to_delete {
            self.store
                .delete(&path)
                .await
                .map_err(|e| Error::Storage(format!("Failed to delete {}: {}", path, e)))?;
        }

        Ok(deleted_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_types::models::Timestamp;

    #[test]
    fn test_partition_key_generation_from_timestamp() {
        // Create a known timestamp: 2024-01-15 14:30:00 UTC
        let timestamp = Timestamp::from_secs(1705328400);

        let path = ColdTier::generate_metric_partition_path("test_metric", &timestamp);

        // Should include date in YYYY-MM-DD format
        assert!(path.contains("2024-01-15"));
        // Should include metrics/data prefix
        assert!(path.starts_with("metrics/data/"));
        // Should end with .vortex
        assert!(path.ends_with(".vortex"));
    }

    #[test]
    fn test_partition_path_construction() {
        let timestamp = Timestamp::from_secs(1705328400); // 2024-01-15 14:30:00 UTC

        let path = ColdTier::generate_metric_partition_path("test_metric", &timestamp);

        // Should have the format: metrics/data/YYYY-MM-DD/HH/{timestamp}.vortex
        // Hour bucket should be 14 (2-hour bucket)
        assert!(path.contains("metrics/data/2024-01-15/14/"));
    }

    #[test]
    fn test_partition_key_different_dates() {
        let timestamp1 = Timestamp::from_secs(1705328400); // 2024-01-15
        let timestamp2 = Timestamp::from_secs(1705414800); // 2024-01-16

        let path1 = ColdTier::generate_metric_partition_path("test_metric", &timestamp1);
        let path2 = ColdTier::generate_metric_partition_path("test_metric", &timestamp2);

        // Different dates should generate different paths
        assert_ne!(path1, path2);
        assert!(path1.contains("2024-01-15"));
        assert!(path2.contains("2024-01-16"));
    }

    #[tokio::test]
    async fn test_cleanup_old_files_via_object_store() {
        use crate::test_helpers::create_test_cold_tier;
        use object_store::path::Path as ObjectPath;
        use object_store::PutPayload;

        let (cold_tier, _temp) = create_test_cold_tier().await;

        // Build paths matching the absolute-path convention used by write_record_batch.
        let base = cold_tier
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&cold_tier.config.uri)
            .to_string();

        let old_nanos: i64 = 946_684_800_000_000_000; // 2000-01-01
        let old_path = ObjectPath::from(
            format!(
                "{}/spans/year=2000/month=01/day=01/{}.vortex",
                base, old_nanos
            )
            .as_str(),
        );
        cold_tier
            .store
            .put(&old_path, PutPayload::from_static(b"old"))
            .await
            .unwrap();

        let future_nanos: i64 = 4_102_444_800_000_000_000; // 2100-01-01
        let new_path = ObjectPath::from(
            format!(
                "{}/spans/year=2100/month=01/day=01/{}.vortex",
                base, future_nanos
            )
            .as_str(),
        );
        cold_tier
            .store
            .put(&new_path, PutPayload::from_static(b"new"))
            .await
            .unwrap();

        let retention = sequins_types::Duration::from_hours(24);
        let deleted = cold_tier
            .cleanup_old_files("spans", retention)
            .await
            .unwrap();

        assert_eq!(deleted, 1, "exactly the old file should be deleted");
        assert!(
            cold_tier.store.get(&old_path).await.is_err(),
            "old file should be deleted"
        );
        assert!(
            cold_tier.store.get(&new_path).await.is_ok(),
            "new file should remain"
        );
    }

    #[test]
    fn test_histogram_partition_path() {
        let timestamp = Timestamp::from_secs(1705328400);

        let path = ColdTier::generate_histogram_partition_path("test_histogram", &timestamp);

        // Should use histograms prefix instead of data
        assert!(path.starts_with("metrics/histograms/"));
        assert!(path.contains("2024-01-15"));
        assert!(path.ends_with(".vortex"));
    }
}
