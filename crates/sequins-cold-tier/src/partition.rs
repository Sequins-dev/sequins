//! Partition path generation and cleanup utilities

use super::cold_tier::ColdTier;
use crate::error::{Error, Result};
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

    pub async fn cleanup_old_files(
        &self,
        telemetry_type: &str,
        retention_period: sequins_types::Duration,
    ) -> Result<usize> {
        use object_store::path::Path as ObjectPath;

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);

        // Calculate cutoff timestamp
        let now = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;
        let cutoff = now - retention_period;
        let cutoff_nanos = cutoff.as_nanos();

        // List all files in the telemetry type directory
        let type_path = format!("{}/{}", base_path, telemetry_type);

        // Check if directory exists
        if !std::path::Path::new(&type_path).exists() {
            return Ok(0);
        }

        let mut deleted_count = 0;

        // Walk through year/month/day directories and find old files
        if let Ok(entries) = std::fs::read_dir(&type_path) {
            for year_entry in entries.flatten() {
                if let Ok(year_meta) = year_entry.metadata() {
                    if !year_meta.is_dir() {
                        continue;
                    }

                    // Read month directories
                    if let Ok(month_entries) = std::fs::read_dir(year_entry.path()) {
                        for month_entry in month_entries.flatten() {
                            if let Ok(month_meta) = month_entry.metadata() {
                                if !month_meta.is_dir() {
                                    continue;
                                }

                                // Read day directories
                                if let Ok(day_entries) = std::fs::read_dir(month_entry.path()) {
                                    for day_entry in day_entries.flatten() {
                                        if let Ok(day_meta) = day_entry.metadata() {
                                            if !day_meta.is_dir() {
                                                continue;
                                            }

                                            // Read parquet files in day directory
                                            if let Ok(file_entries) =
                                                std::fs::read_dir(day_entry.path())
                                            {
                                                for file_entry in file_entries.flatten() {
                                                    if let Some(filename) =
                                                        file_entry.file_name().to_str()
                                                    {
                                                        if filename.ends_with(".vortex") {
                                                            // Extract timestamp from filename
                                                            // Format: {timestamp}.vortex
                                                            if let Some(timestamp_str) =
                                                                filename.strip_suffix(".vortex")
                                                            {
                                                                if let Ok(file_timestamp) =
                                                                    timestamp_str.parse::<i64>()
                                                                {
                                                                    // Delete if older than retention period
                                                                    if file_timestamp < cutoff_nanos
                                                                    {
                                                                        // Delete the file using object store
                                                                        let file_path =
                                                                            file_entry.path();
                                                                        let relative_path = file_path
                                                                            .strip_prefix(base_path)
                                                                            .map_err(|e| {
                                                                                Error::Storage(
                                                                                    format!(
                                                                                "Failed to strip prefix: {}",
                                                                                e
                                                                            ),
                                                                                )
                                                                            })?
                                                                            .to_str()
                                                                            .ok_or_else(|| {
                                                                                Error::Storage(
                                                                                    "Invalid path"
                                                                                        .to_string(),
                                                                                )
                                                                            })?;

                                                                        let object_path =
                                                                            ObjectPath::from(
                                                                                relative_path,
                                                                            );
                                                                        self.store
                                                                            .delete(&object_path)
                                                                            .await
                                                                            .map_err(|e| {
                                                                                Error::Storage(
                                                                                    format!(
                                                                                    "Failed to delete file: {}",
                                                                                    e
                                                                                ),
                                                                                )
                                                                            })?;

                                                                        deleted_count += 1;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
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
