//! Profile writing to cold tier storage

use super::cold_tier::ColdTier;
use super::helpers;
use crate::error::{Error, Result};
use arrow::array::{Array, RecordBatch};
use sequins_types::models::Timestamp;

impl ColdTier {
    pub async fn write_profile_frames(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "profiles/frames",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch.clone(), batch.schema(), &full_path, None)
            .await?;

        Ok(partition_path)
    }

    pub async fn write_profile_stacks(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "profiles/stacks",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch.clone(), batch.schema(), &full_path, None)
            .await?;

        Ok(partition_path)
    }

    pub async fn write_profile_mappings(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "profiles/mappings",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch.clone(), batch.schema(), &full_path, None)
            .await?;

        Ok(partition_path)
    }

    pub async fn write_profile_samples(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "profiles/samples",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch.clone(), batch.schema(), &full_path, None)
            .await?;

        Ok(partition_path)
    }

    /// Write a batch of profiles to cold tier storage.
    ///
    /// The pprof parsing is now handled upstream (in sequins-otlp).
    /// This method writes the pre-converted RecordBatch directly.
    pub async fn write_profiles(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let timestamp = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;

        let partition_path = helpers::generate_partition_path("profiles", &timestamp);

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch.clone(), batch.schema(), &full_path, None)
            .await?;

        // Compute and write profile rollups if applicable
        use super::rollups::{compute_profile_rollups, write_profile_rollups};

        // Build rollup samples from batch: (service, stack_hash, timestamp_ns, value_type, value)
        // We look for service, time_unix_nano, stack_hash, value_type, value columns
        let rollup_samples = Self::extract_profile_rollup_samples(&batch);

        if !rollup_samples.is_empty() {
            let rollups = compute_profile_rollups(&rollup_samples);
            if !rollups.is_empty() {
                write_profile_rollups(
                    rollups,
                    self.store.clone(),
                    base_path,
                    timestamp.as_secs(),
                    self.config.row_block_size,
                    self.config.compact_encodings,
                )
                .await?;
            }
        }

        Ok(partition_path)
    }

    /// Extract profile rollup sample tuples from a RecordBatch.
    ///
    /// Returns Vec<(service, stack_hash, timestamp_ns, value_type, value)>
    fn extract_profile_rollup_samples(batch: &RecordBatch) -> Vec<(String, u64, i64, String, i64)> {
        use arrow::array::{Int64Array, StringViewArray, UInt64Array};

        let service_col = batch
            .column_by_name("service")
            .and_then(|c| c.as_any().downcast_ref::<StringViewArray>());
        let time_col = batch
            .column_by_name("time_unix_nano")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let stack_hash_col = batch
            .column_by_name("stack_hash")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let value_type_col = batch
            .column_by_name("value_type")
            .and_then(|c| c.as_any().downcast_ref::<StringViewArray>());
        let value_col = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

        if service_col.is_none()
            || time_col.is_none()
            || stack_hash_col.is_none()
            || value_type_col.is_none()
            || value_col.is_none()
        {
            return Vec::new();
        }

        let service_col = service_col.unwrap();
        let time_col = time_col.unwrap();
        let stack_hash_col = stack_hash_col.unwrap();
        let value_type_col = value_type_col.unwrap();
        let value_col = value_col.unwrap();

        (0..batch.num_rows())
            .map(|i| {
                let service = if service_col.is_null(i) {
                    "unknown".to_string()
                } else {
                    service_col.value(i).to_string()
                };
                let timestamp_ns = time_col.value(i);
                let stack_hash = stack_hash_col.value(i);
                let value_type = if value_type_col.is_null(i) {
                    "cpu".to_string()
                } else {
                    value_type_col.value(i).to_string()
                };
                let value = value_col.value(i);
                (service, stack_hash, timestamp_ns, value_type, value)
            })
            .collect()
    }
}
