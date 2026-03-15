//! Miscellaneous write methods (resources, scopes, span links, span events)

use super::cold_tier::ColdTier;
use super::helpers;
use crate::error::{Error, Result};
use arrow::array::RecordBatch;
use sequins_types::models::Timestamp;

impl ColdTier {
    pub async fn write_scopes(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "scopes",
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

    pub async fn write_span_links(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "spans/links",
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

    pub async fn write_span_events(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "spans/events",
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

    pub async fn write_resources(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "resources",
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
}
