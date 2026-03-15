//! Log writing to cold tier storage

use super::cold_tier::ColdTier;
use super::helpers;
use crate::error::{Error, Result};
use arrow::array::RecordBatch;
use sequins_types::models::Timestamp;

impl ColdTier {
    /// Write a batch of log entries to cold tier storage.
    pub async fn write_logs(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let batch = Self::sort_batch_by_column(batch, "time_unix_nano")?;

        let timestamp = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;
        let partition_path = helpers::generate_partition_path("logs", &timestamp);
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        // Build companion index and embed it in the Vortex file.
        use super::index::log_index::LogCompanionIndex;
        let companion_index = LogCompanionIndex::build_for_batch(&batch)
            .map_err(|e| Error::Storage(format!("Failed to build log companion index: {}", e)))?;
        let companion_bytes = companion_index
            .into_companion_bytes()
            .map_err(|e| Error::Storage(format!("Failed to serialize log index: {}", e)))?;

        let schema = batch.schema();
        self.write_record_batch(batch.clone(), schema, &full_path, Some(companion_bytes))
            .await?;

        Ok(partition_path)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::create_test_cold_tier;
    use arrow::array::{StringViewArray, TimestampNanosecondArray, UInt32Array, UInt8Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_test_log_batch(n: usize) -> RecordBatch {
        let base = 1_700_000_000_000_000_000i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("log_id", DataType::Utf8View, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "observed_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8View, false),
            Field::new("severity_text", DataType::Utf8View, false),
            Field::new("severity_number", DataType::UInt8, false),
            Field::new("body", DataType::Utf8View, false),
            Field::new("trace_id", DataType::Utf8View, true),
            Field::new("span_id", DataType::Utf8View, true),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
        ]));
        let times: Vec<i64> = (0..n).map(|i| base + i as i64 * 1_000_000_000).collect();
        let log_ids: Vec<String> = (0..n).map(|i| format!("log{:04}", i)).collect();
        let bodies: Vec<String> = (0..n).map(|i| format!("Test log message {}", i)).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(
                    log_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(TimestampNanosecondArray::from(times.clone())) as _,
                Arc::new(TimestampNanosecondArray::from(times)) as _,
                Arc::new(StringViewArray::from(vec!["unknown"; n])) as _,
                Arc::new(StringViewArray::from(vec!["INFO"; n])) as _,
                Arc::new(UInt8Array::from(vec![9u8; n])) as _,
                Arc::new(StringViewArray::from(
                    bodies.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
                Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
                Arc::new(UInt32Array::from(vec![1u32; n])) as _,
                Arc::new(UInt32Array::from(vec![1u32; n])) as _,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_logs_basic() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let batch = make_test_log_batch(3);
        let partition_path = cold_tier.write_logs(batch).await.unwrap();
        assert!(!partition_path.is_empty());
        assert!(partition_path.contains("logs"));
    }

    #[tokio::test]
    async fn test_write_logs_empty() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let schema = make_test_log_batch(0).schema();
        let batch = RecordBatch::new_empty(schema);
        let partition_path = cold_tier.write_logs(batch).await.unwrap();
        assert_eq!(partition_path, "");
    }
}
