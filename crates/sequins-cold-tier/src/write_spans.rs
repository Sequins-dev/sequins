//! Span writing to cold tier storage

use super::cold_tier::ColdTier;
use super::helpers;
use crate::error::{Error, Result};
use arrow::array::RecordBatch;
use sequins_types::models::Timestamp;

impl ColdTier {
    /// Write a batch of spans to cold tier storage.
    pub async fn write_spans(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        // Sort by start_time_unix_nano for zone map effectiveness
        let batch = Self::sort_batch_by_column(batch, "start_time_unix_nano")?;

        let timestamp = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;

        let partition_path = helpers::generate_partition_path("spans", &timestamp);
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        // Build companion index and embed it in the Vortex file (if enabled).
        let companion_bytes = if self.config.companion_index.tantivy_enabled
            || self.config.companion_index.bloom_enabled
        {
            use super::index::span_index::SpanCompanionIndex;
            let companion_index = SpanCompanionIndex::build_for_batch(
                &batch,
                self.config.companion_index.cardinality_threshold,
            )
            .map_err(|e| Error::Storage(format!("Failed to build companion index: {}", e)))?;
            let bytes = companion_index.into_companion_bytes().map_err(|e| {
                Error::Storage(format!("Failed to serialize companion index: {}", e))
            })?;
            Some(bytes)
        } else {
            None
        };

        let schema = batch.schema();
        self.write_record_batch(batch.clone(), schema, &full_path, companion_bytes)
            .await?;

        // Compute and write span rollups
        let rollups = super::rollups::compute_span_rollups(&batch);
        if !rollups.is_empty() {
            let rpath = super::rollups::write_span_rollups(
                rollups.clone(),
                self.store.clone(),
                base_path,
                timestamp.as_secs(),
                self.config.row_block_size,
                self.config.compact_encodings,
            )
            .await?;
            tracing::debug!("Wrote {} span rollups to {}", rollups.len(), rpath);
        }

        Ok(partition_path)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::create_test_cold_tier;
    use arrow::array::{
        Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array, UInt8Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_test_span_batch(n: usize) -> RecordBatch {
        let base = 1_700_000_000_000_000_000i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8View, false),
            Field::new("span_id", DataType::Utf8View, false),
            Field::new("parent_span_id", DataType::Utf8View, true),
            Field::new("name", DataType::Utf8View, false),
            Field::new("kind", DataType::UInt8, false),
            Field::new("status", DataType::UInt8, false),
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "end_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("duration_ns", DataType::Int64, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
        ]));
        let start_times: Vec<i64> = (0..n).map(|i| base + i as i64 * 1_000_000_000).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec!["00000000"; n])) as _,
                Arc::new(StringViewArray::from(vec!["00000001"; n])) as _,
                Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
                Arc::new(StringViewArray::from(vec!["op"; n])) as _,
                Arc::new(UInt8Array::from(vec![0u8; n])) as _,
                Arc::new(UInt8Array::from(vec![1u8; n])) as _, // Ok
                Arc::new(TimestampNanosecondArray::from(start_times.clone())) as _,
                Arc::new(TimestampNanosecondArray::from(
                    start_times
                        .iter()
                        .map(|t| t + 100_000_000)
                        .collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int64Array::from(vec![100_000_000i64; n])) as _,
                Arc::new(UInt32Array::from(vec![1u32; n])) as _,
                Arc::new(UInt32Array::from(vec![1u32; n])) as _,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_spans_basic() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let batch = make_test_span_batch(3);
        let partition_path = cold_tier.write_spans(batch).await.unwrap();
        assert!(!partition_path.is_empty());
        assert!(partition_path.contains("spans"));
    }

    #[tokio::test]
    async fn test_write_spans_empty() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let schema = make_test_span_batch(0).schema();
        let batch = RecordBatch::new_empty(schema);
        let partition_path = cold_tier.write_spans(batch).await.unwrap();
        assert_eq!(partition_path, "");
    }
}
