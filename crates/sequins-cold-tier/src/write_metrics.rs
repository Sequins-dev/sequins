//! Metric writing with rollups and exemplars

use super::cold_tier::ColdTier;
use super::helpers;
use crate::error::{Error, Result};
use arrow::array::{ArrayRef, RecordBatch};
use sequins_types::arrow_schema;
use sequins_types::models::{
    ExponentialHistogramDataPoint, HistogramDataPoint, MetricDataPoint, Timestamp,
};
use std::sync::Arc;

impl ColdTier {
    pub async fn write_metrics(&self, batch: RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            "metrics/metadata",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        // Prepend base path since object store has no prefix
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        let schema = batch.schema();
        self.write_record_batch(batch, schema, &full_path, None)
            .await?;

        Ok(partition_path)
    }

    pub async fn write_metric_data_points(
        &self,
        data_points: Vec<MetricDataPoint>,
    ) -> Result<String> {
        if data_points.is_empty() {
            return Ok(String::new());
        }

        use super::series_index::SeriesId;
        use arrow::array::{Float64Array, StringViewArray, TimestampNanosecondArray, UInt64Array};
        use std::collections::BTreeMap;

        // Step 1: Group data points by metric_id and resolve series IDs
        let mut series_index = self.series_index.write().await;
        // (series_id, metric_id_hex, timestamp_ns, value)
        let mut series_data: Vec<(SeriesId, String, i64, f64)> =
            Vec::with_capacity(data_points.len());

        // Use metric_id as the metric name (derive from first data point if available)
        let metric_name = if let Some(first_dp) = data_points.first() {
            format!("metric_{}", first_dp.metric_id.to_hex())
        } else {
            return Ok(String::new());
        };

        for dp in data_points {
            // Convert attributes to BTreeMap for consistent ordering
            let attrs: BTreeMap<String, String> = dp.attributes.into_iter().collect();
            let series_id = series_index.register(&metric_name, attrs);
            series_data.push((
                series_id,
                dp.metric_id.to_hex(),
                dp.timestamp.as_nanos(),
                dp.value,
            ));
        }
        drop(series_index); // Release write lock

        // Step 2: Sort by series_id for optimal storage locality
        series_data.sort_by_key(|(series_id, _, _, _)| *series_id);

        // Step 3: Build arrays matching series_data_point_schema()
        let series_ids: Vec<u64> = series_data
            .iter()
            .map(|(id, _, _, _)| id.as_u64())
            .collect();
        let metric_id_hexes: Vec<&str> = series_data
            .iter()
            .map(|(_, mid, _, _)| mid.as_str())
            .collect();
        let timestamps: Vec<i64> = series_data.iter().map(|(_, _, ts, _)| *ts).collect();
        let values: Vec<f64> = series_data.iter().map(|(_, _, _, val)| *val).collect();

        let schema = arrow_schema::series_data_point_schema();

        use std::sync::Arc as StdArc;
        let arrays: Vec<ArrayRef> = vec![
            StdArc::new(UInt64Array::from(series_ids)) as ArrayRef,
            StdArc::new(StringViewArray::from(metric_id_hexes)) as ArrayRef,
            StdArc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
            StdArc::new(Float64Array::from(values)) as ArrayRef,
        ];

        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Error::Storage(format!("Failed to create metric batch: {}", e)))?;

        // Step 4: Generate 2-hour partition path
        let now = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;
        let partition_path = Self::generate_metric_partition_path(&metric_name, &now);

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        // Step 5: Write Vortex file
        self.write_record_batch(batch, schema, &full_path, None)
            .await?;

        // Step 6: Compute and write rollups for all three tiers
        use super::rollups::{
            compute_metric_rollups, extract_exemplars, write_exemplars, write_metric_rollups,
            RollupTier,
        };

        // Build (series_id, timestamp_ns, value) input for rollup computation
        let rollup_input: Vec<(SeriesId, i64, f64)> = series_data
            .iter()
            .map(|(series_id, _, ts, val)| (*series_id, *ts, *val))
            .collect();
        let (rollups_1m, rollups_1h, rollups_1d) = compute_metric_rollups(&rollup_input);

        // Write each tier in parallel (cheap since rollups are pre-computed)
        let timestamp = now.as_secs();
        let store = self.store.clone();
        let base = base_path.to_string();

        let row_block_size = self.config.row_block_size;
        let compact_encodings = self.config.compact_encodings;

        let write_1m = write_metric_rollups(
            rollups_1m,
            store.clone(),
            &base,
            RollupTier::OneMinute,
            timestamp,
            row_block_size,
            compact_encodings,
        );
        let write_1h = write_metric_rollups(
            rollups_1h,
            store.clone(),
            &base,
            RollupTier::OneHour,
            timestamp,
            row_block_size,
            compact_encodings,
        );
        let write_1d = write_metric_rollups(
            rollups_1d,
            store.clone(),
            &base,
            RollupTier::OneDay,
            timestamp,
            row_block_size,
            compact_encodings,
        );

        // Execute writes concurrently
        let (_, _, _) = tokio::try_join!(write_1m, write_1h, write_1d)?;

        // Step 6b: Extract and write exemplars (top-10% data points by value)
        let exemplar_input: Vec<(u64, i64, f64)> = series_data
            .iter()
            .map(|(series_id, _, ts, val)| (series_id.as_u64(), *ts, *val))
            .collect();
        let exemplars = extract_exemplars(&exemplar_input);
        write_exemplars(exemplars, store.clone(), &base, timestamp).await?;

        // Step 7: Persist updated series index
        let series_index = self.series_index.read().await;
        series_index.persist(self.store.clone(), base_path).await?;

        Ok(partition_path)
    }

    pub async fn write_histogram_data_points(
        &self,
        data_points: Vec<HistogramDataPoint>,
    ) -> Result<String> {
        if data_points.is_empty() {
            return Ok(String::new());
        }

        use super::series_index::SeriesId;
        use arrow::array::{
            Float64Array, ListArray, StringViewArray, TimestampNanosecondArray, UInt64Array,
        };
        use arrow::datatypes::{DataType, Field};
        use std::collections::BTreeMap;

        // Step 1: Resolve/create series IDs for all data points
        let mut series_index = self.series_index.write().await;
        let mut series_histograms: Vec<(SeriesId, HistogramDataPoint)> =
            Vec::with_capacity(data_points.len());

        // Use metric_id as the metric name (derive from first data point if available)
        let metric_name = if let Some(first_dp) = data_points.first() {
            format!("metric_{}", first_dp.metric_id.to_hex())
        } else {
            return Ok(String::new());
        };

        for dp in data_points {
            // Convert attributes to BTreeMap for consistent ordering
            let attrs: BTreeMap<String, String> = dp.attributes.clone().into_iter().collect();
            let series_id = series_index.register(&metric_name, attrs);
            series_histograms.push((series_id, dp));
        }
        drop(series_index); // Release write lock

        // Step 2: Sort by series_id for optimal storage locality
        series_histograms.sort_by_key(|(series_id, _)| *series_id);

        // Step 3: Build arrays matching histogram_series_data_point_schema()
        let series_ids: Vec<u64> = series_histograms
            .iter()
            .map(|(id, _)| id.as_u64())
            .collect();
        let metric_id_hexes: Vec<String> = series_histograms
            .iter()
            .map(|(_, dp)| dp.metric_id.to_hex())
            .collect();
        let timestamps: Vec<i64> = series_histograms
            .iter()
            .map(|(_, dp)| dp.timestamp.as_nanos())
            .collect();
        let counts: Vec<u64> = series_histograms.iter().map(|(_, dp)| dp.count).collect();
        let sums: Vec<f64> = series_histograms.iter().map(|(_, dp)| dp.sum).collect();

        use arrow::buffer::OffsetBuffer;

        let mut bucket_counts_values = Vec::new();
        let mut bucket_counts_offsets = vec![0i32];
        let mut bounds_values: Vec<f64> = Vec::new();
        let mut bounds_offsets = vec![0i32];

        for (_, dp) in &series_histograms {
            bucket_counts_values.extend_from_slice(&dp.bucket_counts);
            bucket_counts_offsets
                .push(bucket_counts_offsets.last().unwrap() + dp.bucket_counts.len() as i32);
            bounds_values.extend_from_slice(&dp.explicit_bounds);
            bounds_offsets.push(bounds_offsets.last().unwrap() + dp.explicit_bounds.len() as i32);
        }

        let bucket_counts_array = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            OffsetBuffer::new(bucket_counts_offsets.into()),
            Arc::new(UInt64Array::from(bucket_counts_values)),
            None,
        );

        let explicit_bounds_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Float64, false)),
            OffsetBuffer::new(bounds_offsets.into()),
            Arc::new(Float64Array::from(bounds_values)),
            None,
        );

        let schema = arrow_schema::histogram_series_data_point_schema();

        use std::sync::Arc as StdArc;
        let arrays: Vec<ArrayRef> = vec![
            StdArc::new(UInt64Array::from(series_ids)) as ArrayRef,
            StdArc::new(StringViewArray::from(metric_id_hexes)) as ArrayRef,
            StdArc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
            StdArc::new(UInt64Array::from(counts)) as ArrayRef,
            StdArc::new(Float64Array::from(sums)) as ArrayRef,
            StdArc::new(bucket_counts_array) as ArrayRef,
            StdArc::new(explicit_bounds_array) as ArrayRef,
        ];

        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Error::Storage(format!("Failed to create histogram batch: {}", e)))?;

        // Step 4: Generate 2-hour partition path
        let now = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;
        let partition_path = Self::generate_histogram_partition_path(&metric_name, &now);

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        // Step 5: Write Vortex file
        self.write_record_batch(batch, schema, &full_path, None)
            .await?;

        // Step 6: Persist updated series index
        let series_index = self.series_index.read().await;
        series_index.persist(self.store.clone(), base_path).await?;

        Ok(partition_path)
    }

    pub async fn write_exponential_histogram_data_points(
        &self,
        data_points: Vec<ExponentialHistogramDataPoint>,
    ) -> Result<String> {
        if data_points.is_empty() {
            return Ok(String::new());
        }

        let schema = arrow_schema::exp_histogram_data_point_schema();
        let batch = Self::exp_histogram_data_points_to_record_batch(data_points, schema.clone())?;

        let now = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;
        let partition_path = helpers::generate_partition_path("metrics/exp_histograms", &now);

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch, schema, &full_path, None)
            .await?;

        Ok(partition_path)
    }

    pub(crate) fn exp_histogram_data_points_to_record_batch(
        data_points: Vec<ExponentialHistogramDataPoint>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<RecordBatch> {
        use arrow::array::{
            Float64Array, Int32Array, ListArray, StringViewArray, TimestampNanosecondArray,
            UInt64Array,
        };
        use arrow::datatypes::{DataType, Field};

        let num_rows = data_points.len();

        if num_rows == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }

        // Matching exp_histogram_data_point_schema():
        // series_id, metric_id, timestamp_ns, count, sum, scale, zero_count,
        // positive_offset, positive_counts, negative_offset, negative_counts
        let mut series_ids: Vec<u64> = Vec::with_capacity(num_rows);
        let mut metric_ids = Vec::with_capacity(num_rows);
        let mut timestamps = Vec::with_capacity(num_rows);
        let mut counts = Vec::with_capacity(num_rows);
        let mut sums = Vec::with_capacity(num_rows);
        let mut scales: Vec<i32> = Vec::with_capacity(num_rows);
        let mut zero_counts: Vec<u64> = Vec::with_capacity(num_rows);
        let mut positive_offsets: Vec<i32> = Vec::with_capacity(num_rows);
        let mut negative_offsets: Vec<i32> = Vec::with_capacity(num_rows);

        let mut all_positive_counts: Vec<u64> = Vec::new();
        let mut positive_offsets_list = vec![0i32];
        let mut all_negative_counts: Vec<u64> = Vec::new();
        let mut negative_offsets_list = vec![0i32];

        for dp in data_points {
            series_ids.push(0u64);
            metric_ids.push(dp.metric_id.to_hex());
            timestamps.push(dp.timestamp.as_nanos());
            counts.push(dp.count);
            sums.push(dp.sum);
            scales.push(dp.scale);
            zero_counts.push(dp.zero_count);
            positive_offsets.push(dp.positive_offset);
            negative_offsets.push(dp.negative_offset);

            all_positive_counts.extend_from_slice(&dp.positive_counts);
            positive_offsets_list.push(all_positive_counts.len() as i32);

            all_negative_counts.extend_from_slice(&dp.negative_counts);
            negative_offsets_list.push(all_negative_counts.len() as i32);
        }

        let positive_counts_list = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            arrow::buffer::OffsetBuffer::new(positive_offsets_list.into()),
            Arc::new(UInt64Array::from(all_positive_counts)),
            None,
        );

        let negative_counts_list = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            arrow::buffer::OffsetBuffer::new(negative_offsets_list.into()),
            Arc::new(UInt64Array::from(all_negative_counts)),
            None,
        );

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(series_ids)) as ArrayRef,
            Arc::new(StringViewArray::from(metric_ids)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
            Arc::new(UInt64Array::from(counts)) as ArrayRef,
            Arc::new(Float64Array::from(sums)) as ArrayRef,
            Arc::new(Int32Array::from(scales)) as ArrayRef,
            Arc::new(UInt64Array::from(zero_counts)) as ArrayRef,
            Arc::new(Int32Array::from(positive_offsets)) as ArrayRef,
            Arc::new(positive_counts_list) as ArrayRef,
            Arc::new(Int32Array::from(negative_offsets)) as ArrayRef,
            Arc::new(negative_counts_list) as ArrayRef,
        ];

        RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::create_test_cold_tier;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sequins_types::models::{HistogramDataPoint, MetricDataPoint, MetricId, Timestamp};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_metric_batch(count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("metric_id", DataType::Utf8View, false),
            Field::new("name", DataType::Utf8View, false),
            Field::new("description", DataType::Utf8View, false),
            Field::new("unit", DataType::Utf8View, false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("aggregation_temporality", DataType::UInt8, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
        ]));
        use arrow::array::{StringViewArray, UInt32Array, UInt8Array};
        let ids: Vec<String> = (0..count).map(|i| format!("metric{:04}", i)).collect();
        let names: Vec<String> = (0..count).map(|i| format!("test.metric.{}", i)).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(
                    ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringViewArray::from(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringViewArray::from(vec!["desc"; count])) as _,
                Arc::new(StringViewArray::from(vec!["ms"; count])) as _,
                Arc::new(UInt8Array::from(vec![0u8; count])) as _,
                Arc::new(UInt8Array::from(vec![0u8; count])) as _,
                Arc::new(UInt32Array::from(vec![1u32; count])) as _,
                Arc::new(UInt32Array::from(vec![1u32; count])) as _,
            ],
        )
        .unwrap()
    }

    fn create_test_metric_data_points(count: usize) -> Vec<MetricDataPoint> {
        let mut data_points = Vec::new();
        let base_time_ns = 1_700_000_000_000_000_000i64;
        let metric_id = MetricId::new();

        for i in 0..count {
            let mut attributes = HashMap::new();
            attributes.insert("instance".to_string(), format!("instance-{}", i));

            data_points.push(MetricDataPoint {
                metric_id,
                timestamp: Timestamp::from_nanos(base_time_ns + (i as i64 * 60_000_000_000)),
                start_time: None,
                value: 42.0 + i as f64,
                attributes,
                resource_id: 1,
            });
        }

        data_points
    }

    fn create_test_histogram_data_points(count: usize) -> Vec<HistogramDataPoint> {
        let mut data_points = Vec::new();
        let base_time_ns = 1_700_000_000_000_000_000i64;
        let metric_id = MetricId::new();

        for i in 0..count {
            let mut attributes = HashMap::new();
            attributes.insert("endpoint".to_string(), format!("/api/v{}", i));

            data_points.push(HistogramDataPoint {
                metric_id,
                timestamp: Timestamp::from_nanos(base_time_ns + (i as i64 * 60_000_000_000)),
                start_time: None,
                count: 100 + i as u64,
                sum: 1000.0 + (i as f64 * 100.0),
                min: Some(10.0),
                max: Some(500.0),
                bucket_counts: vec![10, 20, 30, 40],
                explicit_bounds: vec![0.1, 0.5, 1.0, 5.0],
                exemplars: Vec::new(),
                attributes,
                resource_id: 1,
            });
        }

        data_points
    }

    #[tokio::test]
    async fn test_write_metrics_basic() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let batch = create_test_metric_batch(3);

        let partition_path = cold_tier.write_metrics(batch).await.unwrap();

        assert!(!partition_path.is_empty());
        assert!(partition_path.contains("metrics/metadata"));
    }

    #[tokio::test]
    async fn test_write_metrics_empty() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let schema = create_test_metric_batch(0).schema();
        let batch = RecordBatch::new_empty(schema);

        let partition_path = cold_tier.write_metrics(batch).await.unwrap();

        assert_eq!(partition_path, "");
    }

    #[tokio::test]
    async fn test_write_metric_data_points_basic() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let data_points = create_test_metric_data_points(5);

        let partition_path = cold_tier
            .write_metric_data_points(data_points)
            .await
            .unwrap();

        assert!(!partition_path.is_empty());
        assert!(partition_path.contains("metrics/data"));
    }

    #[tokio::test]
    async fn test_write_metric_data_points_with_attributes() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let base_time_ns = 1_700_000_000_000_000_000i64;
        let metric_id = MetricId::new();

        // Create data points with various attributes
        let mut data_points = Vec::new();
        for i in 0..3 {
            let mut attributes = HashMap::new();
            attributes.insert("host".to_string(), format!("host-{}", i));
            attributes.insert("region".to_string(), format!("us-east-{}", i));
            attributes.insert("env".to_string(), "production".to_string());

            data_points.push(MetricDataPoint {
                metric_id,
                timestamp: Timestamp::from_nanos(base_time_ns + (i as i64 * 60_000_000_000)),
                start_time: None,
                value: 100.0 + i as f64,
                attributes,
                resource_id: 1,
            });
        }

        let partition_path = cold_tier
            .write_metric_data_points(data_points)
            .await
            .unwrap();

        assert!(!partition_path.is_empty());
    }

    #[tokio::test]
    async fn test_write_histogram_data_points_basic() {
        let (cold_tier, _temp) = create_test_cold_tier().await;
        let data_points = create_test_histogram_data_points(3);

        let partition_path = cold_tier
            .write_histogram_data_points(data_points)
            .await
            .unwrap();

        assert!(!partition_path.is_empty());
        assert!(partition_path.contains("metrics/histograms"));
    }
}
