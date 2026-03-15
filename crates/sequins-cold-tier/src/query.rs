//! Query methods for cold tier storage

use super::cold_tier::ColdTier;
use crate::error::{Error, Result};
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use sequins_types::models::{HistogramDataPoint, MetricDataPoint, Timestamp};
use std::sync::Arc;

impl ColdTier {
    pub async fn query_metric_data_points(
        &self,
        metric_name: Option<&str>,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<MetricDataPoint>> {
        use arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        };
        use futures::TryStreamExt;
        use object_store::path::Path as ObjPath;
        use vortex::file::OpenOptionsSessionExt;
        use vortex::session::VortexSession;
        use vortex::VortexSessionDefault;

        // --- Series filtering via the index ---
        let filter_ids: Option<std::collections::HashSet<u64>> = if let Some(name) = metric_name {
            let index = self.series_index.read().await;
            let ids: std::collections::HashSet<u64> = index
                .resolve_matchers(name, &[])
                .into_iter()
                .map(|sid| sid.as_u64())
                .collect();

            if ids.is_empty() {
                // No series registered for this metric name — nothing to scan
                return Ok(Vec::new());
            }
            Some(ids)
        } else {
            None
        };

        // Build the object-store prefix for metric data files.
        // The write path stores files at: {base_path}/metrics/data/YYYY-MM-DD/HH/ts.vortex
        // where base_path is the URI with "file://" stripped.
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let prefix_str = format!("{}/metrics/data", base_path.trim_end_matches('/'));
        let obj_prefix = ObjPath::from(prefix_str.as_str());

        // Enumerate all .vortex files under the prefix
        let all_objects: Vec<_> = self
            .store
            .list(Some(&obj_prefix))
            .try_collect()
            .await
            .unwrap_or_default();

        let vortex_objects: Vec<_> = all_objects
            .into_iter()
            .filter(|o| o.location.as_ref().ends_with(".vortex"))
            .collect();

        if vortex_objects.is_empty() {
            return Ok(Vec::new());
        }

        // Build the Arrow schema that matches what write_metric_data_points stores.
        let metric_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("series_id", ArrowDataType::UInt64, false),
            ArrowField::new("time_unix_nano", ArrowDataType::Int64, false),
            ArrowField::new("value", ArrowDataType::Float64, false),
        ]));

        let start_ns = start_time.as_nanos();
        let end_ns = end_time.as_nanos();

        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for obj in &vortex_objects {
            let path_str = obj.location.as_ref();
            let vortex_session = VortexSession::default();
            let vxf = match vortex_session
                .open_options()
                .open_object_store(&self.store, path_str)
                .await
            {
                Ok(f) => f,
                Err(_) => continue, // skip unreadable files
            };

            let scan_builder = match vxf.scan() {
                Ok(sb) => sb,
                Err(_) => continue,
            };

            let stream = match scan_builder.into_record_batch_stream(metric_schema.clone()) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut pinned = Box::pin(stream);

            while let Some(batch_result) = futures::StreamExt::next(&mut pinned).await {
                match batch_result {
                    Ok(batch) => all_batches.push(batch),
                    Err(_) => continue,
                }
            }
        }

        // Filter rows in-memory by time range and optional series ID set
        let filtered_batches =
            Self::filter_metric_batches(all_batches, start_ns, end_ns, filter_ids.as_ref());

        // Convert RecordBatches to MetricDataPoints, enriching with series metadata
        let index = self.series_index.read().await;
        Self::record_batches_to_metric_data_points_v2(filtered_batches, &index)
    }

    fn filter_metric_batches(
        batches: Vec<RecordBatch>,
        start_ns: i64,
        end_ns: i64,
        filter_ids: Option<&std::collections::HashSet<u64>>,
    ) -> Vec<RecordBatch> {
        use arrow::array::{Int64Array, UInt64Array};
        use arrow::compute::filter as arrow_filter;

        let mut result = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Expect columns at fixed positions: 0=series_id, 1=time_unix_nano, 2=value
            let series_col = batch.column(0).as_any().downcast_ref::<UInt64Array>();
            let ts_col = batch.column(1).as_any().downcast_ref::<Int64Array>();

            let (Some(series_arr), Some(ts_arr)) = (series_col, ts_col) else {
                continue;
            };

            // Build a boolean mask
            let mut mask = arrow::array::BooleanBuilder::new();
            for row in 0..batch.num_rows() {
                let ts = ts_arr.value(row);
                let sid = series_arr.value(row);
                let in_range = ts >= start_ns && ts <= end_ns;
                let in_series = filter_ids.map(|ids| ids.contains(&sid)).unwrap_or(true);
                mask.append_value(in_range && in_series);
            }
            let mask_array = mask.finish();

            // Apply the filter to each column
            let filtered_columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| arrow_filter(col.as_ref(), &mask_array).unwrap())
                .collect();

            if let Ok(filtered_batch) = RecordBatch::try_new(batch.schema(), filtered_columns) {
                if filtered_batch.num_rows() > 0 {
                    result.push(filtered_batch);
                }
            }
        }

        result
    }

    pub async fn query_histogram_data_points(
        &self,
        metric_name: Option<&str>,
        start_time: Timestamp,
        end_time: Timestamp,
    ) -> Result<Vec<HistogramDataPoint>> {
        use arrow::datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
        };
        use futures::TryStreamExt;
        use object_store::path::Path as ObjPath;
        use vortex::file::OpenOptionsSessionExt;
        use vortex::session::VortexSession;
        use vortex::VortexSessionDefault;

        // --- Series filtering via the index ---
        let filter_ids: Option<std::collections::HashSet<u64>> = if let Some(name) = metric_name {
            let index = self.series_index.read().await;
            let ids: std::collections::HashSet<u64> = index
                .resolve_matchers(name, &[])
                .into_iter()
                .map(|sid| sid.as_u64())
                .collect();

            if ids.is_empty() {
                return Ok(Vec::new());
            }
            Some(ids)
        } else {
            None
        };

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let prefix_str = format!("{}/metrics/histograms", base_path.trim_end_matches('/'));
        let obj_prefix = ObjPath::from(prefix_str.as_str());

        let all_objects: Vec<_> = self
            .store
            .list(Some(&obj_prefix))
            .try_collect()
            .await
            .unwrap_or_default();

        let vortex_objects: Vec<_> = all_objects
            .into_iter()
            .filter(|o| o.location.as_ref().ends_with(".vortex"))
            .collect();

        if vortex_objects.is_empty() {
            return Ok(Vec::new());
        }

        let histogram_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("series_id", ArrowDataType::UInt64, false),
            ArrowField::new("time_unix_nano", ArrowDataType::Int64, false),
            ArrowField::new("count", ArrowDataType::UInt64, false),
            ArrowField::new("sum", ArrowDataType::Float64, false),
            ArrowField::new(
                "bucket_counts",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::UInt64,
                    false,
                ))),
                false,
            ),
            ArrowField::new(
                "explicit_bounds",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::Float64,
                    false,
                ))),
                false,
            ),
        ]));

        let start_ns = start_time.as_nanos();
        let end_ns = end_time.as_nanos();

        let mut all_batches: Vec<RecordBatch> = Vec::new();

        for obj in &vortex_objects {
            let path_str = obj.location.as_ref();
            let vortex_session = VortexSession::default();
            let vxf = match vortex_session
                .open_options()
                .open_object_store(&self.store, path_str)
                .await
            {
                Ok(f) => f,
                Err(_) => continue,
            };

            let scan_builder = match vxf.scan() {
                Ok(sb) => sb,
                Err(_) => continue,
            };

            let stream = match scan_builder.into_record_batch_stream(histogram_schema.clone()) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut pinned = Box::pin(stream);

            while let Some(batch_result) = futures::StreamExt::next(&mut pinned).await {
                match batch_result {
                    Ok(batch) => all_batches.push(batch),
                    Err(_) => continue,
                }
            }
        }

        // Filter rows by time range and optional series-id set
        let filtered_batches =
            Self::filter_histogram_batches(all_batches, start_ns, end_ns, filter_ids.as_ref());

        let index = self.series_index.read().await;
        Self::record_batches_to_histogram_data_points_v2(filtered_batches, &index)
    }

    fn filter_histogram_batches(
        batches: Vec<RecordBatch>,
        start_ns: i64,
        end_ns: i64,
        filter_ids: Option<&std::collections::HashSet<u64>>,
    ) -> Vec<RecordBatch> {
        use arrow::array::{Int64Array, UInt64Array};
        use arrow::compute::filter as arrow_filter;

        let mut result = Vec::new();

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let series_col = batch.column(0).as_any().downcast_ref::<UInt64Array>();
            let ts_col = batch.column(1).as_any().downcast_ref::<Int64Array>();

            let (Some(series_arr), Some(ts_arr)) = (series_col, ts_col) else {
                continue;
            };

            let mut mask = arrow::array::BooleanBuilder::new();
            for row in 0..batch.num_rows() {
                let ts = ts_arr.value(row);
                let sid = series_arr.value(row);
                let in_range = ts >= start_ns && ts <= end_ns;
                let in_series = filter_ids.map(|ids| ids.contains(&sid)).unwrap_or(true);
                mask.append_value(in_range && in_series);
            }
            let mask_array = mask.finish();

            let filtered_columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| arrow_filter(col.as_ref(), &mask_array).unwrap())
                .collect();

            if let Ok(filtered_batch) = RecordBatch::try_new(batch.schema(), filtered_columns) {
                if filtered_batch.num_rows() > 0 {
                    result.push(filtered_batch);
                }
            }
        }

        result
    }

    fn record_batches_to_metric_data_points_v2(
        batches: Vec<RecordBatch>,
        series_index: &super::series_index::SeriesIndex,
    ) -> Result<Vec<MetricDataPoint>> {
        use super::series_index::SeriesId;
        use arrow::array::{Float64Array, Int64Array, UInt64Array};
        use sequins_types::models::MetricId;
        use std::collections::HashMap;

        let mut data_points = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            let series_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| Error::Storage("Invalid series_id column".to_string()))?;

            let timestamps = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::Storage("Invalid time_unix_nano column".to_string()))?;

            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Storage("Invalid value column".to_string()))?;

            for row_idx in 0..num_rows {
                let series_id_u64 = series_ids.value(row_idx);
                let series_id = SeriesId::new(series_id_u64);

                let (metric_id, attributes) = if let Some(metadata) = series_index.lookup(series_id)
                {
                    let mid = MetricId::from_name_and_service(&metadata.metric_name, "");
                    let attrs: HashMap<String, String> =
                        metadata.attributes.clone().into_iter().collect();
                    (mid, attrs)
                } else {
                    let mid =
                        MetricId::from_name_and_service(&format!("series_{}", series_id_u64), "");
                    (mid, HashMap::new())
                };

                let timestamp = Timestamp::from_nanos(timestamps.value(row_idx));
                let value = values.value(row_idx);

                data_points.push(MetricDataPoint {
                    metric_id,
                    timestamp,
                    start_time: None,
                    value,
                    attributes,
                    resource_id: 0,
                });
            }
        }

        Ok(data_points)
    }

    fn record_batches_to_histogram_data_points_v2(
        batches: Vec<RecordBatch>,
        series_index: &super::series_index::SeriesIndex,
    ) -> Result<Vec<HistogramDataPoint>> {
        use super::series_index::SeriesId;
        use arrow::array::{Float64Array, Int64Array, ListArray, UInt64Array};
        use sequins_types::models::{Exemplar, MetricId};
        use std::collections::HashMap;

        let mut data_points = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            let series_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| Error::Storage("Invalid series_id column".to_string()))?;

            let timestamps = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::Storage("Invalid time_unix_nano column".to_string()))?;

            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| Error::Storage("Invalid count column".to_string()))?;

            let sums = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Error::Storage("Invalid sum column".to_string()))?;

            let bucket_counts_array = batch
                .column(4)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::Storage("Invalid bucket_counts column".to_string()))?;

            let explicit_bounds_array = batch
                .column(5)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::Storage("Invalid explicit_bounds column".to_string()))?;

            for row_idx in 0..num_rows {
                let series_id_u64 = series_ids.value(row_idx);
                let series_id = SeriesId::new(series_id_u64);

                let (metric_id, attributes) = if let Some(metadata) = series_index.lookup(series_id)
                {
                    let mid = MetricId::from_name_and_service(&metadata.metric_name, "");
                    let attrs: HashMap<String, String> =
                        metadata.attributes.clone().into_iter().collect();
                    (mid, attrs)
                } else {
                    let mid =
                        MetricId::from_name_and_service(&format!("series_{}", series_id_u64), "");
                    (mid, HashMap::new())
                };

                let timestamp = Timestamp::from_nanos(timestamps.value(row_idx));
                let count = counts.value(row_idx);
                let sum = sums.value(row_idx);

                let bucket_counts_list = bucket_counts_array.value(row_idx);
                let bucket_counts_values = bucket_counts_list
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| Error::Storage("Invalid bucket_counts values".to_string()))?;
                let bucket_counts: Vec<u64> = (0..bucket_counts_values.len())
                    .map(|i| bucket_counts_values.value(i))
                    .collect();

                let explicit_bounds_list = explicit_bounds_array.value(row_idx);
                let explicit_bounds_values = explicit_bounds_list
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| Error::Storage("Invalid explicit_bounds values".to_string()))?;
                let explicit_bounds: Vec<f64> = (0..explicit_bounds_values.len())
                    .map(|i| explicit_bounds_values.value(i))
                    .collect();

                data_points.push(HistogramDataPoint {
                    metric_id,
                    timestamp,
                    start_time: None,
                    count,
                    sum,
                    min: None,
                    max: None,
                    bucket_counts,
                    explicit_bounds,
                    exemplars: Vec::<Exemplar>::new(),
                    attributes,
                    resource_id: 0,
                });
            }
        }

        Ok(data_points)
    }

    pub fn create_vortex_format(&self) -> Arc<dyn datafusion::datasource::file_format::FileFormat> {
        use vortex::session::VortexSession;
        use vortex::VortexSessionDefault;
        use vortex_datafusion::VortexFormat;

        use crate::indexed_layout::register_indexed_layout;

        let vortex_session = VortexSession::default();
        register_indexed_layout(&vortex_session);
        Arc::new(VortexFormat::new(vortex_session))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_filter_metric_batches_time_range() {
        use arrow::array::{Float64Array, Int64Array, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        // Create a test batch with timestamps
        let schema = Arc::new(Schema::new(vec![
            Field::new("series_id", DataType::UInt64, false),
            Field::new("time_unix_nano", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let series_ids = UInt64Array::from(vec![1, 2, 3, 4]);
        let timestamps = Int64Array::from(vec![1000, 2000, 3000, 4000]);
        let values = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(series_ids), Arc::new(timestamps), Arc::new(values)],
        )
        .expect("Failed to create batch");

        // Filter for range 1500-3500 (should get rows with timestamps 2000 and 3000)
        let filtered = ColdTier::filter_metric_batches(vec![batch], 1500, 3500, None);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_filter_metric_batches_with_series_ids() {
        use arrow::array::{Float64Array, Int64Array, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::collections::HashSet;

        let schema = Arc::new(Schema::new(vec![
            Field::new("series_id", DataType::UInt64, false),
            Field::new("time_unix_nano", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let series_ids = UInt64Array::from(vec![1, 2, 3, 4]);
        let timestamps = Int64Array::from(vec![2000, 2000, 2000, 2000]);
        let values = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(series_ids), Arc::new(timestamps), Arc::new(values)],
        )
        .expect("Failed to create batch");

        // Filter for series 1 and 3 only
        let mut filter_ids = HashSet::new();
        filter_ids.insert(1u64);
        filter_ids.insert(3u64);

        let filtered = ColdTier::filter_metric_batches(vec![batch], 1000, 3000, Some(&filter_ids));

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_filter_histogram_batches_time_range() {
        use arrow::array::{Float64Array, Int64Array, ListArray, UInt64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![
            Field::new("series_id", DataType::UInt64, false),
            Field::new("time_unix_nano", DataType::Int64, false),
            Field::new("count", DataType::UInt64, false),
            Field::new("sum", DataType::Float64, false),
            Field::new(
                "bucket_counts",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
                false,
            ),
            Field::new(
                "explicit_bounds",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
                false,
            ),
        ]));

        let series_ids = UInt64Array::from(vec![1, 2]);
        let timestamps = Int64Array::from(vec![1000, 3000]);
        let counts = UInt64Array::from(vec![10, 20]);
        let sums = Float64Array::from(vec![100.0, 200.0]);

        // Create empty lists for bucket_counts and explicit_bounds
        let bucket_values = UInt64Array::from(vec![1, 2, 3, 4]);
        let bucket_offsets = arrow::buffer::OffsetBuffer::new(vec![0, 2, 4].into());
        let bucket_counts = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            bucket_offsets,
            Arc::new(bucket_values),
            None,
        );

        let bounds_values = Float64Array::from(vec![10.0, 50.0, 100.0, 200.0]);
        let bounds_offsets = arrow::buffer::OffsetBuffer::new(vec![0, 2, 4].into());
        let explicit_bounds = ListArray::new(
            Arc::new(Field::new("item", DataType::Float64, false)),
            bounds_offsets,
            Arc::new(bounds_values),
            None,
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(series_ids),
                Arc::new(timestamps),
                Arc::new(counts),
                Arc::new(sums),
                Arc::new(bucket_counts),
                Arc::new(explicit_bounds),
            ],
        )
        .expect("Failed to create batch");

        // Filter for range 500-2000 (should get only first row)
        let filtered = ColdTier::filter_histogram_batches(vec![batch], 500, 2000, None);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].num_rows(), 1);
    }
}
