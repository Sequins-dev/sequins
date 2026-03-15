//! Direct OTLP metric → Arrow RecordBatch conversion
//!
//! Produces four RecordBatches per batch of OTLP metrics:
//! - Metric metadata (`metric_schema`)
//! - Gauge/counter data points (`series_data_point_schema`)
//! - Explicit histogram data points (`histogram_series_data_point_schema`)
//! - Exponential histogram data points (`exp_histogram_data_point_schema`)

use arrow::array::{
    ArrayRef, Float64Array, Int32Array, ListArray, StringViewArray, TimestampNanosecondArray,
    UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::Metric as OtlpMetric;
use sequins_types::arrow_schema::{
    exp_histogram_data_point_schema, histogram_series_data_point_schema, metric_schema,
    series_data_point_schema,
};
use sequins_types::models::MetricId;
use std::sync::Arc;

/// Determine the metric type string from an OTLP metric's data variant.
pub fn otlp_metric_type(metric: &OtlpMetric) -> &'static str {
    match &metric.data {
        Some(Data::Gauge(_)) => "gauge",
        Some(Data::Sum(s)) if s.is_monotonic => "counter",
        Some(Data::Sum(_)) => "gauge",
        Some(Data::Histogram(_)) => "histogram",
        Some(Data::ExponentialHistogram(_)) => "histogram",
        Some(Data::Summary(_)) => "summary",
        None => "gauge",
    }
}

/// Convert a batch of OTLP metrics to a metric metadata `RecordBatch`.
///
/// `items` contains `(OtlpMetric, resource_id, scope_id, service_name)` tuples.
/// The output schema is `metric_schema()`.
pub fn otlp_metrics_to_batch(
    items: &[(OtlpMetric, u32, u32, String)],
) -> Result<RecordBatch, String> {
    let schema = metric_schema();
    let n = items.len();
    if n == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut metric_ids: Vec<String> = Vec::with_capacity(n);
    let mut names: Vec<String> = Vec::with_capacity(n);
    let mut descriptions: Vec<Option<String>> = Vec::with_capacity(n);
    let mut units: Vec<Option<String>> = Vec::with_capacity(n);
    let mut metric_types: Vec<String> = Vec::with_capacity(n);
    let mut service_names: Vec<String> = Vec::with_capacity(n);
    let mut resource_ids: Vec<u32> = Vec::with_capacity(n);
    let mut scope_ids: Vec<u32> = Vec::with_capacity(n);

    for (metric, resource_id, scope_id, service_name) in items {
        let metric_type = otlp_metric_type(metric);
        let metric_id = MetricId::from_fields(
            &metric.name,
            &metric.description,
            &metric.unit,
            metric_type,
            *resource_id,
            *scope_id,
        );
        metric_ids.push(metric_id.to_hex());
        names.push(metric.name.clone());
        descriptions.push(if metric.description.is_empty() {
            None
        } else {
            Some(metric.description.clone())
        });
        units.push(if metric.unit.is_empty() {
            None
        } else {
            Some(metric.unit.clone())
        });
        metric_types.push(metric_type.to_string());
        service_names.push(service_name.clone());
        resource_ids.push(*resource_id);
        scope_ids.push(*scope_id);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringViewArray::from(metric_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(names)) as ArrayRef,
        Arc::new(StringViewArray::from(descriptions)) as ArrayRef,
        Arc::new(StringViewArray::from(units)) as ArrayRef,
        Arc::new(StringViewArray::from(metric_types)) as ArrayRef,
        Arc::new(StringViewArray::from(service_names)) as ArrayRef,
        Arc::new(UInt32Array::from(resource_ids)) as ArrayRef,
        Arc::new(UInt32Array::from(scope_ids)) as ArrayRef,
    ];

    debug_assert!(
        arrays.iter().all(|a| a.len() == n),
        "otlp_metrics_to_batch: column length mismatch; expected {} rows, got lengths: {:?}",
        n,
        arrays.iter().map(|a| a.len()).collect::<Vec<_>>()
    );

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

/// Convert a batch of OTLP metrics to a gauge/counter data point `RecordBatch`.
///
/// Extracts `NumberDataPoint`s from Gauge and Sum metrics.
/// The output schema is `series_data_point_schema()`.
/// `series_id` is set to 0 (placeholder; resolved at cold-tier write time).
pub fn otlp_datapoints_to_batch(
    items: &[(OtlpMetric, u32, u32, String)],
) -> Result<RecordBatch, String> {
    let schema = series_data_point_schema();

    let mut series_ids: Vec<u64> = Vec::new();
    let mut metric_ids: Vec<String> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    for (metric, resource_id, scope_id, _service_name) in items {
        let metric_type = otlp_metric_type(metric);
        let metric_id = MetricId::from_fields(
            &metric.name,
            &metric.description,
            &metric.unit,
            metric_type,
            *resource_id,
            *scope_id,
        );
        let metric_id_hex = metric_id.to_hex();

        let data_points = match &metric.data {
            Some(Data::Gauge(g)) => &g.data_points,
            Some(Data::Sum(s)) => &s.data_points,
            _ => continue,
        };

        for point in data_points {
            use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
            let value = match &point.value {
                Some(Value::AsDouble(v)) => *v,
                Some(Value::AsInt(v)) => *v as f64,
                None => continue,
            };
            series_ids.push(0u64);
            metric_ids.push(metric_id_hex.clone());
            timestamps.push(point.time_unix_nano as i64);
            values.push(value);
        }
    }

    if series_ids.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let n = series_ids.len();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(series_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(metric_ids)) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
        Arc::new(Float64Array::from(values)) as ArrayRef,
    ];

    debug_assert!(
        arrays.iter().all(|a| a.len() == n),
        "otlp_datapoints_to_batch: column length mismatch; expected {} rows, got lengths: {:?}",
        n,
        arrays.iter().map(|a| a.len()).collect::<Vec<_>>()
    );

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

/// Convert a batch of OTLP metrics to an explicit histogram data point `RecordBatch`.
///
/// Extracts `HistogramDataPoint`s from Histogram metrics.
/// The output schema is `histogram_series_data_point_schema()`.
/// `series_id` is set to 0 (placeholder; resolved at cold-tier write time).
pub fn otlp_histograms_to_batch(
    items: &[(OtlpMetric, u32, u32, String)],
) -> Result<RecordBatch, String> {
    let schema = histogram_series_data_point_schema();

    let mut series_ids: Vec<u64> = Vec::new();
    let mut metric_ids: Vec<String> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut counts: Vec<u64> = Vec::new();
    let mut sums: Vec<f64> = Vec::new();
    // bucket_counts as flat values + offsets for ListArray
    let mut bucket_offsets: Vec<i32> = vec![0];
    let mut bucket_values: Vec<u64> = Vec::new();
    // explicit_bounds as flat values + offsets for ListArray
    let mut bounds_offsets: Vec<i32> = vec![0];
    let mut bounds_values: Vec<f64> = Vec::new();

    for (metric, resource_id, scope_id, _service_name) in items {
        let metric_type = otlp_metric_type(metric);
        let metric_id = MetricId::from_fields(
            &metric.name,
            &metric.description,
            &metric.unit,
            metric_type,
            *resource_id,
            *scope_id,
        );
        let metric_id_hex = metric_id.to_hex();

        let data_points = match &metric.data {
            Some(Data::Histogram(h)) => &h.data_points,
            _ => continue,
        };

        for point in data_points {
            series_ids.push(0u64);
            metric_ids.push(metric_id_hex.clone());
            timestamps.push(point.time_unix_nano as i64);
            counts.push(point.count);
            sums.push(point.sum.unwrap_or(0.0));

            let prev_bucket = *bucket_offsets.last().unwrap();
            bucket_values.extend_from_slice(&point.bucket_counts);
            bucket_offsets.push(prev_bucket + point.bucket_counts.len() as i32);

            let prev_bounds = *bounds_offsets.last().unwrap();
            bounds_values.extend_from_slice(&point.explicit_bounds);
            bounds_offsets.push(prev_bounds + point.explicit_bounds.len() as i32);
        }
    }

    if series_ids.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let bucket_counts_array = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, false)),
        OffsetBuffer::new(bucket_offsets.into()),
        Arc::new(UInt64Array::from(bucket_values)),
        None,
    );

    let explicit_bounds_array = ListArray::new(
        Arc::new(Field::new("item", DataType::Float64, false)),
        OffsetBuffer::new(bounds_offsets.into()),
        Arc::new(Float64Array::from(bounds_values)),
        None,
    );

    let n = series_ids.len();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(series_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(metric_ids)) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
        Arc::new(UInt64Array::from(counts)) as ArrayRef,
        Arc::new(Float64Array::from(sums)) as ArrayRef,
        Arc::new(bucket_counts_array) as ArrayRef,
        Arc::new(explicit_bounds_array) as ArrayRef,
    ];

    debug_assert!(
        arrays.iter().all(|a| a.len() == n),
        "otlp_histograms_to_batch: column length mismatch; expected {} rows, got: {:?}",
        n,
        arrays.iter().map(|a| a.len()).collect::<Vec<_>>()
    );

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

/// Convert a batch of OTLP metrics to a native exponential histogram data point `RecordBatch`.
///
/// Extracts `ExponentialHistogramDataPoint`s, preserving the compact scale+offset+counts format.
/// The output schema is `exp_histogram_data_point_schema()`.
/// `series_id` is set to 0 (placeholder; resolved at cold-tier write time).
pub fn otlp_exp_histograms_to_batch(
    items: &[(OtlpMetric, u32, u32, String)],
) -> Result<RecordBatch, String> {
    let schema = exp_histogram_data_point_schema();

    let mut series_ids: Vec<u64> = Vec::new();
    let mut metric_ids: Vec<String> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut counts: Vec<u64> = Vec::new();
    let mut sums: Vec<f64> = Vec::new();
    let mut scales: Vec<i32> = Vec::new();
    let mut zero_counts: Vec<u64> = Vec::new();
    let mut pos_offsets: Vec<i32> = Vec::new();
    let mut neg_offsets: Vec<i32> = Vec::new();
    // positive_counts and negative_counts as flat values + offsets for ListArrays
    let mut pos_offsets_list: Vec<i32> = vec![0];
    let mut pos_values: Vec<u64> = Vec::new();
    let mut neg_offsets_list: Vec<i32> = vec![0];
    let mut neg_values: Vec<u64> = Vec::new();

    for (metric, resource_id, scope_id, _service_name) in items {
        let metric_type = otlp_metric_type(metric);
        let metric_id = MetricId::from_fields(
            &metric.name,
            &metric.description,
            &metric.unit,
            metric_type,
            *resource_id,
            *scope_id,
        );
        let metric_id_hex = metric_id.to_hex();

        let data_points = match &metric.data {
            Some(Data::ExponentialHistogram(eh)) => &eh.data_points,
            _ => continue,
        };

        for point in data_points {
            series_ids.push(0u64);
            metric_ids.push(metric_id_hex.clone());
            timestamps.push(point.time_unix_nano as i64);
            counts.push(point.count);
            sums.push(point.sum.unwrap_or(0.0));
            scales.push(point.scale);
            zero_counts.push(point.zero_count);

            let (positive_offset, positive_counts) = point
                .positive
                .as_ref()
                .map(|b| (b.offset, b.bucket_counts.as_slice()))
                .unwrap_or((0, &[]));
            let (negative_offset, negative_counts) = point
                .negative
                .as_ref()
                .map(|b| (b.offset, b.bucket_counts.as_slice()))
                .unwrap_or((0, &[]));

            pos_offsets.push(positive_offset);
            neg_offsets.push(negative_offset);

            let prev_pos = *pos_offsets_list.last().unwrap();
            pos_values.extend_from_slice(positive_counts);
            pos_offsets_list.push(prev_pos + positive_counts.len() as i32);

            let prev_neg = *neg_offsets_list.last().unwrap();
            neg_values.extend_from_slice(negative_counts);
            neg_offsets_list.push(prev_neg + negative_counts.len() as i32);
        }
    }

    if series_ids.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let positive_counts_array = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, false)),
        OffsetBuffer::new(pos_offsets_list.into()),
        Arc::new(UInt64Array::from(pos_values)),
        None,
    );
    let negative_counts_array = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, false)),
        OffsetBuffer::new(neg_offsets_list.into()),
        Arc::new(UInt64Array::from(neg_values)),
        None,
    );

    let n = series_ids.len();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(series_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(metric_ids)) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
        Arc::new(UInt64Array::from(counts)) as ArrayRef,
        Arc::new(Float64Array::from(sums)) as ArrayRef,
        Arc::new(Int32Array::from(scales)) as ArrayRef,
        Arc::new(UInt64Array::from(zero_counts)) as ArrayRef,
        Arc::new(Int32Array::from(pos_offsets)) as ArrayRef,
        Arc::new(positive_counts_array) as ArrayRef,
        Arc::new(Int32Array::from(neg_offsets)) as ArrayRef,
        Arc::new(negative_counts_array) as ArrayRef,
    ];

    debug_assert!(
        arrays.iter().all(|a| a.len() == n),
        "otlp_exp_histograms_to_batch: column length mismatch; expected {} rows, got lengths: {:?}",
        n,
        arrays.iter().map(|a| a.len()).collect::<Vec<_>>()
    );

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{
        exponential_histogram_data_point::Buckets, ExponentialHistogram,
        ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, NumberDataPoint,
    };

    fn make_gauge_metric(name: &str, value: f64, ts: u64) -> OtlpMetric {
        OtlpMetric {
            name: name.to_string(),
            description: String::new(),
            unit: String::new(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: ts,
                    value: Some(
                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                            value,
                        ),
                    ),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        }
    }

    #[test]
    fn test_otlp_metrics_to_batch_basic() {
        let items = vec![(
            make_gauge_metric("http.duration", 42.0, 1_000_000_000),
            1u32,
            2u32,
            "my-service".to_string(),
        )];
        let batch = otlp_metrics_to_batch(&items).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), metric_schema());
    }

    #[test]
    fn test_otlp_datapoints_to_batch_gauge() {
        let items = vec![(
            make_gauge_metric("cpu.usage", 99.5, 2_000_000_000),
            1u32,
            2u32,
            "svc".to_string(),
        )];
        let batch = otlp_datapoints_to_batch(&items).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), series_data_point_schema());
    }

    #[test]
    fn test_otlp_histograms_to_batch_basic() {
        let hist_metric = OtlpMetric {
            name: "latency".to_string(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 10,
                    sum: Some(250.0),
                    bucket_counts: vec![2, 3, 5],
                    explicit_bounds: vec![10.0, 50.0],
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        let items = vec![(hist_metric, 1u32, 2u32, "svc".to_string())];
        let batch = otlp_histograms_to_batch(&items).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), histogram_series_data_point_schema());
    }

    #[test]
    fn test_otlp_exp_histograms_to_batch_basic() {
        let exp_metric = OtlpMetric {
            name: "exp_latency".to_string(),
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 100,
                    sum: Some(250.0),
                    scale: 0,
                    zero_count: 10,
                    positive: Some(Buckets {
                        offset: 0,
                        bucket_counts: vec![20, 30, 40],
                    }),
                    negative: None,
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        let items = vec![(exp_metric, 1u32, 2u32, "svc".to_string())];
        let batch = otlp_exp_histograms_to_batch(&items).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.schema(), exp_histogram_data_point_schema());
    }
}
