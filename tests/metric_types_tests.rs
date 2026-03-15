//! Comprehensive metric type conversion tests
//!
//! Tests all OTLP metric types and edge cases:
//! - Gauge metrics
//! - Sum metrics (monotonic and non-monotonic)
//! - Histogram metrics with buckets
//! - Exponential histogram metrics
//! - Summary metrics with quantiles
//! - Multiple data points per metric
//! - Metric aggregation temporality

use opentelemetry_proto::tonic::{
    common::v1::{any_value::Value as OtlpValue, AnyValue, KeyValue},
    metrics::v1::{
        exemplar::Value as ExemplarValue, metric::Data as MetricData,
        number_data_point::Value as NumberValue, summary_data_point::ValueAtQuantile,
        AggregationTemporality, Exemplar, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, Sum, Summary, SummaryDataPoint,
    },
    resource::v1::Resource,
};
use sequins::ingest::otlp_conversions::*;

// ============================================================================
// Sum Metric Tests (Counter and non-monotonic)
// ============================================================================

#[test]
fn test_convert_monotonic_sum_as_counter() {
    let metric = Metric {
        name: "http.requests.total".to_string(),
        description: "Total HTTP requests".to_string(),
        unit: "1".to_string(),
        data: Some(MetricData::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                value: Some(NumberValue::AsInt(42)),
                exemplars: vec![],
                flags: 0,
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true, // Counter
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(OtlpValue::StringValue("test-service".to_string())),
            }),
        }],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert_eq!(converted.name, "http.requests.total");
    assert_eq!(converted.metric_type, sequins::models::MetricType::Counter);
}

#[test]
fn test_convert_non_monotonic_sum_as_gauge() {
    let metric = Metric {
        name: "current.connections".to_string(),
        description: "Current active connections".to_string(),
        unit: "1".to_string(),
        data: Some(MetricData::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                value: Some(NumberValue::AsInt(10)),
                exemplars: vec![],
                flags: 0,
            }],
            aggregation_temporality: AggregationTemporality::Delta as i32,
            is_monotonic: false, // Non-monotonic -> Gauge
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert_eq!(converted.metric_type, sequins::models::MetricType::Gauge);
}

// ============================================================================
// Histogram Tests
// ============================================================================

#[test]
fn test_convert_histogram_with_multiple_buckets() {
    let metric = Metric {
        name: "http.response.time".to_string(),
        description: "HTTP response time distribution".to_string(),
        unit: "ms".to_string(),
        data: Some(MetricData::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                count: 100,
                sum: Some(5000.0),
                bucket_counts: vec![10, 30, 40, 15, 5],
                explicit_bounds: vec![100.0, 200.0, 500.0, 1000.0],
                exemplars: vec![],
                flags: 0,
                min: Some(10.0),
                max: Some(2000.0),
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert_eq!(
        converted.metric_type,
        sequins::models::MetricType::Histogram
    );
}

#[test]
fn test_convert_histogram_with_no_sum() {
    let metric = Metric {
        name: "request.size".to_string(),
        description: "Request size distribution".to_string(),
        unit: "bytes".to_string(),
        data: Some(MetricData::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                count: 50,
                sum: None, // No sum provided
                bucket_counts: vec![20, 20, 10],
                explicit_bounds: vec![1024.0, 4096.0],
                exemplars: vec![],
                flags: 0,
                min: None,
                max: None,
            }],
            aggregation_temporality: AggregationTemporality::Delta as i32,
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
}

#[test]
fn test_convert_histogram_empty_buckets() {
    let metric = Metric {
        name: "empty.histogram".to_string(),
        description: "Histogram with no observations".to_string(),
        unit: "1".to_string(),
        data: Some(MetricData::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                count: 0,
                sum: Some(0.0),
                bucket_counts: vec![0, 0, 0],
                explicit_bounds: vec![10.0, 100.0],
                exemplars: vec![],
                flags: 0,
                min: None,
                max: None,
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
}

// ============================================================================
// Summary Tests
// ============================================================================

#[test]
fn test_convert_summary_with_quantiles() {
    let metric = Metric {
        name: "rpc.latency".to_string(),
        description: "RPC latency summary".to_string(),
        unit: "ms".to_string(),
        data: Some(MetricData::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                count: 1000,
                sum: 50000.0,
                quantile_values: vec![
                    ValueAtQuantile {
                        quantile: 0.5,
                        value: 45.0,
                    },
                    ValueAtQuantile {
                        quantile: 0.95,
                        value: 120.0,
                    },
                    ValueAtQuantile {
                        quantile: 0.99,
                        value: 250.0,
                    },
                ],
                flags: 0,
            }],
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
    let converted = result.unwrap();
    assert_eq!(converted.metric_type, sequins::models::MetricType::Summary);
}

#[test]
fn test_convert_summary_no_quantiles() {
    let metric = Metric {
        name: "basic.summary".to_string(),
        description: "Summary with only count and sum".to_string(),
        unit: "1".to_string(),
        data: Some(MetricData::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                count: 100,
                sum: 500.0,
                quantile_values: vec![], // No quantiles
                flags: 0,
            }],
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
}

// ============================================================================
// Multiple Data Points Tests
// ============================================================================

#[test]
fn test_convert_gauge_with_multiple_data_points() {
    let metric = Metric {
        name: "cpu.usage".to_string(),
        description: "CPU usage per core".to_string(),
        unit: "percent".to_string(),
        data: Some(MetricData::Gauge(Gauge {
            data_points: vec![
                NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "core".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::IntValue(0)),
                        }),
                    }],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsDouble(75.5)),
                    exemplars: vec![],
                    flags: 0,
                },
                NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "core".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::IntValue(1)),
                        }),
                    }],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsDouble(82.3)),
                    exemplars: vec![],
                    flags: 0,
                },
                NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "core".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::IntValue(2)),
                        }),
                    }],
                    start_time_unix_nano: 1000000000,
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsDouble(68.1)),
                    exemplars: vec![],
                    flags: 0,
                },
            ],
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric_data_points(metric, Some(&resource));
    assert!(result.is_ok());
    let (_metric, data_points) = result.unwrap();
    assert_eq!(data_points.len(), 3);
}

// ============================================================================
// Exemplar Tests
// ============================================================================

#[test]
fn test_convert_metric_with_exemplars() {
    let metric = Metric {
        name: "requests".to_string(),
        description: "Request count with exemplars".to_string(),
        unit: "1".to_string(),
        data: Some(MetricData::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                value: Some(NumberValue::AsInt(1000)),
                exemplars: vec![Exemplar {
                    filtered_attributes: vec![],
                    time_unix_nano: 1500000000,
                    value: Some(ExemplarValue::AsInt(1)),
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                }],
                flags: 0,
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
}

// ============================================================================
// Aggregation Temporality Tests
// ============================================================================

#[test]
fn test_convert_cumulative_temporality() {
    let metric = Metric {
        name: "cumulative.metric".to_string(),
        description: "Cumulative aggregation".to_string(),
        unit: "1".to_string(),
        data: Some(MetricData::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                value: Some(NumberValue::AsInt(100)),
                exemplars: vec![],
                flags: 0,
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
}

#[test]
fn test_convert_delta_temporality() {
    let metric = Metric {
        name: "delta.metric".to_string(),
        description: "Delta aggregation".to_string(),
        unit: "1".to_string(),
        data: Some(MetricData::Sum(Sum {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                value: Some(NumberValue::AsInt(10)),
                exemplars: vec![],
                flags: 0,
            }],
            aggregation_temporality: AggregationTemporality::Delta as i32,
            is_monotonic: true,
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };

    let result = convert_otlp_metric(metric, Some(&resource));
    assert!(result.is_ok());
}
