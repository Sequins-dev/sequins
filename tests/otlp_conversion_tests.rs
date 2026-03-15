//! Unit tests for OTLP conversion functions
//!
//! These tests verify that OTLP protobuf types are correctly converted to Sequins types,
//! including edge cases, invalid data, and various attribute type conversions.

use opentelemetry_proto::tonic::{
    common::v1::{any_value::Value as OtlpValue, AnyValue, KeyValue},
    logs::v1::LogRecord,
    metrics::v1::{
        metric::Data, number_data_point::Value as NumberValue, Gauge, Histogram,
        Metric as OtlpMetric, NumberDataPoint, Sum,
    },
    resource::v1::Resource,
    trace::v1::{span::SpanKind, status::StatusCode, Span as OtlpSpan, Status},
};
use sequins::ingest::otlp_conversions::{
    convert_otlp_log, convert_otlp_metric, convert_otlp_metric_data_points, convert_otlp_span,
};
use sequins::models::{LogSeverity, MetricType, SpanStatus};

// ============================================================================
// Span Conversion Tests
// ============================================================================

#[test]
fn test_convert_valid_span() {
    let trace_id = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let span_id = vec![1, 2, 3, 4, 5, 6, 7, 8];

    let otlp_span = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: span_id.clone(),
        parent_span_id: vec![],
        name: "test-operation".to_string(),
        kind: SpanKind::Server as i32,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![],
        status: Some(Status {
            code: StatusCode::Ok as i32,
            message: String::new(),
        }),
        ..Default::default()
    };

    let resource = Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(OtlpValue::StringValue("test-service".to_string())),
            }),
        }],
        ..Default::default()
    });

    let span = convert_otlp_span(otlp_span, resource.as_ref()).unwrap();

    assert_eq!(span.operation_name, "test-operation");
    assert_eq!(span.service_name, "test-service");
    assert_eq!(span.span_kind, sequins::models::SpanKind::Server);
    assert_eq!(span.status, SpanStatus::Ok);
    assert!(span.parent_span_id.is_none());
}

#[test]
fn test_convert_span_with_parent() {
    let trace_id = vec![1; 16];
    let span_id = vec![2; 8];
    let parent_span_id = vec![1; 8];

    let otlp_span = OtlpSpan {
        trace_id,
        span_id,
        parent_span_id,
        name: "child-operation".to_string(),
        kind: SpanKind::Internal as i32,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        ..Default::default()
    };

    let span = convert_otlp_span(otlp_span, None).unwrap();

    assert!(span.parent_span_id.is_some());
    assert_eq!(span.service_name, "unknown"); // No resource provided
}

#[test]
fn test_convert_span_invalid_trace_id() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3], // Invalid length (should be 16)
        span_id: vec![1; 8],
        name: "test".to_string(),
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        ..Default::default()
    };

    let result = convert_otlp_span(otlp_span, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("trace_id"));
}

#[test]
fn test_convert_span_invalid_span_id() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1; 16],
        span_id: vec![1, 2, 3], // Invalid length (should be 8)
        name: "test".to_string(),
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        ..Default::default()
    };

    let result = convert_otlp_span(otlp_span, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("span_id"));
}

#[test]
fn test_convert_span_all_span_kinds() {
    let test_cases = vec![
        (0, sequins::models::SpanKind::Unspecified),
        (1, sequins::models::SpanKind::Internal),
        (2, sequins::models::SpanKind::Server),
        (3, sequins::models::SpanKind::Client),
        (4, sequins::models::SpanKind::Producer),
        (5, sequins::models::SpanKind::Consumer),
        (99, sequins::models::SpanKind::Unspecified), // Invalid kind
    ];

    for (otlp_kind, expected_kind) in test_cases {
        let otlp_span = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![1; 8],
            name: "test".to_string(),
            kind: otlp_kind,
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            ..Default::default()
        };

        let span = convert_otlp_span(otlp_span, None).unwrap();
        assert_eq!(span.span_kind, expected_kind);
    }
}

#[test]
fn test_convert_span_all_statuses() {
    let test_cases = vec![
        (0, SpanStatus::Unset),
        (1, SpanStatus::Ok),
        (2, SpanStatus::Error),
        (99, SpanStatus::Unset), // Invalid status
    ];

    for (otlp_status, expected_status) in test_cases {
        let otlp_span = OtlpSpan {
            trace_id: vec![1; 16],
            span_id: vec![1; 8],
            name: "test".to_string(),
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            status: Some(Status {
                code: otlp_status,
                message: String::new(),
            }),
            ..Default::default()
        };

        let span = convert_otlp_span(otlp_span, None).unwrap();
        assert_eq!(span.status, expected_status);
    }
}

#[test]
fn test_convert_span_with_attributes() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1; 16],
        span_id: vec![1; 8],
        name: "test".to_string(),
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![
            KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue("GET".to_string())),
                }),
            },
            KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::IntValue(200)),
                }),
            },
            KeyValue {
                key: "success".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::BoolValue(true)),
                }),
            },
            KeyValue {
                key: "duration_ms".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::DoubleValue(42.5)),
                }),
            },
        ],
        ..Default::default()
    };

    let span = convert_otlp_span(otlp_span, None).unwrap();

    assert_eq!(span.attributes.len(), 4);
    assert!(span.attributes.contains_key("http.method"));
    assert!(span.attributes.contains_key("http.status_code"));
    assert!(span.attributes.contains_key("success"));
    assert!(span.attributes.contains_key("duration_ms"));
}

#[test]
fn test_convert_span_with_events() {
    use opentelemetry_proto::tonic::trace::v1::span::Event as OtlpEvent;

    let otlp_span = OtlpSpan {
        trace_id: vec![1; 16],
        span_id: vec![1; 8],
        name: "test".to_string(),
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        events: vec![
            OtlpEvent {
                time_unix_nano: 1500000000,
                name: "checkpoint".to_string(),
                attributes: vec![KeyValue {
                    key: "stage".to_string(),
                    value: Some(AnyValue {
                        value: Some(OtlpValue::StringValue("processing".to_string())),
                    }),
                }],
                ..Default::default()
            },
            OtlpEvent {
                time_unix_nano: 1800000000,
                name: "error".to_string(),
                attributes: vec![],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let span = convert_otlp_span(otlp_span, None).unwrap();

    assert_eq!(span.events.len(), 2);
    assert_eq!(span.events[0].name, "checkpoint");
    assert_eq!(span.events[1].name, "error");
    assert_eq!(span.events[0].attributes.len(), 1);
}

// ============================================================================
// Log Conversion Tests
// ============================================================================

#[test]
fn test_convert_valid_log() {
    let log_record = LogRecord {
        time_unix_nano: 1000000000,
        observed_time_unix_nano: 1000000001,
        severity_number: 9, // Info
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(OtlpValue::StringValue("Test message".to_string())),
        }),
        attributes: vec![],
        ..Default::default()
    };

    let resource = Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(OtlpValue::StringValue("log-service".to_string())),
            }),
        }],
        ..Default::default()
    });

    let log = convert_otlp_log(log_record, resource.as_ref()).unwrap();

    assert_eq!(log.body, "Test message");
    assert_eq!(log.service_name, "log-service");
    assert_eq!(log.severity, LogSeverity::Info);
}

#[test]
fn test_convert_log_all_severities() {
    let test_cases = vec![
        (1, LogSeverity::Trace),
        (4, LogSeverity::Trace),
        (5, LogSeverity::Debug),
        (8, LogSeverity::Debug),
        (9, LogSeverity::Info),
        (12, LogSeverity::Info),
        (13, LogSeverity::Warn),
        (16, LogSeverity::Warn),
        (17, LogSeverity::Error),
        (20, LogSeverity::Error),
        (21, LogSeverity::Fatal),
        (24, LogSeverity::Fatal),
        (0, LogSeverity::Info),  // Unspecified
        (99, LogSeverity::Info), // Out of range
    ];

    for (severity_number, expected_severity) in test_cases {
        let log_record = LogRecord {
            time_unix_nano: 1000000000,
            observed_time_unix_nano: 1000000001,
            severity_number,
            body: Some(AnyValue {
                value: Some(OtlpValue::StringValue("test".to_string())),
            }),
            ..Default::default()
        };

        let log = convert_otlp_log(log_record, None).unwrap();
        assert_eq!(log.severity, expected_severity);
    }
}

#[test]
fn test_convert_log_empty_body() {
    let log_record = LogRecord {
        time_unix_nano: 1000000000,
        observed_time_unix_nano: 1000000001,
        severity_number: 9,
        body: None, // No body
        ..Default::default()
    };

    let log = convert_otlp_log(log_record, None).unwrap();
    assert_eq!(log.body, ""); // Empty string
}

#[test]
fn test_convert_log_with_trace_context() {
    let trace_id = vec![1; 16];
    let span_id = vec![2; 8];

    let log_record = LogRecord {
        time_unix_nano: 1000000000,
        observed_time_unix_nano: 1000000001,
        severity_number: 9,
        body: Some(AnyValue {
            value: Some(OtlpValue::StringValue("test".to_string())),
        }),
        trace_id: trace_id.clone(),
        span_id: span_id.clone(),
        ..Default::default()
    };

    let log = convert_otlp_log(log_record, None).unwrap();

    assert!(log.trace_id.is_some());
    assert!(log.span_id.is_some());
}

#[test]
fn test_convert_log_without_trace_context() {
    let log_record = LogRecord {
        time_unix_nano: 1000000000,
        observed_time_unix_nano: 1000000001,
        severity_number: 9,
        body: Some(AnyValue {
            value: Some(OtlpValue::StringValue("test".to_string())),
        }),
        trace_id: vec![], // Empty
        span_id: vec![],  // Empty
        ..Default::default()
    };

    let log = convert_otlp_log(log_record, None).unwrap();

    assert!(log.trace_id.is_none());
    assert!(log.span_id.is_none());
}

#[test]
fn test_convert_log_with_resource_attributes() {
    let log_record = LogRecord {
        time_unix_nano: 1000000000,
        observed_time_unix_nano: 1000000001,
        severity_number: 9,
        body: Some(AnyValue {
            value: Some(OtlpValue::StringValue("test".to_string())),
        }),
        ..Default::default()
    };

    let resource = Some(Resource {
        attributes: vec![
            KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue("test-service".to_string())),
                }),
            },
            KeyValue {
                key: "deployment.environment".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue("production".to_string())),
                }),
            },
        ],
        ..Default::default()
    });

    let log = convert_otlp_log(log_record, resource.as_ref()).unwrap();

    assert_eq!(log.service_name, "test-service");
    assert!(log.resource.contains_key("deployment.environment"));
}

// ============================================================================
// Metric Conversion Tests
// ============================================================================

#[test]
fn test_convert_gauge_metric() {
    let metric = OtlpMetric {
        name: "cpu_usage".to_string(),
        description: "CPU usage percentage".to_string(),
        unit: "percent".to_string(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![],
        })),
        ..Default::default()
    };

    let resource = Some(Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(OtlpValue::StringValue("metric-service".to_string())),
            }),
        }],
        ..Default::default()
    });

    let converted = convert_otlp_metric(metric, resource.as_ref()).unwrap();

    assert_eq!(converted.name, "cpu_usage");
    assert_eq!(converted.description, "CPU usage percentage");
    assert_eq!(converted.unit, "percent");
    assert_eq!(converted.metric_type, MetricType::Gauge);
    assert_eq!(converted.service_name, "metric-service");
}

#[test]
fn test_convert_counter_metric() {
    let metric = OtlpMetric {
        name: "request_count".to_string(),
        description: "Total requests".to_string(),
        unit: "1".to_string(),
        data: Some(Data::Sum(Sum {
            data_points: vec![],
            aggregation_temporality: 2, // Cumulative
            is_monotonic: true,         // Counter
        })),
        ..Default::default()
    };

    let converted = convert_otlp_metric(metric, None).unwrap();

    assert_eq!(converted.metric_type, MetricType::Counter);
}

#[test]
fn test_convert_non_monotonic_sum_as_gauge() {
    let metric = OtlpMetric {
        name: "temperature".to_string(),
        description: "Temperature".to_string(),
        unit: "C".to_string(),
        data: Some(Data::Sum(Sum {
            data_points: vec![],
            aggregation_temporality: 1, // Delta
            is_monotonic: false,        // Not a counter
        })),
        ..Default::default()
    };

    let converted = convert_otlp_metric(metric, None).unwrap();

    // Non-monotonic sum should be treated as gauge
    assert_eq!(converted.metric_type, MetricType::Gauge);
}

#[test]
fn test_convert_histogram_metric() {
    let metric = OtlpMetric {
        name: "request_duration".to_string(),
        description: "Request duration histogram".to_string(),
        unit: "ms".to_string(),
        data: Some(Data::Histogram(Histogram {
            data_points: vec![],
            aggregation_temporality: 2,
        })),
        ..Default::default()
    };

    let converted = convert_otlp_metric(metric, None).unwrap();

    assert_eq!(converted.metric_type, MetricType::Histogram);
}

#[test]
fn test_convert_metric_with_data_points() {
    let metric = OtlpMetric {
        name: "cpu_usage".to_string(),
        description: "CPU usage".to_string(),
        unit: "percent".to_string(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![
                NumberDataPoint {
                    time_unix_nano: 1000000000,
                    value: Some(NumberValue::AsDouble(42.5)),
                    attributes: vec![],
                    ..Default::default()
                },
                NumberDataPoint {
                    time_unix_nano: 2000000000,
                    value: Some(NumberValue::AsInt(50)),
                    attributes: vec![],
                    ..Default::default()
                },
            ],
        })),
        ..Default::default()
    };

    let (converted_metric, data_points) = convert_otlp_metric_data_points(metric, None).unwrap();

    assert_eq!(converted_metric.name, "cpu_usage");
    assert_eq!(data_points.len(), 2);
    assert_eq!(data_points[0].value, 42.5);
    assert_eq!(data_points[1].value, 50.0); // Int converted to f64
}

#[test]
fn test_convert_metric_data_point_with_attributes() {
    let metric = OtlpMetric {
        name: "requests".to_string(),
        description: "Requests".to_string(),
        unit: "1".to_string(),
        data: Some(Data::Sum(Sum {
            data_points: vec![NumberDataPoint {
                time_unix_nano: 1000000000,
                value: Some(NumberValue::AsInt(100)),
                attributes: vec![
                    KeyValue {
                        key: "method".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::StringValue("GET".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "status".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::IntValue(200)),
                        }),
                    },
                ],
                ..Default::default()
            }],
            aggregation_temporality: 2,
            is_monotonic: true,
        })),
        ..Default::default()
    };

    let (_, data_points) = convert_otlp_metric_data_points(metric, None).unwrap();

    assert_eq!(data_points.len(), 1);
    assert_eq!(data_points[0].attributes.len(), 2);
    assert!(data_points[0].attributes.contains_key("method"));
    assert!(data_points[0].attributes.contains_key("status"));
}

// ============================================================================
// Attribute Conversion Tests
// ============================================================================

#[test]
fn test_convert_string_array_attribute() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1; 16],
        span_id: vec![1; 8],
        name: "test".to_string(),
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![KeyValue {
            key: "tags".to_string(),
            value: Some(AnyValue {
                value: Some(OtlpValue::ArrayValue(
                    opentelemetry_proto::tonic::common::v1::ArrayValue {
                        values: vec![
                            AnyValue {
                                value: Some(OtlpValue::StringValue("tag1".to_string())),
                            },
                            AnyValue {
                                value: Some(OtlpValue::StringValue("tag2".to_string())),
                            },
                        ],
                    },
                )),
            }),
        }],
        ..Default::default()
    };

    let span = convert_otlp_span(otlp_span, None).unwrap();

    assert_eq!(span.attributes.len(), 1);
    if let Some(sequins::models::AttributeValue::StringArray(arr)) = span.attributes.get("tags") {
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], "tag1");
        assert_eq!(arr[1], "tag2");
    } else {
        panic!("Expected StringArray attribute");
    }
}

#[test]
fn test_service_name_extraction_no_resource() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1; 16],
        span_id: vec![1; 8],
        name: "test".to_string(),
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        ..Default::default()
    };

    let span = convert_otlp_span(otlp_span, None).unwrap();

    assert_eq!(span.service_name, "unknown");
}
