//! Malformed input handling tests for ingest-server
//!
//! Tests error handling for invalid OTLP data:
//! - Invalid trace/span IDs (wrong length)
//! - Missing required fields
//! - Invalid timestamps
//! - Invalid enum values
//! - Boundary conditions

use opentelemetry_proto::tonic::{
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::LogRecord,
    metrics::v1::{metric::Data as MetricData, Gauge, Metric, NumberDataPoint},
    resource::v1::Resource,
    trace::v1::Span as OtlpSpan,
};
use sequins::ingest::otlp_conversions::*;

// ============================================================================
// Invalid Trace/Span ID Tests
// ============================================================================

#[test]
fn test_invalid_trace_id_empty() {
    let otlp_span = OtlpSpan {
        trace_id: vec![], // Empty - should be 16 bytes
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    assert!(result.is_err(), "Should reject empty trace ID");
}

#[test]
fn test_invalid_trace_id_wrong_length() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4], // 4 bytes - should be 16 bytes
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    assert!(result.is_err(), "Should reject wrong-length trace ID");
}

#[test]
fn test_invalid_span_id_empty() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![], // Empty - should be 8 bytes
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    assert!(result.is_err(), "Should reject empty span ID");
}

#[test]
fn test_invalid_span_id_wrong_length() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3], // 3 bytes - should be 8 bytes
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    assert!(result.is_err(), "Should reject wrong-length span ID");
}

// ============================================================================
// Missing Required Fields Tests
// ============================================================================

#[test]
fn test_span_with_empty_name() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: String::new(), // Empty name - may be allowed but should handle gracefully
        kind: 1,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    // Empty name is technically allowed by OTLP spec, so this should succeed
    assert!(result.is_ok());
    assert_eq!(result.unwrap().operation_name, "");
}

#[test]
fn test_log_with_empty_body() {
    let otlp_log = LogRecord {
        time_unix_nano: 1000000000,
        observed_time_unix_nano: 1000000000,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: None, // No body
        attributes: vec![],
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![],
        span_id: vec![],
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_log(otlp_log, Some(&resource));
    // Empty body is allowed - should convert to empty string
    assert!(result.is_ok());
    assert_eq!(result.unwrap().body, "");
}

// ============================================================================
// Invalid Timestamp Tests
// ============================================================================

#[test]
fn test_span_with_zero_timestamps() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 0, // Zero timestamp
        end_time_unix_nano: 0,   // Zero timestamp
        attributes: vec![],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    // Zero timestamps are technically valid (epoch), should be allowed
    assert!(result.is_ok());
}

#[test]
fn test_span_with_end_before_start() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 2000000000,
        end_time_unix_nano: 1000000000, // End before start
        attributes: vec![],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    // This should succeed but produce negative duration
    assert!(result.is_ok());
    let span = result.unwrap();
    assert!(span.duration.as_nanos() < 0, "Duration should be negative");
}

// ============================================================================
// Invalid Attribute Value Tests
// ============================================================================

#[test]
fn test_attribute_without_value() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![KeyValue {
            key: "test.key".to_string(),
            value: None, // No value
        }],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    // Should succeed but skip the attribute
    assert!(result.is_ok());
    let span = result.unwrap();
    assert!(
        !span.attributes.contains_key("test.key"),
        "Attribute without value should be skipped"
    );
}

#[test]
fn test_attribute_with_empty_array() {
    let otlp_span = OtlpSpan {
        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
        name: "test".to_string(),
        kind: 1,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![KeyValue {
            key: "test.array".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::ArrayValue(
                    opentelemetry_proto::tonic::common::v1::ArrayValue { values: vec![] },
                )),
            }),
        }],
        events: vec![],
        links: vec![],
        status: None,
        parent_span_id: vec![],
        trace_state: String::new(),
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 0,
        flags: 0,
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_span(otlp_span, Some(&resource));
    // Should succeed and handle empty array
    assert!(result.is_ok());
}

// ============================================================================
// Metric Edge Cases
// ============================================================================

#[test]
fn test_metric_without_data_points() {
    let metric = Metric {
        name: "empty_metric".to_string(),
        description: String::new(),
        unit: String::new(),
        data: Some(MetricData::Gauge(Gauge {
            data_points: vec![],
        })),
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![],
        ..Default::default()
    };
    let result = convert_otlp_metric(metric, Some(&resource));
    // Metric with no data points should succeed and return a Metric struct
    assert!(result.is_ok());
}

#[test]
fn test_metric_data_point_without_value() {
    let metric = Metric {
        name: "test_metric".to_string(),
        description: String::new(),
        unit: String::new(),
        data: Some(MetricData::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![],
                start_time_unix_nano: 1000000000,
                time_unix_nano: 2000000000,
                value: None, // No value
                exemplars: vec![],
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
    // Data point without value - conversion may succeed but metric may have no value
    assert!(result.is_ok());
}
