//! Conversion functions from OTLP protobuf types to Sequins types

use opentelemetry_proto::tonic::{
    common::v1::{any_value::Value as OtlpValue, KeyValue},
    logs::v1::LogRecord,
    metrics::v1::Metric as OtlpMetric,
    resource::v1::Resource,
    trace::v1::Span as OtlpSpan,
};
use sequins_core::models::{
    AttributeValue, Duration, LogEntry, LogId, LogSeverity, Metric, MetricId, MetricType, Span,
    SpanEvent, SpanId, SpanKind, SpanStatus, Timestamp, TraceId,
};
use std::collections::HashMap;

/// Convert OTLP span to Sequins span
pub fn convert_otlp_span(otlp_span: OtlpSpan, resource: Option<&Resource>) -> Result<Span, String> {
    // Convert trace_id and span_id
    let trace_id = TraceId::from_bytes(
        otlp_span
            .trace_id
            .try_into()
            .map_err(|_| "Invalid trace_id length")?,
    );
    let span_id = SpanId::from_bytes(
        otlp_span
            .span_id
            .try_into()
            .map_err(|_| "Invalid span_id length")?,
    );
    let parent_span_id = if otlp_span.parent_span_id.is_empty() {
        None
    } else {
        Some(SpanId::from_bytes(
            otlp_span
                .parent_span_id
                .try_into()
                .map_err(|_| "Invalid parent_span_id length")?,
        ))
    };

    // Convert timestamps and calculate duration
    let start_time = Timestamp::from_nanos(otlp_span.start_time_unix_nano as i64);
    let end_time = Timestamp::from_nanos(otlp_span.end_time_unix_nano as i64);
    let duration = Duration::from_nanos(end_time.as_nanos() - start_time.as_nanos());

    // Convert attributes
    let attributes = convert_attributes(otlp_span.attributes);

    // Extract service name from resource
    let service_name = extract_service_name(resource);

    // Convert span kind
    let span_kind = match otlp_span.kind {
        1 => SpanKind::Internal,
        2 => SpanKind::Server,
        3 => SpanKind::Client,
        4 => SpanKind::Producer,
        5 => SpanKind::Consumer,
        _ => SpanKind::Unspecified,
    };

    // Convert status
    let status = if let Some(s) = otlp_span.status {
        match s.code {
            0 => SpanStatus::Unset,
            1 => SpanStatus::Ok,
            2 => SpanStatus::Error,
            _ => SpanStatus::Unset,
        }
    } else {
        SpanStatus::Unset
    };

    // Convert events
    let events = otlp_span
        .events
        .into_iter()
        .map(|e| SpanEvent {
            timestamp: Timestamp::from_nanos(e.time_unix_nano as i64),
            name: e.name,
            attributes: convert_attributes(e.attributes),
        })
        .collect();

    Ok(Span {
        trace_id,
        span_id,
        parent_span_id,
        service_name,
        operation_name: otlp_span.name,
        start_time,
        end_time,
        duration,
        attributes,
        events,
        status,
        span_kind,
    })
}

/// Convert OTLP log to Sequins log
pub fn convert_otlp_log(
    otlp_log: LogRecord,
    resource: Option<&Resource>,
) -> Result<LogEntry, String> {
    // Generate log ID
    let id = LogId::new();

    // Convert timestamps
    let timestamp = Timestamp::from_nanos(otlp_log.time_unix_nano as i64);
    let observed_timestamp = Timestamp::from_nanos(otlp_log.observed_time_unix_nano as i64);

    // Convert severity to log level
    let severity = match otlp_log.severity_number {
        1..=4 => LogSeverity::Trace,
        5..=8 => LogSeverity::Debug,
        9..=12 => LogSeverity::Info,
        13..=16 => LogSeverity::Warn,
        17..=20 => LogSeverity::Error,
        21..=24 => LogSeverity::Fatal,
        _ => LogSeverity::Info,
    };

    // Extract message body
    let body = otlp_log
        .body
        .and_then(|b| {
            b.value.and_then(|v| {
                if let OtlpValue::StringValue(s) = v {
                    Some(s)
                } else {
                    None
                }
            })
        })
        .unwrap_or_default();

    // Convert attributes
    let attributes = convert_attributes(otlp_log.attributes);

    // Extract service name from resource
    let service_name = extract_service_name(resource);

    // Extract resource attributes
    let resource_attrs = resource
        .map(|r| convert_resource_attributes(&r.attributes))
        .unwrap_or_default();

    // Extract trace_id and span_id if present
    let trace_id = if otlp_log.trace_id.len() == 16 {
        Some(TraceId::from_bytes(
            otlp_log
                .trace_id
                .try_into()
                .map_err(|_| "Invalid trace_id")?,
        ))
    } else {
        None
    };

    let span_id = if otlp_log.span_id.len() == 8 {
        Some(SpanId::from_bytes(
            otlp_log.span_id.try_into().map_err(|_| "Invalid span_id")?,
        ))
    } else {
        None
    };

    Ok(LogEntry {
        id,
        timestamp,
        observed_timestamp,
        service_name,
        severity,
        body,
        attributes,
        trace_id,
        span_id,
        resource: resource_attrs,
    })
}

/// Convert OTLP metric to Sequins metric
///
/// Note: This creates the metric metadata. Actual data points would be handled
/// separately in a full implementation.
pub fn convert_otlp_metric(
    otlp_metric: OtlpMetric,
    resource: Option<&Resource>,
) -> Result<Metric, String> {
    // Generate metric ID
    let id = MetricId::new();

    // Extract service name from resource
    let service_name = extract_service_name(resource);

    // Determine metric type from the data field
    let metric_type = if let Some(data) = &otlp_metric.data {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;

        match data {
            Data::Gauge(_) => MetricType::Gauge,
            Data::Sum(sum) => {
                if sum.is_monotonic {
                    MetricType::Counter
                } else {
                    MetricType::Gauge
                }
            }
            Data::Histogram(_) => MetricType::Histogram,
            Data::ExponentialHistogram(_) => MetricType::Histogram,
            Data::Summary(_) => MetricType::Summary,
        }
    } else {
        MetricType::Gauge
    };

    Ok(Metric {
        id,
        name: otlp_metric.name,
        description: otlp_metric.description,
        unit: otlp_metric.unit,
        metric_type,
        service_name,
    })
}

/// Convert OTLP attributes to HashMap of AttributeValues
fn convert_attributes(otlp_attrs: Vec<KeyValue>) -> HashMap<String, AttributeValue> {
    otlp_attrs
        .into_iter()
        .filter_map(|kv| {
            kv.value.and_then(|v| {
                v.value.map(|val| {
                    let attr_val = match val {
                        OtlpValue::StringValue(s) => AttributeValue::String(s),
                        OtlpValue::BoolValue(b) => AttributeValue::Bool(b),
                        OtlpValue::IntValue(i) => AttributeValue::Int(i),
                        OtlpValue::DoubleValue(d) => AttributeValue::Double(d),
                        OtlpValue::ArrayValue(arr) => {
                            // Try to extract string array (simplified)
                            let strings: Vec<String> = arr
                                .values
                                .into_iter()
                                .filter_map(|v| {
                                    v.value.and_then(|val| {
                                        if let OtlpValue::StringValue(s) = val {
                                            Some(s)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .collect();
                            if !strings.is_empty() {
                                AttributeValue::StringArray(strings)
                            } else {
                                return None;
                            }
                        }
                        _ => return None,
                    };
                    Some((kv.key, attr_val))
                })
            })
        })
        .flatten()
        .collect()
}

/// Convert resource attributes to simple string HashMap
fn convert_resource_attributes(attrs: &[KeyValue]) -> HashMap<String, String> {
    attrs
        .iter()
        .filter_map(|kv| {
            kv.value.as_ref().and_then(|v| {
                v.value.as_ref().and_then(|val| {
                    let str_val = match val {
                        OtlpValue::StringValue(s) => s.clone(),
                        OtlpValue::BoolValue(b) => b.to_string(),
                        OtlpValue::IntValue(i) => i.to_string(),
                        OtlpValue::DoubleValue(d) => d.to_string(),
                        _ => return None,
                    };
                    Some((kv.key.clone(), str_val))
                })
            })
        })
        .collect()
}

/// Extract service name from OTLP resource
fn extract_service_name(resource: Option<&Resource>) -> String {
    resource
        .and_then(|r| {
            r.attributes.iter().find_map(|kv| {
                if kv.key == "service.name" {
                    kv.value.as_ref().and_then(|v| {
                        if let Some(OtlpValue::StringValue(s)) = &v.value {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| "unknown".to_string())
}
