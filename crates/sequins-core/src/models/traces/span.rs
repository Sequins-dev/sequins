use super::AttributeValue;
use crate::models::{Duration, SpanId, Timestamp, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Individual span within a trace
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: Option<SpanId>,
    pub service_name: String,
    pub operation_name: String,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub duration: Duration,
    pub attributes: HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub status: SpanStatus,
    pub span_kind: SpanKind,
}

/// Span status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
}

/// Span kind from OpenTelemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

/// Span event (log within span)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanEvent {
    pub timestamp: Timestamp,
    pub name: String,
    pub attributes: HashMap<String, AttributeValue>,
}

impl Span {
    /// Check if this is a root span
    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }

    /// Check if this span has an error
    pub fn has_error(&self) -> bool {
        self.status == SpanStatus::Error
    }

    /// Get attribute value by key
    pub fn get_attribute(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_trace_id() -> TraceId {
        TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    }

    fn create_test_span_id(n: u8) -> SpanId {
        SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, n])
    }

    fn create_test_span(
        trace_id: TraceId,
        span_id: SpanId,
        parent: Option<SpanId>,
        start_secs: i64,
        duration_ms: i64,
    ) -> Span {
        let start_time = Timestamp::from_secs(start_secs);
        let duration = Duration::from_millis(duration_ms);
        let end_time = start_time + duration;

        Span {
            trace_id,
            span_id,
            parent_span_id: parent,
            service_name: "test-service".to_string(),
            operation_name: "test-op".to_string(),
            start_time,
            end_time,
            duration,
            attributes: HashMap::new(),
            events: vec![],
            status: SpanStatus::Ok,
            span_kind: SpanKind::Internal,
        }
    }

    #[test]
    fn test_span_is_root() {
        let trace_id = create_test_trace_id();
        let span_id = create_test_span_id(1);

        let root_span = create_test_span(trace_id, span_id, None, 1000, 100);
        assert!(root_span.is_root());

        let child_span = create_test_span(trace_id, span_id, Some(span_id), 1000, 50);
        assert!(!child_span.is_root());
    }

    #[test]
    fn test_span_has_error() {
        let trace_id = create_test_trace_id();
        let span_id = create_test_span_id(1);

        let mut span = create_test_span(trace_id, span_id, None, 1000, 100);
        assert!(!span.has_error());

        span.status = SpanStatus::Error;
        assert!(span.has_error());
    }

    #[test]
    fn test_span_attributes() {
        let trace_id = create_test_trace_id();
        let span_id = create_test_span_id(1);

        let mut span = create_test_span(trace_id, span_id, None, 1000, 100);
        span.attributes.insert(
            "http.method".to_string(),
            AttributeValue::String("GET".to_string()),
        );
        span.attributes
            .insert("http.status_code".to_string(), AttributeValue::Int(200));

        assert!(matches!(
            span.get_attribute("http.method"),
            Some(AttributeValue::String(s)) if s == "GET"
        ));
        assert!(matches!(
            span.get_attribute("http.status_code"),
            Some(AttributeValue::Int(200))
        ));
        assert!(span.get_attribute("nonexistent").is_none());
    }
}
