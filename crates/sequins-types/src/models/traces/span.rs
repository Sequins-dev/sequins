use super::{AttributeValue, SpanLink};
use crate::models::{Duration, SpanId, Timestamp, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Individual span within a trace
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Span {
    /// Trace this span belongs to
    pub trace_id: TraceId,
    /// Unique identifier for this span
    pub span_id: SpanId,
    /// Parent span ID (None for root spans)
    pub parent_span_id: Option<SpanId>,
    /// Operation name (e.g., "GET /api/users")
    pub operation_name: String,
    /// When the operation started
    pub start_time: Timestamp,
    /// When the operation ended
    pub end_time: Timestamp,
    /// How long the operation took
    pub duration: Duration,
    /// Additional attributes attached to this span
    pub attributes: HashMap<String, AttributeValue>,
    /// Events that occurred during this span
    pub events: Vec<SpanEvent>,
    /// Links to other spans
    #[serde(default)]
    pub links: Vec<SpanLink>,
    /// Status code (0=Unset, 1=Ok, 2=Error)
    pub status_code: u8,
    /// Optional status message (error description)
    pub status_message: Option<String>,
    /// Kind of span (0=Unspecified, 1=Internal, 2=Server, 3=Client, 4=Producer, 5=Consumer)
    pub kind: u8,
    /// W3C TraceState header value
    pub trace_state: Option<String>,
    /// Span flags (W3C trace flags, typically 0x01 for sampled)
    pub flags: Option<u32>,
    /// Resource ID reference (FK to ResourceRegistry)
    pub resource_id: u32,
    /// Scope ID reference (FK to ScopeRegistry)
    pub scope_id: u32,
}

/// Span status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SpanStatus {
    /// Status not set
    Unset = 0,
    /// Span completed successfully
    Ok = 1,
    /// Span completed with error
    Error = 2,
}

impl From<u8> for SpanStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => SpanStatus::Unset,
            1 => SpanStatus::Ok,
            2 => SpanStatus::Error,
            _ => SpanStatus::Unset, // Default for unknown
        }
    }
}

impl From<SpanStatus> for u8 {
    fn from(status: SpanStatus) -> Self {
        status as u8
    }
}

/// Span kind from OpenTelemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum SpanKind {
    /// Kind not specified
    Unspecified = 0,
    /// Internal operation within a service
    Internal = 1,
    /// Server-side span (receiving a request)
    Server = 2,
    /// Client-side span (making a request)
    Client = 3,
    /// Producer span (sending a message to a queue)
    Producer = 4,
    /// Consumer span (receiving a message from a queue)
    Consumer = 5,
}

impl From<u8> for SpanKind {
    fn from(value: u8) -> Self {
        match value {
            0 => SpanKind::Unspecified,
            1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => SpanKind::Unspecified, // Default for unknown
        }
    }
}

impl From<SpanKind> for u8 {
    fn from(kind: SpanKind) -> Self {
        kind as u8
    }
}

/// Span event (log within span)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanEvent {
    /// When this event occurred
    pub timestamp: Timestamp,
    /// Name of the event
    pub name: String,
    /// Additional attributes attached to this span
    pub attributes: HashMap<String, AttributeValue>,
}

impl Span {
    /// Check if this is a root span
    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }

    /// Check if this span has an error
    pub fn has_error(&self) -> bool {
        self.status_code == SpanStatus::Error as u8
    }

    /// Get the span status as enum
    pub fn status(&self) -> SpanStatus {
        SpanStatus::from(self.status_code)
    }

    /// Get the span kind as enum
    pub fn span_kind(&self) -> SpanKind {
        SpanKind::from(self.kind)
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
            operation_name: "test-op".to_string(),
            start_time,
            end_time,
            duration,
            attributes: HashMap::new(),
            events: vec![],
            links: vec![],
            status_code: SpanStatus::Ok as u8,
            status_message: None,
            kind: SpanKind::Internal as u8,
            trace_state: None,
            flags: None,
            resource_id: 0,
            scope_id: 0,
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

        span.status_code = SpanStatus::Error as u8;
        assert!(span.has_error());
    }

    #[test]
    fn test_span_enum_conversions() {
        let trace_id = create_test_trace_id();
        let span_id = create_test_span_id(1);

        let mut span = create_test_span(trace_id, span_id, None, 1000, 100);
        span.kind = SpanKind::Server as u8;
        span.status_code = SpanStatus::Ok as u8;

        assert_eq!(span.span_kind(), SpanKind::Server);
        assert_eq!(span.status(), SpanStatus::Ok);
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

    #[test]
    fn test_span_status_from_unknown_u8() {
        // 255 is not a valid SpanStatus — should fall back to Unset
        let status = SpanStatus::from(255u8);
        assert_eq!(status, SpanStatus::Unset);
    }

    #[test]
    fn test_span_kind_from_unknown_u8() {
        // 99 is not a valid SpanKind — should fall back to Unspecified
        let kind = SpanKind::from(99u8);
        assert_eq!(kind, SpanKind::Unspecified);
    }
}
