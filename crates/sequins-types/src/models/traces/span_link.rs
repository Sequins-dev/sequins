use super::AttributeValue;
use crate::models::{SpanId, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Link from a span to another span (potentially in a different trace)
///
/// SpanLinks allow modeling relationships between spans that are not simple parent-child.
/// For example, a batch processor might link to all the spans it processed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanLink {
    /// Trace ID of the linked span
    pub trace_id: TraceId,
    /// Span ID of the linked span
    pub span_id: SpanId,
    /// TraceState from the linked span (W3C trace context)
    pub trace_state: Option<String>,
    /// Additional attributes attached to this link
    pub attributes: HashMap<String, AttributeValue>,
}

impl SpanLink {
    /// Create a new span link
    pub fn new(trace_id: TraceId, span_id: SpanId) -> Self {
        Self {
            trace_id,
            span_id,
            trace_state: None,
            attributes: HashMap::new(),
        }
    }

    /// Create a span link with trace state
    pub fn with_trace_state(trace_id: TraceId, span_id: SpanId, trace_state: String) -> Self {
        Self {
            trace_id,
            span_id,
            trace_state: Some(trace_state),
            attributes: HashMap::new(),
        }
    }

    /// Add attributes to the link
    pub fn with_attributes(mut self, attributes: HashMap<String, AttributeValue>) -> Self {
        self.attributes = attributes;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_link_creation() {
        let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);

        let link = SpanLink::new(trace_id, span_id);
        assert_eq!(link.trace_id, trace_id);
        assert_eq!(link.span_id, span_id);
        assert_eq!(link.trace_state, None);
        assert!(link.attributes.is_empty());
    }

    #[test]
    fn test_span_link_with_trace_state() {
        let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);

        let link = SpanLink::with_trace_state(trace_id, span_id, "state=value".to_string());
        assert_eq!(link.trace_state, Some("state=value".to_string()));
    }

    #[test]
    fn test_span_link_with_attributes() {
        let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);

        let mut attrs = HashMap::new();
        attrs.insert(
            "link.type".to_string(),
            AttributeValue::String("follows_from".to_string()),
        );

        let link = SpanLink::new(trace_id, span_id).with_attributes(attrs.clone());
        assert_eq!(link.attributes, attrs);
    }
}
