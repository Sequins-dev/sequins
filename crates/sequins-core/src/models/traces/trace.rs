use super::{Span, SpanStatus};
use crate::models::{Duration, SpanId, Timestamp, TraceId};

/// Complete trace with all spans
#[derive(Debug, Clone, PartialEq)]
pub struct Trace {
    pub trace_id: TraceId,
    pub service_name: String,
    pub root_span_id: SpanId,
    pub spans: Vec<Span>,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub duration: Duration,
    pub status: TraceStatus,
    pub error_count: u32,
}

/// Trace status (overall)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceStatus {
    Ok,
    Error,
}

impl Trace {
    /// Check if trace has any errors
    pub fn has_errors(&self) -> bool {
        self.error_count > 0
    }

    /// Get root span
    pub fn root_span(&self) -> Option<&Span> {
        self.spans.iter().find(|s| s.span_id == self.root_span_id)
    }

    /// Get children of a span
    pub fn children_of(&self, span_id: SpanId) -> Vec<&Span> {
        self.spans
            .iter()
            .filter(|s| s.parent_span_id == Some(span_id))
            .collect()
    }

    /// Build trace from spans (calculates derived fields)
    pub fn from_spans(spans: Vec<Span>) -> Option<Self> {
        if spans.is_empty() {
            return None;
        }

        // All spans should have the same trace_id
        let trace_id = spans[0].trace_id;
        if !spans.iter().all(|s| s.trace_id == trace_id) {
            return None;
        }

        // Find root span (no parent)
        let root_span = spans.iter().find(|s| s.parent_span_id.is_none())?;

        // Calculate trace bounds
        let start_time = spans.iter().map(|s| s.start_time).min()?;
        let end_time = spans.iter().map(|s| s.end_time).max()?;
        let duration = end_time.duration_since(start_time);

        // Determine status and error count
        let error_count = spans
            .iter()
            .filter(|s| s.status == SpanStatus::Error)
            .count() as u32;
        let status = if error_count > 0 {
            TraceStatus::Error
        } else {
            TraceStatus::Ok
        };

        // Service name from root span
        let service_name = root_span.service_name.clone();

        Some(Trace {
            trace_id,
            service_name,
            root_span_id: root_span.span_id,
            spans,
            start_time,
            end_time,
            duration,
            status,
            error_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::traces::SpanKind;
    use std::collections::HashMap;

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
    fn test_trace_from_spans() {
        let trace_id = create_test_trace_id();
        let root_span_id = create_test_span_id(1);
        let child_span_id = create_test_span_id(2);

        let root = create_test_span(trace_id, root_span_id, None, 1000, 100);
        let child = create_test_span(trace_id, child_span_id, Some(root_span_id), 1010, 50);

        let trace = Trace::from_spans(vec![root, child]).unwrap();

        assert_eq!(trace.trace_id, trace_id);
        assert_eq!(trace.root_span_id, root_span_id);
        assert_eq!(trace.spans.len(), 2);
        assert_eq!(trace.service_name, "test-service");
        assert_eq!(trace.status, TraceStatus::Ok);
        assert_eq!(trace.error_count, 0);
    }

    #[test]
    fn test_trace_from_spans_with_errors() {
        let trace_id = create_test_trace_id();
        let root_span_id = create_test_span_id(1);
        let child_span_id = create_test_span_id(2);

        let root = create_test_span(trace_id, root_span_id, None, 1000, 100);
        let mut child = create_test_span(trace_id, child_span_id, Some(root_span_id), 1010, 50);
        child.status = SpanStatus::Error;

        let trace = Trace::from_spans(vec![root, child]).unwrap();

        assert_eq!(trace.status, TraceStatus::Error);
        assert_eq!(trace.error_count, 1);
        assert!(trace.has_errors());
    }

    #[test]
    fn test_trace_from_empty_spans() {
        let result = Trace::from_spans(vec![]);
        assert!(result.is_none());
    }

    #[test]
    fn test_trace_root_span() {
        let trace_id = create_test_trace_id();
        let root_span_id = create_test_span_id(1);
        let child_span_id = create_test_span_id(2);

        let root = create_test_span(trace_id, root_span_id, None, 1000, 100);
        let child = create_test_span(trace_id, child_span_id, Some(root_span_id), 1010, 50);

        let trace = Trace::from_spans(vec![root.clone(), child]).unwrap();

        let found_root = trace.root_span().unwrap();
        assert_eq!(found_root.span_id, root.span_id);
    }

    #[test]
    fn test_trace_children_of() {
        let trace_id = create_test_trace_id();
        let root_span_id = create_test_span_id(1);
        let child1_span_id = create_test_span_id(2);
        let child2_span_id = create_test_span_id(3);

        let root = create_test_span(trace_id, root_span_id, None, 1000, 100);
        let child1 = create_test_span(trace_id, child1_span_id, Some(root_span_id), 1010, 30);
        let child2 = create_test_span(trace_id, child2_span_id, Some(root_span_id), 1050, 40);

        let trace = Trace::from_spans(vec![root, child1, child2]).unwrap();

        let children = trace.children_of(root_span_id);
        assert_eq!(children.len(), 2);

        let no_children = trace.children_of(child1_span_id);
        assert_eq!(no_children.len(), 0);
    }
}
