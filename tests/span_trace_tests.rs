//! Additional span and trace model tests
//!
//! Tests edge cases and behaviors not covered in the main unit tests:
//! - Span serialization/deserialization
//! - SpanKind and SpanStatus enumerations
//! - SpanEvent handling
//! - Trace with multiple roots (invalid)
//! - Trace with mismatched trace IDs (invalid)
//! - Trace depth calculations
//! - Trace with orphaned spans

use sequins::models::{
    traces::{Span, SpanEvent, SpanKind, SpanStatus, Trace, TraceStatus},
    AttributeValue, Duration, SpanId, Timestamp, TraceId,
};
use std::collections::HashMap;

// ============================================================================
// SpanKind Tests
// ============================================================================

#[test]
fn test_span_kind_values() {
    // Verify all SpanKind enum values
    let kinds = vec![
        SpanKind::Unspecified,
        SpanKind::Internal,
        SpanKind::Server,
        SpanKind::Client,
        SpanKind::Producer,
        SpanKind::Consumer,
    ];
    assert_eq!(kinds.len(), 6);
}

#[test]
fn test_span_kind_equality() {
    assert_eq!(SpanKind::Server, SpanKind::Server);
    assert_ne!(SpanKind::Server, SpanKind::Client);
    assert_ne!(SpanKind::Producer, SpanKind::Consumer);
}

// ============================================================================
// SpanStatus Tests
// ============================================================================

#[test]
fn test_span_status_values() {
    // Verify all SpanStatus enum values
    let statuses = vec![SpanStatus::Unset, SpanStatus::Ok, SpanStatus::Error];
    assert_eq!(statuses.len(), 3);
}

#[test]
fn test_span_status_equality() {
    assert_eq!(SpanStatus::Ok, SpanStatus::Ok);
    assert_ne!(SpanStatus::Ok, SpanStatus::Error);
    assert_ne!(SpanStatus::Unset, SpanStatus::Error);
}

// ============================================================================
// SpanEvent Tests
// ============================================================================

#[test]
fn test_span_event_creation() {
    let mut attributes = HashMap::new();
    attributes.insert(
        "event.type".to_string(),
        AttributeValue::String("checkpoint".to_string()),
    );

    let event = SpanEvent {
        timestamp: Timestamp::from_secs(1000),
        name: "checkpoint-reached".to_string(),
        attributes,
    };

    assert_eq!(event.name, "checkpoint-reached");
    assert_eq!(event.timestamp, Timestamp::from_secs(1000));
    assert_eq!(event.attributes.len(), 1);
}

#[test]
fn test_span_with_multiple_events() {
    let trace_id = TraceId::from_bytes([1; 16]);
    let span_id = SpanId::from_bytes([1; 8]);

    let events = vec![
        SpanEvent {
            timestamp: Timestamp::from_secs(1000),
            name: "start".to_string(),
            attributes: HashMap::new(),
        },
        SpanEvent {
            timestamp: Timestamp::from_secs(1500),
            name: "middle".to_string(),
            attributes: HashMap::new(),
        },
        SpanEvent {
            timestamp: Timestamp::from_secs(2000),
            name: "end".to_string(),
            attributes: HashMap::new(),
        },
    ];

    let span = Span {
        trace_id,
        span_id,
        parent_span_id: None,
        service_name: "test".to_string(),
        operation_name: "operation".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(2000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events,
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    assert_eq!(span.events.len(), 3);
    assert_eq!(span.events[0].name, "start");
    assert_eq!(span.events[1].name, "middle");
    assert_eq!(span.events[2].name, "end");
}

// ============================================================================
// Span Serialization Tests
// ============================================================================

#[test]
fn test_span_serialization() {
    let trace_id = TraceId::from_bytes([1; 16]);
    let span_id = SpanId::from_bytes([1; 8]);

    let span = Span {
        trace_id,
        span_id,
        parent_span_id: None,
        service_name: "test-service".to_string(),
        operation_name: "test-operation".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(2000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let json = serde_json::to_string(&span).unwrap();
    let deserialized: Span = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.trace_id, span.trace_id);
    assert_eq!(deserialized.span_id, span.span_id);
    assert_eq!(deserialized.service_name, span.service_name);
    assert_eq!(deserialized.operation_name, span.operation_name);
}

// ============================================================================
// Trace with Mismatched IDs Tests
// ============================================================================

#[test]
fn test_trace_from_spans_with_different_trace_ids() {
    let trace_id_1 = TraceId::from_bytes([1; 16]);
    let trace_id_2 = TraceId::from_bytes([2; 16]);

    let span1 = Span {
        trace_id: trace_id_1,
        span_id: SpanId::from_bytes([1; 8]),
        parent_span_id: None,
        service_name: "service".to_string(),
        operation_name: "op1".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(2000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let span2 = Span {
        trace_id: trace_id_2, // Different trace ID
        span_id: SpanId::from_bytes([2; 8]),
        parent_span_id: None,
        service_name: "service".to_string(),
        operation_name: "op2".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(2000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let result = Trace::from_spans(vec![span1, span2]);
    assert!(
        result.is_none(),
        "Should reject spans with different trace IDs"
    );
}

#[test]
fn test_trace_with_multiple_roots() {
    let trace_id = TraceId::from_bytes([1; 16]);

    // Two root spans (no parent) - invalid
    let span1 = Span {
        trace_id,
        span_id: SpanId::from_bytes([1; 8]),
        parent_span_id: None, // Root
        service_name: "service".to_string(),
        operation_name: "op1".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(2000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let span2 = Span {
        trace_id,
        span_id: SpanId::from_bytes([2; 8]),
        parent_span_id: None, // Also root - invalid
        service_name: "service".to_string(),
        operation_name: "op2".to_string(),
        start_time: Timestamp::from_secs(1500),
        end_time: Timestamp::from_secs(2500),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    // from_spans will still succeed but pick the first root
    let result = Trace::from_spans(vec![span1.clone(), span2]);
    assert!(result.is_some());
    let trace = result.unwrap();
    // Should pick first root span
    assert_eq!(trace.root_span_id, span1.span_id);
}

// ============================================================================
// Trace Status Tests
// ============================================================================

#[test]
fn test_trace_status_values() {
    assert_eq!(TraceStatus::Ok, TraceStatus::Ok);
    assert_ne!(TraceStatus::Ok, TraceStatus::Error);
}

#[test]
fn test_trace_has_errors_method() {
    let trace_id = TraceId::from_bytes([1; 16]);
    let span_id = SpanId::from_bytes([1; 8]);

    let span = Span {
        trace_id,
        span_id,
        parent_span_id: None,
        service_name: "service".to_string(),
        operation_name: "op".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(2000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Error,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let trace = Trace::from_spans(vec![span]).unwrap();
    assert!(trace.has_errors());
    assert_eq!(trace.error_count, 1);
    assert_eq!(trace.status, TraceStatus::Error);
}

// ============================================================================
// Trace Children Tests
// ============================================================================

#[test]
fn test_trace_children_of_multiple_levels() {
    let trace_id = TraceId::from_bytes([1; 16]);
    let root_id = SpanId::from_bytes([1; 8]);
    let child1_id = SpanId::from_bytes([2; 8]);
    let child2_id = SpanId::from_bytes([3; 8]);
    let grandchild_id = SpanId::from_bytes([4; 8]);

    let root = Span {
        trace_id,
        span_id: root_id,
        parent_span_id: None,
        service_name: "service".to_string(),
        operation_name: "root".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(5000),
        duration: Duration::from_millis(4000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let child1 = Span {
        trace_id,
        span_id: child1_id,
        parent_span_id: Some(root_id),
        service_name: "service".to_string(),
        operation_name: "child1".to_string(),
        start_time: Timestamp::from_secs(1500),
        end_time: Timestamp::from_secs(2500),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Internal,
        resource: HashMap::new(),
    };

    let child2 = Span {
        trace_id,
        span_id: child2_id,
        parent_span_id: Some(root_id),
        service_name: "service".to_string(),
        operation_name: "child2".to_string(),
        start_time: Timestamp::from_secs(3000),
        end_time: Timestamp::from_secs(4000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Internal,
        resource: HashMap::new(),
    };

    let grandchild = Span {
        trace_id,
        span_id: grandchild_id,
        parent_span_id: Some(child1_id),
        service_name: "service".to_string(),
        operation_name: "grandchild".to_string(),
        start_time: Timestamp::from_secs(1700),
        end_time: Timestamp::from_secs(1900),
        duration: Duration::from_millis(200),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Client,
        resource: HashMap::new(),
    };

    let trace = Trace::from_spans(vec![
        root.clone(),
        child1.clone(),
        child2.clone(),
        grandchild.clone(),
    ])
    .unwrap();

    // Root should have 2 children
    let root_children = trace.children_of(root_id);
    assert_eq!(root_children.len(), 2);
    assert!(root_children.iter().any(|s| s.span_id == child1_id));
    assert!(root_children.iter().any(|s| s.span_id == child2_id));

    // Child1 should have 1 child (grandchild)
    let child1_children = trace.children_of(child1_id);
    assert_eq!(child1_children.len(), 1);
    assert_eq!(child1_children[0].span_id, grandchild_id);

    // Child2 should have 0 children
    let child2_children = trace.children_of(child2_id);
    assert_eq!(child2_children.len(), 0);
}

// ============================================================================
// Trace with Orphaned Spans Tests
// ============================================================================

#[test]
fn test_trace_with_orphaned_spans() {
    let trace_id = TraceId::from_bytes([1; 16]);
    let root_id = SpanId::from_bytes([1; 8]);
    let orphan_id = SpanId::from_bytes([2; 8]);
    let nonexistent_parent = SpanId::from_bytes([99; 8]);

    let root = Span {
        trace_id,
        span_id: root_id,
        parent_span_id: None,
        service_name: "service".to_string(),
        operation_name: "root".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(2000),
        duration: Duration::from_millis(1000),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    // Orphaned span - parent doesn't exist in trace
    let orphan = Span {
        trace_id,
        span_id: orphan_id,
        parent_span_id: Some(nonexistent_parent),
        service_name: "service".to_string(),
        operation_name: "orphan".to_string(),
        start_time: Timestamp::from_secs(1500),
        end_time: Timestamp::from_secs(1700),
        duration: Duration::from_millis(200),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Client,
        resource: HashMap::new(),
    };

    let trace = Trace::from_spans(vec![root, orphan]).unwrap();
    // Orphan span is still included but has no parent
    assert_eq!(trace.spans.len(), 2);
    // children_of for nonexistent parent finds the orphan
    let children = trace.children_of(nonexistent_parent);
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].span_id, orphan_id);
}
