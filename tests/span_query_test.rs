//! Tests for span query methods
//!
//! These tests verify that RemoteClient correctly implements span query methods.
//! Note: Server endpoints for span queries are not yet implemented, but client
//! is prepared to use them when available.

use futures::StreamExt;
use sequins::client::RemoteClient;
use sequins::{
    models::{
        Duration, QueryCursor, Span, SpanId, SpanKind, SpanQuery, SpanStatus, Timestamp, TraceId,
    },
    traits::QueryApi,
};
use std::collections::HashMap;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

// ============================================================================
// Span Query Tests - Batch Mode
// ============================================================================

#[tokio::test]
async fn test_query_spans_batch_mode() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let start_time = Timestamp::from_secs(1000);
    let duration = Duration::from_millis(100);

    let span = Span {
        trace_id,
        span_id: SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]),
        parent_span_id: None,
        service_name: "test-service".to_string(),
        operation_name: "test-op".to_string(),
        start_time,
        end_time: start_time + duration,
        duration,
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let spans = vec![span];
    let result = sequins::models::SpanQueryResult {
        spans,
        cursor: QueryCursor::new(Timestamp::from_secs(0)),
    };

    Mock::given(method("POST"))
        .and(path("/api/spans"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&result))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = SpanQuery {
        trace_id: Some(trace_id),
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        attributes: None,
        limit: Some(100),
        live: false,
    };

    let result = client.query_spans(query).await.unwrap();
    let results = result.spans;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].operation_name, "test-op");
}

// ============================================================================
// Span Query Tests - SSE Streaming Mode
// ============================================================================

#[tokio::test]
async fn test_query_spans_sse_streaming() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let start_time = Timestamp::from_secs(1000);
    let duration = Duration::from_millis(100);

    let span = Span {
        trace_id,
        span_id: SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]),
        parent_span_id: None,
        service_name: "test-service".to_string(),
        operation_name: "test-op".to_string(),
        start_time,
        end_time: start_time + duration,
        duration,
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
        resource: HashMap::new(),
    };

    let sse_data = format!(
        "event: span\ndata: {}\n\n",
        serde_json::to_string(&span).unwrap()
    );

    Mock::given(method("GET"))
        .and(path("/api/spans/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = SpanQuery {
        trace_id: Some(trace_id),
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        attributes: None,
        limit: Some(100),
        live: false,
    };

    let cursor = QueryCursor::new(Timestamp::from_secs(0));
    let mut stream = client.subscribe_spans(query, cursor).await.unwrap();
    let result = stream.next().await;

    assert!(result.is_some());
    let returned_span = result.unwrap();
    assert_eq!(returned_span.operation_name, "test-op");
    assert_eq!(returned_span.service_name, "test-service");
}

#[tokio::test]
async fn test_query_spans_empty_result() {
    let mock_server = MockServer::start().await;

    let spans: Vec<Span> = vec![];
    let result = sequins::models::SpanQueryResult {
        spans,
        cursor: QueryCursor::new(Timestamp::from_secs(0)),
    };

    Mock::given(method("POST"))
        .and(path("/api/spans"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&result))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = SpanQuery {
        trace_id: None,
        service: Some("nonexistent-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        attributes: None,
        limit: Some(100),
        live: false,
    };

    let result = client.query_spans(query).await.unwrap();
    let results = result.spans;

    assert_eq!(results.len(), 0);
}
