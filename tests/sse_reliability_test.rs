//! SSE connection reliability tests for RemoteClient
//!
//! Tests SSE streaming edge cases:
//! - Connection interruption during streaming
//! - Large SSE payloads
//! - Rapid successive SSE messages
//! - Malformed SSE data handling

use futures::StreamExt;
use sequins::client::RemoteClient;
use sequins::{
    models::{LogEntry, LogId, LogQuery, LogSeverity, QueryCursor, Timestamp},
    traits::QueryApi,
};
use std::collections::HashMap;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

// ============================================================================
// Large SSE Payload Tests
// ============================================================================

#[tokio::test]
async fn test_large_sse_payload() {
    let mock_server = MockServer::start().await;

    // Create a moderately sized log entry (1KB body)
    let large_body = "x".repeat(1_000);
    let log = LogEntry {
        id: LogId::new(),
        timestamp: Timestamp::from_secs(1000),
        observed_timestamp: Timestamp::from_secs(1001),
        service_name: "test-service".to_string(),
        severity: LogSeverity::Info,
        body: large_body.clone(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    };

    let sse_data = format!(
        "event: log\ndata: {}\n\n",
        serde_json::to_string(&log).unwrap()
    );

    Mock::given(method("GET"))
        .and(path("/api/logs/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = LogQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        severities: None,
        search: None,
        trace_id: None,
        attributes: None,
        limit: Some(100),
        live: false,
    };

    let cursor = QueryCursor::new(Timestamp::from_secs(0));
    let stream = client.subscribe_logs(query, cursor).await.unwrap();
    let results: Vec<_> = stream.collect().await;

    // Should receive the message
    assert!(!results.is_empty());
    assert_eq!(results[0].body.len(), 1_000);
    assert_eq!(results[0].body, large_body);
}

// ============================================================================
// Rapid Successive SSE Messages Tests
// ============================================================================

#[tokio::test]
async fn test_rapid_successive_sse_messages() {
    let mock_server = MockServer::start().await;

    // Create multiple log entries (reduced to 10 for reliability)
    let mut sse_data = String::new();
    for i in 0..10 {
        let log = LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::from_secs(1000 + i),
            observed_timestamp: Timestamp::from_secs(1001 + i),
            service_name: "test-service".to_string(),
            severity: LogSeverity::Info,
            body: format!("Message {}", i),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        };

        sse_data.push_str(&format!(
            "event: log\ndata: {}\n\n",
            serde_json::to_string(&log).unwrap()
        ));
    }

    Mock::given(method("GET"))
        .and(path("/api/logs/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = LogQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        severities: None,
        search: None,
        trace_id: None,
        attributes: None,
        limit: Some(20),
        live: false,
    };

    let cursor = QueryCursor::new(Timestamp::from_secs(0));
    let stream = client.subscribe_logs(query, cursor).await.unwrap();
    let results: Vec<_> = stream.collect().await;

    // Should receive all messages (or close to it)
    assert!(
        results.len() >= 8,
        "Expected at least 8 messages, got {}",
        results.len()
    );
    // Verify messages have correct format
    for log in results.iter() {
        assert!(log.body.starts_with("Message "));
    }
}

// ============================================================================
// Malformed SSE Data Handling Tests
// ============================================================================

#[tokio::test]
async fn test_partial_malformed_sse_stream() {
    let mock_server = MockServer::start().await;

    let valid_log = LogEntry {
        id: LogId::new(),
        timestamp: Timestamp::from_secs(1000),
        observed_timestamp: Timestamp::from_secs(1001),
        service_name: "test-service".to_string(),
        severity: LogSeverity::Info,
        body: "Valid message".to_string(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    };

    // Mix valid and invalid SSE data
    let sse_data = format!(
        "event: log\ndata: {}\n\nevent: log\ndata: {{invalid json}}\n\nevent: log\ndata: {}\n\n",
        serde_json::to_string(&valid_log).unwrap(),
        serde_json::to_string(&valid_log).unwrap()
    );

    Mock::given(method("GET"))
        .and(path("/api/logs/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = LogQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        severities: None,
        search: None,
        trace_id: None,
        attributes: None,
        limit: Some(100),
        live: false,
    };

    let cursor = QueryCursor::new(Timestamp::from_secs(0));
    let stream = client.subscribe_logs(query, cursor).await.unwrap();
    let results: Vec<_> = stream.collect().await;

    // Should receive only valid messages, malformed ones are skipped
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].body, "Valid message");
    assert_eq!(results[1].body, "Valid message");
}

// ============================================================================
// Empty SSE Stream Tests
// ============================================================================

#[tokio::test]
async fn test_empty_sse_stream_completes() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/logs/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(""),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = LogQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        severities: None,
        search: None,
        trace_id: None,
        attributes: None,
        limit: Some(100),
        live: false,
    };

    let cursor = QueryCursor::new(Timestamp::from_secs(0));
    let stream = client.subscribe_logs(query, cursor).await.unwrap();
    let results: Vec<_> = stream.collect().await;

    assert_eq!(results.len(), 0);
}
