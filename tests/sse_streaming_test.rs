//! SSE streaming tests for RemoteClient
//!
//! These tests verify that RemoteClient correctly implements the cursor-based
//! two-phase query API:
//! - Phase 1: query_* methods return historical data + cursor
//! - Phase 2: subscribe_* methods use cursor to stream live updates via SSE
//!
//! The implementation should match the server's behavior in `crates/server/src/query.rs`.

use futures::StreamExt;
use sequins::client::RemoteClient;
use sequins::{
    models::{
        LogEntry, LogId, LogQuery, LogQueryResult, LogSeverity, Metric, MetricId, MetricQuery,
        MetricQueryResult, MetricType, Profile, ProfileId, ProfileQuery, ProfileQueryResult,
        ProfileType, QueryCursor, QueryTrace, SpanId, Timestamp, TraceId, TraceQuery,
        TraceQueryResult,
    },
    traits::QueryApi,
};
use std::collections::HashMap;
use wiremock::{
    matchers::{method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};

// ============================================================================
// Phase 1: Historical Query Tests (Returns result + cursor)
// ============================================================================

#[tokio::test]
async fn test_query_traces_returns_result_with_cursor() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);

    let traces = vec![QueryTrace {
        trace_id,
        root_span_id: span_id,
        spans: vec![],
        duration: 1000,
        has_error: false,
    }];

    let response = TraceQueryResult {
        traces,
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("POST"))
        .and(path("/api/traces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(1000),
        min_duration: None,
        max_duration: None,
        has_error: None,
        attributes: None,
        limit: Some(100),
        live: false,
    };

    let result = client.query_traces(query).await.unwrap();
    assert_eq!(result.traces.len(), 1);
    assert_eq!(result.traces[0].duration, 1000);
    // Cursor is returned for subscription
    assert!(result.cursor.query_timestamp.as_nanos() > 0);
}

#[tokio::test]
async fn test_query_logs_returns_result_with_cursor() {
    let mock_server = MockServer::start().await;

    let timestamp = Timestamp::from_secs(1000);
    let logs = vec![LogEntry {
        id: LogId::new(),
        timestamp,
        observed_timestamp: timestamp,
        service_name: "test-service".to_string(),
        severity: LogSeverity::Info,
        body: "test log message".to_string(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    }];

    let response = LogQueryResult {
        logs,
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("POST"))
        .and(path("/api/logs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
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

    let result = client.query_logs(query).await.unwrap();
    assert_eq!(result.logs.len(), 1);
    assert_eq!(result.logs[0].body, "test log message");
    assert!(result.cursor.query_timestamp.as_nanos() > 0);
}

#[tokio::test]
async fn test_query_metrics_returns_result_with_cursor() {
    let mock_server = MockServer::start().await;

    let metrics = vec![Metric {
        id: MetricId::new(),
        name: "http_requests".to_string(),
        description: "HTTP request count".to_string(),
        unit: "requests".to_string(),
        metric_type: MetricType::Counter,
        service_name: "api-service".to_string(),
        is_generated: false,
    }];

    let response = MetricQueryResult {
        metrics,
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("POST"))
        .and(path("/api/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = MetricQuery {
        name: Some("http_requests".to_string()),
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        limit: Some(100),
        live: false,
    };

    let result = client.query_metrics(query).await.unwrap();
    assert_eq!(result.metrics.len(), 1);
    assert_eq!(result.metrics[0].name, "http_requests");
    assert!(result.cursor.query_timestamp.as_nanos() > 0);
}

#[tokio::test]
async fn test_query_profiles_returns_result_with_cursor() {
    let mock_server = MockServer::start().await;

    let timestamp = Timestamp::from_secs(1000);
    let profiles = vec![Profile {
        id: ProfileId::new(),
        timestamp,
        service_name: "api-service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "samples".to_string(),
        sample_unit: "count".to_string(),
        data: vec![1, 2, 3, 4, 5],
        trace_id: None,
    }];

    let response = ProfileQueryResult {
        profiles,
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("POST"))
        .and(path("/api/profiles"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = ProfileQuery {
        service: None,
        profile_type: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        trace_id: None,
        limit: Some(100),
        live: false,
    };

    let result = client.query_profiles(query).await.unwrap();
    assert_eq!(result.profiles.len(), 1);
    assert_eq!(result.profiles[0].profile_type, ProfileType::Cpu);
    assert!(result.cursor.query_timestamp.as_nanos() > 0);
}

// ============================================================================
// Phase 2: SSE Subscription Tests (Uses cursor for live streaming)
// ============================================================================

#[tokio::test]
async fn test_subscribe_traces_sse_streaming() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);

    let trace1 = QueryTrace {
        trace_id,
        root_span_id: span_id,
        spans: vec![],
        duration: 1000,
        has_error: false,
    };

    let trace2 = QueryTrace {
        trace_id,
        root_span_id: span_id,
        spans: vec![],
        duration: 2000,
        has_error: true,
    };

    // SSE format: "event: trace\ndata: {json}\n\n"
    let sse_data = format!(
        "event: trace\ndata: {}\n\nevent: trace\ndata: {}\n\n",
        serde_json::to_string(&trace1).unwrap(),
        serde_json::to_string(&trace2).unwrap()
    );

    Mock::given(method("GET"))
        .and(path("/api/traces/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(1000),
        min_duration: None,
        max_duration: None,
        has_error: None,
        attributes: None,
        limit: Some(100),
        live: true, // Not used for subscribe, but kept for completeness
    };

    let cursor = QueryCursor::now().unwrap();
    let mut stream = client.subscribe_traces(query, cursor).await.unwrap();

    // Should receive both traces from the SSE stream
    let first = stream.next().await;
    assert!(first.is_some());
    let first_trace = first.unwrap();
    assert_eq!(first_trace.duration, 1000);
    assert!(!first_trace.has_error);

    let second = stream.next().await;
    assert!(second.is_some());
    let second_trace = second.unwrap();
    assert_eq!(second_trace.duration, 2000);
    assert!(second_trace.has_error);
}

#[tokio::test]
async fn test_subscribe_logs_sse_streaming() {
    let mock_server = MockServer::start().await;

    let timestamp = Timestamp::from_secs(1000);
    let log = LogEntry {
        id: LogId::new(),
        timestamp,
        observed_timestamp: timestamp,
        service_name: "test-service".to_string(),
        severity: LogSeverity::Info,
        body: "test log message".to_string(),
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
        live: true,
    };

    let cursor = QueryCursor::now().unwrap();
    let mut stream = client.subscribe_logs(query, cursor).await.unwrap();

    let result = stream.next().await;
    assert!(result.is_some());
    let returned_log = result.unwrap();
    assert_eq!(returned_log.body, "test log message");
    assert_eq!(returned_log.service_name, "test-service");
}

#[tokio::test]
async fn test_subscribe_metrics_sse_streaming() {
    let mock_server = MockServer::start().await;

    let metric = Metric {
        id: MetricId::new(),
        name: "http_requests".to_string(),
        description: "HTTP request count".to_string(),
        unit: "requests".to_string(),
        metric_type: MetricType::Counter,
        service_name: "api-service".to_string(),
        is_generated: false,
    };

    let sse_data = format!(
        "event: metric\ndata: {}\n\n",
        serde_json::to_string(&metric).unwrap()
    );

    Mock::given(method("GET"))
        .and(path("/api/metrics/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = MetricQuery {
        name: Some("http_requests".to_string()),
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        limit: Some(100),
        live: true,
    };

    let cursor = QueryCursor::now().unwrap();
    let mut stream = client.subscribe_metrics(query, cursor).await.unwrap();

    let result = stream.next().await;
    assert!(result.is_some());
    let returned_metric = result.unwrap();
    assert_eq!(returned_metric.name, "http_requests");
    assert_eq!(returned_metric.metric_type, MetricType::Counter);
}

#[tokio::test]
async fn test_subscribe_profiles_sse_streaming() {
    let mock_server = MockServer::start().await;

    let timestamp = Timestamp::from_secs(1000);
    let profile = Profile {
        id: ProfileId::new(),
        timestamp,
        service_name: "api-service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "samples".to_string(),
        sample_unit: "count".to_string(),
        data: vec![1, 2, 3, 4, 5],
        trace_id: None,
    };

    let sse_data = format!(
        "event: profile\ndata: {}\n\n",
        serde_json::to_string(&profile).unwrap()
    );

    Mock::given(method("GET"))
        .and(path("/api/profiles/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = ProfileQuery {
        service: None,
        profile_type: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        trace_id: None,
        limit: Some(100),
        live: true,
    };

    let cursor = QueryCursor::now().unwrap();
    let mut stream = client.subscribe_profiles(query, cursor).await.unwrap();

    let result = stream.next().await;
    assert!(result.is_some());
    let returned_profile = result.unwrap();
    assert_eq!(returned_profile.profile_type, ProfileType::Cpu);
    assert_eq!(returned_profile.service_name, "api-service");
}

// ============================================================================
// SSE Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_sse_handles_malformed_json() {
    let mock_server = MockServer::start().await;

    // Send invalid JSON in SSE stream
    let sse_data = "event: metric\ndata: {invalid json}\n\n";

    Mock::given(method("GET"))
        .and(path("/api/metrics/subscribe"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "text/event-stream")
                .set_body_string(sse_data),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = MetricQuery {
        name: None,
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        limit: Some(100),
        live: true,
    };

    let cursor = QueryCursor::now().unwrap();
    let mut stream = client.subscribe_metrics(query, cursor).await.unwrap();

    // Malformed JSON should be skipped, stream should end
    let result = stream.next().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_sse_empty_stream() {
    let mock_server = MockServer::start().await;

    // Empty SSE stream
    let sse_data = "";

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
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        severities: None,
        search: None,
        trace_id: None,
        attributes: None,
        limit: Some(100),
        live: true,
    };

    let cursor = QueryCursor::now().unwrap();
    let mut stream = client.subscribe_logs(query, cursor).await.unwrap();

    // Empty stream should immediately end
    let result = stream.next().await;
    assert!(result.is_none());
}
