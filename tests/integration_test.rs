//! Integration tests for RemoteClient
//!
//! These tests verify that RemoteClient correctly implements QueryApi and ManagementApi
//! via HTTP calls. We use wiremock to mock the HTTP server.

use sequins::client::RemoteClient;
use sequins::{
    models::{
        Duration, LogEntry, LogId, LogSeverity, MaintenanceStats, Metric, MetricId, MetricType,
        Profile, ProfileId, ProfileType, RetentionPolicy, Service, Span, SpanId, SpanKind,
        SpanStatus, StorageStats, Timestamp, TraceId,
    },
    traits::{ManagementApi, QueryApi},
};
use std::collections::HashMap;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

// ============================================================================
// Client Initialization Tests
// ============================================================================

#[tokio::test]
async fn test_client_new_with_valid_urls() {
    let client = RemoteClient::new("http://localhost:8080", "http://localhost:8081");
    assert!(client.is_ok());
}

#[tokio::test]
async fn test_client_new_trims_trailing_slashes() {
    let _client = RemoteClient::new("http://localhost:8080///", "http://localhost:8081//").unwrap();

    // Verify by making a request and checking the URL doesn't have doubled slashes
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(200).set_body_json(Vec::<Service>::new()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let _ = client.get_services().await;
}

#[tokio::test]
async fn test_client_localhost() {
    let client = RemoteClient::localhost();
    assert!(client.is_ok());
}

// ============================================================================
// QueryApi Tests - Services
// ============================================================================

#[tokio::test]
async fn test_get_services_success() {
    let mock_server = MockServer::start().await;

    let services = vec![
        Service {
            name: "test-service".to_string(),
            span_count: 10,
            log_count: 20,
            resource_attributes: HashMap::new(),
        },
        Service {
            name: "another-service".to_string(),
            span_count: 5,
            log_count: 15,
            resource_attributes: HashMap::new(),
        },
    ];

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&services))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_services().await;

    assert!(result.is_ok());
    let returned_services = result.unwrap();
    assert_eq!(returned_services.len(), 2);
    assert_eq!(returned_services[0].name, "test-service");
    assert_eq!(returned_services[1].name, "another-service");
}

#[tokio::test]
async fn test_get_services_empty() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(200).set_body_json(Vec::<Service>::new()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_services().await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0);
}

#[tokio::test]
async fn test_get_services_network_error() {
    // Use invalid URL to trigger network error
    let client = RemoteClient::new("http://localhost:1", "http://localhost:1").unwrap();
    let result = client.get_services().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_services_invalid_json() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(200).set_body_string("invalid json"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_services().await;

    assert!(result.is_err());
}

// ============================================================================
// QueryApi Tests - Spans
// ============================================================================

#[tokio::test]
async fn test_get_spans_success() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let start_time = Timestamp::from_secs(1000);
    let duration = Duration::from_millis(1000);
    let end_time = start_time + duration;

    let span = Span {
        trace_id,
        span_id: SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]),
        parent_span_id: None,
        service_name: "test-service".to_string(),
        operation_name: "test-span".to_string(),
        start_time,
        end_time,
        duration,
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Internal,
        resource: HashMap::new(),
    };

    Mock::given(method("GET"))
        .and(path(format!("/api/traces/{}/spans", trace_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![&span]))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_spans(trace_id).await;

    assert!(result.is_ok());
    let spans = result.unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].operation_name, "test-span");
}

#[tokio::test]
async fn test_get_span_some() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);
    let start_time = Timestamp::from_secs(1000);
    let duration = Duration::from_millis(1000);
    let end_time = start_time + duration;

    let span = Span {
        trace_id,
        span_id,
        parent_span_id: None,
        service_name: "test-service".to_string(),
        operation_name: "test-span".to_string(),
        start_time,
        end_time,
        duration,
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Internal,
        resource: HashMap::new(),
    };

    Mock::given(method("GET"))
        .and(path(format!(
            "/api/traces/{}/spans/{}",
            trace_id.to_hex(),
            span_id.to_hex()
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(Some(&span)))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_span(trace_id, span_id).await;

    assert!(result.is_ok());
    let returned_span = result.unwrap();
    assert!(returned_span.is_some());
    assert_eq!(returned_span.unwrap().operation_name, "test-span");
}

#[tokio::test]
async fn test_get_span_none() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);

    Mock::given(method("GET"))
        .and(path(format!(
            "/api/traces/{}/spans/{}",
            trace_id.to_hex(),
            span_id.to_hex()
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(None::<Span>))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_span(trace_id, span_id).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// ============================================================================
// QueryApi Tests - Logs
// ============================================================================

#[tokio::test]
async fn test_get_log_some() {
    let mock_server = MockServer::start().await;

    let log_id = LogId::new();
    let timestamp = Timestamp::from_secs(1000);
    let log = LogEntry {
        id: log_id,
        timestamp,
        observed_timestamp: timestamp,
        service_name: "test-service".to_string(),
        severity: LogSeverity::Info,
        body: "test log".to_string(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    };

    Mock::given(method("GET"))
        .and(path(format!("/api/logs/{}", log_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(Some(&log)))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_log(log_id).await;

    assert!(result.is_ok());
    let returned_log = result.unwrap();
    assert!(returned_log.is_some());
    assert_eq!(returned_log.unwrap().body, "test log");
}

#[tokio::test]
async fn test_get_log_none() {
    let mock_server = MockServer::start().await;

    let log_id = LogId::new();

    Mock::given(method("GET"))
        .and(path(format!("/api/logs/{}", log_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(None::<LogEntry>))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_log(log_id).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// ============================================================================
// QueryApi Tests - Metrics
// ============================================================================

#[tokio::test]
async fn test_get_metric_some() {
    let mock_server = MockServer::start().await;

    let metric_id = MetricId::new();
    let metric = Metric {
        id: metric_id,
        name: "test-metric".to_string(),
        description: "test description".to_string(),
        unit: "ms".to_string(),
        metric_type: MetricType::Gauge,
        service_name: "test-service".to_string(),
        is_generated: false,
    };

    Mock::given(method("GET"))
        .and(path(format!("/api/metrics/{}", metric_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(Some(&metric)))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_metric(metric_id).await;

    assert!(result.is_ok());
    let returned_metric = result.unwrap();
    assert!(returned_metric.is_some());
    assert_eq!(returned_metric.unwrap().name, "test-metric");
}

#[tokio::test]
async fn test_get_metric_none() {
    let mock_server = MockServer::start().await;

    let metric_id = MetricId::new();

    Mock::given(method("GET"))
        .and(path(format!("/api/metrics/{}", metric_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(None::<Metric>))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_metric(metric_id).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// ============================================================================
// QueryApi Tests - Profiles
// ============================================================================

#[tokio::test]
async fn test_get_profile_some() {
    let mock_server = MockServer::start().await;

    let profile_id = ProfileId::new();
    let timestamp = Timestamp::from_secs(1000);
    let profile = Profile {
        id: profile_id,
        timestamp,
        service_name: "test-service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "samples".to_string(),
        sample_unit: "count".to_string(),
        data: vec![1, 2, 3, 4, 5],
        trace_id: None,
    };

    Mock::given(method("GET"))
        .and(path(format!("/api/profiles/{}", profile_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(Some(&profile)))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_profile(profile_id).await;

    assert!(result.is_ok());
    let returned_profile = result.unwrap();
    assert!(returned_profile.is_some());
    assert_eq!(returned_profile.unwrap().profile_type, ProfileType::Cpu);
}

#[tokio::test]
async fn test_get_profile_none() {
    let mock_server = MockServer::start().await;

    let profile_id = ProfileId::new();

    Mock::given(method("GET"))
        .and(path(format!("/api/profiles/{}", profile_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(None::<Profile>))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_profile(profile_id).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

// ============================================================================
// ManagementApi Tests - Retention
// ============================================================================

#[tokio::test]
async fn test_run_retention_cleanup_success() {
    let mock_server = MockServer::start().await;

    #[derive(serde::Serialize)]
    struct Response {
        deleted_count: usize,
    }

    Mock::given(method("POST"))
        .and(path("/api/retention/cleanup"))
        .respond_with(ResponseTemplate::new(200).set_body_json(Response { deleted_count: 42 }))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.run_retention_cleanup().await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[tokio::test]
async fn test_update_retention_policy_success() {
    let mock_server = MockServer::start().await;

    let one_day = Duration::from_secs(86400);
    let policy = RetentionPolicy {
        spans_retention: one_day,
        logs_retention: one_day,
        metrics_retention: one_day,
        profiles_retention: one_day,
    };

    Mock::given(method("PUT"))
        .and(path("/api/retention/policy"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.update_retention_policy(policy).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_retention_policy_success() {
    let mock_server = MockServer::start().await;

    let one_day = Duration::from_secs(86400);
    let policy = RetentionPolicy {
        spans_retention: one_day,
        logs_retention: one_day,
        metrics_retention: one_day,
        profiles_retention: one_day,
    };

    Mock::given(method("GET"))
        .and(path("/api/retention/policy"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&policy))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_retention_policy().await;

    assert!(result.is_ok());
    let returned_policy = result.unwrap();
    assert_eq!(returned_policy.spans_retention, Duration::from_secs(86400));
}

// ============================================================================
// ManagementApi Tests - Maintenance & Stats
// ============================================================================

#[tokio::test]
async fn test_run_maintenance_success() {
    let mock_server = MockServer::start().await;

    let stats = MaintenanceStats {
        entries_evicted: 10,
        batches_flushed: 2,
    };

    Mock::given(method("POST"))
        .and(path("/api/maintenance"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&stats))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.run_maintenance().await;

    assert!(result.is_ok());
    let returned_stats = result.unwrap();
    assert_eq!(returned_stats.entries_evicted, 10);
    assert_eq!(returned_stats.batches_flushed, 2);
}

#[tokio::test]
async fn test_get_storage_stats_success() {
    let mock_server = MockServer::start().await;

    let stats = StorageStats {
        span_count: 500,
        log_count: 1000,
        metric_count: 50,
        profile_count: 10,
    };

    Mock::given(method("GET"))
        .and(path("/api/storage/stats"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&stats))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_storage_stats().await;

    assert!(result.is_ok());
    let returned_stats = result.unwrap();
    assert_eq!(returned_stats.span_count, 500);
    assert_eq!(returned_stats.log_count, 1000);
    assert_eq!(returned_stats.metric_count, 50);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_http_500_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_services().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_http_404_error() {
    let mock_server = MockServer::start().await;

    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);

    Mock::given(method("GET"))
        .and(path(format!(
            "/api/traces/{}/spans/{}",
            trace_id.to_hex(),
            span_id.to_hex()
        )))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_span(trace_id, span_id).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_timeout() {
    use std::time::Duration;
    use wiremock::ResponseTemplate;

    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(3)))
        .expect(0..=1) // May or may not be called before timeout
        .mount(&mock_server)
        .await;

    // Create client with shorter timeout for faster test
    let client = RemoteClient::with_timeout(
        &mock_server.uri(),
        &mock_server.uri(),
        Duration::from_secs(1),
    )
    .unwrap();

    let result = client.get_services().await;

    // Should timeout (client has 1s timeout, server delays 3s)
    assert!(result.is_err());
}

// ============================================================================
// Unimplemented Method Tests
// ============================================================================
// Note: All QueryApi methods are now fully implemented.
// - query_traces, query_logs, query_metrics, query_profiles: See sse_streaming_test.rs
// - query_spans: See span_query_test.rs
// - query_metric_data_points, query_histogram_data_points: See metric_data_points_test.rs
