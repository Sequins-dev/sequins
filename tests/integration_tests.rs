//! Integration tests for server concurrency, CORS, and lifecycle
//!
//! These tests verify that the servers handle concurrent requests correctly,
//! CORS headers are set properly, and server lifecycle works as expected.

use reqwest::StatusCode;
use sequins::server::{ManagementServer, QueryServer};
use sequins::{
    error::Result,
    models::{
        HistogramDataPointQueryResult, LogQueryResult, MaintenanceStats, MetricDataPoint,
        MetricDataPointQueryResult, MetricQueryResult, ProfileQueryResult, QueryCursor, QueryTrace,
        RetentionPolicy, Service, SpanQueryResult, StorageStats, Timestamp, TraceQuery,
        TraceQueryResult,
    },
    traits::{ManagementApi, QueryApi},
};
use serial_test::serial;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Mock QueryApi that tracks concurrent request count
#[derive(Clone)]
struct ConcurrentMockQueryApi {
    concurrent_requests: Arc<AtomicUsize>,
    total_requests: Arc<AtomicUsize>,
}

impl ConcurrentMockQueryApi {
    fn new() -> Self {
        Self {
            concurrent_requests: Arc::new(AtomicUsize::new(0)),
            total_requests: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn get_total_requests(&self) -> usize {
        self.total_requests.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl QueryApi for ConcurrentMockQueryApi {
    async fn get_services(&self) -> Result<Vec<Service>> {
        // Track concurrent requests
        self.concurrent_requests.fetch_add(1, Ordering::SeqCst);
        self.total_requests.fetch_add(1, Ordering::SeqCst);

        // Simulate some work
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        self.concurrent_requests.fetch_sub(1, Ordering::SeqCst);

        Ok(vec![Service {
            name: "test-service".to_string(),
            span_count: 100,
            log_count: 50,
            resource_attributes: HashMap::new(),
        }])
    }

    async fn query_traces(&self, _query: TraceQuery) -> Result<TraceQueryResult> {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        Ok(TraceQueryResult {
            traces: vec![],
            cursor: QueryCursor::new(Timestamp::now().unwrap()),
        })
    }

    async fn get_spans(
        &self,
        _trace_id: sequins::models::TraceId,
    ) -> Result<Vec<sequins::models::Span>> {
        Ok(Vec::new())
    }

    async fn get_span(
        &self,
        _trace_id: sequins::models::TraceId,
        _span_id: sequins::models::SpanId,
    ) -> Result<Option<sequins::models::Span>> {
        Ok(None)
    }

    async fn query_spans(&self, _query: sequins::models::SpanQuery) -> Result<SpanQueryResult> {
        Ok(SpanQueryResult {
            spans: vec![],
            cursor: QueryCursor::new(Timestamp::now().unwrap()),
        })
    }

    async fn query_logs(&self, _query: sequins::models::LogQuery) -> Result<LogQueryResult> {
        Ok(LogQueryResult {
            logs: vec![],
            cursor: QueryCursor::new(Timestamp::now().unwrap()),
        })
    }

    async fn get_log(
        &self,
        _log_id: sequins::models::LogId,
    ) -> Result<Option<sequins::models::LogEntry>> {
        Ok(None)
    }

    async fn query_metrics(
        &self,
        _query: sequins::models::MetricQuery,
    ) -> Result<MetricQueryResult> {
        Ok(MetricQueryResult {
            metrics: vec![],
            cursor: QueryCursor::new(Timestamp::now().unwrap()),
        })
    }

    async fn get_metric(
        &self,
        _metric_id: sequins::models::MetricId,
    ) -> Result<Option<sequins::models::Metric>> {
        Ok(None)
    }

    async fn query_profiles(
        &self,
        _query: sequins::models::ProfileQuery,
    ) -> Result<ProfileQueryResult> {
        Ok(ProfileQueryResult {
            profiles: vec![],
            cursor: QueryCursor::new(Timestamp::now().unwrap()),
        })
    }

    async fn get_profile(
        &self,
        _profile_id: sequins::models::ProfileId,
    ) -> Result<Option<sequins::models::Profile>> {
        Ok(None)
    }

    async fn query_metric_data_points(
        &self,
        _query: sequins::models::MetricDataPointQuery,
    ) -> Result<MetricDataPointQueryResult> {
        Ok(MetricDataPointQueryResult {
            data_points: vec![],
            cursor: QueryCursor::new(Timestamp::now().unwrap()),
        })
    }

    async fn query_histogram_data_points(
        &self,
        _query: sequins::models::HistogramDataPointQuery,
    ) -> Result<HistogramDataPointQueryResult> {
        Ok(HistogramDataPointQueryResult {
            data_points: vec![],
            cursor: QueryCursor::new(Timestamp::now().unwrap()),
        })
    }

    async fn get_metric_groups(
        &self,
        _service_name: Option<&str>,
    ) -> Result<Vec<sequins::models::MetricGroup>> {
        Ok(Vec::new())
    }

    async fn get_metric_group(
        &self,
        _base_name: &str,
        _service_name: &str,
    ) -> Result<Option<sequins::models::MetricGroup>> {
        Ok(None)
    }

    async fn subscribe_traces(
        &self,
        _query: TraceQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = QueryTrace> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn subscribe_spans(
        &self,
        _query: sequins::models::SpanQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = sequins::models::Span> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn subscribe_logs(
        &self,
        _query: sequins::models::LogQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = sequins::models::LogEntry> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn subscribe_metrics(
        &self,
        _query: sequins::models::MetricQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = sequins::models::Metric> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn subscribe_metric_data_points(
        &self,
        _query: sequins::models::MetricDataPointQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = MetricDataPoint> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn subscribe_profiles(
        &self,
        _query: sequins::models::ProfileQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = sequins::models::Profile> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn query_profile_samples(
        &self,
        _query: sequins::models::ProfileSampleQuery,
    ) -> Result<sequins::models::ProfileSampleQueryResult> {
        Ok(sequins::models::ProfileSampleQueryResult {
            samples: vec![],
            cursor: QueryCursor::new(sequins::models::Timestamp::from_secs(0)),
        })
    }

    async fn subscribe_profile_samples(
        &self,
        _query: sequins::models::ProfileSampleQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = sequins::models::ProfileSample> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }
}

/// Mock ManagementApi for concurrency testing
#[derive(Clone)]
struct ConcurrentMockManagementApi {
    total_requests: Arc<AtomicUsize>,
}

impl ConcurrentMockManagementApi {
    fn new() -> Self {
        Self {
            total_requests: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn get_total_requests(&self) -> usize {
        self.total_requests.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ManagementApi for ConcurrentMockManagementApi {
    async fn run_retention_cleanup(&self) -> Result<usize> {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok(0)
    }

    async fn update_retention_policy(&self, _policy: RetentionPolicy) -> Result<()> {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn get_retention_policy(&self) -> Result<RetentionPolicy> {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        Ok(RetentionPolicy {
            spans_retention: sequins::models::Duration::from_hours(24),
            logs_retention: sequins::models::Duration::from_hours(24),
            metrics_retention: sequins::models::Duration::from_hours(24),
            profiles_retention: sequins::models::Duration::from_hours(24),
        })
    }

    async fn run_maintenance(&self) -> Result<MaintenanceStats> {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        Ok(MaintenanceStats {
            entries_evicted: 0,
            batches_flushed: 0,
        })
    }

    async fn get_storage_stats(&self) -> Result<StorageStats> {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        Ok(StorageStats {
            span_count: 0,
            log_count: 0,
            metric_count: 0,
            profile_count: 0,
        })
    }
}

async fn start_concurrent_query_server() -> (ConcurrentMockQueryApi, tokio::task::JoinHandle<()>) {
    let mock = ConcurrentMockQueryApi::new();
    let server = QueryServer::new(Arc::new(mock.clone()));

    let handle = tokio::spawn(async move {
        let app = server.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18084")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (mock, handle)
}

async fn start_concurrent_management_server(
) -> (ConcurrentMockManagementApi, tokio::task::JoinHandle<()>) {
    let mock = ConcurrentMockManagementApi::new();
    let server = ManagementServer::new(Arc::new(mock.clone()));

    let handle = tokio::spawn(async move {
        let app = server.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18085")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (mock, handle)
}

#[tokio::test]
#[serial]
async fn test_concurrent_query_requests() {
    let (mock, _handle) = start_concurrent_query_server().await;

    let client = reqwest::Client::new();

    // Send 10 concurrent requests
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .get("http://127.0.0.1:18084/api/services")
                .send()
                .await
                .unwrap()
        }));
    }

    // Wait for all requests to complete
    let results = futures::future::join_all(tasks).await;

    // All requests should succeed
    for result in results {
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Verify all 10 requests were processed
    assert_eq!(mock.get_total_requests(), 10);
}

#[tokio::test]
#[serial]
async fn test_concurrent_management_requests() {
    let (mock, _handle) = start_concurrent_management_server().await;

    let client = reqwest::Client::new();

    // Send 5 concurrent requests to different endpoints
    let mut tasks = Vec::new();

    // Mix of different endpoint calls
    for i in 0..5 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            match i % 3 {
                0 => client
                    .get("http://127.0.0.1:18085/api/retention/policy")
                    .send()
                    .await
                    .unwrap(),
                1 => client
                    .get("http://127.0.0.1:18085/api/storage/stats")
                    .send()
                    .await
                    .unwrap(),
                _ => client
                    .post("http://127.0.0.1:18085/api/retention/cleanup")
                    .send()
                    .await
                    .unwrap(),
            }
        }));
    }

    // Wait for all requests
    let results = futures::future::join_all(tasks).await;

    // All should succeed
    for result in results {
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Verify all 5 requests were processed
    assert_eq!(mock.get_total_requests(), 5);
}

#[tokio::test]
#[serial]
async fn test_cors_headers_query_server() {
    let (__mock, _handle) = start_concurrent_query_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18084/api/services")
        .header("Origin", "http://example.com")
        .send()
        .await
        .unwrap();

    // Check that CORS headers are present (permissive CORS)
    assert!(response
        .headers()
        .contains_key("access-control-allow-origin"));
}

#[tokio::test]
#[serial]
async fn test_cors_headers_management_server() {
    let (_mock, _handle) = start_concurrent_management_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18085/api/retention/policy")
        .header("Origin", "http://example.com")
        .send()
        .await
        .unwrap();

    // Check that CORS headers are present
    assert!(response
        .headers()
        .contains_key("access-control-allow-origin"));
}

#[tokio::test]
#[serial]
async fn test_cors_preflight() {
    let (__mock, _handle) = start_concurrent_query_server().await;

    let client = reqwest::Client::new();
    let response = client
        .request(
            reqwest::Method::OPTIONS,
            "http://127.0.0.1:18084/api/services",
        )
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();

    // OPTIONS request should succeed
    assert!(response.status().is_success() || response.status() == StatusCode::NO_CONTENT);
}

#[tokio::test]
#[serial]
async fn test_server_health_under_load() {
    let (_mock, _handle) = start_concurrent_query_server().await;

    let client = reqwest::Client::new();

    // Send 20 rapid requests
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            client
                .get("http://127.0.0.1:18084/health")
                .send()
                .await
                .unwrap()
        }));
    }

    let results = futures::future::join_all(tasks).await;

    // Health endpoint should always respond OK
    for result in results {
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.text().await.unwrap();
        assert_eq!(body, "OK");
    }

    // Note: health endpoint doesn't increment request counter
    // Only services endpoint does in our mock
}

#[tokio::test]
#[serial]
async fn test_mixed_endpoint_concurrency() {
    let (mock, _handle) = start_concurrent_query_server().await;

    let client = reqwest::Client::new();

    // Mix of services requests and trace queries
    let mut tasks = Vec::new();

    for i in 0..8 {
        let client = client.clone();
        if i % 2 == 0 {
            tasks.push(tokio::spawn(async move {
                client
                    .get("http://127.0.0.1:18084/api/services")
                    .send()
                    .await
                    .unwrap()
            }));
        } else {
            tasks.push(tokio::spawn(async move {
                let query = TraceQuery {
                    service: Some("test".to_string()),
                    start_time: Timestamp::from_secs(0),
                    end_time: Timestamp::from_secs(1000),
                    min_duration: None,
                    max_duration: None,
                    has_error: None,
                    attributes: None,
                    limit: Some(10),
                    live: false,
                };

                client
                    .post("http://127.0.0.1:18084/api/traces")
                    .json(&query)
                    .send()
                    .await
                    .unwrap()
            }));
        }
    }

    let results = futures::future::join_all(tasks).await;

    // All should succeed
    for result in results {
        let response = result.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Should have processed 8 requests (4 services + 4 traces)
    assert_eq!(mock.get_total_requests(), 8);
}
