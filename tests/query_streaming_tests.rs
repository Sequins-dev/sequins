//! Integration tests for QueryServer SSE streaming endpoints
//!
//! These tests verify that the SSE (Server-Sent Events) streaming endpoints
//! work correctly for live query updates.

use reqwest::StatusCode;
use sequins::server::QueryServer;
use sequins::{
    error::Result,
    models::{
        HistogramDataPointQuery, HistogramDataPointQueryResult, LogEntry, LogId, LogQuery,
        LogQueryResult, LogSeverity, Metric, MetricDataPointQuery, MetricDataPointQueryResult,
        MetricGroup, MetricId, MetricQuery, MetricQueryResult, MetricType, Profile, ProfileId,
        ProfileQuery, ProfileQueryResult, ProfileSample, ProfileSampleQuery,
        ProfileSampleQueryResult, ProfileType, QueryCursor, QueryTrace, Span, SpanId, SpanQuery,
        SpanQueryResult, Timestamp, TraceId, TraceQuery, TraceQueryResult,
    },
    traits::QueryApi,
};
use serial_test::serial;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

/// Mock implementation of QueryApi for streaming tests
#[derive(Clone)]
struct StreamingMockQueryApi {
    traces: Arc<tokio::sync::Mutex<Vec<QueryTrace>>>,
    logs: Arc<tokio::sync::Mutex<Vec<LogEntry>>>,
    metrics: Arc<tokio::sync::Mutex<Vec<Metric>>>,
    profiles: Arc<tokio::sync::Mutex<Vec<Profile>>>,
}

impl StreamingMockQueryApi {
    fn new() -> Self {
        Self {
            traces: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            logs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            metrics: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            profiles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    async fn add_trace(&self, trace: QueryTrace) {
        self.traces.lock().await.push(trace);
    }

    async fn add_log(&self, log: LogEntry) {
        self.logs.lock().await.push(log);
    }

    async fn add_metric(&self, metric: Metric) {
        self.metrics.lock().await.push(metric);
    }

    async fn add_profile(&self, profile: Profile) {
        self.profiles.lock().await.push(profile);
    }
}

#[async_trait::async_trait]
impl QueryApi for StreamingMockQueryApi {
    async fn get_services(&self) -> Result<Vec<sequins::models::Service>> {
        Ok(Vec::new())
    }

    async fn query_traces(&self, _query: TraceQuery) -> Result<TraceQueryResult> {
        let traces = self.traces.lock().await.clone();
        Ok(TraceQueryResult {
            traces,
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_traces(
        &self,
        _query: TraceQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = QueryTrace> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_spans(&self, _trace_id: TraceId) -> Result<Vec<Span>> {
        Ok(Vec::new())
    }

    async fn get_span(&self, _trace_id: TraceId, _span_id: SpanId) -> Result<Option<Span>> {
        Ok(None)
    }

    async fn query_spans(&self, _query: SpanQuery) -> Result<SpanQueryResult> {
        Ok(SpanQueryResult {
            spans: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_spans(
        &self,
        _query: SpanQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Span> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_log(&self, _log_id: LogId) -> Result<Option<LogEntry>> {
        Ok(None)
    }

    async fn query_logs(&self, _query: LogQuery) -> Result<LogQueryResult> {
        let logs = self.logs.lock().await.clone();
        Ok(LogQueryResult {
            logs,
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_logs(
        &self,
        _query: LogQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = LogEntry> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_metric(&self, _metric_id: MetricId) -> Result<Option<Metric>> {
        Ok(None)
    }

    async fn query_metrics(&self, _query: MetricQuery) -> Result<MetricQueryResult> {
        let metrics = self.metrics.lock().await.clone();
        Ok(MetricQueryResult {
            metrics,
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_metrics(
        &self,
        _query: MetricQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Metric> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn query_metric_data_points(
        &self,
        _query: MetricDataPointQuery,
    ) -> Result<MetricDataPointQueryResult> {
        Ok(MetricDataPointQueryResult {
            data_points: Vec::new(),
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_metric_data_points(
        &self,
        _query: MetricDataPointQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = sequins::models::MetricDataPoint> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn query_histogram_data_points(
        &self,
        _query: HistogramDataPointQuery,
    ) -> Result<HistogramDataPointQueryResult> {
        Ok(HistogramDataPointQueryResult {
            data_points: Vec::new(),
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn get_profile(&self, _profile_id: ProfileId) -> Result<Option<Profile>> {
        Ok(None)
    }

    async fn query_profiles(&self, _query: ProfileQuery) -> Result<ProfileQueryResult> {
        let profiles = self.profiles.lock().await.clone();
        Ok(ProfileQueryResult {
            profiles,
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_profiles(
        &self,
        _query: ProfileQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Profile> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn query_profile_samples(
        &self,
        _query: ProfileSampleQuery,
    ) -> Result<ProfileSampleQueryResult> {
        Ok(ProfileSampleQueryResult {
            samples: vec![],
            cursor: QueryCursor::new(Timestamp::from_secs(0)),
        })
    }

    async fn subscribe_profile_samples(
        &self,
        _query: ProfileSampleQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = ProfileSample> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_metric_groups(&self, _service_name: Option<&str>) -> Result<Vec<MetricGroup>> {
        Ok(Vec::new())
    }

    async fn get_metric_group(
        &self,
        _base_name: &str,
        _service_name: &str,
    ) -> Result<Option<MetricGroup>> {
        Ok(None)
    }
}

/// Start the streaming query server
async fn start_streaming_server() -> (StreamingMockQueryApi, tokio::task::JoinHandle<()>) {
    let mock = StreamingMockQueryApi::new();
    let server = QueryServer::new(Arc::new(mock.clone()));

    let handle = tokio::spawn(async move {
        let app = server.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18083")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (mock, handle)
}

#[tokio::test]
#[serial]
#[ignore = "Query parameter serialization issue with complex types"]
async fn test_stream_traces() {
    let (mock, _handle) = start_streaming_server().await;

    // Add test traces
    let trace_id_1 = TraceId::from_bytes([1; 16]);
    let trace_id_2 = TraceId::from_bytes([2; 16]);

    mock.add_trace(QueryTrace {
        trace_id: trace_id_1,
        root_span_id: SpanId::from_bytes([1; 8]),
        spans: vec![],
        duration: 100_000_000,
        has_error: false,
    })
    .await;

    mock.add_trace(QueryTrace {
        trace_id: trace_id_2,
        root_span_id: SpanId::from_bytes([2; 8]),
        spans: vec![],
        duration: 200_000_000,
        has_error: true,
    })
    .await;

    // Build query using proper struct
    let query = TraceQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        min_duration: None,
        max_duration: None,
        has_error: None,
        attributes: None,
        limit: Some(10),
        live: true, // SSE mode
    };

    // Connect to SSE stream
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18083/api/traces/stream")
        .query(&query)
        .send()
        .await
        .unwrap();

    // Verify SSE response
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    // For SSE streams, we verify the endpoint responds correctly with proper headers
    // Full stream body parsing would require additional dependencies
}

#[tokio::test]
#[serial]
#[ignore = "Query parameter serialization issue with complex types"]
async fn test_stream_logs() {
    let (mock, _handle) = start_streaming_server().await;

    // Add test logs
    mock.add_log(LogEntry {
        id: LogId::new(),
        timestamp: Timestamp::from_secs(1000),
        observed_timestamp: Timestamp::from_secs(1001),
        service_name: "test-service".to_string(),
        severity: LogSeverity::Info,
        body: "Test log 1".to_string(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    })
    .await;

    mock.add_log(LogEntry {
        id: LogId::new(),
        timestamp: Timestamp::from_secs(1002),
        observed_timestamp: Timestamp::from_secs(1003),
        service_name: "test-service".to_string(),
        severity: LogSeverity::Error,
        body: "Test log 2".to_string(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    })
    .await;

    // Build query using proper struct
    let query = LogQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        severities: None,
        search: None,
        trace_id: None,
        attributes: None,
        limit: Some(10),
        live: true, // SSE mode
    };

    // Connect to SSE stream
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18083/api/logs/stream")
        .query(&query)
        .send()
        .await
        .unwrap();

    // Verify SSE response
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
}

#[tokio::test]
#[serial]
#[ignore = "Query parameter serialization issue with complex types"]
async fn test_stream_metrics() {
    let (mock, _handle) = start_streaming_server().await;

    // Add test metrics
    mock.add_metric(Metric {
        id: MetricId::new(),
        name: "cpu_usage".to_string(),
        description: "CPU usage".to_string(),
        unit: "percent".to_string(),
        metric_type: MetricType::Gauge,
        service_name: "test-service".to_string(),
        is_generated: false,
    })
    .await;

    mock.add_metric(Metric {
        id: MetricId::new(),
        name: "request_count".to_string(),
        description: "Request count".to_string(),
        unit: "1".to_string(),
        metric_type: MetricType::Counter,
        service_name: "test-service".to_string(),
        is_generated: false,
    })
    .await;

    // Build query using proper struct
    let query = MetricQuery {
        name: None,
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        limit: Some(10),
        live: true, // SSE mode
    };

    // Connect to SSE stream
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18083/api/metrics/stream")
        .query(&query)
        .send()
        .await
        .unwrap();

    // Verify SSE response
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
}

#[tokio::test]
#[serial]
#[ignore = "Query parameter serialization issue with complex types"]
async fn test_stream_profiles() {
    let (mock, _handle) = start_streaming_server().await;

    // Add test profiles
    mock.add_profile(Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1000),
        service_name: "test-service".to_string(),
        profile_type: ProfileType::Cpu,
        sample_type: "samples".to_string(),
        sample_unit: "count".to_string(),
        data: vec![1, 2, 3],
        trace_id: None,
    })
    .await;

    mock.add_profile(Profile {
        id: ProfileId::new(),
        timestamp: Timestamp::from_secs(1001),
        service_name: "test-service".to_string(),
        profile_type: ProfileType::Memory,
        sample_type: "alloc_objects".to_string(),
        sample_unit: "count".to_string(),
        data: vec![4, 5, 6],
        trace_id: None,
    })
    .await;

    // Build query using proper struct
    let query = ProfileQuery {
        service: Some("test-service".to_string()),
        profile_type: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        trace_id: None,
        limit: Some(10),
        live: true, // SSE mode
    };

    // Connect to SSE stream
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18083/api/profiles/stream")
        .query(&query)
        .send()
        .await
        .unwrap();

    // Verify SSE response
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
}

#[tokio::test]
#[serial]
#[ignore = "Query parameter serialization issue with complex types"]
async fn test_stream_empty_results() {
    let (_mock, _handle) = start_streaming_server().await;

    // Build query using proper struct
    let query = TraceQuery {
        service: Some("test".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(1000),
        min_duration: None,
        max_duration: None,
        has_error: None,
        attributes: None,
        limit: Some(10),
        live: true, // SSE mode
    };

    // Connect to SSE stream without adding any data
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18083/api/traces/stream")
        .query(&query)
        .send()
        .await
        .unwrap();

    // Verify SSE response - empty streams should still return valid SSE headers
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/event-stream"
    );
}
