//! Integration tests for QueryServer HTTP API
//!
//! These tests verify that the QueryServer correctly exposes QueryApi methods
//! via HTTP endpoints.

use reqwest::StatusCode;
use sequins_core::{
    error::Result,
    models::{
        AttributeValue, Duration, LogEntry, LogId, LogQuery, LogSeverity, Metric, MetricId,
        MetricQuery, MetricType, Profile, ProfileId, ProfileQuery, QueryTrace, Service, Span,
        SpanId, SpanKind, SpanStatus, Timestamp, TraceId, TraceQuery,
    },
    traits::QueryApi,
};
use sequins_server::QueryServer;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Mock implementation of QueryApi that returns predefined test data
#[derive(Clone)]
struct MockQueryApi {
    services: Arc<Mutex<Vec<Service>>>,
    traces: Arc<Mutex<Vec<QueryTrace>>>,
    spans: Arc<Mutex<HashMap<(TraceId, Option<SpanId>), Vec<Span>>>>,
    logs: Arc<Mutex<Vec<LogEntry>>>,
    metrics: Arc<Mutex<Vec<Metric>>>,
    profiles: Arc<Mutex<Vec<Profile>>>,
}

impl MockQueryApi {
    fn new() -> Self {
        Self {
            services: Arc::new(Mutex::new(Vec::new())),
            traces: Arc::new(Mutex::new(Vec::new())),
            spans: Arc::new(Mutex::new(HashMap::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
            metrics: Arc::new(Mutex::new(Vec::new())),
            profiles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn add_service(&self, service: Service) {
        self.services.lock().unwrap().push(service);
    }

    fn add_trace(&self, trace: QueryTrace) {
        self.traces.lock().unwrap().push(trace);
    }

    fn add_spans(&self, trace_id: TraceId, span_id: Option<SpanId>, spans: Vec<Span>) {
        self.spans.lock().unwrap().insert((trace_id, span_id), spans);
    }

    fn add_log(&self, log: LogEntry) {
        self.logs.lock().unwrap().push(log);
    }

    fn add_metric(&self, metric: Metric) {
        self.metrics.lock().unwrap().push(metric);
    }
}

#[async_trait::async_trait]
impl QueryApi for MockQueryApi {
    async fn get_services(&self) -> Result<Vec<Service>> {
        Ok(self.services.lock().unwrap().clone())
    }

    async fn query_traces(&self, _query: TraceQuery) -> Result<Vec<QueryTrace>> {
        Ok(self.traces.lock().unwrap().clone())
    }

    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>> {
        Ok(self
            .spans
            .lock()
            .unwrap()
            .get(&(trace_id, None))
            .cloned()
            .unwrap_or_default())
    }

    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>> {
        Ok(self
            .spans
            .lock()
            .unwrap()
            .get(&(trace_id, Some(span_id)))
            .and_then(|spans| spans.first().cloned()))
    }

    async fn query_logs(&self, _query: LogQuery) -> Result<Vec<LogEntry>> {
        Ok(self.logs.lock().unwrap().clone())
    }

    async fn get_log(&self, log_id: LogId) -> Result<Option<LogEntry>> {
        Ok(self
            .logs
            .lock()
            .unwrap()
            .iter()
            .find(|log| log.id == log_id)
            .cloned())
    }

    async fn query_metrics(&self, _query: MetricQuery) -> Result<Vec<Metric>> {
        Ok(self.metrics.lock().unwrap().clone())
    }

    async fn get_metric(&self, metric_id: MetricId) -> Result<Option<Metric>> {
        Ok(self
            .metrics
            .lock()
            .unwrap()
            .iter()
            .find(|metric| metric.id == metric_id)
            .cloned())
    }

    async fn get_profiles(&self, _query: ProfileQuery) -> Result<Vec<Profile>> {
        Ok(self.profiles.lock().unwrap().clone())
    }

    async fn get_profile(&self, profile_id: ProfileId) -> Result<Option<Profile>> {
        Ok(self
            .profiles
            .lock()
            .unwrap()
            .iter()
            .find(|profile| profile.id == profile_id)
            .cloned())
    }
}

/// Start the QueryServer in the background
async fn start_query_server() -> (MockQueryApi, tokio::task::JoinHandle<()>) {
    let mock = MockQueryApi::new();
    let server = QueryServer::new(Arc::new(mock.clone()));

    let handle = tokio::spawn(async move {
        let app = server.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18080")
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
async fn test_get_services() {
    let (mock, _handle) = start_query_server().await;

    // Add test services
    mock.add_service(Service {
        name: "test-service-1".to_string(),
        span_count: 100,
        log_count: 10,
    });
    mock.add_service(Service {
        name: "test-service-2".to_string(),
        span_count: 50,
        log_count: 5,
    });

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18080/api/services")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let services: Vec<Service> = response.json().await.unwrap();
    assert_eq!(services.len(), 2);
    assert_eq!(services[0].name, "test-service-1");
    assert_eq!(services[1].name, "test-service-2");
}

#[tokio::test]
#[serial]
async fn test_query_traces() {
    let (mock, _handle) = start_query_server().await;

    // Add test trace
    let trace_id = TraceId::from_bytes([1; 16]);
    let root_span_id = SpanId::from_bytes([2; 8]);

    mock.add_trace(QueryTrace {
        trace_id,
        root_span_id,
        spans: vec![],
        duration: 100_000_000, // 100ms in nanos
        has_error: false,
    });

    // Make HTTP request
    let client = reqwest::Client::new();
    let query = TraceQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(10),
    };

    let response = client
        .post("http://127.0.0.1:18080/api/traces")
        .json(&query)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let traces: Vec<QueryTrace> = response.json().await.unwrap();
    assert_eq!(traces.len(), 1);
    assert_eq!(traces[0].trace_id, trace_id);
}

#[tokio::test]
#[serial]
async fn test_get_spans() {
    let (mock, _handle) = start_query_server().await;

    let trace_id = TraceId::from_bytes([1; 16]);
    let span_id = SpanId::from_bytes([2; 8]);

    let span = Span {
        trace_id,
        span_id,
        parent_span_id: None,
        service_name: "test-service".to_string(),
        operation_name: "test-op".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(1100),
        duration: Duration::from_millis(100),
        attributes: HashMap::new(),
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
    };

    mock.add_spans(trace_id, None, vec![span.clone()]);

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "http://127.0.0.1:18080/api/traces/{}/spans",
            trace_id.to_hex()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let spans: Vec<Span> = response.json().await.unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].operation_name, "test-op");
}

#[tokio::test]
#[serial]
async fn test_get_span() {
    let (mock, _handle) = start_query_server().await;

    let trace_id = TraceId::from_bytes([1; 16]);
    let span_id = SpanId::from_bytes([2; 8]);

    let span = Span {
        trace_id,
        span_id,
        parent_span_id: None,
        service_name: "test-service".to_string(),
        operation_name: "test-op".to_string(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(1100),
        duration: Duration::from_millis(100),
        attributes: {
            let mut attrs = HashMap::new();
            attrs.insert("test".to_string(), AttributeValue::String("value".to_string()));
            attrs
        },
        events: vec![],
        status: SpanStatus::Ok,
        span_kind: SpanKind::Server,
    };

    mock.add_spans(trace_id, Some(span_id), vec![span.clone()]);

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "http://127.0.0.1:18080/api/traces/{}/spans/{}",
            trace_id.to_hex(),
            span_id.to_hex()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let returned_span: Option<Span> = response.json().await.unwrap();
    assert!(returned_span.is_some());
    assert_eq!(returned_span.unwrap().operation_name, "test-op");
}

#[tokio::test]
#[serial]
async fn test_query_logs() {
    let (mock, _handle) = start_query_server().await;

    mock.add_log(LogEntry {
        id: LogId::new(),
        timestamp: Timestamp::from_secs(1000),
        observed_timestamp: Timestamp::from_secs(1001),
        service_name: "test-service".to_string(),
        severity: LogSeverity::Info,
        body: "Test log message".to_string(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    });

    // Make HTTP request
    let client = reqwest::Client::new();
    let query = LogQuery {
        service: Some("test-service".to_string()),
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        severity: None,
        search: None,
        trace_id: None,
        limit: Some(10),
    };

    let response = client
        .post("http://127.0.0.1:18080/api/logs")
        .json(&query)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let logs: Vec<LogEntry> = response.json().await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].body, "Test log message");
}

#[tokio::test]
#[serial]
async fn test_get_log() {
    let (mock, _handle) = start_query_server().await;

    let log_id = LogId::new();
    mock.add_log(LogEntry {
        id: log_id,
        timestamp: Timestamp::from_secs(1000),
        observed_timestamp: Timestamp::from_secs(1001),
        service_name: "test-service".to_string(),
        severity: LogSeverity::Warn,
        body: "Warning message".to_string(),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        resource: HashMap::new(),
    });

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "http://127.0.0.1:18080/api/logs/{}",
            log_id.to_hex()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let log: Option<LogEntry> = response.json().await.unwrap();
    assert!(log.is_some());
    assert_eq!(log.unwrap().severity, LogSeverity::Warn);
}

#[tokio::test]
#[serial]
async fn test_query_metrics() {
    let (mock, _handle) = start_query_server().await;

    mock.add_metric(Metric {
        id: MetricId::new(),
        name: "test_metric".to_string(),
        description: "Test metric".to_string(),
        unit: "1".to_string(),
        metric_type: MetricType::Counter,
        service_name: "test-service".to_string(),
    });

    // Make HTTP request
    let client = reqwest::Client::new();
    let query = MetricQuery {
        name: Some("test_metric".to_string()),
        service: None,
        start_time: Timestamp::from_secs(0),
        end_time: Timestamp::from_secs(2000),
        limit: Some(10),
    };

    let response = client
        .post("http://127.0.0.1:18080/api/metrics")
        .json(&query)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let metrics: Vec<Metric> = response.json().await.unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].name, "test_metric");
}

#[tokio::test]
#[serial]
async fn test_get_metric() {
    let (mock, _handle) = start_query_server().await;

    let metric_id = MetricId::new();
    mock.add_metric(Metric {
        id: metric_id,
        name: "test_gauge".to_string(),
        description: "Test gauge".to_string(),
        unit: "ms".to_string(),
        metric_type: MetricType::Gauge,
        service_name: "test-service".to_string(),
    });

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "http://127.0.0.1:18080/api/metrics/{}",
            metric_id.to_hex()
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let metric: Option<Metric> = response.json().await.unwrap();
    assert!(metric.is_some());
    assert_eq!(metric.unwrap().metric_type, MetricType::Gauge);
}

#[tokio::test]
#[serial]
async fn test_health_check() {
    let (_mock, _handle) = start_query_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18080/health")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert_eq!(body, "OK");
}
