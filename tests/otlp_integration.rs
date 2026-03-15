//! Integration tests for OTLP server using protobuf directly
//!
//! These tests verify that the OtlpServer correctly receives and parses
//! OTLP data by sending protobuf messages directly.

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{logs_service_client::LogsServiceClient, ExportLogsServiceRequest},
        metrics::v1::{metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest},
        trace::v1::{trace_service_client::TraceServiceClient, ExportTraceServiceRequest},
    },
    common::v1::{any_value::Value, AnyValue, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        metric::Data, number_data_point::Value as NumberValue, Gauge, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics,
    },
    resource::v1::Resource,
    trace::v1::{
        span::SpanKind, status::StatusCode, ResourceSpans, ScopeSpans, Span as OtlpSpan, Status,
    },
};
use sequins::ingest::OtlpServer;
use sequins::{
    error::Result,
    models::{LogEntry, Metric as SequinsMetric, Profile, Span as SequinsSpan},
    traits::OtlpIngest,
};
use serial_test::serial;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Mock implementation of OtlpIngest that stores received data for verification
#[derive(Clone)]
struct MockOtlpIngest {
    spans: Arc<Mutex<Vec<SequinsSpan>>>,
    logs: Arc<Mutex<Vec<LogEntry>>>,
    metrics: Arc<Mutex<Vec<SequinsMetric>>>,
    profiles: Arc<Mutex<Vec<Profile>>>,
}

impl MockOtlpIngest {
    fn new() -> Self {
        Self {
            spans: Arc::new(Mutex::new(Vec::new())),
            logs: Arc::new(Mutex::new(Vec::new())),
            metrics: Arc::new(Mutex::new(Vec::new())),
            profiles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_spans(&self) -> Vec<SequinsSpan> {
        self.spans.lock().unwrap().clone()
    }

    fn get_logs(&self) -> Vec<LogEntry> {
        self.logs.lock().unwrap().clone()
    }

    fn get_metrics(&self) -> Vec<SequinsMetric> {
        self.metrics.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.spans.lock().unwrap().clear();
        self.logs.lock().unwrap().clear();
        self.metrics.lock().unwrap().clear();
        self.profiles.lock().unwrap().clear();
    }
}

#[async_trait::async_trait]
impl OtlpIngest for MockOtlpIngest {
    async fn ingest_spans(&self, spans: Vec<SequinsSpan>) -> Result<()> {
        self.spans.lock().unwrap().extend(spans);
        Ok(())
    }

    async fn ingest_logs(&self, logs: Vec<LogEntry>) -> Result<()> {
        self.logs.lock().unwrap().extend(logs);
        Ok(())
    }

    async fn ingest_metrics(&self, metrics: Vec<SequinsMetric>) -> Result<()> {
        self.metrics.lock().unwrap().extend(metrics);
        Ok(())
    }

    async fn ingest_profiles(&self, profiles: Vec<Profile>) -> Result<()> {
        self.profiles.lock().unwrap().extend(profiles);
        Ok(())
    }

    async fn ingest_metric_data_points(
        &self,
        _data_points: Vec<sequins::models::MetricDataPoint>,
    ) -> Result<()> {
        Ok(())
    }

    async fn ingest_histogram_data_points(
        &self,
        _data_points: Vec<sequins::models::HistogramDataPoint>,
    ) -> Result<()> {
        Ok(())
    }
}

/// Start the OTLP server in the background and return the mock ingest
async fn start_test_server() -> (MockOtlpIngest, tokio::task::JoinHandle<()>) {
    let mock = MockOtlpIngest::new();
    let server = OtlpServer::new(Arc::new(mock.clone()));

    let handle = tokio::spawn(async move {
        // Serve on test ports
        let _ = server.serve("127.0.0.1:14317", "127.0.0.1:14318").await;
    });

    // Give the server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    (mock, handle)
}

#[tokio::test]
#[serial]
async fn test_trace_ingestion_via_grpc() {
    // Start test server
    let (mock, _handle) = start_test_server().await;
    mock.clear();

    // Create gRPC client
    let mut client = TraceServiceClient::connect("http://127.0.0.1:14317")
        .await
        .expect("Failed to connect to server");

    // Create a test span
    let trace_id = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let span_id = vec![1, 2, 3, 4, 5, 6, 7, 8];

    let otlp_span = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: span_id.clone(),
        parent_span_id: vec![],
        name: "test-operation".to_string(),
        kind: SpanKind::Server as i32,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 2000000000,
        attributes: vec![
            KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("GET".to_string())),
                }),
            },
            KeyValue {
                key: "http.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(200)),
                }),
            },
        ],
        status: Some(Status {
            code: StatusCode::Ok as i32,
            message: String::new(),
        }),
        ..Default::default()
    };

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![otlp_span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    // Send the request
    client
        .export(request)
        .await
        .expect("Failed to export spans");

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we received the span
    let spans = mock.get_spans();
    assert_eq!(spans.len(), 1, "Expected 1 span");

    let received_span = &spans[0];
    assert_eq!(received_span.operation_name, "test-operation");
    assert_eq!(received_span.service_name, "test-service");
    assert_eq!(received_span.span_kind, sequins::models::SpanKind::Server);

    // Verify attributes
    assert!(received_span.attributes.contains_key("http.method"));
    assert!(received_span.attributes.contains_key("http.status_code"));
}

#[tokio::test]
#[serial]
async fn test_log_ingestion_via_grpc() {
    // Start test server
    let (mock, _handle) = start_test_server().await;
    mock.clear();

    // Create gRPC client
    let mut client = LogsServiceClient::connect("http://127.0.0.1:14317")
        .await
        .expect("Failed to connect to server");

    // Create a test log
    let log_record = LogRecord {
        time_unix_nano: 1000000000,
        observed_time_unix_nano: 1000000001,
        severity_number: 9, // Info level
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(Value::StringValue("Test log message".to_string())),
        }),
        attributes: vec![KeyValue {
            key: "log.level".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("info".to_string())),
            }),
        }],
        ..Default::default()
    };

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-log-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![log_record],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    // Send the request
    client.export(request).await.expect("Failed to export logs");

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we received the log
    let logs = mock.get_logs();
    assert_eq!(logs.len(), 1, "Expected 1 log");

    let received_log = &logs[0];
    assert_eq!(received_log.body, "Test log message");
    assert_eq!(received_log.service_name, "test-log-service");
    assert_eq!(received_log.severity, sequins::models::LogSeverity::Info);
}

#[tokio::test]
#[serial]
async fn test_metric_ingestion_via_grpc() {
    // Start test server
    let (mock, _handle) = start_test_server().await;
    mock.clear();

    // Create gRPC client
    let mut client = MetricsServiceClient::connect("http://127.0.0.1:14317")
        .await
        .expect("Failed to connect to server");

    // Create a test gauge metric
    let metric = Metric {
        name: "test_gauge".to_string(),
        description: "A test gauge metric".to_string(),
        unit: "1".to_string(),
        data: Some(Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                attributes: vec![KeyValue {
                    key: "method".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("GET".to_string())),
                    }),
                }],
                time_unix_nano: 1000000000,
                value: Some(NumberValue::AsDouble(42.5)),
                ..Default::default()
            }],
        })),
        ..Default::default()
    };

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-metric-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![metric],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    // Send the request
    client
        .export(request)
        .await
        .expect("Failed to export metrics");

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we received the metric
    let metrics = mock.get_metrics();
    assert_eq!(metrics.len(), 1, "Expected 1 metric");

    let received_metric = &metrics[0];
    assert_eq!(received_metric.name, "test_gauge");
    assert_eq!(received_metric.service_name, "test-metric-service");
    assert_eq!(
        received_metric.metric_type,
        sequins::models::MetricType::Gauge
    );
}

#[tokio::test]
#[serial]
async fn test_span_hierarchy() {
    // Start test server
    let (mock, _handle) = start_test_server().await;
    mock.clear();

    // Create gRPC client
    let mut client = TraceServiceClient::connect("http://127.0.0.1:14317")
        .await
        .expect("Failed to connect to server");

    let trace_id = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let parent_span_id = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let child_span_id = vec![9, 10, 11, 12, 13, 14, 15, 16];

    // Create parent span
    let parent_span = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: parent_span_id.clone(),
        parent_span_id: vec![],
        name: "parent-operation".to_string(),
        kind: SpanKind::Internal as i32,
        start_time_unix_nano: 1000000000,
        end_time_unix_nano: 3000000000,
        ..Default::default()
    };

    // Create child span
    let child_span = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: child_span_id.clone(),
        parent_span_id: parent_span_id.clone(),
        name: "child-operation".to_string(),
        kind: SpanKind::Internal as i32,
        start_time_unix_nano: 1500000000,
        end_time_unix_nano: 2500000000,
        ..Default::default()
    };

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("hierarchy-test".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![parent_span, child_span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    // Send the request
    client
        .export(request)
        .await
        .expect("Failed to export spans");

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify hierarchy
    let spans = mock.get_spans();
    assert_eq!(spans.len(), 2, "Expected 2 spans");

    // Find parent and child
    let parent = spans
        .iter()
        .find(|s| s.operation_name == "parent-operation")
        .expect("Parent span not found");
    let child = spans
        .iter()
        .find(|s| s.operation_name == "child-operation")
        .expect("Child span not found");

    // Verify parent has no parent
    assert!(
        parent.parent_span_id.is_none(),
        "Parent should have no parent"
    );

    // Verify child has parent
    assert!(child.parent_span_id.is_some(), "Child should have a parent");
    assert_eq!(
        child.parent_span_id.unwrap(),
        parent.span_id,
        "Child's parent should match parent's span_id"
    );

    // Verify same trace_id
    assert_eq!(
        child.trace_id, parent.trace_id,
        "Parent and child should share trace_id"
    );
}

// ============================================================================
// HTTP/JSON Endpoint Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_http_json_span_ingestion() {
    // Start test server
    let (mock, _handle) = start_test_server().await;
    mock.clear();

    // Create HTTP client
    let client = reqwest::Client::new();

    // Send a standard OTLP JSON ExportTraceServiceRequest
    let response = client
        .post("http://127.0.0.1:14318/v1/traces")
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-http-service"}}
                    ]
                },
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "01010101010101010101010101010101",
                        "spanId": "0202020202020202",
                        "name": "http-operation",
                        "startTimeUnixNano": "1000000000000",
                        "endTimeUnixNano": "1100000000000",
                        "kind": 2,
                        "status": {"code": 1},
                        "attributes": [
                            {"key": "http.method", "value": {"stringValue": "POST"}},
                            {"key": "http.status_code", "value": {"intValue": "201"}}
                        ]
                    }]
                }]
            }]
        }))
        .send()
        .await
        .expect("Failed to send HTTP request");

    assert!(
        response.status().is_success(),
        "Expected successful response, got: {}",
        response.status()
    );

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we received the span
    let spans = mock.get_spans();
    assert_eq!(spans.len(), 1, "Expected 1 span");

    let received_span = &spans[0];
    assert_eq!(received_span.operation_name, "http-operation");
    assert_eq!(received_span.service_name, "test-http-service");
    assert!(received_span.attributes.contains_key("http.method"));
}

#[tokio::test]
#[serial]
async fn test_http_json_log_ingestion() {
    // Start test server
    let (mock, _handle) = start_test_server().await;
    mock.clear();

    // Create HTTP client
    let client = reqwest::Client::new();

    // Send a standard OTLP JSON ExportLogsServiceRequest
    let response = client
        .post("http://127.0.0.1:14318/v1/logs")
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-http-log-service"}}
                    ]
                },
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "2000000000000",
                        "observedTimeUnixNano": "2001000000000",
                        "severityNumber": 13,
                        "body": {"stringValue": "HTTP test log message"},
                        "attributes": [
                            {"key": "component", "value": {"stringValue": "http-handler"}}
                        ]
                    }]
                }]
            }]
        }))
        .send()
        .await
        .expect("Failed to send HTTP request");

    assert!(
        response.status().is_success(),
        "Expected successful response, got: {}",
        response.status()
    );

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we received the log
    let logs = mock.get_logs();
    assert_eq!(logs.len(), 1, "Expected 1 log");

    let received_log = &logs[0];
    assert_eq!(received_log.body, "HTTP test log message");
    assert_eq!(received_log.service_name, "test-http-log-service");
    assert_eq!(received_log.severity, sequins::models::LogSeverity::Warn);
}

#[tokio::test]
#[serial]
async fn test_http_json_metric_ingestion() {
    // Start test server
    let (mock, _handle) = start_test_server().await;
    mock.clear();

    // Create HTTP client
    let client = reqwest::Client::new();

    // Send a standard OTLP JSON ExportMetricsServiceRequest (Sum/Counter metric)
    let response = client
        .post("http://127.0.0.1:14318/v1/metrics")
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "resourceMetrics": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "test-http-metric-service"}}
                    ]
                },
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "http_requests_total",
                        "description": "Total HTTP requests",
                        "unit": "requests",
                        "sum": {
                            "dataPoints": [{
                                "startTimeUnixNano": "0",
                                "timeUnixNano": "1000000000000",
                                "asDouble": 42.0
                            }],
                            "aggregationTemporality": 2,
                            "isMonotonic": true
                        }
                    }]
                }]
            }]
        }))
        .send()
        .await
        .expect("Failed to send HTTP request");

    assert!(
        response.status().is_success(),
        "Expected successful response, got: {}",
        response.status()
    );

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we received the metric
    let metrics = mock.get_metrics();
    assert_eq!(metrics.len(), 1, "Expected 1 metric");

    let received_metric = &metrics[0];
    assert_eq!(received_metric.name, "http_requests_total");
    assert_eq!(received_metric.service_name, "test-http-metric-service");
    assert_eq!(
        received_metric.metric_type,
        sequins::models::MetricType::Counter
    );
}

#[tokio::test]
#[serial]
async fn test_http_json_health_check() {
    // Start test server
    let (_mock, _handle) = start_test_server().await;

    // Create HTTP client
    let client = reqwest::Client::new();

    // Test health endpoint
    let response = client
        .get("http://127.0.0.1:14318/health")
        .send()
        .await
        .expect("Failed to send health check request");

    assert!(response.status().is_success());
    let body = response.text().await.unwrap();
    assert_eq!(body, "OK");
}
