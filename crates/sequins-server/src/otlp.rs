//! OTLP ingestion server (thin pass-through layer)
//!
//! Provides both gRPC (port 4317) and HTTP (port 4318) endpoints for
//! OpenTelemetry Protocol (OTLP) ingestion. All conversion and decomposition
//! logic is handled by the storage layer - this layer simply decodes requests
//! and encodes responses.
//!
//! # Standards Compliance
//!
//! - gRPC endpoints: Full OTLP protobuf support for traces, logs, metrics, and profiles
//! - HTTP endpoints: Support both protobuf and JSON encoding (based on Content-Type/Accept headers)

use crate::error::{Error, Result};
use axum::{
    body::Bytes,
    extract::{Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        logs_service_server::{LogsService, LogsServiceServer},
        ExportLogsServiceRequest, ExportLogsServiceResponse,
    },
    metrics::v1::{
        metrics_service_server::{MetricsService, MetricsServiceServer},
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    profiles::v1development::{
        profiles_service_server::{ProfilesService, ProfilesServiceServer},
        ExportProfilesServiceRequest, ExportProfilesServiceResponse,
    },
    trace::v1::{
        trace_service_server::{TraceService, TraceServiceServer},
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
};
use prost::Message as ProstMessage;
use sequins_types::OtlpIngest;
use std::sync::Arc;
use tonic::{transport::Server, Request as TonicRequest, Response as TonicResponse, Status};
use tower_http::cors::CorsLayer;

/// OTLP server for ingesting telemetry data
///
/// Generic over the ingestion implementation for zero-cost abstractions.
pub struct OtlpServer<I: OtlpIngest> {
    ingest: Arc<I>,
}

impl<I: OtlpIngest + 'static> OtlpServer<I> {
    /// Create a new OtlpServer wrapping an OtlpIngest implementation
    pub fn new(ingest: Arc<I>) -> Self {
        Self { ingest }
    }

    /// Build the axum router with OTLP endpoints
    ///
    /// Standard OTLP HTTP endpoints (per OTLP spec):
    /// - POST /v1/traces           - Ingest trace spans (stable)
    /// - POST /v1/logs             - Ingest logs (stable)
    /// - POST /v1/metrics          - Ingest metrics (stable)
    /// - POST /v1development/profiles - Ingest profiles (development)
    pub fn router(&self) -> Router {
        // Convert to trait object for axum state compatibility
        let ingest: Arc<dyn OtlpIngest> = self.ingest.clone() as Arc<dyn OtlpIngest>;

        Router::new()
            .route("/v1/traces", post(ingest_traces_http))
            .route("/v1/logs", post(ingest_logs_http))
            .route("/v1/metrics", post(ingest_metrics_http))
            .route("/v1development/profiles", post(ingest_profiles_http))
            .route("/health", get(health_check))
            .layer(CorsLayer::permissive())
            .with_state(ingest)
    }

    /// Serve both gRPC and HTTP OTLP endpoints
    ///
    /// # Arguments
    ///
    /// * `grpc_addr` - gRPC endpoint address (e.g., "0.0.0.0:4317")
    /// * `http_addr` - HTTP endpoint address (e.g., "0.0.0.0:4318")
    ///
    /// # Errors
    ///
    /// Returns an error if either server fails to bind or serve
    pub async fn serve(self, grpc_addr: &str, http_addr: &str) -> Result<()> {
        self.serve_inner(grpc_addr, http_addr, None).await
    }

    /// Like [`serve`], but signals `ready_tx` once the HTTP listener is bound.
    ///
    /// Sends `Ok(())` on success or `Err(message)` if the HTTP bind fails.
    /// Uses a stdlib `mpsc::Sender` so the caller can block via `recv_timeout`
    /// without entering a second tokio async context.
    pub async fn serve_with_ready(
        self,
        grpc_addr: &str,
        http_addr: &str,
        ready_tx: std::sync::mpsc::Sender<std::result::Result<(), String>>,
    ) -> Result<()> {
        self.serve_inner(grpc_addr, http_addr, Some(ready_tx)).await
    }

    async fn serve_inner(
        self,
        grpc_addr: &str,
        http_addr: &str,
        ready_tx: Option<std::sync::mpsc::Sender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        tracing::info!(
            "Starting OTLP server with gRPC={}, HTTP={}",
            grpc_addr,
            http_addr
        );
        let ingest = Arc::clone(&self.ingest);

        // Clone for gRPC services
        let trace_service = OtlpTraceService {
            ingest: Arc::clone(&ingest),
        };
        let logs_service = OtlpLogsService {
            ingest: Arc::clone(&ingest),
        };
        let metrics_service = OtlpMetricsService {
            ingest: Arc::clone(&ingest),
        };
        let profiles_service = OtlpProfilesService {
            ingest: Arc::clone(&ingest),
        };

        // Build gRPC server
        let grpc_addr_parsed = grpc_addr
            .parse()
            .map_err(|e| Error::Grpc(format!("Invalid gRPC address: {}", e)))?;

        let grpc_server = Server::builder()
            .add_service(TraceServiceServer::new(trace_service))
            .add_service(LogsServiceServer::new(logs_service))
            .add_service(MetricsServiceServer::new(metrics_service))
            .add_service(ProfilesServiceServer::new(profiles_service))
            .serve(grpc_addr_parsed);

        // Build HTTP server — bind before signalling ready so callers know the port is live
        let http_app = self.router();
        let http_listener = match tokio::net::TcpListener::bind(http_addr).await {
            Ok(l) => {
                if let Some(tx) = ready_tx {
                    let _ = tx.send(Ok(()));
                }
                l
            }
            Err(e) => {
                let msg = format!("Failed to bind to {}: {}", http_addr, e);
                if let Some(tx) = ready_tx {
                    let _ = tx.send(Err(msg.clone()));
                }
                return Err(Error::Http(format!(
                    "Failed to bind to {}: {}",
                    http_addr, e
                )));
            }
        };

        tracing::info!("gRPC listening on {}", grpc_addr);
        tracing::info!("HTTP listening on {}", http_addr);

        // Run both servers concurrently
        tokio::select! {
            result = grpc_server => {
                result.map_err(|e| Error::Grpc(format!("gRPC server error: {}", e)))?;
            }
            result = axum::serve(http_listener, http_app) => {
                result.map_err(|e| Error::Http(format!("HTTP server error: {}", e)))?;
            }
        }

        Ok(())
    }
}

// ============================================================================
// gRPC Service Implementations (thin pass-through)
// ============================================================================

/// gRPC TraceService implementation
struct OtlpTraceService<I: OtlpIngest> {
    ingest: Arc<I>,
}

#[tonic::async_trait]
impl<I: OtlpIngest + 'static> TraceService for OtlpTraceService<I> {
    async fn export(
        &self,
        request: TonicRequest<ExportTraceServiceRequest>,
    ) -> std::result::Result<TonicResponse<ExportTraceServiceResponse>, Status> {
        tracing::debug!("TraceService::export() - gRPC request received");
        let req = request.into_inner();
        tracing::debug!("Received {} resource_spans", req.resource_spans.len());

        // Pass through to storage layer
        let resp = self
            .ingest
            .ingest_traces(req)
            .await
            .map_err(|e| Status::internal(format!("Failed to ingest traces: {}", e)))?;

        tracing::debug!("TraceService::export() completed successfully");
        Ok(TonicResponse::new(resp))
    }
}

/// gRPC LogsService implementation
struct OtlpLogsService<I: OtlpIngest> {
    ingest: Arc<I>,
}

#[tonic::async_trait]
impl<I: OtlpIngest + 'static> LogsService for OtlpLogsService<I> {
    async fn export(
        &self,
        request: TonicRequest<ExportLogsServiceRequest>,
    ) -> std::result::Result<TonicResponse<ExportLogsServiceResponse>, Status> {
        tracing::debug!("LogsService::export() - gRPC request received");
        let req = request.into_inner();
        tracing::debug!("Received {} resource_logs", req.resource_logs.len());

        // Pass through to storage layer
        let resp = self
            .ingest
            .ingest_logs(req)
            .await
            .map_err(|e| Status::internal(format!("Failed to ingest logs: {}", e)))?;

        tracing::debug!("LogsService::export() completed successfully");
        Ok(TonicResponse::new(resp))
    }
}

/// gRPC MetricsService implementation
struct OtlpMetricsService<I: OtlpIngest> {
    ingest: Arc<I>,
}

#[tonic::async_trait]
impl<I: OtlpIngest + 'static> MetricsService for OtlpMetricsService<I> {
    async fn export(
        &self,
        request: TonicRequest<ExportMetricsServiceRequest>,
    ) -> std::result::Result<TonicResponse<ExportMetricsServiceResponse>, Status> {
        tracing::debug!("MetricsService::export() - gRPC request received");
        let req = request.into_inner();
        tracing::debug!("Received {} resource_metrics", req.resource_metrics.len());

        // Pass through to storage layer
        let resp = self
            .ingest
            .ingest_metrics(req)
            .await
            .map_err(|e| Status::internal(format!("Failed to ingest metrics: {}", e)))?;

        tracing::debug!("MetricsService::export() completed successfully");
        Ok(TonicResponse::new(resp))
    }
}

/// gRPC ProfilesService implementation (v1development)
struct OtlpProfilesService<I: OtlpIngest> {
    ingest: Arc<I>,
}

#[tonic::async_trait]
impl<I: OtlpIngest + 'static> ProfilesService for OtlpProfilesService<I> {
    async fn export(
        &self,
        request: TonicRequest<ExportProfilesServiceRequest>,
    ) -> std::result::Result<TonicResponse<ExportProfilesServiceResponse>, tonic::Status> {
        tracing::debug!("ProfilesService::export() - gRPC request received");
        let req = request.into_inner();

        // Pass through to storage layer
        let resp =
            self.ingest.ingest_profiles(req).await.map_err(|e| {
                tonic::Status::internal(format!("Failed to ingest profiles: {}", e))
            })?;

        tracing::debug!("ProfilesService::export() completed successfully");
        Ok(TonicResponse::new(resp))
    }
}

// ============================================================================
// HTTP Handler functions (thin pass-through)
// ============================================================================
//
// All endpoints implement the OTLP HTTP specification:
//   - Content-Type: application/x-protobuf  → binary protobuf decode
//   - Content-Type: application/json        → protobuf JSON (camelCase fields)
//   - Accept: application/x-protobuf        → binary protobuf encode
//   - Accept: application/json              → protobuf JSON (camelCase fields)

async fn health_check() -> &'static str {
    "OK"
}

/// Decode bytes as OTLP binary protobuf or JSON depending on Content-Type.
#[allow(clippy::result_large_err)]
fn decode_request<T>(content_type: &str, bytes: &Bytes) -> std::result::Result<T, Response>
where
    T: ProstMessage + Default + serde::de::DeserializeOwned,
{
    if content_type.contains("application/x-protobuf")
        || content_type.contains("application/octet-stream")
    {
        T::decode(bytes.as_ref()).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to decode protobuf: {}", e),
            )
                .into_response()
        })
    } else {
        // application/json — protobuf JSON mapping (camelCase fields)
        serde_json::from_slice(bytes).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to deserialize JSON: {}", e),
            )
                .into_response()
        })
    }
}

/// Encode response as OTLP binary protobuf or JSON depending on Accept header.
fn encode_response<T>(accept: Option<&str>, message: T) -> Response
where
    T: ProstMessage + serde::Serialize,
{
    let accept = accept.unwrap_or("application/json");

    if accept.contains("application/x-protobuf") || accept.contains("application/octet-stream") {
        // Encode as binary protobuf
        let bytes = message.encode_to_vec();
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            bytes,
        )
            .into_response()
    } else {
        // Encode as JSON
        match serde_json::to_string(&message) {
            Ok(json) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                json,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to serialize response: {}", e),
            )
                .into_response(),
        }
    }
}

/// POST /v1/traces — OTLP HTTP trace ingestion (protobuf binary or JSON)
async fn ingest_traces_http(
    State(ingest): State<Arc<dyn OtlpIngest>>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();

    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_owned();

    let accept = parts
        .headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok());

    let bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to read body: {}", e),
            )
                .into_response()
        }
    };

    let req: ExportTraceServiceRequest = match decode_request(&content_type, &bytes) {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Pass through to storage layer
    match ingest.ingest_traces(req).await {
        Ok(resp) => encode_response(accept, resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to ingest traces: {}", e),
        )
            .into_response(),
    }
}

/// POST /v1/logs — OTLP HTTP log ingestion (protobuf binary or JSON)
async fn ingest_logs_http(State(ingest): State<Arc<dyn OtlpIngest>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();

    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_owned();

    let accept = parts
        .headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok());

    let bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to read body: {}", e),
            )
                .into_response()
        }
    };

    let req: ExportLogsServiceRequest = match decode_request(&content_type, &bytes) {
        Ok(r) => r,
        Err(e) => return e,
    };

    tracing::debug!(
        "Received {} resource_logs via HTTP",
        req.resource_logs.len()
    );

    // Pass through to storage layer
    match ingest.ingest_logs(req).await {
        Ok(resp) => encode_response(accept, resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to ingest logs: {}", e),
        )
            .into_response(),
    }
}

/// POST /v1/metrics — OTLP HTTP metric ingestion (protobuf binary or JSON)
async fn ingest_metrics_http(
    State(ingest): State<Arc<dyn OtlpIngest>>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();

    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_owned();

    let accept = parts
        .headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok());

    let bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to read body: {}", e),
            )
                .into_response()
        }
    };

    let req: ExportMetricsServiceRequest = match decode_request(&content_type, &bytes) {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Pass through to storage layer
    match ingest.ingest_metrics(req).await {
        Ok(resp) => encode_response(accept, resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to ingest metrics: {}", e),
        )
            .into_response(),
    }
}

/// POST /v1development/profiles — OTLP HTTP profile ingestion (protobuf binary or JSON)
async fn ingest_profiles_http(
    State(ingest): State<Arc<dyn OtlpIngest>>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();

    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_owned();

    let accept = parts
        .headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok());

    let bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to read body: {}", e),
            )
                .into_response()
        }
    };

    let req: ExportProfilesServiceRequest = match decode_request(&content_type, &bytes) {
        Ok(r) => r,
        Err(e) => return e,
    };

    // Pass through to storage layer
    match ingest.ingest_profiles(req).await {
        Ok(resp) => encode_response(accept, resp),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to ingest profiles: {}", e),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use opentelemetry_proto::tonic::common::v1::{
        any_value::Value as OtlpValue, AnyValue, InstrumentationScope, KeyValue,
    };
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::profiles::v1development::{
        Profile, ResourceProfiles, ScopeProfiles,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
    use tower::ServiceExt;

    // Mock OtlpIngest implementation for testing
    struct MockOtlpIngest;

    #[async_trait::async_trait]
    impl OtlpIngest for MockOtlpIngest {
        async fn ingest_traces(
            &self,
            request: ExportTraceServiceRequest,
        ) -> std::result::Result<ExportTraceServiceResponse, sequins_types::Error> {
            // Validate request has data
            if request.resource_spans.is_empty() {
                return Err(sequins_types::Error::Other("No resource spans".to_string()));
            }
            Ok(ExportTraceServiceResponse {
                partial_success: None,
            })
        }

        async fn ingest_logs(
            &self,
            request: ExportLogsServiceRequest,
        ) -> std::result::Result<ExportLogsServiceResponse, sequins_types::Error> {
            // Validate request has data
            if request.resource_logs.is_empty() {
                return Err(sequins_types::Error::Other("No resource logs".to_string()));
            }
            Ok(ExportLogsServiceResponse {
                partial_success: None,
            })
        }

        async fn ingest_metrics(
            &self,
            request: ExportMetricsServiceRequest,
        ) -> std::result::Result<ExportMetricsServiceResponse, sequins_types::Error> {
            // Validate request has data
            if request.resource_metrics.is_empty() {
                return Err(sequins_types::Error::Other(
                    "No resource metrics".to_string(),
                ));
            }
            Ok(ExportMetricsServiceResponse {
                partial_success: None,
            })
        }

        async fn ingest_profiles(
            &self,
            request: ExportProfilesServiceRequest,
        ) -> std::result::Result<ExportProfilesServiceResponse, sequins_types::Error> {
            // Validate request has data
            if request.resource_profiles.is_empty() {
                return Err(sequins_types::Error::Other(
                    "No resource profiles".to_string(),
                ));
            }
            Ok(ExportProfilesServiceResponse {
                partial_success: None,
            })
        }
    }

    // Helper to create test OTLP traces
    fn make_test_otlp_traces() -> ExportTraceServiceRequest {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue("test-service".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        let scope = InstrumentationScope {
            name: "test-tracer".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        let span = Span {
            trace_id: vec![1u8; 16],
            span_id: vec![1u8; 8],
            trace_state: String::new(),
            parent_span_id: vec![],
            flags: 1,
            name: "test-span".to_string(),
            kind: 2,
            start_time_unix_nano: 1_700_000_000_000_000_000,
            end_time_unix_nano: 1_700_000_100_000_000_000,
            attributes: vec![],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: Some(Status {
                message: String::new(),
                code: 1,
            }),
        };

        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource),
                scope_spans: vec![ScopeSpans {
                    scope: Some(scope),
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    // Helper to create test OTLP logs
    fn make_test_otlp_logs() -> ExportLogsServiceRequest {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue("test-service".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        let scope = InstrumentationScope {
            name: "test-logger".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        let log_record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(OtlpValue::StringValue("Test log".to_string())),
            }),
            attributes: vec![],
            dropped_attributes_count: 0,
            flags: 1,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        };

        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope),
                    log_records: vec![log_record],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    // Helper to create test OTLP metrics
    fn make_test_otlp_metrics() -> ExportMetricsServiceRequest {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue("test-service".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        let scope = InstrumentationScope {
            name: "test-meter".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        let data_point = NumberDataPoint {
            attributes: vec![],
            start_time_unix_nano: 1_700_000_000_000_000_000,
            time_unix_nano: 1_700_000_060_000_000_000,
            value: Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.0),
            ),
            exemplars: vec![],
            flags: 0,
        };

        let metric = Metric {
            name: "test.gauge".to_string(),
            description: "Test gauge metric".to_string(),
            unit: "ms".to_string(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![data_point],
            })),
            metadata: vec![],
        };

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(resource),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope),
                    metrics: vec![metric],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    // Helper to create test OTLP profiles
    fn make_test_otlp_profiles() -> ExportProfilesServiceRequest {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue("test-service".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        let scope = InstrumentationScope {
            name: "test-profiler".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        let profile = Profile {
            profile_id: vec![1u8; 16],
            time_unix_nano: 1_700_000_000_000_000_000,
            duration_nano: 60_000_000_000,
            sample_type: None,
            original_payload: vec![0x1f, 0x8b, 0x08, 0x00],
            original_payload_format: "pprof-gzip".to_string(),
            dropped_attributes_count: 0,
            comment_strindices: vec![],
            attribute_indices: vec![],
            period: 0,
            period_type: None,
            sample: vec![],
        };

        ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                resource: Some(resource),
                scope_profiles: vec![ScopeProfiles {
                    scope: Some(scope),
                    profiles: vec![profile],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
            dictionary: None,
        }
    }

    #[tokio::test]
    async fn test_http_ingest_traces_valid() {
        let server = OtlpServer::new(Arc::new(MockOtlpIngest));
        let app = server.router();

        let request_data = make_test_otlp_traces();
        let body_bytes = request_data.encode_to_vec();

        let request = HttpRequest::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_http_ingest_logs_valid() {
        let server = OtlpServer::new(Arc::new(MockOtlpIngest));
        let app = server.router();

        let request_data = make_test_otlp_logs();
        let body_bytes = request_data.encode_to_vec();

        let request = HttpRequest::builder()
            .method("POST")
            .uri("/v1/logs")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_http_ingest_metrics_valid() {
        let server = OtlpServer::new(Arc::new(MockOtlpIngest));
        let app = server.router();

        let request_data = make_test_otlp_metrics();
        let body_bytes = request_data.encode_to_vec();

        let request = HttpRequest::builder()
            .method("POST")
            .uri("/v1/metrics")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_http_ingest_profiles_valid() {
        let server = OtlpServer::new(Arc::new(MockOtlpIngest));
        let app = server.router();

        let request_data = make_test_otlp_profiles();
        let body_bytes = request_data.encode_to_vec();

        let request = HttpRequest::builder()
            .method("POST")
            .uri("/v1development/profiles")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_http_ingest_malformed_protobuf() {
        let server = OtlpServer::new(Arc::new(MockOtlpIngest));
        let app = server.router();

        // Send invalid protobuf data
        let body_bytes = vec![0xFF, 0xFF, 0xFF, 0xFF];

        let request = HttpRequest::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(body_bytes))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_grpc_export_traces() {
        let ingest = Arc::new(MockOtlpIngest);
        let service = OtlpTraceService {
            ingest: Arc::clone(&ingest),
        };

        let request_data = make_test_otlp_traces();
        let request = TonicRequest::new(request_data);

        let response = service.export(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_export_logs() {
        let ingest = Arc::new(MockOtlpIngest);
        let service = OtlpLogsService {
            ingest: Arc::clone(&ingest),
        };

        let request_data = make_test_otlp_logs();
        let request = TonicRequest::new(request_data);

        let response = service.export(request).await;
        assert!(response.is_ok());
    }
}
