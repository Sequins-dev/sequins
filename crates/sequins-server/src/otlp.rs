//! OTLP ingestion server
//!
//! Provides both gRPC (port 4317) and HTTP/JSON (port 4318) endpoints for
//! OpenTelemetry Protocol (OTLP) ingestion.
//!
//! # Standards Compliance
//!
//! - gRPC endpoints: Full OTLP protobuf support for traces, logs, and metrics
//! - HTTP/JSON endpoints: Simplified JSON API for ease of use
//! - Profiles: Sequins extension (HTTP/JSON only, not part of OTLP spec)

use crate::error::Error;
use crate::otlp_conversions::{convert_otlp_log, convert_otlp_metric, convert_otlp_span};
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
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
    trace::v1::{
        trace_service_server::{TraceService, TraceServiceServer},
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
};
use sequins_core::{
    models::{LogEntry, Metric, Profile, Span},
    traits::OtlpIngest,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use tower_http::cors::CorsLayer;

/// OTLP server for ingesting telemetry data
///
/// Uses trait objects for simplicity. May be optimized to use generics in the future.
pub struct OtlpServer {
    ingest: Arc<dyn OtlpIngest>,
}

impl OtlpServer {
    /// Create a new OtlpServer wrapping an OtlpIngest implementation
    pub fn new<I: OtlpIngest + 'static>(ingest: Arc<I>) -> Self {
        Self { ingest }
    }

    /// Build the axum router with OTLP endpoints
    ///
    /// Standard OTLP HTTP/JSON endpoints:
    /// - POST /v1/traces - Ingest trace spans
    /// - POST /v1/logs - Ingest logs
    /// - POST /v1/metrics - Ingest metrics
    /// - POST /v1/profiles - Ingest profiles (non-standard, Sequins extension)
    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/traces", post(ingest_spans))
            .route("/v1/logs", post(ingest_logs))
            .route("/v1/metrics", post(ingest_metrics))
            .route("/v1/profiles", post(ingest_profiles))
            .route("/health", get(health_check))
            .layer(CorsLayer::permissive())
            .with_state(Arc::clone(&self.ingest))
    }

    /// Serve both gRPC and HTTP OTLP endpoints
    ///
    /// # Arguments
    ///
    /// * `grpc_addr` - gRPC endpoint address (e.g., "0.0.0.0:4317")
    /// * `http_addr` - HTTP/JSON endpoint address (e.g., "0.0.0.0:4318")
    ///
    /// # Errors
    ///
    /// Returns an error if either server fails to bind or serve
    pub async fn serve(self, grpc_addr: &str, http_addr: &str) -> crate::Result<()> {
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

        // Build gRPC server
        let grpc_addr_parsed = grpc_addr
            .parse()
            .map_err(|e| Error::Http(format!("Invalid gRPC address: {}", e)))?;

        let grpc_server = Server::builder()
            .add_service(TraceServiceServer::new(trace_service))
            .add_service(LogsServiceServer::new(logs_service))
            .add_service(MetricsServiceServer::new(metrics_service))
            .serve(grpc_addr_parsed);

        // Build HTTP server
        let http_app = self.router();
        let http_listener = tokio::net::TcpListener::bind(http_addr)
            .await
            .map_err(|e| Error::Http(format!("Failed to bind to {}: {}", http_addr, e)))?;

        tracing::info!("OtlpServer (gRPC) listening on {}", grpc_addr);
        tracing::info!("OtlpServer (HTTP) listening on {}", http_addr);

        // Run both servers concurrently
        tokio::select! {
            result = grpc_server => {
                result.map_err(|e| Error::Http(format!("gRPC server error: {}", e)))?;
            }
            result = axum::serve(http_listener, http_app) => {
                result.map_err(|e| Error::Http(format!("HTTP server error: {}", e)))?;
            }
        }

        Ok(())
    }
}

// ============================================================================
// gRPC Service Implementations
// ============================================================================

/// gRPC TraceService implementation
struct OtlpTraceService {
    ingest: Arc<dyn OtlpIngest>,
}

#[tonic::async_trait]
impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let req = request.into_inner();

        // Convert OTLP spans to Sequins spans
        let mut spans = Vec::new();
        for resource_span in req.resource_spans {
            for scope_span in resource_span.scope_spans {
                for otlp_span in scope_span.spans {
                    match convert_otlp_span(otlp_span, resource_span.resource.as_ref()) {
                        Ok(span) => spans.push(span),
                        Err(e) => {
                            tracing::warn!("Failed to convert OTLP span: {}", e);
                            continue;
                        }
                    }
                }
            }
        }

        // Ingest the spans
        self.ingest
            .ingest_spans(spans)
            .await
            .map_err(|e| Status::internal(format!("Failed to ingest spans: {}", e)))?;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

/// gRPC LogsService implementation
struct OtlpLogsService {
    ingest: Arc<dyn OtlpIngest>,
}

#[tonic::async_trait]
impl LogsService for OtlpLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();

        // Convert OTLP logs to Sequins logs
        let mut logs = Vec::new();
        for resource_log in req.resource_logs {
            for scope_log in resource_log.scope_logs {
                for otlp_log in scope_log.log_records {
                    match convert_otlp_log(otlp_log, resource_log.resource.as_ref()) {
                        Ok(log) => logs.push(log),
                        Err(e) => {
                            tracing::warn!("Failed to convert OTLP log: {}", e);
                            continue;
                        }
                    }
                }
            }
        }

        // Ingest the logs
        self.ingest
            .ingest_logs(logs)
            .await
            .map_err(|e| Status::internal(format!("Failed to ingest logs: {}", e)))?;

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

/// gRPC MetricsService implementation
struct OtlpMetricsService {
    ingest: Arc<dyn OtlpIngest>,
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();

        // Convert OTLP metrics to Sequins metrics
        let mut metrics = Vec::new();
        for resource_metric in req.resource_metrics {
            for scope_metric in resource_metric.scope_metrics {
                for otlp_metric in scope_metric.metrics {
                    match convert_otlp_metric(otlp_metric, resource_metric.resource.as_ref()) {
                        Ok(metric) => metrics.push(metric),
                        Err(e) => {
                            tracing::warn!("Failed to convert OTLP metric: {}", e);
                            continue;
                        }
                    }
                }
            }
        }

        // Ingest the metrics
        self.ingest
            .ingest_metrics(metrics)
            .await
            .map_err(|e| Status::internal(format!("Failed to ingest metrics: {}", e)))?;

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

// Handler functions

async fn health_check() -> &'static str {
    "OK"
}

/// Simplified span ingestion request
///
/// In a full OTLP implementation, this would use the protobuf-generated types
/// from opentelemetry-proto. For now, we accept our own Span type.
#[derive(Debug, Serialize, Deserialize)]
struct IngestSpansRequest {
    spans: Vec<Span>,
}

async fn ingest_spans(
    State(ingest): State<Arc<dyn OtlpIngest>>,
    Json(request): Json<IngestSpansRequest>,
) -> Result<Json<serde_json::Value>, Error> {
    ingest.ingest_spans(request.spans).await?;
    Ok(Json(serde_json::json!({ "success": true })))
}

/// Simplified log ingestion request
#[derive(Debug, Serialize, Deserialize)]
struct IngestLogsRequest {
    logs: Vec<LogEntry>,
}

async fn ingest_logs(
    State(ingest): State<Arc<dyn OtlpIngest>>,
    Json(request): Json<IngestLogsRequest>,
) -> Result<Json<serde_json::Value>, Error> {
    ingest.ingest_logs(request.logs).await?;
    Ok(Json(serde_json::json!({ "success": true })))
}

/// Simplified metric ingestion request
#[derive(Debug, Serialize, Deserialize)]
struct IngestMetricsRequest {
    metrics: Vec<Metric>,
}

async fn ingest_metrics(
    State(ingest): State<Arc<dyn OtlpIngest>>,
    Json(request): Json<IngestMetricsRequest>,
) -> Result<Json<serde_json::Value>, Error> {
    ingest.ingest_metrics(request.metrics).await?;
    Ok(Json(serde_json::json!({ "success": true })))
}

/// Simplified profile ingestion request
#[derive(Debug, Serialize, Deserialize)]
struct IngestProfilesRequest {
    profiles: Vec<Profile>,
}

async fn ingest_profiles(
    State(ingest): State<Arc<dyn OtlpIngest>>,
    Json(request): Json<IngestProfilesRequest>,
) -> Result<Json<serde_json::Value>, Error> {
    ingest.ingest_profiles(request.profiles).await?;
    Ok(Json(serde_json::json!({ "success": true })))
}
