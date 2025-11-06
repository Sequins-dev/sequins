//! Query API HTTP server
//!
//! Provides REST HTTP endpoints for the `QueryApi` trait.

use crate::error::Error;
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use sequins_core::{
    models::{
        LogEntry, LogId, LogQuery, Metric, MetricId, MetricQuery, Profile, ProfileId, ProfileQuery,
        QueryTrace, Service, Span, SpanId, TraceId, TraceQuery,
    },
    traits::QueryApi,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// HTTP server for QueryApi
///
/// Uses trait objects for simplicity. May be optimized to use generics in the future.
pub struct QueryServer {
    query_api: Arc<dyn QueryApi>,
}

impl QueryServer {
    /// Create a new QueryServer wrapping a QueryApi implementation
    pub fn new<Q: QueryApi + 'static>(query_api: Arc<Q>) -> Self {
        Self { query_api }
    }

    /// Build the axum router with all query endpoints
    pub fn router(&self) -> Router {
        Router::new()
            // Service endpoints
            .route("/api/services", get(get_services))
            // Trace endpoints
            .route("/api/traces", post(query_traces))
            .route("/api/traces/:trace_id/spans", get(get_spans))
            .route("/api/traces/:trace_id/spans/:span_id", get(get_span))
            // Log endpoints
            .route("/api/logs", post(query_logs))
            .route("/api/logs/:log_id", get(get_log))
            // Metric endpoints
            .route("/api/metrics", post(query_metrics))
            .route("/api/metrics/:metric_id", get(get_metric))
            // Profile endpoints
            .route("/api/profiles", post(get_profiles))
            .route("/api/profiles/:profile_id", get(get_profile))
            // Health check
            .route("/health", get(health_check))
            .layer(CorsLayer::permissive())
            .with_state(Arc::clone(&self.query_api))
    }

    /// Serve the query API on the given address
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind or serve
    pub async fn serve(self, addr: &str) -> crate::Result<()> {
        let app = self.router();
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::Http(format!("Failed to bind to {}: {}", addr, e)))?;

        tracing::info!("QueryServer listening on {}", addr);

        axum::serve(listener, app)
            .await
            .map_err(|e| Error::Http(format!("Server error: {}", e)))?;

        Ok(())
    }
}

// Handler functions

async fn health_check() -> &'static str {
    "OK"
}

async fn get_services(
    State(query_api): State<Arc<dyn QueryApi>>,
) -> Result<Json<Vec<Service>>, Error> {
    let services = query_api.get_services().await?;
    Ok(Json(services))
}

async fn query_traces(
    State(query_api): State<Arc<dyn QueryApi>>,
    Json(query): Json<TraceQuery>,
) -> Result<Json<Vec<QueryTrace>>, Error> {
    let traces = query_api.query_traces(query).await?;
    Ok(Json(traces))
}

async fn get_spans(
    State(query_api): State<Arc<dyn QueryApi>>,
    Path(trace_id): Path<String>,
) -> Result<Json<Vec<Span>>, Error> {
    let trace_id = TraceId::from_hex(&trace_id)
        .map_err(|e| Error::InvalidRequest(format!("Invalid trace ID: {}", e)))?;
    let spans = query_api.get_spans(trace_id).await?;
    Ok(Json(spans))
}

async fn get_span(
    State(query_api): State<Arc<dyn QueryApi>>,
    Path((trace_id, span_id)): Path<(String, String)>,
) -> Result<Json<Option<Span>>, Error> {
    let trace_id = TraceId::from_hex(&trace_id)
        .map_err(|e| Error::InvalidRequest(format!("Invalid trace ID: {}", e)))?;
    let span_id = SpanId::from_hex(&span_id)
        .map_err(|e| Error::InvalidRequest(format!("Invalid span ID: {}", e)))?;
    let span = query_api.get_span(trace_id, span_id).await?;
    Ok(Json(span))
}

async fn query_logs(
    State(query_api): State<Arc<dyn QueryApi>>,
    Json(query): Json<LogQuery>,
) -> Result<Json<Vec<LogEntry>>, Error> {
    let logs = query_api.query_logs(query).await?;
    Ok(Json(logs))
}

async fn get_log(
    State(query_api): State<Arc<dyn QueryApi>>,
    Path(log_id): Path<String>,
) -> Result<Json<Option<LogEntry>>, Error> {
    let log_id = LogId::from_hex(&log_id)
        .map_err(|e| Error::InvalidRequest(format!("Invalid log ID: {}", e)))?;
    let log = query_api.get_log(log_id).await?;
    Ok(Json(log))
}

async fn query_metrics(
    State(query_api): State<Arc<dyn QueryApi>>,
    Json(query): Json<MetricQuery>,
) -> Result<Json<Vec<Metric>>, Error> {
    let metrics = query_api.query_metrics(query).await?;
    Ok(Json(metrics))
}

async fn get_metric(
    State(query_api): State<Arc<dyn QueryApi>>,
    Path(metric_id): Path<String>,
) -> Result<Json<Option<Metric>>, Error> {
    let metric_id = MetricId::from_hex(&metric_id)
        .map_err(|e| Error::InvalidRequest(format!("Invalid metric ID: {}", e)))?;
    let metric = query_api.get_metric(metric_id).await?;
    Ok(Json(metric))
}

async fn get_profiles(
    State(query_api): State<Arc<dyn QueryApi>>,
    Json(query): Json<ProfileQuery>,
) -> Result<Json<Vec<Profile>>, Error> {
    let profiles = query_api.get_profiles(query).await?;
    Ok(Json(profiles))
}

async fn get_profile(
    State(query_api): State<Arc<dyn QueryApi>>,
    Path(profile_id): Path<String>,
) -> Result<Json<Option<Profile>>, Error> {
    let profile_id = ProfileId::from_hex(&profile_id)
        .map_err(|e| Error::InvalidRequest(format!("Invalid profile ID: {}", e)))?;
    let profile = query_api.get_profile(profile_id).await?;
    Ok(Json(profile))
}
