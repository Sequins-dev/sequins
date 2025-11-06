//! Management API HTTP server
//!
//! Provides REST HTTP endpoints for the `ManagementApi` trait.

use crate::error::Error;
use axum::{
    extract::State,
    routing::{get, post, put},
    Json, Router,
};
use sequins_core::{
    models::{MaintenanceStats, RetentionPolicy, StorageStats},
    traits::ManagementApi,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// HTTP server for ManagementApi
///
/// Uses trait objects for simplicity. May be optimized to use generics in the future.
pub struct ManagementServer {
    management_api: Arc<dyn ManagementApi>,
}

impl ManagementServer {
    /// Create a new ManagementServer wrapping a ManagementApi implementation
    pub fn new<M: ManagementApi + 'static>(management_api: Arc<M>) -> Self {
        Self { management_api }
    }

    /// Build the axum router with all management endpoints
    pub fn router(&self) -> Router {
        Router::new()
            // Retention management
            .route("/api/retention/cleanup", post(run_retention_cleanup))
            .route("/api/retention/policy", get(get_retention_policy))
            .route("/api/retention/policy", put(update_retention_policy))
            // Database maintenance
            .route("/api/maintenance", post(run_maintenance))
            .route("/api/storage/stats", get(get_storage_stats))
            // Health check
            .route("/health", get(health_check))
            .layer(CorsLayer::permissive())
            .with_state(Arc::clone(&self.management_api))
    }

    /// Serve the management API on the given address
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to bind or serve
    pub async fn serve(self, addr: &str) -> crate::Result<()> {
        let app = self.router();
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::Http(format!("Failed to bind to {}: {}", addr, e)))?;

        tracing::info!("ManagementServer listening on {}", addr);

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

async fn run_retention_cleanup(
    State(management_api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<serde_json::Value>, Error> {
    let deleted_count = management_api.run_retention_cleanup().await?;
    Ok(Json(serde_json::json!({ "deleted_count": deleted_count })))
}

async fn get_retention_policy(
    State(management_api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<RetentionPolicy>, Error> {
    let policy = management_api.get_retention_policy().await?;
    Ok(Json(policy))
}

async fn update_retention_policy(
    State(management_api): State<Arc<dyn ManagementApi>>,
    Json(policy): Json<RetentionPolicy>,
) -> Result<Json<serde_json::Value>, Error> {
    management_api.update_retention_policy(policy).await?;
    Ok(Json(serde_json::json!({ "success": true })))
}

async fn run_maintenance(
    State(management_api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<MaintenanceStats>, Error> {
    let stats = management_api.run_maintenance().await?;
    Ok(Json(stats))
}

async fn get_storage_stats(
    State(management_api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<StorageStats>, Error> {
    let stats = management_api.get_storage_stats().await?;
    Ok(Json(stats))
}
