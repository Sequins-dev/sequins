//! Management API HTTP server
//!
//! Provides REST HTTP endpoints for the `ManagementApi` trait:
//! - `POST /api/retention/cleanup` — run retention cleanup
//! - `GET  /api/retention/policy`  — get retention policy
//! - `PUT  /api/retention/policy`  — update retention policy
//! - `POST /api/maintenance`       — run maintenance
//! - `GET  /api/storage/stats`     — storage statistics
//! - `GET  /health`                — health check

use super::error::Error;
use axum::{
    extract::State,
    routing::{get, post, put},
    Json, Router,
};
use sequins_types::ManagementApi;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// HTTP server for ManagementApi
pub struct ManagementServer {
    management_api: Arc<dyn ManagementApi>,
}

impl ManagementServer {
    /// Create a new ManagementServer wrapping a ManagementApi implementation
    pub fn new<M: ManagementApi + 'static>(management_api: Arc<M>) -> Self {
        Self {
            management_api: management_api as Arc<dyn ManagementApi>,
        }
    }

    /// Build the axum router with all management endpoints
    pub fn router(&self) -> Router {
        Router::new()
            .route("/api/retention/cleanup", post(run_retention_cleanup))
            .route("/api/retention/policy", get(get_retention_policy))
            .route("/api/retention/policy", put(update_retention_policy))
            .route("/api/maintenance", post(run_maintenance))
            .route("/api/storage/stats", get(get_storage_stats))
            .route("/health", get(health_check))
            .layer(CorsLayer::permissive())
            .with_state(Arc::clone(&self.management_api))
    }

    /// Serve the management API on the given address
    pub async fn serve(self, addr: &str) -> crate::Result<()> {
        let app = self.router();
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::Http(format!("Failed to bind to {addr}: {e}")))?;

        tracing::info!("ManagementServer listening on {}", addr);

        axum::serve(listener, app)
            .await
            .map_err(|e| Error::Http(format!("Server error: {e}")))?;

        Ok(())
    }
}

async fn health_check() -> &'static str {
    "OK"
}

async fn run_retention_cleanup(
    State(api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<serde_json::Value>, Error> {
    let deleted_count = api.run_retention_cleanup().await?;
    Ok(Json(serde_json::json!({ "deleted_count": deleted_count })))
}

async fn get_retention_policy(
    State(api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<sequins_types::models::RetentionPolicy>, Error> {
    let policy = api.get_retention_policy().await?;
    Ok(Json(policy))
}

async fn update_retention_policy(
    State(api): State<Arc<dyn ManagementApi>>,
    Json(policy): Json<sequins_types::models::RetentionPolicy>,
) -> Result<Json<serde_json::Value>, Error> {
    api.update_retention_policy(policy).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn run_maintenance(
    State(api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<sequins_types::models::MaintenanceStats>, Error> {
    let stats = api.run_maintenance().await?;
    Ok(Json(stats))
}

async fn get_storage_stats(
    State(api): State<Arc<dyn ManagementApi>>,
) -> Result<Json<sequins_types::models::StorageStats>, Error> {
    let stats = api.get_storage_stats().await?;
    Ok(Json(stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use sequins_types::models::{Duration, MaintenanceStats, RetentionPolicy, StorageStats};
    use tower::ServiceExt;

    // Mock ManagementApi implementation for testing
    struct MockManagementApi {
        should_fail: bool,
    }

    #[async_trait::async_trait]
    impl ManagementApi for MockManagementApi {
        async fn run_retention_cleanup(&self) -> Result<usize, sequins_types::Error> {
            if self.should_fail {
                return Err(sequins_types::Error::Other("Cleanup failed".to_string()));
            }
            Ok(42)
        }

        async fn get_retention_policy(&self) -> Result<RetentionPolicy, sequins_types::Error> {
            if self.should_fail {
                return Err(sequins_types::Error::Other(
                    "Failed to get policy".to_string(),
                ));
            }
            Ok(RetentionPolicy {
                spans_retention: Duration::from_hours(24 * 7),
                logs_retention: Duration::from_hours(24 * 7),
                metrics_retention: Duration::from_hours(24 * 7),
                profiles_retention: Duration::from_hours(24 * 7),
            })
        }

        async fn update_retention_policy(
            &self,
            _policy: RetentionPolicy,
        ) -> Result<(), sequins_types::Error> {
            if self.should_fail {
                return Err(sequins_types::Error::Other(
                    "Failed to update policy".to_string(),
                ));
            }
            Ok(())
        }

        async fn run_maintenance(&self) -> Result<MaintenanceStats, sequins_types::Error> {
            if self.should_fail {
                return Err(sequins_types::Error::Other(
                    "Maintenance failed".to_string(),
                ));
            }
            Ok(MaintenanceStats {
                entries_evicted: 5,
                batches_flushed: 10,
            })
        }

        async fn get_storage_stats(&self) -> Result<StorageStats, sequins_types::Error> {
            if self.should_fail {
                return Err(sequins_types::Error::Other(
                    "Failed to get stats".to_string(),
                ));
            }
            Ok(StorageStats {
                span_count: 100,
                log_count: 200,
                metric_count: 50,
                profile_count: 10,
            })
        }
    }

    #[tokio::test]
    async fn test_get_storage_stats() {
        let api = Arc::new(MockManagementApi { should_fail: false });
        let server = ManagementServer::new(api);
        let app = server.router();

        let request = Request::builder()
            .method("GET")
            .uri("/api/storage/stats")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify response contains stats
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: StorageStats = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(stats.span_count, 100);
        assert_eq!(stats.log_count, 200);
        assert_eq!(stats.metric_count, 50);
        assert_eq!(stats.profile_count, 10);
    }

    #[tokio::test]
    async fn test_get_health_status() {
        let api = Arc::new(MockManagementApi { should_fail: false });
        let server = ManagementServer::new(api);
        let app = server.router();

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body_bytes[..], b"OK");
    }

    #[tokio::test]
    async fn test_get_retention_policy() {
        let api = Arc::new(MockManagementApi { should_fail: false });
        let server = ManagementServer::new(api);
        let app = server.router();

        let request = Request::builder()
            .method("GET")
            .uri("/api/retention/policy")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let policy: RetentionPolicy = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(policy.spans_retention, Duration::from_hours(24 * 7));
    }

    #[tokio::test]
    async fn test_set_retention_policy() {
        let api = Arc::new(MockManagementApi { should_fail: false });
        let server = ManagementServer::new(api);
        let app = server.router();

        let new_policy = RetentionPolicy {
            spans_retention: Duration::from_hours(48),
            logs_retention: Duration::from_hours(48),
            metrics_retention: Duration::from_hours(48),
            profiles_retention: Duration::from_hours(48),
        };

        let request = Request::builder()
            .method("PUT")
            .uri("/api/retention/policy")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&new_policy).unwrap()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_run_retention_cleanup() {
        let api = Arc::new(MockManagementApi { should_fail: false });
        let server = ManagementServer::new(api);
        let app = server.router();

        let request = Request::builder()
            .method("POST")
            .uri("/api/retention/cleanup")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(result["deleted_count"], 42);
    }

    #[tokio::test]
    async fn test_run_maintenance() {
        let api = Arc::new(MockManagementApi { should_fail: false });
        let server = ManagementServer::new(api);
        let app = server.router();

        let request = Request::builder()
            .method("POST")
            .uri("/api/maintenance")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let stats: MaintenanceStats = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(stats.entries_evicted, 5);
        assert_eq!(stats.batches_flushed, 10);
    }

    #[tokio::test]
    async fn test_storage_stats_error() {
        let api = Arc::new(MockManagementApi { should_fail: true });
        let server = ManagementServer::new(api);
        let app = server.router();

        let request = Request::builder()
            .method("GET")
            .uri("/api/storage/stats")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
