//! Integration tests for ManagementServer HTTP API
//!
//! These tests verify that the ManagementServer correctly exposes ManagementApi methods
//! via HTTP endpoints.

use reqwest::StatusCode;
use sequins::server::ManagementServer;
use sequins::{
    error::Result,
    models::{MaintenanceStats, RetentionPolicy, StorageStats},
    traits::ManagementApi,
};
use serial_test::serial;
use std::sync::{Arc, Mutex};

/// Mock implementation of ManagementApi
#[derive(Clone)]
struct MockManagementApi {
    deleted_count: Arc<Mutex<usize>>,
    policy: Arc<Mutex<RetentionPolicy>>,
    maintenance_stats: Arc<Mutex<MaintenanceStats>>,
    storage_stats: Arc<Mutex<StorageStats>>,
}

impl MockManagementApi {
    fn new() -> Self {
        Self {
            deleted_count: Arc::new(Mutex::new(0)),
            policy: Arc::new(Mutex::new(RetentionPolicy {
                spans_retention: sequins::models::Duration::from_hours(24),
                logs_retention: sequins::models::Duration::from_hours(24),
                metrics_retention: sequins::models::Duration::from_hours(24),
                profiles_retention: sequins::models::Duration::from_hours(24),
            })),
            maintenance_stats: Arc::new(Mutex::new(MaintenanceStats {
                entries_evicted: 0,
                batches_flushed: 0,
            })),
            storage_stats: Arc::new(Mutex::new(StorageStats {
                span_count: 0,
                log_count: 0,
                metric_count: 0,
                profile_count: 0,
            })),
        }
    }

    fn set_deleted_count(&self, count: usize) {
        *self.deleted_count.lock().unwrap() = count;
    }

    fn set_maintenance_stats(&self, stats: MaintenanceStats) {
        *self.maintenance_stats.lock().unwrap() = stats;
    }

    fn set_storage_stats(&self, stats: StorageStats) {
        *self.storage_stats.lock().unwrap() = stats;
    }
}

#[async_trait::async_trait]
impl ManagementApi for MockManagementApi {
    async fn run_retention_cleanup(&self) -> Result<usize> {
        Ok(*self.deleted_count.lock().unwrap())
    }

    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()> {
        *self.policy.lock().unwrap() = policy;
        Ok(())
    }

    async fn get_retention_policy(&self) -> Result<RetentionPolicy> {
        Ok(self.policy.lock().unwrap().clone())
    }

    async fn run_maintenance(&self) -> Result<MaintenanceStats> {
        Ok(self.maintenance_stats.lock().unwrap().clone())
    }

    async fn get_storage_stats(&self) -> Result<StorageStats> {
        Ok(self.storage_stats.lock().unwrap().clone())
    }
}

/// Start the ManagementServer in the background
async fn start_management_server() -> (MockManagementApi, tokio::task::JoinHandle<()>) {
    let mock = MockManagementApi::new();
    let server = ManagementServer::new(Arc::new(mock.clone()));

    let handle = tokio::spawn(async move {
        let app = server.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18081")
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
async fn test_run_retention_cleanup() {
    let (mock, _handle) = start_management_server().await;

    // Set up mock to return specific deleted count
    mock.set_deleted_count(42);

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .post("http://127.0.0.1:18081/api/retention/cleanup")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    #[derive(serde::Deserialize)]
    struct Response {
        deleted_count: usize,
    }

    let result: Response = response.json().await.unwrap();
    assert_eq!(result.deleted_count, 42);
}

#[tokio::test]
#[serial]
async fn test_get_retention_policy() {
    let (mock, _handle) = start_management_server().await;

    // Update the policy
    let new_policy = RetentionPolicy {
        spans_retention: sequins::models::Duration::from_hours(48),
        logs_retention: sequins::models::Duration::from_hours(72),
        metrics_retention: sequins::models::Duration::from_hours(168),
        profiles_retention: sequins::models::Duration::from_hours(24),
    };
    *mock.policy.lock().unwrap() = new_policy.clone();

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18081/api/retention/policy")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let policy: RetentionPolicy = response.json().await.unwrap();
    assert_eq!(
        policy.spans_retention.as_secs(),
        new_policy.spans_retention.as_secs()
    );
    assert_eq!(
        policy.logs_retention.as_secs(),
        new_policy.logs_retention.as_secs()
    );
}

#[tokio::test]
#[serial]
async fn test_update_retention_policy() {
    let (mock, _handle) = start_management_server().await;

    let new_policy = RetentionPolicy {
        spans_retention: sequins::models::Duration::from_hours(96),
        logs_retention: sequins::models::Duration::from_hours(48),
        metrics_retention: sequins::models::Duration::from_hours(336),
        profiles_retention: sequins::models::Duration::from_hours(12),
    };

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .put("http://127.0.0.1:18081/api/retention/policy")
        .json(&new_policy)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify the policy was updated in the mock
    let updated_policy = mock.policy.lock().unwrap().clone();
    assert_eq!(
        updated_policy.spans_retention.as_secs(),
        new_policy.spans_retention.as_secs()
    );
}

#[tokio::test]
#[serial]
async fn test_run_maintenance() {
    let (mock, _handle) = start_management_server().await;

    let stats = MaintenanceStats {
        entries_evicted: 1000,
        batches_flushed: 50,
    };

    mock.set_maintenance_stats(stats.clone());

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .post("http://127.0.0.1:18081/api/maintenance")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let returned_stats: MaintenanceStats = response.json().await.unwrap();
    assert_eq!(returned_stats.entries_evicted, stats.entries_evicted);
    assert_eq!(returned_stats.batches_flushed, stats.batches_flushed);
}

#[tokio::test]
#[serial]
async fn test_get_storage_stats() {
    let (mock, _handle) = start_management_server().await;

    let stats = StorageStats {
        span_count: 10000,
        log_count: 50000,
        metric_count: 5000,
        profile_count: 100,
    };

    mock.set_storage_stats(stats.clone());

    // Make HTTP request
    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18081/api/storage/stats")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let returned_stats: StorageStats = response.json().await.unwrap();
    assert_eq!(returned_stats.span_count, stats.span_count);
    assert_eq!(returned_stats.log_count, stats.log_count);
    assert_eq!(returned_stats.metric_count, stats.metric_count);
    assert_eq!(returned_stats.profile_count, stats.profile_count);
}

#[tokio::test]
#[serial]
async fn test_management_health_check() {
    let (_mock, _handle) = start_management_server().await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18081/health")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert_eq!(body, "OK");
}

// Error handling tests

/// Mock that always returns errors
#[derive(Clone)]
struct ErrorManagementApi;

#[async_trait::async_trait]
impl ManagementApi for ErrorManagementApi {
    async fn run_retention_cleanup(&self) -> Result<usize> {
        Err(sequins::error::Error::Other(
            "Simulated database error".to_string(),
        ))
    }

    async fn update_retention_policy(&self, _policy: RetentionPolicy) -> Result<()> {
        Err(sequins::error::Error::Other(
            "Failed to update policy".to_string(),
        ))
    }

    async fn get_retention_policy(&self) -> Result<RetentionPolicy> {
        Err(sequins::error::Error::Other(
            "Failed to get policy".to_string(),
        ))
    }

    async fn run_maintenance(&self) -> Result<MaintenanceStats> {
        Err(sequins::error::Error::Other(
            "Maintenance failed".to_string(),
        ))
    }

    async fn get_storage_stats(&self) -> Result<StorageStats> {
        Err(sequins::error::Error::Other(
            "Failed to get stats".to_string(),
        ))
    }
}

async fn start_error_management_server() -> tokio::task::JoinHandle<()> {
    let error_api = ErrorManagementApi;
    let server = ManagementServer::new(Arc::new(error_api));

    tokio::spawn(async move {
        let app = server.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18082")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    })
}

#[tokio::test]
#[serial]
async fn test_retention_cleanup_error() {
    let _handle = start_error_management_server().await;

    // Give the server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let response = client
        .post("http://127.0.0.1:18082/api/retention/cleanup")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("Simulated database error"));
}

#[tokio::test]
#[serial]
async fn test_get_retention_policy_error() {
    let _handle = start_error_management_server().await;

    // Give the server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18082/api/retention/policy")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("Failed to get policy"));
}

#[tokio::test]
#[serial]
async fn test_update_retention_policy_error() {
    let _handle = start_error_management_server().await;

    // Give the server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let policy = RetentionPolicy {
        spans_retention: sequins::models::Duration::from_hours(24),
        logs_retention: sequins::models::Duration::from_hours(24),
        metrics_retention: sequins::models::Duration::from_hours(24),
        profiles_retention: sequins::models::Duration::from_hours(24),
    };

    let client = reqwest::Client::new();
    let response = client
        .put("http://127.0.0.1:18082/api/retention/policy")
        .json(&policy)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("Failed to update policy"));
}

#[tokio::test]
#[serial]
async fn test_run_maintenance_error() {
    let _handle = start_error_management_server().await;

    // Give the server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let response = client
        .post("http://127.0.0.1:18082/api/maintenance")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("Maintenance failed"));
}

#[tokio::test]
#[serial]
async fn test_get_storage_stats_error() {
    let _handle = start_error_management_server().await;

    // Give the server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18082/api/storage/stats")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("Failed to get stats"));
}
