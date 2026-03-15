//! HTTP status code coverage tests for RemoteClient
//!
//! Tests that the client handles various HTTP status codes correctly:
//! - 200 OK (already tested extensively)
//! - 400 Bad Request
//! - 401 Unauthorized
//! - 403 Forbidden
//! - 429 Too Many Requests
//! - 503 Service Unavailable

use sequins::client::RemoteClient;
use sequins::traits::{ManagementApi, QueryApi};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

// ============================================================================
// 400 Bad Request Tests
// ============================================================================

#[tokio::test]
async fn test_400_bad_request() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "Bad request - invalid parameters"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_services().await;

    assert!(result.is_err());
}

// ============================================================================
// 401 Unauthorized Tests
// ============================================================================

#[tokio::test]
async fn test_401_unauthorized() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "Unauthorized - authentication required"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_services().await;

    assert!(result.is_err());
}

// ============================================================================
// 403 Forbidden Tests
// ============================================================================

#[tokio::test]
async fn test_403_forbidden() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/retention/cleanup"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error": "Forbidden - insufficient permissions"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.run_retention_cleanup().await;

    assert!(result.is_err());
}

// ============================================================================
// 429 Too Many Requests Tests
// ============================================================================

#[tokio::test]
async fn test_429_too_many_requests() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "60")
                .set_body_json(serde_json::json!({
                    "error": "Too many requests - rate limit exceeded"
                })),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_services().await;

    assert!(result.is_err());
}

// ============================================================================
// 503 Service Unavailable Tests
// ============================================================================

#[tokio::test]
async fn test_503_service_unavailable() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/storage/stats"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error": "Service temporarily unavailable"
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let result = client.get_storage_stats().await;

    assert!(result.is_err());
}

// ============================================================================
// Combined Status Code Tests
// ============================================================================

#[tokio::test]
async fn test_multiple_status_codes_in_sequence() {
    let mock_server = MockServer::start().await;

    // First request: 503
    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Second request: 429
    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Third request: 200
    Mock::given(method("GET"))
        .and(path("/api/services"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(Vec::<sequins::models::Service>::new()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();

    // First two should fail
    assert!(client.get_services().await.is_err());
    assert!(client.get_services().await.is_err());

    // Third should succeed
    assert!(client.get_services().await.is_ok());
}
