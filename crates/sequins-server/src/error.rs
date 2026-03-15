use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

/// Errors that can occur in the server
#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP server error: {0}")]
    Http(String),

    #[error("gRPC error: {0}")]
    Grpc(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Query error: {0}")]
    Query(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<sequins_types::Error> for Error {
    fn from(e: sequins_types::Error) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<sequins_query::QueryError> for Error {
    fn from(e: sequins_query::QueryError) -> Self {
        Self::Query(e.to_string())
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Error::Http(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
            Error::Grpc(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
            Error::InvalidRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Error::Serialization(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Error::Json(e) => (StatusCode::BAD_REQUEST, e.to_string()),
            Error::Storage(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
            Error::Query(m) => (StatusCode::BAD_REQUEST, m.clone()),
        };

        let body = serde_json::json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_to_http_status_codes() {
        // HTTP error -> 500
        let error = Error::Http("Server error".to_string());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // gRPC error -> 500
        let error = Error::Grpc("gRPC error".to_string());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // InvalidRequest -> 400
        let error = Error::InvalidRequest("Bad input".to_string());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Serialization -> 400
        let error = Error::Serialization("Bad JSON".to_string());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // JSON error -> 400
        let json_err = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error = Error::Json(json_err);
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Storage error -> 500
        let error = Error::Storage("Storage failed".to_string());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // Query error -> 400
        let error = Error::Query("Invalid query".to_string());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_storage_error_propagation() {
        // Test that sequins_types::Error converts to server Error
        let storage_error = sequins_types::Error::Other("Storage internal error".to_string());
        let server_error: Error = storage_error.into();

        match server_error {
            Error::Storage(msg) => {
                assert!(msg.contains("Storage internal error"));
            }
            _ => panic!("Expected Storage error variant"),
        }
    }

    #[tokio::test]
    async fn test_query_error_propagation() {
        // Test that sequins_query::QueryError converts to server Error
        let query_error = sequins_query::QueryError::InvalidAst {
            message: "Parse error".to_string(),
        };
        let server_error: Error = query_error.into();

        match server_error {
            Error::Query(msg) => {
                assert!(msg.contains("Parse error"));
            }
            _ => panic!("Expected Query error variant"),
        }
    }

    #[tokio::test]
    async fn test_error_response_format() {
        // Test that error responses include helpful JSON structure
        let error = Error::InvalidRequest("Missing field: name".to_string());
        let response = error.into_response();

        // Extract and parse body
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        // Should have "error" field with message
        assert_eq!(body["error"], "Missing field: name");
    }
}
