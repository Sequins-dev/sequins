//! Server error types

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Server errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Core error from sequins-core
    #[error("core error: {0}")]
    Core(#[from] sequins_core::error::Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// HTTP server error
    #[error("HTTP error: {0}")]
    Http(String),

    /// gRPC server error
    #[error("gRPC error: {0}")]
    Grpc(String),

    /// Invalid request
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Convert server errors to HTTP responses
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::Core(ref e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            Error::Json(ref e) => (StatusCode::BAD_REQUEST, e.to_string()),
            Error::Http(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            Error::Grpc(ref msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            Error::InvalidRequest(ref msg) => (StatusCode::BAD_REQUEST, msg.clone()),
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}
