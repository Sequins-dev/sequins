//! Client error types

/// Client errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// HTTP client error
    #[error("HTTP error: {0}")]
    Http(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid response from server
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

pub type Result<T> = std::result::Result<T, Error>;
