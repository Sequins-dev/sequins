use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("Query error: {0}")]
    Query(#[from] sequins_query::QueryError),
    #[error("Template error: {0}")]
    Template(#[from] minijinja::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("{0}")]
    Other(String),
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        let msg = self.to_string();
        tracing::error!("{}", msg);
        (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response()
    }
}

impl From<anyhow::Error> for WebError {
    fn from(e: anyhow::Error) -> Self {
        WebError::Other(e.to_string())
    }
}
