//! Errors for the app-state store.

/// Result alias for app-state operations.
pub type Result<T> = std::result::Result<T, MetadataError>;

/// Errors persisting or loading app state.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}
