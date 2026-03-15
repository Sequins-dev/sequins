//! Error types for the cold tier.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Vortex error: {0}")]
    Vortex(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, Error>;
