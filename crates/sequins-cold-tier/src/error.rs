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

    /// The batch's Arrow schema cannot be encoded to the cold format (e.g. a
    /// `Map` column Vortex 0.76 doesn't support). This is PERMANENT for that batch —
    /// retrying never helps — so callers should drop it rather than retain it forever.
    #[error("Unsupported for cold storage: {0}")]
    UnsupportedForCold(String),

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

    #[error("Series index error: {0}")]
    SeriesIndex(#[from] sequins_series_index::error::Error),
}

impl Error {
    /// True if this batch can never be written to cold (a permanent schema
    /// incompatibility), so it should be dropped rather than retried — otherwise it
    /// accumulates in the hot tier forever and eventually OOMs the process.
    pub fn is_unsupported_for_cold(&self) -> bool {
        matches!(self, Error::UnsupportedForCold(_))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
