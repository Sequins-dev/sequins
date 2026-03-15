use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Vortex error: {0}")]
    Vortex(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error(transparent)]
    Wal(#[from] sequins_wal::WalError),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Resource limit exceeded: {message}")]
    ResourceLimit { message: String },

    #[error(transparent)]
    LiveQuery(#[from] sequins_live_query::Error),

    #[error(transparent)]
    HotTier(#[from] sequins_hot_tier::HotTierError),

    #[error(transparent)]
    ColdTier(#[from] sequins_cold_tier::Error),

    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
