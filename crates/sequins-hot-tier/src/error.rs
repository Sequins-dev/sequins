use thiserror::Error;

#[derive(Error, Debug)]
pub enum HotTierError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, HotTierError>;
