use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Storage error: {0}")]
    Storage(String),
}

pub type Result<T> = std::result::Result<T, Error>;
