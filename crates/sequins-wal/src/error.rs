use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("WAL serialization error: {0}")]
    Serialization(String),

    #[error("WAL I/O error: {0}")]
    Io(String),

    #[error("WAL segment error: {0}")]
    Segment(String),

    #[error("WAL error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, WalError>;

impl From<bincode::Error> for WalError {
    fn from(e: bincode::Error) -> Self {
        WalError::Serialization(e.to_string())
    }
}

impl From<object_store::Error> for WalError {
    fn from(e: object_store::Error) -> Self {
        WalError::Io(e.to_string())
    }
}
