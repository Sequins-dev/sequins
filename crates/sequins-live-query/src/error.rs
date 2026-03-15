use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Resource limit exceeded: {message}")]
    ResourceLimit { message: String },

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, Error>;
