use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
