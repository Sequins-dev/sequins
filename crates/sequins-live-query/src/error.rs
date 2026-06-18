use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Resource limit exceeded: {message}")]
    ResourceLimit { message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
