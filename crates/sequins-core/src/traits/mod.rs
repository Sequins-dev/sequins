// Storage traits for the three-layer architecture

mod ingest;
mod management;
mod query;
mod storage;

pub use ingest::OtlpIngest;
pub use management::ManagementApi;
pub use query::QueryApi;
pub use storage::{StorageRead, StorageWrite, TierMetadata};
