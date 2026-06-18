//! sequins-traits — Core traits, error types, and shared utility types for Sequins
//!
//! This crate has no dependencies on other Sequins crates and no heavy deps
//! (no Arrow, Vortex, DataFusion, pprof). It defines the integration points
//! that client code, server code, and storage implementations all share.

/// Nanosecond-precision duration type
pub mod duration;
/// Top-level error type and Result alias
pub mod error;
/// OTLP ingestion trait
pub mod ingest;
/// Management / administrative API trait
pub mod management;
/// Query API traits and error types (QueryApi, QueryExec, SeqlStream, QueryError)
pub mod query;
/// Storage model types used by traits (MaintenanceStats, RetentionPolicy, StorageStats)
pub mod storage;

/// UUID-backed ID type macro (`define_uuid_id!`)
pub mod id;
/// Wall-clock time provider (injectable for deterministic testing)
pub mod time_provider;

// Re-export common types at crate root for ergonomics.
pub use duration::Duration;
pub use error::{Error, Result};
pub use ingest::OtlpIngest;
pub use management::ManagementApi;
pub use query::{QueryApi, QueryError, QueryExec, SeqlStream, WarningCode};
pub use storage::{MaintenanceStats, RetentionPolicy, StorageStats};
pub use time_provider::{MockNowTime, NowTime, SystemNowTime};
