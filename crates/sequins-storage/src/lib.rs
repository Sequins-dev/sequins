//! sequins-storage — Unified hot+cold storage engine for Sequins
//!
//! Provides a two-tier storage system:
//! - Hot tier: lock-free in-memory storage (Papaya) for recent data
//! - Cold tier: Vortex columnar storage on object_store for historical data
//!
//! Implements `OtlpIngest` and `ManagementApi` from `sequins-traits`.

pub mod config;
pub mod error;
pub mod storage;
mod wal;

// Re-export the query-facing storage contract.
pub use config::StorageConfig;
pub use error::{Error, Result};
pub use storage::{MaintenanceStats, Storage};
pub use wal::{Wal, WalConfig, WalEntry, WalPayload, WalSegmentMeta, WalSubscriber};

// Test fixtures module — compiled in test mode or when the test-utils feature is enabled
#[cfg(any(test, feature = "test-utils"))]
pub mod test_fixtures;
