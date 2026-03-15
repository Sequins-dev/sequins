//! sequins-storage — Unified hot+cold storage engine for Sequins
//!
//! Provides a two-tier storage system:
//! - Hot tier: lock-free in-memory storage (Papaya) for recent data
//! - Cold tier: Vortex columnar storage on object_store for historical data
//!
//! Implements `OtlpIngest` and `ManagementApi` from sequins-types.

pub use sequins_cold_tier as cold_tier;
pub mod config;
pub mod datafusion_backend;
pub mod error;
pub use sequins_hot_tier as hot_tier;
pub mod storage;
pub mod union_provider;
pub mod wal;

// Re-export live_query module under its original name for crate-internal paths
pub use sequins_live_query as live_query;

// Re-export main types
pub use cold_tier::{ColdTier, SeriesId, SeriesIndex, SeriesMetadata};
pub use config::StorageConfig;
pub use datafusion_backend::DataFusionBackend;
pub use error::{Error, Result};
pub use hot_tier::{EvictionStats, HotTier, ResourceId, ScopeId, StorageStats};
pub use sequins_live_query::{
    DeltaEmitter, HeartbeatEmitter, LiveQueryConfig, LiveQueryManager, SubscriptionGuard,
};
pub use storage::{MaintenanceStats, Storage};
pub use wal::{Wal, WalConfig, WalEntry, WalPayload, WalSegmentMeta, WalSubscriber};

// Test fixtures module - only compiled in test mode
#[cfg(test)]
pub(crate) mod test_fixtures;
