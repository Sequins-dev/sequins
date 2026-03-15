//! sequins-hot-tier — In-memory hot tier storage for Sequins
//!
//! Provides a lock-free in-memory storage layer backed by `BatchChain`s
//! that accumulate Arrow `RecordBatch`es as telemetry data arrives.

pub mod batch_chain;
pub mod config;
pub mod core;
pub mod error;

// Re-export main types
pub use batch_chain::{BatchChain, BatchMeta};
pub use config::HotTierConfig;
pub use core::{EvictionStats, HotTier, ResourceId, ScopeId, StorageStats};
pub use error::{HotTierError, Result};

// Re-export SignalType from sequins-types for convenience
pub use sequins_types::SignalType;
