use crate::config::StorageConfig;
use arrow::array::RecordBatch;
use seql_ast::ast::Signal;
use sequins_cold_tier::ColdTier;
use sequins_hot_tier::HotTier;
use sequins_live_query::LiveQueryManager;
use sequins_types::models::RetentionPolicy;
use sequins_types::NowTime;
use sequins_wal::Wal;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

// Module declarations
mod accessors;
mod background;
mod constructor;
mod health;
mod ingest;
mod maintenance;
mod management;
mod retention;
#[cfg(test)]
mod tests;

/// Tiered storage combining hot (in-memory) and cold (Parquet) tiers
pub struct Storage {
    pub(super) config: StorageConfig,
    /// Stable node identifier; also the object-store prefix this node writes under.
    pub(super) node_id: String,
    pub(super) hot_tier: Arc<HotTier>,
    pub(super) cold_tier: Arc<RwLock<ColdTier>>,
    // Write-Ahead Log for durable ingestion
    pub(super) wal: Arc<Wal>,
    // Live query broadcast channel
    pub(super) live_broadcast: broadcast::Sender<(Signal, Arc<RecordBatch>)>,
    // Live query manager
    pub(super) live_query_manager: Arc<LiveQueryManager>,
    pub(super) shutdown_notify: Arc<tokio::sync::Notify>,
    /// Persisted retention policy (overrides config defaults)
    pub(super) retention_policy: Arc<RwLock<Option<RetentionPolicy>>>,
    /// Path to health config JSON file
    pub(super) health_config_path: PathBuf,
    /// Wall-clock time provider (injectable for deterministic testing)
    pub(crate) clock: Arc<dyn NowTime>,
}

/// Statistics about maintenance operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStats {
    pub entries_evicted: usize,
    pub batches_flushed: usize,
}
