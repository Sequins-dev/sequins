use super::Storage;
use crate::cold_tier::ColdTier;
use crate::config::StorageConfig;
use crate::hot_tier::{HotTier, StorageStats};
use crate::live_query::LiveQueryManager;
use crate::wal::Wal;
use arrow::array::RecordBatch;
use sequins_query::ast::Signal;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

impl Storage {
    /// Get the storage configuration
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Get the shutdown notify handle
    pub fn shutdown_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.shutdown_notify)
    }

    /// Get the WAL instance
    pub fn wal(&self) -> &Arc<Wal> {
        &self.wal
    }

    /// Get the live query broadcast sender (for publishing RecordBatches to live subscribers)
    pub fn live_broadcast_tx(&self) -> broadcast::Sender<(Signal, Arc<RecordBatch>)> {
        self.live_broadcast.clone()
    }

    /// Get the LiveQueryManager
    pub fn live_query_manager(&self) -> &Arc<LiveQueryManager> {
        &self.live_query_manager
    }

    /// Get a cloned Arc to the hot tier (for DataFusion table providers)
    pub fn hot_tier_arc(&self) -> Arc<HotTier> {
        Arc::clone(&self.hot_tier)
    }

    /// Get a cloned Arc to the cold tier (for DataFusion table providers)
    pub fn cold_tier_arc(&self) -> Arc<RwLock<ColdTier>> {
        Arc::clone(&self.cold_tier)
    }

    /// Get current storage statistics - internal use only
    pub(crate) fn stats(&self) -> StorageStats {
        self.hot_tier.stats()
    }

    /// Clear all data from hot tier - internal use only
    #[cfg(test)]
    pub(crate) fn clear_hot_tier(&self) {
        self.hot_tier.clear();
    }
}
