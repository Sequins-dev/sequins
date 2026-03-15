use super::Storage;
use crate::cold_tier::ColdTier;
use crate::config::StorageConfig;
use crate::error::Result;
use crate::live_query::{LiveQueryConfig, LiveQueryManager};
use crate::wal::{Wal, WalConfig};
use arrow::array::RecordBatch;
use sequins_query::ast::Signal;
use sequins_types::{NowTime, SystemNowTime};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};

impl Storage {
    /// Create new tiered storage
    ///
    /// # Errors
    ///
    /// Returns an error if cold tier initialization fails
    pub async fn new(config: StorageConfig) -> Result<Self> {
        Self::new_with_clock(config, Arc::new(SystemNowTime)).await
    }

    /// Create new tiered storage with an explicit time provider.
    ///
    /// Pass `Arc::new(MockNowTime::new(base_ns))` in tests to make
    /// time-dependent logic fully deterministic.
    pub async fn new_with_clock(config: StorageConfig, clock: Arc<dyn NowTime>) -> Result<Self> {
        let hot_tier = Arc::new(crate::hot_tier::HotTier::new(config.hot_tier.clone()));
        let cold_tier_inner = ColdTier::new(config.cold_tier.clone())?;
        let cold_tier = Arc::new(RwLock::new(cold_tier_inner));

        // Load persisted retention policy if it exists
        let retention_policy = Self::load_retention_policy(&config.cold_tier.uri)?;

        // Health config path
        let health_config_path =
            std::path::PathBuf::from(&config.cold_tier.uri).join("health_config.json");

        // Initialize WAL
        // Strip file:// prefix for WAL base_path
        let base_path = config
            .cold_tier
            .uri
            .strip_prefix("file://")
            .unwrap_or(&config.cold_tier.uri)
            .to_string();
        let wal_config = WalConfig {
            base_path,
            segment_size: 10_000,
            flush_interval: 100,
            broadcast_capacity: 1000,
        };
        let store = cold_tier.read().await.store.clone();
        let wal = Arc::new(Wal::new(store.clone(), wal_config).await?);

        // Create live query broadcast channel
        let (live_broadcast, _) = broadcast::channel::<(Signal, Arc<RecordBatch>)>(1000);

        // Initialize LiveQueryManager (subscription accounting only;
        // actual execution is in datafusion_backend::execution::execute_live)
        let live_config = LiveQueryConfig {
            max_subscriptions: 1000,
            heartbeat_interval: Duration::from_secs(5),
        };
        let live_query_manager = Arc::new(LiveQueryManager::new(wal.clone(), live_config));

        Ok(Self {
            config,
            hot_tier,
            cold_tier,
            wal,
            live_broadcast,
            live_query_manager,
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
            retention_policy: Arc::new(RwLock::new(retention_policy)),
            health_config_path,
            clock,
        })
    }
}
