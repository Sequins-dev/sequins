use super::Storage;
use crate::config::StorageConfig;
use crate::error::Result;
use arrow::array::RecordBatch;
use seql_ast::ast::Signal;
use sequins_cold_tier::ColdTier;
use sequins_hot_tier::{HotTier, SignalColdFlushFn};
use sequins_live_query::{LiveQueryConfig, LiveQueryManager};
use sequins_types::{NowTime, SystemNowTime};
use sequins_wal::{Wal, WalConfig};
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
        // Cold storage is a single **shared** dataset across the cluster: every
        // node reads and writes the same `cold_tier.uri` (no per-node prefix), so
        // a query on any node observes data flushed by any node. Nodes avoid
        // collisions by writing globally-unique immutable files (the cold-tier
        // write path stamps the node id into each filename) and content-addressed
        // series ids; background compaction merges them.
        //
        // The **WAL** stays per-node — it only exists to replay a node's own
        // un-flushed hot window after a crash, and no node ever needs another
        // node's WAL — so its segments and the durable watermark beside them live
        // under a `{uri}/{node_id}/wal` sub-prefix (via `checkpoint::wal_base_path`,
        // shared with the watermark path so the two never drift).
        let node_id = config.effective_node_id().to_string();

        let cold_tier_inner = ColdTier::new(config.cold_tier.clone())?.with_node_id(&node_id);
        let cold_tier = Arc::new(RwLock::new(cold_tier_inner));

        // Build a cold-flush callback so completed hot-tier batches are durably
        // persisted. Returns `true` only on a successful write so the hot tier
        // can advance its durable cold-flush watermark.
        let cold_flush: SignalColdFlushFn = {
            let cold_tier_ref = Arc::clone(&cold_tier);
            Arc::new(move |signal, batch| {
                let cold_tier_ref = Arc::clone(&cold_tier_ref);
                Box::pin(async move {
                    let ct = cold_tier_ref.read().await;
                    match ct.write_signal(signal, (*batch).clone()).await {
                        Ok(()) => true,
                        // Permanent: the cold format can't encode this batch's schema
                        // (e.g. a Map column). Retrying never helps, so report success
                        // to EVICT it from the hot tier — otherwise it accumulates
                        // forever and OOM-kills the process. Data is dropped (logged).
                        Err(e) if e.is_unsupported_for_cold() => {
                            tracing::warn!(
                                "dropping {:?} batch that cold storage cannot encode \
                                 (not persisted): {}",
                                signal,
                                e
                            );
                            true
                        }
                        // Transient (I/O, etc.): retain so it retries on the next pass.
                        Err(e) => {
                            tracing::error!("cold flush for {:?} failed (retained): {}", signal, e);
                            false
                        }
                    }
                })
            })
        };
        let hot_tier = Arc::new(HotTier::new_with_cold_writer(
            config.hot_tier.clone(),
            cold_flush,
        ));

        // Object store shared by the WAL, cold tier, and persisted config files.
        let store = cold_tier.read().await.store.clone();

        // Load persisted retention policy from object storage if it exists.
        let retention_policy = Self::load_retention_policy(&store, &config.cold_tier.uri).await?;

        // App state (conversations + dashboards) lives under a *shared* `app_state`
        // prefix of the cold root (no node id) so the whole cluster/team sees the
        // same dashboards and conversations. Hydrate it from durable storage.
        let app_base =
            sequins_cold_tier::store_base_path(&config.cold_tier.uri).trim_end_matches('/');
        let app_state_prefix = if app_base.is_empty() {
            "app_state".to_string()
        } else {
            format!("{app_base}/app_state")
        };
        let app_state = Arc::new(sequins_metadata::AppStateStore::new(
            store.clone(),
            app_state_prefix,
        ));
        app_state
            .load()
            .await
            .map_err(|e| crate::error::Error::Storage(format!("Failed to load app state: {e}")))?;

        // Initialize the WAL under this node's private sub-prefix of the shared
        // cold root (bucket-relative for s3://gs://az://, the local path for
        // file://). Mirrors the watermark path in `checkpoint`.
        let base_path = super::checkpoint::wal_base_path(&config.cold_tier.uri, &node_id);
        let wal_config = WalConfig {
            base_path,
            segment_size: 10_000,
            flush_interval: 100,
            broadcast_capacity: 1000,
        };
        let wal = Arc::new(Wal::new(store.clone(), wal_config).await?);

        // Create live query broadcast channel
        let (live_broadcast, _) = broadcast::channel::<(Signal, Arc<RecordBatch>)>(1000);

        // Initialize LiveQueryManager (subscription accounting only;
        // actual execution is in datafusion_backend::execution::execute_live)
        let live_config = LiveQueryConfig {
            max_subscriptions: 1000,
            heartbeat_interval: Duration::from_secs(5),
        };
        let live_query_manager = Arc::new(LiveQueryManager::new(live_config));

        let storage = Self {
            config,
            node_id,
            hot_tier,
            cold_tier,
            wal,
            live_broadcast,
            live_query_manager,
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
            retention_policy: Arc::new(RwLock::new(retention_policy)),
            app_state,
            clock,
            replay_seq: std::sync::atomic::AtomicU64::new(0),
        };

        // Recover this node's un-flushed hot window: replay WAL entries newer
        // than the durable cold-flush watermark back into the hot tier. Entries
        // at or below the watermark are already in the cold tier, so they are
        // skipped — no duplication. Runs before any server is started.
        let replayed = storage.replay_wal().await?;
        if replayed > 0 {
            tracing::info!(entries = replayed, "replayed WAL entries into hot tier");
        }

        Ok(storage)
    }
}
