//! LiveQueryManager - orchestrates live query subscriptions
//!
//! The manager is responsible for:
//! - Tracking active subscription count for resource limiting
//! - Providing subscription lifecycle hooks (register/unregister)
//!
//! Note: actual live query execution now happens in
//! `datafusion_backend::execution::execute_live`, which routes through
//! `DataFusionBackend::execute()`.  `LiveQueryManager` no longer owns
//! the execution path; it exists solely for subscription accounting and
//! future enrichment hooks.

use crate::error::{Error, Result};
use parking_lot::RwLock;
use sequins_wal::Wal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the LiveQueryManager
#[derive(Debug, Clone)]
pub struct LiveQueryConfig {
    /// Maximum number of concurrent live query subscriptions
    pub max_subscriptions: usize,

    /// How often to emit heartbeat frames
    pub heartbeat_interval: Duration,
}

impl Default for LiveQueryConfig {
    fn default() -> Self {
        Self {
            max_subscriptions: 1000,
            heartbeat_interval: Duration::from_secs(30),
        }
    }
}

/// Manages live query subscription accounting
///
/// Tracks active subscription count and enforces resource limits.
/// Actual query execution is delegated to `execute_live()` in the
/// `datafusion_backend::execution` module.
pub struct LiveQueryManager {
    /// WAL (reserved for future use — e.g. heartbeat watermarks)
    #[allow(dead_code)]
    wal: Arc<Wal>,

    /// Active subscription IDs (value is a placeholder unit)
    subscriptions: Arc<RwLock<HashMap<String, ()>>>,

    /// Configuration
    config: LiveQueryConfig,
}

impl LiveQueryManager {
    /// Create a new LiveQueryManager
    pub fn new(wal: Arc<Wal>, config: LiveQueryConfig) -> Self {
        Self {
            wal,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Register a new subscription, returning its ID.
    ///
    /// Returns `Err(ResourceLimit)` if the subscription cap is reached.
    ///
    /// Uses a single write-lock for the check-then-insert to avoid a TOCTOU
    /// race where concurrent callers could both pass the read-lock check and
    /// then both insert, exceeding `max_subscriptions`.
    pub fn register(&self) -> Result<SubscriptionGuard> {
        let mut subscriptions = self.subscriptions.write();
        if subscriptions.len() >= self.config.max_subscriptions {
            return Err(Error::ResourceLimit {
                message: format!(
                    "Maximum live query subscriptions ({}) exceeded",
                    self.config.max_subscriptions
                ),
            });
        }

        let subscription_id = uuid::Uuid::new_v4().to_string();
        subscriptions.insert(subscription_id.clone(), ());
        drop(subscriptions);

        Ok(SubscriptionGuard {
            id: subscription_id,
            subscriptions: Arc::clone(&self.subscriptions),
        })
    }

    /// Get the number of active subscriptions
    pub fn active_subscriptions(&self) -> usize {
        self.subscriptions.read().len()
    }
}

/// RAII guard that removes the subscription entry when dropped
pub struct SubscriptionGuard {
    pub id: String,
    subscriptions: Arc<RwLock<HashMap<String, ()>>>,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.subscriptions.write().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_wal::WalConfig;

    async fn create_test_wal() -> Arc<Wal> {
        let store = Arc::new(object_store::memory::InMemory::new());
        let wal_config = WalConfig {
            base_path: "wal".to_string(),
            segment_size: 1024,
            flush_interval: 10,
            broadcast_capacity: 100,
        };
        Arc::new(Wal::new(store, wal_config).await.unwrap())
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let wal = create_test_wal().await;
        let config = LiveQueryConfig::default();
        let manager = LiveQueryManager::new(wal, config);
        assert_eq!(manager.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn test_register_subscription() {
        let wal = create_test_wal().await;
        let config = LiveQueryConfig::default();
        let manager = LiveQueryManager::new(wal, config);

        let guard = manager.register().unwrap();
        assert_eq!(manager.active_subscriptions(), 1);

        // Guard ID is a valid UUID string
        assert!(!guard.id.is_empty());
    }

    #[tokio::test]
    async fn test_cleanup_on_guard_drop() {
        let wal = create_test_wal().await;
        let config = LiveQueryConfig::default();
        let manager = LiveQueryManager::new(wal, config);

        {
            let _guard = manager.register().unwrap();
            assert_eq!(manager.active_subscriptions(), 1);
        } // guard dropped here

        assert_eq!(manager.active_subscriptions(), 0);
    }

    #[tokio::test]
    async fn test_max_subscriptions_limit() {
        let wal = create_test_wal().await;
        let config = LiveQueryConfig {
            max_subscriptions: 2,
            ..Default::default()
        };
        let manager = LiveQueryManager::new(wal, config);

        let _g1 = manager.register().unwrap();
        let _g2 = manager.register().unwrap();

        // Third should fail
        let result = manager.register();
        assert!(result.is_err());
        match result {
            Err(e) => assert!(e.to_string().contains("Maximum live query subscriptions")),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[tokio::test]
    async fn test_multiple_subscriptions() {
        let wal = create_test_wal().await;
        let config = LiveQueryConfig::default();
        let manager = LiveQueryManager::new(wal, config);

        let _g1 = manager.register().unwrap();
        let _g2 = manager.register().unwrap();
        let _g3 = manager.register().unwrap();

        assert_eq!(manager.active_subscriptions(), 3);
    }

    #[tokio::test]
    async fn test_concurrent_register_respects_max_subscriptions() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let wal = create_test_wal().await;
        let max = 10;
        let config = LiveQueryConfig {
            max_subscriptions: max,
            ..Default::default()
        };
        let manager = Arc::new(LiveQueryManager::new(wal, config));

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        // Spawn max + 10 concurrent tasks all trying to register at once
        for _ in 0..(max + 10) {
            let manager = Arc::clone(&manager);
            let success_count = Arc::clone(&success_count);
            let handle = tokio::spawn(async move {
                // Use `if let Ok(_guard)` so that the SubscriptionGuard stays alive
                // for the duration of the sleep, keeping its slot occupied.
                // `is_ok()` would immediately drop the guard, freeing the slot
                // before other tasks can see it.
                if let Ok(_guard) = manager.register() {
                    success_count.fetch_add(1, Ordering::Relaxed);
                    // Hold the guard alive so concurrent tasks hit the cap
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    // Guard dropped here, freeing the slot
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        // Exactly `max` registrations should have succeeded
        assert_eq!(
            success_count.load(Ordering::Relaxed),
            max,
            "exactly max_subscriptions tasks should succeed"
        );
    }
}
