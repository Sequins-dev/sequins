//! Integration tests for live query subscription accounting
//!
//! These tests verify that LiveQueryManager correctly tracks subscriptions,
//! enforces resource limits, and cleans up on drop.
//!
//! Live query *execution* is tested via DataFusionBackend in the datafusion
//! execution tests, since execute_live() now owns the streaming path.

use sequins_live_query::{LiveQueryConfig, LiveQueryManager};
use sequins_wal::{Wal, WalConfig};
use std::sync::Arc;
use std::time::Duration;

/// Helper to create a WAL for tests
async fn create_test_wal() -> Arc<Wal> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let wal_config = WalConfig {
        base_path: "wal".to_string(),
        segment_size: 1024 * 1024,
        flush_interval: 10,
        broadcast_capacity: 1000,
    };
    Arc::new(Wal::new(store, wal_config).await.unwrap())
}

#[tokio::test]
async fn test_manager_starts_empty() {
    let wal = create_test_wal().await;
    let config = LiveQueryConfig::default();
    let manager = LiveQueryManager::new(wal, config);
    assert_eq!(manager.active_subscriptions(), 0);
}

#[tokio::test]
async fn test_register_increments_count() {
    let wal = create_test_wal().await;
    let config = LiveQueryConfig::default();
    let manager = LiveQueryManager::new(wal, config);

    let _g1 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 1);

    let _g2 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 2);
}

#[tokio::test]
async fn test_subscription_cleanup_on_drop() {
    let wal = create_test_wal().await;
    let config = LiveQueryConfig::default();
    let manager = LiveQueryManager::new(wal, config);

    {
        let _guard = manager.register().unwrap();
        assert_eq!(manager.active_subscriptions(), 1);
    } // guard dropped

    // Give cleanup time to run
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(manager.active_subscriptions(), 0);
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let wal = create_test_wal().await;
    let config = LiveQueryConfig::default();
    let manager = Arc::new(LiveQueryManager::new(wal, config));

    let g1 = manager.register().unwrap();
    let g2 = manager.register().unwrap();
    let g3 = manager.register().unwrap();

    assert_eq!(manager.active_subscriptions(), 3);

    // Drop all guards
    drop(g1);
    drop(g2);
    drop(g3);

    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(manager.active_subscriptions(), 0);
}

#[tokio::test]
async fn test_max_subscriptions_limit() {
    let wal = create_test_wal().await;
    let config = LiveQueryConfig {
        max_subscriptions: 2,
        heartbeat_interval: Duration::from_secs(30),
    };
    let manager = Arc::new(LiveQueryManager::new(wal, config));

    let _g1 = manager.register().unwrap();
    let _g2 = manager.register().unwrap();

    assert_eq!(manager.active_subscriptions(), 2);

    // Third registration should fail
    let result = manager.register();
    assert!(result.is_err(), "Third subscription should fail");
    let err_msg = format!("{:?}", result.err().unwrap());
    assert!(
        err_msg.contains("Maximum live query subscriptions"),
        "Error should mention subscription limit, got: {}",
        err_msg
    );

    // Count should still be 2 (failed registration doesn't count)
    assert_eq!(manager.active_subscriptions(), 2);
}

#[tokio::test]
async fn test_subscription_guard_has_unique_ids() {
    let wal = create_test_wal().await;
    let config = LiveQueryConfig::default();
    let manager = LiveQueryManager::new(wal, config);

    let g1 = manager.register().unwrap();
    let g2 = manager.register().unwrap();

    assert_ne!(g1.id, g2.id, "Each subscription should have a unique ID");
    assert!(!g1.id.is_empty());
    assert!(!g2.id.is_empty());
}

#[tokio::test]
async fn test_subscription_limit_recovers_after_drop() {
    let wal = create_test_wal().await;
    let config = LiveQueryConfig {
        max_subscriptions: 1,
        heartbeat_interval: Duration::from_secs(30),
    };
    let manager = LiveQueryManager::new(wal, config);

    {
        let _g = manager.register().unwrap();
        // At limit: second registration fails
        assert!(manager.register().is_err());
    } // g dropped

    tokio::time::sleep(Duration::from_millis(10)).await;

    // After drop, should be able to register again
    let _g2 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 1);
}
