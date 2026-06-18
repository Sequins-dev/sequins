//! Integration tests for live query subscription accounting.
//!
//! Live query *execution* is tested via DataFusionBackend in the datafusion
//! execution tests, since execute_live() owns the streaming path.

use sequins_live_query::{LiveQueryConfig, LiveQueryManager};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_manager_starts_empty() {
    let manager = LiveQueryManager::new(LiveQueryConfig::default());
    assert_eq!(manager.active_subscriptions(), 0);
}

#[tokio::test]
async fn test_register_increments_count() {
    let manager = LiveQueryManager::new(LiveQueryConfig::default());
    let _g1 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 1);
    let _g2 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 2);
}

#[tokio::test]
async fn test_subscription_cleanup_on_drop() {
    let manager = LiveQueryManager::new(LiveQueryConfig::default());
    {
        let _guard = manager.register().unwrap();
        assert_eq!(manager.active_subscriptions(), 1);
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(manager.active_subscriptions(), 0);
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let manager = Arc::new(LiveQueryManager::new(LiveQueryConfig::default()));
    let g1 = manager.register().unwrap();
    let g2 = manager.register().unwrap();
    let g3 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 3);
    drop(g1);
    drop(g2);
    drop(g3);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(manager.active_subscriptions(), 0);
}

#[tokio::test]
async fn test_max_subscriptions_limit() {
    let manager = Arc::new(LiveQueryManager::new(LiveQueryConfig {
        max_subscriptions: 2,
        heartbeat_interval: Duration::from_secs(30),
    }));
    let _g1 = manager.register().unwrap();
    let _g2 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 2);
    let result = manager.register();
    assert!(result.is_err(), "Third subscription should fail");
    assert!(format!("{:?}", result.err().unwrap()).contains("Maximum live query subscriptions"),);
    assert_eq!(manager.active_subscriptions(), 2);
}

#[tokio::test]
async fn test_subscription_guard_has_unique_ids() {
    let manager = LiveQueryManager::new(LiveQueryConfig::default());
    let g1 = manager.register().unwrap();
    let g2 = manager.register().unwrap();
    assert_ne!(g1.id, g2.id);
    assert!(!g1.id.is_empty());
    assert!(!g2.id.is_empty());
}

#[tokio::test]
async fn test_subscription_limit_recovers_after_drop() {
    let manager = LiveQueryManager::new(LiveQueryConfig {
        max_subscriptions: 1,
        heartbeat_interval: Duration::from_secs(30),
    });
    {
        let _g = manager.register().unwrap();
        assert!(manager.register().is_err());
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _g2 = manager.register().unwrap();
    assert_eq!(manager.active_subscriptions(), 1);
}
