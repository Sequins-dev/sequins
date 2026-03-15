/// Management API tests for TursoStorage
///
/// Tests retention policies, maintenance operations, and storage statistics.
use sequins::models::*;
use sequins::storage::TursoStorage;
use sequins::traits::{ManagementApi, OtlpIngest, QueryApi};
use tempfile::TempDir;
use tokio::time::{sleep, Duration as TokioDuration};

mod test_utils;
use test_utils::fixtures::OtlpFixtures;

/// Create a test storage instance
async fn create_storage() -> (TursoStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let storage = TursoStorage::new(&db_path).await.unwrap();
    (storage, temp_dir)
}

// =============================================================================
// Retention Policy Tests
// =============================================================================

#[tokio::test]
async fn test_get_default_retention_policy() {
    let (storage, _temp_dir) = create_storage().await;

    let policy = storage.get_retention_policy().await.unwrap();

    // Default is 24 hours for all types
    assert_eq!(policy.spans_retention, Duration::from_hours(24));
    assert_eq!(policy.logs_retention, Duration::from_hours(24));
    assert_eq!(policy.metrics_retention, Duration::from_hours(24));
    assert_eq!(policy.profiles_retention, Duration::from_hours(24));
}

#[tokio::test]
async fn test_update_retention_policy() {
    let (storage, _temp_dir) = create_storage().await;

    let new_policy = RetentionPolicy {
        spans_retention: Duration::from_hours(48),
        logs_retention: Duration::from_hours(72),
        metrics_retention: Duration::from_days(7),
        profiles_retention: Duration::from_days(1),
    };

    storage
        .update_retention_policy(new_policy.clone())
        .await
        .unwrap();

    let retrieved = storage.get_retention_policy().await.unwrap();
    assert_eq!(retrieved.spans_retention, Duration::from_hours(48));
    assert_eq!(retrieved.logs_retention, Duration::from_hours(72));
    assert_eq!(retrieved.metrics_retention, Duration::from_days(7));
    assert_eq!(retrieved.profiles_retention, Duration::from_days(1));
}

#[tokio::test]
async fn test_update_retention_policy_persists() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create storage and update policy
    {
        let storage = TursoStorage::new(&db_path).await.unwrap();
        let new_policy = RetentionPolicy {
            spans_retention: Duration::from_hours(96),
            logs_retention: Duration::from_hours(120),
            metrics_retention: Duration::from_days(14),
            profiles_retention: Duration::from_days(2),
        };
        storage.update_retention_policy(new_policy).await.unwrap();
    }

    // Create new storage instance and verify policy persisted
    let storage = TursoStorage::new(&db_path).await.unwrap();
    let retrieved = storage.get_retention_policy().await.unwrap();
    assert_eq!(retrieved.spans_retention, Duration::from_hours(96));
    assert_eq!(retrieved.logs_retention, Duration::from_hours(120));
    assert_eq!(retrieved.metrics_retention, Duration::from_days(14));
    assert_eq!(retrieved.profiles_retention, Duration::from_days(2));
}

// =============================================================================
// Retention Cleanup Tests
// =============================================================================

#[tokio::test]
async fn test_retention_cleanup_no_old_data() {
    let (storage, _temp_dir) = create_storage().await;

    // Set very long retention (100 years) to ensure nothing is deleted
    // Note: Fixtures use fixed timestamps from 2023, so need very long retention
    let policy = RetentionPolicy {
        spans_retention: Duration::from_days(36500),
        logs_retention: Duration::from_days(36500),
        metrics_retention: Duration::from_days(36500),
        profiles_retention: Duration::from_days(36500),
    };
    storage.update_retention_policy(policy).await.unwrap();

    // Insert data (with timestamps from 2023 in fixtures)
    let spans = OtlpFixtures::large_span_batch(10);
    storage.ingest_spans(spans).await.unwrap();

    let logs = OtlpFixtures::large_log_batch(10);
    storage.ingest_logs(logs).await.unwrap();

    // Run cleanup - should delete nothing with 100-year retention
    let deleted = storage.run_retention_cleanup().await.unwrap();
    assert_eq!(deleted, 0);
}

#[tokio::test]
async fn test_retention_cleanup_deletes_old_spans() {
    let (storage, _temp_dir) = create_storage().await;

    // Set very short retention (1 millisecond)
    let policy = RetentionPolicy {
        spans_retention: Duration::from_millis(1),
        logs_retention: Duration::from_hours(24),
        metrics_retention: Duration::from_hours(24),
        profiles_retention: Duration::from_hours(24),
    };
    storage.update_retention_policy(policy).await.unwrap();

    // Insert spans
    let spans = OtlpFixtures::large_span_batch(5);
    storage.ingest_spans(spans).await.unwrap();

    // Wait for data to become old
    sleep(TokioDuration::from_millis(10)).await;

    // Run cleanup - should delete spans
    let deleted = storage.run_retention_cleanup().await.unwrap();
    assert!(deleted > 0);
}

#[tokio::test]
async fn test_retention_cleanup_deletes_old_logs() {
    let (storage, _temp_dir) = create_storage().await;

    // Set very short retention for logs
    let policy = RetentionPolicy {
        spans_retention: Duration::from_hours(24),
        logs_retention: Duration::from_millis(1),
        metrics_retention: Duration::from_hours(24),
        profiles_retention: Duration::from_hours(24),
    };
    storage.update_retention_policy(policy).await.unwrap();

    // Insert logs
    let logs = OtlpFixtures::large_log_batch(5);
    storage.ingest_logs(logs).await.unwrap();

    // Wait for data to become old
    sleep(TokioDuration::from_millis(10)).await;

    // Run cleanup - should delete logs
    let deleted = storage.run_retention_cleanup().await.unwrap();
    assert!(deleted > 0);
}

#[tokio::test]
async fn test_retention_cleanup_deletes_old_metrics() {
    let (storage, _temp_dir) = create_storage().await;

    // Set very short retention for metrics
    let policy = RetentionPolicy {
        spans_retention: Duration::from_hours(24),
        logs_retention: Duration::from_hours(24),
        metrics_retention: Duration::from_millis(1),
        profiles_retention: Duration::from_hours(24),
    };
    storage.update_retention_policy(policy).await.unwrap();

    // Insert metrics
    let (metric, data_points) = OtlpFixtures::valid_gauge();
    storage.ingest_metrics(vec![metric]).await.unwrap();
    storage
        .ingest_metric_data_points(data_points)
        .await
        .unwrap();

    // Wait for data to become old
    sleep(TokioDuration::from_millis(10)).await;

    // Run cleanup - should delete metrics
    let deleted = storage.run_retention_cleanup().await.unwrap();
    assert!(deleted > 0);
}

#[tokio::test]
async fn test_retention_cleanup_deletes_old_profiles() {
    let (storage, _temp_dir) = create_storage().await;

    // Set very short retention for profiles
    let policy = RetentionPolicy {
        spans_retention: Duration::from_hours(24),
        logs_retention: Duration::from_hours(24),
        metrics_retention: Duration::from_hours(24),
        profiles_retention: Duration::from_millis(1),
    };
    storage.update_retention_policy(policy).await.unwrap();

    // Insert profiles
    let profiles = vec![
        OtlpFixtures::valid_cpu_profile(),
        OtlpFixtures::valid_cpu_profile(),
    ];
    storage.ingest_profiles(profiles).await.unwrap();

    // Wait for data to become old
    sleep(TokioDuration::from_millis(10)).await;

    // Run cleanup - should delete profiles
    let deleted = storage.run_retention_cleanup().await.unwrap();
    assert!(deleted > 0);
}

#[tokio::test]
async fn test_retention_cleanup_selective() {
    let (storage, _temp_dir) = create_storage().await;

    // Set short retention only for logs, very long retention for everything else
    // Note: Fixtures use timestamps from 2023, so need 100-year retention to keep spans
    let policy = RetentionPolicy {
        spans_retention: Duration::from_days(36500),
        logs_retention: Duration::from_millis(1),
        metrics_retention: Duration::from_days(36500),
        profiles_retention: Duration::from_days(36500),
    };
    storage.update_retention_policy(policy).await.unwrap();

    // Insert both spans and logs
    let spans = OtlpFixtures::large_span_batch(3);
    let span_count = spans.len();
    storage.ingest_spans(spans).await.unwrap();

    let logs = OtlpFixtures::large_log_batch(3);
    storage.ingest_logs(logs).await.unwrap();

    // Wait for logs to become old
    sleep(TokioDuration::from_millis(10)).await;

    // Run cleanup - should only delete logs, not spans
    let deleted = storage.run_retention_cleanup().await.unwrap();
    assert!(deleted > 0);

    // Verify spans still exist
    let services = storage.get_services().await.unwrap();
    assert!(!services.is_empty() || span_count == 0);
}

// =============================================================================
// Maintenance Tests
// =============================================================================

#[tokio::test]
async fn test_run_maintenance() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert some data
    let spans = OtlpFixtures::large_span_batch(10);
    storage.ingest_spans(spans).await.unwrap();

    let logs = OtlpFixtures::large_log_batch(10);
    storage.ingest_logs(logs).await.unwrap();

    // Run maintenance
    let stats = storage.run_maintenance().await.unwrap();

    // Should return stats
    // TursoStorage has no hot/cold tiers, so no eviction happens
    // entries_evicted is unsigned, so just verify the call succeeded
    let _ = stats.entries_evicted;
}

#[tokio::test]
async fn test_maintenance_on_empty_database() {
    let (storage, _temp_dir) = create_storage().await;

    // Run maintenance on empty database
    let stats = storage.run_maintenance().await.unwrap();

    // Should not fail
    // TursoStorage has no hot/cold tiers, so no eviction happens
    // entries_evicted is unsigned, so just verify the call succeeded
    let _ = stats.entries_evicted;
}

#[tokio::test]
async fn test_maintenance_after_deletions() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert data
    let spans = OtlpFixtures::large_span_batch(20);
    storage.ingest_spans(spans).await.unwrap();

    // Set short retention and cleanup
    let policy = RetentionPolicy {
        spans_retention: Duration::from_millis(1),
        logs_retention: Duration::from_hours(24),
        metrics_retention: Duration::from_hours(24),
        profiles_retention: Duration::from_hours(24),
    };
    storage.update_retention_policy(policy).await.unwrap();
    sleep(TokioDuration::from_millis(10)).await;
    storage.run_retention_cleanup().await.unwrap();

    // Run maintenance after deletions
    let stats = storage.run_maintenance().await.unwrap();

    // Should vacuum deleted space
    // entries_evicted is unsigned, so just verify the call succeeded
    let _ = stats.entries_evicted;
}

// =============================================================================
// Storage Stats Tests
// =============================================================================

#[tokio::test]
async fn test_get_storage_stats_empty() {
    let (storage, _temp_dir) = create_storage().await;

    let stats = storage.get_storage_stats().await.unwrap();

    assert_eq!(stats.span_count, 0);
    assert_eq!(stats.log_count, 0);
    assert_eq!(stats.metric_count, 0);
    assert_eq!(stats.profile_count, 0);
    // Database size not tracked in StorageStats // Database has schema even if empty
}

#[tokio::test]
async fn test_get_storage_stats_with_spans() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert spans
    let spans = OtlpFixtures::large_span_batch(15);
    storage.ingest_spans(spans).await.unwrap();

    let stats = storage.get_storage_stats().await.unwrap();

    assert_eq!(stats.span_count, 15);
    assert_eq!(stats.log_count, 0);
    // Database size not tracked in StorageStats
}

#[tokio::test]
async fn test_get_storage_stats_with_all_types() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert all types
    let spans = OtlpFixtures::large_span_batch(5);
    storage.ingest_spans(spans).await.unwrap();

    let logs = OtlpFixtures::large_log_batch(7);
    storage.ingest_logs(logs).await.unwrap();

    let (metric, _data_points) = OtlpFixtures::valid_gauge();
    storage.ingest_metrics(vec![metric]).await.unwrap();

    let profiles = vec![
        OtlpFixtures::valid_cpu_profile(),
        OtlpFixtures::valid_cpu_profile(),
    ];
    storage.ingest_profiles(profiles).await.unwrap();

    let stats = storage.get_storage_stats().await.unwrap();

    assert_eq!(stats.span_count, 5);
    assert_eq!(stats.log_count, 7);
    assert_eq!(stats.metric_count, 1);
    assert_eq!(stats.profile_count, 2);
    // Database size not tracked in StorageStats
}

#[tokio::test]
async fn test_storage_stats_after_cleanup() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert data
    let spans = OtlpFixtures::large_span_batch(10);
    storage.ingest_spans(spans).await.unwrap();

    // Get initial stats
    let initial_stats = storage.get_storage_stats().await.unwrap();
    assert_eq!(initial_stats.span_count, 10);

    // Set short retention and cleanup
    let policy = RetentionPolicy {
        spans_retention: Duration::from_millis(1),
        logs_retention: Duration::from_hours(24),
        metrics_retention: Duration::from_hours(24),
        profiles_retention: Duration::from_hours(24),
    };
    storage.update_retention_policy(policy).await.unwrap();
    sleep(TokioDuration::from_millis(10)).await;
    storage.run_retention_cleanup().await.unwrap();

    // Get stats after cleanup
    let final_stats = storage.get_storage_stats().await.unwrap();
    assert_eq!(final_stats.span_count, 0);
}

// =============================================================================
// Retention Policy Edge Cases
// =============================================================================

#[tokio::test]
async fn test_retention_policy_zero_duration() {
    let (storage, _temp_dir) = create_storage().await;

    // Set zero retention (immediate deletion)
    let policy = RetentionPolicy {
        spans_retention: Duration::from_secs(0),
        logs_retention: Duration::from_secs(0),
        metrics_retention: Duration::from_secs(0),
        profiles_retention: Duration::from_secs(0),
    };

    storage
        .update_retention_policy(policy.clone())
        .await
        .unwrap();

    let retrieved = storage.get_retention_policy().await.unwrap();
    assert_eq!(retrieved.spans_retention, Duration::from_secs(0));
}

#[tokio::test]
async fn test_retention_policy_very_long_duration() {
    let (storage, _temp_dir) = create_storage().await;

    // Set very long retention (365 days)
    let policy = RetentionPolicy {
        spans_retention: Duration::from_days(365),
        logs_retention: Duration::from_days(365),
        metrics_retention: Duration::from_days(365),
        profiles_retention: Duration::from_days(365),
    };

    storage
        .update_retention_policy(policy.clone())
        .await
        .unwrap();

    let retrieved = storage.get_retention_policy().await.unwrap();
    assert_eq!(retrieved.spans_retention, Duration::from_days(365));
}

#[tokio::test]
async fn test_retention_policy_different_per_type() {
    let (storage, _temp_dir) = create_storage().await;

    // Different retention for each type
    let policy = RetentionPolicy {
        spans_retention: Duration::from_hours(1),
        logs_retention: Duration::from_hours(6),
        metrics_retention: Duration::from_days(7),
        profiles_retention: Duration::from_days(30),
    };

    storage
        .update_retention_policy(policy.clone())
        .await
        .unwrap();

    let retrieved = storage.get_retention_policy().await.unwrap();
    assert_eq!(retrieved.spans_retention, Duration::from_hours(1));
    assert_eq!(retrieved.logs_retention, Duration::from_hours(6));
    assert_eq!(retrieved.metrics_retention, Duration::from_days(7));
    assert_eq!(retrieved.profiles_retention, Duration::from_days(30));
}

// =============================================================================
// Concurrent Operations Tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_cleanup_calls() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert some data
    let spans = OtlpFixtures::large_span_batch(10);
    storage.ingest_spans(spans).await.unwrap();

    // Run multiple cleanups concurrently
    let storage1 = storage.clone();
    let storage2 = storage.clone();
    let storage3 = storage.clone();

    let handle1 = tokio::spawn(async move { storage1.run_retention_cleanup().await });
    let handle2 = tokio::spawn(async move { storage2.run_retention_cleanup().await });
    let handle3 = tokio::spawn(async move { storage3.run_retention_cleanup().await });

    // All should succeed
    handle1.await.unwrap().unwrap();
    handle2.await.unwrap().unwrap();
    handle3.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_concurrent_maintenance_calls() {
    let (storage, _temp_dir) = create_storage().await;

    // Run multiple maintenance operations concurrently
    let storage1 = storage.clone();
    let storage2 = storage.clone();

    let handle1 = tokio::spawn(async move { storage1.run_maintenance().await });
    let handle2 = tokio::spawn(async move { storage2.run_maintenance().await });

    // Both should succeed
    handle1.await.unwrap().unwrap();
    handle2.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_stats_during_ingestion() {
    let (storage, _temp_dir) = create_storage().await;

    // Start continuous ingestion
    let storage_ingest = storage.clone();
    let ingest_handle = tokio::spawn(async move {
        for _ in 0..5 {
            let spans = OtlpFixtures::large_span_batch(5);
            storage_ingest.ingest_spans(spans).await.unwrap();
            sleep(TokioDuration::from_millis(10)).await;
        }
    });

    // Query stats multiple times during ingestion
    for _ in 0..10 {
        let stats = storage.get_storage_stats().await.unwrap();
        assert!(stats.span_count <= 25); // At most 5 batches * 5 spans
        sleep(TokioDuration::from_millis(5)).await;
    }

    ingest_handle.await.unwrap();
}
