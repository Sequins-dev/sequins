# Data Retention Management

[← Back to Index](INDEX.md)

**Related Documentation:** [database.md](database.md) | [workspace-and-crates.md](workspace-and-crates.md)

---

## Overview

The `RetentionManager` is a separate component that handles automatic cleanup of old data. It supports per-data-type retention policies and runs as a background task managed by the storage layer.

## RetentionPolicy Configuration

```rust
// sequins-storage/src/retention.rs
use std::time::Duration;
use tokio::task::JoinHandle;

/// Retention policy configuration with per-data-type durations
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub traces_hours: u32,
    pub logs_hours: u32,
    pub metrics_hours: u32,
    pub profiles_hours: u32,
    pub cleanup_interval_secs: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            traces_hours: 24,
            logs_hours: 24,
            metrics_hours: 24,
            profiles_hours: 24,
            cleanup_interval_secs: 300, // 5 minutes
        }
    }
}
```

## RetentionManager Implementation

```rust
pub struct RetentionManager {
    object_store: Arc<dyn ObjectStore>,
    policy: RetentionPolicy,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
}

impl RetentionManager {
    /// Create a new RetentionManager with the given policy
    pub fn new(object_store: Arc<dyn ObjectStore>, policy: RetentionPolicy) -> Self {
        Self {
            object_store,
            policy,
            shutdown_tx: None,
            task_handle: None,
        }
    }

    /// Start the background cleanup task
    pub fn start(&mut self) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let object_store = self.object_store.clone();
        let policy = self.policy.clone();

        let task_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                Duration::from_secs(policy.cleanup_interval_secs)
            );

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Self::run_cleanup(&object_store, &policy).await {
                            tracing::error!("Retention cleanup failed: {}", e);
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("RetentionManager shutting down");
                        break;
                    }
                }
            }
        });

        self.shutdown_tx = Some(shutdown_tx);
        self.task_handle = Some(task_handle);
    }

    /// Stop the background cleanup task gracefully
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }
    }

    /// Run cleanup once with the configured policy (deletes old Parquet files)
    async fn run_cleanup(object_store: &Arc<dyn ObjectStore>, policy: &RetentionPolicy) -> Result<()> {
        let now = Self::current_time_ns();

        let traces_cutoff = now - (policy.traces_hours as i64 * 3_600_000_000_000);
        let logs_cutoff = now - (policy.logs_hours as i64 * 3_600_000_000_000);
        let metrics_cutoff = now - (policy.metrics_hours as i64 * 3_600_000_000_000);
        let profiles_cutoff = now - (policy.profiles_hours as i64 * 3_600_000_000_000);

        // Delete old Parquet files for each data type
        let deleted_traces = Self::delete_old_files(object_store, "traces", traces_cutoff).await?;
        let deleted_logs = Self::delete_old_files(object_store, "logs", logs_cutoff).await?;
        let deleted_metrics = Self::delete_old_files(object_store, "metrics", metrics_cutoff).await?;
        let deleted_profiles = Self::delete_old_files(object_store, "profiles", profiles_cutoff).await?;

        tracing::debug!(
            "Retention cleanup: traces={} files, logs={} files, metrics={} files, profiles={} files",
            deleted_traces, deleted_logs, deleted_metrics, deleted_profiles
        );

        Ok(())
    }

    /// Delete Parquet files older than cutoff timestamp
    async fn delete_old_files(
        object_store: &Arc<dyn ObjectStore>,
        prefix: &str,
        cutoff_ns: i64,
    ) -> Result<usize> {
        let mut deleted_count = 0;
        let prefix_path = format!("{}/", prefix).into();
        let mut stream = object_store.list(Some(&prefix_path)).await?;

        while let Some(meta) = stream.next().await.transpose()? {
            // Parse hour bucket from path (e.g., "traces/2025-01-15-14/")
            if let Some(timestamp) = parse_hour_from_path(&meta.location) {
                if timestamp < cutoff_ns {
                    // Delete entire hour directory (all files in it)
                    object_store.delete(&meta.location).await?;
                    deleted_count += 1;
                }
            }
        }

        Ok(deleted_count)
    }

    /// Manual cleanup trigger (useful for testing and on-demand cleanup)
    pub async fn cleanup_now(&self) -> Result<usize> {
        Self::run_cleanup(&self.object_store, &self.policy).await?;
        Ok(0) // Could return total deleted file count if needed
    }

    fn current_time_ns() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }
}

impl Drop for RetentionManager {
    fn drop(&mut self) {
        // Send shutdown signal if task is still running
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Note: We can't .await in Drop, so the task handle will be aborted
        // For graceful shutdown, call .stop().await before dropping
    }
}
```

## Graceful Shutdown

The `RetentionManager` uses Tokio channels for graceful shutdown:

**Shutdown Flow:**
1. Call `stop()` before dropping storage
2. Sends shutdown signal via oneshot channel
3. Background task receives signal via `tokio::select!`
4. Task exits cleanly
5. `await` the task handle to ensure completion

**Drop Safety:**
- `Drop` implementation sends shutdown signal
- But cannot `.await` in `Drop` (not async context)
- Best practice: call `.stop().await` explicitly before dropping

## Usage in TieredStorage

```rust
// sequins-storage/src/lib.rs
use papaya::HashMap;

impl TieredStorage {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        index_type: IndexType,
        policy: RetentionPolicy,
    ) -> Result<Self> {
        let hot = Arc::new(HashMap::new());  // Papaya lock-free HashMap
        let datafusion_ctx = SessionContext::new();
        let index = index_type.map(|path| RocksDbIndex::new(path)).transpose()?;
        let retention_manager = Arc::new(Mutex::new(
            RetentionManager::new(object_store.clone(), policy)
        ));
        Ok(Self { hot, object_store, datafusion_ctx, index, retention_manager })
    }

    /// Start retention manager background task
    pub fn start_retention(&self) {
        let mut manager = self.retention_manager.lock().unwrap();
        manager.start();
    }

    /// Graceful shutdown - call before dropping
    pub async fn shutdown(&self) {
        let mut manager = self.retention_manager.lock().unwrap();
        manager.stop().await;
    }
}
```

## Configuration Examples

### Default Policy (24 hours)

```rust
let object_store = Arc::new(LocalFileSystem::new_with_prefix("/var/lib/sequins")?);
let index = None;  // Use Parquet built-in bloom filters
let storage = TieredStorage::with_defaults(object_store, index)?;
storage.start_retention();
```

### Custom Policy

```rust
let policy = RetentionPolicy {
    traces_hours: 48,      // Keep traces for 2 days
    logs_hours: 24,        // Keep logs for 1 day
    metrics_hours: 168,    // Keep metrics for 7 days
    profiles_hours: 12,    // Keep profiles for 12 hours
    cleanup_interval_secs: 600, // Run cleanup every 10 minutes
};

let object_store = Arc::new(AmazonS3::from_env()?);
let index_type = IndexType::RocksDB("/var/lib/sequins/index".into());
let storage = TieredStorage::new(object_store, index_type, policy)?;
storage.start_retention();
```

### Production Configuration

```kdl
// config.kdl
retention {
    traces-hours 72        // 3 days
    logs-hours 48          // 2 days
    metrics-hours 720      // 30 days
    profiles-hours 24      // 1 day
    cleanup-interval-secs 300  // 5 minutes
}
```

## Cleanup Behavior

### Automatic Cleanup
- Runs on configured interval (default: 5 minutes)
- Deletes Parquet files older than retention period
- Per-data-type retention policies (traces, logs, metrics, profiles)
- Hour-based deletion (entire hour directories)

### Manual Cleanup
- Trigger via `ManagementApi::run_retention_cleanup()`
- Useful for testing
- Useful for on-demand cleanup
- Returns count of deleted files

### File-Based Deletion
- Parquet files are immutable (no UPDATE/DELETE operations)
- Deletion is at hour bucket granularity (e.g., `traces/2025-01-15-14/`)
- Space reclaimed immediately (no VACUUM needed)
- Parquet bloom filters deleted automatically with Parquet files
- RocksDB index compaction can be triggered via `ManagementApi::optimize_indexes()` (if enabled)

## Performance Considerations

### Cleanup Impact
- File deletion is fast (S3 delete API calls)
- No VACUUM needed (space reclaimed immediately)
- Cleanup runs in background, doesn't block ingestion
- Can adjust interval based on data volume
- Hour-based granularity means slight over-retention (up to 1 hour)

### Monitoring
- Log cleanup statistics at debug level
- Track deletion counts per data type (files deleted)
- Monitor object store size over time
- Alert on cleanup failures
- Track index size growth (RocksDB only)

---

**Last Updated:** 2025-11-05
