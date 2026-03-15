use super::Storage;
use std::sync::Arc;

impl Storage {
    /// Start the background flush task that periodically moves data from hot tier to cold tier
    ///
    /// This spawns a tokio task that runs in the background, calling `run_maintenance_internal()`
    /// at the interval specified in `config.lifecycle.flush_interval`.
    ///
    /// Returns a `JoinHandle` that can be awaited to ensure the task completes gracefully.
    /// Call `shutdown()` to signal the task to stop.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use sequins_storage::{Storage, StorageConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage = Arc::new(Storage::new(StorageConfig::default()).await?);
    /// let flush_handle = Storage::start_background_flush(Arc::clone(&storage));
    ///
    /// // ... use storage ...
    ///
    /// // Graceful shutdown
    /// storage.shutdown();
    /// flush_handle.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn start_background_flush(storage: Arc<Storage>) -> tokio::task::JoinHandle<()> {
        let flush_interval = storage.config.lifecycle.flush_interval;
        let shutdown_notify = Arc::clone(&storage.shutdown_notify);

        tokio::spawn(async move {
            // Create interval timer from nanoseconds
            let interval_nanos = flush_interval.as_nanos();
            let interval_duration = if interval_nanos > 0 {
                std::time::Duration::from_nanos(interval_nanos as u64)
            } else {
                // Fallback to 1 second if somehow we get zero
                std::time::Duration::from_secs(1)
            };
            let mut interval = tokio::time::interval(interval_duration);

            // Skip the first tick (fires immediately)
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Run periodic flush
                        if let Err(e) = storage.run_maintenance_internal().await {
                            eprintln!("Background flush error: {}", e);
                        }
                    }
                    _ = shutdown_notify.notified() => {
                        // Graceful shutdown: run one final flush
                        if let Err(e) = storage.run_maintenance_internal().await {
                            eprintln!("Final flush error during shutdown: {}", e);
                        }
                        break;
                    }
                }
            }
        })
    }

    /// Signal the background flush task to shut down gracefully
    ///
    /// This sends a shutdown signal to the background task spawned by `start_background_flush()`.
    /// The task will perform one final flush and then exit.
    ///
    /// To ensure the task has fully stopped, await the `JoinHandle` returned by `start_background_flush()`.
    pub fn shutdown(&self) {
        self.shutdown_notify.notify_one();
    }
}
