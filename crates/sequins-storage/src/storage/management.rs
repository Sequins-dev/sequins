use super::Storage;
use sequins_types::models::{
    MaintenanceStats as CoreMaintenanceStats, RetentionPolicy, StorageStats as CoreStorageStats,
};
use sequins_types::ManagementApi;

#[async_trait::async_trait]
impl ManagementApi for Storage {
    async fn run_retention_cleanup(&self) -> sequins_types::error::Result<usize> {
        // Get the retention policy
        let policy = self.get_retention_policy().await?;

        let cold_tier = self.cold_tier.write().await;

        let mut total_deleted = 0;

        // Cleanup spans
        total_deleted += cold_tier
            .cleanup_old_files("spans", policy.spans_retention)
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to cleanup spans: {}", e))
            })?;

        // Cleanup logs
        total_deleted += cold_tier
            .cleanup_old_files("logs", policy.logs_retention)
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to cleanup logs: {}", e))
            })?;

        // Cleanup metrics
        total_deleted += cold_tier
            .cleanup_old_files("metrics", policy.metrics_retention)
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to cleanup metrics: {}", e))
            })?;

        // Cleanup profiles
        total_deleted += cold_tier
            .cleanup_old_files("profiles", policy.profiles_retention)
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to cleanup profiles: {}", e))
            })?;

        Ok(total_deleted)
    }

    async fn update_retention_policy(
        &self,
        policy: RetentionPolicy,
    ) -> sequins_types::error::Result<()> {
        // Save policy to disk
        self.save_retention_policy(&policy).map_err(|e| {
            sequins_types::error::Error::Other(format!("Failed to save retention policy: {}", e))
        })?;

        // Update in-memory policy
        let mut retention_policy = self.retention_policy.write().await;
        *retention_policy = Some(policy);

        Ok(())
    }

    async fn get_retention_policy(&self) -> sequins_types::error::Result<RetentionPolicy> {
        // Return persisted policy if it exists, otherwise use defaults from config
        let retention_policy = self.retention_policy.read().await;
        Ok(retention_policy.clone().unwrap_or(RetentionPolicy {
            spans_retention: self.config.lifecycle.retention,
            logs_retention: self.config.lifecycle.retention,
            metrics_retention: self.config.lifecycle.retention,
            profiles_retention: self.config.lifecycle.retention,
        }))
    }

    async fn run_maintenance(&self) -> sequins_types::error::Result<CoreMaintenanceStats> {
        // Call the internal run_maintenance_internal method
        let stats = self.run_maintenance_internal().await.map_err(|e| {
            sequins_types::error::Error::Other(format!("Maintenance failed: {}", e))
        })?;

        // Convert local MaintenanceStats to core MaintenanceStats
        Ok(CoreMaintenanceStats {
            entries_evicted: stats.entries_evicted,
            batches_flushed: stats.batches_flushed,
        })
    }

    async fn get_storage_stats(&self) -> sequins_types::error::Result<CoreStorageStats> {
        // Call the existing stats method
        let stats = self.stats();

        // Convert local StorageStats to core StorageStats
        Ok(CoreStorageStats {
            span_count: stats.span_count,
            log_count: stats.log_count,
            metric_count: stats.metric_count,
            profile_count: stats.profile_count,
        })
    }
}
