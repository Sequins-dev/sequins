use crate::error::Result;
use crate::storage::{MaintenanceStats, RetentionPolicy, StorageStats};

/// Trait for administrative operations
#[async_trait::async_trait]
pub trait ManagementApi: Send + Sync {
    /// Run retention cleanup to delete old data
    async fn run_retention_cleanup(&self) -> Result<usize>;

    /// Update retention policy
    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()>;

    /// Get current retention policy
    async fn get_retention_policy(&self) -> Result<RetentionPolicy>;

    /// Compact and optimize storage
    async fn run_maintenance(&self) -> Result<MaintenanceStats>;

    /// Get storage statistics
    async fn get_storage_stats(&self) -> Result<StorageStats>;
}
