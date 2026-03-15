use crate::error::Result;
use crate::models::{MaintenanceStats, RetentionPolicy, StorageStats};

/// Trait for administrative operations
///
/// This trait defines the interface for system management operations.
/// It is implemented by `TieredStorage` (for local operations) and `ManagementClient`
/// (for remote HTTP access to the daemon's Management API server on port 8081).
///
/// **Requires elevated permissions** - Management operations should be protected
/// by authentication/authorization middleware in remote deployments.
#[async_trait::async_trait]
pub trait ManagementApi: Send + Sync {
    // Retention management
    /// Run retention cleanup to delete old data
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails
    ///
    /// Returns the number of entries deleted
    async fn run_retention_cleanup(&self) -> Result<usize>;

    /// Update retention policy
    ///
    /// # Errors
    ///
    /// Returns an error if update fails
    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()>;

    /// Get current retention policy
    ///
    /// # Errors
    ///
    /// Returns an error if retrieval fails
    async fn get_retention_policy(&self) -> Result<RetentionPolicy>;

    // Database maintenance
    /// Compact and optimize storage
    ///
    /// # Errors
    ///
    /// Returns an error if maintenance fails
    async fn run_maintenance(&self) -> Result<MaintenanceStats>;

    /// Get storage statistics
    ///
    /// # Errors
    ///
    /// Returns an error if stats retrieval fails
    async fn get_storage_stats(&self) -> Result<StorageStats>;
}
