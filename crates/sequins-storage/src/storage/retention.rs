use super::Storage;
use crate::error::Result;
use sequins_types::models::RetentionPolicy;

impl Storage {
    /// Get the path to the retention policy file based on the storage URI
    fn retention_policy_path(uri: &str) -> std::path::PathBuf {
        let path = uri.strip_prefix("file://").unwrap_or(uri);
        std::path::Path::new(path).join("retention-policy.json")
    }

    /// Load retention policy from disk if it exists
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed
    pub(super) fn load_retention_policy(uri: &str) -> Result<Option<RetentionPolicy>> {
        let policy_path = Self::retention_policy_path(uri);

        if !policy_path.exists() {
            return Ok(None);
        }

        let contents = std::fs::read_to_string(&policy_path).map_err(|e| {
            crate::error::Error::Storage(format!(
                "Failed to read retention policy from {}: {}",
                policy_path.display(),
                e
            ))
        })?;

        let policy: RetentionPolicy = serde_json::from_str(&contents).map_err(|e| {
            crate::error::Error::Storage(format!(
                "Failed to parse retention policy from {}: {}",
                policy_path.display(),
                e
            ))
        })?;

        Ok(Some(policy))
    }

    /// Save retention policy to disk
    ///
    /// # Errors
    ///
    /// Returns an error if the policy cannot be serialized or written to disk
    pub(super) fn save_retention_policy(&self, policy: &RetentionPolicy) -> Result<()> {
        let policy_path = Self::retention_policy_path(&self.config.cold_tier.uri);

        // Ensure the parent directory exists
        if let Some(parent) = policy_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                crate::error::Error::Storage(format!(
                    "Failed to create directory for retention policy: {}",
                    e
                ))
            })?;
        }

        let contents = serde_json::to_string_pretty(policy).map_err(|e| {
            crate::error::Error::Storage(format!("Failed to serialize retention policy: {}", e))
        })?;

        std::fs::write(&policy_path, contents).map_err(|e| {
            crate::error::Error::Storage(format!(
                "Failed to write retention policy to {}: {}",
                policy_path.display(),
                e
            ))
        })?;

        Ok(())
    }
}
