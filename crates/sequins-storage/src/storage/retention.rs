use super::Storage;
use crate::error::Result;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use sequins_types::models::RetentionPolicy;
use std::sync::Arc;

impl Storage {
    /// Object-store path for the retention policy, under this node's storage
    /// prefix. Lives in object storage (not local disk) so it works uniformly
    /// for `file://` and cloud (`s3://`/`gs://`/`az://`) backends and survives a
    /// read-only root filesystem.
    fn retention_policy_object_path(uri: &str) -> object_store::path::Path {
        let base = sequins_cold_tier::store_base_path(uri).trim_end_matches('/');
        if base.is_empty() {
            object_store::path::Path::from("retention-policy.json")
        } else {
            object_store::path::Path::from(format!("{base}/retention-policy.json"))
        }
    }

    /// Load the retention policy from object storage if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the object exists but cannot be read or parsed.
    pub(super) async fn load_retention_policy(
        store: &Arc<dyn ObjectStore>,
        uri: &str,
    ) -> Result<Option<RetentionPolicy>> {
        let path = Self::retention_policy_object_path(uri);
        let bytes = match store.get(&path).await {
            Ok(result) => result.bytes().await.map_err(|e| {
                crate::error::Error::Storage(format!("Failed to read retention policy: {}", e))
            })?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(crate::error::Error::Storage(format!(
                    "Failed to read retention policy: {}",
                    e
                )))
            }
        };

        let policy: RetentionPolicy = serde_json::from_slice(&bytes).map_err(|e| {
            crate::error::Error::Storage(format!("Failed to parse retention policy: {}", e))
        })?;

        Ok(Some(policy))
    }

    /// Save the retention policy to object storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the policy cannot be serialized or written.
    pub(super) async fn save_retention_policy(&self, policy: &RetentionPolicy) -> Result<()> {
        let store = self.cold_tier.read().await.store.clone();
        let path = Self::retention_policy_object_path(&self.config.cold_tier.uri);

        let contents = serde_json::to_vec_pretty(policy).map_err(|e| {
            crate::error::Error::Storage(format!("Failed to serialize retention policy: {}", e))
        })?;

        store
            .put(&path, PutPayload::from(contents))
            .await
            .map_err(|e| {
                crate::error::Error::Storage(format!("Failed to write retention policy: {}", e))
            })?;

        Ok(())
    }
}
