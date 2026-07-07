use super::Storage;
use crate::error::Result;
use object_store::{ObjectStoreExt, PutPayload};
use sequins_types::health::{HealthMetricRule, HealthThresholdConfig};

impl Storage {
    /// Object-store path for the health-threshold config, under this node's
    /// storage prefix. Lives in object storage (not local disk) so it works for
    /// both `file://` and cloud backends and under a read-only root filesystem.
    fn health_config_object_path(&self) -> object_store::path::Path {
        let base =
            sequins_cold_tier::store_base_path(&self.config.cold_tier.uri).trim_end_matches('/');
        if base.is_empty() {
            object_store::path::Path::from("health_config.json")
        } else {
            object_store::path::Path::from(format!("{base}/health_config.json"))
        }
    }

    /// Get the health threshold configuration
    ///
    /// # Errors
    ///
    /// Returns an error if reading the config object fails
    pub async fn get_health_threshold_config(&self) -> Result<HealthThresholdConfig> {
        let store = self.cold_tier.read().await.store.clone();
        let path = self.health_config_object_path();

        match store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|e| {
                    crate::error::Error::Storage(format!("Failed to read health config: {}", e))
                })?;
                serde_json::from_slice(&bytes).map_err(|e| {
                    crate::error::Error::Serialization(format!(
                        "Failed to parse health config JSON: {}",
                        e
                    ))
                })
            }
            // No config written yet — return defaults.
            Err(object_store::Error::NotFound { .. }) => Ok(HealthThresholdConfig::default()),
            Err(e) => Err(crate::error::Error::Storage(format!(
                "Failed to read health config: {}",
                e
            ))),
        }
    }

    /// Set the health threshold configuration
    ///
    /// # Errors
    ///
    /// Returns an error if writing the config object fails
    pub async fn set_health_threshold_config(&self, config: HealthThresholdConfig) -> Result<()> {
        let store = self.cold_tier.read().await.store.clone();
        let path = self.health_config_object_path();

        let json = serde_json::to_vec_pretty(&config).map_err(|e| {
            crate::error::Error::Serialization(format!("Failed to serialize health config: {}", e))
        })?;

        store
            .put(&path, PutPayload::from(json))
            .await
            .map_err(|e| {
                crate::error::Error::Storage(format!("Failed to write health config: {}", e))
            })?;

        Ok(())
    }

    /// Add a health metric rule
    ///
    /// # Errors
    ///
    /// Returns an error if reading or writing the config file fails
    pub async fn add_health_rule(&self, rule: HealthMetricRule) -> Result<()> {
        let mut config = self.get_health_threshold_config().await?;
        config.rules.push(rule);
        self.set_health_threshold_config(config).await
    }

    /// Remove a health metric rule
    ///
    /// # Errors
    ///
    /// Returns an error if reading or writing the config file fails
    pub async fn remove_health_rule(
        &self,
        metric_name: &str,
        service_name: Option<&str>,
    ) -> Result<()> {
        let mut config = self.get_health_threshold_config().await?;
        config
            .rules
            .retain(|r| r.metric_name != metric_name || r.service_name.as_deref() != service_name);
        self.set_health_threshold_config(config).await
    }
}
