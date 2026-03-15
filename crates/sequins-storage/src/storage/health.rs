use super::Storage;
use crate::error::Result;
use sequins_types::health::{HealthMetricRule, HealthThresholdConfig};

impl Storage {
    /// Get the health threshold configuration
    ///
    /// # Errors
    ///
    /// Returns an error if reading the config file fails
    pub async fn get_health_threshold_config(&self) -> Result<HealthThresholdConfig> {
        use tokio::io::AsyncReadExt;

        // Try to read the health config file
        match tokio::fs::File::open(&self.health_config_path).await {
            Ok(mut file) => {
                let mut contents = String::new();
                file.read_to_string(&mut contents).await?;
                serde_json::from_str(&contents).map_err(|e| {
                    crate::error::Error::Serialization(format!(
                        "Failed to parse health config JSON: {}",
                        e
                    ))
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File doesn't exist, return default config
                Ok(HealthThresholdConfig::default())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Set the health threshold configuration
    ///
    /// # Errors
    ///
    /// Returns an error if writing the config file fails
    pub async fn set_health_threshold_config(&self, config: HealthThresholdConfig) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        // Ensure the parent directory exists
        if let Some(parent) = self.health_config_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let json = serde_json::to_string_pretty(&config).map_err(|e| {
            crate::error::Error::Serialization(format!("Failed to serialize health config: {}", e))
        })?;

        let mut file = tokio::fs::File::create(&self.health_config_path).await?;

        file.write_all(json.as_bytes()).await?;

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
