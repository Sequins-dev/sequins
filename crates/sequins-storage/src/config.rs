use sequins_types::Duration;
use serde::{Deserialize, Serialize};

pub use sequins_cold_tier::{ColdTierConfig, CompanionIndexConfig};
pub use sequins_hot_tier::HotTierConfig;

/// Storage configuration for tiered storage system
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    /// Hot tier configuration
    pub hot_tier: HotTierConfig,

    /// Cold tier configuration
    pub cold_tier: ColdTierConfig,

    /// Data lifecycle configuration
    pub lifecycle: LifecycleConfig,
}

/// Data lifecycle configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LifecycleConfig {
    /// How long to retain data before deletion
    #[serde(default = "LifecycleConfig::default_retention")]
    pub retention: Duration,

    /// How often to flush hot tier to cold tier
    #[serde(default = "LifecycleConfig::default_flush_interval")]
    pub flush_interval: Duration,

    /// How often to check for expired data
    #[serde(default = "LifecycleConfig::default_cleanup_interval")]
    pub cleanup_interval: Duration,
}

impl LifecycleConfig {
    fn default_retention() -> Duration {
        Duration::from_hours(7 * 24)
    }

    fn default_flush_interval() -> Duration {
        Duration::from_minutes(5)
    }

    fn default_cleanup_interval() -> Duration {
        Duration::from_hours(1)
    }
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            retention: Self::default_retention(),
            flush_interval: Self::default_flush_interval(),
            cleanup_interval: Self::default_cleanup_interval(),
        }
    }
}

impl StorageConfig {
    /// Parse storage configuration from YAML string
    ///
    /// # Errors
    ///
    /// Returns an error if the YAML is malformed or contains invalid values
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        serde_yaml::from_str(yaml).map_err(|e| format!("Failed to parse YAML config: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StorageConfig::default();

        assert_eq!(config.hot_tier.max_age.as_minutes(), 5);
        assert_eq!(config.hot_tier.max_entries, 10_000);
        assert_eq!(config.cold_tier.row_block_size, 65536);
        assert!(config.cold_tier.compact_encodings);
        assert!(config.cold_tier.companion_index.bloom_enabled);
        assert_eq!(config.lifecycle.retention.as_hours(), 7 * 24);
    }

    #[test]
    fn test_hot_tier_config() {
        let config = HotTierConfig {
            max_age: Duration::from_minutes(10),
            max_entries: 50_000,
        };

        assert_eq!(config.max_age.as_minutes(), 10);
        assert_eq!(config.max_entries, 50_000);
    }

    #[test]
    fn test_cold_tier_config() {
        let config = ColdTierConfig {
            uri: "s3://my-bucket/telemetry".to_string(),
            row_block_size: 10_000,
            compact_encodings: true,
            index_path: Some("/var/lib/sequins/index".to_string()),
            max_attribute_columns: 256,
            companion_index: CompanionIndexConfig::default(),
        };

        assert_eq!(config.uri, "s3://my-bucket/telemetry");
        assert_eq!(config.row_block_size, 10_000);
        assert!(config.compact_encodings);
    }

    #[test]
    fn test_lifecycle_config() {
        let config = LifecycleConfig {
            retention: Duration::from_hours(30 * 24), // 30 days
            flush_interval: Duration::from_minutes(10),
            cleanup_interval: Duration::from_hours(6),
        };

        assert_eq!(config.retention.as_hours(), 30 * 24);
        assert_eq!(config.flush_interval.as_minutes(), 10);
        assert_eq!(config.cleanup_interval.as_hours(), 6);
    }

    #[test]
    fn test_companion_index_config() {
        let config = CompanionIndexConfig::default();
        assert!(config.tantivy_enabled);
        assert!(config.bloom_enabled);
        assert!(config.trigram_enabled);
        assert_eq!(config.cardinality_threshold, 10_000);
        assert_eq!(config.bloom_fpr, 0.01);
    }

    #[test]
    fn test_yaml_minimal_config() {
        let yaml = r#"
hot-tier:
  max-age: "5m"
  max-entries: 10000

cold-tier:
  uri: "file:///tmp/sequins"

lifecycle:
  retention: "7d"
  flush-interval: "5m"
  cleanup-interval: "1h"
        "#;

        let config = StorageConfig::from_yaml(yaml).unwrap();

        assert_eq!(config.hot_tier.max_age.as_minutes(), 5);
        assert_eq!(config.hot_tier.max_entries, 10000);
        assert_eq!(config.cold_tier.uri, "file:///tmp/sequins");
        assert!(config.cold_tier.compact_encodings); // default
        assert!(config.cold_tier.companion_index.bloom_enabled); // default
        assert_eq!(config.lifecycle.retention.as_hours(), 7 * 24);
    }

    #[test]
    fn test_yaml_duration_units() {
        let yaml = r#"
hot-tier:
  max-age: "300s"
  max-entries: 10000

cold-tier:
  uri: "file:///tmp/sequins"

lifecycle:
  retention: "168h"
  flush-interval: "5m"
  cleanup-interval: "60m"
        "#;

        let config = StorageConfig::from_yaml(yaml).unwrap();

        assert_eq!(config.hot_tier.max_age.as_secs(), 300);
        assert_eq!(config.lifecycle.retention.as_hours(), 168);
        assert_eq!(config.lifecycle.cleanup_interval.as_minutes(), 60);
    }

    #[test]
    fn test_yaml_full_config() {
        let yaml = r#"
hot-tier:
  max-age: "10m"
  max-entries: 50000

cold-tier:
  uri: "s3://bucket/path"
  row-block-size: 10000
  compact-encodings: true
  index-path: "/var/lib/sequins"

lifecycle:
  retention: "30d"
  flush-interval: "10m"
  cleanup-interval: "6h"
        "#;

        let config = StorageConfig::from_yaml(yaml).unwrap();

        assert_eq!(config.hot_tier.max_age.as_minutes(), 10);
        assert_eq!(config.hot_tier.max_entries, 50000);
        assert_eq!(config.cold_tier.uri, "s3://bucket/path");
        assert!(config.cold_tier.compact_encodings);
        assert_eq!(config.cold_tier.row_block_size, 10000);
        assert_eq!(
            config.cold_tier.index_path,
            Some("/var/lib/sequins".to_string())
        );
        assert_eq!(config.lifecycle.retention.as_hours(), 30 * 24);
        assert_eq!(config.lifecycle.flush_interval.as_minutes(), 10);
        assert_eq!(config.lifecycle.cleanup_interval.as_hours(), 6);
    }

    #[test]
    fn test_yaml_partial_config_uses_defaults() {
        let yaml = r#"
hot-tier:
  max-age: "10m"

cold-tier:
  uri: "file:///tmp/sequins"

lifecycle:
  retention: "14d"
        "#;

        let config = StorageConfig::from_yaml(yaml).unwrap();

        // Specified values
        assert_eq!(config.hot_tier.max_age.as_minutes(), 10);
        assert_eq!(config.lifecycle.retention.as_hours(), 14 * 24);

        // Default values
        assert_eq!(config.hot_tier.max_entries, 10_000);
        assert!(config.cold_tier.compact_encodings);
        assert!(config.cold_tier.companion_index.bloom_enabled);
        assert_eq!(config.lifecycle.flush_interval.as_minutes(), 5);
        assert_eq!(config.lifecycle.cleanup_interval.as_hours(), 1);
    }

    #[test]
    fn test_yaml_error_invalid_duration_unit() {
        let yaml = r#"
hot-tier:
  max-age: "5x"
  max-entries: 10000

cold-tier:
  uri: "file:///tmp/sequins"

lifecycle:
  retention: "7d"
  flush-interval: "5m"
  cleanup-interval: "1h"
        "#;

        let result = StorageConfig::from_yaml(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_yaml_companion_index_config() {
        let yaml = r#"
hot-tier:
  max-age: "5m"
  max-entries: 10000

cold-tier:
  uri: "file:///tmp/sequins"
  companion-index:
    tantivy-enabled: false
    bloom-enabled: true
    trigram-enabled: true
    cardinality-threshold: 5000

lifecycle:
  retention: "7d"
  flush-interval: "5m"
  cleanup-interval: "1h"
        "#;

        let config = StorageConfig::from_yaml(yaml).unwrap();
        assert!(!config.cold_tier.companion_index.tantivy_enabled);
        assert!(config.cold_tier.companion_index.bloom_enabled);
        assert!(config.cold_tier.companion_index.trigram_enabled);
        assert_eq!(config.cold_tier.companion_index.cardinality_threshold, 5000);
    }
}
