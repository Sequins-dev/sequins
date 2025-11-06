use sequins_core::Duration;
use serde::{Deserialize, Serialize};

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

/// Hot tier (in-memory) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HotTierConfig {
    /// Maximum duration to keep data in hot tier before flushing
    #[serde(default = "HotTierConfig::default_max_age")]
    pub max_age: Duration,

    /// Maximum number of entries before forcing a flush (per signal type)
    #[serde(default = "HotTierConfig::default_max_entries")]
    pub max_entries: usize,
}

impl HotTierConfig {
    fn default_max_age() -> Duration {
        Duration::from_minutes(5)
    }

    fn default_max_entries() -> usize {
        10_000
    }
}

/// Cold tier (Parquet) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ColdTierConfig {
    /// Object store URI (e.g., "s3://bucket/path", "file:///local/path")
    pub uri: String,

    /// Enable bloom filters on Parquet files for fast lookups
    #[serde(default = "ColdTierConfig::default_bloom_filters")]
    pub enable_bloom_filters: bool,

    /// Parquet compression codec
    #[serde(default = "ColdTierConfig::default_compression")]
    pub compression: CompressionCodec,

    /// Row group size for Parquet files
    #[serde(default = "ColdTierConfig::default_row_group_size")]
    pub row_group_size: usize,

    /// Path to persistent index for disk-based deployments
    ///
    /// If Some, a persistent index will be created at this path for faster lookups.
    /// Currently implemented using RocksDB, but this is an implementation detail.
    pub index_path: Option<String>,
}

impl ColdTierConfig {
    fn default_bloom_filters() -> bool {
        true
    }

    fn default_compression() -> CompressionCodec {
        CompressionCodec::Zstd
    }

    fn default_row_group_size() -> usize {
        5000
    }
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

/// Parquet compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    Uncompressed,
    Snappy,
    Gzip,
    Brotli,
    Lz4,
    Zstd,
}

impl From<CompressionCodec> for parquet::basic::Compression {
    fn from(codec: CompressionCodec) -> Self {
        match codec {
            CompressionCodec::Uncompressed => parquet::basic::Compression::UNCOMPRESSED,
            CompressionCodec::Snappy => parquet::basic::Compression::SNAPPY,
            CompressionCodec::Gzip => parquet::basic::Compression::GZIP(Default::default()),
            CompressionCodec::Brotli => parquet::basic::Compression::BROTLI(Default::default()),
            CompressionCodec::Lz4 => parquet::basic::Compression::LZ4,
            CompressionCodec::Zstd => parquet::basic::Compression::ZSTD(Default::default()),
        }
    }
}

impl Default for HotTierConfig {
    fn default() -> Self {
        Self {
            max_age: Self::default_max_age(),
            max_entries: Self::default_max_entries(),
        }
    }
}

impl Default for ColdTierConfig {
    fn default() -> Self {
        Self {
            uri: "file:///tmp/sequins".to_string(),
            enable_bloom_filters: Self::default_bloom_filters(),
            compression: Self::default_compression(),
            row_group_size: Self::default_row_group_size(),
            index_path: None,
        }
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
        assert_eq!(config.cold_tier.compression, CompressionCodec::Zstd);
        assert!(config.cold_tier.enable_bloom_filters);
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
            enable_bloom_filters: true,
            compression: CompressionCodec::Snappy,
            row_group_size: 10_000,
            index_path: Some("/var/lib/sequins/index".to_string()),
        };

        assert_eq!(config.uri, "s3://my-bucket/telemetry");
        assert_eq!(config.compression, CompressionCodec::Snappy);
        assert_eq!(config.row_group_size, 10_000);
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
    fn test_compression_codec_equality() {
        assert_eq!(CompressionCodec::Zstd, CompressionCodec::Zstd);
        assert_ne!(CompressionCodec::Snappy, CompressionCodec::Gzip);
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
        assert!(config.cold_tier.enable_bloom_filters); // default
        assert_eq!(config.cold_tier.compression, CompressionCodec::Zstd); // default
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
  bloom-filters: true
  compression: "snappy"
  row-group-size: 10000
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
        assert!(config.cold_tier.enable_bloom_filters);
        assert_eq!(config.cold_tier.compression, CompressionCodec::Snappy);
        assert_eq!(config.cold_tier.row_group_size, 10000);
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
        assert!(config.cold_tier.enable_bloom_filters);
        assert_eq!(config.cold_tier.compression, CompressionCodec::Zstd);
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
    fn test_yaml_error_unknown_compression() {
        let yaml = r#"
hot-tier:
  max-age: "5m"
  max-entries: 10000

cold-tier:
  uri: "file:///tmp/sequins"
  compression: "unknown"

lifecycle:
  retention: "7d"
  flush-interval: "5m"
  cleanup-interval: "1h"
        "#;

        let result = StorageConfig::from_yaml(yaml);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains("compression") || error.contains("unknown variant"));
    }
}
