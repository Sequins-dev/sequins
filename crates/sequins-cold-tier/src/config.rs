//! Configuration types for the cold tier.

use serde::{Deserialize, Serialize};

/// Cold tier (Vortex) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ColdTierConfig {
    /// Object store URI (e.g., "s3://bucket/path", "file:///local/path")
    pub uri: String,

    /// Row block size for Vortex files (similar to Parquet row groups)
    #[serde(default = "ColdTierConfig::default_row_block_size")]
    pub row_block_size: usize,

    /// Enable cascading compression with compact encodings
    #[serde(default = "ColdTierConfig::default_compact_encodings")]
    pub compact_encodings: bool,

    /// Maximum number of attribute columns per Vortex file
    ///
    /// This is a safety ceiling to prevent pathological cases where a single batch
    /// has an extreme number of unique attributes. If exceeded, least-frequent
    /// attributes fall back to JSON overflow column. In practice, this limit
    /// should never be hit.
    #[serde(default = "ColdTierConfig::default_max_attribute_columns")]
    pub max_attribute_columns: usize,

    /// Path to persistent index for disk-based deployments
    ///
    /// If Some, a persistent index will be created at this path for faster lookups.
    /// Currently implemented using RocksDB, but this is an implementation detail.
    pub index_path: Option<String>,

    /// Companion index configuration
    #[serde(default)]
    pub companion_index: CompanionIndexConfig,

    /// Connection options for cloud object stores (S3/GCS/Azure). Ignored for
    /// `file://` URIs. Credentials are resolved from the provider's standard
    /// credential chain (instance profile / IRSA / workload identity) unless
    /// overridden here.
    #[serde(default)]
    pub object_store: ObjectStoreConfig,
}

impl ColdTierConfig {
    fn default_row_block_size() -> usize {
        65536
    }

    fn default_compact_encodings() -> bool {
        true
    }

    fn default_max_attribute_columns() -> usize {
        256
    }
}

impl Default for ColdTierConfig {
    fn default() -> Self {
        Self {
            uri: "file:///tmp/sequins".to_string(),
            row_block_size: Self::default_row_block_size(),
            compact_encodings: Self::default_compact_encodings(),
            max_attribute_columns: Self::default_max_attribute_columns(),
            index_path: None,
            companion_index: CompanionIndexConfig::default(),
            object_store: ObjectStoreConfig::default(),
        }
    }
}

/// Connection options for cloud object stores. All fields are optional; unset
/// values fall back to the object-store provider's defaults (including the
/// standard credential chain — instance profile, IRSA / workload identity).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ObjectStoreConfig {
    /// Region (e.g. `us-east-1`). Unset uses the provider default.
    pub region: Option<String>,

    /// Custom endpoint URL for S3-compatible stores (MinIO, Ceph RGW, a gateway).
    pub endpoint: Option<String>,

    /// Allow plain-HTTP endpoints. Required for a local MinIO served over http
    /// (object stores reject non-https endpoints otherwise).
    #[serde(default)]
    pub allow_http: bool,

    /// Force path-style addressing (`endpoint/bucket`) instead of virtual-hosted
    /// (`bucket.endpoint`). Most S3-compatibles (MinIO) need path-style.
    pub virtual_hosted_style: Option<bool>,

    /// Static access key id. Prefer the credential chain (IRSA / instance
    /// profile) in production; set this only for local/dev (e.g. MinIO).
    pub access_key_id: Option<String>,

    /// Static secret access key (paired with `access_key_id`).
    pub secret_access_key: Option<String>,
}

/// Companion index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CompanionIndexConfig {
    /// Enable Tantivy inverted index for metadata fields
    #[serde(default = "CompanionIndexConfig::default_tantivy_enabled")]
    pub tantivy_enabled: bool,

    /// Enable bloom filters for high-cardinality fields
    #[serde(default = "CompanionIndexConfig::default_bloom_enabled")]
    pub bloom_enabled: bool,

    /// Enable trigram index for log body text search
    #[serde(default = "CompanionIndexConfig::default_trigram_enabled")]
    pub trigram_enabled: bool,

    /// Cardinality threshold for inverted index vs bloom filter
    /// Fields with < threshold unique values use inverted index
    /// Fields with >= threshold unique values use bloom filter
    #[serde(default = "CompanionIndexConfig::default_cardinality_threshold")]
    pub cardinality_threshold: usize,

    /// Bloom filter false positive rate (0.0 to 1.0)
    #[serde(default = "CompanionIndexConfig::default_bloom_fpr")]
    pub bloom_fpr: f64,
}

impl CompanionIndexConfig {
    fn default_tantivy_enabled() -> bool {
        true
    }

    fn default_bloom_enabled() -> bool {
        true
    }

    fn default_trigram_enabled() -> bool {
        true
    }

    fn default_cardinality_threshold() -> usize {
        10_000
    }

    fn default_bloom_fpr() -> f64 {
        0.01
    }
}

impl Default for CompanionIndexConfig {
    fn default() -> Self {
        Self {
            tantivy_enabled: Self::default_tantivy_enabled(),
            bloom_enabled: Self::default_bloom_enabled(),
            trigram_enabled: Self::default_trigram_enabled(),
            cardinality_threshold: Self::default_cardinality_threshold(),
            bloom_fpr: Self::default_bloom_fpr(),
        }
    }
}
