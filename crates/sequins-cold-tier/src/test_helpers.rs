//! Test helpers for cold tier tests

use crate::config::{ColdTierConfig, CompanionIndexConfig};
use tempfile::TempDir;

pub async fn create_test_cold_tier() -> (super::cold_tier::ColdTier, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = ColdTierConfig {
        uri: format!("file://{}", temp_dir.path().display()),
        row_block_size: 1000,
        compact_encodings: true,
        companion_index: CompanionIndexConfig {
            tantivy_enabled: false,
            bloom_enabled: false,
            trigram_enabled: false,
            cardinality_threshold: 100,
            bloom_fpr: 0.01,
        },
        index_path: None,
        max_attribute_columns: 256,
    };

    let cold_tier = super::cold_tier::ColdTier::new(config).unwrap();
    (cold_tier, temp_dir)
}
