use sequins_types::Duration;
use serde::{Deserialize, Serialize};

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

impl Default for HotTierConfig {
    fn default() -> Self {
        Self {
            max_age: Self::default_max_age(),
            max_entries: Self::default_max_entries(),
        }
    }
}
