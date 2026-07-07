//! `sequins-cold-tier` — Vortex columnar cold tier storage for Sequins.

pub mod config;
pub mod error;

// Core modules
pub mod helpers;
pub mod partition;
pub mod record_batch;

// Write modules
pub mod write_logs;
pub mod write_metrics;
pub mod write_misc;
pub mod write_profiles;
pub mod write_spans;

// Data modules
pub mod cold_tier;
pub mod compact;
pub mod query;
pub mod rollups;

// series_index, companion indexes, and indexed layout now live in dedicated crates.
// Re-export under the same module names so internal paths (e.g. `crate::index::*`) continue to resolve.
pub use sequins_companion_index as index;
pub use sequins_series_index as series_index;
pub use sequins_vortex_indexed_layout as indexed_layout;

// Test helpers
#[cfg(test)]
pub mod test_helpers;

// Re-export primary types
pub use cold_tier::ColdTier;
pub use config::{ColdTierConfig, CompanionIndexConfig};
pub use error::{Error, Result};
pub use helpers::store_base_path;
pub use rollups::{
    DurationStats, MetricExemplar, MetricRollup, PercentileStats, ProfileRollup, RollupTier,
    SpanRollup,
};
pub use series_index::{SeriesId, SeriesIndex, SeriesMetadata};
