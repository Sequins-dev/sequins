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
pub mod query;
pub mod rollups;
pub mod series_index;

// Companion indexes
pub mod index;

// Custom Vortex layout embedding companion indexes
pub mod indexed_layout;

// Test helpers
#[cfg(test)]
pub mod test_helpers;

// Re-export primary types
pub use cold_tier::ColdTier;
pub use config::{ColdTierConfig, CompanionIndexConfig};
pub use error::{Error, Result};
pub use rollups::{
    DurationStats, MetricExemplar, MetricRollup, PercentileStats, ProfileRollup, RollupTier,
    SpanRollup,
};
pub use series_index::{SeriesId, SeriesIndex, SeriesMetadata};
