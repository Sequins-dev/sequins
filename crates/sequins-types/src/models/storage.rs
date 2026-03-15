use crate::models::Duration;
use serde::{Deserialize, Serialize};

/// Retention policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// How long to keep span data
    pub spans_retention: Duration,

    /// How long to keep log data
    pub logs_retention: Duration,

    /// How long to keep metric data
    pub metrics_retention: Duration,

    /// How long to keep profile data
    pub profiles_retention: Duration,
}

/// Storage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Number of spans in storage
    pub span_count: usize,

    /// Number of logs in storage
    pub log_count: usize,

    /// Number of metrics in storage
    pub metric_count: usize,

    /// Number of profiles in storage
    pub profile_count: usize,
}

/// Maintenance operation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceStats {
    /// Number of entries evicted from hot tier
    pub entries_evicted: usize,

    /// Number of batches flushed to cold tier
    pub batches_flushed: usize,
}
