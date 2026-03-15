//! C-compatible management types

use sequins_types::models::{Duration, MaintenanceStats, RetentionPolicy, StorageStats};

/// C-compatible retention policy configuration
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CRetentionPolicy {
    /// How long to keep span data (seconds)
    pub spans_retention_secs: i64,
    /// How long to keep log data (seconds)
    pub logs_retention_secs: i64,
    /// How long to keep metric data (seconds)
    pub metrics_retention_secs: i64,
    /// How long to keep profile data (seconds)
    pub profiles_retention_secs: i64,
}

impl From<CRetentionPolicy> for RetentionPolicy {
    fn from(c_policy: CRetentionPolicy) -> Self {
        RetentionPolicy {
            spans_retention: Duration::from_secs(c_policy.spans_retention_secs),
            logs_retention: Duration::from_secs(c_policy.logs_retention_secs),
            metrics_retention: Duration::from_secs(c_policy.metrics_retention_secs),
            profiles_retention: Duration::from_secs(c_policy.profiles_retention_secs),
        }
    }
}

impl From<RetentionPolicy> for CRetentionPolicy {
    fn from(policy: RetentionPolicy) -> Self {
        CRetentionPolicy {
            spans_retention_secs: policy.spans_retention.as_secs(),
            logs_retention_secs: policy.logs_retention.as_secs(),
            metrics_retention_secs: policy.metrics_retention.as_secs(),
            profiles_retention_secs: policy.profiles_retention.as_secs(),
        }
    }
}

/// C-compatible storage statistics
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CStorageStats {
    /// Number of spans in storage
    pub span_count: usize,
    /// Number of logs in storage
    pub log_count: usize,
    /// Number of metrics in storage
    pub metric_count: usize,
    /// Number of profiles in storage
    pub profile_count: usize,
}

impl From<StorageStats> for CStorageStats {
    fn from(stats: StorageStats) -> Self {
        CStorageStats {
            span_count: stats.span_count,
            log_count: stats.log_count,
            metric_count: stats.metric_count,
            profile_count: stats.profile_count,
        }
    }
}

/// C-compatible maintenance statistics
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMaintenanceStats {
    /// Number of entries evicted from hot tier
    pub entries_evicted: usize,
    /// Number of batches flushed to cold tier
    pub batches_flushed: usize,
}

impl From<MaintenanceStats> for CMaintenanceStats {
    fn from(stats: MaintenanceStats) -> Self {
        CMaintenanceStats {
            entries_evicted: stats.entries_evicted,
            batches_flushed: stats.batches_flushed,
        }
    }
}

/// OTLP server configuration
#[repr(C)]
#[derive(Copy, Clone)]
pub struct COtlpServerConfig {
    /// gRPC port (0 = disabled, default 4317)
    pub grpc_port: u16,
    /// HTTP port (0 = disabled, default 4318)
    pub http_port: u16,
}
