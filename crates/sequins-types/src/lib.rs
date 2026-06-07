//! sequins-types — Core domain types for Sequins
//!
//! Provides:
//! - Data models: Span, LogEntry, Metric, Profile, etc.
//! - Time types: Timestamp, Duration, TimeRange, TimeWindow

/// Health metric generation
pub mod health;
/// Internal macros for generating boilerplate (e.g. `define_uuid_id!`)
pub(crate) mod macros;
/// Metric aggregation and grouping utilities
pub mod metric_grouping;
/// Data models for traces, logs, metrics, and profiles
pub mod models;
/// Wall-clock time provider (injectable for deterministic testing)
pub mod time_provider;

// Re-export core model types
pub use models::time::{Duration, TimeRange, TimeWindow, Timestamp};
pub use models::{
    HistogramDataPoint, LogEntry, MaintenanceStats, Metric, MetricDataPoint, Profile,
    RetentionPolicy, Span, SpanId, StorageStats, TraceId,
};
pub use time_provider::{MockNowTime, NowTime, SystemNowTime};
