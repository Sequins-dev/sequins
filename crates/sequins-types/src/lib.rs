//! sequins-types — Core domain types and traits for Sequins
//!
//! Provides:
//! - Data models: Span, LogEntry, Metric, Profile, etc.
//! - Core traits: OtlpIngest, ManagementApi
//! - Arrow schema definitions
//! - Time types: Timestamp, Duration, TimeRange, TimeWindow
//! - Profile parsing utilities

/// Apache Arrow schema definitions for telemetry data
pub mod arrow_schema;
/// Helpers for mapping OTLP attribute keys to promoted Arrow column names
pub mod column_names;
/// Error types and result aliases
pub mod error;
/// Vortex extension data types for semantic enum typing
pub mod ext_dtypes;
/// Health metric generation
pub mod health;
/// OTLP ingestion trait
pub mod ingest;
/// Management API trait
pub mod management;
/// Metric aggregation and grouping utilities
pub mod metric_grouping;
/// Data models for traces, logs, metrics, and profiles
pub mod models;
/// pprof binary format parser
pub mod pprof_parser;
/// Central registry of promoted attribute columns (semconv + user config)
pub mod schema_catalog;
/// Signal type enum mapping signal names to Arrow schemas
pub mod signal_type;
/// Wall-clock time provider (injectable for deterministic testing)
pub mod time_provider;

// Re-export core types
pub use error::{Error, Result};
pub use ingest::OtlpIngest;
pub use management::ManagementApi;
pub use models::time::{Duration, TimeRange, TimeWindow, Timestamp};
pub use models::{
    HistogramDataPoint, LogEntry, MaintenanceStats, Metric, MetricDataPoint, Profile,
    RetentionPolicy, Span, SpanId, StorageStats, TraceId,
};
pub use schema_catalog::{
    AttributeValueType, EncodingHint, PromotedAttribute, SchemaCatalog, SEMCONV_ATTRIBUTES,
};
pub use signal_type::SignalType;
pub use time_provider::{MockNowTime, NowTime, SystemNowTime};
