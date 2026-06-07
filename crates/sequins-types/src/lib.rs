//! sequins-types — Core domain types and traits for Sequins
//!
//! Provides:
//! - Data models: Span, LogEntry, Metric, Profile, etc.
//! - Core traits: OtlpIngest, ManagementApi
//! - Arrow schema definitions (re-exported from sequins-arrow-schema)
//! - Time types: Timestamp, Duration, TimeRange, TimeWindow

// Arrow schema modules — owned by sequins-arrow-schema, re-exported here for back-compat.
pub use sequins_arrow_schema::arrow_schema;
pub use sequins_arrow_schema::column_names;
pub use sequins_arrow_schema::ext_dtypes;
pub use sequins_arrow_schema::schema_catalog;
pub use sequins_arrow_schema::signal_type;

// error is now canonical in sequins-traits; re-exported here for back-compat.
pub use sequins_traits::error;
/// Health metric generation
pub mod health;
/// OTLP ingestion trait (thin shim re-exporting from sequins-traits)
pub mod ingest;
/// Internal macros for generating boilerplate (e.g. `define_uuid_id!`)
pub(crate) mod macros;
/// Management API trait
pub mod management;
/// Metric aggregation and grouping utilities
pub mod metric_grouping;
/// Data models for traces, logs, metrics, and profiles
pub mod models;
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
