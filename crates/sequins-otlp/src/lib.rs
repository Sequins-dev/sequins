//! `sequins-otlp` — Direct OTLP protobuf → Arrow `RecordBatch` conversion for Sequins.
//!
//! This crate provides zero-copy, domain-type-free conversion from OTLP protobuf messages
//! to Arrow `RecordBatch`es, following Sequins' Arrow-native pipeline design principle.
//!
//! # Modules
//! - `overflow_map` — CBOR-encoded `_overflow_attrs` column builder and DataFusion UDFs
//! - `helpers` — Resource/scope conversion and service name extraction utilities
//! - `spans` — `otlp_spans_to_batch`
//! - `logs` — `otlp_logs_to_batch`
//! - `span_events` — `otlp_span_events_to_batch`
//! - `span_links` — `otlp_span_links_to_batch`
//! - `metrics` — `otlp_metrics_to_batch`, `otlp_datapoints_to_batch`,
//!   `otlp_histograms_to_batch`, `otlp_exp_histograms_to_batch`
//! - `profiles` — `otlp_profiles_to_batches` (returns `ProfileBatches`)

pub mod helpers;
pub mod logs;
pub mod metrics;
pub mod overflow_map;
pub mod profiles;
pub mod span_events;
pub mod span_links;
pub mod spans;

// Re-export the primary conversion functions for ergonomic use
pub use helpers::{
    convert_attributes, convert_otlp_resource, convert_otlp_scope, convert_resource_attributes,
    extract_service_name,
};
pub use logs::otlp_logs_to_batch;
pub use metrics::{
    otlp_datapoints_to_batch, otlp_exp_histograms_to_batch, otlp_histograms_to_batch,
    otlp_metric_type, otlp_metrics_to_batch,
};
pub use overflow_map::{build_overflow_column, build_overflow_column_domain};
pub use profiles::{otlp_profiles_to_batches, ProfileBatches};
pub use span_events::otlp_span_events_to_batch;
pub use span_links::otlp_span_links_to_batch;
pub use spans::otlp_spans_to_batch;

// DataFusion UDF registration (requires the "datafusion" feature)
#[cfg(feature = "datafusion")]
pub use overflow_map::{
    make_overflow_get_bool_udf, make_overflow_get_f64_udf, make_overflow_get_i64_udf,
    make_overflow_get_str_udf, register_overflow_udfs,
};
