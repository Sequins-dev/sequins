//! sequins-arrow-schema — Arrow schemas, signal types, and schema catalog for Sequins
//!
//! This crate is dependency-free relative to other Sequins crates. It can be
//! used by any layer that needs to know about schema definitions without pulling
//! in domain models, traits, OTLP proto, pprof, or DataFusion.

/// Apache Arrow schema definitions for telemetry data
pub mod arrow_schema;
/// Helpers for mapping OTLP attribute keys to promoted Arrow column names
pub mod column_names;
/// Vortex extension data types for semantic enum typing
pub mod ext_dtypes;
/// Central registry of promoted attribute columns (semconv + user config)
pub mod schema_catalog;
/// Signal type enum mapping signal names to Arrow schemas
pub mod signal_type;

// Re-export the most commonly used types at crate root for ergonomics.
pub use schema_catalog::{
    AttributeValueType, EncodingHint, PromotedAttribute, SchemaCatalog, SEMCONV_ATTRIBUTES,
};
pub use signal_type::SignalType;
