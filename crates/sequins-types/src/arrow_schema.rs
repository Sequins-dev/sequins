use crate::schema_catalog::{AttributeValueType, SchemaCatalog};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use once_cell::sync::Lazy;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Overflow map field
// ---------------------------------------------------------------------------

/// The Arrow `Map<Utf8, LargeBinary>` field used for overflow attributes.
///
/// All OTLP attributes that are NOT in the `SchemaCatalog`'s promoted set
/// are CBOR-encoded and stored as entries in this map column.
pub fn overflow_attrs_field() -> Field {
    // Map<Utf8, LargeBinary> — keys are attribute names, values are CBOR bytes
    let entries_type = DataType::Struct(Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::LargeBinary, true),
    ]));
    Field::new(
        "_overflow_attrs",
        DataType::Map(Arc::new(Field::new("entries", entries_type, false)), false),
        true, // nullable — empty/absent when all attributes are promoted
    )
}

/// Build Arrow `Field`s for all promoted attributes in the catalog.
///
/// Each promoted attribute becomes a nullable column. The column is null
/// for rows where that attribute is not present.
pub fn catalog_promoted_fields(catalog: &SchemaCatalog) -> Vec<Field> {
    catalog
        .promoted_columns()
        .map(|attr| {
            let dtype = match attr.value_type {
                AttributeValueType::String => DataType::Utf8View,
                AttributeValueType::Int64 => DataType::Int64,
                AttributeValueType::Float64 => DataType::Float64,
                AttributeValueType::Boolean => DataType::Boolean,
            };
            Field::new(attr.column_name, dtype, true) // always nullable
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Default catalog (lazily initialised)
// ---------------------------------------------------------------------------

static DEFAULT_CATALOG: Lazy<SchemaCatalog> = Lazy::new(SchemaCatalog::default_catalog);

/// Return a reference to the default (semconv-only) schema catalog.
///
/// Backed by a `Lazy<SchemaCatalog>` static, so the catalog is initialised
/// at most once per process.  Callers that need a per-request catalog can
/// build their own with `SchemaCatalog::default_catalog()`.
pub fn default_schema_catalog() -> &'static SchemaCatalog {
    &DEFAULT_CATALOG
}

/// Core span fields that are always present in every Vortex file
///
/// These fields represent the fixed schema that all span files share.
/// Dynamic attribute columns can be added per file based on actual data.
///
/// Note: IDs are stored as hex strings instead of FixedSizeBinary for Vortex compatibility.
pub fn core_span_fields() -> Vec<Field> {
    vec![
        // IDs (stored as hex strings for Vortex compatibility)
        Field::new("trace_id", DataType::Utf8View, false),
        Field::new("span_id", DataType::Utf8View, false),
        Field::new(
            "parent_span_id",
            DataType::Utf8View,
            true, // nullable for root spans
        ),
        // Metadata
        Field::new("name", DataType::Utf8View, false),
        Field::new("kind", DataType::UInt8, false),
        Field::new("status", DataType::UInt8, false),
        // Timing (nanosecond precision, OTLP spec names)
        Field::new(
            "start_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("duration_ns", DataType::Int64, false),
        // Resource ID (content-addressed FNV-1a hash)
        Field::new("resource_id", DataType::UInt32, false),
        // Scope ID (content-addressed instrumentation scope hash)
        Field::new("scope_id", DataType::UInt32, false),
    ]
}

/// Arrow schema for trace spans with promoted attribute columns and overflow map.
///
/// Uses the default built-in semconv catalog.  Each row represents one span;
/// promoted attributes are first-class nullable columns and remaining attributes
/// are CBOR-encoded in `_overflow_attrs`.
///
/// Schema layout:
///   core fields (11) + promoted columns (N) + `_overflow_attrs: Map<Utf8, LargeBinary>`
pub fn span_schema() -> Arc<Schema> {
    span_schema_with_catalog(&DEFAULT_CATALOG)
}

/// Arrow schema for spans using a specific `SchemaCatalog`.
///
/// Useful when a non-default catalog is in use (e.g. with user-configured
/// promoted attributes).
pub fn span_schema_with_catalog(catalog: &SchemaCatalog) -> Arc<Schema> {
    let mut fields = core_span_fields();
    fields.extend(catalog_promoted_fields(catalog));
    fields.push(overflow_attrs_field());
    Arc::new(Schema::new(fields))
}

/// Core log fields that are always present in every Vortex file
///
/// These fields represent the fixed schema that all log files share.
/// Attribute columns are added by `log_schema_with_catalog()` (promoted) and
/// `_overflow_attrs` (remainder in CBOR-encoded Map).
///
/// Note: IDs are stored as hex strings instead of FixedSizeBinary for Vortex compatibility.
pub fn core_log_fields() -> Vec<Field> {
    vec![
        // ID (stored as hex string for Vortex compatibility)
        Field::new("log_id", DataType::Utf8View, false),
        // Timing (OTLP spec names)
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Metadata
        Field::new("service_name", DataType::Utf8View, false),
        Field::new("severity_text", DataType::Utf8View, false),
        Field::new("severity_number", DataType::UInt8, false),
        Field::new("body", DataType::Utf8View, false),
        // Trace context (optional, stored as hex strings for Vortex compatibility)
        Field::new("trace_id", DataType::Utf8View, true),
        Field::new("span_id", DataType::Utf8View, true),
        // Resource ID (replaces full resource JSON)
        Field::new("resource_id", DataType::UInt32, false),
        // Scope ID (content-addressed instrumentation scope hash)
        Field::new("scope_id", DataType::UInt32, false),
    ]
}

/// Core log fields for dynamic cold tier writes (without attribute columns).
///
/// Alias for `core_log_fields()` — previously distinct when the JSON attributes
/// blob existed.  Now both return the same 11-field set; attribute storage
/// is handled by `log_schema_with_catalog()`.
pub fn core_log_fields_dynamic() -> Vec<Field> {
    core_log_fields()
}

/// Arrow schema for log entries with promoted attribute columns and overflow map.
///
/// Uses the default built-in semconv catalog.  Each row represents one log record;
/// promoted attributes are first-class nullable columns and remaining attributes
/// are CBOR-encoded in `_overflow_attrs`.
///
/// Schema layout:
///   core fields (11) + promoted columns (N) + `_overflow_attrs: Map<Utf8, LargeBinary>`
pub fn log_schema() -> Arc<Schema> {
    log_schema_with_catalog(&DEFAULT_CATALOG)
}

/// Arrow schema for logs using a specific `SchemaCatalog`.
pub fn log_schema_with_catalog(catalog: &SchemaCatalog) -> Arc<Schema> {
    let mut fields = core_log_fields();
    fields.extend(catalog_promoted_fields(catalog));
    fields.push(overflow_attrs_field());
    Arc::new(Schema::new(fields))
}

/// Build a dynamic Arrow schema for a batch of log entries
///
/// Returns:
/// - Schema with core fields + extracted field columns
/// - Vector of field names that were extracted (for use during RecordBatch construction)
///
/// Extracted fields are JSON fields from structured log bodies (e.g., message, error, request_id).
pub fn build_log_schema_for_batch(extracted_fields: &[String]) -> (Schema, Vec<String>) {
    let mut fields = core_log_fields();

    // Add a column for each extracted field (use Utf8View for Vortex compatibility)
    for field_name in extracted_fields {
        // Most extracted fields are strings; could be made smarter with type inference
        fields.push(Field::new(field_name, DataType::Utf8View, true));
    }

    (Schema::new(fields), extracted_fields.to_vec())
}

/// Arrow schema for metric metadata stored in Parquet
pub fn metric_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // ID (use Utf8View for Vortex compatibility)
        Field::new("metric_id", DataType::Utf8View, false),
        // Metadata
        Field::new("name", DataType::Utf8View, false),
        Field::new("description", DataType::Utf8View, true),
        Field::new("unit", DataType::Utf8View, true),
        Field::new("metric_type", DataType::Utf8View, false), // gauge, counter, histogram, summary
        Field::new("service_name", DataType::Utf8View, false),
        // Resource and scope IDs (content-addressed hashes)
        Field::new("resource_id", DataType::UInt32, false),
        Field::new("scope_id", DataType::UInt32, false),
    ]))
}

/// Arrow schema for metric data points (gauge/counter) stored in Parquet
pub fn metric_data_point_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // Reference (use Utf8View for Vortex compatibility)
        Field::new("metric_id", DataType::Utf8View, false),
        // Timing (OTLP spec name)
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Value
        Field::new("value", DataType::Float64, false),
        // Attributes stored as JSON
        Field::new("attributes", DataType::Utf8View, true),
    ]))
}

/// Arrow schema for series-indexed metric data points (gauge/counter)
///
/// This is the primary storage format for metric datapoints.
/// - series_id: maps to SeriesIndex for attribute lookups
/// - metric_id: hex UUID identifying the metric
/// - time_unix_nano: nanosecond precision timestamp (OTLP spec name)
/// - value: float64 measurement
pub fn series_data_point_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new("metric_id", DataType::Utf8View, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// Arrow schema for series-indexed histogram data points
///
/// Stores the essential histogram data per row, including explicit_bounds so that
/// each row is self-contained for visualization (heat maps, percentile estimates).
pub fn histogram_series_data_point_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new("metric_id", DataType::Utf8View, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new(
            "bucket_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        ),
        Field::new(
            "explicit_bounds",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
            false,
        ),
    ]))
}

/// Arrow schema for native exponential histogram data points
///
/// Stores the compact OTLP ExponentialHistogram format instead of converting
/// to explicit bounds. The offset + counts representation is inherently
/// more storage-efficient for wide-range distributions.
pub fn exp_histogram_data_point_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new("metric_id", DataType::Utf8View, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("scale", DataType::Int32, false),
        Field::new("zero_count", DataType::UInt64, false),
        Field::new("positive_offset", DataType::Int32, false),
        Field::new(
            "positive_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        ),
        Field::new("negative_offset", DataType::Int32, false),
        Field::new(
            "negative_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        ),
    ]))
}

/// Arrow schema for histogram data points stored in Parquet
pub fn histogram_data_point_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // Reference (use Utf8View for Vortex compatibility)
        Field::new("metric_id", DataType::Utf8View, false),
        // Timing (OTLP spec name)
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Histogram data
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),
        // Buckets stored as lists
        Field::new(
            "bucket_counts",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            false,
        ),
        Field::new(
            "explicit_bounds",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
            false,
        ),
        // Exemplars and attributes stored as JSON (use Utf8View for Vortex compatibility)
        Field::new("exemplars", DataType::Utf8View, true),
        Field::new("attributes", DataType::Utf8View, true),
    ]))
}

/// Arrow schema for continuous profiling data stored in Parquet
pub fn profile_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // ID (use Utf8View for Vortex compatibility)
        Field::new("profile_id", DataType::Utf8View, false),
        // Timing (OTLP spec name)
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Metadata
        Field::new("service_name", DataType::Utf8View, false),
        // Resource and scope IDs (content-addressed hashes)
        Field::new("resource_id", DataType::UInt32, false),
        Field::new("scope_id", DataType::UInt32, false),
        Field::new("profile_type", DataType::Utf8View, false), // cpu, memory, goroutine, other
        Field::new("sample_type", DataType::Utf8View, false),
        Field::new("sample_unit", DataType::Utf8View, false),
        // Trace context (optional)
        Field::new("trace_id", DataType::Utf8View, true),
        // Profile data (pprof format, stored as binary)
        Field::new("data", DataType::Binary, false),
    ]))
}

/// Arrow schema for decomposed profile samples (for query)
///
/// This schema is used for both hot and cold tier queries to ensure compatibility.
pub fn profile_samples_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("profile_id", DataType::Utf8View, false),
        Field::new("stack_id", DataType::UInt64, false),
        Field::new("service", DataType::Utf8View, false),
        Field::new("time_unix_nano", DataType::Int64, false),
        Field::new("resource_id", DataType::UInt32, false),
        Field::new("scope_id", DataType::UInt32, false),
        Field::new("value_type", DataType::Utf8View, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Arrow schema for profile frames (deduplicated call sites)
pub fn profile_frames_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("frame_id", DataType::UInt64, false),
        Field::new("function_name", DataType::Utf8View, false),
        Field::new("system_name", DataType::Utf8View, true),
        Field::new("filename", DataType::Utf8View, true),
        Field::new("line", DataType::Int64, true),
        Field::new("column", DataType::Int64, true),
        Field::new("mapping_id", DataType::UInt64, true),
        Field::new("inline", DataType::Boolean, false),
    ]))
}

/// Arrow schema for profile stacks (junction table: one row per frame per stack)
///
/// Normalized from the old `[stack_id, frame_ids: List<UInt64>]` format.
/// `position` preserves leaf-first ordering from pprof/OTLP conventions.
pub fn profile_stacks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("stack_id", DataType::UInt64, false),
        Field::new("frame_id", DataType::UInt64, false),
        Field::new("position", DataType::UInt32, false),
    ]))
}

/// Arrow schema for profile mappings (deduplicated binary/library info)
pub fn profile_mappings_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("mapping_id", DataType::UInt64, false),
        Field::new("filename", DataType::Utf8View, false),
        Field::new("build_id", DataType::Utf8View, true),
    ]))
}

/// Arrow schema for resources (deduplicated entities producing telemetry)
pub fn resource_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("resource_id", DataType::UInt32, false),
        // service.name extracted from attributes for fast service discovery
        Field::new("service_name", DataType::Utf8View, false),
        // Store attributes as JSON string for simplicity
        Field::new("attributes", DataType::Utf8View, false),
    ]))
}

/// Arrow schema for instrumentation scopes (deduplicated instrumentation libraries)
pub fn scope_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("scope_id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8View, false),
        Field::new("version", DataType::Utf8View, false),
        // Store attributes as JSON string for simplicity
        Field::new("attributes", DataType::Utf8View, false),
    ]))
}

/// Arrow schema for span events (events embedded in spans, stored as a separate table).
///
/// Uses the overflow map for event attributes.
pub fn span_events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8View, false),
        Field::new("span_id", DataType::Utf8View, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8View, false),
        overflow_attrs_field(),
    ]))
}

/// Arrow schema for span links (links between spans across traces).
///
/// Uses the overflow map for link attributes.
pub fn span_links_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // The span that has this link
        Field::new("source_trace_id", DataType::Utf8View, false),
        Field::new("source_span_id", DataType::Utf8View, false),
        // The linked-to span
        Field::new("target_trace_id", DataType::Utf8View, false),
        Field::new("target_span_id", DataType::Utf8View, false),
        // Optional trace state and attributes
        Field::new("trace_state", DataType::Utf8View, true),
        overflow_attrs_field(),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The number of built-in semconv promoted columns.
    fn promoted_count() -> usize {
        DEFAULT_CATALOG.len()
    }

    #[test]
    fn test_span_schema_fields() {
        let schema = span_schema();
        // 11 core + N promoted + 1 overflow
        let expected = 11 + promoted_count() + 1;
        assert_eq!(
            schema.fields().len(),
            expected,
            "span_schema should have 11 core + {} promoted + 1 overflow = {} fields",
            promoted_count(),
            expected
        );

        // Verify key fields exist and have correct types (Utf8View for Vortex compatibility)
        let trace_id_field = schema.field_with_name("trace_id").unwrap();
        assert_eq!(trace_id_field.data_type(), &DataType::Utf8View);
        assert!(!trace_id_field.is_nullable());

        let span_id_field = schema.field_with_name("span_id").unwrap();
        assert_eq!(span_id_field.data_type(), &DataType::Utf8View);

        let parent_field = schema.field_with_name("parent_span_id").unwrap();
        assert!(parent_field.is_nullable()); // Root spans have no parent

        let start_time_field = schema.field_with_name("start_time_unix_nano").unwrap();
        assert_eq!(
            start_time_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        // Verify service_name and resource columns are not in core
        assert!(schema.field_with_name("resource").is_err());
        // Verify events column is removed (moved to span_events table)
        assert!(schema.field_with_name("events").is_err());

        // Verify overflow column exists
        assert!(schema.field_with_name("_overflow_attrs").is_ok());
        // Verify JSON attributes column is removed
        assert!(schema.field_with_name("attributes").is_err());
    }

    #[test]
    fn test_core_span_fields() {
        let fields = core_span_fields();
        assert_eq!(fields.len(), 11); // trace_id, span_id, parent_span_id, name, kind, status, start, end, duration, resource_id, scope_id

        // Verify resource_id field is included
        let resource_id_field = fields.iter().find(|f| f.name() == "resource_id").unwrap();
        assert_eq!(resource_id_field.data_type(), &DataType::UInt32);
        // Verify scope_id field is included
        let scope_id_field = fields.iter().find(|f| f.name() == "scope_id").unwrap();
        assert_eq!(scope_id_field.data_type(), &DataType::UInt32);
        assert!(!scope_id_field.is_nullable());
        // Verify removed fields are gone
        assert!(!fields.iter().any(|f| f.name() == "service_name"));
        assert!(!fields.iter().any(|f| f.name() == "resource"));
        assert!(!fields.iter().any(|f| f.name() == "events"));
    }

    #[test]
    fn test_span_events_schema_fields() {
        let schema = span_events_schema();
        // trace_id, span_id, time_unix_nano, name, _overflow_attrs
        assert_eq!(schema.fields().len(), 5);

        let trace_id_field = schema.field_with_name("trace_id").unwrap();
        assert_eq!(trace_id_field.data_type(), &DataType::Utf8View);
        assert!(!trace_id_field.is_nullable());

        let time_field = schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(
            time_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let overflow_field = schema.field_with_name("_overflow_attrs").unwrap();
        assert!(overflow_field.is_nullable());
    }

    #[test]
    fn test_core_log_fields() {
        let fields = core_log_fields();
        // log_id, time, observed_time, service_name, severity_text, severity_number, body, trace_id, span_id, resource_id, scope_id
        assert_eq!(
            fields.len(),
            11,
            "core_log_fields should have 11 fields (no JSON attributes column)"
        );

        // Verify resource_id field exists
        let resource_id_field = fields.iter().find(|f| f.name() == "resource_id").unwrap();
        assert_eq!(resource_id_field.data_type(), &DataType::UInt32);
        assert!(!resource_id_field.is_nullable());
        // Verify scope_id field exists
        let scope_id_field = fields.iter().find(|f| f.name() == "scope_id").unwrap();
        assert_eq!(scope_id_field.data_type(), &DataType::UInt32);
        assert!(!scope_id_field.is_nullable());
        // Verify no JSON attributes column
        assert!(!fields.iter().any(|f| f.name() == "attributes"));
    }

    #[test]
    fn test_log_schema_fields() {
        let schema = log_schema();
        // 11 core + N promoted + 1 overflow
        let expected = 11 + promoted_count() + 1;
        assert_eq!(
            schema.fields().len(),
            expected,
            "log_schema should have 11 core + {} promoted + 1 overflow = {} fields",
            promoted_count(),
            expected
        );

        let log_id_field = schema.field_with_name("log_id").unwrap();
        assert_eq!(log_id_field.data_type(), &DataType::Utf8View);

        let severity_field = schema.field_with_name("severity_text").unwrap();
        assert_eq!(severity_field.data_type(), &DataType::Utf8View);

        let trace_id_field = schema.field_with_name("trace_id").unwrap();
        assert!(trace_id_field.is_nullable()); // Logs may not have trace context

        // Verify resource column is gone (was in legacy schema)
        assert!(schema.field_with_name("resource").is_err());

        // Verify scope_id is present
        let scope_id_field = schema.field_with_name("scope_id").unwrap();
        assert_eq!(scope_id_field.data_type(), &DataType::UInt32);
        assert!(!scope_id_field.is_nullable());

        // Verify overflow column exists and JSON attributes is gone
        assert!(schema.field_with_name("_overflow_attrs").is_ok());
        assert!(schema.field_with_name("attributes").is_err());
    }

    #[test]
    fn test_build_log_schema_for_batch_no_extracted_fields() {
        let extracted_fields = vec![];
        let (schema, fields) = build_log_schema_for_batch(&extracted_fields);

        // Should have only core fields (11 — no JSON attributes blob)
        assert_eq!(schema.fields().len(), 11, "core_log_fields has 11 fields");
        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn test_build_log_schema_for_batch_with_extracted_fields() {
        let extracted_fields = vec![
            "message".to_string(),
            "error".to_string(),
            "request_id".to_string(),
        ];
        let (schema, fields) = build_log_schema_for_batch(&extracted_fields);

        // Core fields (11) + 3 extracted fields
        assert_eq!(schema.fields().len(), 14);
        assert_eq!(fields.len(), 3);

        // Verify extracted field columns exist
        let message_field = schema
            .field_with_name("message")
            .expect("message column should exist");
        assert_eq!(message_field.data_type(), &DataType::Utf8View);
        assert!(message_field.is_nullable());

        let error_field = schema
            .field_with_name("error")
            .expect("error column should exist");
        assert_eq!(error_field.data_type(), &DataType::Utf8View);
        assert!(error_field.is_nullable());
    }

    #[test]
    fn test_metric_schema_fields() {
        let schema = metric_schema();
        assert_eq!(schema.fields().len(), 8); // 6 original + resource_id + scope_id

        let metric_id_field = schema.field_with_name("metric_id").unwrap();
        assert_eq!(metric_id_field.data_type(), &DataType::Utf8View);

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8View);
        assert!(!name_field.is_nullable());

        let resource_id_field = schema.field_with_name("resource_id").unwrap();
        assert_eq!(resource_id_field.data_type(), &DataType::UInt32);
        assert!(!resource_id_field.is_nullable());

        let scope_id_field = schema.field_with_name("scope_id").unwrap();
        assert_eq!(scope_id_field.data_type(), &DataType::UInt32);
        assert!(!scope_id_field.is_nullable());
    }

    #[test]
    fn test_metric_data_point_schema_fields() {
        let schema = metric_data_point_schema();
        assert_eq!(schema.fields().len(), 4);

        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(value_field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_histogram_data_point_schema_fields() {
        let schema = histogram_data_point_schema();
        assert_eq!(schema.fields().len(), 8);

        let count_field = schema.field_with_name("count").unwrap();
        assert_eq!(count_field.data_type(), &DataType::UInt64);

        let bucket_counts_field = schema.field_with_name("bucket_counts").unwrap();
        match bucket_counts_field.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::UInt64);
            }
            _ => panic!("Expected List type for bucket_counts"),
        }
    }

    #[test]
    fn test_profile_schema_fields() {
        let schema = profile_schema();
        assert_eq!(schema.fields().len(), 10); // 8 original + resource_id + scope_id

        let profile_id_field = schema.field_with_name("profile_id").unwrap();
        assert_eq!(profile_id_field.data_type(), &DataType::Utf8View);

        let data_field = schema.field_with_name("data").unwrap();
        assert_eq!(data_field.data_type(), &DataType::Binary);

        let resource_id_field = schema.field_with_name("resource_id").unwrap();
        assert_eq!(resource_id_field.data_type(), &DataType::UInt32);
        assert!(!resource_id_field.is_nullable());

        let scope_id_field = schema.field_with_name("scope_id").unwrap();
        assert_eq!(scope_id_field.data_type(), &DataType::UInt32);
        assert!(!scope_id_field.is_nullable());
    }

    #[test]
    fn test_all_schemas_are_valid() {
        // Verify all schemas can be created without panicking
        let _ = span_schema();
        let _ = span_events_schema();
        let _ = log_schema();
        let _ = metric_schema();
        let _ = metric_data_point_schema();
        let _ = series_data_point_schema();
        let _ = histogram_series_data_point_schema();
        let _ = exp_histogram_data_point_schema();
        let _ = histogram_data_point_schema();
        let _ = profile_schema();
    }

    #[test]
    fn test_series_data_point_schema_fields() {
        let schema = series_data_point_schema();
        assert_eq!(schema.fields().len(), 4);

        let series_id_field = schema.field_with_name("series_id").unwrap();
        assert_eq!(series_id_field.data_type(), &DataType::UInt64);
        assert!(!series_id_field.is_nullable());

        let metric_id_field = schema.field_with_name("metric_id").unwrap();
        assert_eq!(metric_id_field.data_type(), &DataType::Utf8View);

        let ts_field = schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(value_field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_span_kind_and_status_are_uint8() {
        let fields = core_span_fields();

        let kind_field = fields.iter().find(|f| f.name() == "kind").unwrap();
        assert_eq!(kind_field.data_type(), &DataType::UInt8);

        let status_field = fields.iter().find(|f| f.name() == "status").unwrap();
        assert_eq!(status_field.data_type(), &DataType::UInt8);
    }

    #[test]
    fn test_exp_histogram_schema_fields() {
        let schema = exp_histogram_data_point_schema();
        assert_eq!(schema.fields().len(), 11);

        let scale_field = schema.field_with_name("scale").unwrap();
        assert_eq!(scale_field.data_type(), &DataType::Int32);

        let pos_counts_field = schema.field_with_name("positive_counts").unwrap();
        match pos_counts_field.data_type() {
            DataType::List(inner) => assert_eq!(inner.data_type(), &DataType::UInt64),
            _ => panic!("Expected List for positive_counts"),
        }
    }

    #[test]
    fn test_profile_samples_schema_fields() {
        let schema = profile_samples_schema();
        assert_eq!(schema.fields().len(), 8); // 7 original + scope_id

        let profile_id_field = schema.field_with_name("profile_id").unwrap();
        assert_eq!(profile_id_field.data_type(), &DataType::Utf8View);
        assert!(!profile_id_field.is_nullable());

        let stack_id_field = schema.field_with_name("stack_id").unwrap();
        assert_eq!(stack_id_field.data_type(), &DataType::UInt64);
        assert!(!stack_id_field.is_nullable());

        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(value_field.data_type(), &DataType::Int64);
        assert!(!value_field.is_nullable());

        let timestamp_field = schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(timestamp_field.data_type(), &DataType::Int64);

        let scope_id_field = schema.field_with_name("scope_id").unwrap();
        assert_eq!(scope_id_field.data_type(), &DataType::UInt32);
        assert!(!scope_id_field.is_nullable());
    }

    #[test]
    fn test_profile_frames_schema_fields() {
        let schema = profile_frames_schema();
        assert_eq!(schema.fields().len(), 8);

        let frame_id_field = schema.field_with_name("frame_id").unwrap();
        assert_eq!(frame_id_field.data_type(), &DataType::UInt64);
        assert!(!frame_id_field.is_nullable());

        let function_name_field = schema.field_with_name("function_name").unwrap();
        assert_eq!(function_name_field.data_type(), &DataType::Utf8View);
        assert!(!function_name_field.is_nullable());

        let filename_field = schema.field_with_name("filename").unwrap();
        assert_eq!(filename_field.data_type(), &DataType::Utf8View);
        assert!(filename_field.is_nullable()); // Filename is optional

        let line_field = schema.field_with_name("line").unwrap();
        assert_eq!(line_field.data_type(), &DataType::Int64);
        assert!(line_field.is_nullable());

        let inline_field = schema.field_with_name("inline").unwrap();
        assert_eq!(inline_field.data_type(), &DataType::Boolean);
        assert!(!inline_field.is_nullable());
    }

    #[test]
    fn test_profile_stacks_schema_fields() {
        // Junction table: one row per (stack_id, frame_id) pair with position
        let schema = profile_stacks_schema();
        assert_eq!(schema.fields().len(), 3);

        let stack_id_field = schema.field_with_name("stack_id").unwrap();
        assert_eq!(stack_id_field.data_type(), &DataType::UInt64);
        assert!(!stack_id_field.is_nullable());

        let frame_id_field = schema.field_with_name("frame_id").unwrap();
        assert_eq!(frame_id_field.data_type(), &DataType::UInt64);
        assert!(!frame_id_field.is_nullable());

        let position_field = schema.field_with_name("position").unwrap();
        assert_eq!(position_field.data_type(), &DataType::UInt32);
        assert!(!position_field.is_nullable());
    }

    #[test]
    fn test_profile_mappings_schema_fields() {
        let schema = profile_mappings_schema();
        assert_eq!(schema.fields().len(), 3);

        let mapping_id_field = schema.field_with_name("mapping_id").unwrap();
        assert_eq!(mapping_id_field.data_type(), &DataType::UInt64);
        assert!(!mapping_id_field.is_nullable());

        let filename_field = schema.field_with_name("filename").unwrap();
        assert_eq!(filename_field.data_type(), &DataType::Utf8View);
        assert!(!filename_field.is_nullable());

        let build_id_field = schema.field_with_name("build_id").unwrap();
        assert_eq!(build_id_field.data_type(), &DataType::Utf8View);
        assert!(build_id_field.is_nullable()); // Build ID is optional
    }

    #[test]
    fn test_resource_schema_fields() {
        let schema = resource_schema();
        assert_eq!(schema.fields().len(), 3);

        let resource_id_field = schema.field_with_name("resource_id").unwrap();
        assert_eq!(resource_id_field.data_type(), &DataType::UInt32);
        assert!(!resource_id_field.is_nullable());

        let service_name_field = schema.field_with_name("service_name").unwrap();
        assert_eq!(service_name_field.data_type(), &DataType::Utf8View);
        assert!(!service_name_field.is_nullable());

        let attributes_field = schema.field_with_name("attributes").unwrap();
        assert_eq!(attributes_field.data_type(), &DataType::Utf8View);
        assert!(!attributes_field.is_nullable()); // Attributes JSON is always present (even if empty)
    }

    #[test]
    fn test_scope_schema_fields() {
        let schema = scope_schema();
        assert_eq!(schema.fields().len(), 4);

        let scope_id_field = schema.field_with_name("scope_id").unwrap();
        assert_eq!(scope_id_field.data_type(), &DataType::UInt32);
        assert!(!scope_id_field.is_nullable());

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8View);
        assert!(!name_field.is_nullable());

        let version_field = schema.field_with_name("version").unwrap();
        assert_eq!(version_field.data_type(), &DataType::Utf8View);
        assert!(!version_field.is_nullable());

        let attributes_field = schema.field_with_name("attributes").unwrap();
        assert_eq!(attributes_field.data_type(), &DataType::Utf8View);
        assert!(!attributes_field.is_nullable());
    }

    #[test]
    fn test_span_links_schema_fields() {
        let schema = span_links_schema();
        // source_trace_id, source_span_id, target_trace_id, target_span_id, trace_state, _overflow_attrs
        assert_eq!(schema.fields().len(), 6);

        let source_trace_id_field = schema.field_with_name("source_trace_id").unwrap();
        assert_eq!(source_trace_id_field.data_type(), &DataType::Utf8View);
        assert!(!source_trace_id_field.is_nullable());

        let source_span_id_field = schema.field_with_name("source_span_id").unwrap();
        assert_eq!(source_span_id_field.data_type(), &DataType::Utf8View);
        assert!(!source_span_id_field.is_nullable());

        let target_trace_id_field = schema.field_with_name("target_trace_id").unwrap();
        assert_eq!(target_trace_id_field.data_type(), &DataType::Utf8View);
        assert!(!target_trace_id_field.is_nullable());

        let target_span_id_field = schema.field_with_name("target_span_id").unwrap();
        assert_eq!(target_span_id_field.data_type(), &DataType::Utf8View);
        assert!(!target_span_id_field.is_nullable());

        let trace_state_field = schema.field_with_name("trace_state").unwrap();
        assert_eq!(trace_state_field.data_type(), &DataType::Utf8View);
        assert!(trace_state_field.is_nullable()); // Trace state is optional

        // Attributes now stored in overflow map
        let overflow_field = schema.field_with_name("_overflow_attrs").unwrap();
        assert!(overflow_field.is_nullable());
        assert!(schema.field_with_name("attributes").is_err());
    }

    #[test]
    fn test_timestamp_fields_use_nanoseconds() {
        // Verify that all timestamp fields use nanosecond precision
        let span_schema = span_schema();
        let start_time = span_schema.field_with_name("start_time_unix_nano").unwrap();
        assert_eq!(
            start_time.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let log_schema = log_schema();
        let timestamp = log_schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(
            timestamp.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let metric_schema = metric_data_point_schema();
        let metric_timestamp = metric_schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(
            metric_timestamp.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let histogram_schema = histogram_data_point_schema();
        let histogram_timestamp = histogram_schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(
            histogram_timestamp.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let profile_schema = profile_schema();
        let profile_timestamp = profile_schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(
            profile_timestamp.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_all_id_fields_use_utf8view() {
        // Verify that all ID fields use Utf8View for Vortex compatibility
        let span_schema = span_schema();
        let trace_id = span_schema.field_with_name("trace_id").unwrap();
        assert_eq!(trace_id.data_type(), &DataType::Utf8View);

        let metric_schema = metric_schema();
        let metric_id = metric_schema.field_with_name("metric_id").unwrap();
        assert_eq!(metric_id.data_type(), &DataType::Utf8View);

        let profile_schema = profile_schema();
        let profile_id = profile_schema.field_with_name("profile_id").unwrap();
        assert_eq!(profile_id.data_type(), &DataType::Utf8View);
    }

    #[test]
    fn test_list_field_types() {
        // Verify that list fields have correct inner types
        let histogram_schema = histogram_data_point_schema();

        let bucket_counts = histogram_schema.field_with_name("bucket_counts").unwrap();
        match bucket_counts.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::UInt64);
                assert!(!inner.is_nullable());
            }
            _ => panic!("Expected List type for bucket_counts"),
        }

        let explicit_bounds = histogram_schema.field_with_name("explicit_bounds").unwrap();
        match explicit_bounds.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Float64);
                assert!(!inner.is_nullable());
            }
            _ => panic!("Expected List type for explicit_bounds"),
        }

        let stacks_schema = profile_stacks_schema();
        assert!(stacks_schema.field_with_name("stack_id").is_ok());
        assert!(stacks_schema.field_with_name("frame_id").is_ok());
        assert!(stacks_schema.field_with_name("position").is_ok());
    }
}
