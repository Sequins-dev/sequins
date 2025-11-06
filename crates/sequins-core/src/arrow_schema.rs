use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// Arrow schema for trace spans stored in Parquet
///
/// Each row represents a single span within a trace
pub fn span_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // IDs
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new(
            "parent_span_id",
            DataType::FixedSizeBinary(8),
            true, // nullable for root spans
        ),
        // Metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("operation_name", DataType::Utf8, false),
        Field::new("span_kind", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        // Timing (nanosecond precision)
        Field::new(
            "start_time_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("duration_ns", DataType::Int64, false),
        // Attributes stored as JSON for flexibility (will use Map in future)
        Field::new("attributes", DataType::Utf8, true),
        // Events stored as JSON array
        Field::new("events", DataType::Utf8, true),
    ]))
}

/// Arrow schema for log entries stored in Parquet
pub fn log_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // ID
        Field::new("log_id", DataType::FixedSizeBinary(16), false),
        // Timing
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("severity_number", DataType::UInt8, false),
        Field::new("body", DataType::Utf8, false),
        // Trace context (optional)
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        // Attributes and resource stored as JSON
        Field::new("attributes", DataType::Utf8, true),
        Field::new("resource", DataType::Utf8, true),
    ]))
}

/// Arrow schema for metric metadata stored in Parquet
pub fn metric_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // ID
        Field::new("metric_id", DataType::FixedSizeBinary(16), false),
        // Metadata
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("metric_type", DataType::Utf8, false), // gauge, counter, histogram, summary
        Field::new("service_name", DataType::Utf8, false),
    ]))
}

/// Arrow schema for metric data points (gauge/counter) stored in Parquet
pub fn metric_data_point_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // Reference
        Field::new("metric_id", DataType::FixedSizeBinary(16), false),
        // Timing
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Value
        Field::new("value", DataType::Float64, false),
        // Attributes stored as JSON
        Field::new("attributes", DataType::Utf8, true),
    ]))
}

/// Arrow schema for histogram data points stored in Parquet
pub fn histogram_data_point_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // Reference
        Field::new("metric_id", DataType::FixedSizeBinary(16), false),
        // Timing
        Field::new(
            "timestamp_ns",
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
        // Exemplars and attributes stored as JSON
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
    ]))
}

/// Arrow schema for continuous profiling data stored in Parquet
pub fn profile_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // ID
        Field::new("profile_id", DataType::FixedSizeBinary(16), false),
        // Timing
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("profile_type", DataType::Utf8, false), // cpu, memory, goroutine, other
        Field::new("sample_type", DataType::Utf8, false),
        Field::new("sample_unit", DataType::Utf8, false),
        // Trace context (optional)
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        // Profile data (pprof format, stored as binary)
        Field::new("data", DataType::Binary, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_schema_fields() {
        let schema = span_schema();
        assert_eq!(schema.fields().len(), 12);

        // Verify key fields exist and have correct types
        let trace_id_field = schema.field_with_name("trace_id").unwrap();
        assert_eq!(trace_id_field.data_type(), &DataType::FixedSizeBinary(16));
        assert!(!trace_id_field.is_nullable());

        let span_id_field = schema.field_with_name("span_id").unwrap();
        assert_eq!(span_id_field.data_type(), &DataType::FixedSizeBinary(8));

        let parent_field = schema.field_with_name("parent_span_id").unwrap();
        assert!(parent_field.is_nullable()); // Root spans have no parent

        let start_time_field = schema.field_with_name("start_time_ns").unwrap();
        assert_eq!(
            start_time_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_log_schema_fields() {
        let schema = log_schema();
        assert_eq!(schema.fields().len(), 11);

        let log_id_field = schema.field_with_name("log_id").unwrap();
        assert_eq!(log_id_field.data_type(), &DataType::FixedSizeBinary(16));

        let severity_field = schema.field_with_name("severity").unwrap();
        assert_eq!(severity_field.data_type(), &DataType::Utf8);

        let trace_id_field = schema.field_with_name("trace_id").unwrap();
        assert!(trace_id_field.is_nullable()); // Logs may not have trace context
    }

    #[test]
    fn test_metric_schema_fields() {
        let schema = metric_schema();
        assert_eq!(schema.fields().len(), 6);

        let metric_id_field = schema.field_with_name("metric_id").unwrap();
        assert_eq!(metric_id_field.data_type(), &DataType::FixedSizeBinary(16));

        let name_field = schema.field_with_name("name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8);
        assert!(!name_field.is_nullable());
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
        assert_eq!(schema.fields().len(), 8);

        let profile_id_field = schema.field_with_name("profile_id").unwrap();
        assert_eq!(profile_id_field.data_type(), &DataType::FixedSizeBinary(16));

        let data_field = schema.field_with_name("data").unwrap();
        assert_eq!(data_field.data_type(), &DataType::Binary);
    }

    #[test]
    fn test_all_schemas_are_valid() {
        // Verify all schemas can be created without panicking
        let _ = span_schema();
        let _ = log_schema();
        let _ = metric_schema();
        let _ = metric_data_point_schema();
        let _ = histogram_data_point_schema();
        let _ = profile_schema();
    }
}
