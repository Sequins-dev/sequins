//! Arrow data conversion utilities

use arrow::datatypes::DataType as ArrowDataType;
use sequins_query::schema::{ColumnDef, ColumnRole, DataType};

/// Convert an Arrow schema into a list of ColumnDef (no batch data needed)
pub(crate) fn schema_to_col_defs(schema: &arrow::datatypes::Schema) -> Vec<ColumnDef> {
    schema
        .fields()
        .iter()
        .map(|f| ColumnDef {
            name: f.name().clone(),
            data_type: arrow_type_to_data_type(f.data_type()),
            role: ColumnRole::Field,
        })
        .collect()
}

/// Map Arrow data type to SeQL schema DataType
pub(crate) fn arrow_type_to_data_type(dt: &ArrowDataType) -> DataType {
    match dt {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            DataType::String
        }
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64 => DataType::Int64,
        ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => DataType::UInt64,
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => {
            DataType::Float64
        }
        ArrowDataType::Boolean => DataType::Bool,
        ArrowDataType::Timestamp(_, _) => DataType::Timestamp,
        ArrowDataType::Duration(_) => DataType::Duration,
        _ => DataType::String,
    }
}

#[cfg(test)]
mod tests {
    use crate::test_fixtures::{
        make_test_otlp_logs, make_test_otlp_metrics, make_test_otlp_profiles,
        make_test_otlp_traces, TestStorageBuilder,
    };
    use sequins_types::ingest::OtlpIngest;

    #[tokio::test]
    async fn test_spans_to_arrow_arrays() {
        // Create storage and ingest test spans
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let request = make_test_otlp_traces(1, 3);
        storage.ingest_traces(request).await.unwrap();

        // Verify spans landed in the hot tier BatchChain
        let hot_tier = &storage.hot_tier;
        assert_eq!(hot_tier.spans.row_count(), 3, "Should have 3 spans");

        // Verify we can convert to Arrow schema
        let schema = sequins_types::arrow_schema::span_schema();
        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("start_time_unix_nano").is_ok());
        assert!(schema.field_with_name("end_time_unix_nano").is_ok());
    }

    #[tokio::test]
    async fn test_logs_to_arrow_arrays() {
        // Create storage and ingest test logs
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let request = make_test_otlp_logs(1, 5);
        storage.ingest_logs(request).await.unwrap();

        // Verify logs landed in the hot tier BatchChain
        let hot_tier = &storage.hot_tier;
        assert_eq!(hot_tier.logs.row_count(), 5, "Should have 5 logs");

        // Verify we can convert to Arrow schema
        let schema = sequins_types::arrow_schema::log_schema();
        assert!(schema.field_with_name("log_id").is_ok());
        assert!(schema.field_with_name("body").is_ok());
        assert!(schema.field_with_name("severity_text").is_ok());
        assert!(schema.field_with_name("time_unix_nano").is_ok());
    }

    #[tokio::test]
    async fn test_metrics_to_arrow_arrays() {
        // Create storage and ingest test metrics
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let request = make_test_otlp_metrics(1, 2, 3);
        storage.ingest_metrics(request).await.unwrap();

        // Verify metrics landed in the hot tier BatchChain
        let hot_tier = &storage.hot_tier;
        assert!(hot_tier.metrics.row_count() > 0, "Should have metrics");

        // Verify we can convert to Arrow schema
        let schema = sequins_types::arrow_schema::metric_schema();
        assert!(schema.field_with_name("metric_id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("metric_type").is_ok());

        // Verify data point schema
        let dp_schema = sequins_types::arrow_schema::metric_data_point_schema();
        assert!(dp_schema.field_with_name("metric_id").is_ok());
        assert!(dp_schema.field_with_name("value").is_ok());
        assert!(dp_schema.field_with_name("time_unix_nano").is_ok());
    }

    #[tokio::test]
    async fn test_profiles_to_arrow_arrays() {
        // Create storage and ingest test profiles
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let request = make_test_otlp_profiles(1, 2);
        storage.ingest_profiles(request).await.unwrap();

        // Verify profiles landed in the hot tier BatchChain
        let hot_tier = &storage.hot_tier;
        assert!(hot_tier.profiles.row_count() > 0, "Should have profiles");

        // Verify we can convert to Arrow schema
        let schema = sequins_types::arrow_schema::profile_schema();
        assert!(schema.field_with_name("profile_id").is_ok());
        assert!(schema.field_with_name("time_unix_nano").is_ok());
        assert!(schema.field_with_name("profile_type").is_ok());
    }
}
