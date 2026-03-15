/// Schema initialization and validation tests
mod test_utils;

use test_utils::TestDatabase;

#[tokio::test]
async fn test_schema_creates_all_tables() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Verify all tables exist
    assert!(
        db.assert_table_exists("spans").await.unwrap(),
        "spans table should exist"
    );
    assert!(
        db.assert_table_exists("logs").await.unwrap(),
        "logs table should exist"
    );
    assert!(
        db.assert_table_exists("logs_fts").await.unwrap(),
        "logs_fts table should exist"
    );
    assert!(
        db.assert_table_exists("metrics").await.unwrap(),
        "metrics table should exist"
    );
    assert!(
        db.assert_table_exists("metric_data_points").await.unwrap(),
        "metric_data_points table should exist"
    );
    assert!(
        db.assert_table_exists("profiles").await.unwrap(),
        "profiles table should exist"
    );
}

#[tokio::test]
async fn test_schema_idempotent() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Run schema initialization again (should not error)
    let result = db
        .execute(
            "CREATE TABLE IF NOT EXISTS spans (
                span_id TEXT PRIMARY KEY NOT NULL,
                trace_id TEXT NOT NULL,
                parent_span_id TEXT,
                name TEXT NOT NULL,
                kind INTEGER NOT NULL,
                start_time INTEGER NOT NULL,
                end_time INTEGER NOT NULL,
                status_code INTEGER NOT NULL,
                status_message TEXT,
                attributes BLOB,
                events BLOB,
                links BLOB,
                service_name TEXT,
                http_method TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.http.method')) VIRTUAL,
                http_status_code INTEGER GENERATED ALWAYS AS (json_extract(json(attributes), '$.http.status_code')) VIRTUAL,
                rpc_method TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.rpc.method')) VIRTUAL,
                rpc_system TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.rpc.system')) VIRTUAL,
                db_system TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.db.system')) VIRTUAL,
                db_operation TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.db.operation')) VIRTUAL,
                messaging_system TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.messaging.system')) VIRTUAL,
                messaging_operation TEXT GENERATED ALWAYS AS (json_extract(json(attributes), '$.messaging.operation')) VIRTUAL
            )",
        )
        .await;

    assert!(result.is_ok(), "Re-running schema creation should not fail");
}

#[tokio::test]
async fn test_spans_table_schema() {
    let db = TestDatabase::with_schema().await.unwrap();
    let columns = db.get_table_info("spans").await.unwrap();

    // Check for required columns
    assert!(columns.iter().any(|(name, _)| name == "span_id"));
    assert!(columns.iter().any(|(name, _)| name == "trace_id"));
    assert!(columns.iter().any(|(name, _)| name == "parent_span_id"));
    assert!(columns.iter().any(|(name, _)| name == "name"));
    assert!(columns.iter().any(|(name, _)| name == "kind"));
    assert!(columns.iter().any(|(name, _)| name == "start_time"));
    assert!(columns.iter().any(|(name, _)| name == "end_time"));
    assert!(columns.iter().any(|(name, _)| name == "status_code"));
    assert!(columns.iter().any(|(name, _)| name == "attributes"));
    assert!(columns.iter().any(|(name, _)| name == "events"));
    assert!(columns.iter().any(|(name, _)| name == "links"));
    assert!(columns.iter().any(|(name, _)| name == "service_name"));

    // Note: Generated columns may not appear in PRAGMA table_info()
    // They should be tested functionally instead (in JSONB tests)
    // SQLite's PRAGMA table_info() often excludes VIRTUAL generated columns
}

#[tokio::test]
async fn test_logs_table_schema() {
    let db = TestDatabase::with_schema().await.unwrap();
    let columns = db.get_table_info("logs").await.unwrap();

    // Check for required columns
    assert!(columns.iter().any(|(name, _)| name == "log_id"));
    assert!(columns.iter().any(|(name, _)| name == "timestamp"));
    assert!(columns.iter().any(|(name, _)| name == "observed_timestamp"));
    assert!(columns.iter().any(|(name, _)| name == "trace_id"));
    assert!(columns.iter().any(|(name, _)| name == "span_id"));
    assert!(columns.iter().any(|(name, _)| name == "severity_number"));
    assert!(columns.iter().any(|(name, _)| name == "severity_text"));
    assert!(columns.iter().any(|(name, _)| name == "body"));
    assert!(columns.iter().any(|(name, _)| name == "attributes"));
    assert!(columns.iter().any(|(name, _)| name == "service_name"));
}

#[tokio::test]
async fn test_metrics_table_schema() {
    let db = TestDatabase::with_schema().await.unwrap();
    let columns = db.get_table_info("metrics").await.unwrap();

    // Check for required columns
    assert!(columns.iter().any(|(name, _)| name == "metric_id"));
    assert!(columns.iter().any(|(name, _)| name == "name"));
    assert!(columns.iter().any(|(name, _)| name == "description"));
    assert!(columns.iter().any(|(name, _)| name == "unit"));
    assert!(columns.iter().any(|(name, _)| name == "type"));
    assert!(columns.iter().any(|(name, _)| name == "temporality"));
    assert!(columns.iter().any(|(name, _)| name == "is_monotonic"));
    assert!(columns.iter().any(|(name, _)| name == "attributes"));
    assert!(columns.iter().any(|(name, _)| name == "service_name"));
}

#[tokio::test]
async fn test_metric_data_points_table_schema() {
    let db = TestDatabase::with_schema().await.unwrap();
    let columns = db.get_table_info("metric_data_points").await.unwrap();

    // Check for required columns
    assert!(columns.iter().any(|(name, _)| name == "id"));
    assert!(columns.iter().any(|(name, _)| name == "metric_id"));
    assert!(columns.iter().any(|(name, _)| name == "timestamp"));
    assert!(columns.iter().any(|(name, _)| name == "start_timestamp"));
    assert!(columns.iter().any(|(name, _)| name == "value_type"));
    assert!(columns.iter().any(|(name, _)| name == "int_value"));
    assert!(columns.iter().any(|(name, _)| name == "double_value"));
    assert!(columns.iter().any(|(name, _)| name == "histogram_data"));
    assert!(columns
        .iter()
        .any(|(name, _)| name == "exponential_histogram_data"));
    assert!(columns.iter().any(|(name, _)| name == "summary_data"));
    assert!(columns.iter().any(|(name, _)| name == "attributes"));
}

#[tokio::test]
async fn test_profiles_table_schema() {
    let db = TestDatabase::with_schema().await.unwrap();
    let columns = db.get_table_info("profiles").await.unwrap();

    // Check for required columns
    assert!(columns.iter().any(|(name, _)| name == "profile_id"));
    assert!(columns.iter().any(|(name, _)| name == "timestamp"));
    assert!(columns.iter().any(|(name, _)| name == "duration"));
    assert!(columns.iter().any(|(name, _)| name == "profile_type"));
    assert!(columns.iter().any(|(name, _)| name == "sample_rate"));
    assert!(columns.iter().any(|(name, _)| name == "samples"));
    assert!(columns.iter().any(|(name, _)| name == "attributes"));
    assert!(columns.iter().any(|(name, _)| name == "service_name"));
}

#[tokio::test]
async fn test_indexes_created() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Spans indexes
    assert!(db.assert_index_exists("idx_spans_trace_id").await.unwrap());
    assert!(db
        .assert_index_exists("idx_spans_start_time")
        .await
        .unwrap());
    assert!(db
        .assert_index_exists("idx_spans_service_name")
        .await
        .unwrap());
    assert!(db
        .assert_index_exists("idx_spans_http_method")
        .await
        .unwrap());
    assert!(db
        .assert_index_exists("idx_spans_http_status_code")
        .await
        .unwrap());

    // Logs indexes
    assert!(db.assert_index_exists("idx_logs_timestamp").await.unwrap());
    assert!(db.assert_index_exists("idx_logs_trace_id").await.unwrap());
    assert!(db
        .assert_index_exists("idx_logs_severity_number")
        .await
        .unwrap());
    assert!(db
        .assert_index_exists("idx_logs_service_name")
        .await
        .unwrap());

    // Metrics indexes
    assert!(db.assert_index_exists("idx_metrics_name").await.unwrap());
    assert!(db
        .assert_index_exists("idx_metrics_service_name")
        .await
        .unwrap());

    // Metric data points indexes
    assert!(db
        .assert_index_exists("idx_metric_data_points_metric_id")
        .await
        .unwrap());
    assert!(db
        .assert_index_exists("idx_metric_data_points_timestamp")
        .await
        .unwrap());

    // Profiles indexes
    assert!(db
        .assert_index_exists("idx_profiles_timestamp")
        .await
        .unwrap());
    assert!(db
        .assert_index_exists("idx_profiles_service_name")
        .await
        .unwrap());
    assert!(db.assert_index_exists("idx_profiles_type").await.unwrap());
}

#[tokio::test]
async fn test_fts5_triggers_created() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Check FTS5 triggers for logs
    assert!(db.assert_trigger_exists("logs_fts_insert").await.unwrap());
    assert!(db.assert_trigger_exists("logs_fts_delete").await.unwrap());
    assert!(db.assert_trigger_exists("logs_fts_update").await.unwrap());
}

#[tokio::test]
async fn test_fts5_table_created() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Verify FTS5 virtual table exists
    assert!(db.assert_table_exists("logs_fts").await.unwrap());
}

#[tokio::test]
async fn test_empty_tables_after_initialization() {
    let db = TestDatabase::with_schema().await.unwrap();

    // All tables should be empty after initialization
    assert_eq!(db.count_rows("spans").await.unwrap(), 0);
    assert_eq!(db.count_rows("logs").await.unwrap(), 0);
    assert_eq!(db.count_rows("metrics").await.unwrap(), 0);
    assert_eq!(db.count_rows("metric_data_points").await.unwrap(), 0);
    assert_eq!(db.count_rows("profiles").await.unwrap(), 0);
}

// Note: Database isolation is already tested in test_utils/database.rs
