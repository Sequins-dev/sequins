/// JSONB storage and generated column tests
mod test_utils;

use serde_json::json;
use test_utils::TestDatabase;

// ============================================================================
// JSONB ROUNDTRIP TESTS
// ============================================================================

#[tokio::test]
async fn test_jsonb_roundtrip_simple_attributes() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with simple attributes
    let attributes = json!({
        "environment": "test",
        "version": "1.0.0",
        "enabled": true,
        "count": 42
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert_eq!(parsed["environment"], "test");
    assert_eq!(parsed["version"], "1.0.0");
    assert_eq!(parsed["enabled"], true);
    assert_eq!(parsed["count"], 42);
}

#[tokio::test]
async fn test_jsonb_roundtrip_nested_attributes() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with nested attributes
    let attributes = json!({
        "http": {
            "method": "GET",
            "status_code": 200,
            "url": "https://api.example.com/users"
        },
        "tags": ["production", "web", "api"],
        "metadata": {
            "region": "us-west-2",
            "datacenter": "dc1"
        }
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify nested structure
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert_eq!(parsed["http"]["method"], "GET");
    assert_eq!(parsed["http"]["status_code"], 200);
    assert_eq!(parsed["tags"][0], "production");
    assert_eq!(parsed["metadata"]["region"], "us-west-2");
}

#[tokio::test]
async fn test_jsonb_handles_unicode() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with unicode content
    let attributes = json!({
        "japanese": "こんにちは世界",
        "chinese": "你好世界",
        "emoji": "🚀🎉💻",
        "mixed": "Hello 世界 🌍"
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify unicode preserved
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert_eq!(parsed["japanese"], "こんにちは世界");
    assert_eq!(parsed["chinese"], "你好世界");
    assert_eq!(parsed["emoji"], "🚀🎉💻");
    assert_eq!(parsed["mixed"], "Hello 世界 🌍");
}

#[tokio::test]
async fn test_jsonb_handles_special_chars() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with special characters that might break JSON/SQL
    let attributes = json!({
        "quotes": r#"It's a "test" with 'quotes'"#,
        "backslashes": r"C:\Users\Test\Path",
        "newlines": "line1\nline2\r\nline3",
        "tabs": "col1\tcol2\tcol3"
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify special chars preserved
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert!(parsed["quotes"].as_str().unwrap().contains("\"test\""));
    assert!(parsed["backslashes"].as_str().unwrap().contains("\\"));
}

#[tokio::test]
async fn test_jsonb_handles_empty_object() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with empty attributes
    let attributes = json!({});

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify empty object
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert!(parsed.is_object());
    assert_eq!(parsed.as_object().unwrap().len(), 0);
}

#[tokio::test]
async fn test_jsonb_handles_null_values() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with null values in attributes
    let attributes = json!({
        "present": "value",
        "null_field": null
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify null preserved
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert_eq!(parsed["present"], "value");
    assert!(parsed["null_field"].is_null());
}

// ============================================================================
// GENERATED COLUMN TESTS (Semantic Conventions)
// ============================================================================

#[tokio::test]
async fn test_http_method_generated_column() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with http.method attribute
    let attributes = json!({
        "http.method": "POST",
        "http.status_code": 201
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query generated column
    let mut rows = db
        .conn
        .query("SELECT http_method FROM spans WHERE span_id = 'span1'", ())
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let http_method: String = row.get(0).unwrap();
    assert_eq!(http_method, "POST");
}

#[tokio::test]
async fn test_http_status_code_generated_column() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with http.status_code attribute
    let attributes = json!({
        "http.method": "GET",
        "http.status_code": 404
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query generated column
    let mut rows = db
        .conn
        .query(
            "SELECT http_status_code FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let status_code: i64 = row.get(0).unwrap();
    assert_eq!(status_code, 404);
}

#[tokio::test]
async fn test_rpc_method_generated_column() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with rpc.method attribute
    let attributes = json!({
        "rpc.system": "grpc",
        "rpc.method": "GetUser",
        "rpc.service": "UserService"
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query generated columns
    let mut rows = db
        .conn
        .query(
            "SELECT rpc_method, rpc_system FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let rpc_method: String = row.get(0).unwrap();
    let rpc_system: String = row.get(1).unwrap();
    assert_eq!(rpc_method, "GetUser");
    assert_eq!(rpc_system, "grpc");
}

#[tokio::test]
async fn test_db_system_generated_column() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with db.system attribute
    let attributes = json!({
        "db.system": "postgresql",
        "db.operation": "SELECT",
        "db.statement": "SELECT * FROM users WHERE id = $1"
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query generated columns
    let mut rows = db
        .conn
        .query(
            "SELECT db_system, db_operation FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let db_system: String = row.get(0).unwrap();
    let db_operation: String = row.get(1).unwrap();
    assert_eq!(db_system, "postgresql");
    assert_eq!(db_operation, "SELECT");
}

#[tokio::test]
async fn test_messaging_system_generated_column() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with messaging attributes
    let attributes = json!({
        "messaging.system": "kafka",
        "messaging.operation": "publish",
        "messaging.destination": "user-events"
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query generated columns
    let mut rows = db
        .conn
        .query(
            "SELECT messaging_system, messaging_operation FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let messaging_system: String = row.get(0).unwrap();
    let messaging_operation: String = row.get(1).unwrap();
    assert_eq!(messaging_system, "kafka");
    assert_eq!(messaging_operation, "publish");
}

#[tokio::test]
async fn test_generated_columns_null_when_attribute_missing() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span without semantic convention attributes
    let attributes = json!({
        "custom.field": "value"
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query generated columns - should be NULL
    let mut rows = db
        .conn
        .query(
            "SELECT http_method, http_status_code, rpc_method, db_system FROM spans WHERE span_id = 'span1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();

    // All generated columns should be NULL when attributes don't exist
    let http_method: Option<String> = row.get(0).unwrap();
    let http_status_code: Option<i64> = row.get(1).unwrap();
    let rpc_method: Option<String> = row.get(2).unwrap();
    let db_system: Option<String> = row.get(3).unwrap();

    assert!(http_method.is_none());
    assert!(http_status_code.is_none());
    assert!(rpc_method.is_none());
    assert!(db_system.is_none());
}

// ============================================================================
// JSONB QUERY TESTS
// ============================================================================

#[tokio::test]
async fn test_query_by_generated_column_uses_index() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert multiple spans with different HTTP methods
    for (i, method) in ["GET", "POST", "PUT", "DELETE"].iter().enumerate() {
        let attributes = json!({
            "http.method": method,
            "http.status_code": 200
        });

        db.conn
            .execute(
                &format!(
                    "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
                     VALUES ('span{}', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
                    i
                ),
                [attributes.to_string()],
            )
            .await
            .unwrap();
    }

    // Query by generated column (should use index)
    let mut rows = db
        .conn
        .query("SELECT span_id FROM spans WHERE http_method = 'POST'", ())
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let span_id: String = row.get(0).unwrap();
    assert_eq!(span_id, "span1"); // POST was second (index 1)
}

#[tokio::test]
async fn test_query_nested_json_attribute() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with nested attributes
    let attributes = json!({
        "http": {
            "method": "GET",
            "url": "https://api.example.com/users"
        }
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query using json_extract for nested field
    let mut rows = db
        .conn
        .query(
            "SELECT span_id FROM spans WHERE json_extract(json(attributes), '$.http.url') = 'https://api.example.com/users'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let span_id: String = row.get(0).unwrap();
    assert_eq!(span_id, "span1");
}

#[tokio::test]
async fn test_query_json_array_contains() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert span with array attribute
    let attributes = json!({
        "tags": ["production", "web", "api"]
    });

    db.conn
        .execute(
            "INSERT INTO spans (span_id, trace_id, name, kind, start_time, end_time, status_code, service_name, attributes)
             VALUES ('span1', 'trace1', 'test', 0, 0, 100, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query for array element
    let mut rows = db
        .conn
        .query(
            "SELECT span_id FROM spans WHERE json_extract(json(attributes), '$.tags[0]') = 'production'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let span_id: String = row.get(0).unwrap();
    assert_eq!(span_id, "span1");
}

// ============================================================================
// LOGS JSONB TESTS
// ============================================================================

#[tokio::test]
async fn test_logs_jsonb_roundtrip() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert log with attributes
    let attributes = json!({
        "user_id": "12345",
        "ip_address": "192.168.1.1",
        "user_agent": "Mozilla/5.0"
    });

    db.conn
        .execute(
            "INSERT INTO logs (log_id, timestamp, observed_timestamp, severity_number, body, service_name, attributes)
             VALUES ('log1', 1700000000, 1700000001, 9, 'Test log', 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM logs WHERE log_id = 'log1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert_eq!(parsed["user_id"], "12345");
    assert_eq!(parsed["ip_address"], "192.168.1.1");
}

// ============================================================================
// METRICS JSONB TESTS
// ============================================================================

#[tokio::test]
async fn test_metrics_jsonb_roundtrip() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert metric with attributes
    let attributes = json!({
        "host": "server1",
        "region": "us-west-2"
    });

    db.conn
        .execute(
            "INSERT INTO metrics (metric_id, name, description, unit, type, temporality, service_name, attributes)
             VALUES ('metric1', 'cpu.usage', 'CPU usage', '%', 0, 0, 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM metrics WHERE metric_id = 'metric1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert_eq!(parsed["host"], "server1");
    assert_eq!(parsed["region"], "us-west-2");
}

// ============================================================================
// PROFILES JSONB TESTS
// ============================================================================

#[tokio::test]
async fn test_profiles_jsonb_roundtrip() {
    let db = TestDatabase::with_schema().await.unwrap();

    // Insert profile with attributes
    let attributes = json!({
        "profile_id": "prof123",
        "language": "rust"
    });

    db.conn
        .execute(
            "INSERT INTO profiles (profile_id, timestamp, duration, profile_type, sample_rate, samples, service_name, attributes)
             VALUES ('profile1', 1700000000, 5000000000, 'cpu', 100, '[]', 'service1', jsonb(?))",
            [attributes.to_string()],
        )
        .await
        .unwrap();

    // Query back and verify
    let mut rows = db
        .conn
        .query(
            "SELECT json(attributes) as attrs FROM profiles WHERE profile_id = 'profile1'",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    let attrs_json: String = row.get(0).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&attrs_json).unwrap();

    assert_eq!(parsed["profile_id"], "prof123");
    assert_eq!(parsed["language"], "rust");
}
