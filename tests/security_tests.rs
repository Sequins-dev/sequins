/// Security tests for TursoStorage
///
/// Tests SQL injection protection, input validation, and security hardening.
use sequins::models::*;
use sequins::storage::TursoStorage;
use sequins::traits::{OtlpIngest, QueryApi};
use tempfile::TempDir;

mod test_utils;
use test_utils::fixtures::OtlpFixtures;

/// Create a test storage instance
async fn create_storage() -> (TursoStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let storage = TursoStorage::new(&db_path).await.unwrap();
    (storage, temp_dir)
}

// =============================================================================
// SQL Injection Tests - Service Names
// =============================================================================

#[tokio::test]
async fn test_sql_injection_in_service_name_single_quote() {
    let (storage, _temp_dir) = create_storage().await;

    // Try SQL injection via service name
    let mut span = OtlpFixtures::valid_span();
    span.service_name = "'; DROP TABLE spans; --".to_string();

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    // Verify span was inserted and table still exists
    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().service_name, "'; DROP TABLE spans; --");
}

#[tokio::test]
async fn test_sql_injection_in_service_name_union() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.service_name = "test' UNION SELECT * FROM spans WHERE '1'='1".to_string();

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(
        result.unwrap().service_name,
        "test' UNION SELECT * FROM spans WHERE '1'='1"
    );
}

#[tokio::test]
async fn test_sql_injection_in_service_query() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert legitimate span
    let span = OtlpFixtures::valid_span();
    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    // Try SQL injection in service filter
    let query = SpanQuery {
        trace_id: None,
        service: Some("'; DROP TABLE spans; --".to_string()),
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        attributes: None,
        limit: Some(10),
        live: false,
    };

    // Should not crash or return legitimate span
    let result = storage.query_spans(query).await.unwrap();
    assert_eq!(result.spans.len(), 0);
}

// =============================================================================
// SQL Injection Tests - Attribute Keys/Values
// =============================================================================

#[tokio::test]
async fn test_sql_injection_in_attribute_key() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.attributes.insert(
        "'; DROP TABLE spans; --".to_string(),
        AttributeValue::String("test".to_string()),
    );

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    let retrieved = result.unwrap();
    assert!(retrieved.attributes.contains_key("'; DROP TABLE spans; --"));
}

#[tokio::test]
async fn test_sql_injection_in_attribute_value() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.attributes.insert(
        "test_key".to_string(),
        AttributeValue::String("'; DROP TABLE spans; --".to_string()),
    );

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    let retrieved = result.unwrap();
    assert_eq!(
        retrieved.attributes.get("test_key"),
        Some(&AttributeValue::String(
            "'; DROP TABLE spans; --".to_string()
        ))
    );
}

#[tokio::test]
async fn test_sql_injection_in_attribute_query() {
    let (storage, _temp_dir) = create_storage().await;

    let span = OtlpFixtures::valid_span();
    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    // Try SQL injection in attribute filter
    let attributes = vec![AttributeFilter {
        key: "'; DROP TABLE spans; --".to_string(),
        value: AttributeFilterValue::Equals("value".to_string()),
    }];

    let query = SpanQuery {
        trace_id: None,
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        attributes: Some(attributes),
        limit: Some(10),
        live: false,
    };

    // Should not crash
    let result = storage.query_spans(query).await.unwrap();
    assert_eq!(result.spans.len(), 0);
}

// =============================================================================
// SQL Injection Tests - Metric Names
// =============================================================================

#[tokio::test]
async fn test_sql_injection_in_metric_name() {
    let (storage, _temp_dir) = create_storage().await;

    let (mut metric, _data_points) = OtlpFixtures::valid_gauge();
    let metric_id = metric.id.clone();
    metric.name = "'; DROP TABLE metrics; --".to_string();

    storage.ingest_metrics(vec![metric.clone()]).await.unwrap();

    let result = storage.get_metric(metric_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().name, "'; DROP TABLE metrics; --");
}

#[tokio::test]
async fn test_sql_injection_in_metric_query() {
    let (storage, _temp_dir) = create_storage().await;

    let (metric, _data_points) = OtlpFixtures::valid_gauge();
    storage.ingest_metrics(vec![metric.clone()]).await.unwrap();

    let query = MetricQuery {
        name: Some("'; DROP TABLE metrics; --".to_string()),
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        limit: Some(10),
        live: false,
    };

    let result = storage.query_metrics(query).await.unwrap();
    assert_eq!(result.metrics.len(), 0);
}

// =============================================================================
// SQL Injection Tests - Log Messages
// =============================================================================

#[tokio::test]
async fn test_sql_injection_in_log_message() {
    let (storage, _temp_dir) = create_storage().await;

    let mut log = OtlpFixtures::valid_log();
    log.body = "'; DROP TABLE logs; --".to_string();

    storage.ingest_logs(vec![log.clone()]).await.unwrap();

    let result = storage.get_log(log.id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().body, "'; DROP TABLE logs; --");
}

// =============================================================================
// Path Traversal / Directory Traversal Tests
// =============================================================================

#[tokio::test]
async fn test_path_traversal_in_service_name() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.service_name = "../../../etc/passwd".to_string();

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().service_name, "../../../etc/passwd");
}

// =============================================================================
// Unicode and Special Character Tests
// =============================================================================

#[tokio::test]
async fn test_unicode_in_service_name() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.service_name = "服务-🔥-test".to_string();

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().service_name, "服务-🔥-test");
}

#[tokio::test]
async fn test_null_bytes_in_service_name() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.service_name = "test\0null\0bytes".to_string();

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    // SQLite truncates strings at null bytes (secure behavior)
    assert_eq!(result.unwrap().service_name, "test");
}

#[tokio::test]
async fn test_control_characters_in_log_message() {
    let (storage, _temp_dir) = create_storage().await;

    let mut log = OtlpFixtures::valid_log();
    log.body = "test\r\n\t\x1b[31mred\x1b[0m".to_string();

    storage.ingest_logs(vec![log.clone()]).await.unwrap();

    let result = storage.get_log(log.id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().body, "test\r\n\t\x1b[31mred\x1b[0m");
}

// =============================================================================
// JSON Injection Tests (JSONB attributes)
// =============================================================================

#[tokio::test]
async fn test_json_injection_in_attributes() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    // Try to inject malicious JSON structure
    span.attributes.insert(
        "test".to_string(),
        AttributeValue::String(r#"{"evil": "payload"}"#.to_string()),
    );

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    let retrieved = result.unwrap();
    // Should be stored as string, not parsed as JSON
    assert_eq!(
        retrieved.attributes.get("test"),
        Some(&AttributeValue::String(
            r#"{"evil": "payload"}"#.to_string()
        ))
    );
}

// =============================================================================
// Extremely Long Input Tests
// =============================================================================

#[tokio::test]
async fn test_very_long_service_name() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.service_name = "a".repeat(10000);

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().service_name.len(), 10000);
}

#[tokio::test]
async fn test_very_long_log_message() {
    let (storage, _temp_dir) = create_storage().await;

    let mut log = OtlpFixtures::valid_log();
    log.body = "x".repeat(100000);

    storage.ingest_logs(vec![log.clone()]).await.unwrap();

    let result = storage.get_log(log.id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().body.len(), 100000);
}

#[tokio::test]
async fn test_many_attributes() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.attributes.clear(); // Clear existing attributes
                             // Add 1000 attributes
    for i in 0..1000 {
        span.attributes.insert(
            format!("attr_{}", i),
            AttributeValue::String(format!("value_{}", i)),
        );
    }

    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    let result = storage.get_span(span.trace_id, span.span_id).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().attributes.len(), 1000);
}

// =============================================================================
// Case Sensitivity Tests
// =============================================================================

#[tokio::test]
async fn test_case_sensitivity_in_service_query() {
    let (storage, _temp_dir) = create_storage().await;

    let mut span = OtlpFixtures::valid_span();
    span.service_name = "TestService".to_string();
    storage.ingest_spans(vec![span.clone()]).await.unwrap();

    // Query with different case
    let query = SpanQuery {
        trace_id: None,
        service: Some("testservice".to_string()),
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        attributes: None,
        limit: Some(10),
        live: false,
    };

    let result = storage.query_spans(query).await.unwrap();
    // SQLite is case-insensitive by default for ASCII, but we should be explicit
    // This test documents current behavior
    assert_eq!(result.spans.len(), 0);
}

// =============================================================================
// Wildcard and Regex Injection Tests
// =============================================================================

#[tokio::test]
async fn test_sql_wildcard_in_service_name() {
    let (storage, _temp_dir) = create_storage().await;

    // Insert two services
    let mut span1 = OtlpFixtures::valid_span();
    span1.service_name = "service-a".to_string();
    let spans = OtlpFixtures::large_span_batch(2);
    let mut span2 = spans[1].clone();
    span2.service_name = "service-b".to_string();

    storage
        .ingest_spans(vec![span1, span2.clone()])
        .await
        .unwrap();

    // Query with SQL wildcard - should NOT match both services
    let query = SpanQuery {
        trace_id: None,
        service: Some("service-%".to_string()),
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        attributes: None,
        limit: Some(10),
        live: false,
    };

    let result = storage.query_spans(query).await.unwrap();
    // Should treat % as literal character, not wildcard
    assert_eq!(result.spans.len(), 0);
}

// =============================================================================
// Concurrent Access Security Tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_inserts_no_corruption() {
    let (storage, _temp_dir) = create_storage().await;

    // Spawn 10 concurrent tasks inserting spans
    let mut handles = vec![];
    for i in 0..10 {
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            let spans = OtlpFixtures::large_span_batch(10);
            let mut modified_spans = vec![];
            for (j, mut span) in spans.into_iter().enumerate() {
                span.service_name = format!("service-{}-{}", i, j);
                span.trace_id = TraceId::from_bytes([
                    i as u8, j as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ]);
                modified_spans.push(span);
            }
            storage_clone.ingest_spans(modified_spans).await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    // Verify all services exist
    let services = storage.get_services().await.unwrap();
    assert_eq!(services.len(), 100); // 10 tasks * 10 spans each
}
