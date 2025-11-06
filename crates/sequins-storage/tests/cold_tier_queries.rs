//! Integration tests for ColdTier DataFusion queries
//!
//! These tests verify that the SQL-based queries work correctly with actual Parquet files.

use sequins_core::models::{
    Duration, LogEntry, LogId, LogQuery, LogSeverity, Metric, MetricId, MetricQuery, MetricType,
    Profile, ProfileId, ProfileQuery, ProfileType, Span, SpanId, SpanKind, SpanStatus, Timestamp,
    TraceId, TraceQuery,
};
use sequins_storage::config::{ColdTierConfig, CompressionCodec};
use sequins_storage::ColdTier;
use std::collections::HashMap;
use tempfile::TempDir;

/// Helper to create a test ColdTier with a temporary directory
fn create_test_cold_tier() -> (ColdTier, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    let config = ColdTierConfig {
        uri: format!("file://{}", path),
        enable_bloom_filters: false,
        compression: CompressionCodec::Snappy,
        row_group_size: 1024,
        index_path: None,
    };

    let cold_tier = ColdTier::new(config).unwrap();
    (cold_tier, temp_dir)
}

/// Helper to create test spans
fn create_test_spans(count: usize) -> Vec<Span> {
    let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    let base_time = Timestamp::from_nanos(1_704_672_000_000_000_000); // 2024-01-08 00:00:00 UTC

    (0..count)
        .map(|i| {
            let span_id = SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, i as u8 + 1]);
            let start_time = base_time + Duration::from_millis(i as i64 * 100);
            let duration = Duration::from_millis(50 + (i as i64 % 3) * 50); // 50ms, 100ms, 150ms
            let end_time = start_time + duration;

            Span {
                trace_id,
                span_id,
                parent_span_id: if i == 0 {
                    None
                } else {
                    Some(SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, 1]))
                },
                service_name: if i % 2 == 0 {
                    "api-gateway".to_string()
                } else {
                    "backend".to_string()
                },
                operation_name: format!("operation-{}", i),
                start_time,
                end_time,
                duration,
                attributes: HashMap::new(),
                events: Vec::new(),
                status: if i % 3 == 0 {
                    SpanStatus::Error
                } else {
                    SpanStatus::Ok
                },
                span_kind: SpanKind::Server,
            }
        })
        .collect()
}

#[tokio::test]
async fn test_query_traces_basic() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write test spans to Parquet
    let spans = create_test_spans(10);
    cold_tier.write_spans(spans.clone()).await.unwrap();

    // Query all spans
    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get all 10 spans back
    assert_eq!(results.len(), 10);

    // Verify they're sorted by start_time descending
    for i in 0..results.len() - 1 {
        assert!(results[i].start_time >= results[i + 1].start_time);
    }
}

#[tokio::test]
async fn test_query_traces_with_service_filter() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write test spans
    let spans = create_test_spans(10);
    cold_tier.write_spans(spans).await.unwrap();

    // Query only api-gateway spans
    let query = TraceQuery {
        service: Some("api-gateway".to_string()),
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get 5 spans (even indices: 0, 2, 4, 6, 8)
    assert_eq!(results.len(), 5);
    assert!(results.iter().all(|s| s.service_name == "api-gateway"));
}

#[tokio::test]
async fn test_query_traces_with_time_range() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write test spans
    let spans = create_test_spans(10);
    let base_time = spans[0].start_time;
    cold_tier.write_spans(spans).await.unwrap();

    // Query only spans in the middle of the time range
    let start = base_time + Duration::from_millis(300);
    let end = base_time + Duration::from_millis(700);

    let query = TraceQuery {
        service: None,
        start_time: start,
        end_time: end,
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get spans 3, 4, 5, 6, 7 (5 spans)
    assert_eq!(results.len(), 5);
    assert!(results
        .iter()
        .all(|s| s.start_time >= start && s.start_time <= end));
}

#[tokio::test]
async fn test_query_traces_with_duration_filter() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write test spans (durations: 50ms, 100ms, 150ms alternating)
    let spans = create_test_spans(10);
    cold_tier.write_spans(spans).await.unwrap();

    // Query only spans with duration >= 100ms
    let min_duration_ns = Duration::from_millis(100).as_nanos();

    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        min_duration: Some(min_duration_ns),
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get spans with 100ms and 150ms durations
    assert!(results.len() >= 6); // At least 6 spans should have duration >= 100ms
    assert!(results
        .iter()
        .all(|s| s.duration.as_nanos() >= min_duration_ns));
}

#[tokio::test]
async fn test_query_traces_with_error_filter() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write test spans (every 3rd span has error status)
    let spans = create_test_spans(10);
    cold_tier.write_spans(spans).await.unwrap();

    // Query only spans with errors
    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        min_duration: None,
        max_duration: None,
        has_error: Some(true),
        limit: Some(100),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get spans 0, 3, 6, 9 (4 spans with errors)
    assert_eq!(results.len(), 4);
    assert!(results.iter().all(|s| s.status == SpanStatus::Error));
}

#[tokio::test]
async fn test_query_traces_with_limit() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write test spans
    let spans = create_test_spans(10);
    cold_tier.write_spans(spans).await.unwrap();

    // Query with limit of 3
    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(3),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get exactly 3 spans
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn test_query_traces_empty_result() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write test spans
    let spans = create_test_spans(10);
    cold_tier.write_spans(spans).await.unwrap();

    // Query with time range that doesn't match any data
    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(1000),
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get no results
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_query_traces_multiple_batches() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Write multiple batches of spans
    let batch1 = create_test_spans(5);
    let batch2 = create_test_spans(5);

    cold_tier.write_spans(batch1).await.unwrap();
    cold_tier.write_spans(batch2).await.unwrap();

    // Query all spans
    let query = TraceQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier.query_traces(&query).await.unwrap();

    // Should get spans from both batches
    assert_eq!(results.len(), 10);
}

#[tokio::test]
async fn test_query_logs_basic() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Create test logs
    let base_time = Timestamp::from_nanos(1_704_672_000_000_000_000);
    let logs: Vec<LogEntry> = (0..5)
        .map(|i| {
            let ts = base_time + Duration::from_millis(i * 100);
            LogEntry {
                id: LogId::new(),
                timestamp: ts,
                observed_timestamp: ts,
                service_name: "test-service".to_string(),
                severity: LogSeverity::Info,
                body: format!("Test log message {}", i),
                attributes: HashMap::new(),
                trace_id: None,
                span_id: None,
                resource: HashMap::new(),
            }
        })
        .collect();

    cold_tier.write_logs(logs.clone()).await.unwrap();

    // Query all logs
    let query = LogQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        severity: None,
        search: None,
        trace_id: None,
        limit: Some(100),
    };

    let results = cold_tier.query_logs(&query).await.unwrap();

    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn test_query_logs_with_search() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Create test logs with different messages
    let base_time = Timestamp::from_nanos(1_704_672_000_000_000_000);
    let logs: Vec<LogEntry> = vec![
        LogEntry {
            id: LogId::new(),
            timestamp: base_time,
            observed_timestamp: base_time,
            service_name: "test-service".to_string(),
            severity: LogSeverity::Info,
            body: "User login successful".to_string(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        },
        LogEntry {
            id: LogId::new(),
            timestamp: base_time + Duration::from_millis(100),
            observed_timestamp: base_time + Duration::from_millis(100),
            service_name: "test-service".to_string(),
            severity: LogSeverity::Error,
            body: "Database connection failed".to_string(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        },
        LogEntry {
            id: LogId::new(),
            timestamp: base_time + Duration::from_millis(200),
            observed_timestamp: base_time + Duration::from_millis(200),
            service_name: "test-service".to_string(),
            severity: LogSeverity::Info,
            body: "User logout successful".to_string(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        },
    ];

    cold_tier.write_logs(logs).await.unwrap();

    // Search for logs containing "successful"
    let query = LogQuery {
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        severity: None,
        search: Some("successful".to_string()),
        trace_id: None,
        limit: Some(100),
    };

    let results = cold_tier.query_logs(&query).await.unwrap();

    // Should get 2 logs (login and logout)
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|l| l.body.contains("successful")));
}

#[tokio::test]
async fn test_query_metrics_basic() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Create test metrics
    let metrics: Vec<Metric> = (0..5)
        .map(|i| Metric {
            id: MetricId::new(),
            name: format!("test.metric.{}", i),
            description: format!("Test metric {}", i),
            unit: "ms".to_string(),
            metric_type: MetricType::Gauge,
            service_name: "test-service".to_string(),
        })
        .collect();

    cold_tier.write_metrics(metrics.clone()).await.unwrap();

    // Query all metrics
    let query = MetricQuery {
        name: None,
        service: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        limit: Some(100),
    };

    let results = cold_tier.query_metrics(&query).await.unwrap();

    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn test_query_profiles_basic() {
    let (cold_tier, _temp_dir) = create_test_cold_tier();

    // Create test profiles
    let base_time = Timestamp::from_nanos(1_704_672_000_000_000_000);
    let profiles: Vec<Profile> = (0..3)
        .map(|i| Profile {
            id: ProfileId::new(),
            timestamp: base_time + Duration::from_millis(i * 100),
            service_name: "backend".to_string(),
            profile_type: ProfileType::Cpu,
            sample_type: "samples".to_string(),
            sample_unit: "count".to_string(),
            data: vec![1, 2, 3, 4, 5],
            trace_id: None,
        })
        .collect();

    cold_tier.write_profiles(profiles.clone()).await.unwrap();

    // Query all profiles
    let query = ProfileQuery {
        service: None,
        profile_type: None,
        start_time: Timestamp::from_nanos(0),
        end_time: Timestamp::from_nanos(i64::MAX),
        trace_id: None,
        limit: Some(100),
    };

    let results = cold_tier.query_profiles(&query).await.unwrap();

    assert_eq!(results.len(), 3);
}
