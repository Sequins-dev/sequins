//! Integration test for Phase 1: MemTable Prototype
//!
//! This test validates the DataFusion integration approach by:
//! 1. Inserting test data into both hot and cold tiers
//! 2. Running a unified SQL query via MemTable
//! 3. Verifying results include data from both tiers
//!
//! This is a proof-of-concept that will be replaced with a custom
//! TableProvider in Phase 2.

use sequins_core::models::{
    Duration, SpanId, SpanKind, SpanStatus, Timestamp, TraceId, TraceQuery,
};
use sequins_storage::{
    config::{ColdTierConfig, CompressionCodec, HotTierConfig},
    ColdTier, HotTier,
};
use std::collections::HashMap;
use tempfile::TempDir;

#[tokio::test]
async fn test_memtable_prototype_unified_query() {
    // Setup
    let temp_dir = TempDir::new().unwrap();
    let hot_config = HotTierConfig {
        max_age: Duration::from_minutes(5),
        max_entries: 1000,
    };
    let cold_config = ColdTierConfig {
        uri: format!("file://{}", temp_dir.path().display()),
        enable_bloom_filters: false,
        compression: CompressionCodec::Snappy,
        row_group_size: 1000,
        index_path: None,
    };

    let hot_tier = HotTier::new(hot_config);
    let cold_tier = ColdTier::new(cold_config).unwrap();

    // Create test spans
    let now = Timestamp::now().unwrap();
    let trace_id = TraceId::from_bytes([1; 16]);

    // Hot tier span (recent)
    let hot_span = sequins_core::models::Span {
        trace_id,
        span_id: SpanId::from_bytes([1; 8]),
        parent_span_id: None,
        service_name: "hot-service".to_string(),
        operation_name: "hot-operation".to_string(),
        start_time: now,
        end_time: now + Duration::from_secs(1),
        duration: Duration::from_secs(1),
        attributes: HashMap::new(),
        events: Vec::new(),
        span_kind: SpanKind::Server,
        status: SpanStatus::Ok,
    };

    // Cold tier span (older)
    let cold_span = sequins_core::models::Span {
        trace_id,
        span_id: SpanId::from_bytes([2; 8]),
        parent_span_id: None,
        service_name: "cold-service".to_string(),
        operation_name: "cold-operation".to_string(),
        start_time: now - Duration::from_minutes(10),
        end_time: now - Duration::from_minutes(10) + Duration::from_secs(2),
        duration: Duration::from_secs(2),
        attributes: HashMap::new(),
        events: Vec::new(),
        span_kind: SpanKind::Client,
        status: SpanStatus::Ok,
    };

    // Insert spans
    hot_tier.insert_span(hot_span.clone()).unwrap();
    cold_tier
        .write_spans(vec![cold_span.clone()])
        .await
        .unwrap();

    // Build query covering both spans
    let query = TraceQuery {
        start_time: now - Duration::from_minutes(15),
        end_time: now + Duration::from_minutes(1),
        service: None,
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    // Execute unified query via MemTable prototype
    let results = cold_tier
        .query_traces_memtable_prototype(&hot_tier, &query)
        .await
        .unwrap();

    // Verify results
    assert_eq!(results.len(), 2, "Should have 2 spans from both tiers");

    // Check we got both spans (order may vary)
    let hot_found = results.iter().any(|s| s.service_name == "hot-service");
    let cold_found = results.iter().any(|s| s.service_name == "cold-service");

    assert!(hot_found, "Should find span from hot tier");
    assert!(cold_found, "Should find span from cold tier");

    // Verify span details
    for span in &results {
        match span.service_name.as_str() {
            "hot-service" => {
                assert_eq!(span.operation_name, "hot-operation");
                assert_eq!(span.span_kind, SpanKind::Server);
                assert_eq!(span.duration, Duration::from_secs(1));
            }
            "cold-service" => {
                assert_eq!(span.operation_name, "cold-operation");
                assert_eq!(span.span_kind, SpanKind::Client);
                assert_eq!(span.duration, Duration::from_secs(2));
            }
            other => panic!("Unexpected service: {}", other),
        }
    }

    println!("✓ MemTable prototype successfully unified hot and cold tier queries");
    println!("  Found {} spans across both tiers", results.len());
}

#[tokio::test]
async fn test_memtable_prototype_time_filtering() {
    // Setup
    let temp_dir = TempDir::new().unwrap();
    let hot_config = HotTierConfig {
        max_age: Duration::from_minutes(5),
        max_entries: 1000,
    };
    let cold_config = ColdTierConfig {
        uri: format!("file://{}", temp_dir.path().display()),
        enable_bloom_filters: false,
        compression: CompressionCodec::Snappy,
        row_group_size: 1000,
        index_path: None,
    };

    let hot_tier = HotTier::new(hot_config);
    let cold_tier = ColdTier::new(cold_config).unwrap();

    let now = Timestamp::now().unwrap();
    let trace_id = TraceId::from_bytes([1; 16]);

    // Create spans at different times
    let spans = vec![
        // Recent span (in query range)
        sequins_core::models::Span {
            trace_id,
            span_id: SpanId::from_bytes([1; 8]),
            parent_span_id: None,
            service_name: "recent-service".to_string(),
            operation_name: "recent-op".to_string(),
            start_time: now - Duration::from_secs(30),
            end_time: now - Duration::from_secs(29),
            duration: Duration::from_secs(1),
            attributes: HashMap::new(),
            events: Vec::new(),
            span_kind: SpanKind::Internal,
            status: SpanStatus::Ok,
        },
        // Old span (outside query range)
        sequins_core::models::Span {
            trace_id,
            span_id: SpanId::from_bytes([2; 8]),
            parent_span_id: None,
            service_name: "old-service".to_string(),
            operation_name: "old-op".to_string(),
            start_time: now - Duration::from_hours(2),
            end_time: now - Duration::from_hours(2) + Duration::from_secs(1),
            duration: Duration::from_secs(1),
            attributes: HashMap::new(),
            events: Vec::new(),
            span_kind: SpanKind::Internal,
            status: SpanStatus::Ok,
        },
    ];

    // Insert recent span to hot tier, old span to cold tier
    hot_tier.insert_span(spans[0].clone()).unwrap();
    cold_tier.write_spans(vec![spans[1].clone()]).await.unwrap();

    // Query for last 1 minute only
    let query = TraceQuery {
        start_time: now - Duration::from_minutes(1),
        end_time: now,
        service: None,
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier
        .query_traces_memtable_prototype(&hot_tier, &query)
        .await
        .unwrap();

    // Should only get the recent span
    assert_eq!(results.len(), 1, "Should only get recent span");
    assert_eq!(
        results[0].service_name, "recent-service",
        "Should be the recent span"
    );

    println!("✓ MemTable prototype correctly filters by time range");
}

#[tokio::test]
async fn test_memtable_prototype_empty_hot_tier() {
    // Setup with empty hot tier
    let temp_dir = TempDir::new().unwrap();
    let hot_config = HotTierConfig {
        max_age: Duration::from_minutes(5),
        max_entries: 1000,
    };
    let cold_config = ColdTierConfig {
        uri: format!("file://{}", temp_dir.path().display()),
        enable_bloom_filters: false,
        compression: CompressionCodec::Snappy,
        row_group_size: 1000,
        index_path: None,
    };

    let hot_tier = HotTier::new(hot_config);
    let cold_tier = ColdTier::new(cold_config).unwrap();

    let now = Timestamp::now().unwrap();
    let trace_id = TraceId::from_bytes([1; 16]);

    // Only insert to cold tier
    let span = sequins_core::models::Span {
        trace_id,
        span_id: SpanId::from_bytes([1; 8]),
        parent_span_id: None,
        service_name: "cold-only".to_string(),
        operation_name: "test-op".to_string(),
        start_time: now - Duration::from_minutes(5),
        end_time: now - Duration::from_minutes(5) + Duration::from_secs(1),
        duration: Duration::from_secs(1),
        attributes: HashMap::new(),
        events: Vec::new(),
        span_kind: SpanKind::Internal,
        status: SpanStatus::Ok,
    };

    cold_tier.write_spans(vec![span.clone()]).await.unwrap();

    let query = TraceQuery {
        start_time: now - Duration::from_minutes(10),
        end_time: now,
        service: None,
        min_duration: None,
        max_duration: None,
        has_error: None,
        limit: Some(100),
    };

    let results = cold_tier
        .query_traces_memtable_prototype(&hot_tier, &query)
        .await
        .unwrap();

    // Should still work with empty hot tier
    assert_eq!(results.len(), 1, "Should get span from cold tier only");
    assert_eq!(results[0].service_name, "cold-only");

    println!("✓ MemTable prototype works with empty hot tier");
}
