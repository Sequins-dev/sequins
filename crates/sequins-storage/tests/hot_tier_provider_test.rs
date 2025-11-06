//! Integration test for Phase 2: Custom HotTierTableProvider
//!
//! This test validates the production implementation of the custom TableProvider:
//! 1. Creates a HotTierTableProvider with test data
//! 2. Registers it with a DataFusion SessionContext
//! 3. Executes SQL queries against it
//! 4. Verifies results match expectations

use datafusion::prelude::*;
use sequins_core::models::{Duration, SpanId, SpanKind, SpanStatus, Timestamp, TraceId};
use sequins_storage::hot_tier_provider::HotTierTableProvider;
use sequins_storage::{config::HotTierConfig, HotTier};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_hot_tier_provider_basic_query() {
    // Setup hot tier with test data
    let hot_config = HotTierConfig {
        max_age: Duration::from_minutes(5),
        max_entries: 1000,
    };
    let hot_tier = Arc::new(HotTier::new(hot_config));

    let now = Timestamp::now().unwrap();
    let trace_id = TraceId::from_bytes([1; 16]);

    // Insert test spans
    let spans = vec![
        sequins_core::models::Span {
            trace_id,
            span_id: SpanId::from_bytes([1; 8]),
            parent_span_id: None,
            service_name: "test-service-1".to_string(),
            operation_name: "test-op-1".to_string(),
            start_time: now,
            end_time: now + Duration::from_secs(1),
            duration: Duration::from_secs(1),
            attributes: HashMap::new(),
            events: Vec::new(),
            span_kind: SpanKind::Server,
            status: SpanStatus::Ok,
        },
        sequins_core::models::Span {
            trace_id,
            span_id: SpanId::from_bytes([2; 8]),
            parent_span_id: None,
            service_name: "test-service-2".to_string(),
            operation_name: "test-op-2".to_string(),
            start_time: now + Duration::from_secs(1),
            end_time: now + Duration::from_secs(2),
            duration: Duration::from_secs(1),
            attributes: HashMap::new(),
            events: Vec::new(),
            span_kind: SpanKind::Client,
            status: SpanStatus::Ok,
        },
    ];

    for span in spans {
        hot_tier.insert_span(span).unwrap();
    }

    // Create DataFusion context and register custom TableProvider
    let ctx = SessionContext::new();
    let provider = HotTierTableProvider::new(hot_tier.clone());
    ctx.register_table("hot_spans", Arc::new(provider)).unwrap();

    // Execute SQL query
    let df = ctx
        .sql("SELECT service_name, operation_name FROM hot_spans ORDER BY service_name")
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Verify results
    assert!(results.len() >= 1, "Should have at least one RecordBatch");
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should have 2 spans total");

    // Debug: print schema and column info
    if results.is_empty() || results[0].num_rows() == 0 {
        panic!("No results returned from query");
    }

    println!("RecordBatch schema: {:?}", results[0].schema());
    println!("Number of columns: {}", results[0].num_columns());
    println!("Column 0 data type: {:?}", results[0].column(0).data_type());

    // Verify service names are correct
    use datafusion::arrow::array::StringArray;
    let service_names = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Column 0 should be a StringArray");

    assert_eq!(service_names.value(0), "test-service-1");
    assert_eq!(service_names.value(1), "test-service-2");

    println!("✓ HotTierTableProvider basic query successful");
}

#[tokio::test]
async fn test_hot_tier_provider_with_limit() {
    // Setup
    let hot_config = HotTierConfig {
        max_age: Duration::from_minutes(5),
        max_entries: 1000,
    };
    let hot_tier = Arc::new(HotTier::new(hot_config));

    let now = Timestamp::now().unwrap();
    let trace_id = TraceId::from_bytes([1; 16]);

    // Insert 5 test spans
    for i in 1..=5 {
        let span = sequins_core::models::Span {
            trace_id,
            span_id: SpanId::from_bytes([i; 8]),
            parent_span_id: None,
            service_name: format!("service-{}", i),
            operation_name: format!("op-{}", i),
            start_time: now + Duration::from_secs(i as i64),
            end_time: now + Duration::from_secs(i as i64 + 1),
            duration: Duration::from_secs(1),
            attributes: HashMap::new(),
            events: Vec::new(),
            span_kind: SpanKind::Internal,
            status: SpanStatus::Ok,
        };
        hot_tier.insert_span(span).unwrap();
    }

    // Register provider
    let ctx = SessionContext::new();
    let provider = HotTierTableProvider::new(hot_tier.clone());
    ctx.register_table("hot_spans", Arc::new(provider)).unwrap();

    // Query with LIMIT
    let df = ctx
        .sql("SELECT service_name FROM hot_spans LIMIT 3")
        .await
        .unwrap();

    let results = df.collect().await.unwrap();

    // Verify limit was applied
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should respect LIMIT clause");

    println!("✓ HotTierTableProvider respects LIMIT clause");
}

#[tokio::test]
async fn test_hot_tier_provider_empty() {
    // Setup empty hot tier
    let hot_config = HotTierConfig {
        max_age: Duration::from_minutes(5),
        max_entries: 1000,
    };
    let hot_tier = Arc::new(HotTier::new(hot_config));

    // Register provider
    let ctx = SessionContext::new();
    let provider = HotTierTableProvider::new(hot_tier.clone());
    ctx.register_table("hot_spans", Arc::new(provider)).unwrap();

    // Query empty table
    let df = ctx.sql("SELECT * FROM hot_spans").await.unwrap();
    let results = df.collect().await.unwrap();

    // Verify empty results
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 0, "Should return no rows for empty hot tier");

    println!("✓ HotTierTableProvider handles empty hot tier");
}
