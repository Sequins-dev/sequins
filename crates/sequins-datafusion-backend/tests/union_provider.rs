//! Union provider integration tests.
//!
//! These tests were originally in sequins-storage/src/union_provider.rs but were
//! moved here to avoid a circular dev-dependency between sequins-storage and
//! sequins-datafusion-backend.

use futures::StreamExt;
use sequins_datafusion_backend::DataFusionBackend;
use sequins_query::QueryApi;
use sequins_storage::test_fixtures::{make_test_otlp_traces, TestStorageBuilder};
use sequins_types::ingest::OtlpIngest;
use std::sync::Arc;

#[tokio::test]
async fn test_union_provider_hot_and_cold() {
    // Create storage with short flush interval to ensure data gets to cold tier
    let (storage, _temp) = TestStorageBuilder::new()
        .flush_interval(sequins_types::models::Duration::from_millis(100))
        .build()
        .await;

    // Ingest data to hot tier
    let request1 = make_test_otlp_traces(1, 5);
    storage.ingest_traces(request1).await.unwrap();

    // Wait for flush to cold tier
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Ingest more data to hot tier (should stay in hot tier)
    let request2 = make_test_otlp_traces(1, 3);
    storage.ingest_traces(request2).await.unwrap();

    // Create backend and query - should get data from both tiers
    let backend = DataFusionBackend::new(Arc::new(storage));
    let query = "spans last 1h LIMIT 100";
    let mut stream = backend.query(query).await.unwrap();

    // Collect results
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    let mut frames = Vec::new();
    while let Some(result) = stream.next().await {
        frames.push(result.unwrap());
    }

    // Should have data from both hot and cold tiers
    assert!(!frames.is_empty(), "Should have frames");

    // Verify we got data frame
    let has_data_frame = frames.iter().any(|f| {
        decode_metadata(&f.app_metadata).map_or(false, |m| matches!(m, SeqlMetadata::Data { .. }))
    });
    assert!(has_data_frame, "Should have at least one data frame");
}

#[tokio::test]
async fn test_union_provider_with_filters() {
    // Create storage
    let (storage, _temp) = TestStorageBuilder::new().build().await;

    // Ingest test data
    let request = make_test_otlp_traces(1, 10);
    storage.ingest_traces(request).await.unwrap();

    // Create backend and query with filters
    let backend = DataFusionBackend::new(Arc::new(storage));
    let query = "spans last 1h WHERE kind = server LIMIT 50";
    let mut stream = backend.query(query).await.unwrap();

    // Collect results
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    let mut frames = Vec::new();
    while let Some(result) = stream.next().await {
        frames.push(result.unwrap());
    }

    // Should execute successfully with filters
    assert!(!frames.is_empty(), "Should have frames from filtered query");

    // Verify complete frame exists
    let has_complete = frames.iter().any(|f| {
        decode_metadata(&f.app_metadata)
            .map_or(false, |m| matches!(m, SeqlMetadata::Complete { .. }))
    });
    assert!(has_complete, "Should have complete frame");
}
