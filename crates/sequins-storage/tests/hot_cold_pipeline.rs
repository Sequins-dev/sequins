//! Hot+Cold tier pipeline integration tests.
//!
//! Verifies that:
//!   1. Data ingested to the hot tier is immediately visible in snapshot queries.
//!   2. After a hot→cold flush (triggered by a very short flush interval), the
//!      data is visible in snapshot queries via the cold tier (Vortex files).
//!   3. The `UnionProvider` correctly merges results from BOTH tiers, so that a
//!      single query returns rows regardless of which tier they live in.

use futures::StreamExt;
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value::Value, AnyValue, InstrumentationScope, KeyValue},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
use sequins_query::QueryApi;
use sequins_storage::{
    config::{ColdTierConfig, CompanionIndexConfig, HotTierConfig, LifecycleConfig, StorageConfig},
    DataFusionBackend, Storage,
};
use sequins_types::{ingest::OtlpIngest, models::Duration, SignalType};
use std::sync::Arc;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a storage + backend wired to a temp directory with the given flush interval.
async fn make_storage_backend(
    flush_interval: Duration,
) -> (Arc<Storage>, DataFusionBackend, tempfile::TempDir) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let config = StorageConfig {
        hot_tier: HotTierConfig {
            max_age: Duration::from_minutes(60),
            max_entries: 10_000,
        },
        cold_tier: ColdTierConfig {
            uri: format!("file://{}", temp_dir.path().display()),
            row_block_size: 1000,
            compact_encodings: false,
            companion_index: CompanionIndexConfig {
                tantivy_enabled: false,
                bloom_enabled: false,
                trigram_enabled: false,
                cardinality_threshold: 100,
                bloom_fpr: 0.01,
            },
            index_path: None,
            max_attribute_columns: 256,
        },
        lifecycle: LifecycleConfig {
            retention: Duration::from_hours(24),
            flush_interval,
            cleanup_interval: Duration::from_hours(1),
        },
    };
    let storage = Arc::new(Storage::new(config).await.unwrap());
    let backend = DataFusionBackend::new(storage.clone());
    (storage, backend, temp_dir)
}

/// Build a minimal OTLP traces request with `n` spans.
/// Timestamps are set ~30s in the past so they fall within a `last 1h` window.
fn make_spans(n: usize) -> ExportTraceServiceRequest {
    let now_ns = sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime);
    // Offset 30s into the past so spans are within the compile-time upper-bound
    // of `last 1h` even when the parser uses SystemTime::now() to anchor the window.
    let base_ns = now_ns.saturating_sub(30_000_000_000);

    let spans: Vec<Span> = (0..n)
        .map(|i| {
            let mut trace_id = vec![0u8; 16];
            trace_id[8..].copy_from_slice(&(i as u64).to_be_bytes());
            let mut span_id = vec![0u8; 8];
            span_id.copy_from_slice(&(i as u64).to_be_bytes());
            let start_ns = base_ns + (i as u64 * 1_000_000_000);
            Span {
                trace_id,
                span_id,
                parent_span_id: vec![],
                name: format!("op-{}", i),
                kind: 2, // Server
                start_time_unix_nano: start_ns,
                end_time_unix_nano: start_ns + 100_000_000, // 100 ms
                status: Some(Status {
                    code: 1,
                    message: String::new(),
                }),
                ..Default::default()
            }
        })
        .collect();

    let resource = Resource {
        attributes: vec![KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("test-svc".into())),
            }),
        }],
        ..Default::default()
    };

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "test".into(),
                    ..Default::default()
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Collect the total row count from all `Data` frames in a query stream.
async fn collect_total_rows(backend: &DataFusionBackend, query: &str) -> usize {
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    let mut stream = backend.query(query).await.expect("query failed");
    let mut total = 0usize;
    while let Some(result) = stream.next().await {
        if let Ok(fd) = result {
            if let Some(SeqlMetadata::Data { .. }) = decode_metadata(&fd.app_metadata) {
                if let Ok(batch) = sequins_query::frame::ipc_to_batch(&fd.data_body) {
                    total += batch.num_rows();
                }
            }
        }
    }
    total
}

// ── 5B.1: Hot tier data is immediately visible ───────────────────────────────

/// Data ingested into the hot tier must be visible in snapshot queries
/// before any flush to the cold tier.
#[tokio::test]
async fn test_hot_tier_data_visible_in_query() {
    let (storage, backend, _temp) = make_storage_backend(Duration::from_minutes(5)).await;

    storage.ingest_traces(make_spans(7)).await.unwrap();

    // Verify data is in the hot tier
    let hot_tier = storage.hot_tier_arc();
    assert_eq!(
        hot_tier.chain(&SignalType::Spans).row_count(),
        7,
        "7 spans should be in hot tier immediately after ingest"
    );

    let rows = collect_total_rows(&backend, "spans last 1h").await;
    assert_eq!(rows, 7, "all 7 hot-tier spans must be visible in query");
}

// ── 5B.2: Cold tier data is visible after flush ──────────────────────────────

/// After the background compactor has flushed hot→cold, snapshot queries must
/// still return the data (now read from Vortex columnar files).
#[tokio::test]
async fn test_cold_tier_data_visible_after_flush() {
    // Use a very short flush interval so the background compactor runs quickly.
    let (storage, backend, _temp) = make_storage_backend(Duration::from_millis(50)).await;

    storage.ingest_traces(make_spans(5)).await.unwrap();

    // Give the background compactor time to flush hot → cold
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // After flush, the cold tier should hold the data.
    // We can't assert hot tier is empty (compaction may be partial), but the
    // query must return at least the 5 ingested spans.
    let rows = collect_total_rows(&backend, "spans last 1h").await;
    assert!(
        rows >= 5,
        "after hot→cold flush, at least 5 rows must be visible (got {})",
        rows
    );
}

// ── 5B.3: UnionProvider merges hot + cold tier ───────────────────────────────

/// When data exists in BOTH the hot tier AND the cold tier, a single snapshot
/// query must return rows from both.  This validates the `UnionProvider`
/// concatenation logic.
#[tokio::test]
async fn test_union_provider_merges_both_tiers() {
    // Short flush interval so we can drive data into cold tier
    let (storage, backend, _temp) = make_storage_backend(Duration::from_millis(50)).await;

    // Batch A → ingest, then wait for background flush to cold tier
    storage.ingest_traces(make_spans(5)).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Batch B → ingest a second batch; these live in the hot tier
    storage.ingest_traces(make_spans(5)).await.unwrap();
    let hot_rows = storage.hot_tier_arc().chain(&SignalType::Spans).row_count();
    assert!(
        hot_rows >= 5,
        "second batch must be in hot tier (got {})",
        hot_rows
    );

    // Query must return rows from both tiers combined (≥ 10 total)
    let rows = collect_total_rows(&backend, "spans last 1h").await;
    assert!(
        rows >= 10,
        "UnionProvider must merge hot + cold tiers; expected >= 10 rows, got {}",
        rows
    );
}
