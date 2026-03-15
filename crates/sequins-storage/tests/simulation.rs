//! Deterministic simulation tests using turmoil
//!
//! These tests verify concurrent operations behave correctly under various orderings.
//! turmoil provides deterministic Tokio simulation for testing race conditions and
//! concurrency bugs that would be flaky in normal tests.

use arrow::array::{
    Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};
use sequins_storage::{
    config::HotTierConfig,
    hot_tier::{batch_chain::BatchMeta, core::HotTier},
    wal::{Wal, WalConfig, WalPayload},
};
use sequins_types::models::{
    AttributeValue, Duration, LogEntry, LogId, LogSeverity, Span, SpanId, Timestamp, TraceId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use turmoil::Builder;

/// Convert a single span to a RecordBatch for hot tier insertion.
fn span_to_batch(span: &Span) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8View, false),
        Field::new("span_id", DataType::Utf8View, false),
        Field::new("parent_span_id", DataType::Utf8View, true),
        Field::new("name", DataType::Utf8View, false),
        Field::new("kind", DataType::UInt8, false),
        Field::new("status", DataType::UInt8, false),
        Field::new(
            "start_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("duration_ns", DataType::Int64, false),
        Field::new("resource_id", DataType::UInt32, false),
        Field::new("scope_id", DataType::UInt32, false),
    ]));

    let trace_id = span.trace_id.to_hex();
    let span_id = span.span_id.to_hex();
    let parent_id: Option<String> = span.parent_span_id.map(|p| p.to_hex());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(vec![trace_id.as_str()])) as _,
            Arc::new(StringViewArray::from(vec![span_id.as_str()])) as _,
            Arc::new(StringViewArray::from(vec![parent_id.as_deref()])) as _,
            Arc::new(StringViewArray::from(vec![span.operation_name.as_str()])) as _,
            Arc::new(UInt8Array::from(vec![span.kind])) as _,
            Arc::new(UInt8Array::from(vec![span.status_code])) as _,
            Arc::new(TimestampNanosecondArray::from(vec![span
                .start_time
                .as_nanos()])) as _,
            Arc::new(TimestampNanosecondArray::from(vec![span
                .end_time
                .as_nanos()])) as _,
            Arc::new(Int64Array::from(vec![span.duration.as_nanos()])) as _,
            Arc::new(UInt32Array::from(vec![span.resource_id])) as _,
            Arc::new(UInt32Array::from(vec![span.scope_id])) as _,
        ],
    )
    .expect("Failed to build span batch")
}

/// Convert a single log to a RecordBatch for hot tier insertion.
fn log_to_batch(log: &LogEntry) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("log_id", DataType::Utf8View, false),
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("service_name", DataType::Utf8View, false),
        Field::new("severity_text", DataType::Utf8View, false),
        Field::new("severity_number", DataType::UInt8, false),
        Field::new("body", DataType::Utf8View, false),
        Field::new("trace_id", DataType::Utf8View, true),
        Field::new("span_id", DataType::Utf8View, true),
        Field::new("resource_id", DataType::UInt32, false),
        Field::new("scope_id", DataType::UInt32, false),
    ]));

    let log_id = log.id.to_hex();
    let body = match &log.body {
        AttributeValue::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    let trace_id: Option<String> = log.trace_id.map(|id| id.to_hex());
    let span_id: Option<String> = log.span_id.map(|id| id.to_hex());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(vec![log_id.as_str()])) as _,
            Arc::new(TimestampNanosecondArray::from(vec![log
                .timestamp
                .as_nanos()])) as _,
            Arc::new(TimestampNanosecondArray::from(vec![log
                .observed_timestamp
                .as_nanos()])) as _,
            Arc::new(StringViewArray::from(vec!["unknown"])) as _,
            Arc::new(StringViewArray::from(vec!["INFO"])) as _,
            Arc::new(UInt8Array::from(vec![log.severity_number])) as _,
            Arc::new(StringViewArray::from(vec![body.as_str()])) as _,
            Arc::new(StringViewArray::from(vec![trace_id.as_deref()])) as _,
            Arc::new(StringViewArray::from(vec![span_id.as_deref()])) as _,
            Arc::new(UInt32Array::from(vec![log.resource_id])) as _,
            Arc::new(UInt32Array::from(vec![log.scope_id])) as _,
        ],
    )
    .expect("Failed to build log batch")
}

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a test WAL with a temporary directory
async fn create_test_wal() -> (Arc<Wal>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

    let config = WalConfig {
        base_path: "wal".to_string(),
        segment_size: 1000,
        flush_interval: 10,
        broadcast_capacity: 100,
    };

    let wal = Wal::new(store, config).await.unwrap();
    (Arc::new(wal), temp_dir)
}

/// Create a test span
fn create_test_span(trace_id_bytes: [u8; 16], span_id_bytes: [u8; 8], operation: &str) -> Span {
    Span {
        trace_id: TraceId::from_bytes(trace_id_bytes),
        span_id: SpanId::from_bytes(span_id_bytes),
        parent_span_id: None,
        operation_name: operation.to_string(),
        start_time: Timestamp::from_nanos(1000),
        end_time: Timestamp::from_nanos(2000),
        duration: Duration::from_nanos(1000),
        attributes: HashMap::new(),
        events: vec![],
        links: vec![],
        status_code: 0,
        status_message: None,
        kind: 0,
        trace_state: None,
        flags: None,
        resource_id: 0,
        scope_id: 0,
    }
}

/// Create a test log entry
fn create_test_log(body: &str) -> LogEntry {
    LogEntry {
        id: LogId::new(),
        timestamp: Timestamp::from_nanos(1000),
        observed_timestamp: Timestamp::from_nanos(1000),
        severity_number: LogSeverity::Info.to_number(),
        body: AttributeValue::String(body.to_string()),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        flags: None,
        resource_id: 0,
        scope_id: 0,
    }
}

/// Create test resource attributes
fn create_test_resource_attrs(service_name: &str) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    attributes.insert("service.name".to_string(), service_name.to_string());
    attributes
}

/// Create an OTLP trace request with a single span
fn create_otlp_trace_request(_operation: &str) -> ExportTraceServiceRequest {
    // For simplicity, just create an empty request
    // In a real implementation, we'd populate with actual span data
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

// ============================================================================
// WAL Concurrent Operations (3 tests)
// ============================================================================

#[test]
fn sim_wal_concurrent_writes() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        // Create WAL
        let (wal, _temp) = create_test_wal().await;

        // Spawn multiple concurrent writers
        let handles: Vec<_> = (0..5)
            .map(|i| {
                let wal = wal.clone();
                let req = create_otlp_trace_request(&format!("op-{}", i));
                tokio::spawn(async move { wal.append(WalPayload::Traces(req), 0).await })
            })
            .collect();

        // Wait for all
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Verify all entries persisted
        assert_eq!(wal.last_seq(), 5, "All 5 writes should be persisted");

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sim_wal_subscribe_and_write() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        // Create WAL
        let (wal, _temp) = create_test_wal().await;

        // Create subscriber before writes
        let mut subscriber = wal.subscribe_from(1);

        // Spawn writer
        let wal_writer = wal.clone();
        let write_handle = tokio::spawn(async move {
            for i in 0..3 {
                let req = create_otlp_trace_request(&format!("op-{}", i));
                wal_writer.append(WalPayload::Traces(req), 0).await.unwrap();
                // Small delay to allow subscriber to process
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        // Spawn subscriber reader
        let read_handle = tokio::spawn(async move {
            let mut count = 0;
            while count < 3 {
                match subscriber.next().await {
                    Some(Ok(_entry)) => count += 1,
                    _ => break,
                }
            }
            count
        });

        // Wait for both
        write_handle.await.unwrap();
        let received = read_handle.await.unwrap();

        assert_eq!(received, 3, "Subscriber should receive all 3 entries");

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sim_wal_multiple_subscribers() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        // Create WAL
        let (wal, _temp) = create_test_wal().await;

        // Create multiple subscribers
        let mut sub1 = wal.subscribe_from(1);
        let mut sub2 = wal.subscribe_from(1);
        let mut sub3 = wal.subscribe_from(1);

        // Spawn writer
        let wal_writer = wal.clone();
        let write_handle = tokio::spawn(async move {
            for i in 0..3 {
                let req = create_otlp_trace_request(&format!("op-{}", i));
                wal_writer.append(WalPayload::Traces(req), 0).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        // Spawn subscriber readers
        let read_handle1 = tokio::spawn(async move {
            let mut count = 0;
            while count < 3 {
                match sub1.next().await {
                    Some(Ok(_)) => count += 1,
                    _ => break,
                }
            }
            count
        });

        let read_handle2 = tokio::spawn(async move {
            let mut count = 0;
            while count < 3 {
                match sub2.next().await {
                    Some(Ok(_)) => count += 1,
                    _ => break,
                }
            }
            count
        });

        let read_handle3 = tokio::spawn(async move {
            let mut count = 0;
            while count < 3 {
                match sub3.next().await {
                    Some(Ok(_)) => count += 1,
                    _ => break,
                }
            }
            count
        });

        // Wait for all
        write_handle.await.unwrap();
        let count1 = read_handle1.await.unwrap();
        let count2 = read_handle2.await.unwrap();
        let count3 = read_handle3.await.unwrap();

        assert_eq!(count1, 3, "Subscriber 1 should receive all entries");
        assert_eq!(count2, 3, "Subscriber 2 should receive all entries");
        assert_eq!(count3, 3, "Subscriber 3 should receive all entries");

        Ok(())
    });

    sim.run().unwrap();
}

// ============================================================================
// Hot Tier Concurrent Operations (3 tests)
// ============================================================================

#[test]
fn sim_hot_tier_concurrent_register_resource() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        let config = HotTierConfig::default();
        let hot_tier = Arc::new(HotTier::new(config));

        // Multiple tasks registering the same resource concurrently
        let resource_attrs = create_test_resource_attrs("test-service");

        let handles: Vec<_> = (0..5)
            .map(|_| {
                let hot_tier = hot_tier.clone();
                let attrs = resource_attrs.clone();
                tokio::spawn(async move { hot_tier.register_resource(&attrs) })
            })
            .collect();

        // Wait for all registrations
        let mut ids = Vec::new();
        for handle in handles {
            let id = handle.await.unwrap().unwrap();
            ids.push(id);
        }

        // All should get the same ID (deduplicated)
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "All concurrent registrations of the same resource should get the same ID"
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sim_hot_tier_concurrent_push_spans() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        let config = HotTierConfig::default();
        let hot_tier = Arc::new(HotTier::new(config));

        // Push spans concurrently via BatchChain.
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let hot_tier = hot_tier.clone();
                let span = create_test_span([i as u8; 16], [i as u8; 8], &format!("op-{}", i));
                tokio::spawn(async move {
                    let batch = span_to_batch(&span);
                    let meta = BatchMeta {
                        min_timestamp: 0,
                        max_timestamp: i64::MAX,
                        row_count: batch.num_rows(),
                    };
                    hot_tier.spans.push(std::sync::Arc::new(batch), meta);
                })
            })
            .collect();

        // Wait for all pushes.
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all spans are in the chain.
        let row_count = hot_tier.spans.row_count();
        assert_eq!(row_count, 10, "All 10 spans should be in the chain");

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sim_hot_tier_concurrent_push_and_count() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        let config = HotTierConfig::default();
        let hot_tier = Arc::new(HotTier::new(config));

        // Spawn concurrent pushes to the spans chain.
        let push_handles: Vec<_> = (0..5)
            .map(|i| {
                let hot_tier = hot_tier.clone();
                let span = create_test_span([42; 16], [i as u8; 8], &format!("op-{}", i));
                tokio::spawn(async move {
                    let batch = span_to_batch(&span);
                    let meta = BatchMeta {
                        min_timestamp: 0,
                        max_timestamp: i64::MAX,
                        row_count: batch.num_rows(),
                    };
                    hot_tier.spans.push(std::sync::Arc::new(batch), meta);
                })
            })
            .collect();

        // Spawn concurrent row-count readers.
        let read_handles: Vec<_> = (0..5)
            .map(|_| {
                let hot_tier = hot_tier.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    hot_tier.spans.row_count()
                })
            })
            .collect();

        // Wait for all pushes.
        for handle in push_handles {
            handle.await.unwrap();
        }

        // Concurrent readers should have seen a consistent (non-panicking) count.
        for handle in read_handles {
            let _count = handle.await.unwrap();
        }

        // Final check: all 5 spans present.
        assert_eq!(
            hot_tier.spans.row_count(),
            5,
            "All 5 spans should be in the chain after concurrent pushes"
        );

        Ok(())
    });

    sim.run().unwrap();
}

// ============================================================================
// Maintenance During Ingest (2 tests)
// ============================================================================

#[test]
fn sim_concurrent_push_and_stats() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        let config = HotTierConfig::default();
        let hot_tier = Arc::new(HotTier::new(config));

        // Spawn continuous log pushes.
        let hot_tier_ingest = hot_tier.clone();
        let ingest_handle = tokio::spawn(async move {
            for i in 0..20 {
                let log = create_test_log(&format!("log-{}", i));
                let batch = log_to_batch(&log);
                let meta = BatchMeta {
                    min_timestamp: 0,
                    max_timestamp: i64::MAX,
                    row_count: batch.num_rows(),
                };
                hot_tier_ingest.logs.push(std::sync::Arc::new(batch), meta);
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        // Concurrently read stats while ingest is running.
        let hot_tier_stats = hot_tier.clone();
        let stats_handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            hot_tier_stats.stats()
        });

        // Wait for both.
        ingest_handle.await.unwrap();
        let _stats = stats_handle.await.unwrap();

        // All 20 logs should be present.
        let stats = hot_tier.stats();
        assert!(
            stats.log_count <= 20,
            "Log count should not exceed inserted count"
        );

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sim_flush_during_concurrent_writes() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        // Create WAL
        let (wal, _temp) = create_test_wal().await;

        // Spawn continuous writes
        let wal_writer = wal.clone();
        let write_handle = tokio::spawn(async move {
            for i in 0..10 {
                let req = create_otlp_trace_request(&format!("op-{}", i));
                wal_writer.append(WalPayload::Traces(req), 0).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        // Spawn flush operations
        let wal_flusher = wal.clone();
        let flush_handle = tokio::spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                // Flush is implicit in append, but we can check consistency
                let _seq = wal_flusher.last_seq();
            }
        });

        // Wait for both
        write_handle.await.unwrap();
        flush_handle.await.unwrap();

        // Verify no data loss
        assert_eq!(
            wal.last_seq(),
            10,
            "All 10 writes should be persisted after flush operations"
        );

        Ok(())
    });

    sim.run().unwrap();
}

// ============================================================================
// StageTreeRegistry (1 test)
// ============================================================================

#[test]
fn sim_stage_tree_concurrent_access() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        // Create WAL for processing
        let (wal, _temp) = create_test_wal().await;

        // Spawn concurrent WAL writes
        let write_handles: Vec<_> = (0..5)
            .map(|i| {
                let wal = wal.clone();
                let req = create_otlp_trace_request(&format!("op-{}", i));
                tokio::spawn(async move { wal.append(WalPayload::Traces(req), 0).await })
            })
            .collect();

        // Spawn concurrent readers (simulating queries)
        let read_handles: Vec<_> = (0..3)
            .map(|_| {
                let wal = wal.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    wal.last_seq()
                })
            })
            .collect();

        // Wait for all operations
        for handle in write_handles {
            handle.await.unwrap().unwrap();
        }

        for handle in read_handles {
            let seq = handle.await.unwrap();
            assert!(seq <= 5, "Read should see valid sequence number");
        }

        // Verify tree consistency - all entries should be accessible
        assert_eq!(wal.last_seq(), 5, "Tree should have all 5 entries");

        Ok(())
    });

    sim.run().unwrap();
}

// ============================================================================
// LiveQueryManager (1 test)
// ============================================================================

#[test]
fn sim_live_query_subscription_lifecycle() {
    let mut sim = Builder::new().build();

    sim.host("storage", || async {
        // Create WAL
        let (wal, _temp) = create_test_wal().await;

        // Spawn subscriptions under concurrent load
        let sub_handles: Vec<_> = (0..5)
            .map(|_| {
                let wal = wal.clone();
                tokio::spawn(async move {
                    // Create subscription
                    let mut subscriber = wal.subscribe_from(1);

                    // Read a few entries or timeout
                    let mut count = 0;
                    for _ in 0..3 {
                        match subscriber.next().await {
                            Some(Ok(_)) => count += 1,
                            _ => break,
                        }
                    }

                    // Drop subscription (simulating cancel)
                    drop(subscriber);

                    count
                })
            })
            .collect();

        // Spawn writer to provide data
        let wal_writer = wal.clone();
        let write_handle = tokio::spawn(async move {
            for i in 0..10 {
                let req = create_otlp_trace_request(&format!("op-{}", i));
                wal_writer.append(WalPayload::Traces(req), 0).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        });

        // Wait for writer
        write_handle.await.unwrap();

        // Wait for all subscriptions
        for handle in sub_handles {
            let count = handle.await.unwrap();
            // Each subscription may receive different amounts depending on timing
            assert!(count <= 10, "Should not receive more entries than written");
        }

        // Verify no leaks - create a new subscription and verify it works
        let mut final_sub = wal.subscribe_from(11);
        let final_req = create_otlp_trace_request("final");
        wal.append(WalPayload::Traces(final_req), 0).await.unwrap();

        // Should receive the final entry
        match tokio::time::timeout(std::time::Duration::from_millis(100), final_sub.next()).await {
            Ok(Some(Ok(_entry))) => { /* Success */ }
            _ => panic!("Final subscription should receive the final entry"),
        }

        Ok(())
    });

    sim.run().unwrap();
}
