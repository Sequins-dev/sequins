//! WAL → live query integration tests.
//!
//! Verifies that ingesting data causes a `DeltaFrame::Append` to arrive on an
//! active live query subscription.  The live query infrastructure connects the
//! WAL broadcast channel to the DataFusion live table provider, so this test
//! exercises the full ingest → WAL broadcast → DataFusion stream path.

use futures::StreamExt;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value::Value, AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use sequins_storage::{
    config::{ColdTierConfig, CompanionIndexConfig, HotTierConfig, LifecycleConfig, StorageConfig},
    DataFusionBackend, Storage,
};
use sequins_types::{ingest::OtlpIngest, models::Duration};
use std::sync::Arc;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn make_storage_backend() -> (Arc<Storage>, DataFusionBackend, tempfile::TempDir) {
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
            flush_interval: Duration::from_minutes(5),
            cleanup_interval: Duration::from_hours(1),
        },
    };
    let storage = Arc::new(Storage::new(config).await.unwrap());
    let backend = DataFusionBackend::new(storage.clone());
    (storage, backend, temp_dir)
}

/// Build an OTLP logs request with `n` log records.
/// Timestamps are anchored ~30s in the past so they fall within a `last 1h` window.
fn make_logs(n: usize) -> ExportLogsServiceRequest {
    let now_ns = sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime);
    let base_ns = now_ns.saturating_sub(30_000_000_000);

    let log_records: Vec<LogRecord> = (0..n)
        .map(|i| LogRecord {
            time_unix_nano: base_ns + (i as u64 * 1_000_000_000),
            observed_time_unix_nano: base_ns + (i as u64 * 1_000_000_000),
            severity_number: 9, // Info
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("test log {}", i))),
            }),
            flags: 1,
            ..Default::default()
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

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "test".into(),
                    ..Default::default()
                }),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

// Local intermediary types for test assertions (DeltaFrame/DeltaOp deleted from sequins_query)
struct DeltaFrame {
    watermark_ns: u64,
    ops: Vec<DeltaOp>,
}

enum DeltaOp {
    Append {
        start_row_id: u64,
        batch: arrow::record_batch::RecordBatch,
    },
    Update {
        row_id: u64,
        batch: arrow::record_batch::RecordBatch,
    },
    Expire {
        row_id: u64,
    },
    Replace {
        batch: arrow::record_batch::RecordBatch,
    },
}

/// Consume frames from a live query stream until we see a `DeltaFrame` or
/// until `timeout` milliseconds have elapsed.  Returns the first `DeltaFrame`
/// seen, or `None` on timeout.
async fn wait_for_delta(
    stream: &mut sequins_query::SeqlStream,
    timeout_ms: u64,
) -> Option<DeltaFrame> {
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    use sequins_query::frame::ipc_to_batch;
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);

    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let result = tokio::time::timeout(remaining, stream.next()).await;
        match result {
            Ok(Some(Ok(fd))) => {
                if let Some(meta) = decode_metadata(&fd.app_metadata) {
                    match meta {
                        SeqlMetadata::Append {
                            start_row_id,
                            watermark_ns,
                            ..
                        } => {
                            if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                                return Some(DeltaFrame {
                                    watermark_ns,
                                    ops: vec![DeltaOp::Append {
                                        start_row_id,
                                        batch,
                                    }],
                                });
                            }
                        }
                        SeqlMetadata::Replace { watermark_ns, .. } => {
                            if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                                return Some(DeltaFrame {
                                    watermark_ns,
                                    ops: vec![DeltaOp::Replace { batch }],
                                });
                            }
                        }
                        SeqlMetadata::Update {
                            row_id,
                            watermark_ns,
                            ..
                        } => {
                            if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                                return Some(DeltaFrame {
                                    watermark_ns,
                                    ops: vec![DeltaOp::Update { row_id, batch }],
                                });
                            }
                        }
                        SeqlMetadata::Expire {
                            row_id,
                            watermark_ns,
                            ..
                        } => {
                            return Some(DeltaFrame {
                                watermark_ns,
                                ops: vec![DeltaOp::Expire { row_id }],
                            });
                        }
                        _ => {} // Schema/Data/Heartbeat/Complete — keep waiting
                    }
                }
                continue;
            }
            Ok(Some(Err(_))) | Ok(None) => return None,
            Err(_) => return None, // timeout
        }
    }
}

// ── 5C.1: WAL broadcast triggers DeltaFrame::Append ─────────────────────────

/// Register a live subscription for logs, ingest log records, and verify that
/// a `DeltaFrame` with at least one `DeltaOp::Append` arrives on the stream.
///
/// This exercises the full path:
///   OTLP ingest → WAL broadcast → DataFusion live table → DeltaFrame
#[tokio::test]
async fn test_wal_broadcast_triggers_delta_frames() {
    let (storage, backend, _temp) = make_storage_backend().await;

    // Start a live query on logs before ingesting
    let mut stream = backend.query_live("logs last 1h").await.unwrap();

    // Consume historical frames (empty storage → just Schema + Complete/Heartbeat)
    // We collect until we see a Heartbeat or until the stream stalls briefly,
    // then proceed to ingest.
    let _ = tokio::time::timeout(tokio::time::Duration::from_millis(200), async {
        use sequins_query::flight::{decode_metadata, SeqlMetadata};
        while let Some(Ok(frame)) = stream.next().await {
            if let Some(meta) = decode_metadata(&frame.app_metadata) {
                if matches!(
                    meta,
                    SeqlMetadata::Heartbeat { .. } | SeqlMetadata::Complete { .. }
                ) {
                    break;
                }
            }
        }
    })
    .await;

    // Ingest log records into the running system
    storage.ingest_logs(make_logs(3)).await.unwrap();

    // A DeltaFrame with DeltaOp::Append must arrive within 2 seconds
    let delta = wait_for_delta(&mut stream, 2_000)
        .await
        .expect("expected a DeltaFrame::Append after ingesting logs but timed out after 2s");

    assert!(
        !delta.ops.is_empty(),
        "DeltaFrame must contain at least one op"
    );
    let has_append = delta
        .ops
        .iter()
        .any(|op| matches!(op, DeltaOp::Append { .. }));
    assert!(
        has_append,
        "DeltaFrame must contain a DeltaOp::Append; got ops: {:?}",
        delta
            .ops
            .iter()
            .map(std::mem::discriminant)
            .collect::<Vec<_>>()
    );
}
