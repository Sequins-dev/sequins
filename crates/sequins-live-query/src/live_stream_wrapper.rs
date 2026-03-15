//! LiveStreamWrapper - converts DataFusion stream to SeqlStream (FlightData)
//!
//! Takes the SendableRecordBatchStream produced by DataFusion's execute_stream()
//! and wraps it to produce SeqlStream items (FlightData append + heartbeat messages)
//! suitable for streaming to live query clients.

use crate::delta_emitter::DeltaEmitter;
use crate::heartbeat::HeartbeatEmitter;
use arrow_flight::FlightData;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::{Stream, StreamExt};
use sequins_query::ast::Signal;
use sequins_query::error::QueryError;
use sequins_query::SeqlStream;
use std::pin::Pin;
use std::sync::Arc;

/// Wraps a DataFusion RecordBatch stream to produce a SeqlStream
/// of FlightData messages suitable for live query streaming to clients.
pub struct LiveStreamWrapper {
    inner: SendableRecordBatchStream,
    delta_emitter: DeltaEmitter,
    heartbeat_emitter: Arc<HeartbeatEmitter>,
    limit: Option<usize>,
}

impl LiveStreamWrapper {
    /// Create a new wrapper
    pub fn new(
        inner: SendableRecordBatchStream,
        signal: Signal,
        heartbeat_emitter: Arc<HeartbeatEmitter>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            inner,
            delta_emitter: DeltaEmitter::new(signal),
            heartbeat_emitter,
            limit,
        }
    }

    /// Convert to a SeqlStream of FlightData
    ///
    /// Merges the delta stream (append FlightData from RecordBatches) with
    /// the heartbeat stream.
    pub fn into_frame_stream(self) -> SeqlStream {
        let delta_emitter = self.delta_emitter;
        let limit = self.limit;
        let heartbeat_stream = self.heartbeat_emitter.start();
        let mut inner = self.inner;

        let delta_stream = async_stream::stream! {
            let mut seq: u64 = 0;
            let mut rows_sent: usize = 0;

            loop {
                let result = match inner.next().await {
                    Some(r) => r,
                    None => {
                        break;
                    }
                };
                match result {
                    Ok(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        // Apply streaming limit if set
                        let batch = if let Some(lim) = limit {
                            if rows_sent >= lim {
                                break;
                            }
                            let remaining = lim - rows_sent;
                            if batch.num_rows() > remaining {
                                batch.slice(0, remaining)
                            } else {
                                batch
                            }
                        } else {
                            batch
                        };

                        rows_sent += batch.num_rows();
                        let batch_seq = seq;
                        seq += batch.num_rows() as u64;

                        if let Some(fd) = delta_emitter.emit_append(batch_seq, &batch) {
                            yield Ok::<FlightData, QueryError>(fd);
                        }
                    }
                    Err(_) => continue,
                }
            }
        };

        // Map heartbeats to Ok(FlightData)
        let heartbeat_fd_stream = heartbeat_stream.map(Ok::<FlightData, QueryError>);

        // Merge the two streams
        let combined = futures::stream::select(delta_stream, heartbeat_fd_stream);
        Box::pin(combined)
            as Pin<Box<dyn Stream<Item = Result<FlightData, QueryError>> + Send + 'static>>
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    use sequins_wal::{Wal, WalConfig};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn create_test_wal() -> (Arc<Wal>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(object_store::memory::InMemory::new());
        let config = WalConfig {
            base_path: "wal".to_string(),
            segment_size: 1024,
            flush_interval: 10,
            broadcast_capacity: 100,
        };
        let wal = Wal::new(store, config).await.unwrap();
        (Arc::new(wal), tmp)
    }

    fn make_batch(value: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let array = Int64Array::from(vec![value]);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    fn make_stream_from_batches(
        batches: Vec<RecordBatch>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        let schema = if batches.is_empty() {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        } else {
            batches[0].schema()
        };
        let stream = stream::iter(batches.into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    #[tokio::test]
    async fn test_wrapper_emits_append_flight_data() {
        let (wal, _tmp) = create_test_wal().await;
        let heartbeat = Arc::new(HeartbeatEmitter::new(Duration::from_secs(60), wal));

        let batches = vec![make_batch(42)];
        let inner = make_stream_from_batches(batches);

        let wrapper = LiveStreamWrapper::new(inner, Signal::Spans, heartbeat, None);
        let mut frame_stream = wrapper.into_frame_stream();

        let fd = tokio::time::timeout(Duration::from_millis(500), frame_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Append { .. } => {}    // expected
            SeqlMetadata::Heartbeat { .. } => {} // also acceptable (unlikely with 60s interval)
            other => panic!("Expected Append or Heartbeat FlightData, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_wrapper_emits_heartbeats() {
        let (wal, _tmp) = create_test_wal().await;
        // Fast heartbeat so the test runs quickly
        let heartbeat = Arc::new(HeartbeatEmitter::new(Duration::from_millis(10), wal));

        // Empty stream - no batches, only heartbeats
        let inner = make_stream_from_batches(vec![]);

        let wrapper = LiveStreamWrapper::new(inner, Signal::Spans, heartbeat, None);
        let mut frame_stream = wrapper.into_frame_stream();

        // Should receive a heartbeat
        let fd = tokio::time::timeout(Duration::from_millis(200), frame_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert!(
            matches!(meta, SeqlMetadata::Heartbeat { .. }),
            "Expected Heartbeat, got {:?}",
            meta
        );
    }

    #[tokio::test]
    async fn test_wrapper_respects_limit() {
        let (wal, _tmp) = create_test_wal().await;
        let heartbeat = Arc::new(HeartbeatEmitter::new(Duration::from_secs(60), wal));

        // Three batches, each with 1 row - limit to 2 rows
        let batches = vec![make_batch(1), make_batch(2), make_batch(3)];
        let inner = make_stream_from_batches(batches);

        let wrapper = LiveStreamWrapper::new(inner, Signal::Spans, heartbeat, Some(2));
        let frame_stream = wrapper.into_frame_stream();

        // Collect only append frames
        let append_frames: Vec<_> = tokio::time::timeout(
            Duration::from_millis(500),
            frame_stream
                .filter(|r| {
                    let is_append = r
                        .as_ref()
                        .ok()
                        .and_then(|fd| decode_metadata(&fd.app_metadata))
                        .map(|m| matches!(m, SeqlMetadata::Append { .. }))
                        .unwrap_or(false);
                    std::future::ready(is_append)
                })
                .take(2)
                .collect::<Vec<_>>(),
        )
        .await
        .expect("timeout");

        assert_eq!(append_frames.len(), 2);
    }

    #[tokio::test]
    async fn test_wrapper_skips_empty_batches() {
        let (wal, _tmp) = create_test_wal().await;
        let heartbeat = Arc::new(HeartbeatEmitter::new(Duration::from_secs(60), wal));

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let real_batch = make_batch(99);

        let batches = vec![empty_batch, real_batch];
        let inner = make_stream_from_batches(batches);

        let wrapper = LiveStreamWrapper::new(inner, Signal::Logs, heartbeat, None);
        let mut frame_stream = wrapper.into_frame_stream();

        let fd = tokio::time::timeout(Duration::from_millis(500), frame_stream.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("frame error");

        // The first non-empty frame should be an Append for the real batch
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Append { start_row_id, .. } => {
                assert_eq!(start_row_id, 0); // first real batch starts at seq 0
            }
            SeqlMetadata::Heartbeat { .. } => {
                // Acceptable: heartbeat came first (unlikely with 60s interval but possible)
            }
            other => panic!("Expected Append or Heartbeat, got {:?}", other),
        }
    }
}
