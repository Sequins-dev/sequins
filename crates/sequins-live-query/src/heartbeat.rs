//! Heartbeat emitter for live queries
//!
//! Emits periodic FlightData heartbeat messages to keep the connection alive and
//! inform clients of the current WAL watermark.

use arrow_flight::FlightData;
use futures::stream::Stream;
use sequins_query::flight::heartbeat_flight_data;
use sequins_wal::Wal;
use std::sync::Arc;
use std::time::Duration;

/// Heartbeat emitter for live queries
pub struct HeartbeatEmitter {
    /// How often to emit heartbeats
    interval: Duration,
    /// WAL for reading current sequence number
    wal: Arc<Wal>,
}

impl HeartbeatEmitter {
    /// Create a new heartbeat emitter
    pub fn new(interval: Duration, wal: Arc<Wal>) -> Self {
        debug_assert!(
            !interval.is_zero(),
            "HeartbeatEmitter interval must be non-zero"
        );
        Self { interval, wal }
    }

    /// Start emitting heartbeats
    ///
    /// Returns a stream that yields `FlightData` heartbeat messages at the configured interval.
    /// The watermark in each message is the current WAL sequence number converted
    /// to nanoseconds (this is a logical watermark, not a real timestamp).
    pub fn start(&self) -> impl Stream<Item = FlightData> {
        let interval = self.interval;
        let wal = Arc::clone(&self.wal);

        async_stream::stream! {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;
                let watermark_ns = wal.current_seq();
                yield heartbeat_flight_data(watermark_ns);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    use sequins_wal::WalConfig;
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

    #[tokio::test]
    async fn test_heartbeat_emitter() {
        let (wal, _tmp) = create_test_wal().await;
        let emitter = HeartbeatEmitter::new(Duration::from_millis(10), wal);

        let mut stream = Box::pin(emitter.start());

        // Should receive first heartbeat quickly
        let fd = tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert!(matches!(meta, SeqlMetadata::Heartbeat { watermark_ns: 1 }));
    }

    #[tokio::test]
    async fn test_heartbeat_reflects_wal_progress() {
        use sequins_wal::WalPayload;

        let (wal, _tmp) = create_test_wal().await;

        // Append some entries to advance WAL
        wal.append(
            WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ),
            0,
        )
        .await
        .unwrap();
        wal.append(
            WalPayload::Logs(
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::default(
                ),
            ),
            0,
        )
        .await
        .unwrap();

        let emitter = HeartbeatEmitter::new(Duration::from_millis(10), wal);
        let mut stream = Box::pin(emitter.start());

        let fd = tokio::time::timeout(Duration::from_millis(100), stream.next())
            .await
            .expect("timeout")
            .expect("stream ended");

        // Should reflect current seq (2 entries appended, seq 1 and 2, next is 3)
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert!(matches!(meta, SeqlMetadata::Heartbeat { watermark_ns: 3 }));
    }

    #[tokio::test]
    async fn test_heartbeat_interval() {
        let (wal, _tmp) = create_test_wal().await;
        let emitter = HeartbeatEmitter::new(Duration::from_millis(50), wal);

        let mut stream = Box::pin(emitter.start());

        let start = tokio::time::Instant::now();
        let _hb1 = stream.next().await.unwrap();
        let _hb2 = stream.next().await.unwrap();
        let elapsed = start.elapsed();

        // Should take at least 50ms for second heartbeat
        assert!(elapsed >= Duration::from_millis(50));
        // But not too much longer (allow some scheduling slack)
        assert!(elapsed < Duration::from_millis(200));
    }
}
