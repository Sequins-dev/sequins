use crate::entry::WalEntry;
use crate::error::Result;
use crate::payload::WalPayload;
use crate::segment::{WalSegment, WalSegmentMeta};
use crate::subscriber::WalSubscriber;
use crate::writer::{WalWriter, WriterConfig};
use object_store::ObjectStore;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Configuration for the WAL
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Base path for WAL segments in object store
    pub base_path: String,
    /// Maximum entries per segment before rotating
    pub segment_size: u64,
    /// Flush to object store every N entries
    pub flush_interval: usize,
    /// Broadcast channel capacity for live subscribers
    pub broadcast_capacity: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            base_path: "sequins".to_string(),
            segment_size: 10_000,
            flush_interval: 100,
            broadcast_capacity: 1024,
        }
    }
}

/// Main WAL coordinator managing append, broadcast, and compaction
pub struct Wal {
    /// Monotonic sequence counter
    seq: AtomicU64,
    /// Buffered writer for active segment
    writer: Arc<RwLock<WalWriter>>,
    /// Segment metadata registry
    segments: Arc<RwLock<Vec<WalSegmentMeta>>>,
    /// Broadcast channel for live subscribers
    broadcast: broadcast::Sender<Arc<WalEntry>>,
    /// Object store backend
    store: Arc<dyn ObjectStore>,
}

impl Wal {
    /// Create a new WAL instance
    pub async fn new(store: Arc<dyn ObjectStore>, config: WalConfig) -> Result<Self> {
        // Discover existing segments
        let segments = Self::discover_segments(store.clone(), &config.base_path).await?;

        // Determine starting sequence and segment ID
        let (start_seq, start_segment_id) = if let Some(last_seg) = segments.last() {
            (last_seg.end_seq + 1, last_seg.segment_id + 1)
        } else {
            (1, 1)
        };

        // Create writer
        let writer_config = WriterConfig {
            segment_size: config.segment_size,
            flush_interval: config.flush_interval,
        };
        let writer = WalWriter::new(
            store.clone(),
            config.base_path.clone(),
            start_segment_id,
            start_seq,
            writer_config,
        );

        // Create broadcast channel
        let (broadcast, _) = broadcast::channel(config.broadcast_capacity);

        Ok(Self {
            seq: AtomicU64::new(start_seq),
            writer: Arc::new(RwLock::new(writer)),
            segments: Arc::new(RwLock::new(segments)),
            broadcast,
            store,
        })
    }

    /// Append a payload to the WAL, returning the assigned sequence number.
    ///
    /// `timestamp_ns` is the current wall-clock time in nanoseconds since UNIX
    /// epoch.  Pass `clock.now_ns()` from your `NowTime` provider rather than
    /// calling `SystemTime::now()` directly.
    pub async fn append(&self, payload: WalPayload, timestamp_ns: u64) -> Result<u64> {
        // Allocate sequence number
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Create entry with caller-provided timestamp
        let entry = WalEntry::new(seq, timestamp_ns, payload);

        // Write to segment
        let mut writer = self.writer.write().await;
        writer.append(&entry).await?;

        // Update segment metadata
        let current_meta = writer.current_segment_meta().clone();
        let mut segments = self.segments.write().await;

        // Check if this is a new segment
        if let Some(last) = segments.last_mut() {
            if last.segment_id == current_meta.segment_id {
                // Update existing segment
                last.end_seq = current_meta.end_seq;
            } else {
                // New segment rotated
                segments.push(current_meta);
            }
        } else {
            // First segment
            segments.push(current_meta);
        }
        drop(segments);
        drop(writer);

        // Broadcast to live subscribers
        let arc_entry = Arc::new(entry);
        let _ = self.broadcast.send(arc_entry); // Ignore error if no subscribers

        Ok(seq)
    }

    /// Create a subscriber starting from a given sequence number
    pub fn subscribe_from(&self, start_seq: u64) -> WalSubscriber {
        let rx = self.broadcast.subscribe();
        WalSubscriber::new(start_seq, rx)
    }

    /// Get the last assigned sequence number
    pub fn last_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst).saturating_sub(1)
    }

    /// Get the current sequence number (next to be assigned)
    pub fn current_seq(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }

    /// Read historical entries from segments in a range
    pub async fn read_range(&self, start_seq: u64, end_seq: u64) -> Result<Vec<WalEntry>> {
        let segments = self.segments.read().await;
        let mut entries = Vec::new();

        for meta in segments.iter() {
            // Check if this segment overlaps the requested range
            if meta.end_seq >= start_seq && meta.start_seq <= end_seq {
                let segment = WalSegment::new(meta.clone(), self.store.clone());
                let segment_entries = segment.read_range(start_seq, end_seq).await?;
                entries.extend(segment_entries);
            }
        }

        // Sort by sequence number (segments might overlap during rotation)
        entries.sort_by_key(|e| e.seq);

        Ok(entries)
    }

    /// Compact (delete) all segments before a given sequence number
    pub async fn compact_before(&self, seq: u64) -> Result<usize> {
        let mut segments = self.segments.write().await;
        let mut deleted_count = 0;

        // Find segments that can be deleted (end_seq < seq)
        let (to_delete, to_keep): (Vec<_>, Vec<_>) = segments
            .iter()
            .cloned()
            .partition(|meta| meta.end_seq < seq);

        // Delete old segments
        for meta in &to_delete {
            let segment = WalSegment::new(meta.clone(), self.store.clone());
            segment.delete().await?;
            deleted_count += 1;
        }

        // Update segments list
        *segments = to_keep;

        Ok(deleted_count)
    }

    /// Flush the current writer to ensure durability
    pub async fn flush(&self) -> Result<()> {
        let mut writer = self.writer.write().await;
        writer.flush().await
    }

    /// Discover existing segments from object store
    async fn discover_segments(
        store: Arc<dyn ObjectStore>,
        base_path: &str,
    ) -> Result<Vec<WalSegmentMeta>> {
        use futures::StreamExt;
        use object_store::path::Path;

        let wal_path = Path::from(format!("{}/wal", base_path));
        let mut segments = Vec::new();

        // Get the stream directly (list() returns a stream, not a future)
        let mut stream = store.list(Some(&wal_path));

        // Note: InMemory store might return empty stream
        while let Some(item) = stream.next().await {
            let meta = match item {
                Ok(m) => m,
                Err(_) => continue, // Skip errors
            };

            let path_str = meta.location.to_string();

            // Parse segment_id from filename: segment_00000001.wal
            if let Some(filename) = path_str.split('/').next_back() {
                if let Some(id_str) = filename
                    .strip_prefix("segment_")
                    .and_then(|s| s.strip_suffix(".wal"))
                {
                    if let Ok(segment_id) = id_str.parse::<u64>() {
                        // Read the segment to get start/end seq
                        let segment_meta = WalSegmentMeta::new(segment_id, 0, base_path);
                        let segment = WalSegment::new(segment_meta, store.clone());

                        // Read all entries to find start/end
                        match segment.read_range(0, u64::MAX).await {
                            Ok(entries) => {
                                if let (Some(first), Some(last)) = (entries.first(), entries.last())
                                {
                                    segments.push(WalSegmentMeta {
                                        segment_id,
                                        start_seq: first.seq,
                                        end_seq: last.seq,
                                        path: meta.location,
                                    });
                                }
                            }
                            Err(_) => {
                                // Skip corrupted segments
                                continue;
                            }
                        }
                    }
                }
            }
        }

        // Sort by segment_id
        segments.sort_by_key(|s| s.segment_id);

        Ok(segments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn wal_append_and_last_seq() {
        let store = Arc::new(InMemory::new());
        let config = WalConfig::default();
        let wal = Wal::new(store, config).await.unwrap();

        let payload = WalPayload::Traces(
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
        );
        let seq1 = wal.append(payload.clone(), 0).await.unwrap();
        let seq2 = wal.append(payload, 0).await.unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(wal.last_seq(), 2);
    }

    #[tokio::test]
    async fn wal_subscribe_from() {
        let store = Arc::new(InMemory::new());
        let config = WalConfig::default();
        let wal = Wal::new(store, config).await.unwrap();

        let mut subscriber = wal.subscribe_from(1);

        // Append entries
        let payload = WalPayload::Logs(
            opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::default(),
        );
        wal.append(payload.clone(), 0).await.unwrap();
        wal.append(payload, 0).await.unwrap();

        // Receive from subscriber
        let entry1 = subscriber.next().await.unwrap().unwrap();
        assert_eq!(entry1.seq, 1);

        let entry2 = subscriber.next().await.unwrap().unwrap();
        assert_eq!(entry2.seq, 2);
    }

    #[tokio::test]
    async fn wal_read_range() {
        let store = Arc::new(InMemory::new());
        let config = WalConfig::default();
        let wal = Wal::new(store, config).await.unwrap();

        // Append multiple entries
        for _ in 0..5 {
            wal.append(WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ), 0)
            .await
            .unwrap();
        }

        wal.flush().await.unwrap();

        // Read range
        let entries = wal.read_range(2, 4).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].seq, 2);
        assert_eq!(entries[1].seq, 3);
        assert_eq!(entries[2].seq, 4);
    }

    #[tokio::test]
    async fn wal_compact_before() {
        let store = Arc::new(InMemory::new());
        let config = WalConfig {
            segment_size: 3, // Small segments for testing
            ..Default::default()
        };
        let wal = Wal::new(store, config).await.unwrap();

        // Write enough to create multiple segments
        for _ in 0..10 {
            wal.append(WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ), 0)
            .await
            .unwrap();
        }

        wal.flush().await.unwrap();

        // Compact before seq 7 (should delete first 2 segments)
        let deleted = wal.compact_before(7).await.unwrap();
        assert!(deleted >= 1); // At least one segment should be deleted

        // Reading old entries should return empty
        let entries = wal.read_range(1, 3).await.unwrap();
        assert!(entries.is_empty());

        // Reading newer entries should still work
        let entries = wal.read_range(8, 10).await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[tokio::test]
    async fn wal_concurrent_append_and_subscribe() {
        let store = Arc::new(InMemory::new());
        let config = WalConfig::default();
        let wal = Arc::new(Wal::new(store, config).await.unwrap());

        let mut subscriber = wal.subscribe_from(1);

        // Spawn writer task
        let wal_clone = wal.clone();
        let writer_handle = tokio::spawn(async move {
            for _ in 0..10 {
                wal_clone
                    .append(WalPayload::Traces(
                        opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
                    ), 0)
                    .await
                    .unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        });

        // Read from subscriber
        let mut received = 0;
        let timeout = tokio::time::sleep(tokio::time::Duration::from_secs(2));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                Some(Ok(entry)) = subscriber.next() => {
                    assert_eq!(entry.seq, received + 1);
                    received += 1;
                    if received == 10 {
                        break;
                    }
                }
                _ = &mut timeout => {
                    panic!("Timeout waiting for entries");
                }
            }
        }

        writer_handle.await.unwrap();
        assert_eq!(received, 10);
    }

    #[tokio::test]
    async fn wal_subscriber_from_middle() {
        let store = Arc::new(InMemory::new());
        let config = WalConfig::default();
        let wal = Wal::new(store, config).await.unwrap();

        // Append 5 entries
        for _ in 0..5 {
            wal.append(WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ), 0)
            .await
            .unwrap();
        }

        // Subscribe from seq 3
        let mut subscriber = wal.subscribe_from(3);

        // Append more entries
        wal.append(WalPayload::Traces(
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
        ), 0)
        .await
        .unwrap();
        wal.append(WalPayload::Traces(
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
        ), 0)
        .await
        .unwrap();

        // Should receive entries starting from seq 6 (not 3, since 3-5 already passed)
        let entry = subscriber.next().await.unwrap().unwrap();
        assert_eq!(entry.seq, 6);
    }

    #[tokio::test]
    async fn wal_flush_durability() {
        let store = Arc::new(InMemory::new());
        let config = WalConfig {
            flush_interval: 1000, // High interval to avoid auto-flush
            ..Default::default()
        };
        let wal = Wal::new(store.clone(), config).await.unwrap();

        // Append without flushing
        wal.append(WalPayload::Traces(
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
        ), 0)
        .await
        .unwrap();

        // Explicit flush
        wal.flush().await.unwrap();

        // Should be able to read after flush
        let entries = wal.read_range(1, 1).await.unwrap();
        assert_eq!(entries.len(), 1);
    }

    // ── Recovery tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_wal_recovery_from_persisted_segments() {
        // Shared InMemory store that persists across WAL instances
        let store = Arc::new(InMemory::new());

        // First WAL: append 3 entries and flush to the object store
        {
            let config = WalConfig {
                flush_interval: 1, // flush after every entry
                ..Default::default()
            };
            let wal = Wal::new(store.clone(), config).await.unwrap();
            for _ in 0..3 {
                wal.append(WalPayload::Traces(
                    opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
                ), 0)
                .await
                .unwrap();
            }
            wal.flush().await.unwrap();
            assert_eq!(wal.last_seq(), 3);
            // WAL is dropped here, but store keeps the segment data
        }

        // Second WAL: opens on the same store and should discover the existing segments
        let config = WalConfig {
            flush_interval: 1,
            ..Default::default()
        };
        let recovered_wal = Wal::new(store.clone(), config).await.unwrap();

        // After recovery, last_seq() should reflect the highest persisted sequence
        assert_eq!(
            recovered_wal.last_seq(),
            3,
            "last_seq should be preserved after recovery"
        );

        // New appends should continue from seq 4
        let seq = recovered_wal
            .append(WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ), 0)
            .await
            .unwrap();
        assert_eq!(
            seq, 4,
            "recovered WAL should continue from next seq after last"
        );
    }

    #[tokio::test]
    async fn test_wal_recovery_empty_store() {
        // A fresh store has no segments — WAL should start from seq 1
        let store = Arc::new(InMemory::new());
        let config = WalConfig::default();
        let wal = Wal::new(store, config).await.unwrap();

        // No appends yet: last_seq() = 0 (saturating_sub from starting seq=1)
        assert_eq!(
            wal.last_seq(),
            0,
            "fresh WAL should start at seq 0 (no entries)"
        );

        // current_seq() is the next to be assigned
        assert_eq!(wal.current_seq(), 1, "fresh WAL next seq should be 1");
    }

    #[tokio::test]
    async fn test_wal_subscriber_after_recovery() {
        let store = Arc::new(InMemory::new());

        // First WAL: write entries and flush
        {
            let config = WalConfig {
                flush_interval: 1,
                ..Default::default()
            };
            let wal = Wal::new(store.clone(), config).await.unwrap();
            for _ in 0..5 {
                wal.append(WalPayload::Traces(
                    opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
                ), 0)
                .await
                .unwrap();
            }
            wal.flush().await.unwrap();
        }

        // Second WAL: recover and subscribe
        let config = WalConfig {
            flush_interval: 1,
            ..Default::default()
        };
        let recovered_wal = Wal::new(store.clone(), config).await.unwrap();
        assert_eq!(
            recovered_wal.last_seq(),
            5,
            "should have recovered 5 entries"
        );

        // Subscribe for new entries (starting from current_seq)
        let start = recovered_wal.current_seq();
        let mut subscriber = recovered_wal.subscribe_from(start);

        // Append 2 more entries on the recovered WAL
        let seq6 = recovered_wal
            .append(WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ), 0)
            .await
            .unwrap();
        let seq7 = recovered_wal
            .append(WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ), 0)
            .await
            .unwrap();

        assert_eq!(seq6, 6, "first new seq after recovery should be 6");
        assert_eq!(seq7, 7);

        // Subscriber should receive the new entries
        let entry1 = tokio::time::timeout(std::time::Duration::from_millis(500), subscriber.next())
            .await
            .expect("timeout waiting for entry 6")
            .unwrap()
            .unwrap();
        assert_eq!(entry1.seq, 6);

        let entry2 = tokio::time::timeout(std::time::Duration::from_millis(500), subscriber.next())
            .await
            .expect("timeout waiting for entry 7")
            .unwrap()
            .unwrap();
        assert_eq!(entry2.seq, 7);
    }
}
