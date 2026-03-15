use crate::entry::WalEntry;
use crate::error::{Result, WalError};
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;

/// Metadata for a WAL segment file
#[derive(Debug, Clone)]
pub struct WalSegmentMeta {
    /// Segment identifier (sequential)
    pub segment_id: u64,
    /// First sequence number in this segment
    pub start_seq: u64,
    /// Last sequence number in this segment (inclusive)
    pub end_seq: u64,
    /// Object store path to this segment
    pub path: Path,
}

impl WalSegmentMeta {
    /// Create a new segment metadata
    pub fn new(segment_id: u64, start_seq: u64, base_path: &str) -> Self {
        let path = Path::from(format!("{}/wal/segment_{:08}.wal", base_path, segment_id));
        Self {
            segment_id,
            start_seq,
            end_seq: start_seq, // Will be updated as entries are written
            path,
        }
    }

    /// Check if a sequence number falls within this segment
    pub fn contains(&self, seq: u64) -> bool {
        seq >= self.start_seq && seq <= self.end_seq
    }
}

/// A single WAL segment file (append-only)
pub struct WalSegment {
    /// Segment metadata
    pub meta: WalSegmentMeta,
    /// Object store backend
    store: Arc<dyn ObjectStore>,
    /// In-memory buffer of segment data (for writing)
    buffer: Vec<u8>,
}

impl WalSegment {
    /// Create a new segment
    pub fn new(meta: WalSegmentMeta, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            meta,
            store,
            buffer: Vec::new(),
        }
    }

    /// Append an entry to this segment (buffered in memory)
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        debug_assert!(
            entry.seq >= self.meta.end_seq,
            "WAL entry sequence {} is not monotonically >= current end_seq {}",
            entry.seq,
            self.meta.end_seq
        );
        // Serialize entry with length prefix
        let entry_bytes = bincode::serialize(entry).map_err(|e| {
            WalError::Serialization(format!("Failed to serialize WAL entry: {}", e))
        })?;

        let len = entry_bytes.len() as u64;
        let len_bytes = len.to_le_bytes();

        // Write length prefix + entry bytes to buffer
        self.buffer.extend_from_slice(&len_bytes);
        self.buffer.extend_from_slice(&entry_bytes);

        // Update end_seq
        self.meta.end_seq = entry.seq;

        Ok(())
    }

    /// Flush buffered writes to object store
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let bytes = bytes::Bytes::from(std::mem::take(&mut self.buffer));
        self.store
            .put(&self.meta.path, bytes.into())
            .await
            .map_err(|e| WalError::Io(format!("Failed to write WAL segment: {}", e)))?;

        Ok(())
    }

    /// Read a range of entries from this segment
    pub async fn read_range(&self, start_seq: u64, end_seq: u64) -> Result<Vec<WalEntry>> {
        // Fetch the segment file from object store
        let bytes = self
            .store
            .get(&self.meta.path)
            .await
            .map_err(|e| WalError::Io(format!("Failed to read WAL segment: {}", e)))?
            .bytes()
            .await
            .map_err(|e| WalError::Io(format!("Failed to read segment bytes: {}", e)))?;

        // Parse entries
        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < bytes.len() {
            // Read length prefix
            if offset + 8 > bytes.len() {
                break; // Incomplete entry
            }

            let len_bytes: [u8; 8] = bytes[offset..offset + 8].try_into().unwrap();
            let len = u64::from_le_bytes(len_bytes) as usize;
            offset += 8;

            // Read entry bytes
            if offset + len > bytes.len() {
                break; // Incomplete entry
            }

            let entry_bytes = &bytes[offset..offset + len];
            let entry: WalEntry = bincode::deserialize(entry_bytes).map_err(|e| {
                WalError::Serialization(format!("Failed to deserialize entry: {}", e))
            })?;

            // Filter by sequence range
            if entry.seq >= start_seq && entry.seq <= end_seq {
                entries.push(entry);
            }

            offset += len;
        }

        Ok(entries)
    }

    /// Delete this segment from object store
    pub async fn delete(&self) -> Result<()> {
        self.store
            .delete(&self.meta.path)
            .await
            .map_err(|e| WalError::Io(format!("Failed to delete WAL segment: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::WalPayload;
    use object_store::memory::InMemory;

    #[test]
    fn segment_meta_contains() {
        let meta = WalSegmentMeta {
            segment_id: 1,
            start_seq: 100,
            end_seq: 199,
            path: Path::from("test.wal"),
        };

        assert!(meta.contains(100));
        assert!(meta.contains(150));
        assert!(meta.contains(199));
        assert!(!meta.contains(99));
        assert!(!meta.contains(200));
    }

    #[tokio::test]
    async fn segment_append_and_read() {
        let store = Arc::new(InMemory::new());
        let meta = WalSegmentMeta::new(1, 1, "test");
        let mut segment = WalSegment::new(meta, store.clone());

        // Append entries
        let entry1 = WalEntry::new(
            1,
            1000,
            WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ),
        );
        let entry2 = WalEntry::new(
            2,
            2000,
            WalPayload::Logs(
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::default(
                ),
            ),
        );

        segment.append(&entry1).unwrap();
        segment.append(&entry2).unwrap();
        segment.flush().await.unwrap();

        // Read back
        let entries = segment.read_range(1, 2).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 1);
        assert_eq!(entries[1].seq, 2);
    }

    #[tokio::test]
    async fn segment_read_filtered_range() {
        let store = Arc::new(InMemory::new());
        let meta = WalSegmentMeta::new(1, 1, "test");
        let mut segment = WalSegment::new(meta, store.clone());

        // Append multiple entries
        for i in 1..=5 {
            let entry = WalEntry::new(
                i,
                i * 1000,
                WalPayload::Traces(
                    opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
                ),
            );
            segment.append(&entry).unwrap();
        }
        segment.flush().await.unwrap();

        // Read partial range
        let entries = segment.read_range(2, 4).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].seq, 2);
        assert_eq!(entries[2].seq, 4);
    }

    #[tokio::test]
    async fn test_corrupted_entry_bytes_gives_error() {
        use object_store::PutPayload;

        let store = Arc::new(InMemory::new());
        let meta = WalSegmentMeta::new(1, 1, "test");
        let segment = WalSegment::new(meta.clone(), store.clone());

        // Write a valid length prefix (16 bytes payload) followed by garbage
        let mut buf = Vec::new();
        let len: u64 = 16;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(&[
            0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09,
            0x0A, 0x0B,
        ]);

        store
            .put(&meta.path, PutPayload::from_bytes(buf.into()))
            .await
            .unwrap();

        // Reading should return a deserialization error, not panic
        let result = segment.read_range(1, 100).await;
        assert!(
            result.is_err(),
            "corrupted entry bytes should produce an error"
        );
    }

    #[tokio::test]
    async fn test_empty_segment_flush_and_read() {
        let store = Arc::new(InMemory::new());
        let meta = WalSegmentMeta::new(1, 1, "test");
        let mut segment = WalSegment::new(meta, store.clone());

        // Flush with no entries — should be a no-op (empty buffer)
        segment.flush().await.unwrap();

        // Reading from a non-existent path should return an IO error (nothing was written)
        let result = segment.read_range(1, 100).await;
        assert!(
            result.is_err(),
            "reading from an empty/absent segment should fail"
        );
    }
}
