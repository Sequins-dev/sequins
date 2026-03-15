use crate::entry::WalEntry;
use crate::error::Result;
use crate::segment::{WalSegment, WalSegmentMeta};
use object_store::ObjectStore;
use std::sync::Arc;

/// Configuration for WAL writer
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Maximum entries per segment before rotating
    pub segment_size: u64,
    /// Flush to object store every N entries
    pub flush_interval: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            segment_size: 10_000,
            flush_interval: 100,
        }
    }
}

/// Buffered writer for the active WAL segment
pub struct WalWriter {
    /// Current active segment
    segment: WalSegment,
    /// Object store backend
    store: Arc<dyn ObjectStore>,
    /// Base path for WAL segments
    base_path: String,
    /// Writer configuration
    config: WriterConfig,
    /// Entries written to current segment
    entries_in_segment: u64,
    /// Entries written since last flush
    entries_since_flush: usize,
}

impl WalWriter {
    /// Create a new writer starting at the given segment ID
    pub fn new(
        store: Arc<dyn ObjectStore>,
        base_path: String,
        start_segment_id: u64,
        start_seq: u64,
        config: WriterConfig,
    ) -> Self {
        let meta = WalSegmentMeta::new(start_segment_id, start_seq, &base_path);
        let segment = WalSegment::new(meta, store.clone());

        Self {
            segment,
            store,
            base_path,
            config,
            entries_in_segment: 0,
            entries_since_flush: 0,
        }
    }

    /// Append an entry to the WAL
    pub async fn append(&mut self, entry: &WalEntry) -> Result<()> {
        // Check if we need to rotate to a new segment
        if self.entries_in_segment >= self.config.segment_size {
            self.rotate().await?;
        }

        // Append to current segment
        self.segment.append(entry)?;
        self.entries_in_segment += 1;
        self.entries_since_flush += 1;

        // Flush if needed
        if self.entries_since_flush >= self.config.flush_interval {
            self.flush().await?;
        }

        Ok(())
    }

    /// Force flush buffered writes to object store
    pub async fn flush(&mut self) -> Result<()> {
        self.segment.flush().await?;
        self.entries_since_flush = 0;
        Ok(())
    }

    /// Rotate to a new segment
    async fn rotate(&mut self) -> Result<()> {
        let old_start_seq = self.segment.meta.start_seq;

        // Flush current segment
        self.flush().await?;

        // Create new segment
        let new_segment_id = self.segment.meta.segment_id + 1;
        let new_start_seq = self.segment.meta.end_seq + 1;
        debug_assert!(
            new_start_seq > old_start_seq,
            "new segment start_seq ({}) must be > old start_seq ({})",
            new_start_seq,
            old_start_seq
        );
        let meta = WalSegmentMeta::new(new_segment_id, new_start_seq, &self.base_path);
        self.segment = WalSegment::new(meta, self.store.clone());
        self.entries_in_segment = 0;

        Ok(())
    }

    /// Get the current segment metadata
    pub fn current_segment_meta(&self) -> &WalSegmentMeta {
        &self.segment.meta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::WalPayload;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn writer_append_and_flush() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

        let store = Arc::new(InMemory::new());
        let config = WriterConfig::default();
        let mut writer = WalWriter::new(store.clone(), "test".to_string(), 1, 1, config);

        let entry = WalEntry::new(
            1,
            1000,
            WalPayload::Traces(ExportTraceServiceRequest::default()),
        );
        writer.append(&entry).await.unwrap();
        writer.flush().await.unwrap();

        assert_eq!(writer.current_segment_meta().end_seq, 1);
    }

    #[tokio::test]
    async fn writer_rotation() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

        let store = Arc::new(InMemory::new());
        let config = WriterConfig {
            segment_size: 3, // Rotate after 3 entries
            flush_interval: 10,
        };
        let mut writer = WalWriter::new(store.clone(), "test".to_string(), 1, 1, config);

        // Write 5 entries (should trigger rotation)
        for i in 1..=5 {
            let entry = WalEntry::new(
                i,
                i * 1000,
                WalPayload::Traces(ExportTraceServiceRequest::default()),
            );
            writer.append(&entry).await.unwrap();
        }

        // Should be in segment 2 now
        assert_eq!(writer.current_segment_meta().segment_id, 2);
        assert_eq!(writer.current_segment_meta().start_seq, 4);
    }
}
