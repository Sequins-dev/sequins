//! Write strategy for the `sequins.indexed` Vortex layout.
//!
//! `IndexedLayoutStrategy` wraps an inner `LayoutStrategy` (e.g. the default
//! BtrBlocks/Compact compressor strategy). After writing the data via the inner
//! strategy, it writes optional companion index bytes (bloom filter, trigram skip
//! index, Tantivy files) as additional segments in the same file and wraps the
//! result in an `IndexedLayout` node.

use std::sync::Arc;

use async_trait::async_trait;
use futures::try_join;
use vortex::array::ArrayContext;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::io::runtime::Handle;
use vortex::layout::segments::SegmentSinkRef;
use vortex::layout::sequence::{SendableSequentialStream, SequencePointer};
use vortex::layout::{LayoutRef, LayoutStrategy};

use crate::indexed_layout::IndexedLayout;
use vortex::layout::IntoLayout;

/// All serialized companion index data for a single Vortex file.
pub struct CompanionIndexBytes {
    /// Serialized `BloomFilterSet` bytes.
    pub bloom_bytes: Vec<u8>,
    /// Serialized `TrigramIndex` bytes.
    pub trigram_bytes: Vec<u8>,
    /// Tantivy index files: (filename, file_bytes).
    pub tantivy_files: Vec<(String, Vec<u8>)>,
}

/// A `LayoutStrategy` that wraps an inner strategy and embeds companion index
/// segments (`bloom`, `trigram`, Tantivy files) in the resulting Vortex file.
///
/// When `companion_data` is `None`, the inner strategy's layout is returned
/// unchanged, so the `sequins.indexed` wrapper is not added.
pub struct IndexedLayoutStrategy {
    inner: Arc<dyn LayoutStrategy>,
    companion_data: Option<CompanionIndexBytes>,
}

impl IndexedLayoutStrategy {
    pub fn new(
        inner: Arc<dyn LayoutStrategy>,
        companion_data: Option<CompanionIndexBytes>,
    ) -> Self {
        Self {
            inner,
            companion_data,
        }
    }
}

#[async_trait]
impl LayoutStrategy for IndexedLayoutStrategy {
    async fn write_stream(
        &self,
        ctx: ArrayContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        mut eof: SequencePointer,
        handle: Handle,
    ) -> VortexResult<LayoutRef> {
        // If no companion data, just pass through to inner strategy unchanged.
        let Some(companion) = &self.companion_data else {
            return self
                .inner
                .write_stream(ctx, segment_sink, stream, eof, handle)
                .await;
        };

        // Split sequence space: data gets the lower range, companion gets the upper range.
        // data_eof < eof, so inner strategy writes to sequences before companion sequences.
        let data_eof = eof.split_off();

        // Conditionally claim sequence IDs only for non-empty companion data.
        // Each segment needs its own unique sequence ID, claimed BEFORE concurrent execution.
        let bloom_seq = if !companion.bloom_bytes.is_empty() {
            Some(eof.advance())
        } else {
            None
        };
        let trigram_seq = if !companion.trigram_bytes.is_empty() {
            Some(eof.advance())
        } else {
            None
        };
        // Bundle all Tantivy files into a single bincode-serialized segment so
        // we can reconstruct the index by filename at read time.
        let tantivy_seq = if !companion.tantivy_files.is_empty() {
            Some(eof.advance())
        } else {
            None
        };

        // Clone companion bytes into the async future.
        let bloom_bytes = companion.bloom_bytes.clone();
        let trigram_bytes = companion.trigram_bytes.clone();
        let tantivy_files = companion.tantivy_files.clone();
        let sink = segment_sink.clone();

        let companion_future = async move {
            let bloom_seg = if let Some(seq) = bloom_seq {
                Some(
                    sink.write(seq, vec![ByteBuffer::copy_from(&bloom_bytes)])
                        .await?,
                )
            } else {
                None
            };
            let trigram_seg = if let Some(seq) = trigram_seq {
                Some(
                    sink.write(seq, vec![ByteBuffer::copy_from(&trigram_bytes)])
                        .await?,
                )
            } else {
                None
            };
            // Serialize all Tantivy files as a single bincode blob preserving filenames.
            let tantivy_seg = if let Some(seq) = tantivy_seq {
                let bundled = bincode::serialize(&tantivy_files).map_err(|e| {
                    vortex::error::vortex_err!("Failed to bundle Tantivy files: {}", e)
                })?;
                Some(
                    sink.write(seq, vec![ByteBuffer::copy_from(&bundled)])
                        .await?,
                )
            } else {
                None
            };

            VortexResult::Ok((bloom_seg, trigram_seg, tantivy_seg))
        };

        // Run inner data write and companion segment writes CONCURRENTLY.
        // This is required because the segment sink may need to order segments by
        // sequence ID (companion seqs > data seqs), so inner MUST make progress
        // while companion waits.
        let (data_layout, (bloom_seg, trigram_seg, tantivy_seg)) = try_join!(
            self.inner
                .write_stream(ctx, segment_sink, stream, data_eof, handle),
            companion_future
        )?;

        // Wrap the data layout in an IndexedLayout node.
        let indexed = IndexedLayout {
            dtype: data_layout.dtype().clone(),
            row_count: data_layout.row_count(),
            data_child: data_layout,
            bloom_segment: bloom_seg,
            trigram_segment: trigram_seg,
            tantivy_segment: tantivy_seg,
        };

        Ok(indexed.into_layout())
    }

    fn buffered_bytes(&self) -> u64 {
        self.inner.buffered_bytes()
    }
}
