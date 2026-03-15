//! Custom Vortex layout encoding that embeds companion indexes inside Vortex files.
//!
//! The `sequins.indexed` layout wraps an inner data layout and adds companion index
//! segments (bloom filters, trigram skip index, Tantivy full-text) stored alongside
//! the data segments in the same file. The reader consults these indexes during
//! pruning to skip files that cannot contain query results.

pub mod reader;
pub mod strategy;

use std::sync::Arc;

use vortex::array::{ArrayContext, DeserializeMetadata, SerializeMetadata};
use vortex::dtype::DType;
use vortex::error::{vortex_bail, VortexResult};
use vortex::layout::segments::{SegmentId, SegmentSource};
use vortex::layout::session::LayoutSessionExt;
use vortex::layout::{
    LayoutChildType, LayoutChildren, LayoutEncodingRef, LayoutId, LayoutReaderRef, LayoutRef,
    VTable,
};
use vortex::session::VortexSession;

use crate::indexed_layout::reader::IndexedLayoutReader;

// Generate the VTable boilerplate for IndexedLayout.
// This macro (defined in vortex_layout) creates IndexedVTable, and implements
// Deref, AsRef, IntoLayout, and From<IndexedLayout> for LayoutRef.
vortex::layout::vtable!(Indexed);

impl VTable for IndexedVTable {
    type Layout = IndexedLayout;
    type Encoding = IndexedLayoutEncoding;
    type Metadata = IndexedMeta;

    fn id(_encoding: &Self::Encoding) -> LayoutId {
        LayoutId::new_ref("sequins.indexed")
    }

    fn encoding(_layout: &Self::Layout) -> LayoutEncodingRef {
        LayoutEncodingRef::new_ref(IndexedLayoutEncoding.as_ref())
    }

    fn row_count(layout: &Self::Layout) -> u64 {
        layout.row_count
    }

    fn dtype(layout: &Self::Layout) -> &DType {
        &layout.dtype
    }

    fn metadata(layout: &Self::Layout) -> Self::Metadata {
        IndexedMeta {
            has_bloom: layout.bloom_segment.is_some(),
            has_trigram: layout.trigram_segment.is_some(),
            has_tantivy: layout.tantivy_segment.is_some(),
        }
    }

    fn segment_ids(layout: &Self::Layout) -> Vec<SegmentId> {
        let mut segs = Vec::new();
        if let Some(id) = layout.bloom_segment {
            segs.push(id);
        }
        if let Some(id) = layout.trigram_segment {
            segs.push(id);
        }
        if let Some(id) = layout.tantivy_segment {
            segs.push(id);
        }
        segs
    }

    fn nchildren(_layout: &Self::Layout) -> usize {
        1
    }

    fn child(layout: &Self::Layout, idx: usize) -> VortexResult<LayoutRef> {
        if idx != 0 {
            vortex_bail!(
                "IndexedLayout has only 1 child (data), requested index {}",
                idx
            );
        }
        Ok(layout.data_child.clone())
    }

    fn child_type(_layout: &Self::Layout, _idx: usize) -> LayoutChildType {
        LayoutChildType::Transparent("data".into())
    }

    fn new_reader(
        layout: &Self::Layout,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: &VortexSession,
    ) -> VortexResult<LayoutReaderRef> {
        let data_reader =
            layout
                .data_child
                .new_reader(name.clone(), segment_source.clone(), session)?;

        Ok(Arc::new(IndexedLayoutReader {
            name,
            dtype: layout.dtype.clone(),
            row_count: layout.row_count,
            data_reader,
            bloom_segment: layout.bloom_segment,
            trigram_segment: layout.trigram_segment,
            tantivy_segment: layout.tantivy_segment,
            segment_source,
        }))
    }

    fn build(
        _encoding: &Self::Encoding,
        dtype: &DType,
        row_count: u64,
        metadata: &IndexedMeta,
        segment_ids: Vec<SegmentId>,
        children: &dyn LayoutChildren,
        _ctx: ArrayContext,
    ) -> VortexResult<Self::Layout> {
        let data_child = children.child(0, dtype)?;

        let mut seg_iter = segment_ids.into_iter();
        let bloom_segment = if metadata.has_bloom {
            seg_iter.next()
        } else {
            None
        };
        let trigram_segment = if metadata.has_trigram {
            seg_iter.next()
        } else {
            None
        };
        let tantivy_segment = if metadata.has_tantivy {
            seg_iter.next()
        } else {
            None
        };

        Ok(IndexedLayout {
            dtype: dtype.clone(),
            row_count,
            data_child,
            bloom_segment,
            trigram_segment,
            tantivy_segment,
        })
    }

    fn with_children(layout: &mut Self::Layout, children: Vec<LayoutRef>) -> VortexResult<()> {
        if children.len() != 1 {
            vortex_bail!(
                "IndexedLayout expects exactly 1 child, got {}",
                children.len()
            );
        }
        layout.data_child = children.into_iter().next().unwrap();
        Ok(())
    }
}

/// Zero-sized encoding tag for the `sequins.indexed` layout.
#[derive(Debug)]
pub struct IndexedLayoutEncoding;

/// A Vortex layout that wraps an inner data layout and embeds companion indexes
/// (bloom filters, trigram skip index, Tantivy) as additional segments.
#[derive(Clone, Debug)]
pub struct IndexedLayout {
    pub(crate) dtype: DType,
    pub(crate) row_count: u64,
    /// The wrapped data layout (e.g. Chunked or Zoned).
    pub(crate) data_child: LayoutRef,
    /// Segment ID for the bloom filter set, if present.
    pub(crate) bloom_segment: Option<SegmentId>,
    /// Segment ID for the trigram skip index, if present.
    pub(crate) trigram_segment: Option<SegmentId>,
    /// Segment ID for the bundled Tantivy index (bincode-serialized Vec<(filename, bytes)>).
    pub(crate) tantivy_segment: Option<SegmentId>,
}

/// Metadata stored in the layout footer for `sequins.indexed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedMeta {
    pub has_bloom: bool,
    pub has_trigram: bool,
    pub has_tantivy: bool,
}

impl SerializeMetadata for IndexedMeta {
    fn serialize(self) -> Vec<u8> {
        // Format: [has_bloom u8, has_trigram u8, has_tantivy u8] = 3 bytes
        vec![
            self.has_bloom as u8,
            self.has_trigram as u8,
            self.has_tantivy as u8,
        ]
    }
}

impl DeserializeMetadata for IndexedMeta {
    type Output = Self;

    fn deserialize(metadata: &[u8]) -> VortexResult<Self::Output> {
        if metadata.len() < 3 {
            vortex_bail!(
                "IndexedMeta: expected at least 3 bytes, got {}",
                metadata.len()
            );
        }
        Ok(Self {
            has_bloom: metadata[0] != 0,
            has_trigram: metadata[1] != 0,
            has_tantivy: metadata[2] != 0,
        })
    }
}

/// Register the `IndexedLayoutEncoding` in a Vortex session.
///
/// Must be called on every session used to open Vortex files written with the
/// `sequins.indexed` layout (i.e. log/span files with companion indexes).
pub fn register_indexed_layout(session: &VortexSession) {
    session
        .layouts()
        .register(LayoutEncodingRef::new_ref(IndexedLayoutEncoding.as_ref()));
}
