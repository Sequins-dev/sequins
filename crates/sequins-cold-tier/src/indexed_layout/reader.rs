//! Layout reader for `sequins.indexed` that consults companion indexes during pruning.
//!
//! `IndexedLayoutReader` wraps the inner data layout reader. In `pruning_evaluation`,
//! it checks the trigram skip index, Tantivy full-text index, and bloom filters before
//! delegating to the inner reader. If the companion indexes prove no rows can match,
//! it returns an all-false mask to skip the file entirely.

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;

use vortex::array::buffer::BufferHandle;
use vortex::array::expr::Expression;
use vortex::array::MaskFuture;
use vortex::buffer::ByteBuffer;
use vortex::dtype::{DType, FieldMask};
use vortex::error::{vortex_err, VortexResult};
use vortex::layout::segments::{SegmentId, SegmentSource};
use vortex::layout::{ArrayFuture, LayoutReader, LayoutReaderRef};
use vortex::mask::Mask;

use crate::index::bloom::BloomFilterSet;
use crate::index::trigram::TrigramIndex;

/// Reader for the `sequins.indexed` layout.
///
/// Delegates all read operations to the inner data reader but intercepts
/// `pruning_evaluation` to check companion indexes when filters are present.
pub struct IndexedLayoutReader {
    pub(crate) name: Arc<str>,
    pub(crate) dtype: DType,
    pub(crate) row_count: u64,
    /// Inner data reader (e.g. ZonedReader wrapping ChunkedReader).
    pub(crate) data_reader: LayoutReaderRef,
    /// Segment ID for the bloom filter set, if present.
    pub(crate) bloom_segment: Option<SegmentId>,
    pub(crate) trigram_segment: Option<SegmentId>,
    /// Segment ID for the bundled Tantivy index (bincode-serialized Vec<(filename, bytes)>).
    pub(crate) tantivy_segment: Option<SegmentId>,
    pub(crate) segment_source: Arc<dyn SegmentSource>,
}

impl LayoutReader for IndexedLayoutReader {
    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn row_count(&self) -> u64 {
        self.row_count
    }

    fn register_splits(
        &self,
        field_mask: &[FieldMask],
        row_range: &Range<u64>,
        splits: &mut BTreeSet<u64>,
    ) -> VortexResult<()> {
        self.data_reader
            .register_splits(field_mask, row_range, splits)
    }

    fn pruning_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &Expression,
        mask: Mask,
    ) -> VortexResult<MaskFuture> {
        let row_len = (row_range.end - row_range.start) as usize;
        let predicates = extract_predicates(expr);

        // If there are no companion-indexable predicates, delegate to inner reader.
        if predicates.ilike_texts.is_empty() && predicates.equalities.is_empty() {
            return self.data_reader.pruning_evaluation(row_range, expr, mask);
        }

        let bloom_segment = self.bloom_segment;
        let trigram_segment = self.trigram_segment;
        let tantivy_segment = self.tantivy_segment;
        let segment_source = self.segment_source.clone();

        Ok(MaskFuture::new(row_len, async move {
            // For each ILIKE text, check trigram then Tantivy.
            for search_text in &predicates.ilike_texts {
                // 1. Trigram check (fast, file-level skip).
                if let Some(seg) = trigram_segment {
                    let bytes = load_bytes(&segment_source, seg).await?;
                    let trigram = TrigramIndex::deserialize(bytes.as_ref())
                        .map_err(|e| vortex_err!("Failed to deserialize trigram index: {}", e))?;
                    let candidates = trigram.candidate_chunks(search_text);
                    if candidates.is_empty() {
                        return Ok(Mask::new_false(row_len));
                    }
                }

                // 2. Tantivy check (precise full-text, more expensive).
                if let Some(seg) = tantivy_segment {
                    if !tantivy_has_match(&segment_source, seg, search_text).await? {
                        return Ok(Mask::new_false(row_len));
                    }
                }
            }

            // For each equality predicate, check the bloom filter.
            if let Some(seg) = bloom_segment {
                if !predicates.equalities.is_empty() {
                    let bytes = load_bytes(&segment_source, seg).await?;
                    let bloom = BloomFilterSet::deserialize(bytes.as_ref())
                        .map_err(|e| vortex_err!("Failed to deserialize bloom filter: {}", e))?;
                    for (field, value) in &predicates.equalities {
                        if !bloom.check(field, value) {
                            return Ok(Mask::new_false(row_len));
                        }
                    }
                }
            }

            // All checks passed — cannot prune.
            Ok(Mask::new_true(row_len))
        }))
    }

    fn filter_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &Expression,
        mask: MaskFuture,
    ) -> VortexResult<MaskFuture> {
        self.data_reader.filter_evaluation(row_range, expr, mask)
    }

    fn projection_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &Expression,
        mask: MaskFuture,
    ) -> VortexResult<ArrayFuture> {
        self.data_reader
            .projection_evaluation(row_range, expr, mask)
    }
}

/// Load a segment's bytes from the segment source.
async fn load_bytes(
    segment_source: &Arc<dyn SegmentSource>,
    seg: SegmentId,
) -> VortexResult<ByteBuffer> {
    let handle: BufferHandle = segment_source.request(seg).await?;
    match handle {
        BufferHandle::Host(b) => Ok(b),
        BufferHandle::Device(d) => d.to_host(),
    }
}

/// Reconstruct a Tantivy index from a bundled segment and search for `search_text`
/// in the `body` field. Returns `true` if any documents match, `false` otherwise.
async fn tantivy_has_match(
    segment_source: &Arc<dyn SegmentSource>,
    tantivy_seg: SegmentId,
    search_text: &str,
) -> VortexResult<bool> {
    use tantivy::collector::TopDocs;
    use tantivy::ReloadPolicy;

    let bytes = load_bytes(segment_source, tantivy_seg).await?;

    // Deserialize the bundled (filename, bytes) pairs.
    let files: Vec<(String, Vec<u8>)> = bincode::deserialize(bytes.as_ref())
        .map_err(|e| vortex_err!("Failed to deserialize Tantivy bundle: {}", e))?;

    // Write files to a tempdir so Tantivy can open the index.
    let tempdir =
        tempfile::tempdir().map_err(|e| vortex_err!("Failed to create Tantivy tempdir: {}", e))?;
    for (filename, file_bytes) in &files {
        let path = tempdir.path().join(filename);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| vortex_err!("Failed to create dirs for {}: {}", filename, e))?;
        }
        std::fs::write(&path, file_bytes)
            .map_err(|e| vortex_err!("Failed to write Tantivy file {}: {}", filename, e))?;
    }

    let index = tantivy::Index::open_in_dir(tempdir.path())
        .map_err(|e| vortex_err!("Failed to open Tantivy index: {}", e))?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(|e| vortex_err!("Failed to create Tantivy reader: {}", e))?;
    let searcher = reader.searcher();
    let schema = index.schema();

    let body_field = schema
        .get_field("body")
        .map_err(|e| vortex_err!("body field not found in Tantivy schema: {}", e))?;

    let query_parser = tantivy::query::QueryParser::for_index(&index, vec![body_field]);
    let query = query_parser
        .parse_query(search_text)
        .map_err(|e| vortex_err!("Failed to parse Tantivy query '{}': {}", search_text, e))?;

    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(1))
        .map_err(|e| vortex_err!("Tantivy search failed: {}", e))?;

    Ok(!top_docs.is_empty())
}

/// Collected predicates extracted from an expression tree.
struct CompanionPredicates {
    /// Text strings from ILIKE expressions (body contains text checks).
    ilike_texts: Vec<String>,
    /// Equality predicates: (field_name, value).
    equalities: Vec<(String, String)>,
}

/// Walk an expression tree and collect companion-indexable predicates.
///
/// Recurses into AND conjunctions; collects ILIKE texts and equality pairs.
fn extract_predicates(expr: &Expression) -> CompanionPredicates {
    let mut predicates = CompanionPredicates {
        ilike_texts: Vec::new(),
        equalities: Vec::new(),
    };
    collect_predicates(expr, &mut predicates);
    predicates
}

fn collect_predicates(expr: &Expression, out: &mut CompanionPredicates) {
    use vortex::array::expr::{Binary, Operator};

    // Recurse into AND conjunctions.
    if let Some(op) = expr.as_opt::<Binary>() {
        if *op == Operator::And {
            collect_predicates(expr.child(0), out);
            collect_predicates(expr.child(1), out);
            return;
        }
    }

    // Try to extract an ILIKE text.
    if let Some(text) = extract_ilike_text(expr) {
        out.ilike_texts.push(text);
        return;
    }

    // Try to extract an equality predicate.
    if let Some(pair) = extract_equality_predicate(expr) {
        out.equalities.push(pair);
    }
}

/// Extract the search text from an ILIKE expression, if one is present.
///
/// Looks for expressions of the form `<column> ILIKE '%text%'` (case-insensitive
/// LIKE with leading and trailing wildcards). Returns the inner text with `%`
/// markers stripped, or `None` if the expression is not of this form.
fn extract_ilike_text(expr: &Expression) -> Option<String> {
    use vortex::array::compute::LikeOptions;
    use vortex::array::expr::Like;
    use vortex::array::expr::Literal;

    // Check if this is a Like expression with case_insensitive=true and negated=false
    let opts: &LikeOptions = expr.as_opt::<Like>()?;
    if opts.negated || !opts.case_insensitive {
        return None;
    }

    // The pattern is the second child (index 1)
    let pattern_expr = expr.child(1);
    let scalar: &vortex::scalar::Scalar = pattern_expr.as_opt::<Literal>()?;

    // Extract the string value
    let text = scalar.as_utf8().value_ref()?.as_str().to_string();

    // Strip leading and trailing '%' wildcards and return the inner text
    let text = text.trim_matches('%');
    if text.is_empty() {
        return None;
    }

    Some(text.to_lowercase())
}

/// Extract an equality predicate of the form `field = 'value'` or `'value' = field`.
///
/// Returns `(field_name, value)` if the expression is an equality between a
/// `GetItem` field access and a string literal, `None` otherwise.
fn extract_equality_predicate(expr: &Expression) -> Option<(String, String)> {
    use vortex::array::expr::{Binary, GetItem, Literal, Operator};

    let op = expr.as_opt::<Binary>()?;
    if *op != Operator::Eq {
        return None;
    }

    let lhs = expr.child(0);
    let rhs = expr.child(1);

    // Try field = literal
    if let (Some(field_name), Some(scalar)) = (lhs.as_opt::<GetItem>(), rhs.as_opt::<Literal>()) {
        if let Some(buf) = scalar.as_utf8().value_ref() {
            return Some((field_name.to_string(), buf.as_str().to_string()));
        }
    }

    // Try literal = field (reversed)
    if let (Some(field_name), Some(scalar)) = (rhs.as_opt::<GetItem>(), lhs.as_opt::<Literal>()) {
        if let Some(buf) = scalar.as_utf8().value_ref() {
            return Some((field_name.to_string(), buf.as_str().to_string()));
        }
    }

    None
}
