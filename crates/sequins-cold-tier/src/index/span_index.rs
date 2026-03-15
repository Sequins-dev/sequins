//! Span companion index for efficient attribute queries
//!
//! This module provides a multi-tier indexing strategy for span attributes:
//! - Low cardinality (< threshold): Tantivy inverted index
//! - Medium cardinality (threshold - 10x threshold): Tantivy fast fields
//! - Ultra-high cardinality (> 10x threshold): Bloom filters only
//!
//! The companion index is stored alongside the Vortex data file and provides
//! fast lookups without scanning the main columnar data.

use super::bloom::BloomFilterSet;
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringViewArray};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tantivy::schema::*;
use tantivy::{Index, IndexWriter, TantivyDocument};

/// Strategy for indexing a specific attribute
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum IndexStrategy {
    /// Inverted index for low-cardinality fields (< cardinality_threshold)
    /// Provides exact match and range queries
    Inverted,

    /// Fast field (columnar) for medium-cardinality fields
    /// Provides efficient filtering and aggregation
    FastField,

    /// Bloom filter only for ultra-high-cardinality fields
    /// Provides membership testing with false positives
    BloomOnly,
}

/// Metadata about how each attribute is indexed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IndexMetadata {
    /// Map of attribute name to index strategy
    pub(crate) strategies: HashMap<String, IndexStrategy>,

    /// Cardinality threshold used for classification
    pub(crate) cardinality_threshold: usize,

    /// Number of spans indexed
    pub(crate) span_count: usize,
}

/// Companion index for a batch of spans
pub struct SpanCompanionIndex {
    /// Keeps the Tantivy temp directory alive for the lifetime of this struct.
    _tantivy_dir: Option<tempfile::TempDir>,

    /// Bloom filters for ultra-high-cardinality fields
    bloom_filters: BloomFilterSet,

    /// Cardinality-based strategies per attribute — used in tests to verify classification.
    #[allow(dead_code)]
    metadata: IndexMetadata,
}

impl SpanCompanionIndex {
    /// Build a companion index for a batch of spans from a RecordBatch.
    ///
    /// # Arguments
    /// * `batch` - RecordBatch containing span data with `attr_*` columns
    /// * `cardinality_threshold` - Threshold for low vs medium cardinality (default: 100)
    ///
    /// # Returns
    /// A companion index ready for querying
    pub fn build_for_batch(
        batch: &RecordBatch,
        cardinality_threshold: usize,
    ) -> Result<Self, String> {
        if batch.num_rows() == 0 {
            return Err("Cannot build index for empty span batch".to_string());
        }

        // Collect all attr_* columns (excluding _overflow_attrs)
        let attr_columns: Vec<(String, &dyn Array)> = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name().starts_with("attr_") && f.name() != "_overflow_attrs")
            .map(|(i, f)| (f.name().clone(), batch.column(i).as_ref()))
            .collect();

        // Step 1: Analyze cardinality of each promoted attribute
        let cardinality_map = Self::analyze_cardinality_from_batch(batch, &attr_columns);

        // Step 2: Classify attributes by index strategy
        let strategies = Self::classify_attributes(&cardinality_map, cardinality_threshold);

        // Step 3: Build Tantivy schema dynamically based on strategies
        let (schema, field_map) = Self::build_schema(&strategies);

        // Step 4: Create on-disk Tantivy index so it can be serialized to segments.
        // The Index object is only needed during build; files remain on disk in tantivy_dir.
        let tantivy_dir =
            tempfile::tempdir().map_err(|e| format!("Failed to create Tantivy temp dir: {}", e))?;
        {
            let tantivy_index = Index::create_in_dir(tantivy_dir.path(), schema.clone())
                .map_err(|e| format!("Failed to create Tantivy index: {}", e))?;
            let mut index_writer = tantivy_index
                .writer(50_000_000) // 50MB buffer
                .map_err(|e| format!("Failed to create Tantivy writer: {}", e))?;

            // Step 5: Index all rows in Tantivy
            Self::index_batch_in_tantivy(batch, &mut index_writer, &field_map, &strategies)?;

            index_writer
                .commit()
                .map_err(|e| format!("Failed to commit Tantivy index: {}", e))?;
        } // tantivy_index dropped here; flushed files remain on disk

        // Step 6: Build bloom filters for BloomOnly fields
        let mut bloom_filters = BloomFilterSet::new();
        Self::build_bloom_filters_from_batch(&mut bloom_filters, batch, &strategies, 0.01);

        // Metadata is kept for test introspection of cardinality classification.
        let metadata = IndexMetadata {
            strategies: strategies.clone(),
            cardinality_threshold,
            span_count: batch.num_rows(),
        };

        Ok(Self {
            _tantivy_dir: Some(tantivy_dir),
            bloom_filters,
            metadata,
        })
    }

    /// Extract a string value from an Arrow array at a given row index
    fn column_string_value(col: &dyn Array, row: usize) -> Option<String> {
        if col.is_null(row) {
            return None;
        }
        if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
            return Some(arr.value(row).to_string());
        }
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            return Some(arr.value(row).to_string());
        }
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            return Some(arr.value(row).to_string());
        }
        if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            return Some(arr.value(row).to_string());
        }
        None
    }

    /// Analyze cardinality of each attribute column in the batch
    fn analyze_cardinality_from_batch(
        batch: &RecordBatch,
        attr_columns: &[(String, &dyn Array)],
    ) -> HashMap<String, usize> {
        let mut unique_values: HashMap<String, HashSet<String>> = HashMap::new();
        for (col_name, col) in attr_columns {
            let attr_key = col_name.strip_prefix("attr_").unwrap_or(col_name);
            for row in 0..batch.num_rows() {
                if let Some(val) = Self::column_string_value(*col, row) {
                    unique_values
                        .entry(attr_key.to_string())
                        .or_default()
                        .insert(val);
                }
            }
        }
        unique_values
            .into_iter()
            .map(|(k, v)| (k, v.len()))
            .collect()
    }

    /// Classify attributes into index strategies based on cardinality
    fn classify_attributes(
        cardinality_map: &HashMap<String, usize>,
        threshold: usize,
    ) -> HashMap<String, IndexStrategy> {
        cardinality_map
            .iter()
            .map(|(key, &cardinality)| {
                let strategy = if cardinality < threshold {
                    IndexStrategy::Inverted
                } else if cardinality < threshold * 10 {
                    IndexStrategy::FastField
                } else {
                    IndexStrategy::BloomOnly
                };
                (key.clone(), strategy)
            })
            .collect()
    }

    /// Build Tantivy schema based on index strategies
    fn build_schema(
        strategies: &HashMap<String, IndexStrategy>,
    ) -> (Schema, HashMap<String, Field>) {
        let mut schema_builder = Schema::builder();
        let mut field_map = HashMap::new();

        // Add core fields (always indexed)
        let trace_id = schema_builder.add_bytes_field("trace_id", STORED | FAST);
        field_map.insert("trace_id".to_string(), trace_id);

        let span_id = schema_builder.add_bytes_field("span_id", STORED | FAST);
        field_map.insert("span_id".to_string(), span_id);

        let service_name = schema_builder.add_text_field("service_name", TEXT | STORED);
        field_map.insert("service_name".to_string(), service_name);

        let operation = schema_builder.add_text_field("operation", TEXT | STORED);
        field_map.insert("operation".to_string(), operation);

        // Add attribute fields based on strategy
        for (key, strategy) in strategies {
            match strategy {
                IndexStrategy::Inverted => {
                    // Text field with inverted index
                    let field = schema_builder.add_text_field(key, TEXT);
                    field_map.insert(key.clone(), field);
                }
                IndexStrategy::FastField => {
                    // Fast field (columnar storage)
                    let field = schema_builder.add_text_field(key, FAST);
                    field_map.insert(key.clone(), field);
                }
                IndexStrategy::BloomOnly => {
                    // Skip Tantivy for bloom-only fields
                }
            }
        }

        (schema_builder.build(), field_map)
    }

    /// Index all rows in the batch into Tantivy
    fn index_batch_in_tantivy(
        batch: &RecordBatch,
        writer: &mut IndexWriter,
        field_map: &HashMap<String, Field>,
        strategies: &HashMap<String, IndexStrategy>,
    ) -> Result<(), String> {
        let trace_id_col = batch
            .column_by_name("trace_id")
            .and_then(|c| c.as_any().downcast_ref::<StringViewArray>());
        let span_id_col = batch
            .column_by_name("span_id")
            .and_then(|c| c.as_any().downcast_ref::<StringViewArray>());
        let name_col = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringViewArray>());

        for row in 0..batch.num_rows() {
            let mut doc = TantivyDocument::default();

            if let (Some(col), Some(field)) = (trace_id_col, field_map.get("trace_id")) {
                doc.add_bytes(*field, col.value(row).as_bytes());
            }
            if let (Some(col), Some(field)) = (span_id_col, field_map.get("span_id")) {
                doc.add_bytes(*field, col.value(row).as_bytes());
            }
            if let Some(field) = field_map.get("service_name") {
                // Try to get service name from attr_service_name column
                let svc = batch
                    .column_by_name("attr_service_name")
                    .and_then(|c| c.as_any().downcast_ref::<StringViewArray>())
                    .and_then(|a| {
                        if a.is_null(row) {
                            None
                        } else {
                            Some(a.value(row))
                        }
                    })
                    .unwrap_or("unknown");
                doc.add_text(*field, svc);
            }
            if let (Some(col), Some(field)) = (name_col, field_map.get("operation")) {
                doc.add_text(*field, col.value(row));
            }

            // Add promoted attr fields
            for (key, &strategy) in strategies {
                if strategy != IndexStrategy::BloomOnly {
                    if let Some(field) = field_map.get(key) {
                        let col_name = format!("attr_{}", key);
                        if let Some(col) = batch.column_by_name(&col_name) {
                            if let Some(val) = Self::column_string_value(col.as_ref(), row) {
                                doc.add_text(*field, &val);
                            }
                        }
                    }
                }
            }

            writer
                .add_document(doc)
                .map_err(|e| format!("Failed to add doc: {}", e))?;
        }
        Ok(())
    }

    /// Build bloom filters for BloomOnly attributes from a RecordBatch
    fn build_bloom_filters_from_batch(
        bloom_filters: &mut BloomFilterSet,
        batch: &RecordBatch,
        strategies: &HashMap<String, IndexStrategy>,
        fpr: f64,
    ) {
        for (key, &strategy) in strategies {
            if strategy == IndexStrategy::BloomOnly {
                let col_name = format!("attr_{}", key);
                if let Some(col) = batch.column_by_name(&col_name) {
                    let values: Vec<String> = (0..batch.num_rows())
                        .filter_map(|row| Self::column_string_value(col.as_ref(), row))
                        .collect();
                    if !values.is_empty() {
                        bloom_filters.build(key.clone(), values, fpr);
                    }
                }
            }
        }
    }

    /// Convert this index into serialized bytes for embedding in a Vortex file.
    ///
    /// Returns a `CompanionIndexBytes` struct containing bloom filter bytes, empty
    /// trigram bytes (spans do not have a trigram index), and Tantivy index files.
    pub fn into_companion_bytes(
        self,
    ) -> Result<crate::indexed_layout::strategy::CompanionIndexBytes, String> {
        let bloom_bytes = self.bloom_filters.serialize()?;

        // Collect all Tantivy index files from the temp directory
        let tantivy_files = if let Some(ref dir) = self._tantivy_dir {
            super::collect_tantivy_files(dir.path())?
        } else {
            Vec::new()
        };

        Ok(crate::indexed_layout::strategy::CompanionIndexBytes {
            bloom_bytes,
            // Spans do not use a trigram index; leave empty so no trigram segment is written.
            trigram_bytes: Vec::new(),
            tantivy_files,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array, UInt8Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Build a span RecordBatch with attr_* columns for testing
    fn make_span_batch_with_attrs(n: usize, attrs: &[(&str, Vec<String>)]) -> RecordBatch {
        let base = 1_700_000_000_000_000_000i64;
        let mut fields = vec![
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
        ];
        for (col_name, _) in attrs {
            fields.push(Field::new(*col_name, DataType::Utf8View, true));
        }
        let schema = Arc::new(Schema::new(fields));

        let start_times: Vec<i64> = (0..n).map(|i| base + i as i64 * 1_000_000_000).collect();

        let mut arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringViewArray::from(vec!["00000000"; n])) as _,
            Arc::new(StringViewArray::from(vec!["00000001"; n])) as _,
            Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
            Arc::new(StringViewArray::from(vec!["op"; n])) as _,
            Arc::new(UInt8Array::from(vec![0u8; n])) as _,
            Arc::new(UInt8Array::from(vec![1u8; n])) as _, // Ok
            Arc::new(TimestampNanosecondArray::from(start_times.clone())) as _,
            Arc::new(TimestampNanosecondArray::from(
                start_times
                    .iter()
                    .map(|t| t + 100_000_000)
                    .collect::<Vec<_>>(),
            )) as _,
            Arc::new(Int64Array::from(vec![100_000_000i64; n])) as _,
            Arc::new(UInt32Array::from(vec![1u32; n])) as _,
            Arc::new(UInt32Array::from(vec![1u32; n])) as _,
        ];

        for (_, values) in attrs {
            let vals: Vec<Option<&str>> =
                (0..n).map(|i| values.get(i).map(|s| s.as_str())).collect();
            arrays.push(Arc::new(StringViewArray::from(vals)) as _);
        }

        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn test_cardinality_classification() {
        let n = 200;

        // Low cardinality: 10 unique values
        let env_values: Vec<String> = (0..n).map(|i| format!("env{}", i % 10)).collect();
        // Medium cardinality: values cycling through 500 (but we only have 200 rows)
        let endpoint_values: Vec<String> = (0..n)
            .map(|i| format!("/api/endpoint{}", i % 500))
            .collect();
        // High cardinality: 200 unique values (all different)
        let request_id_values: Vec<String> = (0..n).map(|i| format!("req-{}", i)).collect();

        let batch = make_span_batch_with_attrs(
            n,
            &[
                ("attr_environment", env_values),
                ("attr_endpoint", endpoint_values),
                ("attr_request_id", request_id_values),
            ],
        );

        let index = SpanCompanionIndex::build_for_batch(&batch, 100).unwrap();

        // Check strategy assignments
        assert_eq!(
            index.metadata.strategies.get("environment"),
            Some(&IndexStrategy::Inverted) // 10 unique < 100
        );
        // endpoint has 200 unique values (0..200 % 500 = all unique up to 200)
        // 100 <= 200 < 1000, so FastField
        assert_eq!(
            index.metadata.strategies.get("endpoint"),
            Some(&IndexStrategy::FastField)
        );
        // request_id has 200 unique values, same range as endpoint
        assert_eq!(
            index.metadata.strategies.get("request_id"),
            Some(&IndexStrategy::FastField)
        );
    }
}
