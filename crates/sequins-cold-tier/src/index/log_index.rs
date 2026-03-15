//! Log companion index for efficient log queries
//!
//! Provides multi-tier indexing for log records:
//! - Tantivy inverted index for severity, service_name, and extracted fields
//! - Bloom filter for trace_id and span_id (high cardinality)
//! - Trigram index for full-text search on body column
//!
//! The companion index is stored alongside the Vortex data file.

use super::bloom::BloomFilterSet;
use super::span_index::IndexStrategy;
use super::trigram::TrigramIndex;
use arrow::array::{Array, StringViewArray};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tantivy::collector::TopDocs;
use tantivy::schema::*;
use tantivy::{Index, IndexWriter, ReloadPolicy, TantivyDocument};

/// Metadata about how log fields are indexed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LogIndexMetadata {
    /// Map of field name to index strategy
    pub(crate) strategies: HashMap<String, IndexStrategy>,

    /// Number of log records indexed
    pub(crate) record_count: usize,
}

/// Companion index for a batch of log records
pub struct LogCompanionIndex {
    /// Tantivy index (on-disk in a tempdir so it can be serialized)
    tantivy_index: Index,

    /// Keeps the Tantivy temp directory alive for the lifetime of this struct.
    _tantivy_dir: Option<tempfile::TempDir>,

    /// Bloom filters for high-cardinality fields (trace_id, span_id)
    bloom_filters: BloomFilterSet,

    /// Trigram index for body text search
    trigram_index: TrigramIndex,

    /// Metadata about indexing strategies
    metadata: LogIndexMetadata,
}

impl LogCompanionIndex {
    /// Build a companion index for a batch of log records from a RecordBatch.
    ///
    /// All `attr_*` columns are treated as extracted fields for inverted indexing.
    /// `trace_id` and `span_id` are indexed with bloom filters.
    /// `body` is indexed with Tantivy full-text and a trigram index.
    pub fn build_for_batch(batch: &RecordBatch) -> Result<Self, String> {
        if batch.num_rows() == 0 {
            return Err("Cannot build index for empty log batch".to_string());
        }

        // Collect attr_* column names as extracted field names (stripped of prefix)
        let extracted_field_names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .filter(|f| f.name().starts_with("attr_") && f.name() != "_overflow_attrs")
            .map(|f| {
                f.name()
                    .strip_prefix("attr_")
                    .unwrap_or(f.name())
                    .to_string()
            })
            .collect();

        // Step 1: Build Tantivy schema
        let (schema, field_map) = Self::build_schema(&extracted_field_names);

        // Step 2: Create on-disk Tantivy index so it can be serialized to segments
        let tantivy_dir =
            tempfile::tempdir().map_err(|e| format!("Failed to create Tantivy temp dir: {}", e))?;
        let tantivy_index = Index::create_in_dir(tantivy_dir.path(), schema.clone())
            .map_err(|e| format!("Failed to create Tantivy index: {}", e))?;
        let mut index_writer = tantivy_index
            .writer(50_000_000) // 50MB buffer
            .map_err(|e| format!("Failed to create Tantivy writer: {}", e))?;

        // Step 3: Index each log record in Tantivy
        for row in 0..batch.num_rows() {
            Self::index_row_in_tantivy(
                batch,
                row,
                &mut index_writer,
                &field_map,
                &extracted_field_names,
            )?;
        }

        // Commit Tantivy index
        index_writer
            .commit()
            .map_err(|e| format!("Failed to commit Tantivy index: {}", e))?;

        // Step 4: Build bloom filters for trace_id and span_id
        let mut bloom_filters = BloomFilterSet::new();
        Self::build_bloom_filters_from_batch(&mut bloom_filters, batch, 0.01);

        // Step 5: Build trigram index for body text
        let bodies: Vec<&str> = if let Some(col) = batch.column_by_name("body") {
            if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                (0..arr.len())
                    .filter_map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        let trigram_index = TrigramIndex::build(&bodies, 10_000);

        // Step 6: Build metadata with index strategies
        let mut strategies = HashMap::new();

        // Core fields use inverted index
        strategies.insert("severity_text".to_string(), IndexStrategy::Inverted);
        strategies.insert("service_name".to_string(), IndexStrategy::Inverted);

        // Extracted attr fields use inverted index
        for field_name in &extracted_field_names {
            strategies.insert(field_name.clone(), IndexStrategy::Inverted);
        }

        // trace_id and span_id use bloom filters
        strategies.insert("trace_id".to_string(), IndexStrategy::BloomOnly);
        strategies.insert("span_id".to_string(), IndexStrategy::BloomOnly);

        let metadata = LogIndexMetadata {
            strategies,
            record_count: batch.num_rows(),
        };

        Ok(Self {
            tantivy_index,
            _tantivy_dir: Some(tantivy_dir),
            bloom_filters,
            trigram_index,
            metadata,
        })
    }

    /// Build Tantivy schema for log metadata
    fn build_schema(extracted_field_names: &[String]) -> (Schema, HashMap<String, Field>) {
        let mut schema_builder = Schema::builder();
        let mut field_map = HashMap::new();

        // Core fields
        let severity = schema_builder.add_text_field("severity_text", TEXT | STORED);
        field_map.insert("severity_text".to_string(), severity);

        let service_name = schema_builder.add_text_field("service_name", TEXT | STORED);
        field_map.insert("service_name".to_string(), service_name);

        // Body field for full-text search
        let body = schema_builder.add_text_field("body", TEXT);
        field_map.insert("body".to_string(), body);

        // log_id field (stored, for returning search results)
        let log_id = schema_builder.add_text_field("log_id", STRING | STORED);
        field_map.insert("log_id".to_string(), log_id);

        // Add extracted attr fields dynamically
        for field_name in extracted_field_names {
            let field = schema_builder.add_text_field(field_name, TEXT);
            field_map.insert(field_name.clone(), field);
        }

        (schema_builder.build(), field_map)
    }

    /// Index a single row from the RecordBatch into Tantivy
    fn index_row_in_tantivy(
        batch: &RecordBatch,
        row: usize,
        writer: &mut IndexWriter,
        field_map: &HashMap<String, Field>,
        extracted_field_names: &[String],
    ) -> Result<(), String> {
        let mut doc = TantivyDocument::default();

        // Add severity_text field
        if let Some(field) = field_map.get("severity_text") {
            if let Some(col) = batch.column_by_name("severity_text") {
                if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                    if !arr.is_null(row) {
                        doc.add_text(*field, arr.value(row));
                    }
                }
            }
        }

        // Add body field for full-text search
        if let Some(field) = field_map.get("body") {
            if let Some(col) = batch.column_by_name("body") {
                if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                    if !arr.is_null(row) {
                        doc.add_text(*field, arr.value(row));
                    }
                }
            }
        }

        // Add log_id for returning search results
        if let Some(field) = field_map.get("log_id") {
            if let Some(col) = batch.column_by_name("log_id") {
                if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                    if !arr.is_null(row) {
                        doc.add_text(*field, arr.value(row));
                    }
                }
            }
        }

        // Add extracted attr fields
        for field_name in extracted_field_names {
            if let Some(field) = field_map.get(field_name) {
                let col_name = format!("attr_{}", field_name);
                if let Some(col) = batch.column_by_name(&col_name) {
                    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                        if !arr.is_null(row) {
                            doc.add_text(*field, arr.value(row));
                        }
                    }
                }
            }
        }

        writer
            .add_document(doc)
            .map_err(|e| format!("Failed to add document to Tantivy: {}", e))?;

        Ok(())
    }

    /// Build bloom filters for high-cardinality fields from a RecordBatch
    fn build_bloom_filters_from_batch(
        bloom_filters: &mut BloomFilterSet,
        batch: &RecordBatch,
        fpr: f64,
    ) {
        // trace_id
        if let Some(col) = batch.column_by_name("trace_id") {
            if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                let values: Vec<String> = (0..arr.len())
                    .filter_map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i).to_string())
                        }
                    })
                    .collect();
                if !values.is_empty() {
                    bloom_filters.build("trace_id".to_string(), values, fpr);
                }
            }
        }

        // span_id
        if let Some(col) = batch.column_by_name("span_id") {
            if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                let values: Vec<String> = (0..arr.len())
                    .filter_map(|i| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i).to_string())
                        }
                    })
                    .collect();
                if !values.is_empty() {
                    bloom_filters.build("span_id".to_string(), values, fpr);
                }
            }
        }
    }

    /// Check if a trace_id might be present (bloom filter check)
    pub fn check_trace_id(&self, trace_id: &str) -> bool {
        self.bloom_filters.check("trace_id", trace_id)
    }

    /// Check if a span_id might be present (bloom filter check)
    pub fn check_span_id(&self, span_id: &str) -> bool {
        self.bloom_filters.check("span_id", span_id)
    }

    /// Get candidate chunks for a text search query
    ///
    /// Returns indices of 10K-record chunks that might contain the query.
    pub fn candidate_chunks_for_search(&self, query: &str) -> Vec<usize> {
        self.trigram_index.candidate_chunks(query)
    }

    /// Search body text using Tantivy full-text search
    ///
    /// Returns a set of log_ids that match the search query
    pub fn search_body(
        &self,
        query_text: &str,
    ) -> Result<std::collections::HashSet<String>, String> {
        use std::collections::HashSet;

        let reader = self
            .tantivy_index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| format!("Failed to create Tantivy reader: {}", e))?;

        let searcher = reader.searcher();
        let schema = self.tantivy_index.schema();

        // Get body and log_id fields
        let body_field = schema
            .get_field("body")
            .map_err(|e| format!("Body field not found: {}", e))?;
        let log_id_field = schema
            .get_field("log_id")
            .map_err(|e| format!("log_id field not found: {}", e))?;

        // Build query for body field
        let query_parser =
            tantivy::query::QueryParser::for_index(&self.tantivy_index, vec![body_field]);
        let query = query_parser
            .parse_query(query_text)
            .map_err(|e| format!("Failed to parse query: {}", e))?;

        // Search with a high limit (we want all matching log_ids)
        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(10_000))
            .map_err(|e| format!("Search failed: {}", e))?;

        // Extract log_ids from results
        let mut log_ids = HashSet::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher
                .doc(doc_address)
                .map_err(|e| format!("Failed to retrieve document: {}", e))?;

            if let Some(log_id_value) = retrieved_doc.get_first(log_id_field) {
                if let Some(log_id) = log_id_value.as_str() {
                    log_ids.insert(log_id.to_string());
                }
            }
        }

        Ok(log_ids)
    }

    /// Query the Tantivy index for a field value
    ///
    /// Returns true if any records match the query
    pub fn check_field(&self, field: &str, value: &str) -> Result<bool, String> {
        let reader = self
            .tantivy_index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| format!("Failed to create Tantivy reader: {}", e))?;

        let searcher = reader.searcher();
        let schema = self.tantivy_index.schema();

        // Get field
        let field_obj = schema
            .get_field(field)
            .map_err(|e| format!("Field '{}' not found in schema: {}", field, e))?;

        // Build query
        let query_parser =
            tantivy::query::QueryParser::for_index(&self.tantivy_index, vec![field_obj]);
        let query = query_parser
            .parse_query(value)
            .map_err(|e| format!("Failed to parse query: {}", e))?;

        // Search (just check if any docs match)
        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(1))
            .map_err(|e| format!("Search failed: {}", e))?;

        Ok(!top_docs.is_empty())
    }

    /// Serialize the companion index to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        // Serialize each component
        let bloom_bytes = self.bloom_filters.serialize()?;
        let trigram_bytes = self.trigram_index.serialize()?;

        // Combine (for now, simple concatenation with length prefixes)
        // In production, use a proper serialization format
        let combined =
            bincode::serialize(&(bloom_bytes, trigram_bytes, self.metadata.record_count))
                .map_err(|e| format!("Failed to serialize log index: {}", e))?;

        Ok(combined)
    }

    /// Convert this index into serialized bytes for embedding in a Vortex file.
    ///
    /// Returns a `CompanionIndexBytes` struct containing:
    /// - Bloom filter bytes
    /// - Trigram index bytes
    /// - Tantivy index files as (filename, bytes) pairs
    pub fn into_companion_bytes(
        self,
    ) -> Result<crate::indexed_layout::strategy::CompanionIndexBytes, String> {
        let bloom_bytes = self.bloom_filters.serialize()?;
        let trigram_bytes = self.trigram_index.serialize()?;

        // Collect all Tantivy index files from the temp directory
        let tantivy_files = if let Some(ref dir) = self._tantivy_dir {
            super::collect_tantivy_files(dir.path())?
        } else {
            Vec::new()
        };

        Ok(crate::indexed_layout::strategy::CompanionIndexBytes {
            bloom_bytes,
            trigram_bytes,
            tantivy_files,
        })
    }

    /// Get the trigram index
    pub fn trigram_index(&self) -> &TrigramIndex {
        &self.trigram_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringViewArray, TimestampNanosecondArray, UInt32Array, UInt8Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Build a minimal log RecordBatch for testing.
    /// rows: (log_id, severity_text, body)
    fn make_log_batch(rows: &[(&str, &str, &str)]) -> RecordBatch {
        let n = rows.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("log_id", DataType::Utf8View, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "observed_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8View, false),
            Field::new("severity_text", DataType::Utf8View, false),
            Field::new("severity_number", DataType::UInt8, false),
            Field::new("body", DataType::Utf8View, false),
            Field::new("trace_id", DataType::Utf8View, true),
            Field::new("span_id", DataType::Utf8View, true),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )) as _,
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64; n])) as _,
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64; n])) as _,
                Arc::new(StringViewArray::from(vec!["unknown"; n])) as _,
                Arc::new(StringViewArray::from(
                    rows.iter().map(|(_, sev, _)| *sev).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt8Array::from(vec![9u8; n])) as _, // Info = 9
                Arc::new(StringViewArray::from(
                    rows.iter().map(|(_, _, body)| *body).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
                Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
                Arc::new(UInt32Array::from(vec![0u32; n])) as _,
                Arc::new(UInt32Array::from(vec![0u32; n])) as _,
            ],
        )
        .unwrap()
    }

    /// Build a log batch with a specific trace_id on one row
    fn make_log_batch_with_trace(n: usize, trace_idx: usize, trace_hex: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("log_id", DataType::Utf8View, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "observed_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8View, false),
            Field::new("severity_text", DataType::Utf8View, false),
            Field::new("severity_number", DataType::UInt8, false),
            Field::new("body", DataType::Utf8View, false),
            Field::new("trace_id", DataType::Utf8View, true),
            Field::new("span_id", DataType::Utf8View, true),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
        ]));

        let log_ids: Vec<String> = (0..n).map(|i| format!("log{:04}", i)).collect();
        let bodies: Vec<String> = (0..n).map(|i| format!("message {}", i)).collect();
        let trace_ids: Vec<Option<&str>> = (0..n)
            .map(|i| {
                if i == trace_idx {
                    Some(trace_hex)
                } else {
                    None
                }
            })
            .collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(
                    log_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64; n])) as _,
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64; n])) as _,
                Arc::new(StringViewArray::from(vec!["unknown"; n])) as _,
                Arc::new(StringViewArray::from(vec!["INFO"; n])) as _,
                Arc::new(UInt8Array::from(vec![9u8; n])) as _,
                Arc::new(StringViewArray::from(
                    bodies.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringViewArray::from(trace_ids)) as _,
                Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
                Arc::new(UInt32Array::from(vec![0u32; n])) as _,
                Arc::new(UInt32Array::from(vec![0u32; n])) as _,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_log_index_build() {
        let batch = make_log_batch(&[
            ("log1", "ERROR", "error connecting to database"),
            ("log2", "WARN", "warning: high memory usage"),
            ("log3", "INFO", "request completed successfully"),
        ]);

        let index = LogCompanionIndex::build_for_batch(&batch).unwrap();

        assert_eq!(index.metadata.record_count, 3);
    }

    #[test]
    fn test_log_index_check_field() {
        let batch = make_log_batch(&[
            ("log1", "ERROR", "error message"),
            ("log2", "INFO", "info message"),
        ]);

        let index = LogCompanionIndex::build_for_batch(&batch).unwrap();

        // Check severity field
        assert!(index.check_field("severity_text", "ERROR").unwrap());
        assert!(index.check_field("severity_text", "INFO").unwrap());
    }

    #[test]
    fn test_log_index_bloom_filter_trace_id() {
        let batch = make_log_batch_with_trace(1000, 0, "0123456789abcdef0123456789abcdef");

        let index = LogCompanionIndex::build_for_batch(&batch).unwrap();

        // Check that the indexed trace_id is always found
        assert!(index.check_trace_id("0123456789abcdef0123456789abcdef"));

        // Note: we don't assert on false-positive absence — bloom filters are probabilistic
        // and a 1% FPR means occasional false positives are expected.
    }

    #[test]
    fn test_log_index_trigram_search() {
        let batch = make_log_batch(&[
            ("log1", "ERROR", "error connecting to database"),
            ("log2", "WARN", "warning: high memory usage"),
            ("log3", "INFO", "request completed successfully"),
        ]);

        let index = LogCompanionIndex::build_for_batch(&batch).unwrap();

        // Search for "error" - should find candidates
        let candidates = index.candidate_chunks_for_search("error");
        assert_eq!(candidates.len(), 1); // All logs in one chunk
        assert_eq!(candidates[0], 0);

        // Search for "database"
        let candidates = index.candidate_chunks_for_search("database");
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn test_log_index_serialization() {
        let batch = make_log_batch(&[("log1", "INFO", "test message")]);

        let index = LogCompanionIndex::build_for_batch(&batch).unwrap();

        let bytes = index.serialize().unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_log_index_empty_logs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("log_id", DataType::Utf8View, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "observed_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8View, false),
            Field::new("severity_text", DataType::Utf8View, false),
            Field::new("severity_number", DataType::UInt8, false),
            Field::new("body", DataType::Utf8View, false),
            Field::new("trace_id", DataType::Utf8View, true),
            Field::new("span_id", DataType::Utf8View, true),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
        ]));
        let empty = RecordBatch::new_empty(Arc::new((*schema).clone()));

        let result = LogCompanionIndex::build_for_batch(&empty);
        assert!(result.is_err());
    }

    #[test]
    fn test_log_index_body_search() {
        let batch = make_log_batch(&[
            ("log1", "ERROR", "error connecting to database"),
            ("log2", "WARN", "warning: high memory usage"),
            ("log3", "INFO", "request completed successfully"),
            ("log4", "ERROR", "database connection timeout"),
        ]);

        let index = LogCompanionIndex::build_for_batch(&batch).unwrap();

        // Search for "database" - should find first and last logs
        let results = index.search_body("database").unwrap();
        assert_eq!(results.len(), 2, "Expected 2 logs with 'database' in body");

        // Verify the log_ids are correct
        assert!(results.contains("log1"));
        assert!(results.contains("log4"));

        // Search for "memory" - should find second log
        let results = index.search_body("memory").unwrap();
        assert_eq!(results.len(), 1, "Expected 1 log with 'memory' in body");
        assert!(results.contains("log2"));

        // Search for non-existent term
        let results = index.search_body("nonexistent").unwrap();
        assert_eq!(results.len(), 0, "Expected 0 logs for non-existent term");
    }

    #[test]
    fn test_log_index_body_search_phrase() {
        let batch = make_log_batch(&[
            ("log1", "ERROR", "connection error occurred"),
            ("log2", "ERROR", "error in connection pool"),
            ("log3", "INFO", "successful connection"),
        ]);

        let index = LogCompanionIndex::build_for_batch(&batch).unwrap();

        // Search for both words - should find all three (OR query by default)
        let results = index.search_body("connection error").unwrap();
        assert!(
            results.len() >= 2,
            "Expected at least 2 logs matching 'connection error'"
        );

        // Search for exact phrase (if Tantivy supports it with quotes)
        let results = index.search_body("\"connection error\"").unwrap();
        // Exact phrase should match only the first log
        assert_eq!(
            results.len(),
            1,
            "Expected 1 log with exact phrase 'connection error'"
        );
        assert!(results.contains("log1"));
    }
}
