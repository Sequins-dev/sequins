//! RecordBatch writing and sorting utilities

use super::cold_tier::ColdTier;
use crate::error::{Error, Result};
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use object_store::path::Path as ObjectPath;
use std::sync::Arc;

impl ColdTier {
    pub(crate) async fn write_record_batch(
        &self,
        batch: RecordBatch,
        _schema: Arc<Schema>,
        path: &str,
        companion_bytes: Option<crate::indexed_layout::strategy::CompanionIndexBytes>,
    ) -> Result<()> {
        use futures::stream;
        use vortex::array::arrow::FromArrowArray;
        use vortex::array::stream::ArrayStreamAdapter;
        use vortex::array::ArrayRef;
        use vortex::dtype::arrow::FromArrowType;
        use vortex::dtype::DType;
        use vortex::error::VortexResult;
        use vortex::file::WriteOptionsSessionExt;
        use vortex::io::{ObjectStoreWriter, VortexWrite};
        use vortex::layout::LayoutStrategy;
        use vortex::session::VortexSession;
        use vortex::VortexSessionDefault;

        use crate::indexed_layout::strategy::IndexedLayoutStrategy;

        // Create a Vortex session for encoding
        let session = VortexSession::default();

        // Build the inner write strategy from config.
        let inner_strategy: std::sync::Arc<dyn LayoutStrategy> = {
            use vortex::compressor::CompactCompressor;
            use vortex::file::WriteStrategyBuilder;
            let mut builder =
                WriteStrategyBuilder::new().with_row_block_size(self.config.row_block_size);
            if self.config.compact_encodings {
                builder = builder.with_compressor(CompactCompressor::default());
            }
            builder.build()
        };

        // Wrap with IndexedLayoutStrategy when companion index data is provided.
        let strategy: std::sync::Arc<dyn LayoutStrategy> = if companion_bytes.is_some() {
            std::sync::Arc::new(IndexedLayoutStrategy::new(inner_strategy, companion_bytes))
        } else {
            inner_strategy
        };

        // Get the Arrow schema to create the DType
        let arrow_schema = batch.schema();
        let dtype = DType::from_arrow(arrow_schema);

        // Convert Arrow RecordBatch to Vortex Array
        // The false parameter indicates not to preserve nullability
        let vortex_array = ArrayRef::from_arrow(batch, false);

        // Create a stream from the single array
        let stream = stream::once(async move { VortexResult::Ok(vortex_array) });
        let array_stream = ArrayStreamAdapter::new(dtype, stream);

        // Create an ObjectStoreWriter for the target path
        let object_path = ObjectPath::from(path);
        let mut writer = ObjectStoreWriter::new(self.store.clone(), &object_path)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create Vortex writer: {}", e)))?;

        // Write to Vortex format using the configured layout strategy
        session
            .write_options()
            .with_strategy(strategy)
            .write(&mut writer, array_stream)
            .await
            .map_err(|e| Error::Storage(format!("Failed to write Vortex: {}", e)))?;

        // Shutdown the writer to flush all data
        writer
            .shutdown()
            .await
            .map_err(|e| Error::Storage(format!("Failed to shutdown Vortex writer: {}", e)))?;

        Ok(())
    }

    /// Sort a RecordBatch by a named column (ascending).
    ///
    /// Used before writing to cold tier so that zone maps cover meaningful time ranges
    /// and Vortex can apply delta encoding on sorted timestamp columns.
    pub(crate) fn sort_batch_by_column(batch: RecordBatch, col_name: &str) -> Result<RecordBatch> {
        use arrow::compute::{sort_to_indices, take, SortOptions};

        debug_assert!(
            batch.schema().column_with_name(col_name).is_some(),
            "sort_batch_by_column: column '{}' does not exist in schema {:?}",
            col_name,
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        let col_idx = batch
            .schema()
            .column_with_name(col_name)
            .map(|(i, _)| i)
            .ok_or_else(|| Error::Storage(format!("Column '{}' not found for sort", col_name)))?;

        let col = batch.column(col_idx);
        let opts = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let indices = sort_to_indices(col, Some(opts), None)
            .map_err(|e| Error::Storage(format!("Failed to sort by '{}': {}", col_name, e)))?;

        let sorted_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &indices, None)
                    .map_err(|e| Error::Storage(format!("Failed to take sorted indices: {}", e)))
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(batch.schema(), sorted_columns).map_err(Error::Arrow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]))
    }

    fn make_batch(ts: Vec<i64>, labels: Vec<Option<&str>>) -> RecordBatch {
        let schema = make_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ts)) as ArrayRef,
                Arc::new(StringArray::from(labels)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_sort_batch_by_column_ascending() {
        let batch = make_batch(vec![30, 10, 20], vec![Some("c"), Some("a"), Some("b")]);
        let sorted = ColdTier::sort_batch_by_column(batch, "ts").unwrap();
        let ts = sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts.values(), &[10, 20, 30]);
    }

    #[test]
    fn test_sort_batch_preserves_row_associations() {
        let batch = make_batch(vec![30, 10, 20], vec![Some("c"), Some("a"), Some("b")]);
        let sorted = ColdTier::sort_batch_by_column(batch, "ts").unwrap();
        let labels = sorted
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // After sorting by ts ascending: 10→"a", 20→"b", 30→"c"
        assert_eq!(labels.value(0), "a");
        assert_eq!(labels.value(1), "b");
        assert_eq!(labels.value(2), "c");
    }

    /// In debug builds the `debug_assert!` fires before the error path is reached,
    /// so we expect a panic.  In release builds the function returns `Err`.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "sort_batch_by_column: column 'nonexistent' does not exist")]
    fn test_sort_batch_missing_column() {
        let batch = make_batch(vec![1, 2], vec![None, None]);
        let _ = ColdTier::sort_batch_by_column(batch, "nonexistent");
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn test_sort_batch_missing_column_release() {
        let batch = make_batch(vec![1, 2], vec![None, None]);
        let result = ColdTier::sort_batch_by_column(batch, "nonexistent");
        assert!(
            result.is_err(),
            "missing column should return an error in release builds"
        );
    }

    #[test]
    fn test_sort_batch_with_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(30), None, Some(10)])) as ArrayRef],
        )
        .unwrap();
        let sorted = ColdTier::sort_batch_by_column(batch, "ts").unwrap();
        let ts = sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // nulls_first=false means nulls sort to end
        assert_eq!(ts.value(0), 10);
        assert_eq!(ts.value(1), 30);
        assert!(ts.is_null(2));
    }
}
