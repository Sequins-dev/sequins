use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Schema frame — describes the shape and columns of the result
///
/// Kept as a metadata type used by `SeqlMetadata::Schema`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaFrame {
    /// Result shape (table, time-series, etc.)
    pub shape: crate::schema::ResponseShape,
    /// Column definitions in order
    pub columns: Vec<crate::schema::ColumnDef>,
    /// Watermark at query start time (nanoseconds since epoch)
    pub initial_watermark_ns: u64,
}

/// Heartbeat frame — sent periodically for live queries to confirm the subscription is alive
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatFrame {
    /// Current watermark (nanoseconds since epoch)
    pub watermark_ns: u64,
}

/// Warning frame — non-fatal warning during query execution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WarningFrame {
    /// Warning code
    pub code: crate::error::WarningCode,
    /// Human-readable message
    pub message: String,
}

/// Detailed execution statistics attached to the Complete message
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryStats {
    /// Wall-clock execution time in microseconds
    pub execution_time_us: u64,
    /// Total rows scanned before filtering
    pub rows_scanned: u64,
    /// Total bytes read from storage
    pub bytes_read: u64,
    /// Number of rows in the final result
    pub rows_returned: u64,
    /// Number of warnings emitted
    pub warning_count: u32,
}

impl QueryStats {
    /// Zero-valued stats
    pub fn zero() -> Self {
        Self {
            execution_time_us: 0,
            rows_scanned: 0,
            bytes_read: 0,
            rows_returned: 0,
            warning_count: 0,
        }
    }
}

// ── IPC helpers ───────────────────────────────────────────────────────────────

/// Serialize a RecordBatch to Arrow IPC stream bytes.
pub fn batch_to_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Ok(mut writer) = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()) {
        let _ = writer.write(batch);
        let _ = writer.finish();
    }
    buf
}

/// Deserialize a RecordBatch from Arrow IPC stream bytes.
pub fn ipc_to_batch(bytes: &[u8]) -> Result<RecordBatch, arrow::error::ArrowError> {
    use arrow::ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    reader
        .next()
        .ok_or_else(|| arrow::error::ArrowError::IpcError("empty IPC stream".into()))?
}

/// Create an empty RecordBatch for a given schema (zero rows).
pub fn empty_batch(schema: Arc<arrow::datatypes::Schema>) -> RecordBatch {
    RecordBatch::new_empty(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("count", ArrowDataType::UInt64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("api"), Some("worker")])),
                Arc::new(UInt64Array::from(vec![Some(10u64), Some(5u64)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn query_stats_zero() {
        let s = QueryStats::zero();
        assert_eq!(s.execution_time_us, 0);
        assert_eq!(s.rows_scanned, 0);
    }

    #[test]
    fn ipc_round_trip() {
        let batch = make_test_batch();
        let bytes = batch_to_ipc(&batch);
        assert!(!bytes.is_empty());
        let recovered = ipc_to_batch(&bytes).unwrap();
        assert_eq!(recovered.num_rows(), 2);
        assert_eq!(recovered.num_columns(), 2);
    }
}
