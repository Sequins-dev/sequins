//! Delta emitter - converts RecordBatch to FlightData
//!
//! Produces incremental FlightData frames from RecordBatch data for live query subscribers.

use arrow::array::RecordBatch;
use arrow_flight::FlightData;
use sequins_query::ast::Signal;
use sequins_query::flight::{
    append_flight_data, expire_flight_data, replace_flight_data, update_flight_data,
};

/// Converts RecordBatch data into FlightData frames for live queries
#[derive(Clone, Copy)]
pub struct DeltaEmitter {
    /// Signal type being queried (reserved for future use)
    #[allow(dead_code)]
    signal: Signal,
}

impl DeltaEmitter {
    /// Create a new DeltaEmitter for the given signal type
    pub fn new(signal: Signal) -> Self {
        Self { signal }
    }

    /// Emit an append FlightData frame from a RecordBatch.
    ///
    /// The entire batch becomes a single Append message with start_row_id = seq.
    /// The watermark is set to the given sequence number.
    /// Returns `None` for empty batches (nothing to append).
    pub fn emit_append(&self, seq: u64, batch: &RecordBatch) -> Option<FlightData> {
        if batch.num_rows() == 0 {
            return None;
        }
        Some(append_flight_data(None, batch, seq, seq))
    }

    /// Emit a replace FlightData frame from a RecordBatch.
    ///
    /// The entire batch replaces the client's current view.
    pub fn emit_replace(&self, seq: u64, batch: &RecordBatch) -> FlightData {
        replace_flight_data(None, batch, seq)
    }

    /// Emit an update FlightData frame for a specific row.
    ///
    /// The `batch` is a single-row `RecordBatch` containing only the changed columns.
    pub fn emit_update(&self, seq: u64, row_id: u64, batch: &RecordBatch) -> FlightData {
        update_flight_data(None, batch, row_id, seq)
    }

    /// Emit an expire FlightData frame for a row that has left the time window.
    pub fn emit_expire(&self, seq: u64, row_id: u64) -> FlightData {
        expire_flight_data(None, row_id, seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("alice"), None, Some("charlie")]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_emit_append() {
        let emitter = DeltaEmitter::new(Signal::Spans);
        let batch = create_test_batch();

        let fd = emitter
            .emit_append(100, &batch)
            .expect("non-empty batch should produce FlightData");

        // app_metadata decodes to Append
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Append {
                table,
                start_row_id,
                watermark_ns,
            } => {
                assert_eq!(table, None);
                assert_eq!(start_row_id, 100);
                assert_eq!(watermark_ns, 100);
            }
            other => panic!("Expected Append, got {:?}", other),
        }
        // data_body contains the batch IPC
        assert!(!fd.data_body.is_empty());
    }

    #[test]
    fn test_emit_update() {
        let emitter = DeltaEmitter::new(Signal::Spans);

        let update_batch = create_test_batch();
        let fd = emitter.emit_update(200, 123, &update_batch);

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Update {
                row_id,
                watermark_ns,
                ..
            } => {
                assert_eq!(row_id, 123);
                assert_eq!(watermark_ns, 200);
            }
            other => panic!("Expected Update, got {:?}", other),
        }
        assert!(!fd.data_body.is_empty());
    }

    #[test]
    fn test_emit_append_empty_batch_returns_none() {
        let emitter = DeltaEmitter::new(Signal::Logs);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let empty_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))])
                .unwrap();

        let result = emitter.emit_append(50, &empty_batch);
        assert!(result.is_none(), "empty batch should return None");
    }

    #[test]
    fn test_emit_replace() {
        let emitter = DeltaEmitter::new(Signal::Metrics);
        let batch = create_test_batch();

        let fd = emitter.emit_replace(42, &batch);

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Replace {
                table,
                watermark_ns,
            } => {
                assert_eq!(table, None);
                assert_eq!(watermark_ns, 42);
            }
            other => panic!("Expected Replace, got {:?}", other),
        }
        assert!(!fd.data_body.is_empty());
    }

    #[test]
    fn test_emit_replace_empty_batch() {
        let emitter = DeltaEmitter::new(Signal::Spans);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let empty_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))])
                .unwrap();

        // Replace always emits FlightData even for empty batches (client needs to clear its view)
        let fd = emitter.emit_replace(99, &empty_batch);
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert!(matches!(meta, SeqlMetadata::Replace { .. }));
    }

    #[test]
    fn test_emit_expire() {
        let emitter = DeltaEmitter::new(Signal::Spans);
        let fd = emitter.emit_expire(77, 42);

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Expire {
                table,
                row_id,
                watermark_ns,
            } => {
                assert_eq!(table, None);
                assert_eq!(row_id, 42);
                assert_eq!(watermark_ns, 77);
            }
            other => panic!("Expected Expire, got {:?}", other),
        }
        assert!(fd.data_body.is_empty());
    }

    #[test]
    fn test_emit_append_multi_row_batch() {
        use arrow::array::{BooleanArray, Float64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("is_error", DataType::Boolean, true),
            Field::new("duration", DataType::Float64, false),
            Field::new("service", DataType::Utf8, false),
        ]));

        let bool_array = BooleanArray::from(vec![Some(true), None]);
        let float_array = Float64Array::from(vec![1.5, 2.5]);
        let str_array = StringArray::from(vec!["api", "worker"]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(bool_array),
                Arc::new(float_array),
                Arc::new(str_array),
            ],
        )
        .unwrap();

        let emitter = DeltaEmitter::new(Signal::Spans);
        let fd = emitter.emit_append(1, &batch).unwrap();

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Append { start_row_id, .. } => {
                assert_eq!(start_row_id, 1);
            }
            other => panic!("Expected Append, got {:?}", other),
        }
        assert!(!fd.data_body.is_empty());
    }
}
