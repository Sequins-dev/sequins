use crate::error::QueryError;
use crate::flight::{decode_metadata, SeqlMetadata};
use crate::frame::{ipc_to_batch, QueryStats, SchemaFrame, WarningFrame};
use arrow::array::{
    Array, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, StringArray, StringViewArray, TimestampNanosecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightData;
use indexmap::IndexMap;
use serde_json::Value;

/// A single row of data, stored as a vector of JSON values
pub type Row = Vec<Value>;

/// Sink that receives typed callbacks as frames arrive
pub trait FrameSink: Send {
    /// Called once with the result schema
    fn on_schema(&self, schema: &SchemaFrame);
    /// Called when rows are inserted (row_id → row values)
    fn on_rows_inserted(&self, rows: &[(u64, Row)]);
    /// Called when rows are updated (row_id → changed (column_index, value) pairs)
    fn on_rows_updated(&self, updates: &[(u64, Vec<(u16, Value)>)]);
    /// Called when rows are removed
    fn on_rows_removed(&self, row_ids: &[u64]);
    /// Called when the query completes
    fn on_complete(&self, stats: &QueryStats);
    /// Called for non-fatal warnings
    fn on_warning(&self, warning: &WarningFrame);
    /// Called if the query terminates with an error
    fn on_error(&self, error: &QueryError);
}

/// Reduces a stream of [`ResponseFrame`]s into typed [`FrameSink`] callbacks
///
/// `FrameReducer` maintains the current row set and watermark, translating
/// incremental delta frames into fine-grained insert/update/remove callbacks.
pub struct FrameReducer {
    schema: Option<SchemaFrame>,
    rows: IndexMap<u64, Row>,
    watermark_ns: u64,
    sink: Box<dyn FrameSink>,
}

impl FrameReducer {
    /// Create a new reducer backed by the given sink
    pub fn new(sink: Box<dyn FrameSink>) -> Self {
        Self {
            schema: None,
            rows: IndexMap::new(),
            watermark_ns: 0,
            sink,
        }
    }

    /// Process one FlightData frame
    ///
    /// Decodes the SeqlMetadata from `app_metadata` and dispatches to the
    /// appropriate sink callbacks. Unknown or undecodable frames are silently ignored.
    pub fn feed(&mut self, flight_data: &FlightData) {
        let metadata = match decode_metadata(&flight_data.app_metadata) {
            Some(m) => m,
            None => return,
        };

        match metadata {
            SeqlMetadata::Schema {
                table: _,
                shape,
                columns,
                watermark_ns,
            } => {
                let schema = SchemaFrame {
                    shape,
                    columns,
                    initial_watermark_ns: watermark_ns,
                };
                self.schema = Some(schema.clone());
                self.sink.on_schema(&schema);
            }
            SeqlMetadata::Data { table: _ } => {
                debug_assert!(
                    self.schema.is_some(),
                    "Data frame received before Schema frame — reducer state is invalid"
                );
                let batch = match ipc_to_batch(&flight_data.data_body) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let rows_vec = batch_to_rows(&batch);
                let mut inserted: Vec<(u64, Row)> = Vec::with_capacity(rows_vec.len());
                for row in rows_vec {
                    let row_id = self.rows.len() as u64;
                    self.rows.insert(row_id, row.clone());
                    inserted.push((row_id, row));
                }
                if !inserted.is_empty() {
                    self.sink.on_rows_inserted(&inserted);
                }
            }
            SeqlMetadata::Append {
                table: _,
                start_row_id,
                watermark_ns,
            } => {
                self.watermark_ns = watermark_ns;
                let batch = match ipc_to_batch(&flight_data.data_body) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let rows_vec = batch_to_rows(&batch);
                let mut inserted: Vec<(u64, Row)> = Vec::with_capacity(rows_vec.len());
                for (i, row) in rows_vec.into_iter().enumerate() {
                    let row_id = start_row_id + i as u64;
                    self.rows.insert(row_id, row.clone());
                    inserted.push((row_id, row));
                }
                if !inserted.is_empty() {
                    self.sink.on_rows_inserted(&inserted);
                }
            }
            SeqlMetadata::Update {
                table: _,
                row_id,
                watermark_ns,
            } => {
                self.watermark_ns = watermark_ns;
                let batch = match ipc_to_batch(&flight_data.data_body) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let batch_schema = batch.schema();
                let col_changes: Vec<(u16, Value)> = if let Some(schema) = &self.schema {
                    batch_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .filter_map(|(batch_col_idx, field)| {
                            schema
                                .columns
                                .iter()
                                .position(|c| c.name.as_str() == field.name())
                                .map(|outer_idx| {
                                    let value =
                                        col_value_to_json(batch.column(batch_col_idx).as_ref(), 0);
                                    (outer_idx as u16, value)
                                })
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                if let Some(row) = self.rows.get_mut(&row_id) {
                    for (col_idx, val) in &col_changes {
                        if let Some(cell) = row.get_mut(*col_idx as usize) {
                            *cell = val.clone();
                        }
                    }
                }
                if !col_changes.is_empty() {
                    self.sink.on_rows_updated(&[(row_id, col_changes)]);
                }
            }
            SeqlMetadata::Expire {
                table: _,
                row_id,
                watermark_ns,
            } => {
                self.watermark_ns = watermark_ns;
                self.rows.shift_remove(&row_id);
                self.sink.on_rows_removed(&[row_id]);
            }
            SeqlMetadata::Replace {
                table: _,
                watermark_ns,
            } => {
                self.watermark_ns = watermark_ns;
                self.rows.clear();
                let batch = match ipc_to_batch(&flight_data.data_body) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let rows_vec = batch_to_rows(&batch);
                let mut inserted: Vec<(u64, Row)> = Vec::with_capacity(rows_vec.len());
                for (row_idx, row) in rows_vec.into_iter().enumerate() {
                    let row_id = row_idx as u64;
                    self.rows.insert(row_id, row.clone());
                    inserted.push((row_id, row));
                }
                if !inserted.is_empty() {
                    self.sink.on_rows_inserted(&inserted);
                }
            }
            SeqlMetadata::Heartbeat { watermark_ns } => {
                self.watermark_ns = watermark_ns;
            }
            SeqlMetadata::Complete { stats } => {
                self.sink.on_complete(&stats);
            }
            SeqlMetadata::Warning { code, message } => {
                use crate::error::WarningCode;
                // code is u32 in SeqlMetadata; map to WarningCode enum
                let warning_code = match code {
                    1 => WarningCode::SlowQuery,
                    2 => WarningCode::ApproximateResult,
                    3 => WarningCode::SchemaResolutionFallback,
                    _ => WarningCode::ResultTruncated,
                };
                self.sink.on_warning(&WarningFrame {
                    code: warning_code,
                    message,
                });
            }
        }
    }

    /// Current row count
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Current watermark in nanoseconds
    pub fn watermark_ns(&self) -> u64 {
        self.watermark_ns
    }

    /// Snapshot of the current row set as row_id → row pairs
    pub fn rows(&self) -> &IndexMap<u64, Row> {
        &self.rows
    }
}

// ── RecordBatch → Row conversion ─────────────────────────────────────────────

/// Convert a RecordBatch into a Vec of Row (each Row is a Vec<serde_json::Value>).
pub fn batch_to_rows(batch: &RecordBatch) -> Vec<Row> {
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let row: Row = batch
            .columns()
            .iter()
            .map(|col| col_value_to_json(col.as_ref(), row_idx))
            .collect();
        rows.push(row);
    }
    rows
}

/// Extract a single cell from an Arrow array as a JSON Value.
fn col_value_to_json(array: &dyn Array, row_idx: usize) -> Value {
    if array.is_null(row_idx) {
        return Value::Null;
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(row_idx))
        }
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(arr.value(row_idx).into())
        }
        ArrowDataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap();
            serde_json::Number::from_f64(arr.value(row_idx) as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(arr.value(row_idx))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(row_idx).to_string())
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(arr.value(row_idx).to_string())
        }
        ArrowDataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            Value::String(arr.value(row_idx).to_string())
        }
        ArrowDataType::Timestamp(_, _) => {
            if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                Value::Number(arr.value(row_idx).into())
            } else {
                Value::Null
            }
        }
        ArrowDataType::Duration(_) => {
            // Duration stored as i64 nanoseconds — emit as number
            if let Some(arr) = array
                .as_any()
                .downcast_ref::<arrow::array::DurationNanosecondArray>()
            {
                Value::Number(arr.value(row_idx).into())
            } else {
                Value::Null
            }
        }
        ArrowDataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .unwrap();
            use base64::Engine as _;
            Value::String(base64::engine::general_purpose::STANDARD.encode(arr.value(row_idx)))
        }
        ArrowDataType::List(_) => {
            use arrow::array::ListArray;
            if let Some(list_arr) = array.as_any().downcast_ref::<ListArray>() {
                let value_arr = list_arr.value(row_idx);
                let items: Vec<Value> = (0..value_arr.len())
                    .map(|i| col_value_to_json(value_arr.as_ref(), i))
                    .collect();
                Value::Array(items)
            } else {
                Value::Null
            }
        }
        ArrowDataType::LargeList(_) => {
            use arrow::array::LargeListArray;
            if let Some(list_arr) = array.as_any().downcast_ref::<LargeListArray>() {
                let value_arr = list_arr.value(row_idx);
                let items: Vec<Value> = (0..value_arr.len())
                    .map(|i| col_value_to_json(value_arr.as_ref(), i))
                    .collect();
                Value::Array(items)
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flight::{
        append_flight_data, complete_flight_data, data_flight_data, expire_flight_data,
        heartbeat_flight_data, replace_flight_data, schema_flight_data, update_flight_data,
    };
    use crate::schema::{ColumnDef, ColumnRole, DataType, ResponseShape};
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use std::sync::{Arc, Mutex};

    /// A mock sink that records all callbacks
    struct MockSink {
        schemas: Arc<Mutex<Vec<SchemaFrame>>>,
        inserted: Arc<Mutex<Vec<(u64, Row)>>>,
        removed: Arc<Mutex<Vec<u64>>>,
        completed: Arc<Mutex<bool>>,
    }

    impl MockSink {
        #[allow(clippy::type_complexity)]
        fn new() -> (
            Self,
            Arc<Mutex<Vec<SchemaFrame>>>,
            Arc<Mutex<Vec<(u64, Row)>>>,
            Arc<Mutex<Vec<u64>>>,
            Arc<Mutex<bool>>,
        ) {
            let schemas = Arc::new(Mutex::new(vec![]));
            let inserted = Arc::new(Mutex::new(vec![]));
            let removed = Arc::new(Mutex::new(vec![]));
            let completed = Arc::new(Mutex::new(false));
            (
                MockSink {
                    schemas: schemas.clone(),
                    inserted: inserted.clone(),
                    removed: removed.clone(),
                    completed: completed.clone(),
                },
                schemas,
                inserted,
                removed,
                completed,
            )
        }
    }

    impl FrameSink for MockSink {
        fn on_schema(&self, schema: &SchemaFrame) {
            self.schemas.lock().unwrap().push(schema.clone());
        }
        fn on_rows_inserted(&self, rows: &[(u64, Row)]) {
            self.inserted.lock().unwrap().extend_from_slice(rows);
        }
        fn on_rows_updated(&self, _: &[(u64, Vec<(u16, Value)>)]) {}
        fn on_rows_removed(&self, row_ids: &[u64]) {
            self.removed.lock().unwrap().extend_from_slice(row_ids);
        }
        fn on_complete(&self, _: &QueryStats) {
            *self.completed.lock().unwrap() = true;
        }
        fn on_warning(&self, _: &WarningFrame) {}
        fn on_error(&self, _: &QueryError) {}
    }

    fn make_col_defs() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "name".into(),
                data_type: DataType::String,
                role: ColumnRole::Field,
            },
            ColumnDef {
                name: "count".into(),
                data_type: DataType::UInt64,
                role: ColumnRole::Aggregation,
            },
        ]
    }

    fn make_arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("count", ArrowDataType::UInt64, true),
        ]))
    }

    fn make_schema_fd() -> arrow_flight::FlightData {
        schema_flight_data(
            None,
            make_arrow_schema(),
            ResponseShape::Table,
            make_col_defs(),
            0,
        )
    }

    fn make_batch_2rows() -> RecordBatch {
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

    fn make_batch_1row() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("count", ArrowDataType::UInt64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("api")])),
                Arc::new(UInt64Array::from(vec![Some(10u64)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn schema_callback_fired() {
        let (sink, schemas, _, _, _) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        reducer.feed(&make_schema_fd());
        assert_eq!(schemas.lock().unwrap().len(), 1);
    }

    #[test]
    fn data_frame_inserts_rows() {
        let (sink, _, inserted, _, _) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        reducer.feed(&make_schema_fd());
        reducer.feed(&data_flight_data(None, &make_batch_2rows()));
        assert_eq!(reducer.row_count(), 2);
        assert_eq!(inserted.lock().unwrap().len(), 2);
    }

    #[test]
    fn delta_expire_removes_rows() {
        let (sink, _, _, removed, _) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        reducer.feed(&make_schema_fd());
        reducer.feed(&data_flight_data(None, &make_batch_1row()));
        reducer.feed(&expire_flight_data(None, 0, 1000));
        assert_eq!(reducer.row_count(), 0);
        assert_eq!(removed.lock().unwrap().len(), 1);
    }

    #[test]
    fn complete_fires_callback() {
        let (sink, _, _, _, completed) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        reducer.feed(&complete_flight_data(QueryStats::zero()));
        assert!(*completed.lock().unwrap());
    }

    #[test]
    fn test_delta_update_modifies_existing_row() {
        let (sink, _, _, _, _) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        reducer.feed(&make_schema_fd());
        reducer.feed(&data_flight_data(None, &make_batch_1row()));

        // Row 0 exists with name="api", count=10.
        // Build a single-row RecordBatch with just the "count" column changed to 99.
        let update_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            ArrowDataType::UInt64,
            true,
        )]));
        let update_batch = RecordBatch::try_new(
            update_schema,
            vec![Arc::new(UInt64Array::from(vec![Some(99u64)]))],
        )
        .unwrap();

        reducer.feed(&update_flight_data(None, &update_batch, 0, 500));

        let rows = reducer.rows();
        assert_eq!(rows.len(), 1);
        // Column index 1 = "count" in make_col_defs() / make_batch_1row()
        assert_eq!(rows[&0][1], serde_json::json!(99u64));
    }

    #[test]
    fn test_delta_replace_clears_and_replaces() {
        let (sink, _, _, _, _) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        reducer.feed(&make_schema_fd());
        reducer.feed(&data_flight_data(None, &make_batch_2rows()));
        assert_eq!(reducer.row_count(), 2);

        // Replace with a fresh 1-row batch
        reducer.feed(&replace_flight_data(None, &make_batch_1row(), 1000));

        // Old rows cleared, new rows from Replace
        assert_eq!(reducer.row_count(), 1);
        let rows = reducer.rows();
        assert!(rows.contains_key(&0));
    }

    #[test]
    fn test_heartbeat_updates_watermark() {
        let (sink, _, _, _, _) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        assert_eq!(reducer.watermark_ns(), 0);

        reducer.feed(&heartbeat_flight_data(42_000_000));
        assert_eq!(reducer.watermark_ns(), 42_000_000);
    }

    #[test]
    fn test_append_inserts_rows_with_correct_ids() {
        let (sink, _, inserted, _, _) = MockSink::new();
        let mut reducer = FrameReducer::new(Box::new(sink));
        reducer.feed(&make_schema_fd());
        reducer.feed(&append_flight_data(None, &make_batch_1row(), 10, 1000));

        // Row inserted with start_row_id=10
        let inserted_guard = inserted.lock().unwrap();
        assert_eq!(inserted_guard.len(), 1);
        assert_eq!(inserted_guard[0].0, 10);
        assert_eq!(reducer.watermark_ns(), 1000);
    }
}
