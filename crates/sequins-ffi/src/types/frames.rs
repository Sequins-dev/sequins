//! C-compatible frame types for SeQL streaming results

use sequins_query::error::QueryError;
use sequins_query::frame::{batch_to_ipc, QueryStats, SchemaFrame, WarningFrame};

// Local intermediary types for constructing CDeltaFrame — were previously in sequins_query::frame
pub struct DeltaFrame {
    pub watermark_ns: u64,
    pub ops: Vec<DeltaOp>,
}

pub enum DeltaOp {
    Append {
        start_row_id: u64,
        batch: arrow::record_batch::RecordBatch,
    },
    Update {
        row_id: u64,
        batch: arrow::record_batch::RecordBatch,
    },
    Expire {
        row_id: u64,
    },
    Replace {
        batch: arrow::record_batch::RecordBatch,
    },
}
use sequins_query::schema::ResponseShape;
use std::ffi::CString;
use std::os::raw::{c_char, c_uint};

// ── ResponseShape ─────────────────────────────────────────────────────────────

/// C-compatible response shape tag
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CResponseShape {
    /// Flat table
    Table = 0,
    /// Time-series lines
    TimeSeries = 1,
    /// 2-D heatmap
    Heatmap = 2,
    /// Hierarchical span tree
    TraceTree = 3,
    /// Waterfall trace timeline
    TraceTimeline = 4,
    /// Log pattern groups
    PatternGroups = 5,
    /// Single scalar value
    Scalar = 6,
}

impl From<&ResponseShape> for CResponseShape {
    fn from(s: &ResponseShape) -> Self {
        match s {
            ResponseShape::Table => CResponseShape::Table,
            ResponseShape::TimeSeries => CResponseShape::TimeSeries,
            ResponseShape::Heatmap => CResponseShape::Heatmap,
            ResponseShape::TraceTree => CResponseShape::TraceTree,
            ResponseShape::TraceTimeline => CResponseShape::TraceTimeline,
            ResponseShape::PatternGroups => CResponseShape::PatternGroups,
            ResponseShape::Scalar => CResponseShape::Scalar,
        }
    }
}

// ── CSchemaFrame ──────────────────────────────────────────────────────────────

/// C-compatible schema frame — describes the shape and columns
#[repr(C)]
pub struct CSchemaFrame {
    /// Result shape
    pub shape: CResponseShape,
    /// Number of columns
    pub column_count: c_uint,
    /// Column names (null-terminated strings)
    pub column_names: *mut *mut c_char,
    /// Watermark at query start (nanoseconds since epoch)
    pub initial_watermark_ns: u64,
    /// Table name for multi-table responses (null = primary table, non-null = auxiliary table alias)
    pub table: *mut c_char,
}

impl CSchemaFrame {
    /// Convert from a Rust `SchemaFrame`. Caller must free with `c_schema_frame_free`.
    pub fn from_schema(schema: &SchemaFrame, table: Option<&str>) -> Box<Self> {
        let column_names: Vec<*mut c_char> = schema
            .columns
            .iter()
            .map(|c| CString::new(c.name.as_str()).unwrap_or_default().into_raw())
            .collect();
        let count = column_names.len();
        let mut boxed_names = column_names.into_boxed_slice();
        let names_ptr = boxed_names.as_mut_ptr();
        std::mem::forget(boxed_names);

        let table_ptr = table
            .and_then(|t| CString::new(t).ok())
            .map(|s| s.into_raw())
            .unwrap_or(std::ptr::null_mut());

        Box::new(CSchemaFrame {
            shape: CResponseShape::from(&schema.shape),
            column_count: count as c_uint,
            column_names: names_ptr,
            initial_watermark_ns: schema.initial_watermark_ns,
            table: table_ptr,
        })
    }
}

// ── FFI normalization ──────────────────────────────────────────────────────────

// ── CDataFrame ────────────────────────────────────────────────────────────────

/// C-compatible data frame — a batch of Arrow IPC bytes
#[repr(C)]
pub struct CDataFrame {
    /// Number of rows
    pub row_count: c_uint,
    /// Arrow IPC streaming-format bytes
    pub ipc_data: *mut u8,
    /// Length of `ipc_data` in bytes
    pub ipc_len: usize,
    /// Table name for multi-table responses (null = primary table, non-null = auxiliary table alias)
    pub table: *mut c_char,
}

impl CDataFrame {
    /// Encode a `RecordBatch` as Arrow IPC. Caller must free with `c_data_frame_free`.
    pub fn from_batch(batch: &arrow::record_batch::RecordBatch, table: Option<&str>) -> Box<Self> {
        let row_count = batch.num_rows() as c_uint;
        let ipc = batch_to_ipc(batch);
        let len = ipc.len();
        let mut boxed = ipc.into_boxed_slice();
        let ptr = boxed.as_mut_ptr();
        std::mem::forget(boxed);

        let table_ptr = table
            .and_then(|t| CString::new(t).ok())
            .map(|s| s.into_raw())
            .unwrap_or(std::ptr::null_mut());

        Box::new(CDataFrame {
            row_count,
            ipc_data: ptr,
            ipc_len: len,
            table: table_ptr,
        })
    }
}

// ── CCompleteFrame ────────────────────────────────────────────────────────────

/// C-compatible complete frame — query finished
#[repr(C)]
pub struct CCompleteFrame {
    /// Wall-clock time in microseconds
    pub execution_time_us: u64,
    /// Rows scanned before filtering
    pub rows_scanned: u64,
    /// Bytes read from storage
    pub bytes_read: u64,
    /// Rows in the final result
    pub rows_returned: u64,
    /// Number of warnings emitted
    pub warning_count: c_uint,
}

impl From<&QueryStats> for CCompleteFrame {
    fn from(s: &QueryStats) -> Self {
        CCompleteFrame {
            execution_time_us: s.execution_time_us,
            rows_scanned: s.rows_scanned,
            bytes_read: s.bytes_read,
            rows_returned: s.rows_returned,
            warning_count: s.warning_count,
        }
    }
}

// ── CWarningFrame ─────────────────────────────────────────────────────────────

/// C-compatible warning frame
#[repr(C)]
pub struct CWarningFrame {
    /// Warning code
    pub code: c_uint,
    /// Human-readable message (null-terminated, caller must free)
    pub message: *mut c_char,
}

impl CWarningFrame {
    /// Convert from a Rust `WarningFrame`. Caller must free with `c_warning_frame_free`.
    pub fn from_warning(w: &WarningFrame) -> Box<Self> {
        use sequins_query::error::WarningCode;
        let code = match w.code {
            WarningCode::ResultTruncated => 0,
            WarningCode::SlowQuery => 1,
            WarningCode::ApproximateResult => 2,
            WarningCode::SchemaResolutionFallback => 3,
        };
        let msg = CString::new(w.message.as_str())
            .unwrap_or_default()
            .into_raw();
        Box::new(CWarningFrame { code, message: msg })
    }
}

// ── CDeltaFrame ───────────────────────────────────────────────────────────────

/// C-compatible delta operation type
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CDeltaOpType {
    /// New rows were added (data is Arrow IPC streaming-format bytes)
    Append = 0,
    /// An existing row was updated
    Update = 1,
    /// A row left the time window
    Expire = 2,
    /// All rows replaced with new result set
    Replace = 3,
}

/// C-compatible delta operation
#[repr(C)]
pub struct CDeltaOp {
    /// Operation type
    pub op_type: CDeltaOpType,
    /// Row ID: start_row_id for Append/Replace, row_id for Update/Expire
    pub row_id: u64,
    /// Payload bytes:
    ///   Append/Replace: Arrow IPC streaming-format bytes
    ///   Update: JSON bytes (UTF-8, array of `[col_idx, value]` pairs)
    ///   Expire: NULL
    pub data: *mut u8,
    /// Length of `data` in bytes (0 for Expire)
    pub data_len: usize,
}

/// C-compatible delta frame — incremental updates for live queries
#[repr(C)]
pub struct CDeltaFrame {
    /// New query watermark (nanoseconds since epoch)
    pub watermark_ns: u64,
    /// Number of operations
    pub ops_count: c_uint,
    /// Array of delta operations
    pub ops: *mut CDeltaOp,
}

impl CDeltaFrame {
    /// Convert from a Rust `DeltaFrame`. Caller must free with `c_delta_frame_free`.
    pub fn from_delta(delta: &DeltaFrame) -> Box<Self> {
        let ops: Vec<CDeltaOp> = delta
            .ops
            .iter()
            .map(|op| match op {
                DeltaOp::Append {
                    start_row_id,
                    batch,
                } => {
                    let ipc = batch_to_ipc(batch);
                    let len = ipc.len();
                    let mut boxed = ipc.into_boxed_slice();
                    let ptr = boxed.as_mut_ptr();
                    std::mem::forget(boxed);
                    CDeltaOp {
                        op_type: CDeltaOpType::Append,
                        row_id: *start_row_id,
                        data: ptr,
                        data_len: len,
                    }
                }
                DeltaOp::Update { row_id, batch } => {
                    let ipc = batch_to_ipc(batch);
                    let len = ipc.len();
                    let mut boxed = ipc.into_boxed_slice();
                    let ptr = boxed.as_mut_ptr();
                    std::mem::forget(boxed);
                    CDeltaOp {
                        op_type: CDeltaOpType::Update,
                        row_id: *row_id,
                        data: ptr,
                        data_len: len,
                    }
                }
                DeltaOp::Expire { row_id } => CDeltaOp {
                    op_type: CDeltaOpType::Expire,
                    row_id: *row_id,
                    data: std::ptr::null_mut(),
                    data_len: 0,
                },
                DeltaOp::Replace { batch } => {
                    let ipc = batch_to_ipc(batch);
                    let len = ipc.len();
                    let mut boxed = ipc.into_boxed_slice();
                    let ptr = boxed.as_mut_ptr();
                    std::mem::forget(boxed);
                    CDeltaOp {
                        op_type: CDeltaOpType::Replace,
                        row_id: 0,
                        data: ptr,
                        data_len: len,
                    }
                }
            })
            .collect();

        let ops_count = ops.len();
        let mut boxed_ops = ops.into_boxed_slice();
        let ops_ptr = boxed_ops.as_mut_ptr();
        std::mem::forget(boxed_ops);

        Box::new(CDeltaFrame {
            watermark_ns: delta.watermark_ns,
            ops_count: ops_count as c_uint,
            ops: ops_ptr,
        })
    }
}

// ── CHeartbeatFrame ───────────────────────────────────────────────────────────

/// C-compatible heartbeat frame — keepalive for live queries
#[repr(C)]
pub struct CHeartbeatFrame {
    /// Current watermark (nanoseconds since epoch)
    pub watermark_ns: u64,
}

impl From<&sequins_query::frame::HeartbeatFrame> for CHeartbeatFrame {
    fn from(h: &sequins_query::frame::HeartbeatFrame) -> Self {
        CHeartbeatFrame {
            watermark_ns: h.watermark_ns,
        }
    }
}

// ── CQueryError ───────────────────────────────────────────────────────────────

/// C-compatible query error
#[repr(C)]
pub struct CQueryError {
    /// Error code (0=UnknownField, 1=InvalidAst, 2=UnsupportedStage, 3=ResourceLimit, 4=Execution)
    pub code: c_uint,
    /// Human-readable message (null-terminated, caller must free)
    pub message: *mut c_char,
}

impl CQueryError {
    /// Convert from a Rust `QueryError`. Caller must free with `c_query_error_free`.
    pub fn from_error(e: &QueryError) -> Box<Self> {
        let (code, msg) = match e {
            QueryError::UnknownField { field } => (0, format!("unknown field: {field}")),
            QueryError::InvalidAst { message } => (1, message.clone()),
            QueryError::UnsupportedStage { stage } => (2, format!("unsupported stage: {stage}")),
            QueryError::ResourceLimit { limit } => (3, format!("resource limit: {limit}")),
            QueryError::Execution { message } => (4, message.clone()),
        };
        let c_msg = CString::new(msg).unwrap_or_default().into_raw();
        Box::new(CQueryError {
            code,
            message: c_msg,
        })
    }
}

// ── Free functions ────────────────────────────────────────────────────────────

/// Free a `CSchemaFrame` allocated by Rust
///
/// # Safety
/// Pointer must be a valid `CSchemaFrame` created by `sequins_seql_*` functions.
#[no_mangle]
pub unsafe extern "C" fn c_schema_frame_free(frame: *mut CSchemaFrame) {
    if frame.is_null() {
        return;
    }
    let frame = Box::from_raw(frame);
    for i in 0..frame.column_count as usize {
        let name_ptr = *frame.column_names.add(i);
        if !name_ptr.is_null() {
            drop(CString::from_raw(name_ptr));
        }
    }
    // Reconstruct the names slice so it's freed
    drop(Vec::from_raw_parts(
        frame.column_names,
        frame.column_count as usize,
        frame.column_count as usize,
    ));
    if !frame.table.is_null() {
        drop(CString::from_raw(frame.table));
    }
}

/// Free a `CDataFrame` allocated by Rust
///
/// # Safety
/// Pointer must be a valid `CDataFrame` created by `sequins_seql_*` functions.
#[no_mangle]
pub unsafe extern "C" fn c_data_frame_free(frame: *mut CDataFrame) {
    if frame.is_null() {
        return;
    }
    let frame = Box::from_raw(frame);
    if !frame.ipc_data.is_null() {
        drop(Vec::from_raw_parts(
            frame.ipc_data,
            frame.ipc_len,
            frame.ipc_len,
        ));
    }
    if !frame.table.is_null() {
        drop(CString::from_raw(frame.table));
    }
}

/// Free a `CWarningFrame` allocated by Rust
///
/// # Safety
/// Pointer must be a valid `CWarningFrame` created by `sequins_seql_*` functions.
#[no_mangle]
pub unsafe extern "C" fn c_warning_frame_free(frame: *mut CWarningFrame) {
    if frame.is_null() {
        return;
    }
    let frame = Box::from_raw(frame);
    if !frame.message.is_null() {
        drop(CString::from_raw(frame.message));
    }
}

/// Free a `CQueryError` allocated by Rust
///
/// # Safety
/// Pointer must be a valid `CQueryError` created by `sequins_seql_*` functions.
#[no_mangle]
pub unsafe extern "C" fn c_query_error_free(err: *mut CQueryError) {
    if err.is_null() {
        return;
    }
    let err = Box::from_raw(err);
    if !err.message.is_null() {
        drop(CString::from_raw(err.message));
    }
}

/// Free a `CDeltaFrame` allocated by Rust
///
/// # Safety
/// Pointer must be a valid `CDeltaFrame` created by `sequins_seql_*` functions.
#[no_mangle]
pub unsafe extern "C" fn c_delta_frame_free(frame: *mut CDeltaFrame) {
    if frame.is_null() {
        return;
    }
    let frame = Box::from_raw(frame);
    // Free each operation's data field (IPC bytes or JSON bytes)
    for i in 0..frame.ops_count as usize {
        let op = &*frame.ops.add(i);
        if !op.data.is_null() && op.data_len > 0 {
            drop(Vec::from_raw_parts(op.data, op.data_len, op.data_len));
        }
    }
    // Reconstruct the ops slice so it's freed
    drop(Vec::from_raw_parts(
        frame.ops,
        frame.ops_count as usize,
        frame.ops_count as usize,
    ));
}

/// Free a `CHeartbeatFrame` allocated by Rust
///
/// # Safety
/// Pointer must be a valid `CHeartbeatFrame` created by `sequins_seql_*` functions.
/// Note: Currently a no-op since CHeartbeatFrame contains no heap allocations,
/// but provided for API consistency.
#[no_mangle]
pub unsafe extern "C" fn c_heartbeat_frame_free(_frame: *mut CHeartbeatFrame) {
    // CHeartbeatFrame contains no heap allocations, but we accept the pointer
    // for API consistency and future-proofing
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{DeltaFrame, DeltaOp};
    use arrow::array::{Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sequins_query::frame::{ipc_to_batch, HeartbeatFrame};
    use std::sync::Arc;

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

    fn make_single_row_batch() -> RecordBatch {
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
    fn test_data_frame_ipc() {
        let batch = make_test_batch();
        let c = CDataFrame::from_batch(&batch, None);
        assert_eq!(c.row_count, 2);
        assert!(!c.ipc_data.is_null());
        assert!(c.ipc_len > 0);

        unsafe {
            let ipc_bytes = std::slice::from_raw_parts(c.ipc_data, c.ipc_len);
            let decoded = ipc_to_batch(ipc_bytes).unwrap();
            assert_eq!(decoded.num_rows(), 2);
            let name_col = decoded
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(name_col.value(0), "api");
            assert_eq!(name_col.value(1), "worker");
            let count_col = decoded
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(count_col.value(0), 10u64);
            assert_eq!(count_col.value(1), 5u64);

            c_data_frame_free(Box::into_raw(c));
        }
    }

    #[test]
    fn test_heartbeat_frame_conversion() {
        let heartbeat = HeartbeatFrame {
            watermark_ns: 1_700_000_000_000_000_000,
        };

        let c_heartbeat = CHeartbeatFrame::from(&heartbeat);
        assert_eq!(c_heartbeat.watermark_ns, 1_700_000_000_000_000_000);

        // Heartbeat frame has no heap allocations, but test the free function anyway
        unsafe {
            c_heartbeat_frame_free(&c_heartbeat as *const _ as *mut _);
        }
    }

    #[test]
    fn test_delta_frame_append_conversion() {
        let delta = DeltaFrame {
            watermark_ns: 1_700_001_000_000_000_000,
            ops: vec![DeltaOp::Append {
                start_row_id: 42,
                batch: make_single_row_batch(),
            }],
        };

        let c_delta = CDeltaFrame::from_delta(&delta);
        assert_eq!(c_delta.watermark_ns, 1_700_001_000_000_000_000);
        assert_eq!(c_delta.ops_count, 1);

        unsafe {
            let op = &*c_delta.ops;
            assert_eq!(op.op_type, CDeltaOpType::Append);
            assert_eq!(op.row_id, 42);
            assert!(!op.data.is_null());
            assert!(op.data_len > 0);

            let ipc_bytes = std::slice::from_raw_parts(op.data, op.data_len);
            let batch = ipc_to_batch(ipc_bytes).unwrap();
            assert_eq!(batch.num_rows(), 1);
            let name_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(name_col.value(0), "api");
            let count_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(count_col.value(0), 10u64);

            c_delta_frame_free(Box::into_raw(c_delta));
        }
    }

    #[test]
    fn test_delta_frame_update_conversion() {
        // Update now carries Arrow IPC bytes (single-row RecordBatch with changed columns)
        let update_batch = make_single_row_batch();
        let delta = DeltaFrame {
            watermark_ns: 1_700_002_000_000_000_000,
            ops: vec![DeltaOp::Update {
                row_id: 123,
                batch: update_batch,
            }],
        };

        let c_delta = CDeltaFrame::from_delta(&delta);
        assert_eq!(c_delta.watermark_ns, 1_700_002_000_000_000_000);
        assert_eq!(c_delta.ops_count, 1);

        unsafe {
            let op = &*c_delta.ops;
            assert_eq!(op.op_type, CDeltaOpType::Update);
            assert_eq!(op.row_id, 123);
            assert!(!op.data.is_null());
            assert!(op.data_len > 0);

            // Verify data is valid Arrow IPC bytes
            let bytes = std::slice::from_raw_parts(op.data, op.data_len);
            let decoded = ipc_to_batch(bytes).unwrap();
            assert_eq!(decoded.num_rows(), 1);
            assert_eq!(decoded.num_columns(), 2);

            c_delta_frame_free(Box::into_raw(c_delta));
        }
    }

    #[test]
    fn test_delta_frame_expire_conversion() {
        let delta = DeltaFrame {
            watermark_ns: 1_700_003_000_000_000_000,
            ops: vec![DeltaOp::Expire { row_id: 999 }],
        };

        let c_delta = CDeltaFrame::from_delta(&delta);
        assert_eq!(c_delta.watermark_ns, 1_700_003_000_000_000_000);
        assert_eq!(c_delta.ops_count, 1);

        unsafe {
            let op = &*c_delta.ops;
            assert_eq!(op.op_type, CDeltaOpType::Expire);
            assert_eq!(op.row_id, 999);
            assert!(op.data.is_null()); // Expire has no data

            c_delta_frame_free(Box::into_raw(c_delta));
        }
    }

    #[test]
    fn test_delta_frame_replace_conversion() {
        let delta = DeltaFrame {
            watermark_ns: 1_700_004_000_000_000_000,
            ops: vec![DeltaOp::Replace {
                batch: make_test_batch(),
            }],
        };

        let c_delta = CDeltaFrame::from_delta(&delta);
        assert_eq!(c_delta.watermark_ns, 1_700_004_000_000_000_000);
        assert_eq!(c_delta.ops_count, 1);

        unsafe {
            let op = &*c_delta.ops;
            assert_eq!(op.op_type, CDeltaOpType::Replace);
            assert_eq!(op.row_id, 0); // Not used for Replace
            assert!(!op.data.is_null());
            assert!(op.data_len > 0);

            let ipc_bytes = std::slice::from_raw_parts(op.data, op.data_len);
            let batch = ipc_to_batch(ipc_bytes).unwrap();
            assert_eq!(batch.num_rows(), 2);
            let name_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(name_col.value(0), "api");
            assert_eq!(name_col.value(1), "worker");
            let count_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(count_col.value(0), 10u64);
            assert_eq!(count_col.value(1), 5u64);

            c_delta_frame_free(Box::into_raw(c_delta));
        }
    }

    #[test]
    fn test_delta_frame_multiple_ops() {
        let update_batch = make_single_row_batch();
        let delta = DeltaFrame {
            watermark_ns: 1_700_005_000_000_000_000,
            ops: vec![
                DeltaOp::Append {
                    start_row_id: 1,
                    batch: make_single_row_batch(),
                },
                DeltaOp::Update {
                    row_id: 2,
                    batch: update_batch,
                },
                DeltaOp::Expire { row_id: 3 },
            ],
        };

        let c_delta = CDeltaFrame::from_delta(&delta);
        assert_eq!(c_delta.watermark_ns, 1_700_005_000_000_000_000);
        assert_eq!(c_delta.ops_count, 3);

        unsafe {
            // Check Append
            let op0 = &*c_delta.ops.add(0);
            assert_eq!(op0.op_type, CDeltaOpType::Append);
            assert_eq!(op0.row_id, 1);
            assert!(!op0.data.is_null());

            // Check Update
            let op1 = &*c_delta.ops.add(1);
            assert_eq!(op1.op_type, CDeltaOpType::Update);
            assert_eq!(op1.row_id, 2);
            assert!(!op1.data.is_null());

            // Check Expire
            let op2 = &*c_delta.ops.add(2);
            assert_eq!(op2.op_type, CDeltaOpType::Expire);
            assert_eq!(op2.row_id, 3);
            assert!(op2.data.is_null());

            c_delta_frame_free(Box::into_raw(c_delta));
        }
    }

    #[test]
    fn test_delta_frame_empty_ops() {
        let delta = DeltaFrame {
            watermark_ns: 1_700_006_000_000_000_000,
            ops: vec![],
        };

        let c_delta = CDeltaFrame::from_delta(&delta);
        assert_eq!(c_delta.watermark_ns, 1_700_006_000_000_000_000);
        assert_eq!(c_delta.ops_count, 0);

        unsafe {
            c_delta_frame_free(Box::into_raw(c_delta));
        }
    }

    #[test]
    fn test_null_delta_frame_free() {
        unsafe {
            c_delta_frame_free(std::ptr::null_mut());
        }
    }

    #[test]
    fn test_null_heartbeat_frame_free() {
        unsafe {
            c_heartbeat_frame_free(std::ptr::null_mut());
        }
    }
}
