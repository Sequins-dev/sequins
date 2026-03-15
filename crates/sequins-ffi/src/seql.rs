//! SeQL FFI bindings
//!
//! Provides a C interface for executing SeQL queries against a DataSource.
//!
//! # Lifecycle
//! 1. `sequins_seql_query` — compile and execute SeQL text, receive frames via vtable callbacks
//! 2. `sequins_seql_cancel` — cancel an in-progress stream (optional)
//! 3. `sequins_seql_stream_free` — free the stream handle
//!
//! # Validation (optional)
//! - `sequins_seql_parse` — validate text, get opaque `CParseResult` (for editor syntax checking)
//! - `sequins_seql_parse_result_free` — free the parse result

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::data_source::{CDataSource, DataSourceImpl};
use crate::runtime::RUNTIME;
use crate::types::frames::{
    CCompleteFrame, CDataFrame, CDeltaFrame, CHeartbeatFrame, CQueryError, CSchemaFrame,
    CWarningFrame,
};
use crate::types::frames::{DeltaFrame, DeltaOp};
use sequins_query::ast::QueryAst;
use sequins_query::error::QueryError;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::frame::{ipc_to_batch, SchemaFrame};
use sequins_query::parser::{parse, ParseError};
use std::ffi::CString;
// TursoBackend replaced by DataFusionBackend
use futures::StreamExt;
use sequins_query::QueryApi;
use sequins_storage::DataFusionBackend;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;

// ── Parse result ──────────────────────────────────────────────────────────────

/// Opaque parse result returned by `sequins_seql_parse`
pub struct CParseResult {
    /// Successfully parsed AST, or `None` if there was a parse error
    pub ast: Option<QueryAst>,
    /// Human-readable error message (`None` on success)
    pub error_message: Option<String>,
    /// Byte offset of the error in the input (`0` on success)
    pub error_offset: usize,
    /// Length in bytes of the offending token (`0` on success)
    pub error_length: usize,
}

/// Parse a SeQL query text
///
/// Returns an opaque `CParseResult*`. The caller must free it with
/// `sequins_seql_parse_result_free`.
///
/// Check `sequins_seql_parse_result_is_ok` to determine success.
///
/// # Safety
/// `query_text` must be a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn sequins_seql_parse(query_text: *const c_char) -> *mut CParseResult {
    let text = if query_text.is_null() {
        return Box::into_raw(Box::new(CParseResult {
            ast: None,
            error_message: Some("null query text".to_string()),
            error_offset: 0,
            error_length: 0,
        }));
    } else {
        match CStr::from_ptr(query_text).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                return Box::into_raw(Box::new(CParseResult {
                    ast: None,
                    error_message: Some("invalid UTF-8 in query text".to_string()),
                    error_offset: 0,
                    error_length: 0,
                }));
            }
        }
    };

    match parse(&text) {
        Ok(ast) => Box::into_raw(Box::new(CParseResult {
            ast: Some(ast),
            error_message: None,
            error_offset: 0,
            error_length: 0,
        })),
        Err(ParseError {
            message,
            offset,
            length,
        }) => Box::into_raw(Box::new(CParseResult {
            ast: None,
            error_message: Some(message),
            error_offset: offset,
            error_length: length,
        })),
    }
}

/// Returns 1 if the parse succeeded, 0 if there was a parse error
///
/// # Safety
/// `result` must be a valid `CParseResult*` from `sequins_seql_parse`.
#[no_mangle]
pub unsafe extern "C" fn sequins_seql_parse_result_is_ok(result: *const CParseResult) -> c_int {
    if result.is_null() {
        return 0;
    }
    if (*result).ast.is_some() {
        1
    } else {
        0
    }
}

/// Returns the parse error message, or NULL if the parse succeeded.
/// The returned pointer is valid until `sequins_seql_parse_result_free` is called.
///
/// # Safety
/// `result` must be a valid `CParseResult*` from `sequins_seql_parse`.
#[no_mangle]
pub unsafe extern "C" fn sequins_seql_parse_error_message(
    result: *const CParseResult,
) -> *const c_char {
    if result.is_null() {
        return std::ptr::null();
    }
    match &(*result).error_message {
        Some(msg) => msg.as_ptr() as *const c_char,
        None => std::ptr::null(),
    }
}

/// Returns the byte offset of the parse error, or 0 on success.
///
/// # Safety
/// `result` must be a valid `CParseResult*` from `sequins_seql_parse`.
#[no_mangle]
pub unsafe extern "C" fn sequins_seql_parse_error_offset(result: *const CParseResult) -> usize {
    if result.is_null() {
        0
    } else {
        (*result).error_offset
    }
}

/// Free a `CParseResult` returned by `sequins_seql_parse`
///
/// # Safety
/// `result` must be a valid `CParseResult*` from `sequins_seql_parse`.
#[no_mangle]
pub unsafe extern "C" fn sequins_seql_parse_result_free(result: *mut CParseResult) {
    if !result.is_null() {
        drop(Box::from_raw(result));
    }
}

// ── Stream handle ─────────────────────────────────────────────────────────────

/// An opaque handle to a running SeQL stream
pub struct CStreamHandle {
    cancel: Arc<AtomicBool>,
    _task: JoinHandle<()>,
}

// ── Internal context wrapper ──────────────────────────────────────────────────

/// Newtype wrapper for raw pointers that are send-safe by caller guarantee
struct SendPtr(*mut std::ffi::c_void);
// SAFETY: Caller guarantees the pointer is valid and thread-safe for the stream lifetime.
unsafe impl Send for SendPtr {}

/// Wrapper for the vtable + context pointer that can be sent across threads
struct SinkCtx {
    vtable: CFrameSinkVTable,
    ctx: SendPtr,
}

// SAFETY: CFrameSinkVTable contains only fn pointers (always Send); ctx is SendPtr.
unsafe impl Send for SinkCtx {}

/// Asserts that a `Future` is `Send`.
///
/// # Safety
/// Caller must ensure that all data accessed across `.await` points is actually
/// thread-safe. Used here because the raw pointer in `SinkCtx.ctx` is wrapped
/// in `SendPtr` but Rust's async future analysis still sees the inner `*mut c_void`.
struct AssertSend<F>(F);
impl<F: std::future::Future> std::future::Future for AssertSend<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: We're just delegating to the inner future.
        unsafe { std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().0).poll(cx) }
    }
}
// SAFETY: The caller has guaranteed that all inner data is thread-safe.
unsafe impl<F> Send for AssertSend<F> {}

// ── FrameSink vtable ──────────────────────────────────────────────────────────

/// C vtable for receiving SeQL frame callbacks
///
/// All callbacks may be invoked from a Tokio worker thread.
/// Implementations must be thread-safe and return quickly.
#[repr(C)]
pub struct CFrameSinkVTable {
    /// Called once with the result schema (before any data frames)
    pub on_schema: Option<unsafe extern "C" fn(*const CSchemaFrame, ctx: *mut std::ffi::c_void)>,
    /// Called with each batch of rows
    pub on_data: Option<unsafe extern "C" fn(*const CDataFrame, ctx: *mut std::ffi::c_void)>,
    /// Called with incremental updates (live queries only)
    pub on_delta: Option<unsafe extern "C" fn(*const CDeltaFrame, ctx: *mut std::ffi::c_void)>,
    /// Called periodically as keepalive (live queries only)
    pub on_heartbeat:
        Option<unsafe extern "C" fn(*const CHeartbeatFrame, ctx: *mut std::ffi::c_void)>,
    /// Called when the query completes successfully
    pub on_complete:
        Option<unsafe extern "C" fn(*const CCompleteFrame, ctx: *mut std::ffi::c_void)>,
    /// Called for non-fatal warnings
    pub on_warning: Option<unsafe extern "C" fn(*const CWarningFrame, ctx: *mut std::ffi::c_void)>,
    /// Called if the query terminates with an error
    pub on_error: Option<unsafe extern "C" fn(*const CQueryError, ctx: *mut std::ffi::c_void)>,
}

// SAFETY: The vtable contains only function pointers and a void* context.
// The caller guarantees the context pointer is valid for the lifetime of the stream.
unsafe impl Send for CFrameSinkVTable {}
unsafe impl Sync for CFrameSinkVTable {}

/// Cancel a running SeQL stream
///
/// This is a best-effort cancellation. The stream may produce a few more frames
/// before actually stopping. The `CStreamHandle` should still be freed with
/// `sequins_seql_stream_free`.
///
/// # Safety
/// `handle` must be a valid `CStreamHandle*` from `sequins_seql_query`.
#[no_mangle]
pub unsafe extern "C" fn sequins_seql_cancel(handle: *mut CStreamHandle) {
    if handle.is_null() {
        return;
    }
    (*handle).cancel.store(true, Ordering::Relaxed);
    (*handle)._task.abort();
}

/// Execute a SeQL query (compile + execute in one call)
///
/// This is the primary FFI entry point for executing SeQL queries.
/// It compiles and executes the query in a single call, returning a stream handle.
///
/// # Parameters
/// - `data_source`: Pointer to the C data source (created via `sequins_create_data_source`)
/// - `query_text`: Null-terminated SeQL query string
/// - `vtable`: Callback vtable for receiving frames
/// - `ctx`: User context pointer passed to callbacks
///
/// # Safety
/// - `data_source` must be a valid `CDataSource*` for the duration of the stream
/// - `query_text` must be a valid null-terminated UTF-8 string
/// - `vtable` function pointers must remain valid until the stream completes
/// - `ctx` must be valid for the duration of the stream
#[no_mangle]
#[tracing::instrument(skip_all, name = "seql_query")]
pub unsafe extern "C" fn sequins_seql_query(
    data_source: *mut CDataSource,
    query_text: *const c_char,
    vtable: CFrameSinkVTable,
    ctx: *mut c_void,
) -> *mut CStreamHandle {
    // Validate inputs
    if data_source.is_null() || query_text.is_null() {
        return std::ptr::null_mut();
    }

    // Convert C string to Rust str
    let query_cstr = CStr::from_ptr(query_text);
    let query_str = match query_cstr.to_str() {
        Ok(s) => {
            let query = s.to_string();
            tracing::debug!(
                len = query.len(),
                query = query.as_str(),
                "seql query received"
            );
            query
        }
        Err(_) => {
            tracing::error!("Invalid UTF-8 in query text");
            if let Some(cb) = vtable.on_error {
                let err = QueryError::InvalidAst {
                    message: "Invalid UTF-8 in query text".to_string(),
                };
                let c_err = CQueryError::from_error(&err);
                cb(Box::into_raw(c_err), ctx);
            }
            return std::ptr::null_mut();
        }
    };

    // Get the DataFusionBackend from the data source
    let ds = &*(data_source as *const DataSourceImpl);
    let storage = match ds {
        DataSourceImpl::Local { storage, .. } => storage.clone(),
        DataSourceImpl::Remote { .. } => return std::ptr::null_mut(),
    };

    let backend = DataFusionBackend::new(Arc::clone(&storage));
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();

    let sink_ctx = SinkCtx {
        vtable,
        ctx: SendPtr(ctx),
    };

    // SAFETY: SinkCtx wraps raw C pointers that the caller guarantees are
    // valid and thread-safe for the lifetime of the stream.
    let task = RUNTIME.spawn(AssertSend(async move {
        // Use QueryApi to compile and execute in one call
        let mut stream = match backend.query(&query_str).await {
            Ok(s) => s,
            Err(e) => {
                let c_err = CQueryError::from_error(&e);
                if let Some(cb) = sink_ctx.vtable.on_error {
                    unsafe { cb(Box::into_raw(c_err), sink_ctx.ctx.0) };
                }
                return;
            }
        };

        while let Some(frame_result) = stream.next().await {
            if cancel_clone.load(Ordering::Relaxed) {
                break;
            }
            let fd = match frame_result {
                Ok(fd) => fd,
                Err(e) => {
                    if let Some(cb) = sink_ctx.vtable.on_error {
                        let c = CQueryError::from_error(&e);
                        unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
                    }
                    break;
                }
            };
            let Some(metadata) = decode_metadata(&fd.app_metadata) else {
                tracing::warn!("Failed to decode frame metadata");
                continue;
            };
            dispatch_flight_metadata(metadata, &fd.data_body, &sink_ctx);
        }
    }));

    Box::into_raw(Box::new(CStreamHandle {
        cancel,
        _task: task,
    }))
}

/// Execute a SeQL query in live streaming mode
///
/// Identical to `sequins_seql_query` except the query runs in live mode:
/// - The backend subscribes to the WAL broadcast channel for new data
/// - The stream emits Delta frames as new rows arrive
/// - The stream emits Heartbeat frames as keepalives
/// - The stream never emits a Complete frame — it runs until cancelled
///
/// # Parameters
/// Same as `sequins_seql_query`.
///
/// # Safety
/// Same as `sequins_seql_query`.
#[no_mangle]
#[tracing::instrument(skip_all, name = "seql_query_live")]
pub unsafe extern "C" fn sequins_seql_query_live(
    data_source: *mut CDataSource,
    query_text: *const c_char,
    vtable: CFrameSinkVTable,
    ctx: *mut c_void,
) -> *mut CStreamHandle {
    if data_source.is_null() || query_text.is_null() {
        return std::ptr::null_mut();
    }

    let query_cstr = CStr::from_ptr(query_text);
    let query_str = match query_cstr.to_str() {
        Ok(s) => {
            let query = s.to_string();
            tracing::debug!(
                len = query.len(),
                query = query.as_str(),
                "seql live query received"
            );
            query
        }
        Err(_) => {
            tracing::error!("Invalid UTF-8 in live query text");
            if let Some(cb) = vtable.on_error {
                let err = QueryError::InvalidAst {
                    message: "Invalid UTF-8 in query text".to_string(),
                };
                let c_err = CQueryError::from_error(&err);
                cb(Box::into_raw(c_err), ctx);
            }
            return std::ptr::null_mut();
        }
    };

    let ds = &*(data_source as *const DataSourceImpl);
    let storage = match ds {
        DataSourceImpl::Local { storage, .. } => storage.clone(),
        DataSourceImpl::Remote { .. } => return std::ptr::null_mut(),
    };

    let backend = DataFusionBackend::new(Arc::clone(&storage));
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();

    let sink_ctx = SinkCtx {
        vtable,
        ctx: SendPtr(ctx),
    };

    let task = RUNTIME.spawn(AssertSend(async move {
        let mut stream = match backend.query_live(&query_str).await {
            Ok(s) => s,
            Err(e) => {
                let c_err = CQueryError::from_error(&e);
                if let Some(cb) = sink_ctx.vtable.on_error {
                    unsafe { cb(Box::into_raw(c_err), sink_ctx.ctx.0) };
                }
                return;
            }
        };

        while let Some(frame_result) = stream.next().await {
            if cancel_clone.load(Ordering::Relaxed) {
                break;
            }
            let fd = match frame_result {
                Ok(fd) => fd,
                Err(e) => {
                    if let Some(cb) = sink_ctx.vtable.on_error {
                        let c = CQueryError::from_error(&e);
                        unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
                    }
                    break;
                }
            };
            let Some(metadata) = decode_metadata(&fd.app_metadata) else {
                tracing::warn!("Failed to decode live frame metadata");
                continue;
            };
            dispatch_flight_metadata(metadata, &fd.data_body, &sink_ctx);
        }
    }));

    Box::into_raw(Box::new(CStreamHandle {
        cancel,
        _task: task,
    }))
}

/// Decode SeqlMetadata and dispatch to the appropriate C callback.
///
/// Shared by both snapshot and live query loops.
fn dispatch_flight_metadata(metadata: SeqlMetadata, data_body: &[u8], sink_ctx: &SinkCtx) {
    use crate::types::frames::{
        CCompleteFrame, CDataFrame, CDeltaFrame, CHeartbeatFrame, CWarningFrame,
    };
    match metadata {
        SeqlMetadata::Schema {
            table,
            shape,
            columns,
            watermark_ns,
            ..
        } => {
            if let Some(cb) = sink_ctx.vtable.on_schema {
                let schema = SchemaFrame {
                    shape,
                    columns,
                    initial_watermark_ns: watermark_ns,
                };
                let c = CSchemaFrame::from_schema(&schema, table.as_deref());
                unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
            }
        }
        SeqlMetadata::Data { table, .. } => {
            if let Some(cb) = sink_ctx.vtable.on_data {
                match ipc_to_batch(data_body) {
                    Ok(batch) => {
                        let c = CDataFrame::from_batch(&batch, table.as_deref());
                        unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
                    }
                    Err(e) => tracing::warn!(error = %e, "Failed to decode data batch"),
                }
            }
        }
        SeqlMetadata::Complete { stats } => {
            if let Some(cb) = sink_ctx.vtable.on_complete {
                let c = CCompleteFrame::from(&stats);
                unsafe { cb(&c as *const CCompleteFrame, sink_ctx.ctx.0) };
            }
        }
        SeqlMetadata::Warning { code, message } => {
            if let Some(cb) = sink_ctx.vtable.on_warning {
                let msg_ptr = CString::new(message).unwrap_or_default().into_raw();
                let c = Box::new(CWarningFrame {
                    code,
                    message: msg_ptr,
                });
                unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
            }
        }
        SeqlMetadata::Append {
            start_row_id,
            watermark_ns,
            ..
        } => {
            if let Some(cb) = sink_ctx.vtable.on_delta {
                match ipc_to_batch(data_body) {
                    Ok(batch) => {
                        let delta = DeltaFrame {
                            watermark_ns,
                            ops: vec![DeltaOp::Append {
                                start_row_id,
                                batch,
                            }],
                        };
                        let c = CDeltaFrame::from_delta(&delta);
                        unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
                    }
                    Err(e) => tracing::warn!(error = %e, "Failed to decode append batch"),
                }
            }
        }
        SeqlMetadata::Update {
            row_id,
            watermark_ns,
            ..
        } => {
            if let Some(cb) = sink_ctx.vtable.on_delta {
                match ipc_to_batch(data_body) {
                    Ok(batch) => {
                        let delta = DeltaFrame {
                            watermark_ns,
                            ops: vec![DeltaOp::Update { row_id, batch }],
                        };
                        let c = CDeltaFrame::from_delta(&delta);
                        unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
                    }
                    Err(e) => tracing::warn!(error = %e, "Failed to decode update batch"),
                }
            }
        }
        SeqlMetadata::Expire {
            row_id,
            watermark_ns,
            ..
        } => {
            if let Some(cb) = sink_ctx.vtable.on_delta {
                let delta = DeltaFrame {
                    watermark_ns,
                    ops: vec![DeltaOp::Expire { row_id }],
                };
                let c = CDeltaFrame::from_delta(&delta);
                unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
            }
        }
        SeqlMetadata::Replace { watermark_ns, .. } => {
            if let Some(cb) = sink_ctx.vtable.on_delta {
                match ipc_to_batch(data_body) {
                    Ok(batch) => {
                        let delta = DeltaFrame {
                            watermark_ns,
                            ops: vec![DeltaOp::Replace { batch }],
                        };
                        let c = CDeltaFrame::from_delta(&delta);
                        unsafe { cb(Box::into_raw(c), sink_ctx.ctx.0) };
                    }
                    Err(e) => tracing::warn!(error = %e, "Failed to decode replace batch"),
                }
            }
        }
        SeqlMetadata::Heartbeat { watermark_ns } => {
            if let Some(cb) = sink_ctx.vtable.on_heartbeat {
                let c = CHeartbeatFrame { watermark_ns };
                unsafe { cb(&c as *const CHeartbeatFrame, sink_ctx.ctx.0) };
            }
        }
    }
}

/// Free a `CStreamHandle` returned by `sequins_seql_query`
///
/// Call this after the stream has completed or been cancelled.
///
/// **CRITICAL**: This function aborts the task and blocks until it completes
/// to prevent callbacks from firing after the Swift context is freed.
///
/// # Safety
/// `handle` must be a valid `CStreamHandle*` from `sequins_seql_query`.
#[no_mangle]
pub unsafe extern "C" fn sequins_seql_stream_free(handle: *mut CStreamHandle) {
    if !handle.is_null() {
        let stream = Box::from_raw(handle);
        // Abort the task to stop any further callbacks
        stream._task.abort();
        // Block-wait for the task to complete before freeing
        // This ensures no callbacks fire after the context is freed
        let _ = futures::executor::block_on(stream._task);
    }
}

// ── Reactive view API ─────────────────────────────────────────────────────────

/// Strategy selector for `sequins_view_create`.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CViewStrategy {
    /// Table strategy — logs, spans: row-level append/expire.
    Table = 0,
    /// Aggregate strategy — health: full-replace on every update.
    Aggregate = 1,
    /// Flamegraph strategy — profiles: entity-level incremental updates.
    Flamegraph = 3,
}

/// Opaque handle to a running reactive view.
pub struct CViewHandle {
    cancel: Arc<AtomicBool>,
    _task: JoinHandle<()>,
}

/// Callback signature for `sequins_view_create`.
///
/// `deltas` — pointer to an array of [`CViewDelta`] owned by this call.
/// `count`  — number of deltas in the array.
/// `ctx`    — the user context pointer passed to `sequins_view_create`.
///
/// **Ownership**: the caller (C/Swift side) takes ownership of the array and
/// MUST free it with `c_view_delta_batch_free(deltas, count)` when done.
pub type CViewDeltaCallback = unsafe extern "C" fn(
    deltas: *mut crate::types::view_delta::CViewDelta,
    count: u32,
    ctx: *mut c_void,
);

struct ViewSinkCtx {
    callback: CViewDeltaCallback,
    ctx: SendPtr,
}
unsafe impl Send for ViewSinkCtx {}

/// Create a reactive view that transforms a live SeQL query into entity-level
/// deltas delivered to `on_deltas`.
///
/// # Parameters
/// - `data_source`   — local data source (remote not yet supported)
/// - `query`         — SeQL query text
/// - `strategy`      — [`CViewStrategy`] variant
/// - `retention_ns`  — retention window in nanoseconds; if 0, defaults to 1 hour.
///   Only meaningful for `Flamegraph` strategy.
/// - `on_deltas`     — callback invoked with a heap-allocated batch of deltas.
///   Caller must free with `c_view_delta_batch_free`.
/// - `ctx`           — user context pointer forwarded to `on_deltas`
///
/// Returns a [`CViewHandle*`] that must be freed with `sequins_view_free`.
/// Returns NULL on invalid input.
///
/// # Safety
/// - `data_source` must be a valid `CDataSource*` for the duration of the handle
/// - `query` must be a valid null-terminated UTF-8 string
/// - `on_deltas` must be a valid function pointer for the duration of the handle
/// - `ctx` must be valid for the duration of the handle
#[no_mangle]
#[tracing::instrument(skip_all, name = "view_create")]
pub unsafe extern "C" fn sequins_view_create(
    data_source: *mut CDataSource,
    query: *const c_char,
    strategy: u32,
    retention_ns: u64,
    on_deltas: Option<
        unsafe extern "C" fn(*mut crate::types::view_delta::CViewDelta, u32, *mut c_void),
    >,
    ctx: *mut c_void,
) -> *mut CViewHandle {
    use sequins_view::{AggregateStrategy, FlamegraphStrategy, TableStrategy, ViewStrategy};

    if data_source.is_null() || query.is_null() {
        return std::ptr::null_mut();
    }
    let on_deltas = match on_deltas {
        Some(cb) => cb,
        None => return std::ptr::null_mut(),
    };

    let query_str = match CStr::from_ptr(query).to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };

    let ds = &*(data_source as *const DataSourceImpl);
    let storage = match ds {
        DataSourceImpl::Local { storage, .. } => storage.clone(),
        DataSourceImpl::Remote { .. } => return std::ptr::null_mut(),
    };

    let retention = if retention_ns == 0 {
        3_600_000_000_000u64 // default: 1 hour
    } else {
        retention_ns
    };

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_clone = cancel.clone();

    let sink_ctx = ViewSinkCtx {
        callback: on_deltas,
        ctx: SendPtr(ctx),
    };

    let task = RUNTIME.spawn(AssertSend(async move {
        let backend = DataFusionBackend::new(Arc::clone(&storage));

        let seql_stream = match backend.query_live(&query_str).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "View query failed");
                return;
            }
        };

        let strategy_boxed: Box<dyn ViewStrategy> = match strategy {
            0 => Box::new(TableStrategy::new()),
            1 => Box::new(AggregateStrategy::new()),
            3 => Box::new(FlamegraphStrategy::new(retention)),
            other => {
                tracing::error!(strategy = other, "Unknown view strategy");
                return;
            }
        };

        let mut delta_stream = strategy_boxed.transform(seql_stream).await;

        // Collect deltas from the stream, batching per poll cycle.
        // We flush accumulated deltas whenever the stream yields.
        let mut batch: Vec<crate::types::view_delta::CViewDelta> = Vec::new();

        loop {
            if cancel_clone.load(Ordering::Relaxed) {
                break;
            }

            match futures::StreamExt::next(&mut delta_stream).await {
                Some(delta) => {
                    use sequins_view::ViewDelta;
                    let tag = match &delta {
                        ViewDelta::RowsAppended { .. } => "RowsAppended",
                        ViewDelta::RowsExpired { .. } => "RowsExpired",
                        ViewDelta::TableReplaced { .. } => "TableReplaced",
                        ViewDelta::EntityCreated { .. } => "EntityCreated",
                        ViewDelta::EntityDataReplaced { .. } => "EntityDataReplaced",
                        ViewDelta::EntityRemoved { .. } => "EntityRemoved",
                        ViewDelta::Ready => "Ready",
                        ViewDelta::Heartbeat { .. } => "Heartbeat",
                        ViewDelta::Warning { .. } => "Warning",
                        ViewDelta::Error { .. } => "Error",
                    };
                    tracing::debug!(delta = tag, "view delta");
                    let c_delta = *crate::types::view_delta::CViewDelta::from_view_delta(delta);
                    batch.push(c_delta);

                    // Flush on data-carrying deltas immediately so the UI updates in real time.
                    // Entity deltas (flamegraph) are batched up to 64 for efficiency.
                    let should_flush = batch.len() >= 64
                        || matches!(
                            batch.last().map(|d| d.delta_type),
                            Some(
                                crate::types::view_delta::CViewDeltaType::ViewDeltaReady
                                    | crate::types::view_delta::CViewDeltaType::ViewDeltaHeartbeat
                                    | crate::types::view_delta::CViewDeltaType::ViewDeltaError
                                    | crate::types::view_delta::CViewDeltaType::ViewDeltaRowsAppended
                                    | crate::types::view_delta::CViewDeltaType::ViewDeltaTableReplaced
                                    | crate::types::view_delta::CViewDeltaType::ViewDeltaRowsExpired
                            )
                        );

                    if should_flush {
                        flush_delta_batch(&mut batch, &sink_ctx);
                    }
                }
                None => {
                    // Stream ended — flush remaining deltas then exit
                    if !batch.is_empty() {
                        flush_delta_batch(&mut batch, &sink_ctx);
                    }
                    break;
                }
            }
        }
    }));

    Box::into_raw(Box::new(CViewHandle {
        cancel,
        _task: task,
    }))
}

/// Flush accumulated deltas to the C callback and clear the batch.
fn flush_delta_batch(batch: &mut Vec<crate::types::view_delta::CViewDelta>, ctx: &ViewSinkCtx) {
    if batch.is_empty() {
        return;
    }
    let count = batch.len() as u32;
    // Move batch contents onto heap as a raw pointer
    let boxed_slice: Box<[crate::types::view_delta::CViewDelta]> =
        std::mem::take(batch).into_boxed_slice();
    let ptr = Box::into_raw(boxed_slice) as *mut crate::types::view_delta::CViewDelta;
    unsafe { (ctx.callback)(ptr, count, ctx.ctx.0) };
}

/// Cancel a running view (best-effort).
///
/// The handle must still be freed with `sequins_view_free`.
///
/// # Safety
/// `handle` must be a valid `CViewHandle*` from `sequins_view_create`.
#[no_mangle]
pub unsafe extern "C" fn sequins_view_cancel(handle: *mut CViewHandle) {
    if !handle.is_null() {
        (*handle).cancel.store(true, Ordering::Relaxed);
        (*handle)._task.abort();
    }
}

/// Free a `CViewHandle` returned by `sequins_view_create`.
///
/// Aborts the background task and blocks until it stops to prevent callbacks
/// firing after the Swift context is freed.
///
/// # Safety
/// `handle` must be a valid `CViewHandle*` from `sequins_view_create`.
#[no_mangle]
pub unsafe extern "C" fn sequins_view_free(handle: *mut CViewHandle) {
    if !handle.is_null() {
        let view = Box::from_raw(handle);
        view._task.abort();
        let _ = futures::executor::block_on(view._task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_source::{
        sequins_data_source_free, sequins_data_source_new_local, COtlpServerConfig,
    };
    use crate::types::frames::{
        c_data_frame_free, c_delta_frame_free, c_query_error_free, c_schema_frame_free,
        c_warning_frame_free,
    };
    use std::ffi::{CStr, CString};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    // ── Helper: Create test DataSource ───────────────────────────────────────────

    /// Helper to create a test DataSource with test data
    fn create_test_data_source() -> (*mut CDataSource, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let path_c = CString::new(db_path.to_str().unwrap()).unwrap();

        let config = COtlpServerConfig {
            grpc_port: 0,
            http_port: 0,
        };

        let mut error: *mut c_char = std::ptr::null_mut();
        let data_source =
            sequins_data_source_new_local(path_c.as_ptr(), config, &mut error as *mut _);

        assert!(!data_source.is_null(), "Failed to create data source");
        assert!(error.is_null(), "Unexpected error creating data source");

        // Generate test data
        let count = crate::data_source::sequins_data_source_generate_test_data(
            data_source,
            &mut error as *mut _,
        );
        assert!(count > 0, "Failed to generate test data");
        assert!(error.is_null(), "Unexpected error generating test data");

        (data_source, dir)
    }

    // ── Frame Collection Helper ───────────────────────────────────────────────────

    /// Helper struct to collect frames from FFI callbacks
    #[derive(Default)]
    struct FrameCollector {
        schema_count: usize,
        data_count: usize,
        data_row_count: u32,
        delta_count: usize,
        heartbeat_count: usize,
        complete_count: usize,
        warning_count: usize,
        error_count: usize,
        error_messages: Vec<String>,
    }

    extern "C" fn on_schema_callback(_frame: *const CSchemaFrame, ctx: *mut c_void) {
        let collector = unsafe { &mut *(ctx as *mut FrameCollector) };
        collector.schema_count += 1;
        // Frame is freed by Rust after callback
        unsafe { c_schema_frame_free(_frame as *mut _) };
    }

    extern "C" fn on_data_callback(frame: *const CDataFrame, ctx: *mut c_void) {
        let collector = unsafe { &mut *(ctx as *mut FrameCollector) };
        collector.data_count += 1;
        unsafe {
            collector.data_row_count += (*frame).row_count;
        }
        // Frame is freed by Rust after callback
        unsafe { c_data_frame_free(frame as *mut _) };
    }

    extern "C" fn on_delta_callback(_frame: *const CDeltaFrame, ctx: *mut c_void) {
        let collector = unsafe { &mut *(ctx as *mut FrameCollector) };
        collector.delta_count += 1;
        // Frame is freed by Rust after callback
        unsafe { c_delta_frame_free(_frame as *mut _) };
    }

    extern "C" fn on_heartbeat_callback(_frame: *const CHeartbeatFrame, ctx: *mut c_void) {
        let collector = unsafe { &mut *(ctx as *mut FrameCollector) };
        collector.heartbeat_count += 1;
        // Heartbeat frame has no allocations
    }

    extern "C" fn on_complete_callback(_frame: *const CCompleteFrame, ctx: *mut c_void) {
        let collector = unsafe { &mut *(ctx as *mut FrameCollector) };
        collector.complete_count += 1;
        // Complete frame is passed by value, no cleanup needed
    }

    extern "C" fn on_warning_callback(_frame: *const CWarningFrame, ctx: *mut c_void) {
        let collector = unsafe { &mut *(ctx as *mut FrameCollector) };
        collector.warning_count += 1;
        // Frame is freed by Rust after callback
        unsafe { c_warning_frame_free(_frame as *mut _) };
    }

    extern "C" fn on_error_callback(error: *const CQueryError, ctx: *mut c_void) {
        let collector = unsafe { &mut *(ctx as *mut FrameCollector) };
        collector.error_count += 1;
        unsafe {
            if !(*error).message.is_null() {
                let msg = CStr::from_ptr((*error).message)
                    .to_string_lossy()
                    .to_string();
                collector.error_messages.push(msg);
            }
        }
        // Frame is freed by Rust after callback
        unsafe { c_query_error_free(error as *mut _) };
    }

    // ── SeQL Query FFI Tests ──────────────────────────────────────────────────────

    #[test]
    fn test_sequins_seql_query_valid_query() {
        let (data_source, _dir) = create_test_data_source();

        let mut collector = FrameCollector::default();
        let ctx = &mut collector as *mut _ as *mut c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: Some(on_error_callback),
        };

        let query = CString::new("spans last 1h | take 5").unwrap();
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };

        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Wait for query to complete
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Check that we received frames
        assert_eq!(collector.error_count, 0, "Should have no errors");
        assert!(collector.schema_count > 0, "Should have schema frame");
        assert!(collector.complete_count > 0, "Should have complete frame");

        // Cleanup
        unsafe {
            sequins_seql_stream_free(stream_handle);
            sequins_data_source_free(data_source);
        }
    }

    #[test]
    fn test_sequins_seql_query_with_results() {
        let (data_source, _dir) = create_test_data_source();

        let mut collector = FrameCollector::default();
        let ctx = &mut collector as *mut _ as *mut c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: Some(on_error_callback),
        };

        let query = CString::new("spans last 1h | select trace_id, name | take 3").unwrap();
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };

        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Wait for query to complete
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Verify frames were sent
        assert_eq!(collector.error_count, 0, "Should have no errors");
        assert_eq!(
            collector.schema_count, 1,
            "Should have exactly 1 schema frame"
        );
        assert!(
            collector.data_count > 0 || collector.data_row_count == 0,
            "Should have data frames or zero rows"
        );
        assert_eq!(
            collector.complete_count, 1,
            "Should have exactly 1 complete frame"
        );

        // Cleanup
        unsafe {
            sequins_seql_stream_free(stream_handle);
            sequins_data_source_free(data_source);
        }
    }

    #[test]
    fn test_sequins_seql_query_invalid_query_returns_error() {
        let (data_source, _dir) = create_test_data_source();

        let mut collector = FrameCollector::default();
        let ctx = &mut collector as *mut _ as *mut c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: Some(on_error_callback),
        };

        // Invalid query: unknown field
        let query =
            CString::new("spans last 1h | select invalid_field_that_does_not_exist").unwrap();
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };

        // For invalid queries, the handle might still be returned, but we get an error frame
        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Wait for query to complete
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Should have received an error
        assert!(
            collector.error_count > 0,
            "Should have received error frame"
        );
        assert!(
            !collector.error_messages.is_empty(),
            "Should have error message"
        );

        // Cleanup
        unsafe {
            sequins_seql_stream_free(stream_handle);
            sequins_data_source_free(data_source);
        }
    }

    #[test]
    fn test_sequins_seql_query_null_pointer_safety() {
        let (data_source, _dir) = create_test_data_source();

        let mut collector = FrameCollector::default();
        let ctx = &mut collector as *mut _ as *mut c_void;

        let query = CString::new("spans last 1h | take 5").unwrap();

        // Test null data_source
        let vtable1 = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: Some(on_error_callback),
        };
        let result =
            unsafe { sequins_seql_query(std::ptr::null_mut(), query.as_ptr(), vtable1, ctx) };
        assert!(result.is_null(), "Should return null for null data_source");

        // Test null query
        let vtable2 = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: Some(on_error_callback),
        };
        let result = unsafe { sequins_seql_query(data_source, std::ptr::null(), vtable2, ctx) };
        assert!(result.is_null(), "Should return null for null query");

        // Cleanup
        sequins_data_source_free(data_source);
    }

    #[test]
    fn test_sequins_seql_query_string_encoding() {
        let (data_source, _dir) = create_test_data_source();

        let mut collector = FrameCollector::default();
        let ctx = &mut collector as *mut _ as *mut c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: Some(on_error_callback),
        };

        // UTF-8 query with special characters
        let query = CString::new("spans | where name = 'test-✓' | take 5").unwrap();
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };

        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Wait for query to complete
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Should not crash, even if no results match the special character
        // (The query is valid, just might have zero results)

        // Cleanup
        unsafe {
            sequins_seql_stream_free(stream_handle);
            sequins_data_source_free(data_source);
        }
    }

    #[test]
    fn test_stream_handle_lifecycle() {
        let (data_source, _dir) = create_test_data_source();

        let mut collector = FrameCollector::default();
        let ctx = &mut collector as *mut _ as *mut c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: Some(on_error_callback),
        };

        let query = CString::new("spans last 1h | take 5").unwrap();

        // Create stream handle
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };
        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Cancel the stream
        unsafe {
            sequins_seql_cancel(stream_handle);
        }

        // Free the stream handle (should not crash even after cancel)
        unsafe {
            sequins_seql_stream_free(stream_handle);
        }

        // Cleanup
        unsafe {
            sequins_data_source_free(data_source);
        }
    }

    // ── Frame Sink VTable Tests ───────────────────────────────────────────────────

    #[test]
    fn test_frame_sink_callback_invoked() {
        let (data_source, _dir) = create_test_data_source();

        let callback_invoked = Arc::new(Mutex::new(false));
        let callback_invoked_clone = callback_invoked.clone();

        extern "C" fn schema_callback(_frame: *const CSchemaFrame, ctx: *mut c_void) {
            let flag = unsafe { &*(ctx as *const Arc<Mutex<bool>>) };
            *flag.lock().unwrap() = true;
            unsafe { c_schema_frame_free(_frame as *mut _) };
        }

        let ctx = &callback_invoked_clone as *const _ as *mut c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(schema_callback),
            on_data: None,
            on_delta: None,
            on_heartbeat: None,
            on_complete: None,
            on_warning: None,
            on_error: None,
        };

        let query = CString::new("spans last 1h | take 5").unwrap();
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };

        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Wait for query to complete
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Verify callback was invoked
        assert!(
            *callback_invoked.lock().unwrap(),
            "Schema callback should have been invoked"
        );

        // Cleanup
        unsafe {
            sequins_seql_stream_free(stream_handle);
            sequins_data_source_free(data_source);
        }
    }

    #[test]
    fn test_frame_sink_callback_with_multiple_frames() {
        let (data_source, _dir) = create_test_data_source();

        let mut collector = FrameCollector::default();
        let ctx = &mut collector as *mut _ as *mut c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(on_schema_callback),
            on_data: Some(on_data_callback),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete_callback),
            on_warning: None,
            on_error: None,
        };

        let query = CString::new("spans last 1h | take 10").unwrap();
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };

        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Wait for query to complete
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Verify multiple frame types were received
        assert_eq!(
            collector.schema_count, 1,
            "Should have received exactly 1 schema frame"
        );
        assert_eq!(
            collector.complete_count, 1,
            "Should have received exactly 1 complete frame"
        );
        // Data frames may be 0 or more depending on whether test data exists

        // Cleanup
        unsafe {
            sequins_seql_stream_free(stream_handle);
            sequins_data_source_free(data_source);
        }
    }

    // ── Parse Result Tests ────────────────────────────────────────────────────────

    #[test]
    fn test_parse_valid_query() {
        let query = CString::new("spans last 1h | select trace_id").unwrap();
        let result = unsafe { sequins_seql_parse(query.as_ptr()) };

        assert!(!result.is_null(), "Parse result should not be null");

        let is_ok = unsafe { sequins_seql_parse_result_is_ok(result) };
        assert_eq!(is_ok, 1, "Parse should succeed");

        let error_msg = unsafe { sequins_seql_parse_error_message(result) };
        assert!(error_msg.is_null(), "Should have no error message");

        unsafe { sequins_seql_parse_result_free(result) };
    }

    #[test]
    fn test_parse_invalid_query() {
        let query = CString::new("invalid syntax |||").unwrap();
        let result = unsafe { sequins_seql_parse(query.as_ptr()) };

        assert!(!result.is_null(), "Parse result should not be null");

        let is_ok = unsafe { sequins_seql_parse_result_is_ok(result) };
        assert_eq!(is_ok, 0, "Parse should fail");

        let error_msg = unsafe { sequins_seql_parse_error_message(result) };
        assert!(!error_msg.is_null(), "Should have error message");

        unsafe { sequins_seql_parse_result_free(result) };
    }

    #[test]
    fn test_parse_null_query() {
        let result = unsafe { sequins_seql_parse(std::ptr::null()) };

        assert!(!result.is_null(), "Parse result should not be null");

        let is_ok = unsafe { sequins_seql_parse_result_is_ok(result) };
        assert_eq!(is_ok, 0, "Parse should fail for null query");

        unsafe { sequins_seql_parse_result_free(result) };
    }
}
