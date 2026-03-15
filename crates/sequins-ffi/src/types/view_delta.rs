//! C-compatible types for the reactive view model API.
//!
//! `CViewDelta` is the unit of delivery across the FFI boundary. Batches of
//! deltas are delivered to the `on_deltas` callback registered via
//! `sequins_view_create` to minimise round-trips.

use sequins_view::ViewDelta;
use std::ffi::CString;
use std::os::raw::c_char;

// ── Delta type tag ─────────────────────────────────────────────────────────────

/// Discriminant for [`CViewDelta`].
///
/// Variants are prefixed with `ViewDelta` to avoid collisions with other C
/// enums in the same global namespace (e.g. `CSpanStatus::Error`).
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CViewDeltaType {
    /// New rows appended to a table (`data` carries Arrow IPC).
    ViewDeltaRowsAppended = 0,
    /// Rows expired from the live time window (`count` is the number expired).
    ViewDeltaRowsExpired = 1,
    /// Entire table replaced (`data` carries Arrow IPC).
    ViewDeltaTableReplaced = 2,
    /// New entity created (`descriptor` + `data` carry Arrow IPC).
    ViewDeltaEntityCreated = 3,
    /// Entity's mutable data changed (`data` carries Arrow IPC).
    ViewDeltaEntityDataReplaced = 4,
    /// Entity removed.
    ViewDeltaEntityRemoved = 5,
    /// Initial data load complete; live updates are now streaming.
    ViewDeltaReady = 6,
    /// Periodic keepalive (`watermark_ns` is set).
    ViewDeltaHeartbeat = 7,
    /// Non-fatal warning (`message` is set).
    ViewDeltaWarning = 8,
    /// Fatal error (`message` is set).
    ViewDeltaError = 9,
}

// ── CViewDelta ─────────────────────────────────────────────────────────────────

/// A single reactive update delivered to the frontend.
///
/// Fields that are not applicable to a given `delta_type` are null/zero.
///
/// ## Memory ownership
/// All heap-allocated fields (`table`, `key`, `data`, `descriptor`, `message`)
/// are owned by the `CViewDelta`. Free the entire batch with
/// `c_view_delta_batch_free` when done.
#[repr(C)]
pub struct CViewDelta {
    /// Delta variant tag.
    pub delta_type: CViewDeltaType,
    /// Table identifier. `NULL` = primary table; non-NULL = auxiliary alias.
    /// Applies to: RowsAppended, RowsExpired, TableReplaced.
    pub table: *mut c_char,
    /// Entity key (e.g. path_key for flamegraph, metric_id for metrics).
    /// Applies to: EntityCreated, EntityDataReplaced, EntityRemoved.
    pub key: *mut c_char,
    /// Arrow IPC bytes for the payload (nullable).
    /// - RowsAppended / TableReplaced: the row batch
    /// - EntityCreated: the mutable data batch
    /// - EntityDataReplaced: the updated mutable data batch
    pub data: *mut u8,
    /// Length of `data` in bytes.
    pub data_len: usize,
    /// Arrow IPC bytes for the immutable descriptor (EntityCreated only).
    pub descriptor: *mut u8,
    /// Length of `descriptor` in bytes.
    pub descriptor_len: usize,
    /// Number of rows expired (RowsExpired only).
    pub count: u64,
    /// Current watermark in nanoseconds since epoch (Heartbeat only).
    pub watermark_ns: u64,
    /// Warning/Error code (Warning only, 0 otherwise).
    pub code: u32,
    /// Human-readable message (Warning / Error only, NULL otherwise).
    pub message: *mut c_char,
}

// SAFETY: CViewDelta is an owned value delivered across the FFI boundary.
// The C caller takes ownership once the callback returns and frees with
// `c_view_delta_batch_free`. There is no shared mutable aliasing.
unsafe impl Send for CViewDelta {}

impl CViewDelta {
    /// Convert a [`ViewDelta`] to its C-compatible representation.
    pub fn from_view_delta(delta: ViewDelta) -> Box<Self> {
        match delta {
            ViewDelta::RowsAppended { table, ipc } => {
                let (data_ptr, data_len) = ipc_to_raw(ipc);
                Box::new(Self {
                    delta_type: CViewDeltaType::ViewDeltaRowsAppended,
                    table: opt_str_to_raw(table.as_deref()),
                    key: std::ptr::null_mut(),
                    data: data_ptr,
                    data_len,
                    descriptor: std::ptr::null_mut(),
                    descriptor_len: 0,
                    count: 0,
                    watermark_ns: 0,
                    code: 0,
                    message: std::ptr::null_mut(),
                })
            }
            ViewDelta::RowsExpired {
                table,
                expired_count,
            } => Box::new(Self {
                delta_type: CViewDeltaType::ViewDeltaRowsExpired,
                table: opt_str_to_raw(table.as_deref()),
                key: std::ptr::null_mut(),
                data: std::ptr::null_mut(),
                data_len: 0,
                descriptor: std::ptr::null_mut(),
                descriptor_len: 0,
                count: expired_count,
                watermark_ns: 0,
                code: 0,
                message: std::ptr::null_mut(),
            }),
            ViewDelta::TableReplaced { table, ipc } => {
                let (data_ptr, data_len) = ipc_to_raw(ipc);
                Box::new(Self {
                    delta_type: CViewDeltaType::ViewDeltaTableReplaced,
                    table: opt_str_to_raw(table.as_deref()),
                    key: std::ptr::null_mut(),
                    data: data_ptr,
                    data_len,
                    descriptor: std::ptr::null_mut(),
                    descriptor_len: 0,
                    count: 0,
                    watermark_ns: 0,
                    code: 0,
                    message: std::ptr::null_mut(),
                })
            }
            ViewDelta::EntityCreated {
                key,
                descriptor_ipc,
                data_ipc,
            } => {
                let (desc_ptr, desc_len) = ipc_to_raw(descriptor_ipc);
                let (data_ptr, data_len) = ipc_to_raw(data_ipc);
                Box::new(Self {
                    delta_type: CViewDeltaType::ViewDeltaEntityCreated,
                    table: std::ptr::null_mut(),
                    key: str_to_raw(&key),
                    data: data_ptr,
                    data_len,
                    descriptor: desc_ptr,
                    descriptor_len: desc_len,
                    count: 0,
                    watermark_ns: 0,
                    code: 0,
                    message: std::ptr::null_mut(),
                })
            }
            ViewDelta::EntityDataReplaced { key, data_ipc } => {
                let (data_ptr, data_len) = ipc_to_raw(data_ipc);
                Box::new(Self {
                    delta_type: CViewDeltaType::ViewDeltaEntityDataReplaced,
                    table: std::ptr::null_mut(),
                    key: str_to_raw(&key),
                    data: data_ptr,
                    data_len,
                    descriptor: std::ptr::null_mut(),
                    descriptor_len: 0,
                    count: 0,
                    watermark_ns: 0,
                    code: 0,
                    message: std::ptr::null_mut(),
                })
            }
            ViewDelta::EntityRemoved { key } => Box::new(Self {
                delta_type: CViewDeltaType::ViewDeltaEntityRemoved,
                table: std::ptr::null_mut(),
                key: str_to_raw(&key),
                data: std::ptr::null_mut(),
                data_len: 0,
                descriptor: std::ptr::null_mut(),
                descriptor_len: 0,
                count: 0,
                watermark_ns: 0,
                code: 0,
                message: std::ptr::null_mut(),
            }),
            ViewDelta::Ready => Box::new(Self {
                delta_type: CViewDeltaType::ViewDeltaReady,
                table: std::ptr::null_mut(),
                key: std::ptr::null_mut(),
                data: std::ptr::null_mut(),
                data_len: 0,
                descriptor: std::ptr::null_mut(),
                descriptor_len: 0,
                count: 0,
                watermark_ns: 0,
                code: 0,
                message: std::ptr::null_mut(),
            }),
            ViewDelta::Heartbeat { watermark_ns } => Box::new(Self {
                delta_type: CViewDeltaType::ViewDeltaHeartbeat,
                table: std::ptr::null_mut(),
                key: std::ptr::null_mut(),
                data: std::ptr::null_mut(),
                data_len: 0,
                descriptor: std::ptr::null_mut(),
                descriptor_len: 0,
                count: 0,
                watermark_ns,
                code: 0,
                message: std::ptr::null_mut(),
            }),
            ViewDelta::Warning { code, message } => Box::new(Self {
                delta_type: CViewDeltaType::ViewDeltaWarning,
                table: std::ptr::null_mut(),
                key: std::ptr::null_mut(),
                data: std::ptr::null_mut(),
                data_len: 0,
                descriptor: std::ptr::null_mut(),
                descriptor_len: 0,
                count: 0,
                watermark_ns: 0,
                code,
                message: str_to_raw(&message),
            }),
            ViewDelta::Error { message } => Box::new(Self {
                delta_type: CViewDeltaType::ViewDeltaError,
                table: std::ptr::null_mut(),
                key: std::ptr::null_mut(),
                data: std::ptr::null_mut(),
                data_len: 0,
                descriptor: std::ptr::null_mut(),
                descriptor_len: 0,
                count: 0,
                watermark_ns: 0,
                code: 0,
                message: str_to_raw(&message),
            }),
        }
    }

    /// Free all heap-allocated fields.
    ///
    /// # Safety
    /// Must only be called once per `CViewDelta`. After this call the struct
    /// is invalid and must not be accessed again.
    pub unsafe fn free_fields(&mut self) {
        free_c_str(self.table);
        self.table = std::ptr::null_mut();
        free_c_str(self.key);
        self.key = std::ptr::null_mut();
        free_ipc(self.data, self.data_len);
        self.data = std::ptr::null_mut();
        self.data_len = 0;
        free_ipc(self.descriptor, self.descriptor_len);
        self.descriptor = std::ptr::null_mut();
        self.descriptor_len = 0;
        free_c_str(self.message);
        self.message = std::ptr::null_mut();
    }
}

// ── Free functions ─────────────────────────────────────────────────────────────

/// Free a batch of [`CViewDelta`]s allocated by `sequins_view_create`.
///
/// # Safety
/// `deltas` must be a pointer returned by the `on_deltas` callback, and
/// `count` must match the value passed to that callback. Must only be called
/// once per batch.
#[no_mangle]
pub unsafe extern "C" fn c_view_delta_batch_free(deltas: *mut CViewDelta, count: u32) {
    if deltas.is_null() || count == 0 {
        return;
    }
    let slice = std::slice::from_raw_parts_mut(deltas, count as usize);
    for delta in slice.iter_mut() {
        delta.free_fields();
    }
    // Reconstruct the Vec to drop it
    drop(Vec::from_raw_parts(deltas, count as usize, count as usize));
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Convert a `Vec<u8>` to a heap-allocated raw pointer + length.
/// The caller must free it with `free_ipc`.
fn ipc_to_raw(ipc: Vec<u8>) -> (*mut u8, usize) {
    if ipc.is_empty() {
        return (std::ptr::null_mut(), 0);
    }
    let len = ipc.len();
    let mut boxed = ipc.into_boxed_slice();
    let ptr = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    (ptr, len)
}

unsafe fn free_ipc(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        drop(Vec::from_raw_parts(ptr, len, len));
    }
}

fn str_to_raw(s: &str) -> *mut c_char {
    CString::new(s).unwrap_or_default().into_raw()
}

fn opt_str_to_raw(s: Option<&str>) -> *mut c_char {
    match s {
        Some(s) => str_to_raw(s),
        None => std::ptr::null_mut(),
    }
}

unsafe fn free_c_str(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}
