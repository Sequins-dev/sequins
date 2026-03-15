//! Span FFI types
//!
//! C-compatible types for OpenTelemetry spans and related structures.

use super::common::{CKeyValueArray, CTimestamp};
use sequins_types::models::{Span, SpanEvent, SpanKind, SpanStatus};
use std::ffi::CString;
use std::os::raw::c_char;

#[cfg(test)]
use sequins_types::models::{SpanId, TraceId};

/// C-compatible span status
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CSpanStatus {
    Unset = 0,
    Ok = 1,
    Error = 2,
}

impl From<SpanStatus> for CSpanStatus {
    fn from(status: SpanStatus) -> Self {
        match status {
            SpanStatus::Unset => CSpanStatus::Unset,
            SpanStatus::Ok => CSpanStatus::Ok,
            SpanStatus::Error => CSpanStatus::Error,
        }
    }
}

/// C-compatible span kind
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CSpanKind {
    Unspecified = 0,
    Internal = 1,
    Server = 2,
    Client = 3,
    Producer = 4,
    Consumer = 5,
}

impl From<SpanKind> for CSpanKind {
    fn from(kind: SpanKind) -> Self {
        match kind {
            SpanKind::Unspecified => CSpanKind::Unspecified,
            SpanKind::Internal => CSpanKind::Internal,
            SpanKind::Server => CSpanKind::Server,
            SpanKind::Client => CSpanKind::Client,
            SpanKind::Producer => CSpanKind::Producer,
            SpanKind::Consumer => CSpanKind::Consumer,
        }
    }
}

/// C-compatible span event
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CSpanEvent {
    /// Event timestamp (nanoseconds since epoch)
    pub timestamp: CTimestamp,
    /// Event name (null-terminated string, must be freed)
    pub name: *mut c_char,
    /// Event attributes
    pub attributes: CKeyValueArray,
}

impl From<SpanEvent> for CSpanEvent {
    fn from(event: SpanEvent) -> Self {
        let name = CString::new(event.name).unwrap().into_raw();
        let attributes = CKeyValueArray::from(event.attributes);

        CSpanEvent {
            timestamp: event.timestamp.as_nanos(),
            name,
            attributes,
        }
    }
}

/// C-compatible array of span events
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CSpanEventArray {
    pub data: *mut CSpanEvent,
    pub len: usize,
}

/// C-compatible span
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CSpan {
    /// Trace ID (32-char hex string, must be freed)
    pub trace_id: *mut c_char,
    /// Span ID (16-char hex string, must be freed)
    pub span_id: *mut c_char,
    /// Parent span ID (16-char hex string, must be freed), null if root span
    pub parent_span_id: *mut c_char,
    /// Service name (must be freed)
    pub service_name: *mut c_char,
    /// Operation name (must be freed)
    pub operation_name: *mut c_char,
    /// Start time (nanoseconds since epoch)
    pub start_time: CTimestamp,
    /// End time (nanoseconds since epoch)
    pub end_time: CTimestamp,
    /// Duration (nanoseconds)
    pub duration: i64,
    /// Span attributes
    pub attributes: CKeyValueArray,
    /// Span events
    pub events: CSpanEventArray,
    /// Span status
    pub status: CSpanStatus,
    /// Span kind
    pub span_kind: CSpanKind,
}

impl From<Span> for CSpan {
    fn from(span: Span) -> Self {
        // Convert IDs to hex strings
        let trace_id = CString::new(span.trace_id.to_hex()).unwrap().into_raw();
        let span_id = CString::new(span.span_id.to_hex()).unwrap().into_raw();
        let parent_span_id = span
            .parent_span_id
            .map(|id| CString::new(id.to_hex()).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());

        // Convert strings
        let service_name = CString::new("unknown").unwrap().into_raw(); // Service name moved to resource registry

        // Extract values before moving
        let start_time = span.start_time.as_nanos();
        let end_time = span.end_time.as_nanos();
        let duration = span.duration.as_nanos();
        let status = CSpanStatus::from(span.status());
        let span_kind = CSpanKind::from(span.span_kind());

        let operation_name = CString::new(span.operation_name).unwrap().into_raw();

        // Convert attributes
        let attributes = CKeyValueArray::from(span.attributes);

        // Convert events
        let mut events_vec: Vec<CSpanEvent> =
            span.events.into_iter().map(CSpanEvent::from).collect();
        let events_len = events_vec.len();
        let events_data = if events_len > 0 {
            let ptr = events_vec.as_mut_ptr();
            std::mem::forget(events_vec);
            ptr
        } else {
            std::ptr::null_mut()
        };

        CSpan {
            trace_id,
            span_id,
            parent_span_id,
            service_name,
            operation_name,
            start_time,
            end_time,
            duration,
            attributes,
            events: CSpanEventArray {
                data: events_data,
                len: events_len,
            },
            status,
            span_kind,
        }
    }
}

/// Free a CSpanEvent and its contents
///
/// # Safety
/// * Must only be called once per CSpanEvent
/// * All pointers must be valid
pub unsafe fn free_span_event(event: CSpanEvent) {
    if !event.name.is_null() {
        let _ = CString::from_raw(event.name);
    }
    super::common::sequins_key_value_array_free(event.attributes);
}

/// C-compatible span query parameters
#[repr(C)]
pub struct CSpanQuery {
    /// Trace ID filter (32-char hex string), null if not filtering
    pub trace_id: *const c_char,
    /// Service name filter (null-terminated string), null if not filtering
    pub service: *const c_char,
    /// Time range start (nanoseconds since epoch)
    pub start_time: super::common::CTimestamp,
    /// Time range end (nanoseconds since epoch)
    pub end_time: super::common::CTimestamp,
    /// Limit number of results, 0 = no limit
    pub limit: usize,
}

/// Stub function to ensure CSpanQuery is exported to C header
#[no_mangle]
pub extern "C" fn sequins_query_spans_stub(_query: CSpanQuery) -> CSpanQueryResult {
    CSpanQueryResult {
        spans: CSpanArray {
            data: std::ptr::null_mut(),
            len: 0,
        },
        cursor: super::common::CQueryCursor {
            opaque: std::ptr::null_mut(),
            timestamp_nanos: 0,
        },
    }
}

/// Free a CSpan and all its contents
///
/// # Safety
/// * Must only be called once per CSpan
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_span_free(span: CSpan) {
    unsafe {
        if !span.trace_id.is_null() {
            let _ = CString::from_raw(span.trace_id);
        }
        if !span.span_id.is_null() {
            let _ = CString::from_raw(span.span_id);
        }
        if !span.parent_span_id.is_null() {
            let _ = CString::from_raw(span.parent_span_id);
        }
        if !span.service_name.is_null() {
            let _ = CString::from_raw(span.service_name);
        }
        if !span.operation_name.is_null() {
            let _ = CString::from_raw(span.operation_name);
        }

        super::common::sequins_key_value_array_free(span.attributes);

        // Free events
        if !span.events.data.is_null() && span.events.len > 0 {
            for i in 0..span.events.len {
                let event = span.events.data.add(i).read();
                free_span_event(event);
            }
            let _ = Vec::from_raw_parts(span.events.data, span.events.len, span.events.len);
        }
    }
}

// =============================================================================
// Span Array and Query Result Types
// =============================================================================

/// C-compatible array of spans
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CSpanArray {
    /// Pointer to array of spans
    pub data: *mut CSpan,
    /// Number of spans in the array
    pub len: usize,
}

impl From<Vec<Span>> for CSpanArray {
    fn from(spans: Vec<Span>) -> Self {
        let len = spans.len();
        if len == 0 {
            return CSpanArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_spans: Vec<CSpan> = spans.into_iter().map(CSpan::from).collect();
        let data = c_spans.as_mut_ptr();
        std::mem::forget(c_spans);
        CSpanArray { data, len }
    }
}

/// C-compatible span query result (historical data + cursor)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CSpanQueryResult {
    /// Array of historical spans
    pub spans: CSpanArray,
    /// Cursor for subscribing to live updates
    pub cursor: super::common::CQueryCursor,
}

impl From<crate::compat::SpanQueryResult> for CSpanQueryResult {
    fn from(result: crate::compat::SpanQueryResult) -> Self {
        CSpanQueryResult {
            spans: CSpanArray::from(result.spans),
            cursor: super::common::CQueryCursor::from(result.cursor),
        }
    }
}

/// Free a CSpanArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_span_array_free(arr: CSpanArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let span = arr.data.add(i).read();
                sequins_span_free(span);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CSpanQueryResult and all its contents
#[no_mangle]
pub extern "C" fn sequins_span_query_result_free(result: CSpanQueryResult) {
    sequins_span_array_free(result.spans);
    super::common::sequins_cursor_free(result.cursor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_types::models::{AttributeValue, Duration, Timestamp};
    use std::collections::HashMap;

    fn create_test_span() -> Span {
        let trace_id = TraceId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let span_id = SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]);
        let parent_span_id = Some(SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, 1]));

        let start_time = Timestamp::from_secs(1000);
        let duration = Duration::from_millis(100);
        let end_time = start_time + duration;

        let mut attributes = HashMap::new();
        attributes.insert(
            "http.method".to_string(),
            AttributeValue::String("GET".to_string()),
        );
        attributes.insert("http.status_code".to_string(), AttributeValue::Int(200));

        let events = vec![SpanEvent {
            timestamp: Timestamp::from_secs(1050),
            name: "request_received".to_string(),
            attributes: HashMap::new(),
        }];

        Span {
            trace_id,
            span_id,
            parent_span_id,
            operation_name: "GET /api/test".to_string(),
            start_time,
            end_time,
            duration,
            attributes,
            events,
            links: Vec::new(),
            status_code: SpanStatus::Ok as u8,
            status_message: None,
            kind: SpanKind::Server as u8,
            trace_state: None,
            flags: None,
            resource_id: 0,
            scope_id: 0,
        }
    }

    #[test]
    fn test_span_conversion() {
        let span = create_test_span();
        let c_span = CSpan::from(span.clone());

        // Verify IDs
        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(c_span.trace_id).to_str().unwrap(),
                span.trace_id.to_hex()
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(c_span.span_id).to_str().unwrap(),
                span.span_id.to_hex()
            );
            assert!(!c_span.parent_span_id.is_null());
        }

        // Verify strings
        unsafe {
            // Service name is now "unknown" because Span has resource_id instead of service_name
            assert_eq!(
                std::ffi::CStr::from_ptr(c_span.service_name)
                    .to_str()
                    .unwrap(),
                "unknown"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(c_span.operation_name)
                    .to_str()
                    .unwrap(),
                "GET /api/test"
            );
        }

        // Verify timestamps
        assert_eq!(c_span.start_time, span.start_time.as_nanos());
        assert_eq!(c_span.end_time, span.end_time.as_nanos());
        assert_eq!(c_span.duration, span.duration.as_nanos());

        // Verify status and kind
        assert_eq!(c_span.status, CSpanStatus::Ok);
        assert_eq!(c_span.span_kind, CSpanKind::Server);

        // Verify attributes
        assert_eq!(c_span.attributes.len, 2);

        // Verify events
        assert_eq!(c_span.events.len, 1);

        sequins_span_free(c_span);
    }

    #[test]
    fn test_span_status_conversion() {
        assert_eq!(CSpanStatus::from(SpanStatus::Unset), CSpanStatus::Unset);
        assert_eq!(CSpanStatus::from(SpanStatus::Ok), CSpanStatus::Ok);
        assert_eq!(CSpanStatus::from(SpanStatus::Error), CSpanStatus::Error);
    }

    #[test]
    fn test_span_kind_conversion() {
        assert_eq!(
            CSpanKind::from(SpanKind::Unspecified),
            CSpanKind::Unspecified
        );
        assert_eq!(CSpanKind::from(SpanKind::Internal), CSpanKind::Internal);
        assert_eq!(CSpanKind::from(SpanKind::Server), CSpanKind::Server);
        assert_eq!(CSpanKind::from(SpanKind::Client), CSpanKind::Client);
        assert_eq!(CSpanKind::from(SpanKind::Producer), CSpanKind::Producer);
        assert_eq!(CSpanKind::from(SpanKind::Consumer), CSpanKind::Consumer);
    }
}
