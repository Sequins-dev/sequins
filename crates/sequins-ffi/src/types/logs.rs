//! Log FFI types
//!
//! C-compatible types for OpenTelemetry logs and related structures.

use super::common::{CKeyValueArray, CTimestamp};
use sequins_types::models::{LogEntry, LogSeverity};
use std::ffi::CString;
use std::os::raw::c_char;

/// C-compatible log severity
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CLogSeverity {
    LogTrace = 1,
    LogDebug = 5,
    LogInfo = 9,
    LogWarn = 13,
    LogError = 17,
    LogFatal = 21,
}

impl From<LogSeverity> for CLogSeverity {
    fn from(severity: LogSeverity) -> Self {
        match severity {
            LogSeverity::Trace => CLogSeverity::LogTrace,
            LogSeverity::Debug => CLogSeverity::LogDebug,
            LogSeverity::Info => CLogSeverity::LogInfo,
            LogSeverity::Warn => CLogSeverity::LogWarn,
            LogSeverity::Error => CLogSeverity::LogError,
            LogSeverity::Fatal => CLogSeverity::LogFatal,
        }
    }
}

/// C-compatible log entry
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CLogEntry {
    /// Log ID (UUID string, must be freed)
    pub id: *mut c_char,
    /// Timestamp (nanoseconds since epoch)
    pub timestamp: CTimestamp,
    /// Observed timestamp (nanoseconds since epoch)
    pub observed_timestamp: CTimestamp,
    /// Service name (must be freed)
    pub service_name: *mut c_char,
    /// Severity level
    pub severity: CLogSeverity,
    /// Log message body (must be freed)
    pub body: *mut c_char,
    /// Log attributes
    pub attributes: CKeyValueArray,
    /// Trace ID (32-char hex string, must be freed), null if not linked to trace
    pub trace_id: *mut c_char,
    /// Span ID (16-char hex string, must be freed), null if not linked to span
    pub span_id: *mut c_char,
}

impl From<LogEntry> for CLogEntry {
    fn from(log: LogEntry) -> Self {
        let id = CString::new(log.id.to_hex()).unwrap().into_raw();
        let service_name = CString::new("unknown").unwrap().into_raw(); // Service name moved to resource registry
        let body_str = match &log.body {
            sequins_types::models::AttributeValue::String(s) => s.clone(),
            other => serde_json::to_string(&other).unwrap_or_default(),
        };
        let body = CString::new(body_str).unwrap().into_raw();

        let timestamp = log.timestamp.as_nanos();
        let observed_timestamp = log.observed_timestamp.as_nanos();
        let severity = CLogSeverity::from(log.severity());

        let trace_id = log
            .trace_id
            .map(|id| CString::new(id.to_hex()).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());

        let span_id = log
            .span_id
            .map(|id| CString::new(id.to_hex()).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());

        let attributes = CKeyValueArray::from(log.attributes);

        CLogEntry {
            id,
            timestamp,
            observed_timestamp,
            service_name,
            severity,
            body,
            attributes,
            trace_id,
            span_id,
        }
    }
}

/// C-compatible log query parameters
#[repr(C)]
pub struct CLogQuery {
    /// Service name filter (null-terminated string), null if not filtering
    pub service: *const c_char,
    /// Time range start (nanoseconds since epoch)
    pub start_time: super::common::CTimestamp,
    /// Time range end (nanoseconds since epoch)
    pub end_time: super::common::CTimestamp,
    /// Array of severity levels to include (null = all severities)
    pub severities: *const CLogSeverity,
    /// Number of severity levels in the array
    pub severities_len: usize,
    /// Full-text search in log body (null-terminated string), null if not searching
    pub search: *const c_char,
    /// Limit number of results, 0 = no limit
    pub limit: usize,
}

/// Stub function to ensure CLogQuery is exported to C header
#[no_mangle]
pub extern "C" fn sequins_query_logs_stub(_query: CLogQuery) -> CLogQueryResult {
    CLogQueryResult {
        logs: CLogEntryArray {
            data: std::ptr::null_mut(),
            len: 0,
        },
        cursor: super::common::CQueryCursor {
            opaque: std::ptr::null_mut(),
            timestamp_nanos: 0,
        },
    }
}

/// Free a CLogEntry and all its contents
///
/// # Safety
/// * Must only be called once per CLogEntry
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_log_entry_free(log: CLogEntry) {
    unsafe {
        if !log.id.is_null() {
            let _ = CString::from_raw(log.id);
        }
        if !log.service_name.is_null() {
            let _ = CString::from_raw(log.service_name);
        }
        if !log.body.is_null() {
            let _ = CString::from_raw(log.body);
        }
        if !log.trace_id.is_null() {
            let _ = CString::from_raw(log.trace_id);
        }
        if !log.span_id.is_null() {
            let _ = CString::from_raw(log.span_id);
        }

        super::common::sequins_key_value_array_free(log.attributes);
    }
}

// =============================================================================
// Log Array and Query Result Types
// =============================================================================

/// C-compatible array of logs
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CLogEntryArray {
    /// Pointer to array of logs
    pub data: *mut CLogEntry,
    /// Number of logs in the array
    pub len: usize,
}

impl From<Vec<LogEntry>> for CLogEntryArray {
    fn from(logs: Vec<LogEntry>) -> Self {
        let len = logs.len();
        if len == 0 {
            return CLogEntryArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_logs: Vec<CLogEntry> = logs.into_iter().map(CLogEntry::from).collect();
        let data = c_logs.as_mut_ptr();
        std::mem::forget(c_logs);
        CLogEntryArray { data, len }
    }
}

/// C-compatible log query result (historical data + cursor)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CLogQueryResult {
    /// Array of historical logs
    pub logs: CLogEntryArray,
    /// Cursor for subscribing to live updates
    pub cursor: super::common::CQueryCursor,
}

impl From<crate::compat::LogQueryResult> for CLogQueryResult {
    fn from(result: crate::compat::LogQueryResult) -> Self {
        CLogQueryResult {
            logs: CLogEntryArray::from(result.logs),
            cursor: super::common::CQueryCursor::from(result.cursor),
        }
    }
}

/// Free a CLogEntryArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_log_entry_array_free(arr: CLogEntryArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let log = arr.data.add(i).read();
                sequins_log_entry_free(log);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CLogQueryResult and all its contents
#[no_mangle]
pub extern "C" fn sequins_log_query_result_free(result: CLogQueryResult) {
    sequins_log_entry_array_free(result.logs);
    super::common::sequins_cursor_free(result.cursor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_types::models::{LogId, Timestamp};
    use std::collections::HashMap;

    fn create_test_log() -> LogEntry {
        use sequins_types::models::AttributeValue;
        LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::from_secs(1000),
            observed_timestamp: Timestamp::from_secs(1001),
            severity_number: LogSeverity::Info as u8,
            body: AttributeValue::String("Test log message".to_string()),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            flags: None,
            resource_id: 0,
            scope_id: 0,
        }
    }

    #[test]
    fn test_log_severity_conversion() {
        assert_eq!(
            CLogSeverity::from(LogSeverity::Trace),
            CLogSeverity::LogTrace
        );
        assert_eq!(
            CLogSeverity::from(LogSeverity::Debug),
            CLogSeverity::LogDebug
        );
        assert_eq!(CLogSeverity::from(LogSeverity::Info), CLogSeverity::LogInfo);
        assert_eq!(CLogSeverity::from(LogSeverity::Warn), CLogSeverity::LogWarn);
        assert_eq!(
            CLogSeverity::from(LogSeverity::Error),
            CLogSeverity::LogError
        );
        assert_eq!(
            CLogSeverity::from(LogSeverity::Fatal),
            CLogSeverity::LogFatal
        );
    }

    #[test]
    fn test_log_entry_conversion() {
        let log = create_test_log();
        let c_log = CLogEntry::from(log.clone());

        // Verify strings
        unsafe {
            // Service name is now "unknown" because LogEntry has resource_id instead of service_name
            assert_eq!(
                std::ffi::CStr::from_ptr(c_log.service_name)
                    .to_str()
                    .unwrap(),
                "unknown"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(c_log.body).to_str().unwrap(),
                "Test log message"
            );
        }

        // Verify timestamps
        assert_eq!(c_log.timestamp, log.timestamp.as_nanos());

        // Verify severity
        assert_eq!(c_log.severity, CLogSeverity::LogInfo);

        // Verify null trace/span IDs
        assert!(c_log.trace_id.is_null());
        assert!(c_log.span_id.is_null());

        sequins_log_entry_free(c_log);
    }
}
