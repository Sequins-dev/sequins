//! Profile sample FFI types
//!
//! C-compatible types for decomposed profile samples with resolved stack frames.

use super::common::CTimestamp;
use super::profiles::CProfileType;
use sequins_types::models::{ProfileSample, StackFrame};
use std::ffi::CString;
use std::os::raw::c_char;

/// C-compatible stack frame (single function in a call stack)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CStackFrame {
    /// Function name (must be freed)
    pub function_name: *mut c_char,
    /// Source file path (null if unknown, must be freed)
    pub file: *mut c_char,
    /// Source line number (0 if unknown)
    pub line: u32,
    /// Module or package name (null if unknown, must be freed)
    pub module: *mut c_char,
}

impl From<StackFrame> for CStackFrame {
    fn from(frame: StackFrame) -> Self {
        let function_name = CString::new(frame.function_name).unwrap().into_raw();
        let file = frame
            .file
            .map(|f| CString::new(f).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());
        let line = frame.line.unwrap_or(0);
        let module = frame
            .module
            .map(|m| CString::new(m).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());

        CStackFrame {
            function_name,
            file,
            line,
            module,
        }
    }
}

/// C-compatible array of stack frames
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CStackFrameArray {
    pub data: *mut CStackFrame,
    pub len: usize,
}

impl From<Vec<StackFrame>> for CStackFrameArray {
    fn from(frames: Vec<StackFrame>) -> Self {
        let len = frames.len();
        if len == 0 {
            return CStackFrameArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_frames: Vec<CStackFrame> = frames.into_iter().map(CStackFrame::from).collect();
        let data = c_frames.as_mut_ptr();
        std::mem::forget(c_frames);
        CStackFrameArray { data, len }
    }
}

/// C-compatible profile sample with resolved stack frames
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CProfileSample {
    /// Profile ID (hex string, must be freed)
    pub profile_id: *mut c_char,
    /// Timestamp (nanoseconds since epoch)
    pub timestamp: CTimestamp,
    /// Service name (must be freed)
    pub service_name: *mut c_char,
    /// Profile type
    pub profile_type: CProfileType,
    /// Value type label (e.g., "cpu", "alloc_objects", must be freed)
    pub value_type: *mut c_char,
    /// Sample value (unit depends on value_type)
    pub value: i64,
    /// Call stack — leaf (innermost) to root (outermost)
    pub stack: CStackFrameArray,
    /// Associated trace ID (32-char hex string, null if not linked, must be freed)
    pub trace_id: *mut c_char,
}

impl From<ProfileSample> for CProfileSample {
    fn from(sample: ProfileSample) -> Self {
        let profile_id = CString::new(sample.profile_id.to_hex()).unwrap().into_raw();
        let service_name = CString::new(sample.service_name).unwrap().into_raw();
        let value_type = CString::new(sample.value_type).unwrap().into_raw();
        let trace_id = sample
            .trace_id
            .map(|id| CString::new(id.to_hex()).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());
        let stack = CStackFrameArray::from(sample.stack);

        CProfileSample {
            profile_id,
            timestamp: sample.timestamp.as_nanos(),
            service_name,
            profile_type: CProfileType::from(sample.profile_type),
            value_type,
            value: sample.value,
            stack,
            trace_id,
        }
    }
}

/// Free a CStackFrame and its contents
///
/// # Safety
/// * Must only be called once per CStackFrame
pub unsafe fn free_stack_frame(frame: CStackFrame) {
    if !frame.function_name.is_null() {
        let _ = CString::from_raw(frame.function_name);
    }
    if !frame.file.is_null() {
        let _ = CString::from_raw(frame.file);
    }
    if !frame.module.is_null() {
        let _ = CString::from_raw(frame.module);
    }
}

/// C-compatible profile sample query parameters
#[repr(C)]
pub struct CProfileSampleQuery {
    /// Service name filter (null-terminated string), null if not filtering
    pub service: *const c_char,
    /// Profile type filter (null-terminated string), null if not filtering
    pub profile_type: *const c_char,
    /// Value type filter (null-terminated string), null if not filtering
    pub value_type: *const c_char,
    /// Time range start (nanoseconds since epoch)
    pub start_time: super::common::CTimestamp,
    /// Time range end (nanoseconds since epoch)
    pub end_time: super::common::CTimestamp,
    /// Trace ID filter (32-char hex string), null if not filtering
    pub trace_id: *const c_char,
    /// Limit number of results, 0 = no limit
    pub limit: usize,
}

/// Stub function to ensure CProfileSampleQuery is exported to C header
#[no_mangle]
pub extern "C" fn sequins_query_profile_samples_stub(
    _query: CProfileSampleQuery,
) -> CProfileSampleQueryResult {
    CProfileSampleQueryResult {
        samples: CProfileSampleArray {
            data: std::ptr::null_mut(),
            len: 0,
        },
        cursor: super::common::CQueryCursor {
            opaque: std::ptr::null_mut(),
            timestamp_nanos: 0,
        },
    }
}

/// Free a CProfileSample and its contents
///
/// # Safety
/// * Must only be called once per CProfileSample
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_profile_sample_free(sample: CProfileSample) {
    unsafe {
        if !sample.profile_id.is_null() {
            let _ = CString::from_raw(sample.profile_id);
        }
        if !sample.service_name.is_null() {
            let _ = CString::from_raw(sample.service_name);
        }
        if !sample.value_type.is_null() {
            let _ = CString::from_raw(sample.value_type);
        }
        if !sample.trace_id.is_null() {
            let _ = CString::from_raw(sample.trace_id);
        }

        // Free stack frames
        if !sample.stack.data.is_null() && sample.stack.len > 0 {
            for i in 0..sample.stack.len {
                let frame = sample.stack.data.add(i).read();
                free_stack_frame(frame);
            }
            let _ = Vec::from_raw_parts(sample.stack.data, sample.stack.len, sample.stack.len);
        }
    }
}

// =============================================================================
// Profile Sample Array and Query Result Types
// =============================================================================

/// C-compatible array of profile samples
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CProfileSampleArray {
    pub data: *mut CProfileSample,
    pub len: usize,
}

impl From<Vec<ProfileSample>> for CProfileSampleArray {
    fn from(samples: Vec<ProfileSample>) -> Self {
        let len = samples.len();
        if len == 0 {
            return CProfileSampleArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_samples: Vec<CProfileSample> =
            samples.into_iter().map(CProfileSample::from).collect();
        let data = c_samples.as_mut_ptr();
        std::mem::forget(c_samples);
        CProfileSampleArray { data, len }
    }
}

/// C-compatible profile sample query result (historical data + cursor)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CProfileSampleQueryResult {
    pub samples: CProfileSampleArray,
    pub cursor: super::common::CQueryCursor,
}

impl From<crate::compat::ProfileSampleQueryResult> for CProfileSampleQueryResult {
    fn from(result: crate::compat::ProfileSampleQueryResult) -> Self {
        CProfileSampleQueryResult {
            samples: CProfileSampleArray::from(result.samples),
            cursor: super::common::CQueryCursor::from(result.cursor),
        }
    }
}

/// Free a CProfileSampleArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_profile_sample_array_free(arr: CProfileSampleArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let sample = arr.data.add(i).read();
                sequins_profile_sample_free(sample);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CProfileSampleQueryResult and all its contents
#[no_mangle]
pub extern "C" fn sequins_profile_sample_query_result_free(result: CProfileSampleQueryResult) {
    sequins_profile_sample_array_free(result.samples);
    super::common::sequins_cursor_free(result.cursor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_types::models::{ProfileId, ProfileType, Timestamp, TraceId};

    fn create_test_sample() -> ProfileSample {
        use sequins_types::models::AttributeValue;
        use std::collections::HashMap;

        ProfileSample {
            profile_id: ProfileId::new(),
            timestamp: Timestamp::from_secs(1000),
            service_name: "test-service".to_string(),
            profile_type: ProfileType::Cpu,
            value_type: "cpu".to_string(),
            value_unit: "nanoseconds".to_string(),
            value: 42_000_000,
            resource_id: 0,
            scope_id: 0,
            stack: vec![
                StackFrame {
                    function_name: "leaf_fn".to_string(),
                    file: Some("src/main.rs".to_string()),
                    line: Some(10),
                    module: Some("my_crate".to_string()),
                },
                StackFrame {
                    function_name: "root_fn".to_string(),
                    file: None,
                    line: None,
                    module: None,
                },
            ],
            stack_id: 1,
            trace_id: None,
            span_id: None,
            attributes: HashMap::new(),
        }
    }

    #[test]
    fn test_profile_sample_conversion() {
        let sample = create_test_sample();
        let c_sample = CProfileSample::from(sample);

        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(c_sample.service_name)
                    .to_str()
                    .unwrap(),
                "test-service"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(c_sample.value_type)
                    .to_str()
                    .unwrap(),
                "cpu"
            );
        }

        assert_eq!(c_sample.value, 42_000_000);
        assert_eq!(c_sample.profile_type, CProfileType::Cpu);
        assert_eq!(c_sample.stack.len, 2);
        assert!(c_sample.trace_id.is_null());

        unsafe {
            let leaf = c_sample.stack.data.read();
            assert_eq!(
                std::ffi::CStr::from_ptr(leaf.function_name)
                    .to_str()
                    .unwrap(),
                "leaf_fn"
            );
            assert_eq!(leaf.line, 10);
            assert!(!leaf.file.is_null());
            assert!(!leaf.module.is_null());

            let root = c_sample.stack.data.add(1).read();
            assert_eq!(
                std::ffi::CStr::from_ptr(root.function_name)
                    .to_str()
                    .unwrap(),
                "root_fn"
            );
            assert_eq!(root.line, 0);
            assert!(root.file.is_null());
            assert!(root.module.is_null());
        }

        sequins_profile_sample_free(c_sample);
    }

    #[test]
    fn test_profile_sample_with_trace() {
        let mut sample = create_test_sample();
        sample.trace_id = Some(TraceId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        ]));

        let c_sample = CProfileSample::from(sample);
        assert!(!c_sample.trace_id.is_null());
        sequins_profile_sample_free(c_sample);
    }

    #[test]
    fn test_empty_stack() {
        let mut sample = create_test_sample();
        sample.stack = vec![];

        let c_sample = CProfileSample::from(sample);
        assert_eq!(c_sample.stack.len, 0);
        assert!(c_sample.stack.data.is_null());

        sequins_profile_sample_free(c_sample);
    }
}
