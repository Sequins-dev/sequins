//! Profile FFI types
//!
//! C-compatible types for pprof profiles and related structures.

use super::common::CTimestamp;
use sequins_types::models::{Profile, ProfileType};
use std::ffi::CString;
use std::os::raw::c_char;

/// C-compatible profile type
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CProfileType {
    Cpu = 0,
    Memory = 1,
    Goroutine = 2,
    Other = 3,
}

impl From<ProfileType> for CProfileType {
    fn from(profile_type: ProfileType) -> Self {
        match profile_type {
            ProfileType::Cpu => CProfileType::Cpu,
            ProfileType::Memory => CProfileType::Memory,
            ProfileType::Goroutine => CProfileType::Goroutine,
            ProfileType::Other => CProfileType::Other,
        }
    }
}

/// C-compatible byte array (for pprof data)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CByteArray {
    pub data: *mut u8,
    pub len: usize,
}

/// C-compatible profile
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CProfile {
    /// Profile ID (UUID hex string, must be freed)
    pub id: *mut c_char,
    /// Timestamp (nanoseconds since epoch)
    pub timestamp: CTimestamp,
    /// Service name (must be freed)
    pub service_name: *mut c_char,
    /// Profile type
    pub profile_type: CProfileType,
    /// Sample type (e.g., "cpu", "alloc_objects", must be freed)
    pub sample_type: *mut c_char,
    /// Sample unit (e.g., "nanoseconds", "bytes", must be freed)
    pub sample_unit: *mut c_char,
    /// Encoded pprof data (must be freed with sequins_byte_array_free)
    pub data: CByteArray,
    /// Associated trace ID (32-char hex string, null if not linked, must be freed)
    pub trace_id: *mut c_char,
}

impl From<Profile> for CProfile {
    fn from(profile: Profile) -> Self {
        let id = CString::new(profile.id.to_hex()).unwrap().into_raw();
        let service_name = CString::new("unknown").unwrap().into_raw(); // Service name moved to resource registry
        let timestamp = profile.timestamp.as_nanos();
        let profile_type = CProfileType::from(profile.get_profile_type());
        let sample_type = CString::new(profile.sample_type).unwrap().into_raw();
        let sample_unit = CString::new(profile.sample_unit).unwrap().into_raw();

        // Convert trace_id
        let trace_id = profile
            .trace_id
            .map(|id| CString::new(id.to_hex()).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut());

        // Convert data bytes
        let data_len = profile.data.len();
        let data = if data_len > 0 {
            let mut data_vec = profile.data;
            let ptr = data_vec.as_mut_ptr();
            std::mem::forget(data_vec);
            CByteArray {
                data: ptr,
                len: data_len,
            }
        } else {
            CByteArray {
                data: std::ptr::null_mut(),
                len: 0,
            }
        };

        CProfile {
            id,
            timestamp,
            service_name,
            profile_type,
            sample_type,
            sample_unit,
            data,
            trace_id,
        }
    }
}

/// Free a CByteArray
///
/// # Safety
/// * Must only be called once per CByteArray
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_byte_array_free(arr: CByteArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// C-compatible profile query parameters
#[repr(C)]
pub struct CProfileQuery {
    /// Service name filter (null-terminated string), null if not filtering
    pub service: *const c_char,
    /// Profile type filter (null-terminated string), null if not filtering
    pub profile_type: *const c_char,
    /// Time range start (nanoseconds since epoch)
    pub start_time: CTimestamp,
    /// Time range end (nanoseconds since epoch)
    pub end_time: CTimestamp,
    /// Trace ID filter (32-char hex string), null if not filtering
    pub trace_id: *const c_char,
    /// Limit number of results, 0 = no limit
    pub limit: usize,
}

/// Stub function to ensure CProfileQuery is exported to C header
///
/// This function is not implemented - use SeQL queries instead
#[no_mangle]
pub extern "C" fn sequins_query_profiles_stub(_query: CProfileQuery) -> CProfileQueryResult {
    CProfileQueryResult {
        profiles: CProfileArray {
            data: std::ptr::null_mut(),
            len: 0,
        },
        cursor: super::common::CQueryCursor {
            opaque: std::ptr::null_mut(),
            timestamp_nanos: 0,
        },
    }
}

/// Free a CProfile and its contents
///
/// # Safety
/// * Must only be called once per CProfile
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_profile_free(profile: CProfile) {
    unsafe {
        if !profile.id.is_null() {
            let _ = CString::from_raw(profile.id);
        }
        if !profile.service_name.is_null() {
            let _ = CString::from_raw(profile.service_name);
        }
        if !profile.sample_type.is_null() {
            let _ = CString::from_raw(profile.sample_type);
        }
        if !profile.sample_unit.is_null() {
            let _ = CString::from_raw(profile.sample_unit);
        }
        if !profile.trace_id.is_null() {
            let _ = CString::from_raw(profile.trace_id);
        }

        sequins_byte_array_free(profile.data);
    }
}

// =============================================================================
// Profile Array and Query Result Types
// =============================================================================

/// C-compatible array of profiles
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CProfileArray {
    /// Pointer to array of profiles
    pub data: *mut CProfile,
    /// Number of profiles in the array
    pub len: usize,
}

impl From<Vec<Profile>> for CProfileArray {
    fn from(profiles: Vec<Profile>) -> Self {
        let len = profiles.len();
        if len == 0 {
            return CProfileArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_profiles: Vec<CProfile> = profiles.into_iter().map(CProfile::from).collect();
        let data = c_profiles.as_mut_ptr();
        std::mem::forget(c_profiles);
        CProfileArray { data, len }
    }
}

/// C-compatible profile query result (historical data + cursor)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CProfileQueryResult {
    /// Array of historical profiles
    pub profiles: CProfileArray,
    /// Cursor for subscribing to live updates
    pub cursor: super::common::CQueryCursor,
}

impl From<crate::compat::ProfileQueryResult> for CProfileQueryResult {
    fn from(result: crate::compat::ProfileQueryResult) -> Self {
        CProfileQueryResult {
            profiles: CProfileArray::from(result.profiles),
            cursor: super::common::CQueryCursor::from(result.cursor),
        }
    }
}

/// Free a CProfileArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_profile_array_free(arr: CProfileArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let profile = arr.data.add(i).read();
                sequins_profile_free(profile);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CProfileQueryResult and all its contents
#[no_mangle]
pub extern "C" fn sequins_profile_query_result_free(result: CProfileQueryResult) {
    sequins_profile_array_free(result.profiles);
    super::common::sequins_cursor_free(result.cursor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_types::models::{ProfileId, Timestamp, TraceId};

    fn create_test_profile() -> Profile {
        use std::collections::HashMap;

        Profile {
            id: ProfileId::new(),
            timestamp: Timestamp::from_secs(1000),
            profile_type: ProfileType::Cpu as u8,
            sample_type: "cpu".to_string(),
            sample_unit: "nanoseconds".to_string(),
            duration_nanos: 1000000,
            period: 10000000,
            period_type: "cpu".to_string(),
            period_unit: "nanoseconds".to_string(),
            resource_id: 0,
            scope_id: 0,
            original_format: None,
            attributes: HashMap::new(),
            data: vec![1, 2, 3, 4, 5],
            trace_id: None,
        }
    }

    #[test]
    fn test_profile_conversion() {
        let profile = create_test_profile();
        let c_profile = CProfile::from(profile);

        unsafe {
            // Service name is now "unknown" because Profile has resource_id instead of service_name
            assert_eq!(
                std::ffi::CStr::from_ptr(c_profile.service_name)
                    .to_str()
                    .unwrap(),
                "unknown"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(c_profile.sample_type)
                    .to_str()
                    .unwrap(),
                "cpu"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(c_profile.sample_unit)
                    .to_str()
                    .unwrap(),
                "nanoseconds"
            );
        }

        assert_eq!(c_profile.profile_type, CProfileType::Cpu);
        assert_eq!(c_profile.data.len, 5);
        assert!(c_profile.trace_id.is_null());

        sequins_profile_free(c_profile);
    }

    #[test]
    fn test_profile_with_trace_context() {
        let mut profile = create_test_profile();
        profile.trace_id = Some(TraceId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        ]));

        let c_profile = CProfile::from(profile);

        assert!(!c_profile.trace_id.is_null());

        sequins_profile_free(c_profile);
    }

    #[test]
    fn test_profile_type_conversion() {
        assert_eq!(CProfileType::from(ProfileType::Cpu), CProfileType::Cpu);
        assert_eq!(
            CProfileType::from(ProfileType::Memory),
            CProfileType::Memory
        );
        assert_eq!(
            CProfileType::from(ProfileType::Goroutine),
            CProfileType::Goroutine
        );
        assert_eq!(CProfileType::from(ProfileType::Other), CProfileType::Other);
    }
}
