use sequins_types::models::traces::AttributeValue;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_char;

/// C-compatible timestamp (nanoseconds since epoch)
pub type CTimestamp = i64;

/// C-compatible duration (nanoseconds)
pub type CDuration = i64;

/// Tag for discriminated union CAttributeValue
#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CAttributeValueTag {
    String = 0,
    Bool = 1,
    Int = 2,
    Double = 3,
    StringArray = 4,
    BoolArray = 5,
    IntArray = 6,
    DoubleArray = 7,
}

/// C-compatible array of strings
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CStringArray {
    pub data: *mut *mut c_char,
    pub len: usize,
}

/// C-compatible array of booleans
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CBoolArray {
    pub data: *mut bool,
    pub len: usize,
}

/// C-compatible array of int64
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CIntArray {
    pub data: *mut i64,
    pub len: usize,
}

/// C-compatible array of doubles
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CDoubleArray {
    pub data: *mut f64,
    pub len: usize,
}

/// C-compatible attribute value (discriminated union)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CAttributeValue {
    pub tag: CAttributeValueTag,
    pub value: CAttributeValueUnion,
}

/// Union for attribute value data
#[repr(C)]
#[derive(Copy, Clone)]
pub union CAttributeValueUnion {
    pub string_val: *mut c_char,
    pub bool_val: bool,
    pub int_val: i64,
    pub double_val: f64,
    pub string_array: CStringArray,
    pub bool_array: CBoolArray,
    pub int_array: CIntArray,
    pub double_array: CDoubleArray,
}

/// C-compatible key-value pair
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CKeyValue {
    pub key: *mut c_char,
    pub value: CAttributeValue,
}

/// C-compatible array of key-value pairs (for OTEL attributes)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CKeyValueArray {
    pub data: *mut CKeyValue,
    pub len: usize,
}

// Conversion from Rust AttributeValue to C
impl From<AttributeValue> for CAttributeValue {
    fn from(value: AttributeValue) -> Self {
        match value {
            AttributeValue::String(s) => {
                let c_str = CString::new(s).unwrap();
                CAttributeValue {
                    tag: CAttributeValueTag::String,
                    value: CAttributeValueUnion {
                        string_val: c_str.into_raw(),
                    },
                }
            }
            AttributeValue::Bool(b) => CAttributeValue {
                tag: CAttributeValueTag::Bool,
                value: CAttributeValueUnion { bool_val: b },
            },
            AttributeValue::Int(i) => CAttributeValue {
                tag: CAttributeValueTag::Int,
                value: CAttributeValueUnion { int_val: i },
            },
            AttributeValue::Double(d) => CAttributeValue {
                tag: CAttributeValueTag::Double,
                value: CAttributeValueUnion { double_val: d },
            },
            AttributeValue::StringArray(arr) => {
                let len = arr.len();
                let mut c_strings: Vec<*mut c_char> = arr
                    .into_iter()
                    .map(|s| CString::new(s).unwrap().into_raw())
                    .collect();
                let data = c_strings.as_mut_ptr();
                std::mem::forget(c_strings); // Prevent Vec from freeing the pointers
                CAttributeValue {
                    tag: CAttributeValueTag::StringArray,
                    value: CAttributeValueUnion {
                        string_array: CStringArray { data, len },
                    },
                }
            }
            AttributeValue::BoolArray(arr) => {
                let len = arr.len();
                let mut vec = arr.into_iter().collect::<Vec<_>>();
                let data = vec.as_mut_ptr();
                std::mem::forget(vec);
                CAttributeValue {
                    tag: CAttributeValueTag::BoolArray,
                    value: CAttributeValueUnion {
                        bool_array: CBoolArray { data, len },
                    },
                }
            }
            AttributeValue::IntArray(arr) => {
                let len = arr.len();
                let mut vec = arr.into_iter().collect::<Vec<_>>();
                let data = vec.as_mut_ptr();
                std::mem::forget(vec);
                CAttributeValue {
                    tag: CAttributeValueTag::IntArray,
                    value: CAttributeValueUnion {
                        int_array: CIntArray { data, len },
                    },
                }
            }
            AttributeValue::DoubleArray(arr) => {
                let len = arr.len();
                let mut vec = arr.into_iter().collect::<Vec<_>>();
                let data = vec.as_mut_ptr();
                std::mem::forget(vec);
                CAttributeValue {
                    tag: CAttributeValueTag::DoubleArray,
                    value: CAttributeValueUnion {
                        double_array: CDoubleArray { data, len },
                    },
                }
            }
            AttributeValue::Bytes(bytes) => {
                use base64::Engine as _;
                let s = base64::engine::general_purpose::STANDARD.encode(&bytes);
                let c_str = CString::new(s).unwrap();
                CAttributeValue {
                    tag: CAttributeValueTag::String,
                    value: CAttributeValueUnion {
                        string_val: c_str.into_raw(),
                    },
                }
            }
            AttributeValue::KvList(kvs) => {
                let json_str = serde_json::to_string(&kvs).unwrap_or_default();
                let c_str = CString::new(json_str).unwrap();
                CAttributeValue {
                    tag: CAttributeValueTag::String,
                    value: CAttributeValueUnion {
                        string_val: c_str.into_raw(),
                    },
                }
            }
            AttributeValue::Array(arr) => {
                let json_str = serde_json::to_string(&arr).unwrap_or_default();
                let c_str = CString::new(json_str).unwrap();
                CAttributeValue {
                    tag: CAttributeValueTag::String,
                    value: CAttributeValueUnion {
                        string_val: c_str.into_raw(),
                    },
                }
            }
        }
    }
}

// Conversion from HashMap to CKeyValueArray
impl From<HashMap<String, AttributeValue>> for CKeyValueArray {
    fn from(map: HashMap<String, AttributeValue>) -> Self {
        let len = map.len();
        let mut pairs: Vec<CKeyValue> = map
            .into_iter()
            .map(|(k, v)| {
                let key = CString::new(k).unwrap().into_raw();
                let value = CAttributeValue::from(v);
                CKeyValue { key, value }
            })
            .collect();
        let data = pairs.as_mut_ptr();
        std::mem::forget(pairs);
        CKeyValueArray { data, len }
    }
}

/// Free a CAttributeValue and its contents
#[no_mangle]
pub extern "C" fn sequins_attribute_value_free(value: CAttributeValue) {
    unsafe {
        match value.tag {
            CAttributeValueTag::String => {
                if !value.value.string_val.is_null() {
                    let _ = CString::from_raw(value.value.string_val);
                }
            }
            CAttributeValueTag::StringArray => {
                let arr = value.value.string_array;
                if !arr.data.is_null() && arr.len > 0 {
                    for i in 0..arr.len {
                        let ptr = *arr.data.add(i);
                        if !ptr.is_null() {
                            let _ = CString::from_raw(ptr);
                        }
                    }
                    let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
                }
            }
            CAttributeValueTag::BoolArray => {
                let arr = value.value.bool_array;
                if !arr.data.is_null() && arr.len > 0 {
                    let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
                }
            }
            CAttributeValueTag::IntArray => {
                let arr = value.value.int_array;
                if !arr.data.is_null() && arr.len > 0 {
                    let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
                }
            }
            CAttributeValueTag::DoubleArray => {
                let arr = value.value.double_array;
                if !arr.data.is_null() && arr.len > 0 {
                    let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
                }
            }
            _ => {} // Other types don't allocate
        }
    }
}

/// Free a CKeyValueArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_key_value_array_free(arr: CKeyValueArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let pair = arr.data.add(i).read();
                if !pair.key.is_null() {
                    let _ = CString::from_raw(pair.key);
                }
                sequins_attribute_value_free(pair.value);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

// =============================================================================
// Query Cursor Type
// =============================================================================

/// C-compatible query cursor for two-phase query pattern
///
/// The cursor contains an opaque string that encodes the timestamp when the
/// historical query was executed. It should be passed to subscribe functions
/// to resume from where the query left off.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CQueryCursor {
    /// Opaque cursor string (base64-encoded, must be freed with sequins_string_free)
    pub opaque: *mut c_char,
    /// Timestamp in nanoseconds (for inspection, but use opaque for subscribe)
    pub timestamp_nanos: CTimestamp,
}

impl From<crate::compat::QueryCursor> for CQueryCursor {
    fn from(cursor: crate::compat::QueryCursor) -> Self {
        let opaque = CString::new(cursor.to_opaque()).unwrap().into_raw();
        CQueryCursor {
            opaque,
            timestamp_nanos: cursor.query_timestamp.as_nanos(),
        }
    }
}

/// Free a CQueryCursor
#[no_mangle]
pub extern "C" fn sequins_cursor_free(cursor: CQueryCursor) {
    unsafe {
        if !cursor.opaque.is_null() {
            let _ = CString::from_raw(cursor.opaque);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_value_string() {
        let rust_val = AttributeValue::String("test".to_string());
        let c_val = CAttributeValue::from(rust_val);
        assert_eq!(c_val.tag, CAttributeValueTag::String);
        unsafe {
            let s = std::ffi::CStr::from_ptr(c_val.value.string_val);
            assert_eq!(s.to_str().unwrap(), "test");
        }
        sequins_attribute_value_free(c_val);
    }

    #[test]
    fn test_attribute_value_int() {
        let rust_val = AttributeValue::Int(42);
        let c_val = CAttributeValue::from(rust_val);
        assert_eq!(c_val.tag, CAttributeValueTag::Int);
        unsafe {
            assert_eq!(c_val.value.int_val, 42);
        }
    }

    #[test]
    fn test_hashmap_conversion() {
        let mut map = HashMap::new();
        map.insert(
            "key1".to_string(),
            AttributeValue::String("value1".to_string()),
        );
        map.insert("key2".to_string(), AttributeValue::Int(123));

        let c_arr = CKeyValueArray::from(map);
        assert_eq!(c_arr.len, 2);
        sequins_key_value_array_free(c_arr);
    }
}
