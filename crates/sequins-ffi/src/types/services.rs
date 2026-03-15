//! Service FFI types
//!
//! C-compatible types for service listing.

use crate::compat::Service;
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_char;

/// C-compatible resource attribute (key with comma-separated values)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CResourceAttribute {
    /// Attribute key (e.g., "service.version")
    pub key: *mut c_char,
    /// Comma-separated values (e.g., "1.0.0, 1.0.1")
    pub values: *mut c_char,
}

/// C-compatible array of resource attributes
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CResourceAttributeArray {
    pub data: *mut CResourceAttribute,
    pub len: usize,
}

impl From<HashMap<String, Vec<String>>> for CResourceAttributeArray {
    fn from(attrs: HashMap<String, Vec<String>>) -> Self {
        let len = attrs.len();
        if len == 0 {
            return CResourceAttributeArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }

        let mut c_attrs: Vec<CResourceAttribute> = attrs
            .into_iter()
            .map(|(key, values)| CResourceAttribute {
                key: CString::new(key).unwrap().into_raw(),
                values: CString::new(values.join(", ")).unwrap().into_raw(),
            })
            .collect();

        let data = c_attrs.as_mut_ptr();
        std::mem::forget(c_attrs);
        CResourceAttributeArray { data, len }
    }
}

/// C-compatible service entry
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CService {
    /// Service name (must be freed)
    pub name: *mut c_char,
    /// Number of spans from this service
    pub span_count: usize,
    /// Number of logs from this service
    pub log_count: usize,
    /// Aggregated resource attributes
    pub resource_attributes: CResourceAttributeArray,
}

impl From<Service> for CService {
    fn from(service: Service) -> Self {
        let name = CString::new(service.name).unwrap().into_raw();
        let resource_attributes = CResourceAttributeArray::from(service.resource_attributes);

        CService {
            name,
            span_count: service.span_count,
            log_count: service.log_count,
            resource_attributes,
        }
    }
}

/// C-compatible array of services
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CServiceArray {
    pub data: *mut CService,
    pub len: usize,
}

impl From<Vec<Service>> for CServiceArray {
    fn from(services: Vec<Service>) -> Self {
        let len = services.len();
        let mut c_services: Vec<CService> = services.into_iter().map(CService::from).collect();
        let data = c_services.as_mut_ptr();
        std::mem::forget(c_services);
        CServiceArray { data, len }
    }
}

/// Free a CResourceAttribute and its contents
///
/// # Safety
/// * Must only be called once per CResourceAttribute
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_resource_attribute_free(attr: CResourceAttribute) {
    unsafe {
        if !attr.key.is_null() {
            let _ = CString::from_raw(attr.key);
        }
        if !attr.values.is_null() {
            let _ = CString::from_raw(attr.values);
        }
    }
}

/// Free a CResourceAttributeArray and all its contents
///
/// # Safety
/// * Must only be called once per CResourceAttributeArray
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_resource_attribute_array_free(arr: CResourceAttributeArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let attr = arr.data.add(i).read();
                sequins_resource_attribute_free(attr);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CService and its contents
///
/// # Safety
/// * Must only be called once per CService
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_service_free(service: CService) {
    unsafe {
        if !service.name.is_null() {
            let _ = CString::from_raw(service.name);
        }
    }
    sequins_resource_attribute_array_free(service.resource_attributes);
}

/// Free a CServiceArray and all its contents
///
/// # Safety
/// * Must only be called once per CServiceArray
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_service_array_free(arr: CServiceArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let service = arr.data.add(i).read();
                sequins_service_free(service);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_service() -> Service {
        let mut resource_attributes = HashMap::new();
        resource_attributes.insert("service.version".to_string(), vec!["1.0.0".to_string()]);
        resource_attributes.insert(
            "deployment.environment".to_string(),
            vec!["production".to_string(), "staging".to_string()],
        );

        Service {
            name: "test-service".to_string(),
            span_count: 100,
            log_count: 50,
            resource_attributes,
        }
    }

    #[test]
    fn test_service_conversion() {
        let service = create_test_service();
        let c_service = CService::from(service.clone());

        // Verify strings
        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(c_service.name).to_str().unwrap(),
                "test-service"
            );
        }

        // Verify counts
        assert_eq!(c_service.span_count, 100);
        assert_eq!(c_service.log_count, 50);

        // Verify resource attributes
        assert_eq!(c_service.resource_attributes.len, 2);

        sequins_service_free(c_service);
    }

    #[test]
    fn test_service_array_conversion() {
        let mut resource_attributes = HashMap::new();
        resource_attributes.insert("service.version".to_string(), vec!["1.0.0".to_string()]);

        let services = vec![
            Service {
                name: "service-a".to_string(),
                span_count: 10,
                log_count: 5,
                resource_attributes: resource_attributes.clone(),
            },
            Service {
                name: "service-b".to_string(),
                span_count: 20,
                log_count: 15,
                resource_attributes: HashMap::new(),
            },
        ];

        let c_arr = CServiceArray::from(services);
        assert_eq!(c_arr.len, 2);

        sequins_service_array_free(c_arr);
    }

    #[test]
    fn test_empty_resource_attributes() {
        let service = Service {
            name: "empty-service".to_string(),
            span_count: 0,
            log_count: 0,
            resource_attributes: HashMap::new(),
        };

        let c_service = CService::from(service);
        assert!(c_service.resource_attributes.data.is_null());
        assert_eq!(c_service.resource_attributes.len, 0);

        sequins_service_free(c_service);
    }
}
