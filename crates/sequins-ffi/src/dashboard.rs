//! FFI: app-state persistence — dashboards (list/get/save/delete) plus conversation
//! deletion — normalized over Local (in-process `Storage`) and Remote (`RemoteClient`
//! REST). JSON in, JSON out.
//!
//! Management-style ABI: each fn returns `bool` (success), writes JSON to an
//! out-param (caller frees with `sequins_string_free`), and on failure writes an
//! owned error string to `error_out`.

use std::ffi::{c_char, CStr, CString};

use sequins_metadata::{Dashboard, DashboardApi};

use crate::data_source::{CDataSource, DataSourceImpl};
use crate::runtime::RUNTIME;

/// Resolve the dashboard interface for this data source (Local `Storage` or Remote
/// `RemoteClient` — both implement `DashboardApi`).
fn dashboard_api(impl_ref: &DataSourceImpl) -> &dyn DashboardApi {
    match impl_ref {
        #[cfg(feature = "local")]
        DataSourceImpl::Local { storage, .. } => storage.as_ref(),
        DataSourceImpl::Remote { client } => client.as_ref(),
    }
}

unsafe fn set_error(error_out: *mut *mut c_char, msg: &str) {
    if !error_out.is_null() {
        *error_out = CString::new(msg).unwrap_or_default().into_raw();
    }
}

unsafe fn write_json<T: serde::Serialize>(out: *mut *mut c_char, value: &T) -> bool {
    match serde_json::to_string(value) {
        Ok(s) => {
            if !out.is_null() {
                *out = CString::new(s).unwrap_or_default().into_raw();
            }
            true
        }
        Err(_) => false,
    }
}

/// List all dashboards. On success writes a JSON array to `out_json`.
///
/// # Safety
/// `data_source` must be valid; `out_json`/`error_out` are out-params.
#[no_mangle]
pub unsafe extern "C" fn sequins_dashboard_list(
    data_source: *mut CDataSource,
    out_json: *mut *mut c_char,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() {
        set_error(error_out, "data source is null");
        return false;
    }
    let api = dashboard_api(&*(data_source as *const DataSourceImpl));
    match RUNTIME.block_on(api.list_dashboards()) {
        Ok(list) => write_json(out_json, &list),
        Err(e) => {
            set_error(error_out, &e.to_string());
            false
        }
    }
}

/// Get a dashboard by id. Writes a JSON object (or `null`) to `out_json`.
///
/// # Safety
/// `data_source`/`id` must be valid; `out_json`/`error_out` are out-params.
#[no_mangle]
pub unsafe extern "C" fn sequins_dashboard_get(
    data_source: *mut CDataSource,
    id: *const c_char,
    out_json: *mut *mut c_char,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || id.is_null() {
        set_error(error_out, "data source or id is null");
        return false;
    }
    let id = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(error_out, "id is not valid UTF-8");
            return false;
        }
    };
    let api = dashboard_api(&*(data_source as *const DataSourceImpl));
    match RUNTIME.block_on(api.get_dashboard(id)) {
        Ok(dash) => write_json(out_json, &dash),
        Err(e) => {
            set_error(error_out, &e.to_string());
            false
        }
    }
}

/// Create or update a dashboard from a JSON object. Writes the stored dashboard
/// (with id/timestamps) to `out_json`.
///
/// # Safety
/// `data_source`/`dashboard_json` must be valid; `out_json`/`error_out` are out-params.
#[no_mangle]
pub unsafe extern "C" fn sequins_dashboard_save(
    data_source: *mut CDataSource,
    dashboard_json: *const c_char,
    out_json: *mut *mut c_char,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || dashboard_json.is_null() {
        set_error(error_out, "data source or dashboard is null");
        return false;
    }
    let dashboard: Dashboard = match CStr::from_ptr(dashboard_json)
        .to_str()
        .ok()
        .and_then(|s| serde_json::from_str(s).ok())
    {
        Some(d) => d,
        None => {
            set_error(error_out, "invalid dashboard JSON");
            return false;
        }
    };
    let api = dashboard_api(&*(data_source as *const DataSourceImpl));
    match RUNTIME.block_on(api.save_dashboard(dashboard)) {
        Ok(stored) => write_json(out_json, &stored),
        Err(e) => {
            set_error(error_out, &e.to_string());
            false
        }
    }
}

/// Delete a dashboard by id.
///
/// # Safety
/// `data_source`/`id` must be valid; `error_out` is an out-param.
#[no_mangle]
pub unsafe extern "C" fn sequins_dashboard_delete(
    data_source: *mut CDataSource,
    id: *const c_char,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || id.is_null() {
        set_error(error_out, "data source or id is null");
        return false;
    }
    let id = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(error_out, "id is not valid UTF-8");
            return false;
        }
    };
    let api = dashboard_api(&*(data_source as *const DataSourceImpl));
    match RUNTIME.block_on(api.delete_dashboard(id)) {
        Ok(()) => true,
        Err(e) => {
            set_error(error_out, &e.to_string());
            false
        }
    }
}

/// Delete a persisted conversation by id (in-memory + durable). Local only; remote
/// connections report an error until the daemon exposes conversation deletion.
///
/// # Safety
/// `data_source`/`id` must be valid; `error_out` is an out-param.
#[no_mangle]
pub unsafe extern "C" fn sequins_conversation_delete(
    data_source: *mut CDataSource,
    id: *const c_char,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || id.is_null() {
        set_error(error_out, "data source or id is null");
        return false;
    }
    let id = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error(error_out, "id is not valid UTF-8");
            return false;
        }
    };
    match &*(data_source as *const DataSourceImpl) {
        #[cfg(feature = "local")]
        DataSourceImpl::Local { storage, .. } => {
            match RUNTIME.block_on(storage.app_state().delete_conversation(id)) {
                Ok(()) => true,
                Err(e) => {
                    set_error(error_out, &e.to_string());
                    false
                }
            }
        }
        DataSourceImpl::Remote { .. } => {
            set_error(
                error_out,
                "deleting conversations is not supported on remote connections yet",
            );
            false
        }
    }
}

#[cfg(all(test, feature = "local"))]
mod tests {
    use super::*;
    use crate::data_source::{
        sequins_data_source_free, sequins_data_source_new_local, sequins_string_free,
        COtlpServerConfig,
    };

    unsafe fn take(ptr: *mut c_char) -> String {
        assert!(!ptr.is_null());
        let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        sequins_string_free(ptr);
        s
    }

    #[test]
    fn dashboard_ffi_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let db = CString::new(tmp.path().to_str().unwrap()).unwrap();
        let mut err: *mut c_char = std::ptr::null_mut();
        let ds = sequins_data_source_new_local(
            db.as_ptr(),
            COtlpServerConfig {
                grpc_port: 0,
                http_port: 0,
            },
            &mut err,
        );
        assert!(!ds.is_null());

        // Save.
        let dashboard = serde_json::json!({
            "id": "", "title": "Errors", "created_at_ns": 0, "updated_at_ns": 0,
            "rows": [{ "height": 280.0, "panels": [
                { "visualization": { "seql": "logs last 1h", "title": "logs", "shape": "table" },
                  "weight": 6.0 } ] }]
        });
        let dashboard_c = CString::new(dashboard.to_string()).unwrap();
        let mut out: *mut c_char = std::ptr::null_mut();
        let mut err2: *mut c_char = std::ptr::null_mut();
        let ok = unsafe { sequins_dashboard_save(ds, dashboard_c.as_ptr(), &mut out, &mut err2) };
        assert!(ok);
        let saved: serde_json::Value = serde_json::from_str(&unsafe { take(out) }).unwrap();
        let id = saved["id"].as_str().unwrap().to_string();
        assert!(!id.is_empty());

        // List → 1.
        let mut list_out: *mut c_char = std::ptr::null_mut();
        let ok = unsafe { sequins_dashboard_list(ds, &mut list_out, &mut err2) };
        assert!(ok);
        let list: serde_json::Value = serde_json::from_str(&unsafe { take(list_out) }).unwrap();
        assert_eq!(list.as_array().unwrap().len(), 1);

        // Delete.
        let id_c = CString::new(id).unwrap();
        let ok = unsafe { sequins_dashboard_delete(ds, id_c.as_ptr(), &mut err2) };
        assert!(ok);

        sequins_data_source_free(ds);
    }
}
