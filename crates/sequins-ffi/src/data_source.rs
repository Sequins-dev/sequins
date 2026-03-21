//! DataSource FFI bindings
//!
//! C FFI functions are not marked `unsafe` because the entire FFI boundary
//! is inherently unsafe from Rust's perspective. Callers (C/Swift) are
//! responsible for passing valid pointers.

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use sequins_client::RemoteClient;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::{Arc, Mutex};

#[cfg(feature = "local")]
use sequins_server::OtlpServer;
#[cfg(feature = "local")]
use sequins_storage::Storage;

/// Opaque C type for DataSource - never actually defined
/// This provides type safety without exposing internals
pub enum CDataSource {}

/// OTLP server handle for background task management
#[cfg(feature = "local")]
pub(crate) struct OtlpServerHandle {
    server_task: tokio::task::JoinHandle<()>,
    health_task: tokio::task::JoinHandle<()>,
    flush_task: tokio::task::JoinHandle<()>,
    grpc_port: u16,
    http_port: u16,
}

#[cfg(feature = "local")]
impl Drop for OtlpServerHandle {
    fn drop(&mut self) {
        // Abort all background tasks so they release their Arc<Storage> references
        // and free the bound ports immediately.  Dropping a JoinHandle only detaches
        // (the task keeps running), so we must abort explicitly.
        self.server_task.abort();
        self.flush_task.abort();
        self.health_task.abort();
    }
}

/// Internal Rust implementation of DataSource
/// This enum handles both local and remote modes transparently
#[allow(dead_code)] // client field will be used when query methods are implemented
pub(crate) enum DataSourceImpl {
    #[cfg(feature = "local")]
    Local {
        storage: Arc<Storage>,
        otlp_server: Mutex<Option<OtlpServerHandle>>,
    },
    Remote {
        client: Arc<RemoteClient>,
    },
}

// TODO: Re-enable when Storage API is updated
// Health metrics generation is currently disabled
// #[cfg(feature = "local")]
// async fn generate_health_for_services(...) { ... }

/// OTLP server configuration for local mode
#[repr(C)]
#[derive(Copy, Clone)]
pub struct COtlpServerConfig {
    /// gRPC port (0 = disabled, default 4317)
    pub grpc_port: u16,
    /// HTTP port (0 = disabled, default 4318)
    pub http_port: u16,
}

/// Create a new local data source with embedded storage
///
/// # Arguments
/// * `db_path` - Path to SQLite database file
/// * `config` - OTLP server configuration (ports to bind)
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * Pointer to CDataSource on success, null on failure
/// * On failure, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `db_path` must be a valid null-terminated C string
/// * Caller must call `sequins_data_source_free` when done
#[cfg(feature = "local")]
#[no_mangle]
#[tracing::instrument(skip_all, name = "ds_new_local")]
pub extern "C" fn sequins_data_source_new_local(
    db_path: *const c_char,
    _config: COtlpServerConfig,
    error_out: *mut *mut c_char,
) -> *mut CDataSource {
    crate::logging::init();

    if db_path.is_null() {
        set_error(error_out, "db_path cannot be null");
        return std::ptr::null_mut();
    }

    let path_str = unsafe {
        match CStr::from_ptr(db_path).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_error(error_out, &format!("Invalid UTF-8 in db_path: {}", e));
                return std::ptr::null_mut();
            }
        }
    };

    // Create Storage configuration
    let mut config = sequins_storage::config::StorageConfig::default();
    config.cold_tier.uri = format!("file://{}", path_str);

    let storage = match crate::runtime::RUNTIME.block_on(Storage::new(config)) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            set_error(error_out, &format!("Failed to create Storage: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Create DataSourceImpl (OTLP server will be started separately via start_otlp_server)
    let impl_box = Box::new(DataSourceImpl::Local {
        storage,
        otlp_server: Mutex::new(None),
    });

    Box::into_raw(impl_box) as *mut CDataSource
}

/// Create a new remote data source (connects to sequins-daemon)
///
/// # Arguments
/// * `query_url` - URL for query API endpoint
/// * `management_url` - URL for management API endpoint
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * Pointer to CDataSource on success, null on failure
/// * On failure, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `query_url` and `management_url` must be valid null-terminated C strings
/// * Caller must call `sequins_data_source_free` when done
#[no_mangle]
pub extern "C" fn sequins_data_source_new_remote(
    query_url: *const c_char,
    management_url: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut CDataSource {
    if query_url.is_null() {
        set_error(error_out, "query_url cannot be null");
        return std::ptr::null_mut();
    }

    if management_url.is_null() {
        set_error(error_out, "management_url cannot be null");
        return std::ptr::null_mut();
    }

    let query_str = unsafe {
        match CStr::from_ptr(query_url).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_error(error_out, &format!("Invalid UTF-8 in query_url: {}", e));
                return std::ptr::null_mut();
            }
        }
    };

    let _management_str = unsafe {
        match CStr::from_ptr(management_url).to_str() {
            Ok(s) => s,
            Err(e) => {
                set_error(
                    error_out,
                    &format!("Invalid UTF-8 in management_url: {}", e),
                );
                return std::ptr::null_mut();
            }
        }
    };

    // Create RemoteClient — synchronous (lazy channel, no actual TCP connection yet)
    let client = match RemoteClient::new(query_str) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            set_error(error_out, &format!("Failed to create RemoteClient: {}", e));
            return std::ptr::null_mut();
        }
    };

    let impl_box = Box::new(DataSourceImpl::Remote { client });
    Box::into_raw(impl_box) as *mut CDataSource
}

/// Free a DataSource and all associated resources
///
/// # Safety
/// * `data_source` must be a valid pointer returned from `sequins_data_source_new_*`
/// * Must only be called once per DataSource
/// * For local mode, OTLP server will be stopped if running
#[no_mangle]
pub extern "C" fn sequins_data_source_free(data_source: *mut CDataSource) {
    if data_source.is_null() {
        return;
    }

    unsafe {
        let impl_box = Box::from_raw(data_source as *mut DataSourceImpl);
        drop(impl_box);
    }
}

/// Start OTLP server for local data source
///
/// # Arguments
/// * `data_source` - Local data source
/// * `config` - OTLP server configuration
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on failure
/// * On failure, `error_out` contains error message
///
/// # Safety
/// * `data_source` must be a valid local DataSource
/// * Only works with local DataSource (returns error for remote)
#[cfg(feature = "local")]
#[no_mangle]
#[tracing::instrument(skip_all, name = "ds_start_otlp")]
pub extern "C" fn sequins_data_source_start_otlp_server(
    data_source: *mut CDataSource,
    config: COtlpServerConfig,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() {
        set_error(error_out, "data_source cannot be null");
        return false;
    }

    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    match impl_ref {
        DataSourceImpl::Local {
            storage,
            otlp_server,
        } => {
            let mut server_guard = match otlp_server.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    set_error(error_out, &format!("Failed to lock OTLP server: {}", e));
                    return false;
                }
            };

            if server_guard.is_some() {
                set_error(error_out, "OTLP server already running");
                return false;
            }

            // Determine ports to use
            let grpc_port = if config.grpc_port == 0 {
                4317 // Default gRPC port
            } else {
                config.grpc_port
            };

            let http_port = if config.http_port == 0 {
                4318 // Default HTTP port
            } else {
                config.http_port
            };

            // Create OTLP server
            let server = OtlpServer::new(storage.clone());

            let grpc_addr = format!("0.0.0.0:{}", grpc_port);
            let http_addr = format!("0.0.0.0:{}", http_port);

            // Use a stdlib sync channel so we can block on startup confirmation without
            // requiring a second tokio `block_on` call (which can be unreliable when
            // called from a foreign thread pool such as Swift's cooperative executor).
            let (ready_tx, ready_rx) =
                std::sync::mpsc::channel::<std::result::Result<(), String>>();

            let grpc_addr_task = grpc_addr.clone();
            let http_addr_task = http_addr.clone();
            let server_task = super::runtime::RUNTIME.spawn(async move {
                if let Err(e) = server
                    .serve_with_ready(&grpc_addr_task, &http_addr_task, ready_tx)
                    .await
                {
                    tracing::error!(error = %e, "OTLP server error");
                }
            });

            // Block the calling thread until the HTTP listener is bound (or fails).
            // 5-second safety timeout in case the task is unexpectedly slow to start.
            match ready_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                Ok(Ok(())) => {
                    // HTTP listener is bound — server is up.
                }
                Ok(Err(bind_err)) => {
                    set_error(
                        error_out,
                        &format!("OTLP server failed to start: {}", bind_err),
                    );
                    server_task.abort();
                    return false;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    set_error(error_out, "OTLP server task exited before signalling ready");
                    server_task.abort();
                    return false;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    set_error(error_out, "Timed out waiting for OTLP server to start");
                    server_task.abort();
                    return false;
                }
            }

            // Start background flush task to persist data from hot tier to cold tier
            // We spawn this on the FFI runtime instead of using Storage::start_background_flush
            // which would call tokio::spawn() without a runtime context
            let flush_task = {
                let storage_clone = storage.clone();
                super::runtime::RUNTIME.spawn(async move {
                    let flush_interval = storage_clone.config().lifecycle.flush_interval;
                    let shutdown_notify = storage_clone.shutdown_notify();

                    // Create interval timer from nanoseconds
                    let interval_nanos = flush_interval.as_nanos();
                    let interval_duration = if interval_nanos > 0 {
                        std::time::Duration::from_nanos(interval_nanos as u64)
                    } else {
                        // Fallback to 1 second if somehow we get zero
                        std::time::Duration::from_secs(1)
                    };
                    let mut interval = tokio::time::interval(interval_duration);

                    // Skip the first tick (fires immediately)
                    interval.tick().await;

                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                // Run periodic flush
                                if let Err(e) = storage_clone.run_maintenance_internal().await {
                                    tracing::warn!(error = %e, "Background flush error");
                                }
                            }
                            _ = shutdown_notify.notified() => {
                                // Graceful shutdown: run one final flush
                                if let Err(e) = storage_clone.run_maintenance_internal().await {
                                    tracing::warn!(error = %e, "Final flush error during shutdown");
                                }
                                break;
                            }
                        }
                    }
                })
            };
            tracing::info!("Started background flush task");

            // TODO: Re-enable health metrics generation when Storage API is updated
            // Placeholder task for now
            let health_task = super::runtime::RUNTIME.spawn(async move {
                // Health monitoring disabled - waiting for Storage API update
                tracing::info!("Health metrics generation is currently disabled");
            });

            *server_guard = Some(OtlpServerHandle {
                server_task,
                health_task,
                flush_task,
                grpc_port,
                http_port,
            });

            true
        }
        DataSourceImpl::Remote { .. } => {
            set_error(error_out, "Cannot start OTLP server on remote data source");
            false
        }
    }
}

/// Stop OTLP server for local data source
///
/// # Arguments
/// * `data_source` - Local data source
///
/// # Safety
/// * `data_source` must be a valid local DataSource
/// * Idempotent - safe to call even if server not running
#[cfg(feature = "local")]
#[no_mangle]
pub extern "C" fn sequins_data_source_stop_otlp_server(data_source: *mut CDataSource) {
    if data_source.is_null() {
        return;
    }

    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    if let DataSourceImpl::Local { otlp_server, .. } = impl_ref {
        if let Ok(mut server_guard) = otlp_server.lock() {
            *server_guard = None; // Drop the server, stopping it
        }
    }
}

/// Get OTLP server ports for local data source
///
/// # Arguments
/// * `data_source` - Local data source
/// * `grpc_port_out` - Output parameter for gRPC port (0 if disabled)
/// * `http_port_out` - Output parameter for HTTP port (0 if disabled)
///
/// # Returns
/// * true if server is running, false otherwise
///
/// # Safety
/// * `data_source` must be a valid local DataSource
/// * `grpc_port_out` and `http_port_out` must be valid pointers
#[cfg(feature = "local")]
#[no_mangle]
pub extern "C" fn sequins_data_source_get_otlp_ports(
    data_source: *mut CDataSource,
    grpc_port_out: *mut u16,
    http_port_out: *mut u16,
) -> bool {
    if data_source.is_null() || grpc_port_out.is_null() || http_port_out.is_null() {
        return false;
    }

    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    if let DataSourceImpl::Local { otlp_server, .. } = impl_ref {
        if let Ok(server_guard) = otlp_server.lock() {
            if let Some(handle) = &*server_guard {
                unsafe {
                    *grpc_port_out = handle.grpc_port;
                    *http_port_out = handle.http_port;
                }
                return true;
            }
        }
    }

    unsafe {
        *grpc_port_out = 0;
        *http_port_out = 0;
    }
    false
}

/// Check whether the OTLP server task is still alive
///
/// Returns `true` when the server has been started and its background task has
/// not yet exited.  Returns `false` if the server was never started, or if the
/// task has exited (e.g., because a port was stolen after startup).
///
/// # Safety
/// * `data_source` must be a valid local DataSource pointer (or null)
#[cfg(feature = "local")]
#[no_mangle]
pub extern "C" fn sequins_data_source_is_server_alive(data_source: *mut CDataSource) -> bool {
    if data_source.is_null() {
        return false;
    }

    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    if let DataSourceImpl::Local { otlp_server, .. } = impl_ref {
        if let Ok(server_guard) = otlp_server.lock() {
            if let Some(handle) = &*server_guard {
                return !handle.server_task.is_finished();
            }
        }
    }

    false
}

/// Helper function to set error message
pub(crate) fn set_error(error_out: *mut *mut c_char, message: &str) {
    if error_out.is_null() {
        return;
    }

    if let Ok(c_string) = CString::new(message) {
        unsafe {
            *error_out = c_string.into_raw();
        }
    }
}

/// Free a string allocated by Rust
///
/// # Safety
/// * `s` must be a valid pointer returned from a Rust FFI function
/// * Must only be called once per string
#[no_mangle]
pub extern "C" fn sequins_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }

    unsafe {
        let _ = CString::from_raw(s);
    }
}

/// Generate test data for local data source
///
/// # Arguments
/// * `data_source` - Local data source
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * Number of spans created on success, 0 on failure
/// * On failure, `error_out` contains error message
///
/// # Safety
/// * `data_source` must be a valid local DataSource
/// * Only works with local DataSource (returns error for remote)
#[no_mangle]
pub extern "C" fn sequins_data_source_generate_test_data(
    data_source: *mut CDataSource,
    error_out: *mut *mut c_char,
) -> usize {
    if data_source.is_null() {
        set_error(error_out, "data_source cannot be null");
        return 0;
    }

    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    match impl_ref {
        #[cfg(feature = "local")]
        DataSourceImpl::Local { storage, .. } => match storage.generate_test_data() {
            Ok(count) => count,
            Err(e) => {
                set_error(error_out, &format!("Failed to generate test data: {}", e));
                0
            }
        },
        DataSourceImpl::Remote { .. } => {
            set_error(error_out, "Cannot generate test data on remote data source");
            0
        }
        #[cfg(not(feature = "local"))]
        _ => {
            set_error(
                error_out,
                "Test data generation not available in this build",
            );
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    #[cfg(feature = "local")]
    fn test_create_local_data_source() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let path_c = CString::new(db_path.to_str().unwrap()).unwrap();

        let config = COtlpServerConfig {
            grpc_port: 0, // Will use default 4317
            http_port: 0, // Will use default 4318
        };

        let mut error: *mut c_char = std::ptr::null_mut();
        let data_source =
            sequins_data_source_new_local(path_c.as_ptr(), config, &mut error as *mut _);

        assert!(!data_source.is_null(), "Failed to create data source");
        assert!(error.is_null(), "Unexpected error");

        sequins_data_source_free(data_source);
    }

    #[test]
    fn test_create_remote_data_source() {
        let query_url = CString::new("http://localhost:8080").unwrap();
        let management_url = CString::new("http://localhost:8081").unwrap();

        let mut error: *mut c_char = std::ptr::null_mut();
        let data_source = sequins_data_source_new_remote(
            query_url.as_ptr(),
            management_url.as_ptr(),
            &mut error as *mut _,
        );

        assert!(!data_source.is_null(), "Failed to create data source");
        assert!(error.is_null(), "Unexpected error");

        sequins_data_source_free(data_source);
    }

    #[test]
    fn test_null_db_path() {
        let config = COtlpServerConfig {
            grpc_port: 4317,
            http_port: 4318,
        };

        let mut error: *mut c_char = std::ptr::null_mut();

        #[cfg(feature = "local")]
        {
            let data_source =
                sequins_data_source_new_local(std::ptr::null(), config, &mut error as *mut _);

            assert!(data_source.is_null(), "Should fail with null db_path");
            assert!(!error.is_null(), "Should have error message");

            if !error.is_null() {
                unsafe {
                    let err_str = CStr::from_ptr(error).to_string_lossy();
                    assert!(err_str.contains("cannot be null"));
                    sequins_string_free(error);
                }
            }
        }
    }

    // ── DataSource Query Integration Tests ────────────────────────────────────────

    #[test]
    #[cfg(feature = "local")]
    fn test_data_source_query_integration() {
        use crate::seql::{sequins_seql_query, sequins_seql_stream_free, CFrameSinkVTable};
        use crate::types::frames::{
            c_data_frame_free, c_query_error_free, c_schema_frame_free, CCompleteFrame, CDataFrame,
            CQueryError, CSchemaFrame,
        };
        use std::sync::{Arc, Mutex};

        // Create test data source
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
        assert!(error.is_null(), "Unexpected error");

        // Generate test data
        let count = sequins_data_source_generate_test_data(data_source, &mut error as *mut _);
        assert!(count > 0, "Failed to generate test data");
        assert!(error.is_null(), "Unexpected error generating test data");

        // Execute query through DataSource
        #[derive(Default)]
        struct QueryResult {
            schema_received: bool,
            data_received: bool,
            complete_received: bool,
            error_received: bool,
        }

        let result = Arc::new(Mutex::new(QueryResult::default()));
        let result_clone = result.clone();

        extern "C" fn on_schema(_frame: *const CSchemaFrame, ctx: *mut std::ffi::c_void) {
            let res = unsafe { &*(ctx as *const Arc<Mutex<QueryResult>>) };
            res.lock().unwrap().schema_received = true;
            unsafe { c_schema_frame_free(_frame as *mut _) };
        }

        extern "C" fn on_data(_frame: *const CDataFrame, ctx: *mut std::ffi::c_void) {
            let res = unsafe { &*(ctx as *const Arc<Mutex<QueryResult>>) };
            res.lock().unwrap().data_received = true;
            unsafe { c_data_frame_free(_frame as *mut _) };
        }

        extern "C" fn on_complete(_frame: *const CCompleteFrame, ctx: *mut std::ffi::c_void) {
            let res = unsafe { &*(ctx as *const Arc<Mutex<QueryResult>>) };
            res.lock().unwrap().complete_received = true;
        }

        extern "C" fn on_error(_error: *const CQueryError, ctx: *mut std::ffi::c_void) {
            let res = unsafe { &*(ctx as *const Arc<Mutex<QueryResult>>) };
            res.lock().unwrap().error_received = true;
            unsafe { c_query_error_free(_error as *mut _) };
        }

        let ctx = &result_clone as *const _ as *mut std::ffi::c_void;

        let vtable = CFrameSinkVTable {
            on_schema: Some(on_schema),
            on_data: Some(on_data),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete),
            on_warning: None,
            on_error: Some(on_error),
        };

        let query = CString::new("spans last 1h | take 5").unwrap();
        let stream_handle = unsafe { sequins_seql_query(data_source, query.as_ptr(), vtable, ctx) };

        assert!(!stream_handle.is_null(), "Stream handle should not be null");

        // Wait for query to complete
        std::thread::sleep(std::time::Duration::from_millis(500));

        // Verify query executed successfully
        let final_result = result.lock().unwrap();
        assert!(final_result.schema_received, "Should have received schema");
        assert!(
            final_result.complete_received,
            "Should have received complete"
        );
        assert!(
            !final_result.error_received,
            "Should not have received error"
        );

        // Cleanup
        unsafe {
            sequins_seql_stream_free(stream_handle);
            sequins_data_source_free(data_source);
        }
    }

    #[test]
    #[cfg(feature = "local")]
    fn test_data_source_concurrent_queries() {
        use crate::seql::{sequins_seql_query, sequins_seql_stream_free, CFrameSinkVTable};
        use crate::types::frames::{
            c_data_frame_free, c_query_error_free, c_schema_frame_free, CCompleteFrame, CDataFrame,
            CQueryError, CSchemaFrame,
        };
        use std::sync::{Arc, Mutex};

        // Create test data source
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
        assert!(error.is_null(), "Unexpected error");

        // Generate test data
        let count = sequins_data_source_generate_test_data(data_source, &mut error as *mut _);
        assert!(count > 0, "Failed to generate test data");

        // Execute multiple concurrent queries
        #[derive(Default)]
        struct QueryResult {
            complete_count: usize,
        }

        let result1 = Arc::new(Mutex::new(QueryResult::default()));
        let result2 = Arc::new(Mutex::new(QueryResult::default()));

        extern "C" fn on_schema(_frame: *const CSchemaFrame, _ctx: *mut std::ffi::c_void) {
            unsafe { c_schema_frame_free(_frame as *mut _) };
        }

        extern "C" fn on_data(_frame: *const CDataFrame, _ctx: *mut std::ffi::c_void) {
            unsafe { c_data_frame_free(_frame as *mut _) };
        }

        extern "C" fn on_complete(_frame: *const CCompleteFrame, ctx: *mut std::ffi::c_void) {
            let res = unsafe { &*(ctx as *const Arc<Mutex<QueryResult>>) };
            res.lock().unwrap().complete_count += 1;
        }

        extern "C" fn on_error(_error: *const CQueryError, _ctx: *mut std::ffi::c_void) {
            unsafe { c_query_error_free(_error as *mut _) };
        }

        // Start first query
        let ctx1 = &result1 as *const _ as *mut std::ffi::c_void;
        let query1 = CString::new("spans last 1h | take 5").unwrap();
        let vtable1 = CFrameSinkVTable {
            on_schema: Some(on_schema),
            on_data: Some(on_data),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete),
            on_warning: None,
            on_error: Some(on_error),
        };
        let handle1 = unsafe { sequins_seql_query(data_source, query1.as_ptr(), vtable1, ctx1) };

        // Start second query concurrently
        let ctx2 = &result2 as *const _ as *mut std::ffi::c_void;
        let query2 = CString::new("spans last 1h | select trace_id | take 3").unwrap();
        let vtable2 = CFrameSinkVTable {
            on_schema: Some(on_schema),
            on_data: Some(on_data),
            on_delta: None,
            on_heartbeat: None,
            on_complete: Some(on_complete),
            on_warning: None,
            on_error: Some(on_error),
        };
        let handle2 = unsafe { sequins_seql_query(data_source, query2.as_ptr(), vtable2, ctx2) };

        assert!(!handle1.is_null(), "First stream handle should not be null");
        assert!(
            !handle2.is_null(),
            "Second stream handle should not be null"
        );

        // Wait for both queries to complete
        std::thread::sleep(std::time::Duration::from_millis(1000));

        // Verify both queries completed
        assert_eq!(
            result1.lock().unwrap().complete_count,
            1,
            "First query should have completed"
        );
        assert_eq!(
            result2.lock().unwrap().complete_count,
            1,
            "Second query should have completed"
        );

        // Cleanup
        unsafe {
            sequins_seql_stream_free(handle1);
            sequins_seql_stream_free(handle2);
            sequins_data_source_free(data_source);
        }
    }
}
