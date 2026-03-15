//! Management API FFI bindings
//!
//! Administrative operations for retention policies, storage stats, and maintenance.

#![allow(clippy::not_unsafe_ptr_arg_deref)]

use super::data_source::DataSourceImpl;
use sequins_types::models::{Duration, MaintenanceStats, RetentionPolicy, StorageStats};
use sequins_types::ManagementApi;
use std::os::raw::c_char;

/// C-compatible retention policy configuration
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CRetentionPolicy {
    /// How long to keep span data (seconds)
    pub spans_retention_secs: i64,
    /// How long to keep log data (seconds)
    pub logs_retention_secs: i64,
    /// How long to keep metric data (seconds)
    pub metrics_retention_secs: i64,
    /// How long to keep profile data (seconds)
    pub profiles_retention_secs: i64,
}

impl From<CRetentionPolicy> for RetentionPolicy {
    fn from(c_policy: CRetentionPolicy) -> Self {
        RetentionPolicy {
            spans_retention: Duration::from_secs(c_policy.spans_retention_secs),
            logs_retention: Duration::from_secs(c_policy.logs_retention_secs),
            metrics_retention: Duration::from_secs(c_policy.metrics_retention_secs),
            profiles_retention: Duration::from_secs(c_policy.profiles_retention_secs),
        }
    }
}

impl From<RetentionPolicy> for CRetentionPolicy {
    fn from(policy: RetentionPolicy) -> Self {
        CRetentionPolicy {
            spans_retention_secs: policy.spans_retention.as_secs(),
            logs_retention_secs: policy.logs_retention.as_secs(),
            metrics_retention_secs: policy.metrics_retention.as_secs(),
            profiles_retention_secs: policy.profiles_retention.as_secs(),
        }
    }
}

/// C-compatible storage statistics
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CStorageStats {
    /// Number of spans in storage
    pub span_count: usize,
    /// Number of logs in storage
    pub log_count: usize,
    /// Number of metrics in storage
    pub metric_count: usize,
    /// Number of profiles in storage
    pub profile_count: usize,
}

impl From<StorageStats> for CStorageStats {
    fn from(stats: StorageStats) -> Self {
        CStorageStats {
            span_count: stats.span_count,
            log_count: stats.log_count,
            metric_count: stats.metric_count,
            profile_count: stats.profile_count,
        }
    }
}

/// C-compatible maintenance statistics
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMaintenanceStats {
    /// Number of entries evicted from hot tier
    pub entries_evicted: usize,
    /// Number of batches flushed to cold tier
    pub batches_flushed: usize,
}

impl From<MaintenanceStats> for CMaintenanceStats {
    fn from(stats: MaintenanceStats) -> Self {
        CMaintenanceStats {
            entries_evicted: stats.entries_evicted,
            batches_flushed: stats.batches_flushed,
        }
    }
}

/// Update retention policy for data source
///
/// # Arguments
/// * `data_source` - DataSource to configure
/// * `policy` - New retention policy
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
#[no_mangle]
pub extern "C" fn sequins_management_update_retention_policy(
    data_source: *mut super::data_source::CDataSource,
    policy: CRetentionPolicy,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() {
        super::data_source::set_error(error_out, "data_source cannot be null");
        return false;
    }

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task - management API is only available for local storage
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    let rust_policy = RetentionPolicy::from(policy);

    // Run async operation
    let result = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage.update_retention_policy(rust_policy).await;
        }

        // Remote mode doesn't support management API yet
        Err(sequins_types::Error::Other(
            "Management API not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(()) => true,
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to update retention policy: {}", e),
            );
            false
        }
    }
}

/// Get current retention policy
///
/// # Arguments
/// * `data_source` - DataSource to query
/// * `policy_out` - Output parameter for retention policy
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On success, `policy_out` contains the current policy
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `policy_out` must be a valid pointer
#[no_mangle]
pub extern "C" fn sequins_management_get_retention_policy(
    data_source: *mut super::data_source::CDataSource,
    policy_out: *mut CRetentionPolicy,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || policy_out.is_null() {
        super::data_source::set_error(error_out, "data_source and policy_out cannot be null");
        return false;
    }

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task - management API is only available for local storage
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result: Result<RetentionPolicy, _> = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage.get_retention_policy().await;
        }

        // Remote mode doesn't support management API yet
        Err(sequins_types::Error::Other(
            "Management API not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(policy) => {
            unsafe {
                *policy_out = CRetentionPolicy::from(policy);
            }
            true
        }
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to get retention policy: {}", e),
            );
            false
        }
    }
}

/// Get storage statistics
///
/// # Arguments
/// * `data_source` - DataSource to query
/// * `stats_out` - Output parameter for storage stats
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On success, `stats_out` contains current statistics
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `stats_out` must be a valid pointer
#[no_mangle]
pub extern "C" fn sequins_management_get_storage_stats(
    data_source: *mut super::data_source::CDataSource,
    stats_out: *mut CStorageStats,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || stats_out.is_null() {
        super::data_source::set_error(error_out, "data_source and stats_out cannot be null");
        return false;
    }

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task - management API is only available for local storage
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result: Result<StorageStats, _> = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage.get_storage_stats().await;
        }

        // Remote mode doesn't support management API yet
        Err(sequins_types::Error::Other(
            "Management API not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(stats) => {
            unsafe {
                *stats_out = CStorageStats::from(stats);
            }
            true
        }
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to get storage stats: {}", e),
            );
            false
        }
    }
}

/// Run retention cleanup to delete old data
///
/// # Arguments
/// * `data_source` - DataSource to clean up
/// * `deleted_count_out` - Output parameter for number of deleted entries
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On success, `deleted_count_out` contains number of entries deleted
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `deleted_count_out` must be a valid pointer
#[no_mangle]
pub extern "C" fn sequins_management_run_retention_cleanup(
    data_source: *mut super::data_source::CDataSource,
    deleted_count_out: *mut usize,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || deleted_count_out.is_null() {
        super::data_source::set_error(
            error_out,
            "data_source and deleted_count_out cannot be null",
        );
        return false;
    }

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task - management API is only available for local storage
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result: Result<usize, _> = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage.run_retention_cleanup().await;
        }

        // Remote mode doesn't support management API yet
        Err(sequins_types::Error::Other(
            "Management API not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(count) => {
            unsafe {
                *deleted_count_out = count;
            }
            true
        }
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to run retention cleanup: {}", e),
            );
            false
        }
    }
}

/// Run database maintenance (compaction and optimization)
///
/// # Arguments
/// * `data_source` - DataSource to maintain
/// * `stats_out` - Output parameter for maintenance statistics
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On success, `stats_out` contains maintenance statistics
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `stats_out` must be a valid pointer
#[no_mangle]
pub extern "C" fn sequins_management_run_maintenance(
    data_source: *mut super::data_source::CDataSource,
    stats_out: *mut CMaintenanceStats,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || stats_out.is_null() {
        super::data_source::set_error(error_out, "data_source and stats_out cannot be null");
        return false;
    }

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task - management API is only available for local storage
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result: Result<MaintenanceStats, _> = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage.run_maintenance().await;
        }

        // Remote mode doesn't support management API yet
        Err(sequins_types::Error::Other(
            "Management API not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(stats) => {
            unsafe {
                *stats_out = CMaintenanceStats::from(stats);
            }
            true
        }
        Err(e) => {
            super::data_source::set_error(error_out, &format!("Failed to run maintenance: {}", e));
            false
        }
    }
}

// =============================================================================
// Health Threshold Config API
// =============================================================================

/// Get current health threshold configuration
///
/// # Arguments
/// * `data_source` - DataSource to query
/// * `config_out` - Output parameter for health config
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On success, `config_out` contains the current config
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `config_out` must be a valid pointer
/// * Caller must free the returned config with `sequins_health_threshold_config_free`
#[no_mangle]
pub extern "C" fn sequins_management_get_health_threshold_config(
    data_source: *mut super::data_source::CDataSource,
    config_out: *mut super::types::CHealthThresholdConfig,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || config_out.is_null() {
        super::data_source::set_error(error_out, "data_source and config_out cannot be null");
        return false;
    }

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task - health config is only available for local storage
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result: Result<sequins_types::health::HealthThresholdConfig, _> = super::runtime::RUNTIME
        .block_on(async {
            #[cfg(feature = "local")]
            if let Some(storage) = storage_arc {
                return storage.get_health_threshold_config().await;
            }

            // Remote mode doesn't support health config yet
            Err(sequins_storage::Error::Other(
                "Health config not supported in remote mode".to_string(),
            ))
        });

    match result {
        Ok(config) => {
            unsafe {
                *config_out = super::types::CHealthThresholdConfig::from(config);
            }
            true
        }
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to get health threshold config: {}", e),
            );
            false
        }
    }
}

/// Set health threshold configuration
///
/// # Arguments
/// * `data_source` - DataSource to configure
/// * `config` - New health threshold config
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `config` must be a valid pointer to a CHealthThresholdConfig
#[no_mangle]
pub extern "C" fn sequins_management_set_health_threshold_config(
    data_source: *mut super::data_source::CDataSource,
    config: *const super::types::CHealthThresholdConfig,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || config.is_null() {
        super::data_source::set_error(error_out, "data_source and config cannot be null");
        return false;
    }

    // Convert from C to Rust
    let rust_config =
        match sequins_types::health::HealthThresholdConfig::try_from(unsafe { &*config }) {
            Ok(c) => c,
            Err(e) => {
                super::data_source::set_error(
                    error_out,
                    &format!("Failed to convert health config: {:?}", e),
                );
                return false;
            }
        };

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage.set_health_threshold_config(rust_config).await;
        }

        Err(sequins_storage::Error::Other(
            "Health config not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(()) => true,
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to set health threshold config: {}", e),
            );
            false
        }
    }
}

/// Add a health metric rule
///
/// If a rule with the same metric_name and service_name already exists, it is replaced.
///
/// # Arguments
/// * `data_source` - DataSource to configure
/// * `rule` - Health metric rule to add
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `rule` must be a valid pointer to a CHealthMetricRule
#[no_mangle]
pub extern "C" fn sequins_management_add_health_rule(
    data_source: *mut super::data_source::CDataSource,
    rule: *const super::types::CHealthMetricRule,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || rule.is_null() {
        super::data_source::set_error(error_out, "data_source and rule cannot be null");
        return false;
    }

    // Convert from C to Rust
    let rust_rule = match sequins_types::health::HealthMetricRule::try_from(unsafe { &*rule }) {
        Ok(r) => r,
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to convert health rule: {:?}", e),
            );
            return false;
        }
    };

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage.add_health_rule(rust_rule).await;
        }

        Err(sequins_storage::Error::Other(
            "Health config not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(()) => true,
        Err(e) => {
            super::data_source::set_error(error_out, &format!("Failed to add health rule: {}", e));
            false
        }
    }
}

/// Remove a health metric rule
///
/// # Arguments
/// * `data_source` - DataSource to configure
/// * `metric_name` - Metric name of the rule to remove
/// * `service_name` - Service name filter (null = all services)
/// * `error_out` - Output parameter for error message (if any)
///
/// # Returns
/// * true on success, false on error
/// * On error, `error_out` contains error message (caller must free with `sequins_string_free`)
///
/// # Safety
/// * `data_source` must be a valid DataSource pointer
/// * `metric_name` must be a valid C string
/// * `service_name` can be null (means all services)
#[no_mangle]
pub extern "C" fn sequins_management_remove_health_rule(
    data_source: *mut super::data_source::CDataSource,
    metric_name: *const c_char,
    service_name: *const c_char,
    error_out: *mut *mut c_char,
) -> bool {
    if data_source.is_null() || metric_name.is_null() {
        super::data_source::set_error(error_out, "data_source and metric_name cannot be null");
        return false;
    }

    // Convert from C to Rust
    let metric_name_str = unsafe {
        std::ffi::CStr::from_ptr(metric_name)
            .to_string_lossy()
            .into_owned()
    };

    let service_name_opt = if service_name.is_null() {
        None
    } else {
        Some(unsafe {
            std::ffi::CStr::from_ptr(service_name)
                .to_string_lossy()
                .into_owned()
        })
    };

    // Get DataSource implementation
    let impl_ref = unsafe { &*(data_source as *const DataSourceImpl) };

    // Clone Arc for async task
    #[cfg(feature = "local")]
    let storage_arc = if let DataSourceImpl::Local { storage, .. } = impl_ref {
        Some(std::sync::Arc::clone(storage))
    } else {
        None
    };

    #[cfg(not(feature = "local"))]
    let storage_arc: Option<std::sync::Arc<()>> = None;

    // Run async operation
    let result = super::runtime::RUNTIME.block_on(async {
        #[cfg(feature = "local")]
        if let Some(storage) = storage_arc {
            return storage
                .remove_health_rule(&metric_name_str, service_name_opt.as_deref())
                .await;
        }

        Err(sequins_storage::Error::Other(
            "Health config not supported in remote mode".to_string(),
        ))
    });

    match result {
        Ok(()) => true,
        Err(e) => {
            super::data_source::set_error(
                error_out,
                &format!("Failed to remove health rule: {}", e),
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_policy_conversion() {
        let c_policy = CRetentionPolicy {
            spans_retention_secs: 86400,     // 1 day
            logs_retention_secs: 604800,     // 7 days
            metrics_retention_secs: 2592000, // 30 days
            profiles_retention_secs: 172800, // 2 days
        };

        let rust_policy = RetentionPolicy::from(c_policy);
        assert_eq!(rust_policy.spans_retention.as_secs(), 86400);
        assert_eq!(rust_policy.logs_retention.as_secs(), 604800);
        assert_eq!(rust_policy.metrics_retention.as_secs(), 2592000);
        assert_eq!(rust_policy.profiles_retention.as_secs(), 172800);

        let c_policy_back = CRetentionPolicy::from(rust_policy);
        assert_eq!(c_policy_back.spans_retention_secs, 86400);
        assert_eq!(c_policy_back.logs_retention_secs, 604800);
        assert_eq!(c_policy_back.metrics_retention_secs, 2592000);
        assert_eq!(c_policy_back.profiles_retention_secs, 172800);
    }

    #[test]
    fn test_storage_stats_conversion() {
        let stats = StorageStats {
            span_count: 100,
            log_count: 200,
            metric_count: 300,
            profile_count: 50,
        };

        let c_stats = CStorageStats::from(stats);
        assert_eq!(c_stats.span_count, 100);
        assert_eq!(c_stats.log_count, 200);
        assert_eq!(c_stats.metric_count, 300);
        assert_eq!(c_stats.profile_count, 50);
    }

    #[test]
    fn test_maintenance_stats_conversion() {
        let stats = MaintenanceStats {
            entries_evicted: 42,
            batches_flushed: 7,
        };

        let c_stats = CMaintenanceStats::from(stats);
        assert_eq!(c_stats.entries_evicted, 42);
        assert_eq!(c_stats.batches_flushed, 7);
    }
}
