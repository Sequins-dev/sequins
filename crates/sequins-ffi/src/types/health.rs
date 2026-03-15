//! FFI types for health configuration
//!
//! C-compatible types for passing health threshold configuration across the FFI boundary.

use sequins_types::health::{HealthMetricRule, HealthThresholdConfig};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

/// C-compatible health metric rule
#[repr(C)]
#[derive(Clone)]
pub struct CHealthMetricRule {
    /// Metric name (e.g., "sequins.health.span_error_rate")
    pub metric_name: *mut c_char,
    /// Service name filter (null = applies to all services)
    pub service_name: *mut c_char,
    /// Warning threshold
    pub warning_threshold: f64,
    /// Error threshold
    pub error_threshold: f64,
    /// Whether higher values are worse (true) or lower values are worse (false)
    pub higher_is_worse: bool,
    /// Weight for overall health score (0.0-1.0)
    pub weight: f64,
    /// Human-readable display name
    pub display_name: *mut c_char,
}

/// C-compatible array of health metric rules
#[repr(C)]
#[derive(Clone)]
pub struct CHealthMetricRuleArray {
    pub data: *mut CHealthMetricRule,
    pub len: usize,
}

/// C-compatible health threshold configuration
#[repr(C)]
#[derive(Clone)]
pub struct CHealthThresholdConfig {
    pub rules: CHealthMetricRuleArray,
}

// Conversion from Rust HealthMetricRule to C
impl From<HealthMetricRule> for CHealthMetricRule {
    fn from(rule: HealthMetricRule) -> Self {
        let metric_name = CString::new(rule.metric_name).unwrap().into_raw();
        let service_name = match rule.service_name {
            Some(s) => CString::new(s).unwrap().into_raw(),
            None => ptr::null_mut(),
        };
        let display_name = CString::new(rule.display_name).unwrap().into_raw();

        CHealthMetricRule {
            metric_name,
            service_name,
            warning_threshold: rule.warning_threshold,
            error_threshold: rule.error_threshold,
            higher_is_worse: rule.higher_is_worse,
            weight: rule.weight,
            display_name,
        }
    }
}

// Conversion from C to Rust HealthMetricRule
impl TryFrom<&CHealthMetricRule> for HealthMetricRule {
    type Error = std::ffi::NulError;

    fn try_from(c_rule: &CHealthMetricRule) -> Result<Self, Self::Error> {
        unsafe {
            let metric_name = if c_rule.metric_name.is_null() {
                String::new()
            } else {
                CStr::from_ptr(c_rule.metric_name)
                    .to_string_lossy()
                    .into_owned()
            };

            let service_name = if c_rule.service_name.is_null() {
                None
            } else {
                Some(
                    CStr::from_ptr(c_rule.service_name)
                        .to_string_lossy()
                        .into_owned(),
                )
            };

            let display_name = if c_rule.display_name.is_null() {
                String::new()
            } else {
                CStr::from_ptr(c_rule.display_name)
                    .to_string_lossy()
                    .into_owned()
            };

            Ok(HealthMetricRule {
                metric_name,
                service_name,
                warning_threshold: c_rule.warning_threshold,
                error_threshold: c_rule.error_threshold,
                higher_is_worse: c_rule.higher_is_worse,
                weight: c_rule.weight,
                display_name,
            })
        }
    }
}

// Conversion from Rust HealthThresholdConfig to C
impl From<HealthThresholdConfig> for CHealthThresholdConfig {
    fn from(config: HealthThresholdConfig) -> Self {
        let len = config.rules.len();
        let mut c_rules: Vec<CHealthMetricRule> =
            config.rules.into_iter().map(|r| r.into()).collect();
        let data = c_rules.as_mut_ptr();
        std::mem::forget(c_rules); // Prevent Vec from freeing the data

        CHealthThresholdConfig {
            rules: CHealthMetricRuleArray { data, len },
        }
    }
}

// Conversion from C to Rust HealthThresholdConfig
impl TryFrom<&CHealthThresholdConfig> for HealthThresholdConfig {
    type Error = std::ffi::NulError;

    fn try_from(c_config: &CHealthThresholdConfig) -> Result<Self, Self::Error> {
        let mut rules = Vec::with_capacity(c_config.rules.len);

        unsafe {
            for i in 0..c_config.rules.len {
                let c_rule = &*c_config.rules.data.add(i);
                rules.push(HealthMetricRule::try_from(c_rule)?);
            }
        }

        Ok(HealthThresholdConfig { rules })
    }
}

/// Free a CHealthMetricRule and its contents
#[no_mangle]
pub extern "C" fn sequins_health_metric_rule_free(rule: CHealthMetricRule) {
    unsafe {
        if !rule.metric_name.is_null() {
            let _ = CString::from_raw(rule.metric_name);
        }
        if !rule.service_name.is_null() {
            let _ = CString::from_raw(rule.service_name);
        }
        if !rule.display_name.is_null() {
            let _ = CString::from_raw(rule.display_name);
        }
    }
}

/// Free a CHealthMetricRuleArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_health_metric_rule_array_free(arr: CHealthMetricRuleArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let rule = arr.data.add(i).read();
                sequins_health_metric_rule_free(rule);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CHealthThresholdConfig and all its contents
#[no_mangle]
pub extern "C" fn sequins_health_threshold_config_free(config: CHealthThresholdConfig) {
    sequins_health_metric_rule_array_free(config.rules);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_metric_rule_conversion() {
        let rule = HealthMetricRule {
            metric_name: "test.metric".to_string(),
            service_name: Some("test-service".to_string()),
            warning_threshold: 0.5,
            error_threshold: 1.0,
            higher_is_worse: true,
            weight: 0.25,
            display_name: "Test Metric".to_string(),
        };

        let c_rule = CHealthMetricRule::from(rule.clone());
        let back_rule = HealthMetricRule::try_from(&c_rule).unwrap();

        assert_eq!(back_rule.metric_name, rule.metric_name);
        assert_eq!(back_rule.service_name, rule.service_name);
        assert_eq!(back_rule.warning_threshold, rule.warning_threshold);
        assert_eq!(back_rule.error_threshold, rule.error_threshold);
        assert_eq!(back_rule.higher_is_worse, rule.higher_is_worse);
        assert_eq!(back_rule.weight, rule.weight);
        assert_eq!(back_rule.display_name, rule.display_name);

        sequins_health_metric_rule_free(c_rule);
    }

    #[test]
    fn test_health_config_conversion() {
        let config = HealthThresholdConfig::default();
        let original_len = config.rules.len();

        let c_config = CHealthThresholdConfig::from(config);
        assert_eq!(c_config.rules.len, original_len);

        let back_config = HealthThresholdConfig::try_from(&c_config).unwrap();
        assert_eq!(back_config.rules.len(), original_len);

        sequins_health_threshold_config_free(c_config);
    }
}
