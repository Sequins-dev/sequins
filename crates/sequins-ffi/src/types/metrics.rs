//! Metric FFI types
//!
//! C-compatible types for OpenTelemetry metrics and related structures.

use super::common::{CStringArray, CTimestamp};
use sequins_types::models::{Metric, MetricDataPoint, MetricType};
use std::ffi::CString;
use std::os::raw::c_char;

/// C-compatible metric type
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CMetricType {
    Gauge = 0,
    Counter = 1,
    Histogram = 2,
    Summary = 3,
}

impl From<MetricType> for CMetricType {
    fn from(metric_type: MetricType) -> Self {
        match metric_type {
            MetricType::Gauge => CMetricType::Gauge,
            MetricType::Counter => CMetricType::Counter,
            MetricType::Histogram => CMetricType::Histogram,
            MetricType::Summary => CMetricType::Summary,
        }
    }
}

/// C-compatible metric definition
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMetric {
    /// Metric ID (UUID hex string, must be freed)
    pub id: *mut c_char,
    /// Metric name (must be freed)
    pub name: *mut c_char,
    /// Description (must be freed)
    pub description: *mut c_char,
    /// Unit of measurement (must be freed)
    pub unit: *mut c_char,
    /// Metric type
    pub metric_type: CMetricType,
    /// Service name (must be freed)
    pub service_name: *mut c_char,
    /// Whether this metric was generated internally (e.g., health metrics)
    pub is_generated: bool,
}

impl From<Metric> for CMetric {
    fn from(metric: Metric) -> Self {
        let id = CString::new(metric.id.to_hex()).unwrap().into_raw();
        let metric_type = CMetricType::from(metric.get_metric_type());
        let is_generated = metric.is_generated;
        let name = CString::new(metric.name).unwrap().into_raw();
        let description = CString::new(metric.description).unwrap().into_raw();
        let unit = CString::new(metric.unit).unwrap().into_raw();
        let service_name = CString::new("unknown").unwrap().into_raw(); // Service name moved to resource registry

        CMetric {
            id,
            name,
            description,
            unit,
            metric_type,
            service_name,
            is_generated,
        }
    }
}

/// C-compatible metric data point query parameters
#[repr(C)]
pub struct CMetricDataPointQuery {
    /// Metric ID to fetch data points for (UUID hex string)
    pub metric_id: *const c_char,
    /// Time range start (nanoseconds since epoch)
    pub start_time: super::common::CTimestamp,
    /// Time range end (nanoseconds since epoch)
    pub end_time: super::common::CTimestamp,
    /// Bucket duration for downsampling (nanoseconds), 0 = no downsampling
    pub bucket_duration_nanos: i64,
}

/// Stub function to ensure CMetricDataPointQuery is exported to C header
#[no_mangle]
pub extern "C" fn sequins_query_metric_data_points_stub(
    _query: CMetricDataPointQuery,
) -> CMetricDataPointQueryResult {
    CMetricDataPointQueryResult {
        data_points: CMetricDataPointArray {
            data: std::ptr::null_mut(),
            len: 0,
        },
        cursor: super::common::CQueryCursor {
            opaque: std::ptr::null_mut(),
            timestamp_nanos: 0,
        },
    }
}

/// C-compatible metric query parameters
#[repr(C)]
pub struct CMetricQuery {
    /// Metric name filter (null-terminated string), null if not filtering
    pub name: *const c_char,
    /// Service name filter (null-terminated string), null if not filtering
    pub service: *const c_char,
    /// Time range start (nanoseconds since epoch)
    pub start_time: super::common::CTimestamp,
    /// Time range end (nanoseconds since epoch)
    pub end_time: super::common::CTimestamp,
    /// Limit number of results, 0 = no limit
    pub limit: usize,
}

/// Stub function to ensure CMetricQuery is exported to C header
#[no_mangle]
pub extern "C" fn sequins_query_metrics_stub(_query: CMetricQuery) -> CMetricQueryResult {
    CMetricQueryResult {
        metrics: CMetricArray {
            data: std::ptr::null_mut(),
            len: 0,
        },
        cursor: super::common::CQueryCursor {
            opaque: std::ptr::null_mut(),
            timestamp_nanos: 0,
        },
    }
}

/// Free a CMetric and its contents
///
/// # Safety
/// * Must only be called once per CMetric
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_metric_free(metric: CMetric) {
    unsafe {
        if !metric.id.is_null() {
            let _ = CString::from_raw(metric.id);
        }
        if !metric.name.is_null() {
            let _ = CString::from_raw(metric.name);
        }
        if !metric.description.is_null() {
            let _ = CString::from_raw(metric.description);
        }
        if !metric.unit.is_null() {
            let _ = CString::from_raw(metric.unit);
        }
        if !metric.service_name.is_null() {
            let _ = CString::from_raw(metric.service_name);
        }
    }
}

/// C-compatible metric data point
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMetricDataPoint {
    /// Metric ID this point belongs to (UUID hex string, must be freed)
    pub metric_id: *mut c_char,
    /// Timestamp (nanoseconds since epoch)
    pub timestamp: CTimestamp,
    /// Numeric value
    pub value: f64,
    /// Attribute keys (must be freed)
    pub attribute_keys: CStringArray,
    /// Attribute values (must be freed)
    pub attribute_values: CStringArray,
}

impl From<MetricDataPoint> for CMetricDataPoint {
    fn from(point: MetricDataPoint) -> Self {
        let metric_id = CString::new(point.metric_id.to_hex()).unwrap().into_raw();

        // Convert HashMap<String, String> to parallel arrays
        let keys: Vec<String> = point.attributes.keys().cloned().collect();
        let values: Vec<String> = point.attributes.values().cloned().collect();

        let keys_len = keys.len();
        let values_len = values.len();

        // Convert to C string arrays
        let mut key_ptrs: Vec<*mut c_char> = keys
            .into_iter()
            .map(|k| CString::new(k).unwrap().into_raw())
            .collect();
        let mut value_ptrs: Vec<*mut c_char> = values
            .into_iter()
            .map(|v| CString::new(v).unwrap().into_raw())
            .collect();

        let attribute_keys = if keys_len > 0 {
            let data = key_ptrs.as_mut_ptr();
            std::mem::forget(key_ptrs);
            CStringArray {
                data,
                len: keys_len,
            }
        } else {
            CStringArray {
                data: std::ptr::null_mut(),
                len: 0,
            }
        };

        let attribute_values = if values_len > 0 {
            let data = value_ptrs.as_mut_ptr();
            std::mem::forget(value_ptrs);
            CStringArray {
                data,
                len: values_len,
            }
        } else {
            CStringArray {
                data: std::ptr::null_mut(),
                len: 0,
            }
        };

        CMetricDataPoint {
            metric_id,
            timestamp: point.timestamp.as_nanos(),
            value: point.value,
            attribute_keys,
            attribute_values,
        }
    }
}

/// Free a CMetricDataPoint and its contents
///
/// # Safety
/// * Must only be called once per CMetricDataPoint
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_metric_data_point_free(point: CMetricDataPoint) {
    unsafe {
        if !point.metric_id.is_null() {
            let _ = CString::from_raw(point.metric_id);
        }

        // Free attribute keys
        if !point.attribute_keys.data.is_null() && point.attribute_keys.len > 0 {
            for i in 0..point.attribute_keys.len {
                let ptr = *point.attribute_keys.data.add(i);
                if !ptr.is_null() {
                    let _ = CString::from_raw(ptr);
                }
            }
            let _ = Vec::from_raw_parts(
                point.attribute_keys.data,
                point.attribute_keys.len,
                point.attribute_keys.len,
            );
        }

        // Free attribute values
        if !point.attribute_values.data.is_null() && point.attribute_values.len > 0 {
            for i in 0..point.attribute_values.len {
                let ptr = *point.attribute_values.data.add(i);
                if !ptr.is_null() {
                    let _ = CString::from_raw(ptr);
                }
            }
            let _ = Vec::from_raw_parts(
                point.attribute_values.data,
                point.attribute_values.len,
                point.attribute_values.len,
            );
        }
    }
}

// =============================================================================
// Metric Array and Query Result Types
// =============================================================================

/// C-compatible array of metrics
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMetricArray {
    /// Pointer to array of metrics
    pub data: *mut CMetric,
    /// Number of metrics in the array
    pub len: usize,
}

impl From<Vec<Metric>> for CMetricArray {
    fn from(metrics: Vec<Metric>) -> Self {
        let len = metrics.len();
        if len == 0 {
            return CMetricArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_metrics: Vec<CMetric> = metrics.into_iter().map(CMetric::from).collect();
        let data = c_metrics.as_mut_ptr();
        std::mem::forget(c_metrics);
        CMetricArray { data, len }
    }
}

/// C-compatible metric query result (historical data + cursor)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMetricQueryResult {
    /// Array of historical metrics
    pub metrics: CMetricArray,
    /// Cursor for subscribing to live updates
    pub cursor: super::common::CQueryCursor,
}

impl From<crate::compat::MetricQueryResult> for CMetricQueryResult {
    fn from(result: crate::compat::MetricQueryResult) -> Self {
        CMetricQueryResult {
            metrics: CMetricArray::from(result.metrics),
            cursor: super::common::CQueryCursor::from(result.cursor),
        }
    }
}

/// Free a CMetricArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_metric_array_free(arr: CMetricArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let metric = arr.data.add(i).read();
                sequins_metric_free(metric);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CMetricQueryResult and all its contents
#[no_mangle]
pub extern "C" fn sequins_metric_query_result_free(result: CMetricQueryResult) {
    sequins_metric_array_free(result.metrics);
    super::common::sequins_cursor_free(result.cursor);
}

/// C-compatible array of metric data points
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMetricDataPointArray {
    /// Pointer to array of data points
    pub data: *mut CMetricDataPoint,
    /// Number of data points in the array
    pub len: usize,
}

impl From<Vec<MetricDataPoint>> for CMetricDataPointArray {
    fn from(points: Vec<MetricDataPoint>) -> Self {
        let len = points.len();
        if len == 0 {
            return CMetricDataPointArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_points: Vec<CMetricDataPoint> =
            points.into_iter().map(CMetricDataPoint::from).collect();
        let data = c_points.as_mut_ptr();
        std::mem::forget(c_points);
        CMetricDataPointArray { data, len }
    }
}

/// C-compatible metric data point query result (historical data + cursor)
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMetricDataPointQueryResult {
    /// Array of historical data points
    pub data_points: CMetricDataPointArray,
    /// Cursor for subscribing to live updates
    pub cursor: super::common::CQueryCursor,
}

impl From<crate::compat::MetricDataPointQueryResult> for CMetricDataPointQueryResult {
    fn from(result: crate::compat::MetricDataPointQueryResult) -> Self {
        CMetricDataPointQueryResult {
            data_points: CMetricDataPointArray::from(result.data_points),
            cursor: super::common::CQueryCursor::from(result.cursor),
        }
    }
}

/// Free a CMetricDataPointArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_metric_data_point_array_free(arr: CMetricDataPointArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let point = arr.data.add(i).read();
                sequins_metric_data_point_free(point);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

/// Free a CMetricDataPointQueryResult and all its contents
#[no_mangle]
pub extern "C" fn sequins_metric_data_point_query_result_free(result: CMetricDataPointQueryResult) {
    sequins_metric_data_point_array_free(result.data_points);
    super::common::sequins_cursor_free(result.cursor);
}

// =============================================================================
// Metric Group Types
// =============================================================================

/// C-compatible grouping pattern enum
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CGroupingPattern {
    StatisticalVariants = 0,
    HistogramFamily = 1,
    AttributeStreams = 2,
    Custom = 3,
    Namespace = 4,
}

impl From<sequins_types::models::GroupingPattern> for CGroupingPattern {
    fn from(pattern: sequins_types::models::GroupingPattern) -> Self {
        match pattern {
            sequins_types::models::GroupingPattern::StatisticalVariants => {
                CGroupingPattern::StatisticalVariants
            }
            sequins_types::models::GroupingPattern::HistogramFamily => {
                CGroupingPattern::HistogramFamily
            }
            sequins_types::models::GroupingPattern::AttributeStreams => {
                CGroupingPattern::AttributeStreams
            }
            sequins_types::models::GroupingPattern::Custom => CGroupingPattern::Custom,
            sequins_types::models::GroupingPattern::Namespace => CGroupingPattern::Namespace,
        }
    }
}

/// C-compatible visualization type enum
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum CVisualizationType {
    VisualizationMultiLineChart = 0,
    VisualizationBoxPlot = 1,
    VisualizationHistogram = 2,
    VisualizationRangeChart = 3,
    VisualizationStackedArea = 4,
    VisualizationSeparateCharts = 5,
}

impl From<sequins_types::models::VisualizationType> for CVisualizationType {
    fn from(viz: sequins_types::models::VisualizationType) -> Self {
        match viz {
            sequins_types::models::VisualizationType::MultiLineChart => {
                CVisualizationType::VisualizationMultiLineChart
            }
            sequins_types::models::VisualizationType::BoxPlot => {
                CVisualizationType::VisualizationBoxPlot
            }
            sequins_types::models::VisualizationType::Histogram => {
                CVisualizationType::VisualizationHistogram
            }
            sequins_types::models::VisualizationType::RangeChart => {
                CVisualizationType::VisualizationRangeChart
            }
            sequins_types::models::VisualizationType::StackedArea => {
                CVisualizationType::VisualizationStackedArea
            }
            sequins_types::models::VisualizationType::SeparateCharts => {
                CVisualizationType::VisualizationSeparateCharts
            }
        }
    }
}

/// C-compatible metric group
#[repr(C)]
pub struct CMetricGroup {
    /// Base name of the metric group (e.g., "nodejs.eventloop.delay")
    pub base_name: *mut c_char,
    /// Names of all metrics in this group
    pub metric_names: CStringArray,
    /// The detected grouping pattern
    pub pattern: CGroupingPattern,
    /// Service name this group belongs to
    pub service_name: *mut c_char,
    /// Metric type (if all metrics share the same type)
    pub metric_type: CMetricType,
    /// Whether metric_type is valid
    pub has_metric_type: bool,
    /// Shared unit for all metrics in the group
    pub unit: *mut c_char,
    /// Recommended visualization type for this group
    pub visualization: CVisualizationType,
}

impl From<sequins_types::models::MetricGroup> for CMetricGroup {
    fn from(group: sequins_types::models::MetricGroup) -> Self {
        let base_name = CString::new(group.base_name).unwrap().into_raw();
        let service_name = CString::new(group.service_name).unwrap().into_raw();
        let unit = CString::new(group.unit).unwrap().into_raw();

        // Convert metric_names Vec<String> to CStringArray
        let names_len = group.metric_names.len();
        let metric_names = if names_len > 0 {
            let mut name_ptrs: Vec<*mut c_char> = group
                .metric_names
                .into_iter()
                .map(|n| CString::new(n).unwrap().into_raw())
                .collect();
            let data = name_ptrs.as_mut_ptr();
            std::mem::forget(name_ptrs);
            CStringArray {
                data,
                len: names_len,
            }
        } else {
            CStringArray {
                data: std::ptr::null_mut(),
                len: 0,
            }
        };

        let (metric_type, has_metric_type) = match group.metric_type {
            Some(mt) => (CMetricType::from(mt), true),
            None => (CMetricType::Gauge, false),
        };

        CMetricGroup {
            base_name,
            metric_names,
            pattern: CGroupingPattern::from(group.pattern),
            service_name,
            metric_type,
            has_metric_type,
            unit,
            visualization: CVisualizationType::from(group.visualization),
        }
    }
}

/// Free a CMetricGroup and its contents
///
/// # Safety
/// * Must only be called once per CMetricGroup
/// * All pointers must be valid
#[no_mangle]
pub extern "C" fn sequins_metric_group_free(group: CMetricGroup) {
    unsafe {
        if !group.base_name.is_null() {
            let _ = CString::from_raw(group.base_name);
        }
        if !group.service_name.is_null() {
            let _ = CString::from_raw(group.service_name);
        }
        if !group.unit.is_null() {
            let _ = CString::from_raw(group.unit);
        }

        // Free metric_names
        if !group.metric_names.data.is_null() && group.metric_names.len > 0 {
            for i in 0..group.metric_names.len {
                let ptr = *group.metric_names.data.add(i);
                if !ptr.is_null() {
                    let _ = CString::from_raw(ptr);
                }
            }
            let _ = Vec::from_raw_parts(
                group.metric_names.data,
                group.metric_names.len,
                group.metric_names.len,
            );
        }
    }
}

/// C-compatible array of metric groups
#[repr(C)]
#[derive(Copy, Clone)]
pub struct CMetricGroupArray {
    /// Pointer to array of metric groups
    pub data: *mut CMetricGroup,
    /// Number of groups in the array
    pub len: usize,
}

impl From<Vec<sequins_types::models::MetricGroup>> for CMetricGroupArray {
    fn from(groups: Vec<sequins_types::models::MetricGroup>) -> Self {
        let len = groups.len();
        if len == 0 {
            return CMetricGroupArray {
                data: std::ptr::null_mut(),
                len: 0,
            };
        }
        let mut c_groups: Vec<CMetricGroup> = groups.into_iter().map(CMetricGroup::from).collect();
        let data = c_groups.as_mut_ptr();
        std::mem::forget(c_groups);
        CMetricGroupArray { data, len }
    }
}

/// Free a CMetricGroupArray and all its contents
#[no_mangle]
pub extern "C" fn sequins_metric_group_array_free(arr: CMetricGroupArray) {
    unsafe {
        if !arr.data.is_null() && arr.len > 0 {
            for i in 0..arr.len {
                let group = arr.data.add(i).read();
                sequins_metric_group_free(group);
            }
            let _ = Vec::from_raw_parts(arr.data, arr.len, arr.len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_types::models::{MetricId, Timestamp};
    use std::collections::HashMap;

    fn create_test_metric() -> Metric {
        Metric {
            id: MetricId::new(),
            name: "http.server.duration".to_string(),
            description: "HTTP server request duration".to_string(),
            unit: "ms".to_string(),
            metric_type: MetricType::Histogram as u8,
            aggregation_temporality: 0, // Unspecified
            is_generated: false,
            resource_id: 0,
            scope_id: 0,
        }
    }

    #[test]
    fn test_metric_conversion() {
        let metric = create_test_metric();
        let c_metric = CMetric::from(metric.clone());

        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(c_metric.name).to_str().unwrap(),
                "http.server.duration"
            );
            // Service name is now "unknown" because Metric has resource_id instead of service_name
            assert_eq!(
                std::ffi::CStr::from_ptr(c_metric.service_name)
                    .to_str()
                    .unwrap(),
                "unknown"
            );
        }

        assert_eq!(c_metric.metric_type, CMetricType::Histogram);

        sequins_metric_free(c_metric);
    }

    #[test]
    fn test_metric_type_conversion() {
        assert_eq!(CMetricType::from(MetricType::Gauge), CMetricType::Gauge);
        assert_eq!(CMetricType::from(MetricType::Counter), CMetricType::Counter);
        assert_eq!(
            CMetricType::from(MetricType::Histogram),
            CMetricType::Histogram
        );
        assert_eq!(CMetricType::from(MetricType::Summary), CMetricType::Summary);
    }

    #[test]
    fn test_metric_data_point_conversion() {
        let mut attributes = HashMap::new();
        attributes.insert("method".to_string(), "GET".to_string());
        attributes.insert("status".to_string(), "200".to_string());

        let point = MetricDataPoint {
            metric_id: MetricId::new(),
            timestamp: Timestamp::from_secs(1000),
            start_time: None,
            value: 42.5,
            attributes,
            resource_id: 0,
        };

        let c_point = CMetricDataPoint::from(point);

        assert_eq!(c_point.value, 42.5);
        assert_eq!(c_point.attribute_keys.len, 2);
        assert_eq!(c_point.attribute_values.len, 2);

        sequins_metric_data_point_free(c_point);
    }
}
