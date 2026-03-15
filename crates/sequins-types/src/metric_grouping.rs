/// Metric grouping pattern detection engine
///
/// Analyzes metric names to identify groups of related metrics that should
/// be visualized together based on naming conventions and patterns.
use crate::models::{
    GroupingPattern, Metric, MetricGroup, MetricType, PROM_HISTOGRAM_SUFFIXES, STAT_SUFFIXES,
};
use std::collections::HashMap;

/// Detect metric groups from a list of metrics
///
/// This function analyzes metric names and identifies patterns that indicate
/// metrics should be grouped together for visualization.
pub fn detect_metric_groups(metrics: Vec<Metric>) -> Vec<MetricGroup> {
    let mut groups = Vec::new();

    // Group metrics by resource_id first, then detect patterns within each resource group
    let mut by_resource: HashMap<u32, Vec<Metric>> = HashMap::new();
    for metric in metrics {
        by_resource
            .entry(metric.resource_id)
            .or_default()
            .push(metric);
    }

    // For each resource group, detect patterns
    for (resource_id, resource_metrics) in by_resource {
        // Use resource_id as service identifier (in real usage, would lookup in ResourceRegistry)
        let service_name = format!("resource_{}", resource_id);

        // Detect statistical variant groups
        groups.extend(detect_statistical_variants(
            &resource_metrics,
            &service_name,
        ));

        // Detect histogram families (Prometheus format)
        groups.extend(detect_histogram_families(&resource_metrics, &service_name));

        // Detect namespace groups (e.g., v8js.memory.heap.*)
        groups.extend(detect_namespace_groups(&resource_metrics, &service_name));
    }

    groups
}

/// Detect statistical variant groups
///
/// Finds metrics that share a common prefix and only differ by statistical
/// suffixes like .min, .max, .p50, .p90, .p99
///
/// Example: nodejs.eventloop.delay.{min,max,mean,stddev,p50,p90,p99}
fn detect_statistical_variants(metrics: &[Metric], service_name: &str) -> Vec<MetricGroup> {
    // Group by (prefix, type, unit) to ensure we only group compatible metrics
    let mut prefix_groups: HashMap<(String, u8, String), Vec<Metric>> = HashMap::new();

    for metric in metrics {
        // Split on last dot to get prefix and suffix
        if let Some(last_dot_idx) = metric.name.rfind('.') {
            let prefix = &metric.name[..last_dot_idx];
            let suffix = &metric.name[last_dot_idx + 1..];

            // Check if suffix is a statistical variant
            if is_stat_suffix(suffix) {
                let key = (prefix.to_string(), metric.metric_type, metric.unit.clone());
                prefix_groups.entry(key).or_default().push(metric.clone());
            }
        }
    }

    // Create MetricGroup for each prefix with 2+ variants
    // Limit group size to avoid creating massive unhelpful groups
    const MAX_GROUP_SIZE: usize = 20;

    prefix_groups
        .into_iter()
        .filter(|(_, group_metrics)| {
            group_metrics.len() >= 2 && group_metrics.len() <= MAX_GROUP_SIZE
        })
        .map(|((base_name, metric_type, unit), group_metrics)| {
            // Sort metric names for consistent ordering
            let mut metric_names: Vec<String> =
                group_metrics.iter().map(|m| m.name.clone()).collect();
            metric_names.sort();
            MetricGroup::new(
                base_name,
                metric_names,
                GroupingPattern::StatisticalVariants,
                service_name.to_string(),
                Some(MetricType::from(metric_type)),
                unit,
            )
        })
        .collect()
}

/// Detect histogram families (Prometheus format)
///
/// Finds metrics with the same base name and suffixes: _bucket, _count, _sum
///
/// Example: http_request_duration_seconds_{bucket,count,sum}
fn detect_histogram_families(metrics: &[Metric], service_name: &str) -> Vec<MetricGroup> {
    // Group by (base_name, type, unit)
    let mut base_names: HashMap<(String, u8, String), Vec<Metric>> = HashMap::new();

    for metric in metrics {
        // Check for Prometheus histogram suffixes
        for &suffix in PROM_HISTOGRAM_SUFFIXES {
            if let Some(base_name) = metric.name.strip_suffix(suffix) {
                let key = (
                    base_name.to_string(),
                    metric.metric_type,
                    metric.unit.clone(),
                );
                base_names.entry(key).or_default().push(metric.clone());
                break;
            }
        }
    }

    // A histogram family needs at least _count and _sum (buckets are multiple metrics)
    base_names
        .into_iter()
        .filter(|((base_name, _, _), group_metrics)| {
            // Must have at least 2 components
            if group_metrics.len() < 2 {
                return false;
            }
            // Check for _count and _sum
            let has_count = group_metrics
                .iter()
                .any(|m| m.name == format!("{}_count", base_name));
            let has_sum = group_metrics
                .iter()
                .any(|m| m.name == format!("{}_sum", base_name));
            has_count && has_sum
        })
        .map(|((base_name, metric_type, unit), group_metrics)| {
            let mut metric_names: Vec<String> =
                group_metrics.iter().map(|m| m.name.clone()).collect();
            metric_names.sort();
            MetricGroup::new(
                base_name,
                metric_names,
                GroupingPattern::HistogramFamily,
                service_name.to_string(),
                Some(MetricType::from(metric_type)),
                unit,
            )
        })
        .collect()
}

/// Detect namespace groups
///
/// Finds metrics that share a common namespace prefix (e.g., v8js.memory.heap.*)
/// This groups metrics like:
/// - v8js.memory.heap.{limit, used}
/// - v8js.memory.heap.space.{available_size, physical_size}
fn detect_namespace_groups(metrics: &[Metric], service_name: &str) -> Vec<MetricGroup> {
    // Group by (namespace, type, unit)
    let mut namespace_groups: HashMap<(String, u8, String), Vec<Metric>> = HashMap::new();

    // Build hierarchy and identify potential groups
    for metric in metrics {
        let parts: Vec<&str> = metric.name.split('.').collect();

        // Try different namespace depths (3 to 4 levels deep)
        for depth in 3..=4 {
            if parts.len() > depth {
                let namespace = parts[..depth].join(".");
                let key = (namespace.clone(), metric.metric_type, metric.unit.clone());
                namespace_groups
                    .entry(key)
                    .or_default()
                    .push(metric.clone());
            }
        }
    }

    // Filter to namespaces with 2-10 metrics
    const MIN_NAMESPACE_SIZE: usize = 2;
    const MAX_NAMESPACE_SIZE: usize = 10;

    namespace_groups
        .into_iter()
        .filter(|(_, group_metrics)| {
            group_metrics.len() >= MIN_NAMESPACE_SIZE && group_metrics.len() <= MAX_NAMESPACE_SIZE
        })
        .map(|((namespace, metric_type, unit), group_metrics)| {
            let mut metric_names: Vec<String> =
                group_metrics.iter().map(|m| m.name.clone()).collect();
            metric_names.sort();
            MetricGroup::new(
                namespace,
                metric_names,
                GroupingPattern::Custom,
                service_name.to_string(),
                Some(MetricType::from(metric_type)),
                unit,
            )
        })
        .collect()
}

/// Build a hierarchical tree of metric names
///
/// Useful for organizing metrics by namespace hierarchy
pub fn build_metric_hierarchy(metrics: &[Metric]) -> HashMap<String, Vec<String>> {
    let mut hierarchy: HashMap<String, Vec<String>> = HashMap::new();

    for metric in metrics {
        let parts: Vec<&str> = metric.name.split('.').collect();

        // Build hierarchy for each level
        for i in 1..parts.len() {
            let prefix = parts[..i].join(".");
            hierarchy
                .entry(prefix)
                .or_default()
                .push(metric.name.clone());
        }
    }

    hierarchy
}

/// Check if a suffix is a known statistical suffix
fn is_stat_suffix(suffix: &str) -> bool {
    STAT_SUFFIXES.contains(&suffix)
}

/// Extract the base name from a metric (everything before the last dot)
pub fn extract_base_name(metric_name: &str) -> Option<&str> {
    metric_name.rfind('.').map(|idx| &metric_name[..idx])
}

/// Extract the suffix from a metric (everything after the last dot)
pub fn extract_suffix(metric_name: &str) -> Option<&str> {
    metric_name.rfind('.').map(|idx| &metric_name[idx + 1..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{MetricId, MetricType};

    fn create_test_metric(name: &str, service: &str) -> Metric {
        // Map service name to resource_id for testing (simple hash-like mapping)
        let resource_id = if service == "service1" {
            1
        } else if service == "service2" {
            2
        } else {
            0
        };
        Metric {
            id: MetricId::new(),
            name: name.to_string(),
            description: String::new(),
            unit: String::new(),
            metric_type: MetricType::Gauge as u8,
            aggregation_temporality: 0,
            resource_id,
            scope_id: 0,
            is_generated: false,
        }
    }

    #[test]
    fn test_detect_statistical_variants() {
        let metrics = vec![
            create_test_metric("nodejs.eventloop.delay.min", "my-service"),
            create_test_metric("nodejs.eventloop.delay.max", "my-service"),
            create_test_metric("nodejs.eventloop.delay.mean", "my-service"),
            create_test_metric("nodejs.eventloop.delay.p50", "my-service"),
            create_test_metric("nodejs.eventloop.delay.p90", "my-service"),
            create_test_metric("nodejs.eventloop.delay.p99", "my-service"),
            create_test_metric("unrelated.metric", "my-service"),
        ];

        let groups = detect_statistical_variants(&metrics, "my-service");

        assert_eq!(groups.len(), 1);
        let group = &groups[0];
        assert_eq!(group.base_name, "nodejs.eventloop.delay");
        assert_eq!(group.count(), 6);
        assert_eq!(group.pattern, GroupingPattern::StatisticalVariants);
        assert!(group.contains("nodejs.eventloop.delay.min"));
        assert!(group.contains("nodejs.eventloop.delay.p99"));
        assert!(!group.contains("unrelated.metric"));
    }

    #[test]
    fn test_detect_histogram_families() {
        let metrics = vec![
            create_test_metric("http_request_duration_seconds_bucket", "my-service"),
            create_test_metric("http_request_duration_seconds_count", "my-service"),
            create_test_metric("http_request_duration_seconds_sum", "my-service"),
            create_test_metric("other_metric", "my-service"),
        ];

        let groups = detect_histogram_families(&metrics, "my-service");

        assert_eq!(groups.len(), 1);
        let group = &groups[0];
        assert_eq!(group.base_name, "http_request_duration_seconds");
        assert_eq!(group.count(), 3);
        assert_eq!(group.pattern, GroupingPattern::HistogramFamily);
        assert!(group.contains("http_request_duration_seconds_bucket"));
        assert!(group.contains("http_request_duration_seconds_count"));
        assert!(group.contains("http_request_duration_seconds_sum"));
    }

    #[test]
    fn test_histogram_family_requires_count_and_sum() {
        // Missing _sum, should not create a group
        let metrics = vec![
            create_test_metric("incomplete_bucket", "my-service"),
            create_test_metric("incomplete_count", "my-service"),
        ];

        let groups = detect_histogram_families(&metrics, "my-service");
        assert_eq!(groups.len(), 0);
    }

    #[test]
    fn test_detect_metric_groups_integration() {
        let metrics = vec![
            // Statistical variants group
            create_test_metric("nodejs.eventloop.delay.min", "service1"),
            create_test_metric("nodejs.eventloop.delay.max", "service1"),
            create_test_metric("nodejs.eventloop.delay.p99", "service1"),
            // Histogram family group
            create_test_metric("http_duration_bucket", "service1"),
            create_test_metric("http_duration_count", "service1"),
            create_test_metric("http_duration_sum", "service1"),
            // Standalone metrics
            create_test_metric("cpu.usage", "service1"),
        ];

        let groups = detect_metric_groups(metrics);

        // Should have 3 groups: statistical variants + histogram family + namespace
        assert_eq!(groups.len(), 3);

        // Find the groups
        let stat_group = groups
            .iter()
            .find(|g| g.pattern == GroupingPattern::StatisticalVariants);
        let hist_group = groups
            .iter()
            .find(|g| g.pattern == GroupingPattern::HistogramFamily);
        let namespace_group = groups.iter().find(|g| g.pattern == GroupingPattern::Custom);

        assert!(stat_group.is_some());
        assert!(hist_group.is_some());
        assert!(namespace_group.is_some());
    }

    #[test]
    fn test_is_stat_suffix() {
        assert!(is_stat_suffix("min"));
        assert!(is_stat_suffix("max"));
        assert!(is_stat_suffix("p50"));
        assert!(is_stat_suffix("p99"));
        assert!(is_stat_suffix("mean"));
        assert!(is_stat_suffix("stddev"));
        assert!(!is_stat_suffix("other"));
        assert!(!is_stat_suffix("bucket"));
    }

    #[test]
    fn test_extract_base_name() {
        assert_eq!(
            extract_base_name("nodejs.eventloop.delay.min"),
            Some("nodejs.eventloop.delay")
        );
        assert_eq!(extract_base_name("http.count"), Some("http"));
        assert_eq!(extract_base_name("standalone"), None);
    }

    #[test]
    fn test_extract_suffix() {
        assert_eq!(extract_suffix("nodejs.eventloop.delay.min"), Some("min"));
        assert_eq!(extract_suffix("http.count"), Some("count"));
        assert_eq!(extract_suffix("standalone"), None);
    }

    #[test]
    fn test_build_metric_hierarchy() {
        let metrics = vec![
            create_test_metric("nodejs.eventloop.delay", "service1"),
            create_test_metric("nodejs.eventloop.utilization", "service1"),
            create_test_metric("nodejs.gc.duration", "service1"),
            create_test_metric("http.server.duration", "service1"),
        ];

        let hierarchy = build_metric_hierarchy(&metrics);

        // Check top-level namespaces
        assert!(hierarchy.contains_key("nodejs"));
        assert!(hierarchy.contains_key("http"));

        // Check second-level
        assert!(hierarchy.contains_key("nodejs.eventloop"));
        assert!(hierarchy.contains_key("nodejs.gc"));
        assert!(hierarchy.contains_key("http.server"));

        // Verify counts
        assert_eq!(hierarchy.get("nodejs").unwrap().len(), 3); // All nodejs.* metrics
        assert_eq!(hierarchy.get("nodejs.eventloop").unwrap().len(), 2);
    }

    #[test]
    fn test_groups_by_service() {
        let metrics = vec![
            create_test_metric("metric.min", "service1"),
            create_test_metric("metric.max", "service1"),
            create_test_metric("metric.min", "service2"),
            create_test_metric("metric.max", "service2"),
        ];

        let groups = detect_metric_groups(metrics);

        // Should create 2 groups, one per resource
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().any(|g| g.service_name == "resource_1"));
        assert!(groups.iter().any(|g| g.service_name == "resource_2"));
    }
}
