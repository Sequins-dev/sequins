use serde::{Deserialize, Serialize};

use super::MetricType;

/// A group of related metrics that should be visualized together
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricGroup {
    /// Base name of the metric group (e.g., "nodejs.eventloop.delay")
    pub base_name: String,

    /// Names of all metrics in this group
    pub metric_names: Vec<String>,

    /// The detected grouping pattern
    pub pattern: GroupingPattern,

    /// Service name this group belongs to
    pub service_name: String,

    /// Shared metric type for all metrics in the group
    pub metric_type: Option<MetricType>,

    /// Shared unit for all metrics in the group
    pub unit: String,

    /// Recommended visualization type for this group
    pub visualization: VisualizationType,
}

/// Pattern that indicates metrics should be grouped together
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupingPattern {
    /// Statistical variants (min, max, mean, stddev, percentiles)
    /// Example: nodejs.eventloop.delay.{min,max,mean,p50,p90,p99}
    StatisticalVariants,

    /// Histogram family with _bucket, _count, _sum suffixes (Prometheus format)
    /// Example: http_request_duration_seconds_{bucket,count,sum}
    HistogramFamily,

    /// Same metric name with different attribute combinations
    /// Example: http.server.request.duration{method="GET"} vs {method="POST"}
    AttributeStreams,

    /// User-defined custom grouping
    Custom,

    /// Hierarchical namespace grouping
    /// Example: All metrics under "http.server.*"
    Namespace,
}

/// Recommended visualization type for a metric group
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VisualizationType {
    /// Multiple lines on single chart (good for percentiles, attribute streams)
    MultiLineChart,

    /// Box plot over time (min, median, max, percentiles)
    BoxPlot,

    /// Histogram bars (for bucket data)
    Histogram,

    /// Range chart with shaded area (min/mean/max)
    RangeChart,

    /// Stacked area chart (for additive metrics)
    StackedArea,

    /// Separate individual charts
    SeparateCharts,
}

impl MetricGroup {
    /// Create a new metric group
    pub fn new(
        base_name: String,
        metric_names: Vec<String>,
        pattern: GroupingPattern,
        service_name: String,
        metric_type: Option<MetricType>,
        unit: String,
    ) -> Self {
        let visualization = Self::default_visualization(&pattern);
        Self {
            base_name,
            metric_names,
            pattern,
            service_name,
            metric_type,
            unit,
            visualization,
        }
    }

    /// Determine default visualization type based on grouping pattern
    fn default_visualization(pattern: &GroupingPattern) -> VisualizationType {
        match pattern {
            GroupingPattern::StatisticalVariants => {
                // Percentiles work well as multi-line charts
                VisualizationType::MultiLineChart
            }
            GroupingPattern::HistogramFamily => {
                // Histogram data should be shown as histogram bars
                VisualizationType::Histogram
            }
            GroupingPattern::AttributeStreams => {
                // Different attribute combinations work well as multi-line
                VisualizationType::MultiLineChart
            }
            GroupingPattern::Custom => {
                // Let user decide, default to separate
                VisualizationType::SeparateCharts
            }
            GroupingPattern::Namespace => {
                // Namespace grouping is more organizational, show separately
                VisualizationType::SeparateCharts
            }
        }
    }

    /// Get the number of metrics in this group
    pub fn count(&self) -> usize {
        self.metric_names.len()
    }

    /// Check if this group contains a specific metric name
    pub fn contains(&self, metric_name: &str) -> bool {
        self.metric_names.iter().any(|n| n == metric_name)
    }
}

/// Known statistical suffixes that indicate related metrics
pub const STAT_SUFFIXES: &[&str] = &[
    "min", "max", "mean", "median", "avg", "average", "stddev", "std_dev", "variance", "p50",
    "p75", "p90", "p95", "p99", "p999", "count", "sum", "total",
];

/// Known Prometheus histogram/summary suffixes
pub const PROM_HISTOGRAM_SUFFIXES: &[&str] = &["_bucket", "_count", "_sum"];

/// Known Prometheus summary suffixes
pub const PROM_SUMMARY_SUFFIXES: &[&str] = &["_count", "_sum"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_group_creation() {
        let group = MetricGroup::new(
            "nodejs.eventloop.delay".to_string(),
            vec![
                "nodejs.eventloop.delay.min".to_string(),
                "nodejs.eventloop.delay.max".to_string(),
                "nodejs.eventloop.delay.p99".to_string(),
            ],
            GroupingPattern::StatisticalVariants,
            "my-service".to_string(),
            Some(MetricType::Gauge),
            "ms".to_string(),
        );

        assert_eq!(group.base_name, "nodejs.eventloop.delay");
        assert_eq!(group.count(), 3);
        assert_eq!(group.visualization, VisualizationType::MultiLineChart);
        assert_eq!(group.metric_type, Some(MetricType::Gauge));
        assert_eq!(group.unit, "ms");
    }

    #[test]
    fn test_contains_metric() {
        let group = MetricGroup::new(
            "test.metric".to_string(),
            vec!["test.metric.min".to_string(), "test.metric.max".to_string()],
            GroupingPattern::StatisticalVariants,
            "service".to_string(),
            Some(MetricType::Counter),
            "".to_string(),
        );

        assert!(group.contains("test.metric.min"));
        assert!(group.contains("test.metric.max"));
        assert!(!group.contains("test.metric.p99"));
    }

    #[test]
    fn test_default_visualizations() {
        let stat_viz = MetricGroup::default_visualization(&GroupingPattern::StatisticalVariants);
        assert_eq!(stat_viz, VisualizationType::MultiLineChart);

        let hist_viz = MetricGroup::default_visualization(&GroupingPattern::HistogramFamily);
        assert_eq!(hist_viz, VisualizationType::Histogram);

        let attr_viz = MetricGroup::default_visualization(&GroupingPattern::AttributeStreams);
        assert_eq!(attr_viz, VisualizationType::MultiLineChart);
    }
}
