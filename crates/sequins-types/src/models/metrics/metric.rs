use super::MetricId;
use serde::{Deserialize, Serialize};

/// Metric definition from OTLP
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// Unique identifier for this metric
    pub id: MetricId,
    /// Metric name (e.g., "http.server.duration")
    pub name: String,
    /// Human-readable description
    pub description: String,
    /// Unit of measurement (e.g., "ms", "bytes", "1")
    pub unit: String,
    /// Type of metric (0=Gauge, 1=Counter, 2=Histogram, 3=Summary)
    pub metric_type: u8,
    /// Aggregation temporality (0=Unspecified, 1=Delta, 2=Cumulative)
    pub aggregation_temporality: u8,
    /// Resource ID reference (FK to ResourceRegistry)
    pub resource_id: u32,
    /// Scope ID reference (FK to ScopeRegistry)
    pub scope_id: u32,
    /// Whether this metric was generated internally (e.g., health metrics)
    /// vs reported via OTLP. Defaults to false for OTLP-reported metrics.
    #[serde(default)]
    pub is_generated: bool,
}

/// Metric type from OpenTelemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum MetricType {
    /// Point-in-time value (can go up or down)
    Gauge = 0,
    /// Monotonically increasing value
    Counter = 1,
    /// Distribution of values in buckets
    Histogram = 2,
    /// Statistical summary (quantiles)
    Summary = 3,
}

impl From<u8> for MetricType {
    fn from(value: u8) -> Self {
        match value {
            0 => MetricType::Gauge,
            1 => MetricType::Counter,
            2 => MetricType::Histogram,
            3 => MetricType::Summary,
            _ => MetricType::Gauge, // Default for unknown
        }
    }
}

impl From<MetricType> for u8 {
    fn from(metric_type: MetricType) -> Self {
        metric_type as u8
    }
}

impl Metric {
    /// Get the metric type as enum
    pub fn get_metric_type(&self) -> MetricType {
        MetricType::from(self.metric_type)
    }

    /// Check if this is a counter metric
    pub fn is_counter(&self) -> bool {
        self.metric_type == MetricType::Counter as u8
    }

    /// Check if this is a gauge metric
    pub fn is_gauge(&self) -> bool {
        self.metric_type == MetricType::Gauge as u8
    }

    /// Check if this is a histogram metric
    pub fn is_histogram(&self) -> bool {
        self.metric_type == MetricType::Histogram as u8
    }

    /// Check if this is a summary metric
    pub fn is_summary(&self) -> bool {
        self.metric_type == MetricType::Summary as u8
    }

    /// Check if this metric was generated internally (e.g., health metrics)
    pub fn is_generated(&self) -> bool {
        self.is_generated
    }

    /// Check if this is a health metric (sequins.health.* namespace)
    pub fn is_health_metric(&self) -> bool {
        self.name.starts_with("sequins.health.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_metric(metric_type: MetricType) -> Metric {
        Metric {
            id: MetricId::new(),
            name: "test.metric".to_string(),
            description: "Test metric".to_string(),
            unit: "ms".to_string(),
            metric_type: metric_type as u8,
            aggregation_temporality: 0,
            resource_id: 0,
            scope_id: 0,
            is_generated: false,
        }
    }

    #[test]
    fn test_metric_type_checks() {
        let counter = create_test_metric(MetricType::Counter);
        assert!(counter.is_counter());
        assert!(!counter.is_gauge());
        assert!(!counter.is_histogram());

        let gauge = create_test_metric(MetricType::Gauge);
        assert!(gauge.is_gauge());
        assert!(!gauge.is_counter());

        let histogram = create_test_metric(MetricType::Histogram);
        assert!(histogram.is_histogram());
        assert!(!histogram.is_summary());

        let summary = create_test_metric(MetricType::Summary);
        assert!(summary.is_summary());
    }

    #[test]
    fn test_metric_type_from_unknown_u8() {
        // 99 is not a valid MetricType — should fall back to Gauge
        let mt = MetricType::from(99u8);
        assert_eq!(mt, MetricType::Gauge);
    }
}
