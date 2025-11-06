use super::MetricId;
use serde::{Deserialize, Serialize};

/// Metric definition from OTLP
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    pub id: MetricId,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub metric_type: MetricType,
    pub service_name: String,
}

/// Metric type from OpenTelemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricType {
    Gauge,
    Counter,
    Histogram,
    Summary,
}

impl Metric {
    /// Check if this is a counter metric
    pub fn is_counter(&self) -> bool {
        matches!(self.metric_type, MetricType::Counter)
    }

    /// Check if this is a gauge metric
    pub fn is_gauge(&self) -> bool {
        matches!(self.metric_type, MetricType::Gauge)
    }

    /// Check if this is a histogram metric
    pub fn is_histogram(&self) -> bool {
        matches!(self.metric_type, MetricType::Histogram)
    }

    /// Check if this is a summary metric
    pub fn is_summary(&self) -> bool {
        matches!(self.metric_type, MetricType::Summary)
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
            metric_type,
            service_name: "test-service".to_string(),
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
}
