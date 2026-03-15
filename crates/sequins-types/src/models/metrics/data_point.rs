use super::MetricId;
use crate::models::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metric data point (for gauge/counter)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricDataPoint {
    /// ID of the metric this point belongs to
    pub metric_id: MetricId,
    /// When this data point was recorded
    pub timestamp: Timestamp,
    /// Start time for cumulative metrics (optional)
    pub start_time: Option<Timestamp>,
    /// Numeric value of the metric
    pub value: f64,
    /// Additional attributes for this data point
    pub attributes: HashMap<String, String>,
    /// Resource ID reference (FK to ResourceRegistry)
    pub resource_id: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_data_point() {
        let metric_id = MetricId::new();
        let timestamp = Timestamp::now().unwrap();
        let mut attributes = HashMap::new();
        attributes.insert("method".to_string(), "GET".to_string());

        let data_point = MetricDataPoint {
            metric_id,
            timestamp,
            start_time: None,
            value: 42.5,
            attributes,
            resource_id: 0,
        };

        assert_eq!(data_point.value, 42.5);
        assert_eq!(
            data_point.attributes.get("method"),
            Some(&"GET".to_string())
        );
    }
}
