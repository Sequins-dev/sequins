use super::MetricId;
use crate::models::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metric data point (for gauge/counter)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricDataPoint {
    pub metric_id: MetricId,
    pub timestamp: Timestamp,
    pub value: f64,
    pub attributes: HashMap<String, String>,
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
            value: 42.5,
            attributes,
        };

        assert_eq!(data_point.value, 42.5);
        assert_eq!(
            data_point.attributes.get("method"),
            Some(&"GET".to_string())
        );
    }
}
