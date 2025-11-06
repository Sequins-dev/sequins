use super::MetricId;
use crate::models::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Histogram data point
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramDataPoint {
    pub metric_id: MetricId,
    pub timestamp: Timestamp,
    pub count: u64,
    pub sum: f64,
    pub bucket_counts: Vec<u64>,
    pub explicit_bounds: Vec<f64>,
    pub exemplars: Vec<Exemplar>,
    pub attributes: HashMap<String, String>,
}

/// Exemplar from OpenTelemetry histogram
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exemplar {
    pub timestamp: Timestamp,
    pub value: f64,
    pub filtered_attributes: HashMap<String, String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

impl HistogramDataPoint {
    /// Calculate average value
    pub fn average(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    /// Get percentile from histogram buckets (approximate)
    pub fn percentile(&self, p: f64) -> Option<f64> {
        if !(0.0..=100.0).contains(&p) || self.count == 0 {
            return None;
        }

        let target_count = (self.count as f64 * p / 100.0) as u64;
        let mut cumulative = 0u64;

        for (i, &bucket_count) in self.bucket_counts.iter().enumerate() {
            cumulative += bucket_count;
            if cumulative >= target_count {
                return self.explicit_bounds.get(i).copied();
            }
        }

        // If we've exhausted all buckets, return the last bound
        self.explicit_bounds.last().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_average() {
        let metric_id = MetricId::new();
        let histogram = HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::now().unwrap(),
            count: 100,
            sum: 500.0,
            bucket_counts: vec![10, 20, 30, 40],
            explicit_bounds: vec![1.0, 5.0, 10.0, 50.0],
            exemplars: vec![],
            attributes: HashMap::new(),
        };

        assert_eq!(histogram.average(), 5.0); // 500 / 100
    }

    #[test]
    fn test_histogram_average_empty() {
        let metric_id = MetricId::new();
        let histogram = HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::now().unwrap(),
            count: 0,
            sum: 0.0,
            bucket_counts: vec![],
            explicit_bounds: vec![],
            exemplars: vec![],
            attributes: HashMap::new(),
        };

        assert_eq!(histogram.average(), 0.0);
    }

    #[test]
    fn test_histogram_percentile() {
        let metric_id = MetricId::new();
        let histogram = HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::now().unwrap(),
            count: 100,
            sum: 500.0,
            bucket_counts: vec![10, 20, 30, 40],
            explicit_bounds: vec![1.0, 5.0, 10.0, 50.0],
            exemplars: vec![],
            attributes: HashMap::new(),
        };

        // P50 should be in the third bucket (cumulative: 10+20+30=60, which is >= 50)
        assert_eq!(histogram.percentile(50.0), Some(10.0));

        // P90 should be in the fourth bucket (cumulative: 10+20+30+40=100, which is >= 90)
        assert_eq!(histogram.percentile(90.0), Some(50.0));

        // P10 should be in the first bucket
        assert_eq!(histogram.percentile(10.0), Some(1.0));
    }

    #[test]
    fn test_histogram_percentile_invalid() {
        let metric_id = MetricId::new();
        let histogram = HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::now().unwrap(),
            count: 100,
            sum: 500.0,
            bucket_counts: vec![10, 20, 30, 40],
            explicit_bounds: vec![1.0, 5.0, 10.0, 50.0],
            exemplars: vec![],
            attributes: HashMap::new(),
        };

        assert_eq!(histogram.percentile(-1.0), None); // Negative
        assert_eq!(histogram.percentile(101.0), None); // > 100
    }
}
