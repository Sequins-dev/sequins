use super::MetricId;
use crate::models::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Histogram data point
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramDataPoint {
    /// ID of the metric this histogram belongs to
    pub metric_id: MetricId,
    /// When this histogram was recorded
    pub timestamp: Timestamp,
    /// Start time for cumulative histograms (optional)
    pub start_time: Option<Timestamp>,
    /// Total count of observations
    pub count: u64,
    /// Sum of all observed values
    pub sum: f64,
    /// Minimum observed value (optional, per OTLP spec)
    pub min: Option<f64>,
    /// Maximum observed value (optional, per OTLP spec)
    pub max: Option<f64>,
    /// Count of observations in each bucket
    pub bucket_counts: Vec<u64>,
    /// Upper bounds for each bucket
    pub explicit_bounds: Vec<f64>,
    /// Sample traces associated with buckets
    pub exemplars: Vec<Exemplar>,
    /// Additional attributes for this histogram
    pub attributes: HashMap<String, String>,
    /// Resource ID reference (FK to ResourceRegistry)
    pub resource_id: u32,
}

/// Exemplar from OpenTelemetry histogram
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exemplar {
    /// When this exemplar was recorded
    pub timestamp: Timestamp,
    /// Value of this exemplar
    pub value: f64,
    /// Filtered attributes for this exemplar
    pub filtered_attributes: HashMap<String, String>,
    /// Associated trace ID (if available)
    pub trace_id: Option<String>,
    /// Associated span ID (if available)
    pub span_id: Option<String>,
}

/// Exponential histogram data point (native OTLP format)
///
/// Stores the compact exponential histogram representation instead of converting
/// to explicit bucket boundaries, preserving the natural structure for better
/// compression (sparse bucket representation with offset + counts).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExponentialHistogramDataPoint {
    /// ID of the metric this histogram belongs to
    pub metric_id: MetricId,
    /// When this histogram was recorded
    pub timestamp: Timestamp,
    /// Start time for cumulative histograms (optional)
    pub start_time: Option<Timestamp>,
    /// Total count of observations
    pub count: u64,
    /// Sum of all observed values
    pub sum: f64,
    /// Minimum observed value (optional)
    pub min: Option<f64>,
    /// Maximum observed value (optional)
    pub max: Option<f64>,
    /// Exponential scale factor: base = 2^(2^-scale)
    pub scale: i32,
    /// Count of zero-valued observations
    pub zero_count: u64,
    /// Offset for the positive bucket range
    pub positive_offset: i32,
    /// Bucket counts for positive values (sparse)
    pub positive_counts: Vec<u64>,
    /// Offset for the negative bucket range
    pub negative_offset: i32,
    /// Bucket counts for negative values (sparse)
    pub negative_counts: Vec<u64>,
    /// Additional attributes for this histogram
    pub attributes: HashMap<String, String>,
    /// Resource ID reference
    pub resource_id: u32,
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
            start_time: None,
            count: 100,
            sum: 500.0,
            min: None,
            max: None,
            bucket_counts: vec![10, 20, 30, 40],
            explicit_bounds: vec![1.0, 5.0, 10.0, 50.0],
            exemplars: vec![],
            attributes: HashMap::new(),
            resource_id: 0,
        };

        assert_eq!(histogram.average(), 5.0); // 500 / 100
    }

    #[test]
    fn test_histogram_average_empty() {
        let metric_id = MetricId::new();
        let histogram = HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::now().unwrap(),
            start_time: None,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            bucket_counts: vec![],
            explicit_bounds: vec![],
            exemplars: vec![],
            attributes: HashMap::new(),
            resource_id: 0,
        };

        assert_eq!(histogram.average(), 0.0);
    }

    #[test]
    fn test_histogram_percentile() {
        let metric_id = MetricId::new();
        let histogram = HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::now().unwrap(),
            start_time: None,
            count: 100,
            sum: 500.0,
            min: None,
            max: None,
            bucket_counts: vec![10, 20, 30, 40],
            explicit_bounds: vec![1.0, 5.0, 10.0, 50.0],
            exemplars: vec![],
            attributes: HashMap::new(),
            resource_id: 0,
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
            start_time: None,
            count: 100,
            sum: 500.0,
            min: None,
            max: None,
            bucket_counts: vec![10, 20, 30, 40],
            explicit_bounds: vec![1.0, 5.0, 10.0, 50.0],
            exemplars: vec![],
            attributes: HashMap::new(),
            resource_id: 0,
        };

        assert_eq!(histogram.percentile(-1.0), None); // Negative
        assert_eq!(histogram.percentile(101.0), None); // > 100
    }
}
