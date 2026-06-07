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

    /// Get percentile from histogram buckets (approximate).
    ///
    /// Returns the upper bound of the bucket that contains the Nth observation
    /// (where N = `count * p / 100`, rounded up to at least 1).  When the
    /// percentile falls in the implicit overflow bucket (`> last_bound`), the
    /// last explicit bound is returned as a conservative lower-bound estimate.
    ///
    /// Returns `None` for out-of-range `p` (< 0 or > 100) or empty histogram.
    pub fn percentile(&self, p: f64) -> Option<f64> {
        if !(0.0..=100.0).contains(&p) || self.count == 0 {
            return None;
        }

        // Round up so that p=0 targets the 1st observation rather than 0,
        // which would immediately match even an empty bucket.
        let target_count = ((self.count as f64 * p / 100.0).ceil() as u64).max(1);
        let mut cumulative = 0u64;

        for (i, &bucket_count) in self.bucket_counts.iter().enumerate() {
            cumulative += bucket_count;
            if cumulative >= target_count {
                // `explicit_bounds` has one fewer entry than `bucket_counts`
                // (the final "overflow" bucket has no upper bound).  Fall
                // through to `last()` instead of returning None.
                if let Some(&bound) = self.explicit_bounds.get(i) {
                    return Some(bound);
                }
                break;
            }
        }

        // Percentile falls in (or past) the overflow bucket — best estimate is
        // the last explicit bound.
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

    fn make_histogram(bucket_counts: Vec<u64>, explicit_bounds: Vec<f64>) -> HistogramDataPoint {
        let count = bucket_counts.iter().sum();
        HistogramDataPoint {
            metric_id: MetricId::new(),
            timestamp: Timestamp::now().unwrap(),
            start_time: None,
            count,
            sum: 0.0,
            min: None,
            max: None,
            bucket_counts,
            explicit_bounds,
            exemplars: vec![],
            attributes: HashMap::new(),
            resource_id: 0,
        }
    }

    #[test]
    fn test_histogram_percentile_no_overflow() {
        // Proper OTLP histogram: 4 bounds, 5 buckets (last is overflow with 0 items).
        // counts = [10, 20, 30, 40, 0], bounds = [1.0, 5.0, 10.0, 50.0]
        // total = 100; no overflow observations.
        let h = make_histogram(vec![10, 20, 30, 40, 0], vec![1.0, 5.0, 10.0, 50.0]);

        // P10 → ceil(100 * 10/100) = 10th obs → bucket 0 (cumulative=10) → 1.0
        assert_eq!(h.percentile(10.0), Some(1.0));
        // P50 → ceil(50) = 50th obs → bucket 2 (cumulative=60 >= 50) → 10.0
        assert_eq!(h.percentile(50.0), Some(10.0));
        // P90 → ceil(90) = 90th obs → bucket 3 (cumulative=100 >= 90) → 50.0
        assert_eq!(h.percentile(90.0), Some(50.0));
        // P100 → ceil(100) = 100th obs → bucket 3 (cumulative=100 >= 100) → 50.0
        assert_eq!(h.percentile(100.0), Some(50.0));
        // P0 → target=max(ceil(0),1)=1 → bucket 0 (cumulative=10 >= 1) → 1.0
        assert_eq!(h.percentile(0.0), Some(1.0));
    }

    #[test]
    fn test_histogram_percentile_with_overflow() {
        // 4 bounds, 5 buckets — the 5th bucket has 10 observations above 50.0.
        // total = 110: 10+20+30+40+10
        let h = make_histogram(vec![10, 20, 30, 40, 10], vec![1.0, 5.0, 10.0, 50.0]);

        // P90 → ceil(110 * 90/100) = ceil(99) = 99th obs → bucket 3 (cumulative=100 >= 99) → 50.0
        assert_eq!(h.percentile(90.0), Some(50.0));
        // P99 → ceil(110 * 99/100) = ceil(108.9) = 109th obs → overflow bucket → last bound = 50.0
        assert_eq!(h.percentile(99.0), Some(50.0));
        // P100 → ceil(110) = 110th obs → overflow bucket → last bound = 50.0
        assert_eq!(h.percentile(100.0), Some(50.0));
    }

    #[test]
    fn test_histogram_percentile_invalid() {
        let h = make_histogram(vec![10, 20, 30, 40], vec![1.0, 5.0, 10.0, 50.0]);
        assert_eq!(h.percentile(-1.0), None); // Negative
        assert_eq!(h.percentile(101.0), None); // > 100
    }
}
