use serde::{Deserialize, Serialize};

use super::{Duration, Timestamp};

/// Represents either an absolute time range or a sliding window
///
/// This type is used in filters to support both:
/// - Fixed time ranges (e.g., "Jan 1 to Jan 31")
/// - Sliding windows (e.g., "last 5 minutes")
///
/// Sliding windows are particularly useful for real-time dashboards where
/// the view should automatically exclude old data as time progresses.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TimeRange {
    /// Absolute time range with specific start/end timestamps
    ///
    /// Both boundaries are optional:
    /// - `None` start means "from the beginning of time"
    /// - `None` end means "to the end of time"
    Absolute {
        /// Start timestamp (inclusive)
        #[serde(skip_serializing_if = "Option::is_none")]
        start: Option<Timestamp>,
        /// End timestamp (inclusive)
        #[serde(skip_serializing_if = "Option::is_none")]
        end: Option<Timestamp>,
    },

    /// Sliding window: from (now - duration) to now
    ///
    /// This range continuously updates as time progresses.
    /// The reference time is determined when `contains_at()` is called.
    SlidingWindow {
        /// How far back to look from "now"
        duration: Duration,
    },
}

impl TimeRange {
    /// Create an absolute time range
    pub fn absolute(start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        TimeRange::Absolute { start, end }
    }

    /// Create a sliding window
    pub fn sliding(duration: Duration) -> Self {
        TimeRange::SlidingWindow { duration }
    }

    /// Check if a timestamp falls within this range at a specific time
    ///
    /// For absolute ranges, checks against fixed boundaries.
    /// For sliding windows, checks against (now - duration) to now.
    ///
    /// Use this method when you need precise control over the current time,
    /// such as in sliding window cleanup tasks where all items in a batch
    /// should use the same reference time.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp to check
    /// * `now` - The reference time to use for "now" (important for sliding windows)
    ///
    /// # Examples
    ///
    /// ```
    /// use sequins_types::models::{Duration, Timestamp};
    /// use sequins_types::models::time::TimeRange;
    ///
    /// let range = TimeRange::sliding(Duration::from_minutes(5));
    /// let now = Timestamp::from_secs(1000);
    /// let recent = Timestamp::from_secs(996); // 4 seconds ago (within 5 minutes)
    /// let old = Timestamp::from_secs(400);    // 600 seconds ago (outside 5 minutes)
    ///
    /// assert!(range.contains_at(recent, now));
    /// assert!(!range.contains_at(old, now));
    /// ```
    pub fn contains_at(&self, timestamp: Timestamp, now: Timestamp) -> bool {
        match self {
            TimeRange::Absolute { start, end } => {
                let after_start = start.map_or(true, |s| timestamp >= s);
                let before_end = end.map_or(true, |e| timestamp <= e);
                after_start && before_end
            }
            TimeRange::SlidingWindow { duration } => {
                let window_start = now - *duration;
                timestamp >= window_start && timestamp <= now
            }
        }
    }

    /// Check if a timestamp falls within this range (using current time)
    ///
    /// Convenience method that gets the current timestamp and calls `contains_at`.
    /// Use this for simple validation; use `contains_at` when you need precise
    /// control over the reference time.
    ///
    /// # Examples
    ///
    /// ```
    /// use sequins_types::models::{Duration, Timestamp};
    /// use sequins_types::models::time::TimeRange;
    ///
    /// let range = TimeRange::absolute(
    ///     Some(Timestamp::from_secs(1000)),
    ///     Some(Timestamp::from_secs(2000))
    /// );
    ///
    /// assert!(range.contains(Timestamp::from_secs(1500)));
    /// assert!(!range.contains(Timestamp::from_secs(500)));
    /// ```
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        // For now, use unwrap - we'll address this if needed
        let now = Timestamp::now().expect("Failed to get current time");
        self.contains_at(timestamp, now)
    }

    /// Check if this is a sliding window range
    ///
    /// Returns `true` if this range will change over time,
    /// requiring periodic cleanup in SelectionModel.
    pub fn is_sliding(&self) -> bool {
        matches!(self, TimeRange::SlidingWindow { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_absolute_range_both_bounds() {
        let range = TimeRange::absolute(
            Some(Timestamp::from_secs(1000)),
            Some(Timestamp::from_secs(2000)),
        );

        let now = Timestamp::from_secs(5000); // Irrelevant for absolute ranges

        assert!(range.contains_at(Timestamp::from_secs(1000), now));
        assert!(range.contains_at(Timestamp::from_secs(1500), now));
        assert!(range.contains_at(Timestamp::from_secs(2000), now));
        assert!(!range.contains_at(Timestamp::from_secs(999), now));
        assert!(!range.contains_at(Timestamp::from_secs(2001), now));
    }

    #[test]
    fn test_absolute_range_no_start() {
        let range = TimeRange::absolute(None, Some(Timestamp::from_secs(2000)));

        let now = Timestamp::from_secs(5000);

        assert!(range.contains_at(Timestamp::from_secs(0), now));
        assert!(range.contains_at(Timestamp::from_secs(1000), now));
        assert!(range.contains_at(Timestamp::from_secs(2000), now));
        assert!(!range.contains_at(Timestamp::from_secs(2001), now));
    }

    #[test]
    fn test_absolute_range_no_end() {
        let range = TimeRange::absolute(Some(Timestamp::from_secs(1000)), None);

        let now = Timestamp::from_secs(5000);

        assert!(!range.contains_at(Timestamp::from_secs(999), now));
        assert!(range.contains_at(Timestamp::from_secs(1000), now));
        assert!(range.contains_at(Timestamp::from_secs(5000), now));
        assert!(range.contains_at(Timestamp::from_secs(10000), now));
    }

    #[test]
    fn test_absolute_range_no_bounds() {
        let range = TimeRange::absolute(None, None);

        let now = Timestamp::from_secs(5000);

        assert!(range.contains_at(Timestamp::from_secs(0), now));
        assert!(range.contains_at(Timestamp::from_secs(5000), now));
        assert!(range.contains_at(Timestamp::from_secs(10000), now));
    }

    #[test]
    fn test_sliding_window() {
        let range = TimeRange::sliding(Duration::from_secs(300)); // 5 minutes

        let now = Timestamp::from_secs(1000);

        // Within window
        assert!(range.contains_at(Timestamp::from_secs(1000), now)); // Exactly now
        assert!(range.contains_at(Timestamp::from_secs(800), now)); // 200s ago
        assert!(range.contains_at(Timestamp::from_secs(700), now)); // 300s ago (boundary)

        // Outside window
        assert!(!range.contains_at(Timestamp::from_secs(699), now)); // Just outside
        assert!(!range.contains_at(Timestamp::from_secs(500), now)); // Way outside
        assert!(!range.contains_at(Timestamp::from_secs(1001), now)); // Future
    }

    #[test]
    fn test_sliding_window_moves_with_time() {
        let range = TimeRange::sliding(Duration::from_secs(100));

        let early_now = Timestamp::from_secs(500);
        let later_now = Timestamp::from_secs(600);

        let timestamp = Timestamp::from_secs(450);

        // At early_now (500), timestamp (450) is within window (400-500)
        assert!(range.contains_at(timestamp, early_now));

        // At later_now (600), same timestamp (450) is outside window (500-600)
        assert!(!range.contains_at(timestamp, later_now));
    }

    #[test]
    fn test_is_sliding() {
        let absolute = TimeRange::absolute(
            Some(Timestamp::from_secs(1000)),
            Some(Timestamp::from_secs(2000)),
        );
        let sliding = TimeRange::sliding(Duration::from_secs(300));

        assert!(!absolute.is_sliding());
        assert!(sliding.is_sliding());
    }
}
