use thiserror::Error;

use super::{Duration, Timestamp, TimestampError};

/// Time window specific errors
#[derive(Error, Debug)]
pub enum TimeWindowError {
    #[error("Invalid time window: end ({end}) is before start ({start})")]
    EndBeforeStart { start: Timestamp, end: Timestamp },

    #[error("Timestamp error: {0}")]
    Timestamp(#[from] TimestampError),
}

/// Time window for queries (start/end range)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    start: Timestamp,
    end: Timestamp,
}

impl TimeWindow {
    /// Create new time window (validates end >= start)
    ///
    /// # Errors
    ///
    /// Returns an error if end is before start
    pub fn new(start: Timestamp, end: Timestamp) -> Result<Self, TimeWindowError> {
        if end < start {
            return Err(TimeWindowError::EndBeforeStart { start, end });
        }
        Ok(unsafe { Self::new_unchecked(start, end) })
    }

    /// Create new time window without validation (use with caution)
    ///
    /// # Safety
    ///
    /// Start timestamp must be before end timestamp
    pub unsafe fn new_unchecked(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Get start timestamp
    pub fn start(&self) -> Timestamp {
        self.start
    }

    /// Get end timestamp
    pub fn end(&self) -> Timestamp {
        self.end
    }

    /// Get duration of window
    pub fn duration(&self) -> Duration {
        self.end.duration_since(self.start)
    }

    /// Check if timestamp is within window
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }

    /// Check if two windows overlap
    pub fn overlaps(&self, other: &TimeWindow) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    /// Convenience constructor: last N minutes from now
    ///
    /// # Errors
    ///
    /// Returns an error if the system time is out of range
    pub fn last_minutes(minutes: u32) -> Result<Self, TimeWindowError> {
        let now = Timestamp::now()?;
        let duration = Duration::from_minutes(minutes as i64);
        Ok(unsafe { Self::new_unchecked(now - duration, now) })
    }

    /// Convenience constructor: last hour from now
    ///
    /// # Errors
    ///
    /// Returns an error if the system time is out of range
    pub fn last_hour() -> Result<Self, TimeWindowError> {
        Self::last_minutes(60)
    }

    /// Convenience constructor: last day from now
    ///
    /// # Errors
    ///
    /// Returns an error if the system time is out of range
    pub fn last_day() -> Result<Self, TimeWindowError> {
        Self::last_minutes(24 * 60)
    }

    /// Convenience constructor: last week from now
    ///
    /// # Errors
    ///
    /// Returns an error if the system time is out of range
    pub fn last_week() -> Result<Self, TimeWindowError> {
        Self::last_minutes(7 * 24 * 60)
    }

    /// Convenience constructor: all data within retention period
    ///
    /// # Errors
    ///
    /// Returns an error if the system time is out of range
    pub fn all(retention_hours: u32) -> Result<Self, TimeWindowError> {
        let now = Timestamp::now()?;
        let duration = Duration::from_hours(retention_hours as i64);
        Ok(unsafe { Self::new_unchecked(now - duration, now) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_window_valid() {
        let start = Timestamp::from_secs(1000);
        let end = Timestamp::from_secs(2000);

        let window = TimeWindow::new(start, end).unwrap();
        assert_eq!(window.start(), start);
        assert_eq!(window.end(), end);
        assert_eq!(window.duration().as_secs(), 1000);
    }

    #[test]
    fn test_time_window_invalid() {
        let start = Timestamp::from_secs(2000);
        let end = Timestamp::from_secs(1000);

        let result = TimeWindow::new(start, end);
        assert!(result.is_err());
    }

    #[test]
    fn test_time_window_contains() {
        let start = Timestamp::from_secs(1000);
        let end = Timestamp::from_secs(2000);
        let window = TimeWindow::new(start, end).unwrap();

        assert!(window.contains(Timestamp::from_secs(1500)));
        assert!(window.contains(Timestamp::from_secs(1000))); // Inclusive start
        assert!(window.contains(Timestamp::from_secs(2000))); // Inclusive end
        assert!(!window.contains(Timestamp::from_secs(500)));
        assert!(!window.contains(Timestamp::from_secs(2500)));
    }

    #[test]
    fn test_time_window_overlaps() {
        let w1 = TimeWindow::new(Timestamp::from_secs(1000), Timestamp::from_secs(2000)).unwrap();

        let w2 = TimeWindow::new(Timestamp::from_secs(1500), Timestamp::from_secs(2500)).unwrap();

        assert!(w1.overlaps(&w2));
        assert!(w2.overlaps(&w1));

        let w3 = TimeWindow::new(Timestamp::from_secs(3000), Timestamp::from_secs(4000)).unwrap();

        assert!(!w1.overlaps(&w3));
        assert!(!w3.overlaps(&w1));
    }

    #[test]
    fn test_time_window_last_minutes() {
        let window = TimeWindow::last_minutes(5).unwrap();
        let duration = window.duration();

        // Should be approximately 5 minutes (300 seconds)
        // Allow small difference due to time passing during test
        assert!((duration.as_secs() - 300).abs() < 2);
    }

    #[test]
    fn test_time_window_convenience_constructors() {
        let hour = TimeWindow::last_hour().unwrap();
        assert!((hour.duration().as_minutes() - 60).abs() < 1);

        let day = TimeWindow::last_day().unwrap();
        assert!((day.duration().as_hours() - 24).abs() < 1);

        let week = TimeWindow::last_week().unwrap();
        assert!((week.duration().as_hours() - (7 * 24)).abs() < 1);
    }
}
