use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Sub};
use thiserror::Error;

use super::Duration;

/// Timestamp specific errors
#[derive(Error, Debug)]
pub enum TimestampError {
    #[error("System time out of range")]
    SystemTimeOutOfRange,

    #[error("Timestamp {0} is out of representable range")]
    OutOfRange(i64),
}

/// Nanosecond-precision Unix timestamp (strongly typed)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Create timestamp from nanoseconds since Unix epoch
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Create timestamp from seconds since Unix epoch
    pub fn from_secs(secs: i64) -> Self {
        Self(secs * 1_000_000_000)
    }

    /// Create timestamp from milliseconds since Unix epoch
    pub fn from_millis(millis: i64) -> Self {
        Self(millis * 1_000_000)
    }

    /// Get current timestamp
    ///
    /// # Errors
    ///
    /// Returns an error if the system time is outside the representable range
    pub fn now() -> Result<Self, TimestampError> {
        let now = Utc::now();
        let nanos = now
            .timestamp_nanos_opt()
            .ok_or(TimestampError::SystemTimeOutOfRange)?;
        Ok(Self::from_nanos(nanos))
    }

    /// Get nanoseconds since Unix epoch
    pub fn as_nanos(&self) -> i64 {
        self.0
    }

    /// Get seconds since Unix epoch
    pub fn as_secs(&self) -> i64 {
        self.0 / 1_000_000_000
    }

    /// Get milliseconds since Unix epoch
    pub fn as_millis(&self) -> i64 {
        self.0 / 1_000_000
    }

    /// Format as ISO 8601 string
    ///
    /// # Errors
    ///
    /// Returns an error if the timestamp is outside the representable range
    pub fn as_datetime(&self) -> Result<String, TimestampError> {
        let dt = DateTime::from_timestamp(self.as_secs(), (self.0 % 1_000_000_000) as u32)
            .ok_or(TimestampError::OutOfRange(self.0))?;
        Ok(dt.to_rfc3339())
    }

    /// Duration since another timestamp
    pub fn duration_since(&self, earlier: Timestamp) -> Duration {
        Duration::from_nanos(self.0 - earlier.0)
    }
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, duration: Duration) -> Self::Output {
        Timestamp(self.0 + duration.as_nanos())
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, duration: Duration) -> Self::Output {
        Timestamp(self.0 - duration.as_nanos())
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format as ISO 8601 if possible, otherwise show nanoseconds
        match self.as_datetime() {
            Ok(datetime) => write!(f, "{}", datetime),
            Err(_) => write!(f, "{}ns", self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_creation() {
        let ts = Timestamp::from_secs(1000);
        assert_eq!(ts.as_secs(), 1000);
        assert_eq!(ts.as_nanos(), 1000 * 1_000_000_000);
    }

    #[test]
    fn test_timestamp_from_millis() {
        let ts = Timestamp::from_millis(5000);
        assert_eq!(ts.as_millis(), 5000);
        assert_eq!(ts.as_secs(), 5);
    }

    #[test]
    fn test_timestamp_arithmetic() {
        let ts = Timestamp::from_secs(1000);
        let duration = Duration::from_secs(100);

        let later = ts + duration;
        assert_eq!(later.as_secs(), 1100);

        let earlier = ts - duration;
        assert_eq!(earlier.as_secs(), 900);
    }

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = Timestamp::from_secs(1000);
        let ts2 = Timestamp::from_secs(2000);

        assert!(ts1 < ts2);
        assert!(ts2 > ts1);
        assert_eq!(ts1, ts1);
    }

    #[test]
    fn test_timestamp_duration_since() {
        let earlier = Timestamp::from_secs(1000);
        let later = Timestamp::from_secs(1100);

        let duration = later.duration_since(earlier);
        assert_eq!(duration.as_secs(), 100);
    }
}
