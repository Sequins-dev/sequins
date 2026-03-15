use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::{Add, Sub};

/// Nanosecond-precision duration (strongly typed)
///
/// Supports human-readable serialization formats like "5m", "1h", "7d", "30s"
/// when deserializing from string formats (KDL, TOML, YAML, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration(i64);

impl Duration {
    /// Create duration from nanoseconds
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Create duration from seconds
    pub fn from_secs(secs: i64) -> Self {
        Self(secs * 1_000_000_000)
    }

    /// Create duration from milliseconds
    pub fn from_millis(millis: i64) -> Self {
        Self(millis * 1_000_000)
    }

    /// Create duration from minutes
    pub fn from_minutes(minutes: i64) -> Self {
        Self(minutes * 60 * 1_000_000_000)
    }

    /// Create duration from hours
    pub fn from_hours(hours: i64) -> Self {
        Self(hours * 3600 * 1_000_000_000)
    }

    /// Create duration from days
    pub fn from_days(days: i64) -> Self {
        Self(days * 24 * 3600 * 1_000_000_000)
    }

    /// Get nanoseconds
    pub fn as_nanos(&self) -> i64 {
        self.0
    }

    /// Get seconds
    pub fn as_secs(&self) -> i64 {
        self.0 / 1_000_000_000
    }

    /// Get milliseconds
    pub fn as_millis(&self) -> i64 {
        self.0 / 1_000_000
    }

    /// Get minutes
    pub fn as_minutes(&self) -> i64 {
        self.0 / (60 * 1_000_000_000)
    }

    /// Get hours
    pub fn as_hours(&self) -> i64 {
        self.0 / (3600 * 1_000_000_000)
    }
}

impl Add for Duration {
    type Output = Duration;

    fn add(self, other: Duration) -> Self::Output {
        Duration(self.0 + other.0)
    }
}

impl Sub for Duration {
    type Output = Duration;

    fn sub(self, other: Duration) -> Self::Output {
        Duration(self.0 - other.0)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

// Custom serde implementations to support human-readable duration strings
impl Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Always serialize as i64 nanoseconds for precision
        // Human-readable formats can still parse duration strings on input
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurationVisitor;

        impl<'de> serde::de::Visitor<'de> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a duration string like \"5m\", \"1h\", \"7d\", \"30s\" or an integer nanoseconds")
            }

            fn visit_str<E>(self, value: &str) -> Result<Duration, E>
            where
                E: serde::de::Error,
            {
                parse_duration_string(value).map_err(serde::de::Error::custom)
            }

            fn visit_i64<E>(self, value: i64) -> Result<Duration, E>
            where
                E: serde::de::Error,
            {
                Ok(Duration::from_nanos(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Duration, E>
            where
                E: serde::de::Error,
            {
                Ok(Duration::from_nanos(value as i64))
            }
        }

        deserializer.deserialize_any(DurationVisitor)
    }
}

/// Parse duration string like "5m", "1h", "7d", "30s", "100ns"
/// If no unit is provided, the value is interpreted as nanoseconds
fn parse_duration_string(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration string".to_string());
    }

    // Find the first letter (the start of the unit)
    match s.find(|c: char| c.is_alphabetic()) {
        Some(pos) => {
            // Split into number and unit
            let (number_str, unit) = s.split_at(pos);
            let number: i64 = number_str
                .parse()
                .map_err(|e| format!("Invalid duration number '{}': {}", number_str, e))?;

            match unit {
                "ns" => Ok(Duration::from_nanos(number)),
                "s" => Ok(Duration::from_secs(number)),
                "m" => Ok(Duration::from_minutes(number)),
                "h" => Ok(Duration::from_hours(number)),
                "d" => Ok(Duration::from_hours(number * 24)),
                _ => Err(format!(
                    "Unknown duration unit: {}. Supported units: ns, s, m, h, d",
                    unit
                )),
            }
        }
        None => {
            // No unit, parse as nanoseconds
            let number: i64 = s
                .parse()
                .map_err(|e| format!("Invalid duration number '{}': {}", s, e))?;
            Ok(Duration::from_nanos(number))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_creation() {
        let d = Duration::from_secs(60);
        assert_eq!(d.as_secs(), 60);
        assert_eq!(d.as_minutes(), 1);
    }

    #[test]
    fn test_duration_from_minutes() {
        let d = Duration::from_minutes(5);
        assert_eq!(d.as_minutes(), 5);
        assert_eq!(d.as_secs(), 300);
    }

    #[test]
    fn test_duration_from_hours() {
        let d = Duration::from_hours(2);
        assert_eq!(d.as_hours(), 2);
        assert_eq!(d.as_minutes(), 120);
        assert_eq!(d.as_secs(), 7200);
    }

    #[test]
    fn test_duration_arithmetic() {
        let d1 = Duration::from_secs(100);
        let d2 = Duration::from_secs(50);

        let sum = d1 + d2;
        assert_eq!(sum.as_secs(), 150);

        let diff = d1 - d2;
        assert_eq!(diff.as_secs(), 50);
    }
}
