use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Log severity levels from OpenTelemetry
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LogSeverity {
    /// Trace-level logging (most verbose)
    Trace = 1,
    /// Debug-level logging
    Debug = 5,
    /// Informational messages
    Info = 9,
    /// Warning messages
    Warn = 13,
    /// Error messages
    Error = 17,
    /// Fatal/critical error messages
    Fatal = 21,
}

impl LogSeverity {
    /// Parse from OpenTelemetry severity number
    pub fn from_number(n: u8) -> Self {
        match n {
            1..=4 => LogSeverity::Trace,
            5..=8 => LogSeverity::Debug,
            9..=12 => LogSeverity::Info,
            13..=16 => LogSeverity::Warn,
            17..=20 => LogSeverity::Error,
            21..=24 => LogSeverity::Fatal,
            _ => LogSeverity::Info, // Default for unknown
        }
    }

    /// Convert to OpenTelemetry severity number
    pub fn to_number(self) -> u8 {
        self as u8
    }

    /// Get display string
    pub fn as_str(&self) -> &'static str {
        match self {
            LogSeverity::Trace => "TRACE",
            LogSeverity::Debug => "DEBUG",
            LogSeverity::Info => "INFO",
            LogSeverity::Warn => "WARN",
            LogSeverity::Error => "ERROR",
            LogSeverity::Fatal => "FATAL",
        }
    }
}

impl FromStr for LogSeverity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "TRACE" => Ok(LogSeverity::Trace),
            "DEBUG" => Ok(LogSeverity::Debug),
            "INFO" => Ok(LogSeverity::Info),
            "WARN" | "WARNING" => Ok(LogSeverity::Warn),
            "ERROR" => Ok(LogSeverity::Error),
            "FATAL" | "CRITICAL" => Ok(LogSeverity::Fatal),
            _ => Err(format!("Unknown log severity: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_severity_ordering() {
        assert!(LogSeverity::Trace < LogSeverity::Debug);
        assert!(LogSeverity::Debug < LogSeverity::Info);
        assert!(LogSeverity::Info < LogSeverity::Warn);
        assert!(LogSeverity::Warn < LogSeverity::Error);
        assert!(LogSeverity::Error < LogSeverity::Fatal);
    }

    #[test]
    fn test_log_severity_from_number() {
        assert_eq!(LogSeverity::from_number(1), LogSeverity::Trace);
        assert_eq!(LogSeverity::from_number(4), LogSeverity::Trace);
        assert_eq!(LogSeverity::from_number(5), LogSeverity::Debug);
        assert_eq!(LogSeverity::from_number(9), LogSeverity::Info);
        assert_eq!(LogSeverity::from_number(13), LogSeverity::Warn);
        assert_eq!(LogSeverity::from_number(17), LogSeverity::Error);
        assert_eq!(LogSeverity::from_number(21), LogSeverity::Fatal);
        assert_eq!(LogSeverity::from_number(99), LogSeverity::Info); // Unknown defaults to Info
    }

    #[test]
    fn test_log_severity_to_number() {
        assert_eq!(LogSeverity::Trace.to_number(), 1);
        assert_eq!(LogSeverity::Debug.to_number(), 5);
        assert_eq!(LogSeverity::Info.to_number(), 9);
        assert_eq!(LogSeverity::Warn.to_number(), 13);
        assert_eq!(LogSeverity::Error.to_number(), 17);
        assert_eq!(LogSeverity::Fatal.to_number(), 21);
    }

    #[test]
    fn test_log_severity_as_str() {
        assert_eq!(LogSeverity::Trace.as_str(), "TRACE");
        assert_eq!(LogSeverity::Debug.as_str(), "DEBUG");
        assert_eq!(LogSeverity::Info.as_str(), "INFO");
        assert_eq!(LogSeverity::Warn.as_str(), "WARN");
        assert_eq!(LogSeverity::Error.as_str(), "ERROR");
        assert_eq!(LogSeverity::Fatal.as_str(), "FATAL");
    }

    #[test]
    fn test_log_severity_from_str() {
        assert_eq!("TRACE".parse::<LogSeverity>(), Ok(LogSeverity::Trace));
        assert_eq!("trace".parse::<LogSeverity>(), Ok(LogSeverity::Trace));
        assert_eq!("DEBUG".parse::<LogSeverity>(), Ok(LogSeverity::Debug));
        assert_eq!("INFO".parse::<LogSeverity>(), Ok(LogSeverity::Info));
        assert_eq!("WARN".parse::<LogSeverity>(), Ok(LogSeverity::Warn));
        assert_eq!("WARNING".parse::<LogSeverity>(), Ok(LogSeverity::Warn));
        assert_eq!("ERROR".parse::<LogSeverity>(), Ok(LogSeverity::Error));
        assert_eq!("FATAL".parse::<LogSeverity>(), Ok(LogSeverity::Fatal));
        assert_eq!("CRITICAL".parse::<LogSeverity>(), Ok(LogSeverity::Fatal));
        assert!("UNKNOWN".parse::<LogSeverity>().is_err());
    }
}
