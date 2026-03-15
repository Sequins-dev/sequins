use super::{LogId, LogSeverity};
use crate::models::{AttributeValue, SpanId, Timestamp, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Log entry from OTLP logs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Unique identifier for this log entry
    pub id: LogId,
    /// When the log was created (from application perspective)
    pub timestamp: Timestamp,
    /// When the log was received by the collector
    pub observed_timestamp: Timestamp,
    /// Severity number (1-24 per OTLP spec)
    pub severity_number: u8,
    /// Log message body (can be any AttributeValue type per OTLP)
    pub body: AttributeValue,
    /// Additional attributes attached to the log
    pub attributes: HashMap<String, AttributeValue>,
    /// Associated trace ID (if part of a trace)
    pub trace_id: Option<TraceId>,
    /// Associated span ID (if part of a span)
    pub span_id: Option<SpanId>,
    /// Log flags (OTLP spec)
    pub flags: Option<u32>,
    /// Resource ID reference (FK to ResourceRegistry)
    pub resource_id: u32,
    /// Scope ID reference (FK to ScopeRegistry)
    pub scope_id: u32,
}

impl LogEntry {
    /// Check if log is linked to a trace
    pub fn has_trace_context(&self) -> bool {
        self.trace_id.is_some() || self.span_id.is_some()
    }

    /// Check if log has error severity
    pub fn is_error(&self) -> bool {
        let severity = LogSeverity::from_number(self.severity_number);
        severity >= LogSeverity::Error
    }

    /// Get the severity as enum
    pub fn severity(&self) -> LogSeverity {
        LogSeverity::from_number(self.severity_number)
    }

    /// Get attribute value by key
    pub fn get_attribute(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }

    /// Get the body as string if it's a string variant
    pub fn body_as_string(&self) -> Option<&str> {
        if let AttributeValue::String(s) = &self.body {
            Some(s.as_str())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_log(severity: LogSeverity, body: &str) -> LogEntry {
        LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::now().unwrap(),
            observed_timestamp: Timestamp::now().unwrap(),
            severity_number: severity.to_number(),
            body: AttributeValue::String(body.to_string()),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            flags: None,
            resource_id: 0,
            scope_id: 0,
        }
    }

    #[test]
    fn test_log_entry_is_error() {
        let info_log = create_test_log(LogSeverity::Info, "Info message");
        assert!(!info_log.is_error());

        let warn_log = create_test_log(LogSeverity::Warn, "Warning message");
        assert!(!warn_log.is_error());

        let error_log = create_test_log(LogSeverity::Error, "Error message");
        assert!(error_log.is_error());

        let fatal_log = create_test_log(LogSeverity::Fatal, "Fatal message");
        assert!(fatal_log.is_error());
    }

    #[test]
    fn test_log_entry_has_trace_context() {
        let mut log = create_test_log(LogSeverity::Info, "Test");
        assert!(!log.has_trace_context());

        log.trace_id = Some(TraceId::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        ]));
        assert!(log.has_trace_context());

        log.trace_id = None;
        log.span_id = Some(SpanId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8]));
        assert!(log.has_trace_context());
    }

    #[test]
    fn test_log_entry_attributes() {
        let mut log = create_test_log(LogSeverity::Info, "Test");
        log.attributes.insert(
            "http.method".to_string(),
            AttributeValue::String("GET".to_string()),
        );
        log.attributes
            .insert("http.status_code".to_string(), AttributeValue::Int(200));

        assert!(matches!(
            log.get_attribute("http.method"),
            Some(AttributeValue::String(s)) if s == "GET"
        ));
        assert!(matches!(
            log.get_attribute("http.status_code"),
            Some(AttributeValue::Int(200))
        ));
        assert!(log.get_attribute("nonexistent").is_none());
    }

    #[test]
    fn test_log_severity_conversion() {
        let log = create_test_log(LogSeverity::Error, "Test error");
        assert_eq!(log.severity(), LogSeverity::Error);
        assert_eq!(log.severity_number, LogSeverity::Error.to_number());
    }

    #[test]
    fn test_log_body_variants() {
        let mut log = create_test_log(LogSeverity::Info, "String body");
        assert_eq!(log.body_as_string(), Some("String body"));

        log.body = AttributeValue::Int(42);
        assert_eq!(log.body_as_string(), None);
    }
}
