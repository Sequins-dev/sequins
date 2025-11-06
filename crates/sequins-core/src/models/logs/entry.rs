use super::{LogId, LogSeverity};
use crate::models::{AttributeValue, SpanId, Timestamp, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Log entry from OTLP logs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: LogId,
    pub timestamp: Timestamp,
    pub observed_timestamp: Timestamp,
    pub service_name: String,
    pub severity: LogSeverity,
    pub body: String,
    pub attributes: HashMap<String, AttributeValue>,
    pub trace_id: Option<TraceId>,
    pub span_id: Option<SpanId>,
    pub resource: HashMap<String, String>,
}

impl LogEntry {
    /// Check if log is linked to a trace
    pub fn has_trace_context(&self) -> bool {
        self.trace_id.is_some() || self.span_id.is_some()
    }

    /// Check if log has error severity
    pub fn is_error(&self) -> bool {
        self.severity >= LogSeverity::Error
    }

    /// Get attribute value by key
    pub fn get_attribute(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }

    /// Get resource attribute by key
    pub fn get_resource(&self, key: &str) -> Option<&str> {
        self.resource.get(key).map(|s| s.as_str())
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
            service_name: "test-service".to_string(),
            severity,
            body: body.to_string(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
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
    fn test_log_entry_resource() {
        let mut log = create_test_log(LogSeverity::Info, "Test");
        log.resource
            .insert("service.version".to_string(), "1.0.0".to_string());
        log.resource.insert(
            "deployment.environment".to_string(),
            "production".to_string(),
        );

        assert_eq!(log.get_resource("service.version"), Some("1.0.0"));
        assert_eq!(
            log.get_resource("deployment.environment"),
            Some("production")
        );
        assert_eq!(log.get_resource("nonexistent"), None);
    }
}
