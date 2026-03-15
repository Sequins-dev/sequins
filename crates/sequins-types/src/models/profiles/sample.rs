use super::{ProfileId, ProfileType, StackFrame};
use crate::models::{AttributeValue, SpanId, Timestamp, TraceId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single decomposed profile sample with fully resolved stack frames
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileSample {
    /// ID of the originating profile
    pub profile_id: ProfileId,
    /// When this sample was captured
    pub timestamp: Timestamp,
    /// Service that produced this sample
    pub service_name: String,
    /// Type of profile (CPU, memory, etc.)
    pub profile_type: ProfileType,
    /// Type of value being measured (e.g., "cpu", "alloc_objects")
    pub value_type: String,
    /// Unit of the measured value (e.g., "nanoseconds", "bytes")
    pub value_unit: String,
    /// Measured value (e.g., nanoseconds of CPU time, bytes allocated)
    pub value: i64,
    /// Call stack from leaf (innermost) to root (outermost)
    pub stack: Vec<StackFrame>,
    /// Reference to normalized stack ID (for normalized storage)
    pub stack_id: u64,
    /// Associated trace ID (if available)
    pub trace_id: Option<TraceId>,
    /// Associated span ID (if available)
    pub span_id: Option<SpanId>,
    /// Additional attributes attached to this sample
    pub attributes: HashMap<String, AttributeValue>,
    /// Resource ID reference (FK to ResourceRegistry)
    pub resource_id: u32,
    /// Scope ID reference (FK to ScopeRegistry)
    pub scope_id: u32,
}

impl ProfileSample {
    /// Get the profile ID
    #[must_use]
    pub fn profile_id(&self) -> &ProfileId {
        &self.profile_id
    }

    /// Get the timestamp
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Get the service name
    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Get the profile type
    #[must_use]
    pub fn profile_type(&self) -> ProfileType {
        self.profile_type
    }

    /// Get the value type
    #[must_use]
    pub fn value_type(&self) -> &str {
        &self.value_type
    }

    /// Get the value unit
    #[must_use]
    pub fn value_unit(&self) -> &str {
        &self.value_unit
    }

    /// Get the measured value
    #[must_use]
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Get the stack frames
    #[must_use]
    pub fn stack(&self) -> &[StackFrame] {
        &self.stack
    }

    /// Get the stack ID
    #[must_use]
    pub fn stack_id(&self) -> u64 {
        self.stack_id
    }

    /// Get the trace ID if present
    #[must_use]
    pub fn trace_id(&self) -> Option<TraceId> {
        self.trace_id
    }

    /// Get the span ID if present
    #[must_use]
    pub fn span_id(&self) -> Option<SpanId> {
        self.span_id
    }

    /// Get the attributes
    #[must_use]
    pub fn attributes(&self) -> &HashMap<String, AttributeValue> {
        &self.attributes
    }

    /// Check if this sample is correlated with a trace
    #[must_use]
    pub fn is_correlated_with_trace(&self) -> bool {
        self.trace_id.is_some()
    }

    /// Check if this sample is correlated with a span
    #[must_use]
    pub fn is_correlated_with_span(&self) -> bool {
        self.span_id.is_some()
    }

    /// Get the stack depth (number of frames)
    #[must_use]
    pub fn stack_depth(&self) -> usize {
        self.stack.len()
    }

    /// Get an attribute value by key
    #[must_use]
    pub fn get_attribute(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_sample_getters() {
        let profile_id = ProfileId::from_hex("0123456789abcdef0123456789abcdef").unwrap();
        let trace_id = TraceId::from_hex("0123456789abcdef0123456789abcdef").unwrap();
        let span_id = SpanId::from_hex("0123456789abcdef").unwrap();

        let sample = ProfileSample {
            profile_id,
            timestamp: Timestamp::from_nanos(1000),
            service_name: "test-service".to_string(),
            profile_type: ProfileType::Cpu,
            value_type: "cpu".to_string(),
            value_unit: "nanoseconds".to_string(),
            value: 1000000,
            stack: vec![],
            stack_id: 42,
            trace_id: Some(trace_id),
            span_id: Some(span_id),
            attributes: HashMap::new(),
            resource_id: 0,
            scope_id: 0,
        };

        assert_eq!(sample.profile_id(), &profile_id);
        assert_eq!(sample.timestamp(), Timestamp::from_nanos(1000));
        assert_eq!(sample.service_name(), "test-service");
        assert_eq!(sample.profile_type(), ProfileType::Cpu);
        assert_eq!(sample.value_type(), "cpu");
        assert_eq!(sample.value_unit(), "nanoseconds");
        assert_eq!(sample.value(), 1000000);
        assert_eq!(sample.stack_id(), 42);
        assert_eq!(sample.trace_id(), Some(trace_id));
        assert_eq!(sample.span_id(), Some(span_id));
        assert!(sample.is_correlated_with_trace());
        assert!(sample.is_correlated_with_span());
    }

    #[test]
    fn test_profile_sample_without_correlation() {
        let profile_id = ProfileId::from_hex("0123456789abcdef0123456789abcdef").unwrap();

        let sample = ProfileSample {
            profile_id,
            timestamp: Timestamp::from_nanos(2000),
            service_name: "test".to_string(),
            profile_type: ProfileType::Memory,
            value_type: "alloc_objects".to_string(),
            value_unit: "count".to_string(),
            value: 100,
            stack: vec![],
            stack_id: 1,
            trace_id: None,
            span_id: None,
            attributes: HashMap::new(),
            resource_id: 0,
            scope_id: 0,
        };

        assert!(!sample.is_correlated_with_trace());
        assert!(!sample.is_correlated_with_span());
    }

    #[test]
    fn test_profile_sample_stack_depth() {
        let profile_id = ProfileId::from_hex("0123456789abcdef0123456789abcdef").unwrap();

        let frame1 = StackFrame {
            function_name: "main".to_string(),
            file: Some("main.rs".to_string()),
            line: Some(10),
            module: None,
        };

        let frame2 = StackFrame {
            function_name: "process".to_string(),
            file: Some("lib.rs".to_string()),
            line: Some(42),
            module: None,
        };

        let sample = ProfileSample {
            profile_id,
            timestamp: Timestamp::from_nanos(3000),
            service_name: "test".to_string(),
            profile_type: ProfileType::Cpu,
            value_type: "cpu".to_string(),
            value_unit: "nanoseconds".to_string(),
            value: 500000,
            stack: vec![frame1, frame2],
            stack_id: 10,
            trace_id: None,
            span_id: None,
            attributes: HashMap::new(),
            resource_id: 0,
            scope_id: 0,
        };

        assert_eq!(sample.stack_depth(), 2);
        assert_eq!(sample.stack().len(), 2);
    }

    #[test]
    fn test_profile_sample_attributes() {
        let profile_id = ProfileId::from_hex("0123456789abcdef0123456789abcdef").unwrap();
        let mut attributes = HashMap::new();
        attributes.insert("thread_id".to_string(), AttributeValue::Int(12345));
        attributes.insert(
            "thread_name".to_string(),
            AttributeValue::String("worker-1".to_string()),
        );

        let sample = ProfileSample {
            profile_id,
            timestamp: Timestamp::from_nanos(4000),
            service_name: "test".to_string(),
            profile_type: ProfileType::Cpu,
            value_type: "cpu".to_string(),
            value_unit: "nanoseconds".to_string(),
            value: 250000,
            stack: vec![],
            stack_id: 5,
            trace_id: None,
            span_id: None,
            attributes: attributes.clone(),
            resource_id: 0,
            scope_id: 0,
        };

        assert_eq!(sample.attributes(), &attributes);
        assert_eq!(
            sample.get_attribute("thread_id"),
            Some(&AttributeValue::Int(12345))
        );
        assert_eq!(
            sample.get_attribute("thread_name"),
            Some(&AttributeValue::String("worker-1".to_string()))
        );
        assert_eq!(sample.get_attribute("nonexistent"), None);
    }

    #[test]
    fn test_profile_sample_serde() {
        let profile_id = ProfileId::from_hex("0123456789abcdef0123456789abcdef").unwrap();
        let trace_id = TraceId::from_hex("0123456789abcdef0123456789abcdef").unwrap();

        let sample = ProfileSample {
            profile_id,
            timestamp: Timestamp::from_nanos(5000),
            service_name: "serde-test".to_string(),
            profile_type: ProfileType::Cpu,
            value_type: "cpu".to_string(),
            value_unit: "nanoseconds".to_string(),
            value: 750000,
            stack: vec![],
            stack_id: 100,
            trace_id: Some(trace_id),
            span_id: None,
            attributes: HashMap::new(),
            resource_id: 0,
            scope_id: 0,
        };

        let json = serde_json::to_string(&sample).unwrap();
        let deserialized: ProfileSample = serde_json::from_str(&json).unwrap();

        assert_eq!(sample, deserialized);
    }
}
