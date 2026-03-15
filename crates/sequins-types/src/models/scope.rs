use super::AttributeValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// InstrumentationScope represents the instrumentation library that produced telemetry
///
/// In OTLP, each ScopeSpans/ScopeLogs/ScopeMetrics/ScopeProfiles carries information about
/// the instrumentation library (name, version, attributes) that generated the data.
///
/// Scopes are deduplicated and assigned a unique `scope_id` to avoid storing
/// the same scope information repeatedly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentationScope {
    /// Name of the instrumentation library (e.g., "io.opentelemetry.javaagent.servlet")
    pub name: String,
    /// Version of the instrumentation library (e.g., "1.15.0")
    pub version: String,
    /// Additional attributes attached to the scope
    pub attributes: HashMap<String, AttributeValue>,
}

impl InstrumentationScope {
    /// Create a new instrumentation scope
    pub fn new(name: String, version: String, attributes: HashMap<String, AttributeValue>) -> Self {
        Self {
            name,
            version,
            attributes,
        }
    }

    /// Create a scope with just name and version, no attributes
    pub fn simple(name: String, version: String) -> Self {
        Self {
            name,
            version,
            attributes: HashMap::new(),
        }
    }

    /// Get an attribute by key
    pub fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.attributes.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_creation() {
        let mut attrs = HashMap::new();
        attrs.insert(
            "telemetry.sdk.name".to_string(),
            AttributeValue::String("opentelemetry".to_string()),
        );

        let scope = InstrumentationScope::new(
            "io.opentelemetry.javaagent.servlet".to_string(),
            "1.15.0".to_string(),
            attrs,
        );

        assert_eq!(scope.name, "io.opentelemetry.javaagent.servlet");
        assert_eq!(scope.version, "1.15.0");
        assert_eq!(
            scope.get("telemetry.sdk.name"),
            Some(&AttributeValue::String("opentelemetry".to_string()))
        );
    }

    #[test]
    fn test_simple_scope() {
        let scope = InstrumentationScope::simple("test.library".to_string(), "2.0.0".to_string());
        assert_eq!(scope.name, "test.library");
        assert_eq!(scope.version, "2.0.0");
        assert!(scope.attributes.is_empty());
    }
}
