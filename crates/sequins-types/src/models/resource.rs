use super::AttributeValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Resource represents an entity producing telemetry
///
/// In OTLP, all signals (traces, logs, metrics, profiles) are associated with a Resource
/// that describes the service, host, container, or other entity that produced the data.
///
/// Resources are deduplicated and assigned a unique `resource_id` to avoid storing
/// the same resource attributes repeatedly across signals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    /// Resource attributes (e.g., service.name, host.name, deployment.environment)
    pub attributes: HashMap<String, AttributeValue>,
}

impl Resource {
    /// Create a new resource with attributes
    pub fn new(attributes: HashMap<String, AttributeValue>) -> Self {
        Self { attributes }
    }

    /// Create an empty resource
    pub fn empty() -> Self {
        Self {
            attributes: HashMap::new(),
        }
    }

    /// Get the service name from resource attributes
    pub fn service_name(&self) -> Option<&str> {
        self.attributes.get("service.name").and_then(|v| {
            if let AttributeValue::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
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
    fn test_resource_creation() {
        let mut attrs = HashMap::new();
        attrs.insert(
            "service.name".to_string(),
            AttributeValue::String("test-service".to_string()),
        );
        attrs.insert(
            "service.version".to_string(),
            AttributeValue::String("1.0.0".to_string()),
        );

        let resource = Resource::new(attrs);
        assert_eq!(resource.service_name(), Some("test-service"));
        assert_eq!(
            resource.get("service.version"),
            Some(&AttributeValue::String("1.0.0".to_string()))
        );
    }

    #[test]
    fn test_empty_resource() {
        let resource = Resource::empty();
        assert!(resource.attributes.is_empty());
        assert_eq!(resource.service_name(), None);
    }
}
