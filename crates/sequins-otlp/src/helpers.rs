//! OTLP helper functions for resource/scope registration and attribute conversion

use opentelemetry_proto::tonic::common::v1::{any_value::Value as OtlpValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use sequins_types::models::AttributeValue;
use std::collections::HashMap;

/// Convert OTLP attributes to HashMap of AttributeValues
pub fn convert_attributes(otlp_attrs: Vec<KeyValue>) -> HashMap<String, AttributeValue> {
    otlp_attrs
        .into_iter()
        .filter_map(|kv| {
            kv.value.and_then(|v| {
                v.value.map(|val| {
                    let attr_val = match val {
                        OtlpValue::StringValue(s) => AttributeValue::String(s),
                        OtlpValue::BoolValue(b) => AttributeValue::Bool(b),
                        OtlpValue::IntValue(i) => AttributeValue::Int(i),
                        OtlpValue::DoubleValue(d) => AttributeValue::Double(d),
                        OtlpValue::ArrayValue(arr) => {
                            // Try to extract string array (simplified)
                            let strings: Vec<String> = arr
                                .values
                                .into_iter()
                                .filter_map(|v| {
                                    v.value.and_then(|val| {
                                        if let OtlpValue::StringValue(s) = val {
                                            Some(s)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .collect();
                            if !strings.is_empty() {
                                AttributeValue::StringArray(strings)
                            } else {
                                return None;
                            }
                        }
                        _ => return None,
                    };
                    Some((kv.key, attr_val))
                })
            })
        })
        .flatten()
        .collect()
}

/// Convert resource attributes to simple string HashMap (for registry)
pub fn convert_resource_attributes(attrs: &[KeyValue]) -> HashMap<String, String> {
    attrs
        .iter()
        .filter_map(|kv| {
            kv.value.as_ref().and_then(|v| {
                v.value.as_ref().and_then(|val| {
                    let str_val = match val {
                        OtlpValue::StringValue(s) => s.clone(),
                        OtlpValue::BoolValue(b) => b.to_string(),
                        OtlpValue::IntValue(i) => i.to_string(),
                        OtlpValue::DoubleValue(d) => d.to_string(),
                        _ => return None,
                    };
                    Some((kv.key.clone(), str_val))
                })
            })
        })
        .collect()
}

/// Extract service name from OTLP resource
pub fn extract_service_name(resource: Option<&Resource>) -> String {
    resource
        .and_then(|r| {
            r.attributes.iter().find_map(|kv| {
                if kv.key == "service.name" {
                    kv.value.as_ref().and_then(|v| {
                        if let Some(OtlpValue::StringValue(s)) = &v.value {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            })
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Convert OTLP Resource to Sequins Resource entity
pub fn convert_otlp_resource(resource: Option<&Resource>) -> sequins_types::models::Resource {
    use sequins_types::models::Resource as SequinsResource;

    let attributes = resource
        .map(|r| convert_attributes(r.attributes.clone()))
        .unwrap_or_default();

    SequinsResource::new(attributes)
}

/// Convert OTLP InstrumentationScope to Sequins InstrumentationScope entity
pub fn convert_otlp_scope(
    scope: Option<&opentelemetry_proto::tonic::common::v1::InstrumentationScope>,
) -> sequins_types::models::InstrumentationScope {
    use sequins_types::models::InstrumentationScope as SequinsScope;

    if let Some(s) = scope {
        SequinsScope {
            name: s.name.clone(),
            version: s.version.clone(),
            attributes: convert_attributes(s.attributes.clone()),
        }
    } else {
        SequinsScope {
            name: String::new(),
            version: String::new(),
            attributes: std::collections::HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, InstrumentationScope};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn kv(key: &str, val: OtlpValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(val) }),
        }
    }

    fn kv_no_value(key: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: None,
        }
    }

    // ── convert_attributes ────────────────────────────────────────────────────

    #[test]
    fn test_convert_attributes_string() {
        let attrs = vec![kv("k", OtlpValue::StringValue("v".into()))];
        let map = convert_attributes(attrs);
        assert!(matches!(map["k"], AttributeValue::String(ref s) if s == "v"));
    }

    #[test]
    fn test_convert_attributes_bool() {
        let attrs = vec![kv("flag", OtlpValue::BoolValue(true))];
        let map = convert_attributes(attrs);
        assert!(matches!(map["flag"], AttributeValue::Bool(true)));
    }

    #[test]
    fn test_convert_attributes_int() {
        let attrs = vec![kv("n", OtlpValue::IntValue(42))];
        let map = convert_attributes(attrs);
        assert!(matches!(map["n"], AttributeValue::Int(42)));
    }

    #[test]
    fn test_convert_attributes_double() {
        let attrs = vec![kv("f", OtlpValue::DoubleValue(1.5))];
        let map = convert_attributes(attrs);
        if let AttributeValue::Double(v) = map["f"] {
            assert!((v - 1.5).abs() < 1e-10);
        } else {
            panic!("expected Double");
        }
    }

    #[test]
    fn test_convert_attributes_string_array() {
        let arr = ArrayValue {
            values: vec![
                AnyValue {
                    value: Some(OtlpValue::StringValue("a".into())),
                },
                AnyValue {
                    value: Some(OtlpValue::StringValue("b".into())),
                },
            ],
        };
        let attrs = vec![kv("tags", OtlpValue::ArrayValue(arr))];
        let map = convert_attributes(attrs);
        if let AttributeValue::StringArray(v) = &map["tags"] {
            assert_eq!(v, &["a", "b"]);
        } else {
            panic!("expected StringArray");
        }
    }

    #[test]
    fn test_convert_attributes_empty_value_dropped() {
        // KeyValue with value=None should be filtered out
        let attrs = vec![kv_no_value("missing")];
        let map = convert_attributes(attrs);
        assert!(!map.contains_key("missing"));
    }

    #[test]
    fn test_convert_attributes_bytes_dropped() {
        // BytesValue is not mapped to AttributeValue and should be dropped
        let attrs = vec![kv("blob", OtlpValue::BytesValue(vec![1, 2, 3]))];
        let map = convert_attributes(attrs);
        assert!(!map.contains_key("blob"));
    }

    #[test]
    fn test_convert_attributes_empty_array_dropped() {
        // Empty string array has no strings → filtered out
        let arr = ArrayValue { values: vec![] };
        let attrs = vec![kv("empty", OtlpValue::ArrayValue(arr))];
        let map = convert_attributes(attrs);
        assert!(!map.contains_key("empty"), "empty array should be dropped");
    }

    // ── convert_resource_attributes ───────────────────────────────────────────

    #[test]
    fn test_convert_resource_attributes_stringifies() {
        let attrs = vec![
            kv("str_key", OtlpValue::StringValue("hello".into())),
            kv("int_key", OtlpValue::IntValue(7)),
            kv("bool_key", OtlpValue::BoolValue(false)),
            kv("float_key", OtlpValue::DoubleValue(2.5)),
        ];
        let map = convert_resource_attributes(&attrs);
        assert_eq!(map["str_key"], "hello");
        assert_eq!(map["int_key"], "7");
        assert_eq!(map["bool_key"], "false");
        assert_eq!(map["float_key"], "2.5");
    }

    // ── extract_service_name ──────────────────────────────────────────────────

    #[test]
    fn test_extract_service_name_present() {
        let resource = Resource {
            attributes: vec![kv("service.name", OtlpValue::StringValue("my-svc".into()))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        assert_eq!(extract_service_name(Some(&resource)), "my-svc");
    }

    #[test]
    fn test_extract_service_name_missing() {
        let resource = Resource {
            attributes: vec![kv("other.key", OtlpValue::StringValue("x".into()))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        assert_eq!(extract_service_name(Some(&resource)), "unknown");
    }

    #[test]
    fn test_extract_service_name_none_resource() {
        assert_eq!(extract_service_name(None), "unknown");
    }

    #[test]
    fn test_extract_service_name_non_string_value() {
        let resource = Resource {
            attributes: vec![kv("service.name", OtlpValue::IntValue(42))],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };
        // Non-string service.name falls back to "unknown"
        assert_eq!(extract_service_name(Some(&resource)), "unknown");
    }

    // ── convert_otlp_scope ────────────────────────────────────────────────────

    #[test]
    fn test_convert_otlp_scope_some_with_attrs() {
        let scope = InstrumentationScope {
            name: "my-lib".into(),
            version: "1.0".into(),
            attributes: vec![kv("env", OtlpValue::StringValue("prod".into()))],
            dropped_attributes_count: 0,
        };
        let result = convert_otlp_scope(Some(&scope));
        assert_eq!(result.name, "my-lib");
        assert_eq!(result.version, "1.0");
        assert!(result.attributes.contains_key("env"));
    }

    #[test]
    fn test_convert_otlp_scope_none() {
        let result = convert_otlp_scope(None);
        assert!(result.name.is_empty());
        assert!(result.version.is_empty());
        assert!(result.attributes.is_empty());
    }
}
