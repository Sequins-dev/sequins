use serde::{Deserialize, Serialize};

/// OpenTelemetry attribute value
///
/// Supports all OTLP attribute value types including bytes, key-value lists,
/// and mixed-type arrays (per OTLP spec).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttributeValue {
    /// String value
    String(String),
    /// Boolean value
    Bool(bool),
    /// 64-bit integer value
    Int(i64),
    /// 64-bit floating point value
    Double(f64),
    /// Byte array value
    Bytes(Vec<u8>),
    /// Key-value list (nested attributes)
    KvList(Vec<(String, AttributeValue)>),
    /// Array of attribute values (can be mixed types per OTLP spec)
    Array(Vec<AttributeValue>),
    /// Array of strings (legacy, kept for backward compatibility)
    StringArray(Vec<String>),
    /// Array of booleans (legacy, kept for backward compatibility)
    BoolArray(Vec<bool>),
    /// Array of integers (legacy, kept for backward compatibility)
    IntArray(Vec<i64>),
    /// Array of floating point values (legacy, kept for backward compatibility)
    DoubleArray(Vec<f64>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_value_variants() {
        let _string_attr = AttributeValue::String("test".to_string());
        let _bool_attr = AttributeValue::Bool(true);
        let _int_attr = AttributeValue::Int(42);
        let _double_attr = AttributeValue::Double(2.5);
        let _bytes_attr = AttributeValue::Bytes(vec![1, 2, 3]);
        let _kvlist_attr = AttributeValue::KvList(vec![(
            "key".to_string(),
            AttributeValue::String("value".to_string()),
        )]);
        let _array_attr = AttributeValue::Array(vec![
            AttributeValue::Int(1),
            AttributeValue::String("test".to_string()),
        ]);

        // AttributeValue variants exist and compile correctly
    }

    #[test]
    fn test_bytes_variant() {
        let bytes = vec![0x01, 0x02, 0x03, 0x04];
        let attr = AttributeValue::Bytes(bytes.clone());
        assert_eq!(attr, AttributeValue::Bytes(bytes));
    }

    #[test]
    fn test_kvlist_variant() {
        let kvlist = vec![
            (
                "name".to_string(),
                AttributeValue::String("test".to_string()),
            ),
            ("count".to_string(), AttributeValue::Int(42)),
        ];
        let attr = AttributeValue::KvList(kvlist.clone());
        assert_eq!(attr, AttributeValue::KvList(kvlist));
    }

    #[test]
    fn test_mixed_array_variant() {
        let array = vec![
            AttributeValue::Int(1),
            AttributeValue::String("two".to_string()),
            AttributeValue::Bool(true),
            AttributeValue::Double(4.0),
        ];
        let attr = AttributeValue::Array(array.clone());
        assert_eq!(attr, AttributeValue::Array(array));
    }
}
