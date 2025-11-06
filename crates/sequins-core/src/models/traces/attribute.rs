use serde::{Deserialize, Serialize};

/// OpenTelemetry attribute value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttributeValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
    StringArray(Vec<String>),
    BoolArray(Vec<bool>),
    IntArray(Vec<i64>),
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

        // AttributeValue variants exist and compile correctly
    }
}
