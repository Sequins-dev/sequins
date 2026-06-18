//! Helpers for mapping OpenTelemetry attribute keys to Arrow column names.
//!
//! Attribute columns are prefixed with `attr_` to avoid collisions with core signal
//! fields, and dots / hyphens / slashes are replaced with underscores so the names
//! are valid identifiers in DataFusion SQL.
//!
//! Both the storage layer (schema_builder) and the query layer (SeQL compiler) need
//! this mapping, so it lives here in the shared types crate.

/// Convert an OTLP attribute key to a promoted Arrow column name.
///
/// Examples:
/// - `"http.status_code"` → `"attr_http_status_code"`
/// - `"custom-attr"` → `"attr_custom_attr"`
/// - `"path/to/resource"` → `"attr_path_to_resource"`
pub fn attribute_key_to_column_name(key: &str) -> String {
    format!("attr_{}", key.replace(['.', '-', '/'], "_"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dot_separated_key() {
        assert_eq!(
            attribute_key_to_column_name("http.status_code"),
            "attr_http_status_code"
        );
    }

    #[test]
    fn test_hyphenated_key() {
        assert_eq!(
            attribute_key_to_column_name("custom-attr"),
            "attr_custom_attr"
        );
    }

    #[test]
    fn test_slash_separated_key() {
        assert_eq!(
            attribute_key_to_column_name("path/to/resource"),
            "attr_path_to_resource"
        );
    }

    #[test]
    fn test_plain_key() {
        assert_eq!(attribute_key_to_column_name("simple"), "attr_simple");
    }
}
