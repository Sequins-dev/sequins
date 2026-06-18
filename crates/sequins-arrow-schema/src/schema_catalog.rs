//! Schema catalog: central registry of promoted attribute columns
//!
//! The `SchemaCatalog` drives both the write path (attribute routing)
//! and the query path (attr.* column resolution).  Promoted attributes
//! are stored as first-class typed columns in span/log schemas, while
//! all remaining attributes are CBOR-encoded into the `_overflow_attrs`
//! Map column.
//!
//! The built-in registry is derived from OpenTelemetry Semantic
//! Conventions (v1.29.0).  User configuration can promote additional
//! attributes on top of the semconv baseline.

use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The Arrow data type for a promoted attribute column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeValueType {
    /// Stored as `Utf8View` (for Vortex compatibility)
    String,
    /// Stored as `Int64`
    Int64,
    /// Stored as `Float64`
    Float64,
    /// Stored as `Boolean`
    Boolean,
}

/// Encoding hint for a promoted column (guides Vortex compression).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodingHint {
    /// Low-cardinality string → run-length / dictionary encoded
    DictionaryEncoded,
    /// High-cardinality string → bloom filter indexed
    BloomFiltered,
    /// Numeric value — no special hint needed
    Numeric,
    /// Boolean value
    Boolean,
}

/// A single promoted attribute column.
#[derive(Debug, Clone)]
pub struct PromotedAttribute {
    /// The original OTLP attribute key (e.g. `"http.request.method"`)
    pub key: &'static str,
    /// The Arrow column name (dots replaced with underscores, e.g. `"http_request_method"`)
    pub column_name: &'static str,
    /// The Arrow data type for this column
    pub value_type: AttributeValueType,
    /// Encoding hint for Vortex compression
    pub encoding_hint: EncodingHint,
}

// ---------------------------------------------------------------------------
// Built-in semconv attributes (OTel Semantic Conventions v1.29.0)
// ---------------------------------------------------------------------------

/// Built-in promoted attributes from OTel Semantic Conventions v1.29.0.
///
/// This list covers the most commonly queried HTTP, database, RPC,
/// messaging, and error attributes.  Additional attributes can be promoted
/// via `StorageConfig`.
pub static SEMCONV_ATTRIBUTES: &[PromotedAttribute] = &[
    // -----------------------------------------------------------------------
    // HTTP (stable + migration from http.method/http.status_code)
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "http.request.method",
        column_name: "http_request_method",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "http.method",
        column_name: "http_method",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "http.response.status_code",
        column_name: "http_response_status_code",
        value_type: AttributeValueType::Int64,
        encoding_hint: EncodingHint::Numeric,
    },
    PromotedAttribute {
        key: "http.status_code",
        column_name: "http_status_code",
        value_type: AttributeValueType::Int64,
        encoding_hint: EncodingHint::Numeric,
    },
    PromotedAttribute {
        key: "http.route",
        column_name: "http_route",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "url.path",
        column_name: "url_path",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "url.full",
        column_name: "url_full",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "http.url",
        column_name: "http_url",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "server.address",
        column_name: "server_address",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "server.port",
        column_name: "server_port",
        value_type: AttributeValueType::Int64,
        encoding_hint: EncodingHint::Numeric,
    },
    // -----------------------------------------------------------------------
    // Database
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "db.system",
        column_name: "db_system",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "db.name",
        column_name: "db_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "db.operation.name",
        column_name: "db_operation_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "db.operation",
        column_name: "db_operation",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "db.statement",
        column_name: "db_statement",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "db.collection.name",
        column_name: "db_collection_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    // -----------------------------------------------------------------------
    // RPC
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "rpc.system",
        column_name: "rpc_system",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "rpc.method",
        column_name: "rpc_method",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "rpc.service",
        column_name: "rpc_service",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "rpc.grpc.status_code",
        column_name: "rpc_grpc_status_code",
        value_type: AttributeValueType::Int64,
        encoding_hint: EncodingHint::Numeric,
    },
    // -----------------------------------------------------------------------
    // Messaging
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "messaging.system",
        column_name: "messaging_system",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "messaging.destination.name",
        column_name: "messaging_destination_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "messaging.operation.name",
        column_name: "messaging_operation_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    // -----------------------------------------------------------------------
    // Error / exception
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "error.type",
        column_name: "error_type",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "exception.type",
        column_name: "exception_type",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "exception.message",
        column_name: "exception_message",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    // -----------------------------------------------------------------------
    // Network / peer
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "network.peer.address",
        column_name: "network_peer_address",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "net.peer.name",
        column_name: "net_peer_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "net.peer.port",
        column_name: "net_peer_port",
        value_type: AttributeValueType::Int64,
        encoding_hint: EncodingHint::Numeric,
    },
    // -----------------------------------------------------------------------
    // Service / deployment
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "service.name",
        column_name: "service_name_attr",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "service.version",
        column_name: "service_version",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "deployment.environment.name",
        column_name: "deployment_environment_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    // -----------------------------------------------------------------------
    // Compute / infrastructure
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "k8s.namespace.name",
        column_name: "k8s_namespace_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "k8s.pod.name",
        column_name: "k8s_pod_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
    PromotedAttribute {
        key: "k8s.deployment.name",
        column_name: "k8s_deployment_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    // -----------------------------------------------------------------------
    // Log-specific
    // -----------------------------------------------------------------------
    PromotedAttribute {
        key: "log.iostream",
        column_name: "log_iostream",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::DictionaryEncoded,
    },
    PromotedAttribute {
        key: "log.file.name",
        column_name: "log_file_name",
        value_type: AttributeValueType::String,
        encoding_hint: EncodingHint::BloomFiltered,
    },
];

// ---------------------------------------------------------------------------
// SchemaCatalog
// ---------------------------------------------------------------------------

/// Central registry of promoted attribute columns.
///
/// Used by both write path (attribute routing) and query path (attr.* resolution).
pub struct SchemaCatalog {
    /// All promoted attributes ordered by their position in the schema.
    pub promoted: Vec<PromotedAttribute>,
    /// Fast lookup: OTLP attribute key → index in `promoted`
    key_to_index: HashMap<String, usize>,
    /// Set of all promoted attribute keys for fast write-path filtering
    promoted_keys: HashSet<String>,
}

impl SchemaCatalog {
    /// Create a catalog from an explicit list of promoted attributes.
    ///
    /// Both the dot-notation OTLP key (e.g. `"db.system"`) and the underscore
    /// column name (e.g. `"db_system"`) are indexed so that libraries like
    /// `opentelemetry-appender-tracing` that emit underscore-keyed attributes
    /// still get promoted to the correct column.
    pub fn new(attrs: Vec<PromotedAttribute>) -> Self {
        let mut key_to_index = HashMap::with_capacity(attrs.len() * 2);
        let mut promoted_keys = HashSet::with_capacity(attrs.len() * 2);
        for (idx, attr) in attrs.iter().enumerate() {
            key_to_index.insert(attr.key.to_string(), idx);
            promoted_keys.insert(attr.key.to_string());
            // Also index by the underscore form (column_name) for tracing compat.
            // Use `or_insert` so the first attr wins if two share a column_name.
            if attr.column_name != attr.key {
                key_to_index
                    .entry(attr.column_name.to_string())
                    .or_insert(idx);
                promoted_keys.insert(attr.column_name.to_string());
            }
        }
        SchemaCatalog {
            promoted: attrs,
            key_to_index,
            promoted_keys,
        }
    }

    /// Create the default catalog from built-in semconv attributes.
    pub fn default_catalog() -> Self {
        let attrs: Vec<PromotedAttribute> = SEMCONV_ATTRIBUTES.to_vec();
        Self::new(attrs)
    }

    /// Return `true` if `key` is a promoted attribute.
    #[inline]
    pub fn is_promoted(&self, key: &str) -> bool {
        self.promoted_keys.contains(key)
    }

    /// Return the index of the promoted column for `key`, if any.
    #[inline]
    pub fn column_index(&self, key: &str) -> Option<usize> {
        self.key_to_index.get(key).copied()
    }

    /// Iterate over all promoted attributes.
    pub fn promoted_columns(&self) -> impl Iterator<Item = &PromotedAttribute> {
        self.promoted.iter()
    }

    /// Number of promoted columns.
    pub fn len(&self) -> usize {
        self.promoted.len()
    }

    /// True when there are no promoted columns.
    pub fn is_empty(&self) -> bool {
        self.promoted.is_empty()
    }
}

impl Clone for SchemaCatalog {
    fn clone(&self) -> Self {
        Self::new(self.promoted.clone())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_catalog_is_non_empty() {
        let cat = SchemaCatalog::default_catalog();
        assert!(
            !cat.is_empty(),
            "default catalog should have promoted attributes"
        );
        assert!(
            cat.len() > 10,
            "default catalog should have >10 promoted attributes"
        );
    }

    #[test]
    fn test_is_promoted_known_key() {
        let cat = SchemaCatalog::default_catalog();
        assert!(cat.is_promoted("http.request.method"));
        assert!(cat.is_promoted("db.system"));
        assert!(cat.is_promoted("error.type"));
    }

    #[test]
    fn test_is_promoted_unknown_key() {
        let cat = SchemaCatalog::default_catalog();
        assert!(!cat.is_promoted("custom.attribute.key"));
        assert!(!cat.is_promoted("my.team.feature_flag"));
    }

    #[test]
    fn test_column_index_returns_some_for_promoted() {
        let cat = SchemaCatalog::default_catalog();
        assert!(cat.column_index("http.request.method").is_some());
        assert!(cat.column_index("db.system").is_some());
    }

    #[test]
    fn test_column_index_returns_none_for_non_promoted() {
        let cat = SchemaCatalog::default_catalog();
        assert!(cat.column_index("custom.key").is_none());
    }

    #[test]
    fn test_promoted_column_names_are_snake_case() {
        let cat = SchemaCatalog::default_catalog();
        for attr in cat.promoted_columns() {
            assert!(
                !attr.column_name.contains('.'),
                "column_name should not contain dots: {}",
                attr.column_name
            );
        }
    }

    #[test]
    fn test_catalog_clone() {
        let cat = SchemaCatalog::default_catalog();
        let cloned = cat.clone();
        assert_eq!(cat.len(), cloned.len());
        assert!(cloned.is_promoted("http.request.method"));
    }

    // -- Underscore-key indexing (Bug 2 regression tests) ----------------------

    #[test]
    fn test_underscore_key_is_promoted() {
        // Attributes like `http_request_method` (underscore form of `http.request.method`)
        // must be indexed so that tracing libraries that emit underscore keys still route
        // to the correct promoted column.
        let cat = SchemaCatalog::default_catalog();
        assert!(
            cat.is_promoted("http_request_method"),
            "underscore form 'http_request_method' should be promoted"
        );
        assert!(
            cat.is_promoted("db_system"),
            "underscore form 'db_system' should be promoted"
        );
        assert!(
            cat.is_promoted("error_type"),
            "underscore form 'error_type' should be promoted"
        );
    }

    #[test]
    fn test_underscore_key_column_index_matches_dot_key() {
        // The underscore key must resolve to the SAME column index as the dot key,
        // so that attribute routing puts the value in the correct column.
        let cat = SchemaCatalog::default_catalog();
        let dot_idx = cat.column_index("http.request.method");
        let under_idx = cat.column_index("http_request_method");
        assert!(dot_idx.is_some(), "dot key should have a column index");
        assert_eq!(
            dot_idx, under_idx,
            "underscore key and dot key should map to the same column index"
        );
    }

    #[test]
    fn test_custom_catalog_underscore_indexing() {
        // Verify that custom (user-defined) attributes also get underscore indexing.
        let attrs = vec![PromotedAttribute {
            key: "my.custom.attribute",
            column_name: "my_custom_attribute",
            value_type: AttributeValueType::String,
            encoding_hint: EncodingHint::BloomFiltered,
        }];
        let cat = SchemaCatalog::new(attrs);
        assert!(cat.is_promoted("my.custom.attribute"));
        assert!(
            cat.is_promoted("my_custom_attribute"),
            "underscore form of custom attribute should be promoted"
        );
        assert_eq!(
            cat.column_index("my.custom.attribute"),
            cat.column_index("my_custom_attribute"),
            "both forms should map to the same column index"
        );
    }

    #[test]
    fn test_semconv_attributes_have_unique_keys() {
        let mut seen_keys = HashSet::new();
        for attr in SEMCONV_ATTRIBUTES {
            assert!(
                seen_keys.insert(attr.key),
                "duplicate semconv key: {}",
                attr.key
            );
        }
    }

    #[test]
    fn test_semconv_attributes_have_unique_column_names() {
        let mut seen_names = HashSet::new();
        for attr in SEMCONV_ATTRIBUTES {
            assert!(
                seen_names.insert(attr.column_name),
                "duplicate column name: {}",
                attr.column_name
            );
        }
    }
}
