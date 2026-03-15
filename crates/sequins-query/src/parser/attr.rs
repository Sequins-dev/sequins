use crate::ast::{AttrScope, FieldRef};
use crate::parser::lexer::identifier;
use winnow::combinator::{alt, preceded};
use winnow::token::literal;
use winnow::{ModalResult, Parser};

/// Known signal fields for each signal type (used for auto-resolution).
/// Field names match OTLP proto field names exactly.
const SPAN_FIELDS: &[&str] = &[
    "trace_id",
    "span_id",
    "parent_span_id",
    "service_name",
    "name",
    "kind",
    "start_time_unix_nano",
    "end_time_unix_nano",
    "duration_ns",
    "status",
];

const LOG_FIELDS: &[&str] = &[
    "log_id",
    "time_unix_nano",
    "observed_time_unix_nano",
    "service_name",
    "severity_text",
    "severity_number",
    "body",
    "trace_id",
    "span_id",
];

const METRIC_FIELDS: &[&str] = &["metric_id", "timestamp", "value"];

const SAMPLE_FIELDS: &[&str] = &["id", "timestamp", "service_name", "profile_type"];

/// Parse a field reference, resolving scope from prefix notation:
/// - `resource.X` → Resource
/// - `scope.X` → Scope
/// - `attr.X` → Attribute
/// - `X` where X is a known signal field → Signal
/// - `X` otherwise → Auto
pub fn field_ref(input: &mut &str) -> ModalResult<FieldRef> {
    // Try explicit scope prefixes first
    let explicit = alt((
        preceded(literal("resource."), identifier).map(|name: &str| FieldRef {
            scope: AttrScope::Resource,
            name: name.to_string(),
        }),
        preceded(literal("scope."), identifier).map(|name: &str| FieldRef {
            scope: AttrScope::Scope,
            name: name.to_string(),
        }),
        preceded(literal("attr."), identifier).map(|name: &str| FieldRef {
            scope: AttrScope::Attribute,
            name: name.to_string(),
        }),
    ));

    // Bare name — check against known signal fields
    let bare = identifier.map(|name: &str| {
        let scope = if is_signal_field(name) {
            AttrScope::Signal
        } else {
            AttrScope::Auto
        };
        FieldRef {
            scope,
            name: name.to_string(),
        }
    });

    alt((explicit, bare)).parse_next(input)
}

/// Returns true if `name` is a well-known field for any signal type
fn is_signal_field(name: &str) -> bool {
    SPAN_FIELDS.contains(&name)
        || LOG_FIELDS.contains(&name)
        || METRIC_FIELDS.contains(&name)
        || SAMPLE_FIELDS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_signal_field() {
        let f = field_ref.parse("duration_ns").unwrap();
        assert_eq!(f.scope, AttrScope::Signal);
        assert_eq!(f.name, "duration_ns");
    }

    #[test]
    fn parse_resource_field() {
        let f = field_ref.parse("resource.service.name").unwrap();
        assert_eq!(f.scope, AttrScope::Resource);
        assert_eq!(f.name, "service.name");
    }

    #[test]
    fn parse_attr_field() {
        let f = field_ref.parse("attr.http.method").unwrap();
        assert_eq!(f.scope, AttrScope::Attribute);
        assert_eq!(f.name, "http.method");
    }

    #[test]
    fn parse_scope_field() {
        let f = field_ref.parse("scope.name").unwrap();
        assert_eq!(f.scope, AttrScope::Scope);
        assert_eq!(f.name, "name");
    }

    #[test]
    fn parse_auto_field() {
        let f = field_ref.parse("my_custom_attr").unwrap();
        assert_eq!(f.scope, AttrScope::Auto);
        assert_eq!(f.name, "my_custom_attr");
    }

    #[test]
    fn parse_service_name_is_signal() {
        let f = field_ref.parse("service_name").unwrap();
        assert_eq!(f.scope, AttrScope::Signal);
    }
}
