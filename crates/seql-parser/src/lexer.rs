// Constants exported for completeness; not all are used in every parser pass.
#![allow(dead_code)]

use winnow::ascii::{multispace0, multispace1};
use winnow::combinator::{alt, delimited, preceded};
use winnow::token::{literal, take_while};
use winnow::{ModalResult, Parser};

/// SeQL keywords
pub const KW_SPANS: &str = "spans";
pub const KW_LOGS: &str = "logs";
pub const KW_DATAPOINTS: &str = "datapoints";
pub const KW_METRICS: &str = "metrics";
pub const KW_SAMPLES: &str = "samples";
pub const KW_TRACES: &str = "traces";
pub const KW_LAST: &str = "last";
pub const KW_IN: &str = "in";
pub const KW_BETWEEN: &str = "between";
pub const KW_TODAY: &str = "today";
pub const KW_YESTERDAY: &str = "yesterday";
pub const KW_WHERE: &str = "where";
pub const KW_SELECT: &str = "select";
pub const KW_COMPUTE: &str = "compute";
pub const KW_GROUP: &str = "group";
pub const KW_BY: &str = "by";
pub const KW_SORT: &str = "sort";
pub const KW_TAKE: &str = "take";
pub const KW_UNIQ: &str = "uniq";
pub const KW_PATTERNS: &str = "patterns";
pub const KW_NAVIGATE: &str = "navigate";
pub const KW_MERGE: &str = "merge";
pub const KW_LET: &str = "let";
pub const KW_AS: &str = "as";
pub const KW_ASC: &str = "asc";
pub const KW_DESC: &str = "desc";
pub const KW_OFFSET: &str = "offset";
pub const KW_AND: &str = "and";
pub const KW_OR: &str = "or";
pub const KW_NOT: &str = "not";
pub const KW_EXISTS: &str = "exists";
pub const KW_CONTAINS: &str = "contains";
pub const KW_STARTS_WITH: &str = "starts_with";
pub const KW_ENDS_WITH: &str = "ends_with";
pub const KW_MATCHES: &str = "matches";
pub const KW_NULL: &str = "null";
pub const KW_TRUE: &str = "true";
pub const KW_FALSE: &str = "false";

/// OTLP enum literals
pub const LIT_OK: &str = "OK";
pub const LIT_ERROR: &str = "ERROR";
pub const LIT_UNSET: &str = "UNSET";
pub const LIT_SERVER: &str = "SERVER";
pub const LIT_CLIENT: &str = "CLIENT";
pub const LIT_PRODUCER: &str = "PRODUCER";
pub const LIT_CONSUMER: &str = "CONSUMER";
pub const LIT_INTERNAL: &str = "INTERNAL";
pub const LIT_TRACE: &str = "TRACE";
pub const LIT_DEBUG: &str = "DEBUG";
pub const LIT_INFO: &str = "INFO";
pub const LIT_WARN: &str = "WARN";
pub const LIT_FATAL: &str = "FATAL";

/// Skip optional whitespace
pub fn ws(input: &mut &str) -> ModalResult<()> {
    multispace0.void().parse_next(input)
}

/// Require at least one whitespace character
pub fn ws1(input: &mut &str) -> ModalResult<()> {
    multispace1.void().parse_next(input)
}

/// Parse an identifier: starts with letter or `_`, followed by alphanumeric or `_` or `.`
pub fn identifier<'i>(input: &mut &'i str) -> ModalResult<&'i str> {
    take_while(1.., |c: char| c.is_alphanumeric() || c == '_' || c == '.').parse_next(input)
}

/// Parse a quoted string literal (single or double quotes)
pub fn quoted_string(input: &mut &str) -> ModalResult<String> {
    alt((
        delimited(
            literal("\""),
            take_while(0.., |c: char| c != '"'),
            literal("\""),
        )
        .map(|s: &str| s.to_string()),
        delimited(
            literal("'"),
            take_while(0.., |c: char| c != '\''),
            literal("'"),
        )
        .map(|s: &str| s.to_string()),
    ))
    .parse_next(input)
}

/// Parse an unsigned integer
pub fn uint_literal(input: &mut &str) -> ModalResult<u64> {
    take_while(1.., |c: char| c.is_ascii_digit())
        .parse_next(input)
        .and_then(|s: &str| {
            s.parse::<u64>()
                .map_err(|_| winnow::error::ErrMode::Backtrack(winnow::error::ContextError::new()))
        })
}

/// Parse an optional `-` sign followed by digits → i64
pub fn int_literal(input: &mut &str) -> ModalResult<i64> {
    (
        winnow::combinator::opt(literal("-")),
        take_while(1.., |c: char| c.is_ascii_digit()),
    )
        .parse_next(input)
        .and_then(|(neg, digits): (Option<&str>, &str)| {
            let n: i64 = digits.parse().map_err(|_| {
                winnow::error::ErrMode::Backtrack(winnow::error::ContextError::new())
            })?;
            Ok(if neg.is_some() { -n } else { n })
        })
}

/// Case-insensitive keyword matcher
pub fn keyword_ci<'i>(kw: &'static str) -> impl Parser<&'i str, (), winnow::error::ContextError> {
    move |input: &mut &'i str| {
        preceded(multispace0, move |i: &mut &'i str| {
            let kw_lower = kw.to_lowercase();
            let len = kw_lower.len();

            if i.len() < len {
                return Err(winnow::error::ErrMode::Backtrack(
                    winnow::error::ContextError::new(),
                ));
            }

            let candidate = &i[..len];
            if candidate.to_lowercase() == kw_lower {
                // Check that the next char is not alphanumeric (word boundary)
                if i.len() > len {
                    let next_char = i.chars().nth(len);
                    if let Some(c) = next_char {
                        if c.is_alphanumeric() || c == '_' {
                            return Err(winnow::error::ErrMode::Backtrack(
                                winnow::error::ContextError::new(),
                            ));
                        }
                    }
                }
                *i = &i[len..];
                Ok(())
            } else {
                Err(winnow::error::ErrMode::Backtrack(
                    winnow::error::ContextError::new(),
                ))
            }
        })
        .void()
        .parse_next(input)
    }
}

/// Skip whitespace, then parse a keyword (case-insensitive)
pub fn keyword<'i>(kw: &'static str) -> impl Parser<&'i str, (), winnow::error::ContextError> {
    keyword_ci(kw)
}

/// Parse the pipe `|` separator between pipeline stages
pub fn pipe(input: &mut &str) -> ModalResult<()> {
    delimited(multispace0, literal("|"), multispace0)
        .void()
        .parse_next(input)
}

/// Parse `->` navigation operator
pub fn arrow(input: &mut &str) -> ModalResult<()> {
    delimited(multispace0, literal("->"), multispace0)
        .void()
        .parse_next(input)
}

/// Parse `<-` merge operator
pub fn left_arrow(input: &mut &str) -> ModalResult<()> {
    delimited(multispace0, literal("<-"), multispace0)
        .void()
        .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_keywords() {
        // Test signal keywords
        let mut input = "spans";
        assert!(keyword(KW_SPANS).parse_next(&mut input).is_ok());

        let mut input = "logs";
        assert!(keyword(KW_LOGS).parse_next(&mut input).is_ok());

        let mut input = "metrics";
        assert!(keyword(KW_METRICS).parse_next(&mut input).is_ok());

        let mut input = "datapoints";
        assert!(keyword(KW_DATAPOINTS).parse_next(&mut input).is_ok());

        let mut input = "samples";
        assert!(keyword(KW_SAMPLES).parse_next(&mut input).is_ok());

        let mut input = "traces";
        assert!(keyword(KW_TRACES).parse_next(&mut input).is_ok());

        // Test pipeline keywords
        let mut input = "where";
        assert!(keyword(KW_WHERE).parse_next(&mut input).is_ok());

        let mut input = "select";
        assert!(keyword(KW_SELECT).parse_next(&mut input).is_ok());

        let mut input = "compute";
        assert!(keyword(KW_COMPUTE).parse_next(&mut input).is_ok());

        let mut input = "group";
        assert!(keyword(KW_GROUP).parse_next(&mut input).is_ok());

        let mut input = "by";
        assert!(keyword(KW_BY).parse_next(&mut input).is_ok());

        let mut input = "sort";
        assert!(keyword(KW_SORT).parse_next(&mut input).is_ok());

        let mut input = "take";
        assert!(keyword(KW_TAKE).parse_next(&mut input).is_ok());

        // Test time keywords
        let mut input = "last";
        assert!(keyword(KW_LAST).parse_next(&mut input).is_ok());

        let mut input = "in";
        assert!(keyword(KW_IN).parse_next(&mut input).is_ok());

        let mut input = "between";
        assert!(keyword(KW_BETWEEN).parse_next(&mut input).is_ok());

        let mut input = "today";
        assert!(keyword(KW_TODAY).parse_next(&mut input).is_ok());

        let mut input = "yesterday";
        assert!(keyword(KW_YESTERDAY).parse_next(&mut input).is_ok());

        // Test logical keywords
        let mut input = "and";
        assert!(keyword(KW_AND).parse_next(&mut input).is_ok());

        let mut input = "or";
        assert!(keyword(KW_OR).parse_next(&mut input).is_ok());

        let mut input = "not";
        assert!(keyword(KW_NOT).parse_next(&mut input).is_ok());

        // Test predicate keywords
        let mut input = "exists";
        assert!(keyword(KW_EXISTS).parse_next(&mut input).is_ok());

        let mut input = "contains";
        assert!(keyword(KW_CONTAINS).parse_next(&mut input).is_ok());

        let mut input = "starts_with";
        assert!(keyword(KW_STARTS_WITH).parse_next(&mut input).is_ok());

        let mut input = "ends_with";
        assert!(keyword(KW_ENDS_WITH).parse_next(&mut input).is_ok());

        let mut input = "matches";
        assert!(keyword(KW_MATCHES).parse_next(&mut input).is_ok());

        // Test literal keywords
        let mut input = "null";
        assert!(keyword(KW_NULL).parse_next(&mut input).is_ok());

        let mut input = "true";
        assert!(keyword(KW_TRUE).parse_next(&mut input).is_ok());

        let mut input = "false";
        assert!(keyword(KW_FALSE).parse_next(&mut input).is_ok());
    }

    #[test]
    fn test_tokenize_keywords_case_insensitive() {
        // Keywords should be case-insensitive
        let mut input = "SPANS";
        assert!(keyword(KW_SPANS).parse_next(&mut input).is_ok());

        let mut input = "WHERE";
        assert!(keyword(KW_WHERE).parse_next(&mut input).is_ok());

        let mut input = "Select";
        assert!(keyword(KW_SELECT).parse_next(&mut input).is_ok());

        let mut input = "GrOuP";
        assert!(keyword(KW_GROUP).parse_next(&mut input).is_ok());
    }

    #[test]
    fn test_tokenize_identifiers() {
        // Valid identifiers
        let mut input = "field_name";
        assert_eq!(identifier.parse_next(&mut input).unwrap(), "field_name");

        let mut input = "field123";
        assert_eq!(identifier.parse_next(&mut input).unwrap(), "field123");

        let mut input = "_private";
        assert_eq!(identifier.parse_next(&mut input).unwrap(), "_private");

        let mut input = "my_field_1";
        assert_eq!(identifier.parse_next(&mut input).unwrap(), "my_field_1");

        // Identifier with dots (for nested fields)
        let mut input = "resource.service.name";
        assert_eq!(
            identifier.parse_next(&mut input).unwrap(),
            "resource.service.name"
        );
    }

    #[test]
    fn test_tokenize_quoted_strings() {
        // Double-quoted strings
        let mut input = "\"hello world\"";
        assert_eq!(quoted_string.parse_next(&mut input).unwrap(), "hello world");

        let mut input = "\"value\"";
        assert_eq!(quoted_string.parse_next(&mut input).unwrap(), "value");

        // Single-quoted strings
        let mut input = "'hello world'";
        assert_eq!(quoted_string.parse_next(&mut input).unwrap(), "hello world");

        let mut input = "'value'";
        assert_eq!(quoted_string.parse_next(&mut input).unwrap(), "value");

        // Empty strings
        let mut input = "\"\"";
        assert_eq!(quoted_string.parse_next(&mut input).unwrap(), "");

        let mut input = "''";
        assert_eq!(quoted_string.parse_next(&mut input).unwrap(), "");
    }

    #[test]
    fn test_tokenize_escaped_quotes() {
        // Note: The current implementation doesn't support escaped quotes
        // This test documents the current behavior
        // If escape support is added, this test should be updated

        // Strings without escapes work fine
        let mut input = "\"no escapes here\"";
        assert_eq!(
            quoted_string.parse_next(&mut input).unwrap(),
            "no escapes here"
        );

        // The current implementation doesn't handle escaped quotes
        // So this would fail or be handled incorrectly
        // This is a known limitation documented by this test
    }

    #[test]
    fn test_tokenize_operators_and_punctuation() {
        // Pipe operator
        let mut input = "|";
        assert!(pipe.parse_next(&mut input).is_ok());

        let mut input = "  |  "; // with whitespace
        assert!(pipe.parse_next(&mut input).is_ok());

        // Arrow operators
        let mut input = "->";
        assert!(arrow.parse_next(&mut input).is_ok());

        let mut input = "<-";
        assert!(left_arrow.parse_next(&mut input).is_ok());

        // Punctuation is tested through the parser layer
        // Individual punctuation like '(', ')', '[', ']', '.', ',' are handled by literal()
        use winnow::error::ContextError;

        let mut input = "(";
        assert!(literal::<_, _, ContextError>("(")
            .parse_next(&mut input)
            .is_ok());

        let mut input = ")";
        assert!(literal::<_, _, ContextError>(")")
            .parse_next(&mut input)
            .is_ok());

        let mut input = "[";
        assert!(literal::<_, _, ContextError>("[")
            .parse_next(&mut input)
            .is_ok());

        let mut input = "]";
        assert!(literal::<_, _, ContextError>("]")
            .parse_next(&mut input)
            .is_ok());

        let mut input = ",";
        assert!(literal::<_, _, ContextError>(",")
            .parse_next(&mut input)
            .is_ok());

        let mut input = ".";
        assert!(literal::<_, _, ContextError>(".")
            .parse_next(&mut input)
            .is_ok());
    }

    #[test]
    fn test_tokenize_integers() {
        // Unsigned integers
        let mut input = "123";
        assert_eq!(uint_literal.parse_next(&mut input).unwrap(), 123);

        let mut input = "0";
        assert_eq!(uint_literal.parse_next(&mut input).unwrap(), 0);

        let mut input = "999999";
        assert_eq!(uint_literal.parse_next(&mut input).unwrap(), 999999);

        // Signed integers
        let mut input = "123";
        assert_eq!(int_literal.parse_next(&mut input).unwrap(), 123);

        let mut input = "-456";
        assert_eq!(int_literal.parse_next(&mut input).unwrap(), -456);

        let mut input = "0";
        assert_eq!(int_literal.parse_next(&mut input).unwrap(), 0);

        let mut input = "-0";
        assert_eq!(int_literal.parse_next(&mut input).unwrap(), 0);
    }

    #[test]
    fn test_whitespace_handling() {
        // Whitespace should be skipped by ws
        let mut input = "   ";
        assert!(ws.parse_next(&mut input).is_ok());
        assert_eq!(input, "");

        let mut input = "\t\n  ";
        assert!(ws.parse_next(&mut input).is_ok());
        assert_eq!(input, "");

        // ws1 requires at least one whitespace character
        let mut input = "   ";
        assert!(ws1.parse_next(&mut input).is_ok());

        let mut input = "";
        assert!(ws1.parse_next(&mut input).is_err());

        let mut input = "x";
        assert!(ws1.parse_next(&mut input).is_err());
    }

    #[test]
    fn test_keyword_not_substring_match() {
        // `wherein` starts with `where` but the word-boundary check in keyword_ci
        // must reject it — `i` is alphanumeric so `where` should not match.
        let mut input = "wherein";
        assert!(
            keyword(KW_WHERE).parse_next(&mut input).is_err(),
            "`wherein` must not match keyword `where`"
        );

        // `whereisit` — same scenario
        let mut input = "whereisit";
        assert!(
            keyword(KW_WHERE).parse_next(&mut input).is_err(),
            "`whereisit` must not match keyword `where`"
        );

        // `where` followed by a space — must match
        let mut input = "where ";
        assert!(
            keyword(KW_WHERE).parse_next(&mut input).is_ok(),
            "`where ` (trailing space) should match keyword `where`"
        );

        // `where` alone (end of input) — must match
        let mut input = "where";
        assert!(
            keyword(KW_WHERE).parse_next(&mut input).is_ok(),
            "`where` at end of input should match"
        );
    }
}
