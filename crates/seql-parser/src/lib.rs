mod attr;
mod expr;
mod lexer;
mod stages;
mod time;

use seql_ast::ast::QueryAst;
use serde::{Deserialize, Serialize};

/// Parser options for query syntax that needs external context.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ParseOptions {
    /// Current timestamp in nanoseconds since the Unix epoch.
    pub now_ns: u64,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            now_ns: time::system_now_ns(),
        }
    }
}

/// A parse error with byte offset for UI highlighting
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParseError {
    /// Human-readable description of the problem
    pub message: String,
    /// Byte offset in the input where the error starts
    pub offset: usize,
    /// Length in bytes of the offending token (0 if unknown)
    pub length: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (at offset {})", self.message, self.offset)
    }
}

impl std::error::Error for ParseError {}

/// Parse a SeQL text query into a [`QueryAst`]
pub fn parse(input: &str) -> Result<QueryAst, ParseError> {
    parse_with_options(input, ParseOptions::default())
}

/// Parse a SeQL text query using explicit parser context.
pub fn parse_with_options(input: &str, options: ParseOptions) -> Result<QueryAst, ParseError> {
    stages::parse_query_with_options(input, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use seql_ast::ast::Signal;

    #[test]
    fn parse_simple_spans() {
        let ast = parse("spans last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
    }

    #[test]
    fn parse_simple_logs() {
        let ast = parse("logs last 15m").unwrap();
        assert_eq!(ast.scan.signal, Signal::Logs);
    }

    #[test]
    fn parse_simple_datapoints() {
        let ast = parse("datapoints last 24h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Datapoints);
    }

    #[test]
    fn parse_simple_samples() {
        let ast = parse("samples last 30m").unwrap();
        assert_eq!(ast.scan.signal, Signal::Samples);
    }

    #[test]
    fn parse_simple_traces() {
        let ast = parse("traces last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Traces);
    }

    #[test]
    fn parse_with_filter() {
        let ast = parse(r#"spans last 1h | where status == ERROR"#).unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
        assert_eq!(ast.stages.len(), 1);
    }

    #[test]
    fn parse_with_limit() {
        let ast = parse("spans last 1h | take 100").unwrap();
        assert_eq!(ast.stages.len(), 1);
    }

    #[test]
    fn parse_error_has_offset() {
        let err = parse("INVALID").unwrap_err();
        assert_eq!(err.offset, 0);
    }

    #[test]
    fn parse_error_round_trip() {
        let err = ParseError {
            message: "unexpected token".into(),
            offset: 5,
            length: 3,
        };
        let json = serde_json::to_string(&err).unwrap();
        let back: ParseError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }

    #[test]
    fn parse_simple_profiles() {
        let ast = parse("profiles last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Profiles);
    }

    #[test]
    fn parse_simple_stacks() {
        let ast = parse("stacks last 30m").unwrap();
        assert_eq!(ast.scan.signal, Signal::Stacks);
    }

    #[test]
    fn parse_simple_frames() {
        let ast = parse("frames last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Frames);
    }

    #[test]
    fn parse_simple_mappings() {
        let ast = parse("mappings last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Mappings);
    }

    #[test]
    fn parse_simple_resources() {
        let ast = parse("resources last 24h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Resources);
    }

    #[test]
    fn parse_simple_scopes() {
        let ast = parse("scopes last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Scopes);
    }

    #[test]
    fn parse_simple_span_links() {
        let ast = parse("span_links last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::SpanLinks);
    }

    #[test]
    fn parse_simple_histograms() {
        let ast = parse("histograms last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Histograms);
    }

    #[test]
    fn parse_with_options_uses_injected_now_for_day_scopes() {
        let day_ns = 86_400_000_000_000;
        let options = ParseOptions {
            now_ns: day_ns * 3 + 123,
        };

        let today = parse_with_options("spans today", options).unwrap();
        assert_eq!(
            today.scan.time_range,
            Some(seql_ast::ast::TimeRange::Absolute {
                start_ns: day_ns * 3,
                end_ns: day_ns * 4,
            })
        );

        let yesterday = parse_with_options("spans yesterday", options).unwrap();
        assert_eq!(
            yesterday.scan.time_range,
            Some(seql_ast::ast::TimeRange::Absolute {
                start_ns: day_ns * 2,
                end_ns: day_ns * 3,
            })
        );
    }
}
