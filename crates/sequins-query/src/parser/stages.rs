use crate::ast::{
    AggregateFn, AggregateStage, Aggregation, AttrScope, CompareExpr, CompareOp, ComputeStage,
    Derivation, Expr, FieldRef, FilterStage, GroupExpr, LimitStage, Literal, MergeStage,
    NavigateStage, PatternsStage, Predicate, ProjectField, ProjectStage, QueryAst, QueryMode, Scan,
    Signal, SortExpr, SortStage, Stage, TimeRange, TimeRangeStage, UniqueStage,
};
use crate::parser::attr::field_ref;
use crate::parser::expr::{parse_expr, parse_predicate};
use crate::parser::lexer::{
    identifier, keyword, uint_literal, ws, ws1, KW_AS, KW_ASC, KW_BY, KW_COMPUTE, KW_DESC,
    KW_GROUP, KW_LAST, KW_MERGE, KW_NAVIGATE, KW_OFFSET, KW_PATTERNS, KW_SELECT, KW_SORT, KW_TAKE,
    KW_UNIQ, KW_WHERE,
};
use crate::parser::time::{duration_ns, parse_time_scope};
use crate::parser::ParseError;
use winnow::combinator::{alt, delimited, opt, preceded, separated};
use winnow::token::literal;
use winnow::{ModalResult, Parser};

// ── Signal selector ───────────────────────────────────────────────────────────

fn parse_signal(input: &mut &str) -> ModalResult<Signal> {
    alt((
        literal("spans").value(Signal::Spans),
        literal("span_links").value(Signal::SpanLinks),
        literal("logs").value(Signal::Logs),
        literal("datapoints").value(Signal::Datapoints),
        literal("histograms").value(Signal::Histograms),
        literal("metrics").value(Signal::Metrics),
        literal("samples").value(Signal::Samples),
        literal("traces").value(Signal::Traces),
        literal("profiles").value(Signal::Profiles),
        literal("stacks").value(Signal::Stacks),
        literal("frames").value(Signal::Frames),
        literal("mappings").value(Signal::Mappings),
        literal("resources").value(Signal::Resources),
        literal("scopes").value(Signal::Scopes),
    ))
    .parse_next(input)
}

fn parse_scan(input: &mut &str) -> ModalResult<Scan> {
    let signal = parse_signal.parse_next(input)?;
    ws1.parse_next(input)?;
    let time_range = parse_time_scope.parse_next(input)?;
    Ok(Scan { signal, time_range })
}

// ── ID-lookup shorthand ────────────────────────────────────────────────────────

/// Maps a singular keyword to the corresponding [`Signal`] variant.
/// Order matters: longer prefixes (e.g. `span_link`) must appear before
/// their prefixes (e.g. `span`).
fn parse_singular_signal(input: &mut &str) -> ModalResult<Signal> {
    alt((
        literal("span_link").value(Signal::SpanLinks),
        literal("span_event").value(Signal::SpanLinks), // alias: span_event → SpanLinks
        literal("span").value(Signal::Spans),
        literal("trace").value(Signal::Traces),
        literal("log").value(Signal::Logs),
        literal("datapoint").value(Signal::Datapoints),
        literal("histogram").value(Signal::Histograms),
        literal("metric").value(Signal::Metrics),
        literal("sample").value(Signal::Samples),
        literal("profile").value(Signal::Profiles),
        literal("stack").value(Signal::Stacks),
        literal("frame").value(Signal::Frames),
        literal("mapping").value(Signal::Mappings),
        literal("resource").value(Signal::Resources),
        literal("scope").value(Signal::Scopes),
    ))
    .parse_next(input)
}

/// Parses a bare ID value: alphanumeric characters and hyphens (covers hex IDs and UUIDs).
fn parse_id_value(input: &mut &str) -> ModalResult<String> {
    use winnow::token::take_while;
    take_while(1.., |c: char| c.is_alphanumeric() || c == '-')
        .map(|s: &str| s.to_string())
        .parse_next(input)
}

/// Parses the `signal(id)` shorthand and desugars it into a [`QueryAst`] with:
/// - A 24-hour sliding-window scan over the named signal
/// - A `Filter` stage matching the signal's primary ID column
/// - A `Limit(1)` stage
///
/// Examples: `span(abc123)`, `log(some-log-id)`, `trace(b9fe465597df3a11)`.
fn parse_id_lookup(input: &mut &str) -> ModalResult<QueryAst> {
    let signal = parse_singular_signal.parse_next(input)?;
    ws.parse_next(input)?;
    literal("(").parse_next(input)?;
    ws.parse_next(input)?;
    let id = parse_id_value.parse_next(input)?;
    ws.parse_next(input)?;
    literal(")").parse_next(input)?;

    let id_col = signal.join_key().to_string();
    // 24-hour sliding window expressed in nanoseconds
    let start_ns: u64 = 24 * 60 * 60 * 1_000_000_000;

    Ok(QueryAst {
        bindings: vec![],
        scan: Scan {
            signal,
            time_range: TimeRange::SlidingWindow { start_ns },
        },
        stages: vec![
            Stage::Filter(FilterStage {
                predicate: Predicate::Compare(CompareExpr {
                    left: Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: id_col,
                    }),
                    op: CompareOp::Eq,
                    right: Expr::Literal(Literal::String(id)),
                }),
            }),
            Stage::Limit(LimitStage {
                limit: 1,
                offset: None,
            }),
        ],
        mode: QueryMode::Snapshot,
    })
}

// ── Aggregate functions ───────────────────────────────────────────────────────

fn parse_agg_fn(input: &mut &str) -> ModalResult<AggregateFn> {
    alt((
        literal("count()").value(AggregateFn::Count),
        literal("error_rate()").value(AggregateFn::ErrorRate),
        literal("throughput()").value(AggregateFn::Throughput),
        preceded(
            literal("sum("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::Sum(e)),
        ),
        preceded(
            literal("avg("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::Avg(e)),
        ),
        preceded(
            literal("min("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::Min(e)),
        ),
        preceded(
            literal("max("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::Max(e)),
        ),
        preceded(
            literal("p50("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::P50(e)),
        ),
        preceded(
            literal("p95("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::P95(e)),
        ),
        preceded(
            literal("p99("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::P99(e)),
        ),
        preceded(
            literal("distinct("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::Distinct(e)),
        ),
        preceded(
            literal("heatmap("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::Heatmap(e)),
        ),
        preceded(
            literal("sample("),
            (uint_literal, literal(")")).map(|(n, _)| AggregateFn::Sample(n)),
        ),
        preceded(
            literal("first("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::First(e)),
        ),
        preceded(
            literal("last("),
            (parse_expr, literal(")")).map(|(e, _)| AggregateFn::Last(e)),
        ),
    ))
    .parse_next(input)
}

// ── Stage parsers ─────────────────────────────────────────────────────────────

fn parse_filter_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_WHERE).parse_next(input)?;
    ws1.parse_next(input)?;
    let predicate = parse_predicate.parse_next(input)?;
    Ok(Stage::Filter(FilterStage { predicate }))
}

fn parse_project_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_SELECT).parse_next(input)?;
    ws1.parse_next(input)?;
    let fields: Vec<ProjectField> = separated(
        1..,
        (
            ws,
            field_ref,
            opt(preceded((ws, literal(KW_AS), ws1), identifier)),
        )
            .map(|(_, field, alias)| ProjectField {
                field,
                alias: alias.map(str::to_string),
            }),
        (ws, literal(","), ws),
    )
    .parse_next(input)?;
    Ok(Stage::Project(ProjectStage { fields }))
}

fn parse_compute_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_COMPUTE).parse_next(input)?;
    ws1.parse_next(input)?;
    let derivations: Vec<Derivation> = separated(
        1..,
        (ws, parse_expr, ws, literal(KW_AS), ws1, identifier).map(
            |(_, expr, _, _, _, alias): ((), Expr, (), &str, (), &str)| Derivation {
                alias: alias.to_string(),
                expr,
            },
        ),
        (ws, literal(","), ws),
    )
    .parse_next(input)?;
    Ok(Stage::Compute(ComputeStage { derivations }))
}

fn parse_aggregate_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_GROUP).parse_next(input)?;
    ws1.parse_next(input)?;
    literal(KW_BY).parse_next(input)?;
    ws.parse_next(input)?;
    // group by { field [as alias] [bin <duration>], ... } { aggfn as alias, ... }
    let group_by: Vec<GroupExpr> = delimited(
        (literal("{"), ws),
        separated(
            0..,
            (
                ws,
                parse_expr,
                // ws (not ws1): parse_expr's loops consume trailing whitespace,
                // so there may be 0 spaces left before "bin" / "as".
                opt(preceded((ws, literal("bin"), ws1), duration_ns)),
                opt(preceded((ws, literal(KW_AS), ws1), identifier)),
                ws,
            )
                .map(|(_, expr, bin_ns, alias, _)| GroupExpr {
                    expr,
                    alias: alias.map(str::to_string),
                    bin_ns,
                }),
            (ws, literal(","), ws),
        ),
        (ws, literal("}")),
    )
    .parse_next(input)?;

    ws.parse_next(input)?;
    let aggregations: Vec<Aggregation> = delimited(
        (literal("{"), ws),
        separated(
            1..,
            (
                ws,
                parse_agg_fn,
                ws,
                opt(preceded((literal(KW_WHERE), ws1), parse_predicate)),
                ws,
                literal(KW_AS),
                ws1,
                identifier,
                ws,
            )
                .map(
                    |(_, function, _, filter, _, _, _, alias, _): (
                        (),
                        AggregateFn,
                        (),
                        Option<_>,
                        (),
                        &str,
                        (),
                        &str,
                        (),
                    )| {
                        Aggregation {
                            function,
                            alias: alias.to_string(),
                            filter,
                        }
                    },
                ),
            (ws, literal(","), ws),
        ),
        (ws, literal("}")),
    )
    .parse_next(input)?;

    Ok(Stage::Aggregate(AggregateStage {
        group_by,
        aggregations,
    }))
}

fn parse_sort_stage<'i>(input: &mut &'i str) -> ModalResult<Stage> {
    literal(KW_SORT).parse_next(input)?;
    ws1.parse_next(input)?;
    let exprs: Vec<SortExpr> = separated(
        1..,
        |input: &mut &'i str| {
            ws.parse_next(input)?;
            let expr = parse_expr.parse_next(input)?;
            // parse_expr's inner loops may have consumed trailing whitespace;
            // use ws (0+) here so we still find the direction keyword.
            let ascending = opt(alt((
                (ws, literal(KW_ASC)).map(|_| true),
                (ws, literal(KW_DESC)).map(|_| false),
            )))
            .parse_next(input)?
            .unwrap_or(true);
            Ok(SortExpr { expr, ascending })
        },
        (ws, literal(","), ws),
    )
    .parse_next(input)?;
    Ok(Stage::Sort(SortStage { exprs }))
}

fn parse_limit_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_TAKE).parse_next(input)?;
    ws1.parse_next(input)?;
    let limit = uint_literal.parse_next(input)?;
    let offset = opt(preceded((ws1, literal(KW_OFFSET), ws1), uint_literal)).parse_next(input)?;
    Ok(Stage::Limit(LimitStage { limit, offset }))
}

fn parse_navigate_stage(input: &mut &str) -> ModalResult<Stage> {
    alt((literal("->").void(), keyword(KW_NAVIGATE))).parse_next(input)?;
    ws.parse_next(input)?;
    let target = parse_signal.parse_next(input)?;
    Ok(Stage::Navigate(NavigateStage { target }))
}

fn signal_name(signal: &Signal) -> &'static str {
    match signal {
        Signal::Spans => "spans",
        Signal::SpanLinks => "span_links",
        Signal::Logs => "logs",
        Signal::Datapoints => "datapoints",
        Signal::Histograms => "histograms",
        Signal::Metrics => "metrics",
        Signal::Samples => "samples",
        Signal::Traces => "traces",
        Signal::Profiles => "profiles",
        Signal::Stacks => "stacks",
        Signal::Frames => "frames",
        Signal::Mappings => "mappings",
        Signal::Resources => "resources",
        Signal::Scopes => "scopes",
    }
}

fn parse_merge_stage(input: &mut &str) -> ModalResult<Stage> {
    alt((literal("<-").void(), keyword(KW_MERGE))).parse_next(input)?;
    ws.parse_next(input)?;

    let (target, inner_stages) = if input.starts_with('(') {
        literal("(").parse_next(input)?;
        ws.parse_next(input)?;
        let target = parse_signal.parse_next(input)?;
        let mut stages = Vec::new();
        loop {
            let trimmed = input.trim_start();
            if trimmed.is_empty() || trimmed.starts_with(')') {
                break;
            }
            if let Some(rest) = trimmed.strip_prefix('|') {
                *input = rest;
            } else if trimmed.starts_with("->") {
                *input = trimmed; // don't consume `->` — parse_navigate_stage expects to see it
            } else if trimmed.starts_with("<-") {
                *input = trimmed; // don't consume `<-` — parse_merge_stage expects to see it
            } else {
                break;
            }
            ws.parse_next(input)?;
            let stage = parse_stage.parse_next(input)?;
            stages.push(stage);
        }
        ws.parse_next(input)?;
        literal(")").parse_next(input)?;
        (target, stages)
    } else {
        let target = parse_signal.parse_next(input)?;
        let mut stages = Vec::new();
        loop {
            let trimmed = input.trim_start();
            if trimmed.is_empty() {
                break;
            }
            if let Some(rest) = trimmed.strip_prefix('|') {
                *input = rest;
            } else if trimmed.starts_with("->") || trimmed.starts_with("<-") {
                // Don't strip prefix — parse_navigate_stage / parse_merge_stage expect to see it
                *input = trimmed;
            } else {
                break;
            }
            ws.parse_next(input)?;
            let stage = parse_stage.parse_next(input)?;
            stages.push(stage);
        }
        (target, stages)
    };

    let alias = opt(preceded((ws, literal(KW_AS), ws1), identifier))
        .parse_next(input)?
        .map(|s: &str| s.to_string())
        .unwrap_or_else(|| signal_name(&target).to_string());

    Ok(Stage::Merge(MergeStage {
        target,
        stages: inner_stages,
        alias,
    }))
}

fn parse_unique_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_UNIQ).parse_next(input)?;
    ws1.parse_next(input)?;
    let field = field_ref.parse_next(input)?;
    Ok(Stage::Unique(UniqueStage { field }))
}

fn parse_patterns_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_PATTERNS).parse_next(input)?;
    let field = opt(preceded(ws1, field_ref)).parse_next(input)?;
    Ok(Stage::Patterns(PatternsStage { field }))
}

fn parse_time_range_stage(input: &mut &str) -> ModalResult<Stage> {
    literal(KW_LAST).parse_next(input)?;
    ws.parse_next(input)?;
    let duration_ns = duration_ns.parse_next(input)?;
    Ok(Stage::TimeRange(TimeRangeStage { duration_ns }))
}

/// Parse a single pipeline stage
fn parse_stage(input: &mut &str) -> ModalResult<Stage> {
    ws.parse_next(input)?;
    alt((
        parse_filter_stage,
        parse_project_stage,
        parse_compute_stage,
        parse_aggregate_stage,
        parse_sort_stage,
        parse_limit_stage,
        parse_navigate_stage,
        parse_merge_stage,
        parse_unique_stage,
        parse_patterns_stage,
        parse_time_range_stage,
    ))
    .parse_next(input)
}

/// Parse a complete SeQL query string
pub fn parse_query(input: &str) -> Result<QueryAst, ParseError> {
    let mut s = input.trim();
    let original = s;

    // Try the `signal(id)` shorthand first (e.g. `span(abc123)`, `log(some-id)`).
    // If it succeeds we still need to check for any trailing pipeline stages.
    let mut ast = if let Ok(lookup_ast) = parse_id_lookup.parse_next(&mut s) {
        lookup_ast
    } else {
        // Reset to the trimmed input in case parse_id_lookup consumed some chars
        s = input.trim();

        // Parse optional `let` bindings
        let bindings = Vec::new();
        // (simple implementation — let bindings not yet supported)

        // Parse the primary scan
        let scan = match parse_scan.parse_next(&mut s) {
            Ok(scan) => scan,
            Err(_) => {
                return Err(ParseError {
                    message:
                        "expected signal keyword (spans, span_links, logs, datapoints, histograms, metrics, samples, traces, profiles, stacks, frames, mappings, resources, scopes) or id-lookup shorthand (e.g. span(abc123))"
                            .into(),
                    offset: 0,
                    length: s.len().min(20),
                });
            }
        };

        QueryAst {
            bindings,
            scan,
            stages: Vec::new(),
            mode: QueryMode::Snapshot,
        }
    };

    // Parse pipeline stages separated by `|` or `->` (navigate)
    loop {
        // Skip whitespace and look for a stage boundary
        let trimmed = s.trim_start();
        if trimmed.is_empty() {
            break;
        }
        if let Some(stripped) = trimmed.strip_prefix('|') {
            s = stripped; // consume `|`
        } else if trimmed.starts_with("->") {
            s = trimmed; // don't consume `->` — parse_navigate_stage expects to see it
        } else if trimmed.starts_with("<-") {
            s = trimmed; // don't consume `<-` — parse_merge_stage expects to see it
        } else {
            break;
        }
        let stage = match parse_stage.parse_next(&mut s) {
            Ok(st) => st,
            Err(_) => {
                let offset = original.len() - s.len();
                return Err(ParseError {
                    message: "expected a pipeline stage after |, -> or <-".into(),
                    offset,
                    length: s.len().min(20),
                });
            }
        };
        ast.stages.push(stage);
    }

    Ok(ast)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{
        AttrScope, CompareExpr, CompareOp, Expr, FieldRef, FilterStage, LimitStage, Literal,
        NavigateStage, Predicate, Signal, TimeRange,
    };

    #[test]
    fn parse_spans_last_1h() {
        let ast = parse_query("spans last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
        assert_eq!(
            ast.scan.time_range,
            TimeRange::SlidingWindow {
                start_ns: 3_600_000_000_000
            }
        );
        assert!(ast.stages.is_empty());
    }

    #[test]
    fn parse_logs_with_filter() {
        let ast = parse_query(r#"logs last 15m | where severity_text == ERROR"#).unwrap();
        assert_eq!(ast.scan.signal, Signal::Logs);
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(ast.stages[0], Stage::Filter(_)));
    }

    #[test]
    fn parse_with_limit() {
        let ast = parse_query("spans last 1h | take 100").unwrap();
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(
            ast.stages[0],
            Stage::Limit(LimitStage { limit: 100, .. })
        ));
    }

    #[test]
    fn parse_with_sort_desc() {
        let ast = parse_query("spans last 1h | sort duration desc").unwrap();
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Sort(SortStage { exprs }) = &ast.stages[0] {
            assert_eq!(exprs.len(), 1);
            assert!(!exprs[0].ascending);
        } else {
            panic!("expected sort stage");
        }
    }

    #[test]
    fn parse_filter_and_limit() {
        let ast = parse_query("spans last 1h | where status == ERROR | take 50").unwrap();
        assert_eq!(ast.stages.len(), 2);
        assert!(matches!(ast.stages[0], Stage::Filter(_)));
        assert!(matches!(ast.stages[1], Stage::Limit(_)));
    }

    #[test]
    fn parse_uniq() {
        let ast = parse_query("spans last 1h | uniq service_name").unwrap();
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(ast.stages[0], Stage::Unique(_)));
    }

    #[test]
    fn parse_patterns() {
        let ast = parse_query("logs last 1h | patterns").unwrap();
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(ast.stages[0], Stage::Patterns(_)));
    }

    #[test]
    fn parse_invalid_signal() {
        let err = parse_query("INVALID").unwrap_err();
        assert_eq!(err.offset, 0);
    }

    #[test]
    fn parse_aggregate_stage() {
        let ast =
            parse_query("spans last 1h | group by { service_name } { count() as count }").unwrap();
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(ast.stages[0], Stage::Aggregate(_)));
    }

    #[test]
    fn parse_group_by_with_bin() {
        // Regression: parse_expr's loops consume trailing ws, so "bin" must accept ws (0+) before it
        let ast = parse_query(
            "spans last 1h | group by { start_time_unix_nano bin 5m as bucket } { count() as n }",
        )
        .unwrap();
        if let Stage::Aggregate(agg) = &ast.stages[0] {
            assert_eq!(agg.group_by.len(), 1);
            assert!(agg.group_by[0].bin_ns.is_some());
            assert_eq!(agg.group_by[0].alias.as_deref(), Some("bucket"));
        } else {
            panic!("expected aggregate stage");
        }
    }

    #[test]
    fn parse_traces_where_string_eq() {
        let result =
            parse_query(r#"traces last 1h | where trace_id == "b9fe465597df3a11cc3e7fc4dbe44f00""#);
        match &result {
            Err(e) => panic!("parse failed: {:?} at offset {}", e.message, e.offset),
            Ok(ast) => {
                assert_eq!(ast.scan.signal, Signal::Traces);
                assert_eq!(ast.stages.len(), 1);
                assert!(matches!(ast.stages[0], Stage::Filter(_)));
            }
        }
    }

    #[test]
    fn parse_time_range_stage() {
        let ast = parse_query("spans last 1h | last 30m").unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
        assert_eq!(ast.stages.len(), 1);
        match &ast.stages[0] {
            Stage::TimeRange(tr) => {
                assert_eq!(tr.duration_ns, 30 * 60 * 1_000_000_000); // 30 minutes in ns
            }
            _ => panic!("expected TimeRange stage"),
        }
    }

    #[test]
    fn parse_multiple_time_ranges() {
        // Multiple time range stages should be allowed
        let ast = parse_query("logs last 24h | last 1h | where severity_text == ERROR").unwrap();
        assert_eq!(ast.scan.signal, Signal::Logs);
        assert_eq!(ast.stages.len(), 2);
        assert!(matches!(ast.stages[0], Stage::TimeRange(_)));
        assert!(matches!(ast.stages[1], Stage::Filter(_)));
    }

    #[test]
    fn parse_navigate() {
        let ast = parse_query("spans last 1h | -> traces").unwrap();
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(
            ast.stages[0],
            Stage::Navigate(NavigateStage {
                target: Signal::Traces
            })
        ));
    }

    #[test]
    fn parse_navigate_no_pipe() {
        // `->` without a preceding `|` should be valid
        let ast = parse_query("traces last 1h -> spans").unwrap();
        assert_eq!(ast.scan.signal, Signal::Traces);
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(
            ast.stages[0],
            Stage::Navigate(NavigateStage {
                target: Signal::Spans
            })
        ));
    }

    #[test]
    fn parse_navigate_no_pipe_with_filter_after() {
        // `->` without `|` followed by post-navigate filter
        let ast = parse_query("traces last 1h -> spans | where status == ERROR | take 10").unwrap();
        assert_eq!(ast.scan.signal, Signal::Traces);
        assert_eq!(ast.stages.len(), 3);
        assert!(matches!(ast.stages[0], Stage::Navigate(_)));
        assert!(matches!(ast.stages[1], Stage::Filter(_)));
        assert!(matches!(ast.stages[2], Stage::Limit(_)));
    }

    #[test]
    fn parse_merge_basic() {
        let ast = parse_query("spans last 1h <- logs").unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.target, Signal::Logs);
            assert_eq!(m.alias, "logs");
            assert!(m.stages.is_empty());
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_merge_with_alias() {
        let ast = parse_query("spans last 1h <- logs as error_logs").unwrap();
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.alias, "error_logs");
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_merge_with_parens() {
        let ast = parse_query("spans last 1h <- (logs | where severity_text == ERROR)").unwrap();
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.target, Signal::Logs);
            assert_eq!(m.stages.len(), 1);
            assert!(matches!(m.stages[0], Stage::Filter(_)));
            assert_eq!(m.alias, "logs");
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_merge_with_parens_and_alias() {
        let ast =
            parse_query("spans last 1h <- (logs | where severity_text == ERROR) as error_logs")
                .unwrap();
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.stages.len(), 1);
            assert_eq!(m.alias, "error_logs");
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_merge_then_outer_filter() {
        // Without parens, `<- logs | where ...` is greedy: filter is inside the merge
        let ast = parse_query(r#"spans last 1h <- logs | where name == "x""#).unwrap();
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.target, Signal::Logs);
            assert_eq!(m.stages.len(), 1);
            assert!(matches!(m.stages[0], Stage::Filter(_)));
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_multiple_merges() {
        let ast = parse_query(
            "spans last 1h <- logs as all_logs <- (logs | where severity_text == ERROR) as error_logs",
        )
        .unwrap();
        assert_eq!(ast.stages.len(), 2);
        if let Stage::Merge(m1) = &ast.stages[0] {
            assert_eq!(m1.alias, "all_logs");
        } else {
            panic!("expected first merge");
        }
        if let Stage::Merge(m2) = &ast.stages[1] {
            assert_eq!(m2.alias, "error_logs");
            assert_eq!(m2.stages.len(), 1);
        } else {
            panic!("expected second merge");
        }
    }

    #[test]
    fn parse_merge_no_pipe_prefix() {
        // `<-` recognized as boundary without preceding `|`
        let ast = parse_query("spans last 1h <- logs").unwrap();
        assert!(matches!(ast.stages[0], Stage::Merge(_)));
    }

    #[test]
    fn parse_metrics_merge_datapoints() {
        let ast = parse_query("metrics last 24h <- datapoints").unwrap();
        assert_eq!(ast.scan.signal, Signal::Metrics);
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.target, Signal::Datapoints);
            assert_eq!(m.alias, "datapoints");
            assert!(m.stages.is_empty());
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_metrics_merge_with_group_by_substages() {
        // The bucketed metrics query must use parens to put group by INSIDE the merge.
        // Without parens, `| group by ...` would be an outer stage on the metrics rows.
        let ast = parse_query(
            "metrics last 24h <- (datapoints | group by { ts() bin 5m as bucket } { avg(value) as val }) as datapoints",
        )
        .unwrap();
        assert_eq!(ast.scan.signal, Signal::Metrics);
        // Only one outer stage (the merge itself)
        assert_eq!(
            ast.stages.len(),
            1,
            "group by must be inside the merge, not an outer stage"
        );
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.target, Signal::Datapoints);
            assert_eq!(m.alias, "datapoints");
            assert_eq!(
                m.stages.len(),
                1,
                "merge should have one inner stage (group by)"
            );
            assert!(
                matches!(m.stages[0], Stage::Aggregate(_)),
                "inner stage should be Aggregate"
            );
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_metrics_merge_without_parens_group_by_is_inner() {
        // Without parens, `| group by` is greedy: aggregate is inside the merge.
        let ast = parse_query(
            "metrics last 24h <- datapoints | group by { ts() bin 5m as bucket } { avg(value) as val } as datapoints",
        )
        .unwrap();
        assert_eq!(ast.scan.signal, Signal::Metrics);
        // One outer stage: the merge (with group by inside)
        assert_eq!(
            ast.stages.len(),
            1,
            "without parens, group by is inside the merge (greedy)"
        );
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.target, Signal::Datapoints);
            assert_eq!(m.alias, "datapoints");
            assert_eq!(
                m.stages.len(),
                1,
                "merge should have one inner stage (group by)"
            );
            assert!(
                matches!(m.stages[0], Stage::Aggregate(_)),
                "inner stage should be Aggregate"
            );
        } else {
            panic!("expected merge stage");
        }
    }

    #[test]
    fn parse_greedy_nested_merge_without_parens() {
        // `profiles last 1h <- samples <- stacks` — greedy: stacks is inside samples
        let ast = parse_query("profiles last 1h <- samples <- stacks").unwrap();
        assert_eq!(ast.stages.len(), 1, "one outer stage (the samples merge)");
        if let Stage::Merge(outer) = &ast.stages[0] {
            assert_eq!(outer.target, Signal::Samples);
            assert_eq!(outer.stages.len(), 1, "stacks merge is inside samples");
            if let Stage::Merge(inner) = &outer.stages[0] {
                assert_eq!(inner.target, Signal::Stacks);
                assert!(inner.stages.is_empty());
            } else {
                panic!("expected inner merge stage");
            }
        } else {
            panic!("expected outer merge stage");
        }
    }

    #[test]
    fn parse_parens_stop_greedy() {
        // `<- (samples) <- stacks` — parens stop greedy, so two sibling merges
        let ast = parse_query("profiles last 1h <- (samples) <- stacks").unwrap();
        assert_eq!(ast.stages.len(), 2, "two sibling merges");
        assert!(matches!(ast.stages[0], Stage::Merge(_)));
        assert!(matches!(ast.stages[1], Stage::Merge(_)));
        if let Stage::Merge(m1) = &ast.stages[0] {
            assert_eq!(m1.target, Signal::Samples);
            assert!(m1.stages.is_empty());
        }
        if let Stage::Merge(m2) = &ast.stages[1] {
            assert_eq!(m2.target, Signal::Stacks);
        }
    }

    #[test]
    fn parse_alias_stops_greedy() {
        // `<- logs as my_logs <- resources` — alias stops greedy, two sibling merges
        let ast = parse_query("spans last 1h <- logs as my_logs <- resources").unwrap();
        assert_eq!(ast.stages.len(), 2, "two sibling merges");
        if let Stage::Merge(m1) = &ast.stages[0] {
            assert_eq!(m1.target, Signal::Logs);
            assert_eq!(m1.alias, "my_logs");
            assert!(m1.stages.is_empty());
        } else {
            panic!("expected first merge");
        }
        if let Stage::Merge(m2) = &ast.stages[1] {
            assert_eq!(m2.target, Signal::Resources);
        } else {
            panic!("expected second merge");
        }
    }

    #[test]
    fn parse_three_level_nested_merge() {
        // `a last 1h <- b <- c <- d` — greedy produces 3-level nesting
        let ast = parse_query("profiles last 1h <- samples <- stacks <- frames").unwrap();
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Merge(outer) = &ast.stages[0] {
            assert_eq!(outer.target, Signal::Samples);
            assert_eq!(outer.stages.len(), 1);
            if let Stage::Merge(mid) = &outer.stages[0] {
                assert_eq!(mid.target, Signal::Stacks);
                assert_eq!(mid.stages.len(), 1);
                if let Stage::Merge(inner) = &mid.stages[0] {
                    assert_eq!(inner.target, Signal::Frames);
                    assert!(inner.stages.is_empty());
                } else {
                    panic!("expected innermost merge");
                }
            } else {
                panic!("expected mid merge");
            }
        } else {
            panic!("expected outer merge");
        }
    }

    #[test]
    fn parse_profiles() {
        let ast = parse_query("profiles last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Profiles);
    }

    #[test]
    fn parse_stacks() {
        let ast = parse_query("stacks last 30m").unwrap();
        assert_eq!(ast.scan.signal, Signal::Stacks);
    }

    #[test]
    fn parse_frames() {
        let ast = parse_query("frames last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Frames);
    }

    #[test]
    fn parse_mappings() {
        let ast = parse_query("mappings last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Mappings);
    }

    #[test]
    fn parse_resources() {
        let ast = parse_query("resources last 24h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Resources);
    }

    #[test]
    fn parse_scopes() {
        let ast = parse_query("scopes last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Scopes);
    }

    #[test]
    fn parse_span_links() {
        let ast = parse_query("span_links last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::SpanLinks);
    }

    #[test]
    fn parse_histograms() {
        let ast = parse_query("histograms last 1h").unwrap();
        assert_eq!(ast.scan.signal, Signal::Histograms);
    }

    #[test]
    fn parse_nested_merge_with_parens() {
        // Test nested merge: profiles <- (samples <- stacks)
        let ast = parse_query("profiles last 1h <- (samples <- stacks)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Profiles);
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Merge(outer_merge) = &ast.stages[0] {
            assert_eq!(outer_merge.target, Signal::Samples);
            assert_eq!(outer_merge.alias, "samples");
            assert_eq!(outer_merge.stages.len(), 1);
            // The inner merge should be a stage within the outer merge
            if let Stage::Merge(inner_merge) = &outer_merge.stages[0] {
                assert_eq!(inner_merge.target, Signal::Stacks);
                assert_eq!(inner_merge.alias, "stacks");
                assert!(inner_merge.stages.is_empty());
            } else {
                panic!("expected inner merge stage");
            }
        } else {
            panic!("expected outer merge stage");
        }
    }

    #[test]
    fn parse_nested_merge_with_pipe_and_parens() {
        // Test nested merge with explicit pipe: profiles | <- (samples <- stacks)
        let ast = parse_query("profiles last 1h | <- (samples <- stacks)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Profiles);
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Merge(outer_merge) = &ast.stages[0] {
            assert_eq!(outer_merge.target, Signal::Samples);
            assert_eq!(outer_merge.alias, "samples");
            assert_eq!(outer_merge.stages.len(), 1);
            if let Stage::Merge(inner_merge) = &outer_merge.stages[0] {
                assert_eq!(inner_merge.target, Signal::Stacks);
                assert_eq!(inner_merge.alias, "stacks");
            } else {
                panic!("expected inner merge stage");
            }
        } else {
            panic!("expected outer merge stage");
        }
    }

    // ── ID-lookup shorthand tests ──────────────────────────────────────────────

    #[test]
    fn parse_span_id_lookup() {
        let ast = parse_query("span(abc123)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
        assert_eq!(
            ast.scan.time_range,
            TimeRange::SlidingWindow {
                start_ns: 24 * 60 * 60 * 1_000_000_000
            }
        );
        // Should have Filter + Limit
        assert_eq!(ast.stages.len(), 2);
        if let Stage::Filter(FilterStage { predicate }) = &ast.stages[0] {
            if let Predicate::Compare(CompareExpr { left, op, right }) = predicate {
                assert_eq!(
                    left,
                    &Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "span_id".into()
                    })
                );
                assert_eq!(op, &CompareOp::Eq);
                assert_eq!(right, &Expr::Literal(Literal::String("abc123".into())));
            } else {
                panic!("expected Compare predicate");
            }
        } else {
            panic!("expected Filter stage");
        }
        assert!(matches!(
            ast.stages[1],
            Stage::Limit(LimitStage {
                limit: 1,
                offset: None
            })
        ));
    }

    #[test]
    fn parse_log_id_lookup() {
        let ast = parse_query("log(some-log-id)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Logs);
        assert_eq!(ast.stages.len(), 2);
        if let Stage::Filter(FilterStage { predicate }) = &ast.stages[0] {
            if let Predicate::Compare(CompareExpr { left, .. }) = predicate {
                assert_eq!(
                    left,
                    &Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "log_id".into()
                    })
                );
            } else {
                panic!("expected Compare predicate");
            }
        } else {
            panic!("expected Filter stage");
        }
    }

    #[test]
    fn parse_trace_id_lookup() {
        let ast = parse_query("trace(b9fe465597df3a11cc3e7fc4dbe44f00)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Traces);
        assert_eq!(ast.stages.len(), 2);
        if let Stage::Filter(FilterStage { predicate }) = &ast.stages[0] {
            if let Predicate::Compare(CompareExpr { left, right, .. }) = predicate {
                assert_eq!(
                    left,
                    &Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "trace_id".into()
                    })
                );
                assert_eq!(
                    right,
                    &Expr::Literal(Literal::String("b9fe465597df3a11cc3e7fc4dbe44f00".into()))
                );
            } else {
                panic!("expected Compare predicate");
            }
        } else {
            panic!("expected Filter stage");
        }
    }

    #[test]
    fn parse_span_link_id_lookup() {
        // span_link must match before span
        let ast = parse_query("span_link(abc123)").unwrap();
        assert_eq!(ast.scan.signal, Signal::SpanLinks);
        assert_eq!(ast.stages.len(), 2);
        if let Stage::Filter(FilterStage { predicate }) = &ast.stages[0] {
            if let Predicate::Compare(CompareExpr { left, .. }) = predicate {
                assert_eq!(
                    left,
                    &Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "source_span_id".into()
                    })
                );
            } else {
                panic!("expected Compare predicate");
            }
        } else {
            panic!("expected Filter stage");
        }
    }

    #[test]
    fn parse_metric_id_lookup() {
        let ast = parse_query("metric(metric-abc123)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Metrics);
        assert_eq!(ast.stages.len(), 2);
        if let Stage::Filter(FilterStage { predicate }) = &ast.stages[0] {
            if let Predicate::Compare(CompareExpr { left, .. }) = predicate {
                assert_eq!(
                    left,
                    &Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "metric_id".into()
                    })
                );
            } else {
                panic!("expected Compare predicate");
            }
        } else {
            panic!("expected Filter stage");
        }
    }

    #[test]
    fn parse_profile_id_lookup() {
        let ast = parse_query("profile(prof-deadbeef)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Profiles);
        assert_eq!(ast.stages.len(), 2);
        if let Stage::Filter(FilterStage { predicate }) = &ast.stages[0] {
            if let Predicate::Compare(CompareExpr { left, .. }) = predicate {
                assert_eq!(
                    left,
                    &Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "profile_id".into()
                    })
                );
            } else {
                panic!("expected Compare predicate");
            }
        } else {
            panic!("expected Filter stage");
        }
    }

    #[test]
    fn parse_id_lookup_with_trailing_pipeline() {
        // id-lookup followed by additional pipeline stages
        let ast = parse_query("span(abc123) -> traces").unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
        // Filter + Limit (from lookup) + Navigate (appended)
        assert_eq!(ast.stages.len(), 3);
        assert!(matches!(ast.stages[0], Stage::Filter(_)));
        assert!(matches!(ast.stages[1], Stage::Limit(_)));
        assert!(matches!(
            ast.stages[2],
            Stage::Navigate(NavigateStage {
                target: Signal::Traces
            })
        ));
    }

    #[test]
    fn parse_id_lookup_uuid_with_hyphens() {
        let ast = parse_query("log(550e8400-e29b-41d4-a716-446655440000)").unwrap();
        assert_eq!(ast.scan.signal, Signal::Logs);
        if let Stage::Filter(FilterStage { predicate }) = &ast.stages[0] {
            if let Predicate::Compare(CompareExpr { right, .. }) = predicate {
                assert_eq!(
                    right,
                    &Expr::Literal(Literal::String(
                        "550e8400-e29b-41d4-a716-446655440000".into()
                    ))
                );
            } else {
                panic!("expected Compare predicate");
            }
        } else {
            panic!("expected Filter stage");
        }
    }

    #[test]
    fn parse_id_lookup_does_not_break_regular_queries() {
        // Regular plural-signal queries must still parse correctly
        let ast = parse_query("spans last 1h | where status == ERROR").unwrap();
        assert_eq!(ast.scan.signal, Signal::Spans);
        assert_eq!(ast.stages.len(), 1);
        assert!(matches!(ast.stages[0], Stage::Filter(_)));
    }

    // ── navigate / merge keyword alias tests ──────────────────────────────────

    #[test]
    fn test_navigate_keyword_parses() {
        // `navigate` keyword should produce the same AST as `->` after a pipe
        let ast_keyword = parse_query("spans last 1h | navigate traces").unwrap();
        let ast_arrow = parse_query("spans last 1h | -> traces").unwrap();

        assert_eq!(ast_keyword.scan.signal, Signal::Spans);
        assert_eq!(ast_keyword.stages.len(), 1);
        assert!(matches!(
            ast_keyword.stages[0],
            Stage::Navigate(NavigateStage {
                target: Signal::Traces
            })
        ));
        // Both forms produce equivalent ASTs
        assert_eq!(
            ast_keyword.stages.len(),
            ast_arrow.stages.len(),
            "keyword and arrow forms should produce same number of stages"
        );
        if let (Stage::Navigate(kw), Stage::Navigate(arrow)) =
            (&ast_keyword.stages[0], &ast_arrow.stages[0])
        {
            assert_eq!(kw.target, arrow.target);
        } else {
            panic!("expected Navigate stage from both forms");
        }
    }

    #[test]
    fn test_merge_keyword_parses() {
        // `merge` keyword should produce the same AST as `<-` after a pipe
        let ast_keyword = parse_query("spans last 1h | merge logs as l").unwrap();
        let ast_arrow = parse_query("spans last 1h | <- logs as l").unwrap();

        assert_eq!(ast_keyword.scan.signal, Signal::Spans);
        assert_eq!(ast_keyword.stages.len(), 1);
        if let Stage::Merge(m) = &ast_keyword.stages[0] {
            assert_eq!(m.target, Signal::Logs);
            assert_eq!(m.alias, "l");
            assert!(m.stages.is_empty());
        } else {
            panic!("expected Merge stage");
        }
        // Both forms produce equivalent ASTs
        if let (Stage::Merge(kw), Stage::Merge(arrow)) =
            (&ast_keyword.stages[0], &ast_arrow.stages[0])
        {
            assert_eq!(kw.target, arrow.target);
            assert_eq!(kw.alias, arrow.alias);
        } else {
            panic!("expected Merge stage from both forms");
        }
    }

    #[test]
    fn test_merge_keyword_with_subpipeline() {
        // `merge` with a parenthesised sub-pipeline
        let ast =
            parse_query("spans last 1h | merge (logs | where severity_text == ERROR) as errs")
                .unwrap();

        assert_eq!(ast.scan.signal, Signal::Spans);
        assert_eq!(ast.stages.len(), 1);
        if let Stage::Merge(m) = &ast.stages[0] {
            assert_eq!(m.target, Signal::Logs);
            assert_eq!(m.alias, "errs");
            assert_eq!(m.stages.len(), 1, "sub-pipeline should have 1 filter stage");
            assert!(matches!(m.stages[0], Stage::Filter(_)));
        } else {
            panic!("expected Merge stage");
        }
    }

    #[test]
    fn parse_aggregate_with_filter() {
        let ast =
            parse_query("spans last 1h | group by {} { count() where status == 2 as errors }")
                .unwrap();
        if let Stage::Aggregate(agg) = &ast.stages[0] {
            assert_eq!(agg.aggregations.len(), 1);
            assert_eq!(agg.aggregations[0].alias, "errors");
            assert!(agg.aggregations[0].filter.is_some());
        } else {
            panic!("expected aggregate stage");
        }
    }

    #[test]
    fn parse_aggregate_filter_on_attr() {
        let ast = parse_query(
            "spans last 1h | group by {} { avg(duration_ns) where attr.http_status_code >= 500 as avg_5xx_dur }",
        )
        .unwrap();
        if let Stage::Aggregate(agg) = &ast.stages[0] {
            assert_eq!(agg.aggregations.len(), 1);
            assert_eq!(agg.aggregations[0].alias, "avg_5xx_dur");
            assert!(agg.aggregations[0].filter.is_some());
        } else {
            panic!("expected aggregate stage");
        }
    }

    #[test]
    fn parse_aggregate_without_filter_still_works() {
        let ast = parse_query("spans last 1h | group by {} { count() as total }").unwrap();
        if let Stage::Aggregate(agg) = &ast.stages[0] {
            assert_eq!(agg.aggregations.len(), 1);
            assert_eq!(agg.aggregations[0].alias, "total");
            assert!(agg.aggregations[0].filter.is_none());
        } else {
            panic!("expected aggregate stage");
        }
    }

    #[test]
    fn parse_aggregate_mixed_filtered_and_unfiltered() {
        let ast = parse_query(
            "spans last 1h | group by {} { count() as total, count() where status == 2 as errors }",
        )
        .unwrap();
        if let Stage::Aggregate(agg) = &ast.stages[0] {
            assert_eq!(agg.aggregations.len(), 2);
            assert!(agg.aggregations[0].filter.is_none());
            assert!(agg.aggregations[1].filter.is_some());
        } else {
            panic!("expected aggregate stage");
        }
    }
}
