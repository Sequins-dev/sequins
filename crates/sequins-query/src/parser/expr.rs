use crate::ast::{
    ArithOp, CompareExpr, CompareOp, Expr, Literal, Predicate, ScalarFn, SeverityLiteral,
    SpanKindLiteral, StatusLiteral,
};
use crate::parser::attr::field_ref;
use crate::parser::lexer::{
    int_literal, keyword_ci, quoted_string, uint_literal, ws, ws1, KW_FALSE, KW_NULL, KW_TRUE,
    LIT_CLIENT, LIT_CONSUMER, LIT_DEBUG, LIT_ERROR, LIT_FATAL, LIT_INFO, LIT_INTERNAL, LIT_OK,
    LIT_PRODUCER, LIT_SERVER, LIT_TRACE, LIT_UNSET, LIT_WARN,
};
use winnow::combinator::{alt, delimited, opt, preceded, separated};
use winnow::token::literal;
use winnow::{ModalResult, Parser};

// ── Literals ─────────────────────────────────────────────────────────────────

/// Helper to parse a case-insensitive keyword and return a value
fn kw<'i, O>(
    keyword: &'static str,
    value: O,
) -> impl Parser<&'i str, O, winnow::error::ContextError>
where
    O: Clone,
{
    move |input: &mut &'i str| {
        keyword_ci(keyword).parse_next(input)?;
        Ok(value.clone())
    }
}

fn parse_null(input: &mut &str) -> ModalResult<Literal> {
    kw(KW_NULL, Literal::Null).parse_next(input)
}

fn parse_bool(input: &mut &str) -> ModalResult<Literal> {
    alt((
        kw(KW_TRUE, Literal::Bool(true)),
        kw(KW_FALSE, Literal::Bool(false)),
    ))
    .parse_next(input)
}

fn parse_status(input: &mut &str) -> ModalResult<Literal> {
    alt((
        literal(LIT_OK).value(Literal::Status(StatusLiteral::Ok)),
        literal(LIT_ERROR).value(Literal::Status(StatusLiteral::Error)),
        literal(LIT_UNSET).value(Literal::Status(StatusLiteral::Unset)),
    ))
    .parse_next(input)
}

fn parse_span_kind(input: &mut &str) -> ModalResult<Literal> {
    alt((
        literal(LIT_SERVER).value(Literal::SpanKind(SpanKindLiteral::Server)),
        literal(LIT_CLIENT).value(Literal::SpanKind(SpanKindLiteral::Client)),
        literal(LIT_PRODUCER).value(Literal::SpanKind(SpanKindLiteral::Producer)),
        literal(LIT_CONSUMER).value(Literal::SpanKind(SpanKindLiteral::Consumer)),
        literal(LIT_INTERNAL).value(Literal::SpanKind(SpanKindLiteral::Internal)),
    ))
    .parse_next(input)
}

fn parse_severity(input: &mut &str) -> ModalResult<Literal> {
    alt((
        literal(LIT_TRACE).value(Literal::Severity(SeverityLiteral::Trace)),
        literal(LIT_DEBUG).value(Literal::Severity(SeverityLiteral::Debug)),
        literal(LIT_INFO).value(Literal::Severity(SeverityLiteral::Info)),
        literal(LIT_WARN).value(Literal::Severity(SeverityLiteral::Warn)),
        literal(LIT_FATAL).value(Literal::Severity(SeverityLiteral::Fatal)),
        // ERROR is already matched by parse_status; FATAL/DEBUG etc are unambiguous
    ))
    .parse_next(input)
}

fn parse_float(input: &mut &str) -> ModalResult<Literal> {
    use winnow::ascii::float;
    let v: f64 = float.parse_next(input)?;
    Ok(Literal::Float(v))
}

fn parse_uint(input: &mut &str) -> ModalResult<Literal> {
    uint_literal.map(Literal::UInt).parse_next(input)
}

fn parse_int(input: &mut &str) -> ModalResult<Literal> {
    int_literal.map(Literal::Int).parse_next(input)
}

fn parse_string(input: &mut &str) -> ModalResult<Literal> {
    quoted_string.map(Literal::String).parse_next(input)
}

/// Parse any literal value
pub fn literal_value(input: &mut &str) -> ModalResult<Literal> {
    // Order matters: status/kind/severity before identifiers, float before int
    alt((
        parse_null,
        parse_bool,
        parse_status,
        parse_span_kind,
        parse_severity,
        parse_string,
        parse_float,
        parse_uint,
        parse_int,
    ))
    .parse_next(input)
}

// ── Expressions ───────────────────────────────────────────────────────────────

/// Parse `ts()` — zero-argument function that resolves to the signal's time column.
fn parse_ts_fn(input: &mut &str) -> ModalResult<Expr> {
    (keyword_ci("ts"), ws, literal("("), ws, literal(")")).parse_next(input)?;
    Ok(Expr::FunctionCall {
        function: ScalarFn::Timestamp,
        args: vec![],
    })
}

/// Parse a primary expression (literal, ts() call, or field reference)
fn parse_primary(input: &mut &str) -> ModalResult<Expr> {
    ws.parse_next(input)?;
    alt((
        literal_value.map(Expr::Literal),
        parse_ts_fn,
        field_ref.map(Expr::Field),
    ))
    .parse_next(input)
}

/// Parse a multiplicative expression (*, /, %)
fn parse_mul(input: &mut &str) -> ModalResult<Expr> {
    let mut left = parse_primary.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        let op = match opt(alt((
            literal("*").value(ArithOp::Mul),
            literal("/").value(ArithOp::Div),
            literal("%").value(ArithOp::Mod),
        )))
        .parse_next(input)?
        {
            Some(op) => op,
            None => break,
        };
        ws.parse_next(input)?;
        let right = parse_primary.parse_next(input)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

/// Parse an additive expression (+, -)
pub fn parse_expr(input: &mut &str) -> ModalResult<Expr> {
    let mut left = parse_mul.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        let op = match opt(alt((
            literal("+").value(ArithOp::Add),
            literal("-").value(ArithOp::Sub),
        )))
        .parse_next(input)?
        {
            Some(op) => op,
            None => break,
        };
        ws.parse_next(input)?;
        let right = parse_mul.parse_next(input)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

// ── Predicates ────────────────────────────────────────────────────────────────

fn parse_compare_op(input: &mut &str) -> ModalResult<CompareOp> {
    ws.parse_next(input)?;
    alt((
        literal("==").value(CompareOp::Eq),
        literal("!=").value(CompareOp::Neq),
        literal(">=").value(CompareOp::Gte),
        literal(">").value(CompareOp::Gt),
        literal("<=").value(CompareOp::Lte),
        literal("<").value(CompareOp::Lt),
        // SQL-style single `=` is an alias for `==`; must come after `==`
        literal("=").value(CompareOp::Eq),
    ))
    .parse_next(input)
}

/// Parse a comparison or string predicate
fn parse_atom_predicate(input: &mut &str) -> ModalResult<Predicate> {
    ws.parse_next(input)?;

    // `exists field`
    let exists_pred = preceded(
        (keyword_ci("exists"), ws1),
        field_ref.map(Predicate::Exists),
    );
    // `not_exists field`
    let not_exists_pred = preceded(
        (keyword_ci("not_exists"), ws1),
        field_ref.map(Predicate::NotExists),
    );

    // String predicates: `field contains "x"`, etc.
    let contains_pred = (field_ref, ws1, keyword_ci("contains"), ws1, quoted_string)
        .map(|(field, _, _, _, value)| Predicate::Contains { field, value });
    let starts_with_pred = (
        field_ref,
        ws1,
        keyword_ci("starts_with"),
        ws1,
        quoted_string,
    )
        .map(|(field, _, _, _, value)| Predicate::StartsWith { field, value });
    let ends_with_pred = (field_ref, ws1, keyword_ci("ends_with"), ws1, quoted_string)
        .map(|(field, _, _, _, value)| Predicate::EndsWith { field, value });
    let matches_pred = (field_ref, ws1, keyword_ci("matches"), ws1, quoted_string)
        .map(|(field, _, _, _, pattern)| Predicate::Matches { field, pattern });

    // `field in [v1, v2, ...]`
    let in_pred = (
        field_ref,
        ws,
        keyword_ci("in"),
        ws,
        delimited(
            literal("["),
            separated(
                0..,
                (ws, literal_value, ws).map(|(_, v, _)| v),
                literal(","),
            ),
            literal("]"),
        ),
    )
        .map(|(field, _, _, _, values)| Predicate::In { field, values });

    // Comparison: `expr op expr`
    let compare_pred = (parse_expr, parse_compare_op, parse_expr)
        .map(|(left, op, right)| Predicate::Compare(CompareExpr { left, op, right }));

    // Negation: `!pred` or `not pred`
    let not_pred = preceded(
        alt((literal("!"), (keyword_ci("not"), ws1).take())),
        parse_atom_predicate,
    )
    .map(|p| Predicate::Not(Box::new(p)));

    // Parenthesised predicate
    let paren_pred = delimited(literal("("), parse_predicate, literal(")")).map(|p| p);

    alt((
        exists_pred,
        not_exists_pred,
        not_pred,
        paren_pred,
        contains_pred,
        starts_with_pred,
        ends_with_pred,
        matches_pred,
        in_pred,
        compare_pred,
    ))
    .parse_next(input)
}

/// Parse a conjunction of predicates (`&&` / `and`)
fn parse_and_predicate(input: &mut &str) -> ModalResult<Predicate> {
    let mut left = parse_atom_predicate.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        let matched =
            opt(alt((literal("&&"), (keyword_ci("and"), ws1).take()))).parse_next(input)?;
        if matched.is_none() {
            break;
        }
        ws.parse_next(input)?;
        let right = parse_atom_predicate.parse_next(input)?;
        left = Predicate::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

/// Parse a disjunction of predicates (`||` / `or`)
pub fn parse_predicate(input: &mut &str) -> ModalResult<Predicate> {
    let mut left = parse_and_predicate.parse_next(input)?;
    loop {
        ws.parse_next(input)?;
        let matched =
            opt(alt((literal("||"), (keyword_ci("or"), ws1).take()))).parse_next(input)?;
        if matched.is_none() {
            break;
        }
        ws.parse_next(input)?;
        let right = parse_and_predicate.parse_next(input)?;
        left = Predicate::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_eq_status() {
        let pred = parse_predicate.parse("status == ERROR").unwrap();
        assert!(matches!(pred, Predicate::Compare(_)));
    }

    #[test]
    fn parse_and_predicate_test() {
        let pred = parse_predicate
            .parse("status == ERROR && duration > 1000")
            .unwrap();
        assert!(matches!(pred, Predicate::And(_, _)));
    }

    #[test]
    fn parse_or_predicate_test() {
        let pred = parse_predicate
            .parse("status == OK || status == ERROR")
            .unwrap();
        assert!(matches!(pred, Predicate::Or(_, _)));
    }

    #[test]
    fn parse_contains_predicate() {
        let pred = parse_predicate.parse(r#"body contains "error""#).unwrap();
        assert!(matches!(pred, Predicate::Contains { .. }));
    }

    #[test]
    fn parse_exists_predicate() {
        let pred = parse_predicate.parse("exists attr.http.method").unwrap();
        assert!(matches!(pred, Predicate::Exists(_)));
    }

    #[test]
    fn parse_in_predicate() {
        let pred = parse_predicate
            .parse(r#"service_name in ["api", "worker"]"#)
            .unwrap();
        assert!(matches!(pred, Predicate::In { .. }));
    }

    #[test]
    fn parse_literal_null() {
        assert!(matches!(
            literal_value.parse("null").unwrap(),
            Literal::Null
        ));
    }

    #[test]
    fn parse_literal_bool() {
        assert_eq!(literal_value.parse("true").unwrap(), Literal::Bool(true));
    }

    #[test]
    fn parse_literal_string() {
        assert_eq!(
            literal_value.parse(r#""hello""#).unwrap(),
            Literal::String("hello".into())
        );
    }

    #[test]
    fn parse_arith_expr() {
        let expr = parse_expr.parse("duration + 1000").unwrap();
        assert!(matches!(
            expr,
            Expr::BinaryOp {
                op: ArithOp::Add,
                ..
            }
        ));
    }

    #[test]
    fn test_operator_precedence_and_vs_or() {
        // AND binds tighter than OR, so:
        // `a == 1 OR b == 2 AND c == 3`
        // should parse as `(a == 1) OR ((b == 2) AND (c == 3))`
        let pred = parse_predicate
            .parse("a == 1 OR b == 2 AND c == 3")
            .unwrap();

        // The top-level node must be Or (a == 1) OR (...)
        match pred {
            Predicate::Or(left, right) => {
                // left is the a == 1 compare
                assert!(
                    matches!(*left, Predicate::Compare(_)),
                    "left of OR should be a Compare"
                );
                // right is the AND of b == 2 AND c == 3
                assert!(
                    matches!(*right, Predicate::And(_, _)),
                    "right of OR should be AND (AND binds tighter than OR)"
                );
            }
            other => panic!("expected Or at top level, got {:?}", other),
        }
    }

    #[test]
    fn parse_ts_fn_expression() {
        let expr = parse_expr.parse("ts()").unwrap();
        assert!(
            matches!(
                expr,
                Expr::FunctionCall {
                    function: ScalarFn::Timestamp,
                    ..
                }
            ),
            "ts() should parse as ScalarFn::Timestamp, got {:?}",
            expr
        );
    }

    #[test]
    fn parse_ts_fn_field_not_confused() {
        // `ts` without parentheses should parse as a plain field ref
        let expr = parse_expr.parse("ts").unwrap();
        assert!(
            matches!(expr, Expr::Field(_)),
            "ts without () should parse as a field ref"
        );
    }

    #[test]
    fn parse_ts_fn_in_arithmetic() {
        // ts() can appear in arithmetic expressions
        let expr = parse_expr.parse("ts() + 1000").unwrap();
        assert!(
            matches!(
                expr,
                Expr::BinaryOp {
                    op: crate::ast::ArithOp::Add,
                    ..
                }
            ),
            "ts() + 1000 should produce BinaryOp::Add"
        );
    }
}
