use crate::attr::field_ref;
use crate::lexer::{
    int_literal, keyword_ci, quoted_string, uint_literal, ws, ws1, KW_FALSE, KW_NULL, KW_TRUE,
    LIT_CLIENT, LIT_CONSUMER, LIT_DEBUG, LIT_ERROR, LIT_FATAL, LIT_INFO, LIT_INTERNAL, LIT_OK,
    LIT_PRODUCER, LIT_SERVER, LIT_TRACE, LIT_UNSET, LIT_WARN,
};
use seql_ast::ast::{
    ArithOp, CaseBranch, CompareExpr, CompareOp, Expr, Literal, Predicate, ScalarFn,
    SeverityLiteral, SpanKindLiteral, StatusLiteral,
};
use winnow::combinator::{alt, delimited, opt, preceded, separated};
use winnow::error::{ContextError, ErrMode};
use winnow::token::{literal, take_while};
use winnow::{ModalResult, Parser};

// ── AST construction helpers (used by score()/grade() desugaring) ───────────────

fn e_lit(x: f64) -> Expr {
    Expr::Literal(Literal::Float(x))
}

fn e_bin(left: Expr, op: ArithOp, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn p_cmp(left: Expr, op: CompareOp, right: Expr) -> Predicate {
    Predicate::Compare(CompareExpr { left, op, right })
}

fn e_call(function: ScalarFn, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall { function, args }
}

/// Map a lowercase identifier to a built-in scalar function, or `None` if it isn't one.
fn scalar_fn_by_name(name: &str) -> Option<ScalarFn> {
    Some(match name {
        "abs" => ScalarFn::Abs,
        "round" => ScalarFn::Round,
        "ceil" => ScalarFn::Ceil,
        "floor" => ScalarFn::Floor,
        "to_millis" => ScalarFn::ToMillis,
        "to_seconds" => ScalarFn::ToSeconds,
        "to_string" => ScalarFn::ToString,
        "len" => ScalarFn::Len,
        "lower" => ScalarFn::Lower,
        "upper" => ScalarFn::Upper,
        "exp" => ScalarFn::Exp,
        "coalesce" => ScalarFn::Coalesce,
        "least" => ScalarFn::Least,
        "greatest" => ScalarFn::Greatest,
        "float" => ScalarFn::ToFloat,
        "int" => ScalarFn::ToInt,
        _ => return None,
    })
}

/// Expand `score(value, warn, err)` into the health scoring piecewise CASE (higher
/// value = worse): linear 1.0→0.7 below `warn`, linear 0.7→0.3 between `warn` and
/// `err`, then `greatest(0, 0.3·exp(-(value-err)/err))` above `err`. Pure sugar over
/// `Expr::Case` + arithmetic + `exp()`, so it stays plain-SQL-expressible.
fn build_score_expr(v: Expr, w: Expr, e: Expr) -> Expr {
    // 1.0 - (v / w) * 0.3
    let healthy = e_bin(
        e_lit(1.0),
        ArithOp::Sub,
        e_bin(
            e_bin(v.clone(), ArithOp::Div, w.clone()),
            ArithOp::Mul,
            e_lit(0.3),
        ),
    );
    // 0.7 - ((v - w) / (e - w)) * 0.4
    let degraded = e_bin(
        e_lit(0.7),
        ArithOp::Sub,
        e_bin(
            e_bin(
                e_bin(v.clone(), ArithOp::Sub, w.clone()),
                ArithOp::Div,
                e_bin(e.clone(), ArithOp::Sub, w.clone()),
            ),
            ArithOp::Mul,
            e_lit(0.4),
        ),
    );
    // greatest(0.0, 0.3 * exp((e - v) / e))   [ = 0.3 * exp(-(v-e)/e) ]
    let unhealthy = e_call(
        ScalarFn::Greatest,
        vec![
            e_lit(0.0),
            e_bin(
                e_lit(0.3),
                ArithOp::Mul,
                e_call(
                    ScalarFn::Exp,
                    vec![e_bin(
                        e_bin(e.clone(), ArithOp::Sub, v.clone()),
                        ArithOp::Div,
                        e.clone(),
                    )],
                ),
            ),
        ],
    );
    Expr::Case {
        branches: vec![
            CaseBranch {
                condition: p_cmp(v.clone(), CompareOp::Lte, w),
                result: healthy,
            },
            CaseBranch {
                condition: p_cmp(v, CompareOp::Lte, e),
                result: degraded,
            },
        ],
        otherwise: Some(Box::new(unhealthy)),
    }
}

/// Expand `grade(value, warn, err)` into a CASE yielding `'healthy'` / `'degraded'`
/// / `'unhealthy'` — the status label matching `score()`'s thresholds.
fn build_grade_expr(v: Expr, w: Expr, e: Expr) -> Expr {
    Expr::Case {
        branches: vec![
            CaseBranch {
                condition: p_cmp(v.clone(), CompareOp::Lte, w),
                result: Expr::Literal(Literal::String("healthy".into())),
            },
            CaseBranch {
                condition: p_cmp(v, CompareOp::Lte, e),
                result: Expr::Literal(Literal::String("degraded".into())),
            },
        ],
        otherwise: Some(Box::new(Expr::Literal(Literal::String("unhealthy".into())))),
    }
}

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

/// Parse a parenthesised sub-expression: `( <expr> )` — arithmetic grouping.
fn parse_paren_expr(input: &mut &str) -> ModalResult<Expr> {
    delimited((literal("("), ws), parse_expr, (ws, literal(")"))).parse_next(input)
}

/// Parse a built-in scalar function call `name(arg, …)` (abs, round, exp, coalesce,
/// least, greatest, float, int, …). Backtracks when `name` isn't a known function so a
/// same-named field still parses as a field reference.
fn parse_scalar_call(input: &mut &str) -> ModalResult<Expr> {
    let name = take_while(1.., |c: char| c.is_ascii_lowercase() || c == '_').parse_next(input)?;
    let Some(function) = scalar_fn_by_name(name) else {
        return Err(ErrMode::Backtrack(ContextError::new()));
    };
    ws.parse_next(input)?;
    literal("(").parse_next(input)?;
    let args: Vec<Expr> =
        separated(0.., delimited(ws, parse_expr, ws), literal(",")).parse_next(input)?;
    ws.parse_next(input)?;
    literal(")").parse_next(input)?;
    Ok(Expr::FunctionCall { function, args })
}

/// Parse the `score(value, warn, err)` / `grade(value, warn, err)` sugar and expand it
/// into an `Expr::Case`. Both take exactly three arguments.
fn parse_score_grade(input: &mut &str) -> ModalResult<Expr> {
    let name = take_while(1.., |c: char| c.is_ascii_lowercase()).parse_next(input)?;
    let is_score = match name {
        "score" => true,
        "grade" => false,
        _ => return Err(ErrMode::Backtrack(ContextError::new())),
    };
    ws.parse_next(input)?;
    literal("(").parse_next(input)?;
    let args: Vec<Expr> =
        separated(1.., delimited(ws, parse_expr, ws), literal(",")).parse_next(input)?;
    ws.parse_next(input)?;
    literal(")").parse_next(input)?;
    if args.len() != 3 {
        // A hard error (not a backtrack) so the arity mistake surfaces clearly.
        return Err(ErrMode::Cut(ContextError::new()));
    }
    let mut it = args.into_iter();
    let v = it.next().expect("3 args");
    let w = it.next().expect("3 args");
    let e = it.next().expect("3 args");
    Ok(if is_score {
        build_score_expr(v, w, e)
    } else {
        build_grade_expr(v, w, e)
    })
}

/// Parse a `CASE WHEN <predicate> THEN <expr> … [ELSE <expr>] END` expression.
fn parse_case(input: &mut &str) -> ModalResult<Expr> {
    keyword_ci("case").parse_next(input)?;
    ws1.parse_next(input)?;

    // Sub-parsers (parse_predicate / parse_expr) consume their own surrounding
    // whitespace, so inter-token spacing here is `ws` (zero-or-more), not `ws1`.
    let mut branches = Vec::new();
    loop {
        ws.parse_next(input)?;
        if opt(keyword_ci("when")).parse_next(input)?.is_none() {
            break;
        }
        let condition = parse_predicate.parse_next(input)?;
        ws.parse_next(input)?;
        keyword_ci("then").parse_next(input)?;
        let result = parse_expr.parse_next(input)?;
        branches.push(CaseBranch { condition, result });
    }
    if branches.is_empty() {
        return Err(ErrMode::Cut(ContextError::new()));
    }

    ws.parse_next(input)?;
    let otherwise = if opt(keyword_ci("else")).parse_next(input)?.is_some() {
        Some(Box::new(parse_expr.parse_next(input)?))
    } else {
        None
    };
    ws.parse_next(input)?;
    keyword_ci("end").parse_next(input)?;

    Ok(Expr::Case {
        branches,
        otherwise,
    })
}

/// Parse a primary expression: CASE, `score`/`grade` sugar, a parenthesised
/// sub-expression, a scalar function call, `ts()`, a literal, or a field reference.
fn parse_primary(input: &mut &str) -> ModalResult<Expr> {
    ws.parse_next(input)?;
    alt((
        parse_case,
        parse_score_grade,
        parse_paren_expr,
        parse_ts_fn,
        parse_scalar_call,
        literal_value.map(Expr::Literal),
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
    fn parse_paren_grouping() {
        // Parentheses force grouping against the default precedence.
        let expr = parse_expr.parse("(a + b) * c").unwrap();
        assert!(
            matches!(
                expr,
                Expr::BinaryOp {
                    op: ArithOp::Mul,
                    ..
                }
            ),
            "top op should be Mul over the parenthesised sum, got {:?}",
            expr
        );
    }

    #[test]
    fn parse_scalar_call_exp() {
        let expr = parse_expr.parse("exp(x)").unwrap();
        assert!(matches!(
            expr,
            Expr::FunctionCall {
                function: ScalarFn::Exp,
                ..
            }
        ));
    }

    #[test]
    fn parse_scalar_call_variadic_greatest() {
        let expr = parse_expr.parse("greatest(0.0, x, y)").unwrap();
        match expr {
            Expr::FunctionCall {
                function: ScalarFn::Greatest,
                args,
            } => assert_eq!(args.len(), 3),
            other => panic!("expected greatest() call, got {:?}", other),
        }
    }

    #[test]
    fn known_fn_name_as_bare_field_still_parses() {
        // `exp` without parentheses is a plain field, not a call.
        let expr = parse_expr.parse("exp").unwrap();
        assert!(matches!(expr, Expr::Field(_)));
    }

    #[test]
    fn parse_case_expression() {
        let expr = parse_expr
            .parse("case when x <= 1 then 1.0 when x <= 2 then 0.5 else 0.0 end")
            .unwrap();
        match expr {
            Expr::Case {
                branches,
                otherwise,
            } => {
                assert_eq!(branches.len(), 2);
                assert!(otherwise.is_some());
            }
            other => panic!("expected Case, got {:?}", other),
        }
    }

    #[test]
    fn parse_case_without_else() {
        let expr = parse_expr.parse("case when x <= 1 then 1.0 end").unwrap();
        match expr {
            Expr::Case {
                branches,
                otherwise,
            } => {
                assert_eq!(branches.len(), 1);
                assert!(otherwise.is_none());
            }
            other => panic!("expected Case, got {:?}", other),
        }
    }

    #[test]
    fn score_desugars_to_case() {
        let expr = parse_expr.parse("score(rate, 0.01, 0.05)").unwrap();
        match expr {
            Expr::Case {
                branches,
                otherwise,
            } => {
                // Two thresholds → two WHEN branches + an exp-decay ELSE.
                assert_eq!(branches.len(), 2);
                assert!(otherwise.is_some());
            }
            other => panic!("score() should expand to Case, got {:?}", other),
        }
    }

    #[test]
    fn grade_desugars_to_case_of_strings() {
        let expr = parse_expr.parse("grade(rate, 0.01, 0.05)").unwrap();
        match expr {
            Expr::Case { branches, .. } => {
                assert!(matches!(
                    branches[0].result,
                    Expr::Literal(Literal::String(_))
                ));
            }
            other => panic!("grade() should expand to Case, got {:?}", other),
        }
    }

    #[test]
    fn score_wrong_arity_is_error() {
        assert!(parse_expr.parse("score(rate, 0.01)").is_err());
    }

    #[test]
    fn parse_ts_fn_in_arithmetic() {
        // ts() can appear in arithmetic expressions
        let expr = parse_expr.parse("ts() + 1000").unwrap();
        assert!(
            matches!(
                expr,
                Expr::BinaryOp {
                    op: ArithOp::Add,
                    ..
                }
            ),
            "ts() + 1000 should produce BinaryOp::Add"
        );
    }
}
