use serde::{Deserialize, Serialize};

/// A complete SeQL query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryAst {
    /// Named sub-queries that can be referenced in the main pipeline
    pub bindings: Vec<Binding>,
    /// The primary signal and time range to scan
    pub scan: Scan,
    /// Pipeline stages applied in order
    pub stages: Vec<Stage>,
    /// Execution mode: snapshot or live
    pub mode: QueryMode,
}

/// A named sub-query binding (`let name = signal | stages`)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    /// Binding name referenced later in the pipeline
    pub name: String,
    /// Signal scan for this binding
    pub scan: Scan,
    /// Pipeline stages for this binding
    pub stages: Vec<Stage>,
}

/// Query execution mode
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum QueryMode {
    /// Execute once and return a complete result
    #[default]
    Snapshot,
    /// Subscribe to live updates from the WAL broadcast channel
    Live,
}

/// Signal source and time range to scan
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scan {
    /// Which signal type to scan
    pub signal: Signal,
    /// Time range to scan
    pub time_range: TimeRange,
}

/// Signal type to query
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Signal {
    /// Distributed tracing spans
    Spans,
    /// Span link relationships
    SpanLinks,
    /// Structured log entries
    Logs,
    /// Scalar metric data points
    Datapoints,
    /// Histogram metric data points
    Histograms,
    /// Metric metadata
    Metrics,
    /// Profiling samples
    Samples,
    /// Full traces (root + all child spans)
    Traces,
    /// Profiling metadata per collection
    Profiles,
    /// Deduplicated call stacks
    Stacks,
    /// Deduplicated call frames
    Frames,
    /// Deduplicated binary/library mappings
    Mappings,
    /// OTLP resource entities
    Resources,
    /// OTLP instrumentation scopes
    Scopes,
}

/// Time range specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimeRange {
    /// Explicit start/end in nanoseconds since epoch
    Absolute {
        /// Start timestamp (nanoseconds since Unix epoch)
        start_ns: u64,
        /// End timestamp (nanoseconds since Unix epoch)
        end_ns: u64,
    },
    /// Sliding window from `start_ns` to now
    SlidingWindow {
        /// Window start offset in nanoseconds (duration from now)
        start_ns: u64,
    },
}

/// A pipeline stage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stage {
    /// Filter rows by a predicate
    Filter(FilterStage),
    /// Select a subset of fields
    Project(ProjectStage),
    /// Add computed columns
    Compute(ComputeStage),
    /// Aggregate and group rows
    Aggregate(AggregateStage),
    /// Sort rows
    Sort(SortStage),
    /// Limit the number of rows
    Limit(LimitStage),
    /// Navigate to a related signal
    Navigate(NavigateStage),
    /// Deduplicate rows by a field
    Unique(UniqueStage),
    /// Detect patterns in log lines
    Patterns(PatternsStage),
    /// Merge a correlated sub-query as a nested collection column
    Merge(MergeStage),
    /// Filter by time range (sliding window)
    TimeRange(TimeRangeStage),
}

// ── Filter ────────────────────────────────────────────────────────────────────

/// Filter stage — retain only rows matching the predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterStage {
    /// Boolean predicate to evaluate for each row
    pub predicate: Predicate,
}

/// A boolean predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    /// Both sub-predicates must be true
    And(Box<Predicate>, Box<Predicate>),
    /// At least one sub-predicate must be true
    Or(Box<Predicate>, Box<Predicate>),
    /// Sub-predicate must be false
    Not(Box<Predicate>),
    /// Comparison between two expressions
    Compare(CompareExpr),
    /// Field exists (non-null)
    Exists(FieldRef),
    /// Field does not exist (null)
    NotExists(FieldRef),
    /// String field contains a substring
    Contains {
        /// Field to test
        field: FieldRef,
        /// Substring to search for
        value: String,
    },
    /// String field starts with a prefix
    StartsWith {
        /// Field to test
        field: FieldRef,
        /// Prefix to match
        value: String,
    },
    /// String field ends with a suffix
    EndsWith {
        /// Field to test
        field: FieldRef,
        /// Suffix to match
        value: String,
    },
    /// String field matches a regex
    Matches {
        /// Field to test
        field: FieldRef,
        /// Regular expression pattern
        pattern: String,
    },
    /// Field value is in a set of literals
    In {
        /// Field to test
        field: FieldRef,
        /// Set of acceptable values
        values: Vec<Literal>,
    },
}

/// Comparison expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompareExpr {
    /// Left-hand side
    pub left: Expr,
    /// Comparison operator
    pub op: CompareOp,
    /// Right-hand side
    pub right: Expr,
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    /// Equal (`==`)
    Eq,
    /// Not equal (`!=`)
    Neq,
    /// Greater than (`>`)
    Gt,
    /// Greater than or equal (`>=`)
    Gte,
    /// Less than (`<`)
    Lt,
    /// Less than or equal (`<=`)
    Lte,
}

// ── Expressions ───────────────────────────────────────────────────────────────

/// A scalar expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Reference to a signal or attribute field
    Field(FieldRef),
    /// A literal value
    Literal(Literal),
    /// Binary arithmetic operation
    BinaryOp {
        /// Left operand
        left: Box<Expr>,
        /// Operator
        op: ArithOp,
        /// Right operand
        right: Box<Expr>,
    },
    /// A scalar function call
    FunctionCall {
        /// Function name
        function: ScalarFn,
        /// Arguments
        args: Vec<Expr>,
    },
}

/// Arithmetic operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArithOp {
    /// Addition
    Add,
    /// Subtraction
    Sub,
    /// Multiplication
    Mul,
    /// Division
    Div,
    /// Modulo
    Mod,
}

/// Built-in scalar functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarFn {
    /// Absolute value
    Abs,
    /// Round to nearest integer
    Round,
    /// Ceiling
    Ceil,
    /// Floor
    Floor,
    /// Milliseconds from a duration/timestamp
    ToMillis,
    /// Seconds from a duration/timestamp
    ToSeconds,
    /// Convert to string
    ToString,
    /// String length
    Len,
    /// Lowercase
    Lower,
    /// Uppercase
    Upper,
    /// The signal's primary time column (resolved at compile time)
    Timestamp,
}

/// A reference to a field on a signal row or its attributes
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldRef {
    /// Which attribute bag the field lives in
    pub scope: AttrScope,
    /// Field name
    pub name: String,
}

/// Attribute scope for field resolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttrScope {
    /// Direct signal field (e.g. `duration`, `status`)
    Signal,
    /// OTLP resource attribute
    Resource,
    /// OTLP scope/instrumentation library attribute
    Scope,
    /// OTLP span/log/metric attribute
    Attribute,
    /// Resolve automatically: signal field first, then attribute
    Auto,
}

// ── Literals ──────────────────────────────────────────────────────────────────

/// A scalar literal value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    /// SQL NULL
    Null,
    /// Boolean
    Bool(bool),
    /// Signed integer
    Int(i64),
    /// Unsigned integer
    UInt(u64),
    /// Floating-point number
    Float(f64),
    /// UTF-8 string
    String(String),
    /// Duration in nanoseconds
    Duration(u64),
    /// Timestamp in nanoseconds since epoch
    Timestamp(u64),
    /// Span/trace status
    Status(StatusLiteral),
    /// Span kind
    SpanKind(SpanKindLiteral),
    /// Log severity
    Severity(SeverityLiteral),
}

/// Status literal for use in query expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusLiteral {
    /// Status not set
    Unset,
    /// Successful
    Ok,
    /// Error
    Error,
}

/// Span kind literal for use in query expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpanKindLiteral {
    /// Internal span
    Internal,
    /// Server-side span
    Server,
    /// Client-side span
    Client,
    /// Message producer
    Producer,
    /// Message consumer
    Consumer,
}

/// Log severity literal for use in query expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SeverityLiteral {
    /// Trace severity
    Trace,
    /// Debug severity
    Debug,
    /// Informational
    Info,
    /// Warning
    Warn,
    /// Error
    Error,
    /// Fatal
    Fatal,
}

// ── Stages (continued) ────────────────────────────────────────────────────────

/// Project stage — select a subset of fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectStage {
    /// Fields to include in the output
    pub fields: Vec<ProjectField>,
}

/// A projected field, optionally renamed
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectField {
    /// The field to project
    pub field: FieldRef,
    /// Optional alias for the output column
    pub alias: Option<String>,
}

/// Compute stage — add derived columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComputeStage {
    /// Columns to derive
    pub derivations: Vec<Derivation>,
}

/// A derived column definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Derivation {
    /// Output column name
    pub alias: String,
    /// Expression to compute
    pub expr: Expr,
}

/// Aggregate stage — group and aggregate rows
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateStage {
    /// Fields to group by
    pub group_by: Vec<GroupExpr>,
    /// Aggregations to compute
    pub aggregations: Vec<Aggregation>,
}

/// An expression used for grouping
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupExpr {
    /// Expression to group by
    pub expr: Expr,
    /// Optional alias for the group key column
    pub alias: Option<String>,
    /// Optional time bin width in nanoseconds (for time bucketing)
    pub bin_ns: Option<u64>,
}

/// An aggregation computation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Aggregation {
    /// The aggregate function and its arguments
    pub function: AggregateFn,
    /// Output column name
    pub alias: String,
    /// Optional filter predicate applied before aggregating
    pub filter: Option<Predicate>,
}

/// Aggregate functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFn {
    /// Count of rows
    Count,
    /// Sum of a field
    Sum(Expr),
    /// Average of a field
    Avg(Expr),
    /// Minimum of a field
    Min(Expr),
    /// Maximum of a field
    Max(Expr),
    /// 50th percentile of a field
    P50(Expr),
    /// 95th percentile of a field
    P95(Expr),
    /// 99th percentile of a field
    P99(Expr),
    /// Error rate (fraction of rows with status=Error)
    ErrorRate,
    /// Request throughput (rows per second)
    Throughput,
    /// Distinct values of a field
    Distinct(Expr),
    /// Heatmap distribution of a field
    Heatmap(Expr),
    /// Random sample of n rows
    Sample(u64),
    /// First value of a field (by insertion order)
    First(Expr),
    /// Last value of a field (by insertion order)
    Last(Expr),
}

/// Sort stage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortStage {
    /// Sort expressions in priority order
    pub exprs: Vec<SortExpr>,
}

/// A single sort expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortExpr {
    /// Expression to sort by
    pub expr: Expr,
    /// Sort direction
    pub ascending: bool,
}

/// Limit stage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitStage {
    /// Maximum number of rows to return
    pub limit: u64,
    /// Number of rows to skip
    pub offset: Option<u64>,
}

/// Navigate stage — follow correlations to another signal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NavigateStage {
    /// Target signal to navigate to
    pub target: Signal,
}

/// Unique stage — deduplicate by a field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UniqueStage {
    /// Field to deduplicate on
    pub field: FieldRef,
}

/// Patterns stage — detect patterns in string fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatternsStage {
    /// Field to run pattern detection on (defaults to log body)
    pub field: Option<FieldRef>,
}

/// Merge stage — enrich each outer row with a nested collection from a correlated sub-query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeStage {
    /// Target signal to merge from
    pub target: Signal,
    /// Pipeline stages to apply to the inner sub-query (empty = no filtering)
    pub stages: Vec<Stage>,
    /// Alias for the merged column (defaults to the signal name, e.g. "logs")
    pub alias: String,
}

/// TimeRange stage — filter rows by a sliding time window
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeRangeStage {
    /// Duration in nanoseconds to look back from now
    pub duration_ns: u64,
}

impl Signal {
    /// Returns the primary ID column name for this signal type.
    /// Used by the `signal(id)` shorthand syntax to build a filter predicate.
    pub fn join_key(&self) -> &'static str {
        match self {
            Signal::Spans => "span_id",
            Signal::SpanLinks => "source_span_id",
            Signal::Logs => "log_id",
            Signal::Datapoints => "metric_id",
            Signal::Histograms => "metric_id",
            Signal::Metrics => "metric_id",
            Signal::Samples => "profile_id",
            Signal::Traces => "trace_id",
            Signal::Profiles => "profile_id",
            Signal::Stacks => "stack_id",
            Signal::Frames => "frame_id",
            Signal::Mappings => "mapping_id",
            Signal::Resources => "resource_id",
            Signal::Scopes => "scope_id",
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_query() -> QueryAst {
        QueryAst {
            bindings: vec![],
            scan: Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![
                Stage::Filter(FilterStage {
                    predicate: Predicate::Compare(CompareExpr {
                        left: Expr::Field(FieldRef {
                            scope: AttrScope::Signal,
                            name: "status".into(),
                        }),
                        op: CompareOp::Eq,
                        right: Expr::Literal(Literal::Status(StatusLiteral::Error)),
                    }),
                }),
                Stage::Sort(SortStage {
                    exprs: vec![SortExpr {
                        expr: Expr::Field(FieldRef {
                            scope: AttrScope::Signal,
                            name: "duration".into(),
                        }),
                        ascending: false,
                    }],
                }),
                Stage::Limit(LimitStage {
                    limit: 100,
                    offset: None,
                }),
            ],
            mode: QueryMode::Snapshot,
        }
    }

    #[test]
    fn query_ast_round_trip() {
        let q = sample_query();
        let json = serde_json::to_string(&q).unwrap();
        let back: QueryAst = serde_json::from_str(&json).unwrap();
        assert_eq!(q, back);
    }

    #[test]
    fn all_signals_round_trip() {
        let signals = [
            Signal::Spans,
            Signal::SpanLinks,
            Signal::Logs,
            Signal::Datapoints,
            Signal::Histograms,
            Signal::Metrics,
            Signal::Samples,
            Signal::Traces,
            Signal::Profiles,
            Signal::Stacks,
            Signal::Frames,
            Signal::Mappings,
            Signal::Resources,
            Signal::Scopes,
        ];
        for s in &signals {
            let json = serde_json::to_string(s).unwrap();
            let back: Signal = serde_json::from_str(&json).unwrap();
            assert_eq!(s, &back);
        }
    }

    #[test]
    fn all_literals_round_trip() {
        let lits = vec![
            Literal::Null,
            Literal::Bool(true),
            Literal::Int(-42),
            Literal::UInt(99),
            Literal::Float(1.5),
            Literal::String("hello".into()),
            Literal::Duration(1_000_000_000),
            Literal::Timestamp(1_700_000_000_000_000_000),
            Literal::Status(StatusLiteral::Error),
            Literal::SpanKind(SpanKindLiteral::Server),
            Literal::Severity(SeverityLiteral::Warn),
        ];
        for lit in &lits {
            let json = serde_json::to_string(lit).unwrap();
            let back: Literal = serde_json::from_str(&json).unwrap();
            assert_eq!(lit, &back);
        }
    }

    #[test]
    fn aggregate_stage_round_trip() {
        let stage = Stage::Aggregate(AggregateStage {
            group_by: vec![GroupExpr {
                expr: Expr::Field(FieldRef {
                    scope: AttrScope::Auto,
                    name: "service_name".into(),
                }),
                alias: Some("service".into()),
                bin_ns: None,
            }],
            aggregations: vec![
                Aggregation {
                    function: AggregateFn::Count,
                    alias: "count".into(),
                    filter: None,
                },
                Aggregation {
                    function: AggregateFn::P99(Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "duration".into(),
                    })),
                    alias: "p99".into(),
                    filter: None,
                },
                Aggregation {
                    function: AggregateFn::ErrorRate,
                    alias: "error_rate".into(),
                    filter: None,
                },
            ],
        });
        let json = serde_json::to_string(&stage).unwrap();
        let back: Stage = serde_json::from_str(&json).unwrap();
        assert_eq!(stage, back);
    }

    #[test]
    fn predicate_round_trip() {
        let pred = Predicate::And(
            Box::new(Predicate::Compare(CompareExpr {
                left: Expr::Field(FieldRef {
                    scope: AttrScope::Auto,
                    name: "service_name".into(),
                }),
                op: CompareOp::Eq,
                right: Expr::Literal(Literal::String("api".into())),
            })),
            Box::new(Predicate::Or(
                Box::new(Predicate::Exists(FieldRef {
                    scope: AttrScope::Attribute,
                    name: "http.method".into(),
                })),
                Box::new(Predicate::Contains {
                    field: FieldRef {
                        scope: AttrScope::Auto,
                        name: "body".into(),
                    },
                    value: "error".into(),
                }),
            )),
        );
        let json = serde_json::to_string(&pred).unwrap();
        let back: Predicate = serde_json::from_str(&json).unwrap();
        assert_eq!(pred, back);
    }

    #[test]
    fn binding_round_trip() {
        let binding = Binding {
            name: "errors".into(),
            scan: Scan {
                signal: Signal::Spans,
                time_range: TimeRange::Absolute {
                    start_ns: 1_700_000_000_000_000_000,
                    end_ns: 1_700_003_600_000_000_000,
                },
            },
            stages: vec![Stage::Filter(FilterStage {
                predicate: Predicate::Compare(CompareExpr {
                    left: Expr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "status".into(),
                    }),
                    op: CompareOp::Eq,
                    right: Expr::Literal(Literal::Status(StatusLiteral::Error)),
                }),
            })],
        };
        let json = serde_json::to_string(&binding).unwrap();
        let back: Binding = serde_json::from_str(&json).unwrap();
        assert_eq!(binding, back);
    }
}
