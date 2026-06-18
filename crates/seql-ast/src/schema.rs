use crate::ast::{AggregateFn, NavigateStage, QueryAst, Signal, Stage};
use serde::{Deserialize, Serialize};

/// The shape of a query result, determining how the UI renders it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseShape {
    /// Flat rows and columns
    Table,
    /// Time-series lines (time + one or more value columns)
    TimeSeries,
    /// 2-D heatmap (time × bucket)
    Heatmap,
    /// Hierarchical span tree
    TraceTree,
    /// Waterfall trace timeline
    TraceTimeline,
    /// Log pattern groups
    PatternGroups,
    /// Single scalar value
    Scalar,
}

impl ResponseShape {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResponseShape::Table => "table",
            ResponseShape::TimeSeries => "timeseries",
            ResponseShape::Heatmap => "heatmap",
            ResponseShape::TraceTree => "trace_tree",
            ResponseShape::TraceTimeline => "trace_timeline",
            ResponseShape::PatternGroups => "pattern_groups",
            ResponseShape::Scalar => "scalar",
        }
    }

    pub fn from_shape_str(s: &str) -> Option<Self> {
        match s {
            "table" => Some(ResponseShape::Table),
            "timeseries" => Some(ResponseShape::TimeSeries),
            "heatmap" => Some(ResponseShape::Heatmap),
            "trace_tree" => Some(ResponseShape::TraceTree),
            "trace_timeline" => Some(ResponseShape::TraceTimeline),
            "pattern_groups" => Some(ResponseShape::PatternGroups),
            "scalar" => Some(ResponseShape::Scalar),
            _ => None,
        }
    }
}

/// The data type of a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    /// UTF-8 string
    String,
    /// Signed 64-bit integer
    Int64,
    /// Unsigned 64-bit integer
    UInt64,
    /// 64-bit floating point
    Float64,
    /// Boolean
    Bool,
    /// Nanoseconds since Unix epoch
    Timestamp,
    /// Duration in nanoseconds
    Duration,
    /// Span/trace status
    Status,
    /// Span kind
    SpanKind,
    /// Log severity
    Severity,
    /// Ordered list of values
    List(Box<DataType>),
    /// Named fields
    Struct(Vec<ColumnDef>),
}

/// The semantic role of a column in the result set
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnRole {
    /// A group-by key
    GroupKey,
    /// An aggregate computation result
    Aggregation,
    /// A projected signal field
    Field,
    /// A derived/computed column
    Computed,
    /// A navigation link column
    Navigation,
    /// A trace group identifier
    TraceGroup,
    /// Opaque row identifier for incremental updates
    RowId,
}

/// Metadata for one output column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name in the result
    pub name: String,
    /// Column data type
    pub data_type: DataType,
    /// Semantic role
    pub role: ColumnRole,
}

/// Walk the stages to find the effective signal after all Navigate stages.
fn effective_signal(ast: &QueryAst) -> Signal {
    let mut signal = ast.scan.signal;
    for stage in &ast.stages {
        if let Stage::Navigate(NavigateStage { target }) = stage {
            signal = *target;
        }
    }
    signal
}

/// Infer the [`ResponseShape`] from a [`QueryAst`]
pub fn infer_shape(ast: &QueryAst) -> ResponseShape {
    let final_sig = effective_signal(ast);
    let has_navigate = ast.stages.iter().any(|s| matches!(s, Stage::Navigate(_)));

    // traces -> spans navigation → waterfall trace timeline
    if has_navigate && final_sig == Signal::Spans {
        return ResponseShape::TraceTimeline;
    }

    // Bare traces scan or filtered traces → table (trace summary listing)
    if ast.scan.signal == Signal::Traces && !has_navigate {
        return ResponseShape::Table;
    }

    // Check stages for aggregate functions that imply specific shapes
    for stage in &ast.stages {
        if let Stage::Aggregate(agg) = stage {
            // Heatmap aggregation
            for aggregation in &agg.aggregations {
                if matches!(aggregation.function, AggregateFn::Heatmap(_)) {
                    return ResponseShape::Heatmap;
                }
            }
            // Time-bucketed group by → time series
            for group in &agg.group_by {
                if group.bin_ns.is_some() {
                    return ResponseShape::TimeSeries;
                }
            }
            // No group-by fields → single-row scalar
            if agg.group_by.is_empty() {
                return ResponseShape::Scalar;
            }
            // Grouped aggregation → table
            return ResponseShape::Table;
        }
        if let Stage::Patterns(_) = stage {
            return ResponseShape::PatternGroups;
        }
    }

    // No aggregate → flat table
    ResponseShape::Table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{
        AggregateFn, AggregateStage, Aggregation, AttrScope, Expr, FieldRef, GroupExpr, LimitStage,
        NavigateStage, PatternsStage, QueryMode, Scan, Stage, TimeRange,
    };

    fn base_ast(signal: Signal) -> QueryAst {
        QueryAst {
            bindings: vec![],
            scan: Scan {
                signal,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![],
            mode: QueryMode::Snapshot,
        }
    }

    #[test]
    fn infer_shape_spans_no_agg() {
        let ast = base_ast(Signal::Spans);
        assert_eq!(infer_shape(&ast), ResponseShape::Table);
    }

    #[test]
    fn infer_shape_traces_bare() {
        // Bare traces scan → table (listing traces)
        let ast = base_ast(Signal::Traces);
        assert_eq!(infer_shape(&ast), ResponseShape::Table);
    }

    #[test]
    fn infer_shape_traces_bare_is_table() {
        // Explicit test per plan: traces last 1h → Table
        let ast = base_ast(Signal::Traces);
        assert_eq!(infer_shape(&ast), ResponseShape::Table);
    }

    #[test]
    fn infer_shape_traces_filtered() {
        // Filtered traces (with limit) → Table (trace summary listing, not timeline)
        let mut ast = base_ast(Signal::Traces);
        ast.stages.push(Stage::Limit(LimitStage {
            limit: 100,
            offset: None,
        }));
        // traces + limit (no navigate) → still a trace table listing
        assert_eq!(infer_shape(&ast), ResponseShape::Table);
    }

    #[test]
    fn infer_shape_traces_navigate_spans_is_timeline() {
        // traces last 1h -> spans → TraceTimeline (waterfall)
        let mut ast = base_ast(Signal::Traces);
        ast.stages.push(Stage::Navigate(NavigateStage {
            target: Signal::Spans,
        }));
        assert_eq!(infer_shape(&ast), ResponseShape::TraceTimeline);
    }

    #[test]
    fn infer_shape_time_series() {
        let mut ast = base_ast(Signal::Spans);
        ast.stages.push(Stage::Aggregate(AggregateStage {
            group_by: vec![GroupExpr {
                expr: Expr::Field(FieldRef {
                    scope: AttrScope::Signal,
                    name: "start_time".into(),
                }),
                alias: Some("bucket".into()),
                bin_ns: Some(60_000_000_000), // 1-minute buckets
            }],
            aggregations: vec![Aggregation {
                function: AggregateFn::Count,
                alias: "count".into(),
                filter: None,
            }],
        }));
        assert_eq!(infer_shape(&ast), ResponseShape::TimeSeries);
    }

    #[test]
    fn infer_shape_heatmap() {
        let mut ast = base_ast(Signal::Spans);
        ast.stages.push(Stage::Aggregate(AggregateStage {
            group_by: vec![],
            aggregations: vec![Aggregation {
                function: AggregateFn::Heatmap(Expr::Field(FieldRef {
                    scope: AttrScope::Signal,
                    name: "duration".into(),
                })),
                alias: "distribution".into(),
                filter: None,
            }],
        }));
        assert_eq!(infer_shape(&ast), ResponseShape::Heatmap);
    }

    #[test]
    fn infer_shape_patterns() {
        let mut ast = base_ast(Signal::Logs);
        ast.stages
            .push(Stage::Patterns(PatternsStage { field: None }));
        assert_eq!(infer_shape(&ast), ResponseShape::PatternGroups);
    }

    #[test]
    fn column_def_round_trip() {
        let col = ColumnDef {
            name: "duration_ns".into(),
            data_type: DataType::Duration,
            role: ColumnRole::Field,
        };
        let json = serde_json::to_string(&col).unwrap();
        let back: ColumnDef = serde_json::from_str(&json).unwrap();
        assert_eq!(col, back);
    }

    #[test]
    fn response_shape_round_trip() {
        let shapes = vec![
            ResponseShape::Table,
            ResponseShape::TimeSeries,
            ResponseShape::Heatmap,
            ResponseShape::TraceTree,
            ResponseShape::TraceTimeline,
            ResponseShape::PatternGroups,
            ResponseShape::Scalar,
        ];
        for shape in &shapes {
            let json = serde_json::to_string(shape).unwrap();
            let back: ResponseShape = serde_json::from_str(&json).unwrap();
            assert_eq!(shape, &back);
        }
    }
}
