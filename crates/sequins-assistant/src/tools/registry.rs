//! The single tool registry — one list of `(name, description, JSON schema, invoker)`
//! for every assistant tool, shared by both adapters:
//!
//! - the **Rig** adapter turns [`tool_definitions`] into the model's tool set and
//!   dispatches its calls through [`invoke`];
//! - the **MCP** adapter lists the same [`specs`] and dispatches `tools/call`
//!   through the same [`invoke`].
//!
//! Keeping the schema and the dispatch in one place guarantees the two surfaces
//! never drift.

use rig::completion::ToolDefinition;
use schemars::{schema_for, JsonSchema};
use serde::de::DeserializeOwned;

use super::ops::{
    AddChartArgs, ArrangeDashboardArgs, AttributeValuesArgs, ColumnProfileArgs,
    CreateDashboardArgs, DescribeSchemaArgs, ExplainArgs, GetDashboardArgs, ListAttributesArgs,
    ListDashboardsArgs, ListMetricsArgs, ListTablesArgs, MetricLabelValuesArgs, MetricLabelsArgs,
    OverviewArgs, RenameDashboardArgs, RunSeqlArgs, RunSqlArgs, SampleArgs, TimeRangeArgs,
    UpdateChartArgs, ValidateSeqlArgs,
};
use super::{OpError, Tools};

/// A tool's model-facing metadata.
pub struct ToolSpec {
    /// Tool name (must match what the model calls).
    pub name: &'static str,
    /// Human-readable description sent to the model.
    pub description: &'static str,
    /// JSON Schema for the tool's arguments.
    pub parameters: serde_json::Value,
}

/// The names of every tool this assistant executes in-process. Used by the
/// middleware model to tell *our* tool calls from a caller's own tools.
pub const TOOL_NAMES: &[&str] = &[
    "overview",
    "list_tables",
    "describe_schema",
    "list_attributes",
    "attribute_values",
    "list_metrics",
    "metric_labels",
    "metric_label_values",
    "column_profile",
    "time_range",
    "sample",
    "explain",
    "run_sql",
    "validate_seql",
    "run_seql",
    "list_dashboards",
    "get_dashboard",
    "create_dashboard",
    "rename_dashboard",
    "add_chart",
    "update_chart",
    "arrange_dashboard",
];

/// Is `name` one of our in-process tools (vs. a caller-provided tool)?
pub fn is_ours(name: &str) -> bool {
    TOOL_NAMES.contains(&name)
}

fn schema<T: JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schema_for!(T)).unwrap_or_else(|_| serde_json::json!({ "type": "object" }))
}

/// Every tool's spec, in a stable order.
pub fn specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "overview",
            description: "Orient yourself: per-signal-table row counts and time spans, flagging empty tables. Call this first.",
            parameters: schema::<OverviewArgs>(),
        },
        ToolSpec {
            name: "list_tables",
            description: "List the queryable signal tables (spans, logs, datapoints, …) and their time columns.",
            parameters: schema::<ListTablesArgs>(),
        },
        ToolSpec {
            name: "describe_schema",
            description: "Show a signal table's columns and Arrow types. Call before writing a query.",
            parameters: schema::<DescribeSchemaArgs>(),
        },
        ToolSpec {
            name: "list_attributes",
            description: "Enumerate the attribute keys present on a table: promoted (populated) columns plus common overflow-map keys (or resource/scope keys). Observability data is dominated by attributes — use this to discover real keys before filtering on attr.<key>.",
            parameters: schema::<ListAttributesArgs>(),
        },
        ToolSpec {
            name: "attribute_values",
            description: "The most-frequent values of a given attribute key (promoted column or overflow key) — e.g. which http.route or service values exist.",
            parameters: schema::<AttributeValuesArgs>(),
        },
        ToolSpec {
            name: "list_metrics",
            description: "List available metrics with their type, unit, and series cardinality. Use before charting a metric.",
            parameters: schema::<ListMetricsArgs>(),
        },
        ToolSpec {
            name: "metric_labels",
            description: "The per-series label keys a metric is broken down by (metric attributes aren't in the SQL tables). Optionally scope to one metric.",
            parameters: schema::<MetricLabelsArgs>(),
        },
        ToolSpec {
            name: "metric_label_values",
            description: "The distinct values a metric label key takes (optionally within one metric).",
            parameters: schema::<MetricLabelValuesArgs>(),
        },
        ToolSpec {
            name: "column_profile",
            description: "Profile a column: total/non-null counts, approximate distinct cardinality, min/max, and the most frequent values.",
            parameters: schema::<ColumnProfileArgs>(),
        },
        ToolSpec {
            name: "time_range",
            description: "Report the earliest and latest timestamps (unix nanoseconds) of a signal table.",
            parameters: schema::<TimeRangeArgs>(),
        },
        ToolSpec {
            name: "sample",
            description: "Return a handful of sample rows from a signal table to see real values.",
            parameters: schema::<SampleArgs>(),
        },
        ToolSpec {
            name: "explain",
            description: "Show the optimized logical plan of a read-only SQL SELECT, without running it.",
            parameters: schema::<ExplainArgs>(),
        },
        ToolSpec {
            name: "run_sql",
            description: "Run a read-only SQL SELECT over the signal tables (DDL/DML rejected). For ad-hoc exploration only.",
            parameters: schema::<RunSqlArgs>(),
        },
        ToolSpec {
            name: "validate_seql",
            description: "Parse-check a SeQL query. Returns {ok:true} or {ok:false, error:{message,offset,length}}.",
            parameters: schema::<ValidateSeqlArgs>(),
        },
        ToolSpec {
            name: "run_seql",
            description: "Execute a SeQL query and summarize its result: shape, columns, row count, sample rows, and stats. The final answer must be a SeQL query.",
            parameters: schema::<RunSeqlArgs>(),
        },
        ToolSpec {
            name: "list_dashboards",
            description: "List saved dashboards with ids, titles, and chart counts.",
            parameters: schema::<ListDashboardsArgs>(),
        },
        ToolSpec {
            name: "get_dashboard",
            description: "Show a dashboard's full layout — rows/heights and each chart's [row,col] position, title, weight, type, and query. Read this before editing to get chart indices.",
            parameters: schema::<GetDashboardArgs>(),
        },
        ToolSpec {
            name: "create_dashboard",
            description: "Create a new empty dashboard with a title.",
            parameters: schema::<CreateDashboardArgs>(),
        },
        ToolSpec {
            name: "rename_dashboard",
            description: "Rename an existing dashboard.",
            parameters: schema::<RenameDashboardArgs>(),
        },
        ToolSpec {
            name: "add_chart",
            description: "Add a chart (a SeQL query + title + optional type) to a dashboard, either into an existing row at a position/weight or as a new full-width row.",
            parameters: schema::<AddChartArgs>(),
        },
        ToolSpec {
            name: "update_chart",
            description: "Edit an existing chart's title, query, and/or type by its [row,col] position.",
            parameters: schema::<UpdateChartArgs>(),
        },
        ToolSpec {
            name: "arrange_dashboard",
            description: "Rearrange a dashboard: place existing charts into new rows/columns with new widths (weights) and row heights. Charts are referenced by current position; unlisted charts are dropped. Use to move, resize, and reorder in one call.",
            parameters: schema::<ArrangeDashboardArgs>(),
        },
    ]
}

/// Rig tool definitions for the model's request.
pub fn tool_definitions() -> Vec<ToolDefinition> {
    specs()
        .into_iter()
        .map(|s| ToolDefinition {
            name: s.name.to_string(),
            description: s.description.to_string(),
            parameters: s.parameters,
        })
        .collect()
}

/// Dispatch a tool call by name, parsing `args` into the tool's typed arguments.
/// Returns the rendered result string, or an error message the model can read.
pub async fn invoke(tools: &Tools, name: &str, args: serde_json::Value) -> Result<String, String> {
    // Tools with no arguments may arrive as `null`; treat that as `{}`.
    let args = if args.is_null() {
        serde_json::json!({})
    } else {
        args
    };

    fn parse<T: DeserializeOwned>(v: serde_json::Value) -> Result<T, String> {
        serde_json::from_value(v).map_err(|e| format!("invalid arguments: {e}"))
    }

    let result: Result<String, OpError> = match name {
        "overview" => tools.overview(parse(args)?).await,
        "list_tables" => tools.list_tables(parse(args)?).await,
        "describe_schema" => tools.describe_schema(parse(args)?).await,
        "list_attributes" => tools.list_attributes(parse(args)?).await,
        "attribute_values" => tools.attribute_values(parse(args)?).await,
        "list_metrics" => tools.list_metrics(parse(args)?).await,
        "metric_labels" => tools.metric_labels(parse(args)?).await,
        "metric_label_values" => tools.metric_label_values(parse(args)?).await,
        "column_profile" => tools.column_profile(parse(args)?).await,
        "time_range" => tools.time_range(parse(args)?).await,
        "sample" => tools.sample(parse(args)?).await,
        "explain" => tools.explain(parse(args)?).await,
        "run_sql" => tools.run_sql(parse(args)?).await,
        "validate_seql" => tools.validate_seql(parse(args)?).await,
        "run_seql" => tools.run_seql(parse(args)?).await,
        "list_dashboards" => tools.list_dashboards(parse(args)?).await,
        "get_dashboard" => tools.get_dashboard(parse(args)?).await,
        "create_dashboard" => tools.create_dashboard(parse(args)?).await,
        "rename_dashboard" => tools.rename_dashboard(parse(args)?).await,
        "add_chart" => tools.add_chart(parse(args)?).await,
        "update_chart" => tools.update_chart(parse(args)?).await,
        "arrange_dashboard" => tools.arrange_dashboard(parse(args)?).await,
        other => return Err(format!("unknown tool: {other}")),
    };
    result.map_err(|e| e.to_string())
}
