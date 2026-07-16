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
    ColumnProfileArgs, DescribeSchemaArgs, ExplainArgs, ListTablesArgs, RunSeqlArgs, RunSqlArgs,
    SampleArgs, TimeRangeArgs, ValidateSeqlArgs,
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
    "list_tables",
    "describe_schema",
    "column_profile",
    "time_range",
    "sample",
    "explain",
    "run_sql",
    "validate_seql",
    "run_seql",
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
        "list_tables" => tools.list_tables(parse(args)?).await,
        "describe_schema" => tools.describe_schema(parse(args)?).await,
        "column_profile" => tools.column_profile(parse(args)?).await,
        "time_range" => tools.time_range(parse(args)?).await,
        "sample" => tools.sample(parse(args)?).await,
        "explain" => tools.explain(parse(args)?).await,
        "run_sql" => tools.run_sql(parse(args)?).await,
        "validate_seql" => tools.validate_seql(parse(args)?).await,
        "run_seql" => tools.run_seql(parse(args)?).await,
        other => return Err(format!("unknown tool: {other}")),
    };
    result.map_err(|e| e.to_string())
}
