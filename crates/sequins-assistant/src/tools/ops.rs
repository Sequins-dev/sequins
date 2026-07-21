//! The assistant's tool **operation layer** — one implementation of every tool,
//! shared by both the Rig agent adapter and the MCP server adapter.
//!
//! Each operation takes a typed args struct (deriving [`serde`] + [`schemars::JsonSchema`]
//! so the same definition drives both the Rig `ToolDefinition` and the MCP tool schema)
//! and returns an LLM-legible `String`. Two lanes, per the design:
//!
//! - **explore** (`list_tables`, `describe_schema`, `column_profile`, `time_range`,
//!   `sample`, `explain`, `run_sql`) — direct DataFusion over the backend's
//!   [`SessionContext`], so the agent can understand the data before writing a query.
//!   These are domain-agnostic (later extractable as `datafusion-assistant`).
//! - **present** (`validate_seql`, `run_seql`) — the SeQL query path, so the agent's
//!   final answer is a SeQL string the app re-executes through its normal render path.
//!
//! `run_sql`/`explain` are **read-only**: DDL, DML and multi-statement scripts are
//! rejected via [`SQLOptions`], so ad-hoc SQL can scan the signal tables but never
//! mutate storage or pollute the shared query context.

use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SQLOptions;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use schemars::JsonSchema;
use sequins_arrow_schema::{PromotedAttribute, SignalType};
use sequins_datafusion_backend::{hot_signal_tables, hot_signal_type_for_table, DataFusionBackend};
use sequins_flight::{decode_metadata, ipc_to_batch, SeqlMetadata};
use sequins_metadata::{
    Dashboard, DashboardApi, DashboardRow, RowPanel, SavedVisualization, DEFAULT_ROW_HEIGHT,
};
use sequins_traits::QueryApi;
use serde::{Deserialize, Serialize};

/// Default number of rows returned by `sample` and rendered by `run_sql`/`run_seql`.
const DEFAULT_ROWS: usize = 10;
/// Hard cap on rows any single tool will render, to keep tool output token-bounded.
const MAX_ROWS: usize = 100;
/// Default number of most-frequent values returned by `column_profile`.
const DEFAULT_TOP_K: usize = 10;

/// Errors surfaced by the tool operation layer. Rendered into the tool's string
/// result by the adapters (the model sees the message and can correct itself).
#[derive(Debug, thiserror::Error)]
pub enum OpError {
    #[error("unknown table '{table}' — known tables: {known}")]
    UnknownTable { table: String, known: String },
    #[error("unknown column '{column}' in table '{table}'")]
    UnknownColumn { column: String, table: String },
    #[error("query error: {0}")]
    Query(#[from] sequins_traits::QueryError),
    #[error("sql error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("dashboard editing is not available for this connection")]
    DashboardsUnavailable,
    #[error("no dashboard matching '{key}' — use list_dashboards to see ids/titles")]
    UnknownDashboard { key: String },
    #[error("no chart at [row {row}, column {column}] — use get_dashboard to see indices")]
    UnknownPanel { row: usize, column: usize },
    #[error("dashboard store error: {0}")]
    Metadata(#[from] sequins_metadata::MetadataError),
}

// ---------------------------------------------------------------------------
// Args — one struct per tool. `deny_unknown_fields` keeps model mistakes loud.
// ---------------------------------------------------------------------------

/// Arguments for `list_tables` (none).
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListTablesArgs {}

/// Arguments for `describe_schema`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DescribeSchemaArgs {
    /// Signal table to describe, e.g. `spans`, `logs`, `datapoints`.
    pub table: String,
}

/// Arguments for `column_profile`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ColumnProfileArgs {
    /// Signal table containing the column.
    pub table: String,
    /// Column to profile.
    pub column: String,
    /// How many most-frequent values to list (default 10).
    #[serde(default)]
    pub top_k: Option<usize>,
}

/// Arguments for `time_range`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TimeRangeArgs {
    /// Signal table whose time span to report.
    pub table: String,
}

/// Arguments for `sample`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SampleArgs {
    /// Signal table to sample rows from.
    pub table: String,
    /// Number of rows to return (default 10, max 100).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for `explain`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ExplainArgs {
    /// A read-only SQL `SELECT` whose optimized logical plan to show.
    pub sql: String,
}

/// Arguments for `run_sql`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunSqlArgs {
    /// A read-only SQL `SELECT` over the signal tables. DDL/DML is rejected.
    pub sql: String,
    /// Max rows to return (default 10, max 100).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for `validate_seql`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ValidateSeqlArgs {
    /// The SeQL query string to parse-check.
    pub query: String,
}

/// Arguments for `run_seql`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunSeqlArgs {
    /// The SeQL query string to execute.
    pub query: String,
    /// How many sample result rows to include (default 10, max 100).
    #[serde(default)]
    pub sample_rows: Option<usize>,
}

/// Arguments for `overview` (none).
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OverviewArgs {}

/// Arguments for `list_metrics` (none).
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListMetricsArgs {}

/// Which kind of attribute keys to enumerate.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AttributeKind {
    /// Signal-level attributes (`attr.<key>` — promoted columns + overflow map).
    #[default]
    Attr,
    /// Resource attributes (from the `resources` table).
    Resource,
    /// Instrumentation-scope attributes (from the `scopes` table).
    Scope,
}

/// Arguments for `list_attributes`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListAttributesArgs {
    /// Signal table whose attributes to enumerate (e.g. `spans`, `logs`). Ignored for
    /// `kind=resource`/`scope`, which read the `resources`/`scopes` tables.
    pub table: String,
    /// Which attribute namespace to list (default `attr`).
    #[serde(default)]
    pub kind: AttributeKind,
    /// How many overflow/resource/scope keys to list (default 40, max 100).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for `attribute_values`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AttributeValuesArgs {
    /// Signal table containing the attribute.
    pub table: String,
    /// Attribute key — an OTLP key like `http.route` or its promoted column name.
    pub key: String,
    /// How many most-frequent values to list (default 10, max 100).
    #[serde(default)]
    pub top_k: Option<usize>,
}

/// Arguments for `list_dashboards` (none).
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListDashboardsArgs {}

/// Arguments for `get_dashboard`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GetDashboardArgs {
    /// Dashboard id or (case-insensitive) title.
    pub dashboard: String,
}

/// Arguments for `create_dashboard`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CreateDashboardArgs {
    /// Title for the new dashboard.
    pub title: String,
}

/// Arguments for `rename_dashboard`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RenameDashboardArgs {
    /// Dashboard id or title to rename.
    pub dashboard: String,
    /// New title.
    pub title: String,
}

/// Arguments for `add_chart`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AddChartArgs {
    /// Target dashboard id or title (must already exist — use create_dashboard first).
    pub dashboard: String,
    /// SeQL query whose result the chart renders.
    pub query: String,
    /// Short chart title.
    pub title: String,
    /// Optional chart type (`line`/`bar`/`stat`/`table`/`heatmap`/…). Omit to auto-select.
    #[serde(default)]
    pub chart_type: Option<String>,
    /// Existing row index to add into; omit to append the chart as a new full-width row.
    #[serde(default)]
    pub row: Option<usize>,
    /// Column position within `row` (default: end of the row).
    #[serde(default)]
    pub position: Option<usize>,
    /// Relative width weight within the row (default 1.0).
    #[serde(default)]
    pub weight: Option<f64>,
}

/// Arguments for `update_chart`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UpdateChartArgs {
    /// Dashboard id or title.
    pub dashboard: String,
    /// Row index of the chart (from get_dashboard).
    pub row: usize,
    /// Column index of the chart within the row.
    pub column: usize,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub chart_type: Option<String>,
}

/// Arguments for `arrange_dashboard` — a declarative new layout referencing existing
/// charts by their current position, setting per-panel widths and per-row heights.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ArrangeDashboardArgs {
    /// Dashboard id or title.
    pub dashboard: String,
    /// The desired rows, top to bottom.
    pub rows: Vec<ArrangeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ArrangeRow {
    /// Row height in points (default 280).
    #[serde(default)]
    pub height: Option<f64>,
    /// The charts in this row, left to right, referenced by current position.
    pub panels: Vec<ArrangePanel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ArrangePanel {
    /// Current row index of the chart to place here.
    pub from_row: usize,
    /// Current column index of the chart to place here.
    pub from_column: usize,
    /// New relative width weight (default: keep the chart's current weight).
    #[serde(default)]
    pub weight: Option<f64>,
}

// ---------------------------------------------------------------------------
// Tools — holds the backend; every op is a method returning rendered text.
// ---------------------------------------------------------------------------

/// The shared tool operation layer over a [`DataFusionBackend`].
#[derive(Clone)]
pub struct Tools {
    backend: Arc<DataFusionBackend>,
    /// Dashboard read/write, when the host wires it in (the FFI local assistant and the
    /// Pro daemon both have a `DashboardApi`). Absent ⇒ dashboard tools report they're
    /// unavailable rather than failing opaquely.
    dashboards: Option<Arc<dyn DashboardApi>>,
}

impl Tools {
    /// Build the tool layer over an existing backend.
    pub fn new(backend: Arc<DataFusionBackend>) -> Self {
        Self {
            backend,
            dashboards: None,
        }
    }

    /// Attach a dashboard store so the assistant can read and edit dashboards.
    pub fn with_dashboards(mut self, dashboards: Arc<dyn DashboardApi>) -> Self {
        self.dashboards = Some(dashboards);
        self
    }

    fn dashboard_api(&self) -> Result<&Arc<dyn DashboardApi>, OpError> {
        self.dashboards
            .as_ref()
            .ok_or(OpError::DashboardsUnavailable)
    }

    async fn ctx(&self) -> Result<SessionContext, OpError> {
        Ok(self.backend.session().await?)
    }

    /// List the signal tables the assistant can query, with their time column.
    pub async fn list_tables(&self, _args: ListTablesArgs) -> Result<String, OpError> {
        let mut out = String::from("Signal tables (query with SeQL or read-only SQL):\n");
        for (name, signal) in hot_signal_tables() {
            let time_col = signal.time_column().unwrap_or("(none)");
            out.push_str(&format!(
                "- {name}: signal={}, time_column={time_col}\n",
                signal.name()
            ));
        }
        out.push_str("\nUse describe_schema(table) for columns.");
        Ok(out)
    }

    /// Describe a table's columns and Arrow types.
    pub async fn describe_schema(&self, args: DescribeSchemaArgs) -> Result<String, OpError> {
        validate_table(&args.table)?;
        let ctx = self.ctx().await?;
        let df = ctx.table(&args.table).await?;
        let schema = df.schema().as_arrow();
        let mut out = format!(
            "Schema of `{}` ({} columns):\n",
            args.table,
            schema.fields().len()
        );
        for f in schema.fields() {
            out.push_str(&format!(
                "- {}: {}{}\n",
                f.name(),
                f.data_type(),
                if f.is_nullable() { " (nullable)" } else { "" }
            ));
        }
        Ok(out)
    }

    /// Profile a column: total/non-null/approx-distinct, min/max, and top-k values.
    pub async fn column_profile(&self, args: ColumnProfileArgs) -> Result<String, OpError> {
        validate_table(&args.table)?;
        let ctx = self.ctx().await?;
        self.validate_column(&ctx, &args.table, &args.column)
            .await?;

        let t = quote_ident(&args.table);
        let c = quote_ident(&args.column);
        let top_k = args.top_k.unwrap_or(DEFAULT_TOP_K).clamp(1, MAX_ROWS);

        let mut out = format!("Profile of `{}`.`{}`:\n", args.table, args.column);

        // Row counts always work for any column type — kept separate from
        // approx_distinct so an unsupported type (e.g. Float64, which DataFusion's
        // HyperLogLog can't hash) doesn't hide the counts too.
        let counts_sql = format!("SELECT count(*) AS total_rows, count({c}) AS non_null FROM {t}");
        match self.query_readonly(&ctx, &counts_sql, MAX_ROWS).await {
            Ok(batches) => out.push_str(&render_batches(&batches)?),
            Err(e) => out.push_str(&format!("(counts unavailable: {e})\n")),
        }

        // Approximate distinct cardinality — best effort; unsupported for some types.
        let distinct_sql = format!("SELECT approx_distinct({c}) AS approx_distinct FROM {t}");
        match self.query_readonly(&ctx, &distinct_sql, MAX_ROWS).await {
            Ok(batches) => {
                out.push_str("\nApprox distinct:\n");
                out.push_str(&render_batches(&batches)?);
            }
            Err(_) => out.push_str("\nApprox distinct: unavailable for this column type.\n"),
        }

        let minmax_sql = format!("SELECT min({c}) AS min, max({c}) AS max FROM {t}");
        if let Ok(batches) = self.query_readonly(&ctx, &minmax_sql, MAX_ROWS).await {
            out.push_str("\nRange:\n");
            out.push_str(&render_batches(&batches)?);
        }

        let topk_sql = format!(
            "SELECT {c} AS value, count(*) AS n FROM {t} GROUP BY {c} ORDER BY n DESC LIMIT {top_k}"
        );
        match self.query_readonly(&ctx, &topk_sql, top_k).await {
            Ok(batches) => {
                out.push_str(&format!("\nTop {top_k} values:\n"));
                out.push_str(&render_batches(&batches)?);
            }
            Err(e) => out.push_str(&format!("\n(top values unavailable: {e})\n")),
        }
        Ok(out)
    }

    /// Report the min/max of a table's time column (nanoseconds since epoch).
    pub async fn time_range(&self, args: TimeRangeArgs) -> Result<String, OpError> {
        let signal = validate_table(&args.table)?;
        let Some(time_col) = signal.time_column() else {
            return Ok(format!("Table `{}` has no time column.", args.table));
        };
        let ctx = self.ctx().await?;
        let sql = format!(
            "SELECT min({c}) AS earliest_ns, max({c}) AS latest_ns, count(*) AS rows FROM {t}",
            c = quote_ident(time_col),
            t = quote_ident(&args.table),
        );
        let batches = self.query_readonly(&ctx, &sql, MAX_ROWS).await?;
        Ok(format!(
            "Time range of `{}` (column `{time_col}`, unix nanoseconds):\n{}",
            args.table,
            render_batches(&batches)?
        ))
    }

    /// Return up to `limit` sample rows from a table.
    pub async fn sample(&self, args: SampleArgs) -> Result<String, OpError> {
        validate_table(&args.table)?;
        let limit = args.limit.unwrap_or(DEFAULT_ROWS).clamp(1, MAX_ROWS);
        let ctx = self.ctx().await?;
        let sql = format!("SELECT * FROM {} LIMIT {limit}", quote_ident(&args.table));
        let batches = self.query_readonly(&ctx, &sql, limit).await?;
        Ok(format!(
            "Up to {limit} rows from `{}`:\n{}",
            args.table,
            render_batches(&batches)?
        ))
    }

    /// Show the optimized logical plan of a read-only SQL query (no execution).
    pub async fn explain(&self, args: ExplainArgs) -> Result<String, OpError> {
        let ctx = self.ctx().await?;
        let df = ctx.sql_with_options(&args.sql, read_only()).await?;
        let plan = df.into_optimized_plan()?;
        Ok(format!(
            "Optimized logical plan:\n{}",
            plan.display_indent()
        ))
    }

    /// Execute a read-only SQL query and render up to `limit` rows.
    pub async fn run_sql(&self, args: RunSqlArgs) -> Result<String, OpError> {
        let limit = args.limit.unwrap_or(DEFAULT_ROWS).clamp(1, MAX_ROWS);
        let ctx = self.ctx().await?;
        let batches = self.query_readonly(&ctx, &args.sql, limit).await?;
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        Ok(format!(
            "{rows} row(s) (capped at {limit}):\n{}",
            render_batches(&batches)?
        ))
    }

    /// Parse-check a SeQL query, returning `{ok}` or `{ok:false, error:{...}}`.
    pub async fn validate_seql(&self, args: ValidateSeqlArgs) -> Result<String, OpError> {
        let value = match seql_parser::parse(&args.query) {
            Ok(_) => serde_json::json!({ "ok": true }),
            Err(e) => serde_json::json!({
                "ok": false,
                "error": { "message": e.message, "offset": e.offset, "length": e.length }
            }),
        };
        Ok(value.to_string())
    }

    /// Execute a SeQL query and summarize its result shape, columns, row count,
    /// sample rows, and stats — the feedback loop for writing a good final query.
    pub async fn run_seql(&self, args: RunSeqlArgs) -> Result<String, OpError> {
        let sample_rows = args.sample_rows.unwrap_or(DEFAULT_ROWS).clamp(1, MAX_ROWS);
        let mut stream = self.backend.query(&args.query).await?;

        let mut shape: Option<String> = None;
        let mut columns: Vec<String> = Vec::new();
        let mut sample: Vec<RecordBatch> = Vec::new();
        let mut sampled_rows = 0usize;
        let mut total_rows = 0usize;
        let mut stats_line = String::new();

        while let Some(frame) = stream.next().await {
            let fd = frame?;
            let Some(meta) = decode_metadata(&fd.app_metadata) else {
                continue;
            };
            match meta {
                // Only the primary result table (`table: None`) feeds the summary;
                // auxiliary merge/navigate tables are skipped.
                SeqlMetadata::Schema {
                    table: None,
                    shape: s,
                    columns: cols,
                    ..
                } => {
                    shape = Some(s.as_str().to_string());
                    columns = cols
                        .iter()
                        .map(|c| format!("{} ({:?}, {:?})", c.name, c.data_type, c.role))
                        .collect();
                }
                SeqlMetadata::Data { table: None } => {
                    if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                        total_rows += batch.num_rows();
                        if sampled_rows < sample_rows {
                            let take = (sample_rows - sampled_rows).min(batch.num_rows());
                            sample.push(batch.slice(0, take));
                            sampled_rows += take;
                        }
                    }
                }
                SeqlMetadata::Complete { stats } => {
                    stats_line = format!(
                        "rows_returned={}, rows_scanned={}, exec_us={}",
                        stats.rows_returned, stats.rows_scanned, stats.execution_time_us
                    );
                }
                _ => {}
            }
        }

        let mut out = format!(
            "SeQL executed.\nshape: {}\ncolumns:\n",
            shape.as_deref().unwrap_or("(unknown)")
        );
        for c in &columns {
            out.push_str(&format!("  - {c}\n"));
        }
        out.push_str(&format!("rows returned: {total_rows}\n"));
        if !stats_line.is_empty() {
            out.push_str(&format!("stats: {stats_line}\n"));
        }
        if !sample.is_empty() {
            out.push_str(&format!("sample (up to {sample_rows} rows):\n"));
            out.push_str(&render_batches(&sample)?);
        }
        Ok(out)
    }

    /// A compact per-signal-table overview: row counts and (where a time column
    /// exists) the time span, flagging empty tables so the model avoids them.
    pub async fn overview(&self, _args: OverviewArgs) -> Result<String, OpError> {
        let ctx = self.ctx().await?;
        let mut out = String::from("Signal data overview:\n");
        for (name, signal) in hot_signal_tables() {
            let t = quote_ident(name);
            let (n, span) = match signal.time_column() {
                Some(tc) => {
                    let sql = format!(
                        "SELECT count(*) AS n, min(CAST({c} AS BIGINT)) AS lo, max(CAST({c} AS BIGINT)) AS hi FROM {t}",
                        c = quote_ident(tc),
                    );
                    match self.query_readonly(&ctx, &sql, 1).await {
                        Ok(b) => {
                            let n = scalar_i64(&b, "n").unwrap_or(0);
                            let span = match (scalar_i64(&b, "lo"), scalar_i64(&b, "hi")) {
                                (Some(lo), Some(hi)) if hi >= lo => Some(format!(
                                    ", spanning {}",
                                    human_duration_ns((hi - lo) as u64)
                                )),
                                _ => None,
                            };
                            (n, span)
                        }
                        Err(_) => (0, None),
                    }
                }
                None => {
                    let sql = format!("SELECT count(*) AS n FROM {t}");
                    let n = self
                        .query_readonly(&ctx, &sql, 1)
                        .await
                        .ok()
                        .and_then(|b| scalar_i64(&b, "n"))
                        .unwrap_or(0);
                    (n, None)
                }
            };
            if n == 0 {
                out.push_str(&format!("- {name}: empty\n"));
            } else {
                out.push_str(&format!("- {name}: {n} rows{}\n", span.unwrap_or_default()));
            }
        }
        out.push_str("\nUse describe_schema/list_attributes/list_metrics to go deeper.");
        Ok(out)
    }

    /// List the available metrics with their type, unit, and series cardinality.
    pub async fn list_metrics(&self, _args: ListMetricsArgs) -> Result<String, OpError> {
        let ctx = self.ctx().await?;
        let sql = "SELECT m.name AS metric, m.metric_type AS type, m.unit AS unit, \
                   count(DISTINCT d.series_id) AS series, count(d.series_id) AS datapoints \
                   FROM metrics m LEFT JOIN datapoints d ON m.metric_id = d.metric_id \
                   GROUP BY m.name, m.metric_type, m.unit ORDER BY m.name";
        let batches = self.query_readonly(&ctx, sql, MAX_ROWS).await?;
        Ok(format!(
            "Metrics (name, type, unit, series cardinality, datapoints):\n{}\n\
             Chart a metric by name via a `datapoints` SeQL query; use metric_labels for its label keys.",
            render_batches(&batches)?
        ))
    }

    /// Enumerate the attribute keys present in a table: promoted (populated) columns
    /// plus the most common overflow-map keys; or resource/scope attribute keys.
    pub async fn list_attributes(&self, args: ListAttributesArgs) -> Result<String, OpError> {
        let ctx = self.ctx().await?;
        let limit = args.limit.unwrap_or(40).clamp(1, MAX_ROWS);
        match args.kind {
            AttributeKind::Resource | AttributeKind::Scope => {
                let (table, label) = match args.kind {
                    AttributeKind::Resource => ("resources", "resource"),
                    _ => ("scopes", "scope"),
                };
                let keys = self.json_attribute_keys(&ctx, table, limit).await?;
                if keys.is_empty() {
                    return Ok(format!(
                        "No {label} attributes found (table `{table}` empty)."
                    ));
                }
                Ok(format!(
                    "{label} attribute keys (from `{table}.attributes`):\n{}",
                    keys.iter()
                        .map(|k| format!("- {k}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                ))
            }
            AttributeKind::Attr => {
                validate_table(&args.table)?;
                let cols = self.table_columns(&ctx, &args.table).await?;
                let mut out = format!("Attributes on `{}`:\n", args.table);

                // Promoted attributes that actually have data (one scan, all counts).
                let present: Vec<&PromotedAttribute> = sequins_arrow_schema::SEMCONV_ATTRIBUTES
                    .iter()
                    .filter(|a| cols.iter().any(|c| c == a.column_name))
                    .collect();
                if !present.is_empty() {
                    let selects: Vec<String> = present
                        .iter()
                        .map(|a| {
                            format!(
                                "count({}) AS {}",
                                quote_ident(a.column_name),
                                quote_ident(a.column_name)
                            )
                        })
                        .collect();
                    let sql = format!(
                        "SELECT {} FROM {}",
                        selects.join(", "),
                        quote_ident(&args.table)
                    );
                    out.push_str("promoted (typed columns, with non-null counts):\n");
                    match self.query_readonly(&ctx, &sql, 1).await {
                        Ok(b) => {
                            let mut any = false;
                            for a in &present {
                                if let Some(n) = scalar_i64(&b, a.column_name) {
                                    if n > 0 {
                                        any = true;
                                        out.push_str(&format!(
                                            "  - {} (col `{}`, {:?}): {n}\n",
                                            a.key, a.column_name, a.value_type
                                        ));
                                    }
                                }
                            }
                            if !any {
                                out.push_str("  (none populated)\n");
                            }
                        }
                        Err(e) => out.push_str(&format!("  (counts unavailable: {e})\n")),
                    }
                }

                // Overflow (non-promoted) keys from the CBOR map column.
                out.push_str("\ncustom (overflow) keys by frequency:\n");
                let overflow_sql = format!(
                    "SELECT k AS key, count(*) AS n FROM (SELECT unnest(map_keys(\"_overflow_attrs\")) AS k \
                     FROM {t} WHERE \"_overflow_attrs\" IS NOT NULL) GROUP BY k ORDER BY n DESC LIMIT {limit}",
                    t = quote_ident(&args.table),
                );
                match self.query_readonly(&ctx, &overflow_sql, limit).await {
                    Ok(b) if b.iter().any(|x| x.num_rows() > 0) => {
                        out.push_str(&render_batches(&b)?)
                    }
                    Ok(_) => out.push_str("  (none)\n"),
                    Err(_) => out.push_str("  (overflow-key enumeration unavailable)\n"),
                }
                out.push_str("\nRead a promoted key as a column; a custom key via attr.<key> in SeQL. Use attribute_values to see values.");
                Ok(out)
            }
        }
    }

    /// The most frequent values for a given attribute key (promoted column or overflow).
    pub async fn attribute_values(&self, args: AttributeValuesArgs) -> Result<String, OpError> {
        validate_table(&args.table)?;
        let ctx = self.ctx().await?;
        let top_k = args.top_k.unwrap_or(DEFAULT_TOP_K).clamp(1, MAX_ROWS);
        let cols = self.table_columns(&ctx, &args.table).await?;
        let t = quote_ident(&args.table);

        let v = match resolve_promoted_column(&cols, &args.key) {
            Some(col) => quote_ident(&col),
            None => format!(
                "overflow_get_str(\"_overflow_attrs\", '{}')",
                args.key.replace('\'', "''")
            ),
        };
        let sql = format!(
            "SELECT {v} AS value, count(*) AS n FROM {t} WHERE {v} IS NOT NULL \
             GROUP BY {v} ORDER BY n DESC LIMIT {top_k}"
        );
        let batches = self.query_readonly(&ctx, &sql, top_k).await?;
        Ok(format!(
            "Top {top_k} values of `{}`.`{}`:\n{}",
            args.table,
            args.key,
            render_batches(&batches)?
        ))
    }

    /// The column names of a table.
    async fn table_columns(
        &self,
        ctx: &SessionContext,
        table: &str,
    ) -> Result<Vec<String>, OpError> {
        let df = ctx.table(table).await?;
        Ok(df
            .schema()
            .as_arrow()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect())
    }

    /// Distinct attribute keys parsed from a JSON `attributes` string column (resources/scopes).
    async fn json_attribute_keys(
        &self,
        ctx: &SessionContext,
        table: &str,
        limit: usize,
    ) -> Result<Vec<String>, OpError> {
        let sql = format!(
            "SELECT DISTINCT attributes FROM {} WHERE attributes IS NOT NULL LIMIT 500",
            quote_ident(table)
        );
        let batches = self.query_readonly(ctx, &sql, 500).await?;
        let mut keys = std::collections::BTreeSet::new();
        for batch in &batches {
            if let Some(arr) = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
            {
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    if let Ok(serde_json::Value::Object(map)) =
                        serde_json::from_str::<serde_json::Value>(arr.value(i))
                    {
                        for k in map.keys() {
                            keys.insert(k.clone());
                        }
                    }
                }
            }
        }
        Ok(keys.into_iter().take(limit).collect())
    }

    // ---- Dashboard authoring ----------------------------------------------

    /// Resolve a dashboard by id, then by case-insensitive title.
    async fn resolve_dashboard(&self, key: &str) -> Result<Dashboard, OpError> {
        let api = self.dashboard_api()?;
        if let Some(d) = api.get_dashboard(key).await? {
            return Ok(d);
        }
        api.list_dashboards()
            .await?
            .into_iter()
            .find(|d| d.title.eq_ignore_ascii_case(key))
            .ok_or_else(|| OpError::UnknownDashboard {
                key: key.to_string(),
            })
    }

    /// List saved dashboards with their ids, titles, and panel counts.
    pub async fn list_dashboards(&self, _args: ListDashboardsArgs) -> Result<String, OpError> {
        let list = self.dashboard_api()?.list_dashboards().await?;
        if list.is_empty() {
            return Ok("No dashboards yet. Use create_dashboard to make one.".to_string());
        }
        let mut out = String::from("Dashboards:\n");
        for d in &list {
            out.push_str(&format!(
                "- \"{}\" (id={}, {} rows, {} charts)\n",
                d.title,
                d.id,
                d.rows.len(),
                d.panel_count()
            ));
        }
        Ok(out)
    }

    /// Show a dashboard's full structure — rows, heights, and each chart's position,
    /// title, weight, type, and query. Read this before editing to get chart indices.
    pub async fn get_dashboard(&self, args: GetDashboardArgs) -> Result<String, OpError> {
        let d = self.resolve_dashboard(&args.dashboard).await?;
        Ok(render_dashboard(&d))
    }

    /// Create a new empty dashboard.
    pub async fn create_dashboard(&self, args: CreateDashboardArgs) -> Result<String, OpError> {
        let api = self.dashboard_api()?;
        let d = api
            .save_dashboard(Dashboard {
                id: String::new(),
                title: args.title,
                created_at_ns: 0,
                updated_at_ns: 0,
                rows: Vec::new(),
            })
            .await?;
        Ok(format!("Created dashboard \"{}\" (id={}).", d.title, d.id))
    }

    /// Rename a dashboard.
    pub async fn rename_dashboard(&self, args: RenameDashboardArgs) -> Result<String, OpError> {
        let api = self.dashboard_api()?;
        let mut d = self.resolve_dashboard(&args.dashboard).await?;
        d.title = args.title;
        let d = api.save_dashboard(d).await?;
        Ok(format!(
            "Renamed dashboard to \"{}\" (id={}).",
            d.title, d.id
        ))
    }

    /// Add a chart to a dashboard — either into an existing row (at a position/weight) or
    /// as a new full-width row.
    pub async fn add_chart(&self, args: AddChartArgs) -> Result<String, OpError> {
        let api = self.dashboard_api()?;
        let mut d = self.resolve_dashboard(&args.dashboard).await?;
        let title = args.title.clone();
        let panel = RowPanel {
            visualization: SavedVisualization {
                seql: args.query,
                title: args.title,
                shape: args.chart_type,
            },
            weight: args.weight.unwrap_or(1.0),
        };
        match args.row {
            Some(r) if r < d.rows.len() => {
                let row = &mut d.rows[r];
                let pos = args
                    .position
                    .unwrap_or(row.panels.len())
                    .min(row.panels.len());
                row.panels.insert(pos, panel);
            }
            _ => d.rows.push(DashboardRow {
                height: DEFAULT_ROW_HEIGHT,
                panels: vec![panel],
            }),
        }
        let d = api.save_dashboard(d).await?;
        Ok(format!(
            "Added chart \"{title}\".\n{}",
            render_dashboard(&d)
        ))
    }

    /// Edit an existing chart's title, query, and/or type by position.
    pub async fn update_chart(&self, args: UpdateChartArgs) -> Result<String, OpError> {
        let api = self.dashboard_api()?;
        let mut d = self.resolve_dashboard(&args.dashboard).await?;
        let panel = d
            .rows
            .get_mut(args.row)
            .and_then(|row| row.panels.get_mut(args.column))
            .ok_or(OpError::UnknownPanel {
                row: args.row,
                column: args.column,
            })?;
        if let Some(t) = args.title {
            panel.visualization.title = t;
        }
        if let Some(q) = args.query {
            panel.visualization.seql = q;
        }
        if let Some(ct) = args.chart_type {
            panel.visualization.shape = Some(ct);
        }
        let d = api.save_dashboard(d).await?;
        Ok(format!(
            "Updated chart [{},{}].\n{}",
            args.row,
            args.column,
            render_dashboard(&d)
        ))
    }

    /// Rearrange a dashboard declaratively: place existing charts into new rows/columns
    /// with new widths (weights) and row heights. Charts are referenced by their current
    /// `(from_row, from_column)`; any chart not listed is dropped.
    pub async fn arrange_dashboard(&self, args: ArrangeDashboardArgs) -> Result<String, OpError> {
        let api = self.dashboard_api()?;
        let mut d = self.resolve_dashboard(&args.dashboard).await?;
        let mut new_rows = Vec::with_capacity(args.rows.len());
        for spec in &args.rows {
            let mut panels = Vec::with_capacity(spec.panels.len());
            for ps in &spec.panels {
                let src = d
                    .rows
                    .get(ps.from_row)
                    .and_then(|row| row.panels.get(ps.from_column))
                    .ok_or(OpError::UnknownPanel {
                        row: ps.from_row,
                        column: ps.from_column,
                    })?;
                let mut panel = src.clone();
                if let Some(w) = ps.weight {
                    panel.weight = w;
                }
                panels.push(panel);
            }
            new_rows.push(DashboardRow {
                height: spec.height.unwrap_or(DEFAULT_ROW_HEIGHT),
                panels,
            });
        }
        d.rows = new_rows;
        let d = api.save_dashboard(d).await?;
        Ok(format!("Rearranged dashboard.\n{}", render_dashboard(&d)))
    }

    /// Run a read-only SQL statement and collect up to `limit` rows.
    async fn query_readonly(
        &self,
        ctx: &SessionContext,
        sql: &str,
        limit: usize,
    ) -> Result<Vec<RecordBatch>, OpError> {
        let df = ctx.sql_with_options(sql, read_only()).await?;
        let df = df.limit(0, Some(limit))?;
        Ok(df.collect().await?)
    }

    async fn validate_column(
        &self,
        ctx: &SessionContext,
        table: &str,
        column: &str,
    ) -> Result<(), OpError> {
        let df = ctx.table(table).await?;
        if df.schema().as_arrow().field_with_name(column).is_err() {
            return Err(OpError::UnknownColumn {
                column: column.to_string(),
                table: table.to_string(),
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read-only SQL: reject DDL, DML, and multi-statement scripts so ad-hoc SQL
/// can never mutate storage or the shared session context.
fn read_only() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// Validate a signal table name, returning its [`SignalType`].
fn validate_table(table: &str) -> Result<SignalType, OpError> {
    hot_signal_type_for_table(table).ok_or_else(|| OpError::UnknownTable {
        table: table.to_string(),
        known: hot_signal_tables()
            .map(|(n, _)| n)
            .collect::<Vec<_>>()
            .join(", "),
    })
}

/// Quote a SQL identifier, escaping embedded double quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Extract a single `Int64`/`UInt64` scalar (e.g. a `count(*)`/`CAST(.. AS BIGINT)`)
/// from the first row of `batches` by column name.
fn scalar_i64(batches: &[RecordBatch], col: &str) -> Option<i64> {
    use arrow::array::{Int64Array, UInt64Array};
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let idx = batch.schema().index_of(col).ok()?;
    let arr = batch.column(idx);
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return (!a.is_null(0)).then(|| a.value(0));
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return (!a.is_null(0)).then(|| a.value(0) as i64);
    }
    None
}

/// Format a nanosecond duration compactly (e.g. `2h 15m`, `450ms`).
fn human_duration_ns(ns: u64) -> String {
    let secs = ns / 1_000_000_000;
    if secs == 0 {
        return format!("{}ms", ns / 1_000_000);
    }
    let (d, h, m, s) = (
        secs / 86400,
        (secs % 86400) / 3600,
        (secs % 3600) / 60,
        secs % 60,
    );
    let mut parts = Vec::new();
    if d > 0 {
        parts.push(format!("{d}d"));
    }
    if h > 0 {
        parts.push(format!("{h}h"));
    }
    if m > 0 && d == 0 {
        parts.push(format!("{m}m"));
    }
    if s > 0 && d == 0 && h == 0 {
        parts.push(format!("{s}s"));
    }
    if parts.is_empty() {
        parts.push(format!("{secs}s"));
    }
    parts.join(" ")
}

/// Render a dashboard's structure for the model: rows with heights, and each chart's
/// `[row,col]` position, title, weight, type, and query — the addresses edit tools use.
fn render_dashboard(d: &Dashboard) -> String {
    let mut out = format!(
        "Dashboard \"{}\" (id={}, {} rows, {} charts):\n",
        d.title,
        d.id,
        d.rows.len(),
        d.panel_count()
    );
    if d.rows.is_empty() {
        out.push_str("  (empty — add_chart to populate)\n");
    }
    for (r, row) in d.rows.iter().enumerate() {
        out.push_str(&format!("  row {r} (height={}):\n", row.height));
        for (c, p) in row.panels.iter().enumerate() {
            out.push_str(&format!(
                "    [{r},{c}] \"{}\" weight={} type={} :: {}\n",
                p.visualization.title,
                p.weight,
                p.visualization.shape.as_deref().unwrap_or("auto"),
                p.visualization.seql
            ));
        }
    }
    out
}

/// Resolve an attribute key to a promoted column name present in `cols`, accepting the
/// OTLP dotted key, the underscore column name, or a raw existing column.
fn resolve_promoted_column(cols: &[String], key: &str) -> Option<String> {
    let underscore = key.replace('.', "_");
    for a in sequins_arrow_schema::SEMCONV_ATTRIBUTES {
        if (a.key == key || a.column_name == key || a.column_name == underscore)
            && cols.iter().any(|c| c == a.column_name)
        {
            return Some(a.column_name.to_string());
        }
    }
    cols.iter().find(|c| **c == underscore).cloned()
}

/// Render record batches as an LLM-legible ASCII table.
fn render_batches(batches: &[RecordBatch]) -> Result<String, OpError> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok("(no rows)\n".to_string());
    }
    Ok(format!(
        "{}\n",
        arrow::util::pretty::pretty_format_batches(batches)?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_storage::test_fixtures::{
        make_test_otlp_metrics, make_test_otlp_traces, TestStorageBuilder,
    };
    use sequins_traits::OtlpIngest;

    /// Build a backend seeded with `n_spans` test spans.
    async fn seeded_tools(n_spans: usize) -> (Tools, tempfile::TempDir) {
        let (storage, temp) = TestStorageBuilder::new().build().await;
        storage
            .ingest_traces(make_test_otlp_traces(1, n_spans))
            .await
            .unwrap();
        let backend = Arc::new(DataFusionBackend::new(Arc::new(storage)));
        (Tools::new(backend), temp)
    }

    /// Profiling a `Float64` column (DataFusion's `approx_distinct` can't hash floats)
    /// must still return the row counts, degrading only the distinct estimate.
    #[tokio::test]
    async fn column_profile_float_column_degrades_gracefully() {
        let (storage, _t) = TestStorageBuilder::new().build().await;
        storage
            .ingest_metrics(make_test_otlp_metrics(1, 2, 4))
            .await
            .unwrap();
        let tools = Tools::new(Arc::new(DataFusionBackend::new(Arc::new(storage))));

        let out = tools
            .column_profile(ColumnProfileArgs {
                table: "datapoints".into(),
                column: "value".into(),
                top_k: Some(3),
            })
            .await
            .unwrap();

        assert!(out.contains("total_rows"), "counts must survive: {out}");
        assert!(
            out.contains("unavailable for this column type"),
            "approx_distinct should degrade, not error: {out}"
        );
    }

    #[tokio::test]
    async fn list_tables_includes_spans() {
        let (tools, _t) = seeded_tools(3).await;
        let out = tools.list_tables(ListTablesArgs {}).await.unwrap();
        assert!(out.contains("spans"));
        assert!(out.contains("logs"));
    }

    #[tokio::test]
    async fn describe_schema_spans() {
        let (tools, _t) = seeded_tools(3).await;
        let out = tools
            .describe_schema(DescribeSchemaArgs {
                table: "spans".into(),
            })
            .await
            .unwrap();
        assert!(out.contains("trace_id"));
        assert!(out.contains("span_id"));
    }

    #[tokio::test]
    async fn describe_schema_unknown_table_errors() {
        let (tools, _t) = seeded_tools(1).await;
        let err = tools
            .describe_schema(DescribeSchemaArgs {
                table: "nope".into(),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, OpError::UnknownTable { .. }));
    }

    #[tokio::test]
    async fn sample_returns_rows() {
        let (tools, _t) = seeded_tools(5).await;
        let out = tools
            .sample(SampleArgs {
                table: "spans".into(),
                limit: Some(3),
            })
            .await
            .unwrap();
        assert!(out.contains("rows from `spans`"));
        assert!(out.contains("trace_id"));
    }

    #[tokio::test]
    async fn time_range_reports_span() {
        let (tools, _t) = seeded_tools(5).await;
        let out = tools
            .time_range(TimeRangeArgs {
                table: "spans".into(),
            })
            .await
            .unwrap();
        assert!(out.contains("earliest_ns"));
        assert!(out.contains("latest_ns"));
    }

    #[tokio::test]
    async fn column_profile_name() {
        let (tools, _t) = seeded_tools(5).await;
        let out = tools
            .column_profile(ColumnProfileArgs {
                table: "spans".into(),
                column: "name".into(),
                top_k: Some(3),
            })
            .await
            .unwrap();
        assert!(out.contains("total"));
        assert!(out.contains("approx_distinct"));
    }

    #[tokio::test]
    async fn column_profile_unknown_column_errors() {
        let (tools, _t) = seeded_tools(1).await;
        let err = tools
            .column_profile(ColumnProfileArgs {
                table: "spans".into(),
                column: "not_a_col".into(),
                top_k: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, OpError::UnknownColumn { .. }));
    }

    #[tokio::test]
    async fn run_sql_select_works() {
        let (tools, _t) = seeded_tools(7).await;
        let out = tools
            .run_sql(RunSqlArgs {
                sql: "SELECT count(*) AS c FROM spans".into(),
                limit: None,
            })
            .await
            .unwrap();
        assert!(out.contains("row(s)"));
    }

    #[tokio::test]
    async fn run_sql_rejects_ddl() {
        let (tools, _t) = seeded_tools(1).await;
        let err = tools
            .run_sql(RunSqlArgs {
                sql: "CREATE TABLE evil (x INT)".into(),
                limit: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, OpError::DataFusion(_)));
    }

    #[tokio::test]
    async fn explain_shows_plan() {
        let (tools, _t) = seeded_tools(3).await;
        let out = tools
            .explain(ExplainArgs {
                sql: "SELECT name FROM spans".into(),
            })
            .await
            .unwrap();
        assert!(out.to_lowercase().contains("plan"));
    }

    #[tokio::test]
    async fn validate_seql_ok_and_err() {
        let (tools, _t) = seeded_tools(1).await;
        let ok = tools
            .validate_seql(ValidateSeqlArgs {
                query: "spans last 1h".into(),
            })
            .await
            .unwrap();
        assert!(ok.contains("\"ok\":true"));

        let bad = tools
            .validate_seql(ValidateSeqlArgs {
                query: "this is not seql @@@".into(),
            })
            .await
            .unwrap();
        assert!(bad.contains("\"ok\":false"));
    }

    #[tokio::test]
    async fn overview_lists_tables_with_counts() {
        let (tools, _t) = seeded_tools(5).await;
        let out = tools.overview(OverviewArgs {}).await.unwrap();
        assert!(out.contains("spans"), "overview mentions spans: {out}");
        // Empty tables should be flagged so the model avoids them.
        assert!(out.contains("empty"), "some tables are empty: {out}");
    }

    #[tokio::test]
    async fn list_attributes_runs_on_spans() {
        let (tools, _t) = seeded_tools(5).await;
        let out = tools
            .list_attributes(ListAttributesArgs {
                table: "spans".into(),
                kind: AttributeKind::Attr,
                limit: None,
            })
            .await
            .unwrap();
        assert!(out.contains("Attributes on `spans`"), "{out}");
    }

    #[tokio::test]
    async fn attribute_values_runs() {
        let (tools, _t) = seeded_tools(5).await;
        // `name` isn't an attribute, but the query path (promoted/overflow) must execute.
        let out = tools
            .attribute_values(AttributeValuesArgs {
                table: "spans".into(),
                key: "service.name".into(),
                top_k: Some(5),
            })
            .await
            .unwrap();
        assert!(out.contains("values of"), "{out}");
    }

    #[tokio::test]
    async fn seql_not_null_filter_keeps_rows() {
        let (tools, _t) = seeded_tools(6).await;
        // `name` is always populated, so `!= null` (meaning IS NOT NULL) must keep all 6
        // rows — not the SQL footgun `name <> NULL`, which drops every row.
        let out = tools
            .run_seql(RunSeqlArgs {
                query: "spans last 1h | where name != null | group by {} { count() as n }".into(),
                sample_rows: Some(1),
            })
            .await
            .unwrap();
        // The SQL footgun `name <> NULL` would drop every row → count 0. IS NOT NULL keeps
        // them, so the count must be non-zero.
        assert!(
            !out.contains("| 0 "),
            "!= null dropped all rows (should mean IS NOT NULL): {out}"
        );
    }

    #[tokio::test]
    async fn list_metrics_runs() {
        let (storage, _t) = TestStorageBuilder::new().build().await;
        storage
            .ingest_metrics(make_test_otlp_metrics(1, 2, 4))
            .await
            .unwrap();
        let tools = Tools::new(Arc::new(DataFusionBackend::new(Arc::new(storage))));
        let out = tools.list_metrics(ListMetricsArgs {}).await.unwrap();
        assert!(out.contains("Metrics"), "{out}");
    }

    #[tokio::test]
    async fn dashboard_crud_via_tools() {
        let (storage, _t) = TestStorageBuilder::new().build().await;
        let storage = Arc::new(storage);
        let backend = Arc::new(DataFusionBackend::new(storage.clone()));
        let tools = Tools::new(backend).with_dashboards(storage.clone() as Arc<dyn DashboardApi>);

        let out = tools
            .create_dashboard(CreateDashboardArgs {
                title: "Errors".into(),
            })
            .await
            .unwrap();
        assert!(out.contains("Created dashboard \"Errors\""), "{out}");

        tools
            .add_chart(AddChartArgs {
                dashboard: "Errors".into(),
                query: "spans last 1h | group by { ts() bin 1m as bucket } { count() as n }".into(),
                title: "Spans/min".into(),
                chart_type: Some("line".into()),
                row: None,
                position: None,
                weight: None,
            })
            .await
            .unwrap();
        let after = tools
            .add_chart(AddChartArgs {
                dashboard: "Errors".into(),
                query: "spans last 1h | group by {} { count() as total }".into(),
                title: "Total".into(),
                chart_type: Some("stat".into()),
                row: Some(0),
                position: None,
                weight: Some(2.0),
            })
            .await
            .unwrap();
        // Second chart added into row 0 → two side-by-side panels.
        assert!(
            after.contains("[0,0]") && after.contains("[0,1]"),
            "{after}"
        );

        let upd = tools
            .update_chart(UpdateChartArgs {
                dashboard: "Errors".into(),
                row: 0,
                column: 0,
                title: Some("Spans per minute".into()),
                query: None,
                chart_type: None,
            })
            .await
            .unwrap();
        assert!(upd.contains("Spans per minute"), "{upd}");

        // Rearrange into two rows, resizing one.
        let arr = tools
            .arrange_dashboard(ArrangeDashboardArgs {
                dashboard: "Errors".into(),
                rows: vec![
                    ArrangeRow {
                        height: None,
                        panels: vec![ArrangePanel {
                            from_row: 0,
                            from_column: 1,
                            weight: None,
                        }],
                    },
                    ArrangeRow {
                        height: Some(200.0),
                        panels: vec![ArrangePanel {
                            from_row: 0,
                            from_column: 0,
                            weight: None,
                        }],
                    },
                ],
            })
            .await
            .unwrap();
        assert!(arr.contains("row 0") && arr.contains("row 1"), "{arr}");

        // Without a dashboard handle the tools report unavailability.
        let no_dash = Tools::new(Arc::new(DataFusionBackend::new(storage.clone())));
        let err = no_dash
            .list_dashboards(ListDashboardsArgs {})
            .await
            .unwrap_err();
        assert!(matches!(err, OpError::DashboardsUnavailable));
    }

    #[tokio::test]
    async fn run_seql_summarizes_result() {
        let (tools, _t) = seeded_tools(6).await;
        let out = tools
            .run_seql(RunSeqlArgs {
                query: "spans last 1h LIMIT 5".into(),
                sample_rows: Some(3),
            })
            .await
            .unwrap();
        assert!(out.contains("shape:"));
        assert!(out.contains("columns:"));
    }
}
