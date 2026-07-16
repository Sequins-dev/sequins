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

use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SQLOptions;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use schemars::JsonSchema;
use sequins_arrow_schema::SignalType;
use sequins_datafusion_backend::{hot_signal_tables, hot_signal_type_for_table, DataFusionBackend};
use sequins_flight::{decode_metadata, ipc_to_batch, SeqlMetadata};
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

// ---------------------------------------------------------------------------
// Tools — holds the backend; every op is a method returning rendered text.
// ---------------------------------------------------------------------------

/// The shared tool operation layer over a [`DataFusionBackend`].
#[derive(Clone)]
pub struct Tools {
    backend: Arc<DataFusionBackend>,
}

impl Tools {
    /// Build the tool layer over an existing backend.
    pub fn new(backend: Arc<DataFusionBackend>) -> Self {
        Self { backend }
    }

    /// The backend these tools operate over.
    pub fn backend(&self) -> &Arc<DataFusionBackend> {
        &self.backend
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
