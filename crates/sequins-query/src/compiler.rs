//! SeQL to Substrait compiler
//!
//! Converts SeQL text or AST into Substrait binary plans for execution.
//! The output is a multi-root Substrait `Plan` with `SeqlExtension` metadata
//! embedded in `advanced_extensions.enhancement`.

use crate::ast::{
    AggregateFn, AggregateStage, ArithOp, CompareOp, ComputeStage, Expr as AstExpr, FieldRef,
    FilterStage, GroupExpr, LimitStage, Literal, MergeStage, Predicate, ProjectStage, QueryAst,
    QueryMode, ScalarFn, Signal, SortStage, Stage, TimeRange, UniqueStage,
};
use crate::correlation::{merge_join_key, navigate_join_key};
use crate::error::QueryError;
use crate::parser;
use crate::schema::infer_shape;
use arrow::datatypes::DataType as ArrowDataType;
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::SessionContext;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    lit, ColumnarValue, Expr as DfExpr, JoinType, LogicalPlan, LogicalPlanBuilder,
    ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::*;
use datafusion_expr::{cast, Expr, ExprFunctionExt};
use datafusion_functions_aggregate::expr_fn::{
    avg, count, count_distinct, first_value, last_value, max, min, sum,
};
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use datafusion_substrait::substrait::proto::extensions::AdvancedExtension;

/// Return type for `compile_merge_aux` — list of (alias, plan, signal) tuples boxed for recursion.
type MergeAuxFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Vec<(String, LogicalPlan, Signal)>, QueryError>>
            + Send
            + 'a,
    >,
>;
use prost::Message;

/// Compile SeQL text into a multi-root Substrait plan with `SeqlExtension` metadata.
///
/// # Arguments
/// * `seql` - The SeQL query text
/// * `ctx` - SessionContext with table schemas registered (use `schema_context()` for client-side compilation)
///
/// # Returns
/// Serialized Substrait `Plan` bytes containing:
/// - `relations[0]`: primary query (Navigate compiled as LeftSemi JOINs)
/// - `relations[1..N]`: auxiliary tables from Merge stages
/// - `advanced_extensions.enhancement`: `SeqlExtension` protobuf Any
pub async fn compile(seql: &str, ctx: &SessionContext) -> Result<Vec<u8>, QueryError> {
    let ast = parser::parse(seql).map_err(|e| QueryError::InvalidAst {
        message: format!("Parse error at offset {}: {}", e.offset, e.message),
    })?;
    compile_ast(ast, ctx).await
}

/// Compile a parsed `QueryAst` into a multi-root Substrait plan with `SeqlExtension` metadata.
///
/// Unlike `compile()`, this accepts a pre-parsed AST, allowing the caller to
/// modify the AST before compilation (e.g., set `mode = QueryMode::Live`).
pub async fn compile_ast(ast: QueryAst, ctx: &SessionContext) -> Result<Vec<u8>, QueryError> {
    let (primary_plan, auxiliary_plans) = ast_to_logical_plan(&ast, ctx).await?;

    // Serialize primary plan to Substrait
    let mut plan =
        *to_substrait_plan(&primary_plan, &ctx.state()).map_err(|e| QueryError::Execution {
            message: format!("Failed to serialize primary plan to Substrait: {}", e),
        })?;

    // Serialize each auxiliary plan as a complete, self-contained Substrait Plan
    // (with its own extensions). Storing only the relations was broken because
    // aux relations reference function anchors that only exist in the aux plan's
    // extensions — dropping extensions caused "function reference not found" errors.
    let mut auxiliary_aliases = Vec::new();
    let mut auxiliary_signals = Vec::new();
    let mut auxiliary_plan_bytes = Vec::new();
    for (alias, aux_plan, aux_signal) in &auxiliary_plans {
        let aux_substrait =
            to_substrait_plan(aux_plan, &ctx.state()).map_err(|e| QueryError::Execution {
                message: format!(
                    "Failed to serialize auxiliary plan '{}' to Substrait: {}",
                    alias, e
                ),
            })?;
        auxiliary_plan_bytes.push(aux_substrait.encode_to_vec());
        auxiliary_aliases.push(alias.clone());
        auxiliary_signals.push(signal_to_name(*aux_signal).to_string());
    }

    // Build SeqlExtension
    let response_shape = infer_shape(&ast);
    let time_range_proto = time_range_to_proto(&ast.scan.time_range);
    let mode_val = match ast.mode {
        QueryMode::Snapshot => crate::seql_ext::QueryMode::Snapshot as i32,
        QueryMode::Live => crate::seql_ext::QueryMode::Live as i32,
    };
    let seql_ext = crate::seql_ext::SeqlExtension {
        response_shape: response_shape.as_str().to_string(),
        signal: signal_to_name(ast.scan.signal).to_string(),
        time_range: Some(time_range_proto),
        mode: mode_val,
        cursor: None,
        auxiliary_aliases,
        auxiliary_signals,
        auxiliary_plan_bytes,
    };

    // Embed SeqlExtension in advanced_extensions.enhancement
    plan.advanced_extensions = Some(AdvancedExtension {
        enhancement: Some(::pbjson_types::Any {
            type_url: "type.googleapis.com/seql_extension.SeqlExtension".to_string(),
            value: seql_ext.encode_to_vec().into(),
        }),
        optimization: vec![],
    });

    Ok(plan.encode_to_vec())
}

/// Convert a QueryAst to a DataFusion primary LogicalPlan plus auxiliary plans from Merge stages.
///
/// Navigate stages are compiled as LeftSemi JOINs in the primary plan.
/// Merge stages produce additional auxiliary plans returned in the second element.
pub async fn ast_to_logical_plan(
    ast: &QueryAst,
    ctx: &SessionContext,
) -> Result<(LogicalPlan, Vec<(String, LogicalPlan, Signal)>), QueryError> {
    let mut auxiliary_plans: Vec<(String, LogicalPlan, Signal)> = Vec::new();
    let mut current_signal = ast.scan.signal;

    // Start with table scan
    let table_name = signal_to_table_name(current_signal);
    let table_provider =
        ctx.table_provider(table_name)
            .await
            .map_err(|e| QueryError::Execution {
                message: format!("Failed to get table provider for {}: {}", table_name, e),
            })?;
    let table_source = provider_as_source(table_provider);
    let mut builder = LogicalPlanBuilder::scan(table_name, table_source, None).map_err(|e| {
        QueryError::Execution {
            message: format!("Failed to create table scan for {}: {}", table_name, e),
        }
    })?;

    // Apply time range filter
    builder = apply_time_range_filter(builder, current_signal, &ast.scan.time_range)?;

    // Apply each stage in order
    for stage in &ast.stages {
        match stage {
            Stage::Filter(filter) => {
                builder = apply_filter(builder, filter, current_signal, ctx)?;
            }
            Stage::Project(project) => {
                builder = apply_project(builder, project, ctx)?;
            }
            Stage::Compute(compute) => {
                builder = apply_compute(builder, compute, current_signal, ctx)?;
            }
            Stage::Aggregate(aggregate) => {
                builder = apply_aggregate(builder, aggregate, current_signal, ctx)?;
            }
            Stage::Sort(sort) => {
                builder = apply_sort(builder, sort, current_signal, ctx)?;
            }
            Stage::Limit(limit) => {
                builder = apply_limit(builder, limit)?;
            }
            Stage::Unique(unique) => {
                builder = apply_unique(builder, unique)?;
            }
            Stage::TimeRange(time_range) => {
                builder = apply_time_range_stage(builder, current_signal, time_range)?;
            }
            Stage::Navigate(nav) => {
                // Compile Navigate as a LeftSemi JOIN: keep target rows where
                // the join_key appears in the current (filtered source) result.
                let join_key =
                    navigate_join_key(&current_signal, &nav.target).ok_or_else(|| {
                        QueryError::Execution {
                            message: format!(
                                "No navigate path from {:?} to {:?}",
                                current_signal, nav.target
                            ),
                        }
                    })?;

                let target_table = signal_to_table_name(nav.target);
                let target_provider =
                    ctx.table_provider(target_table)
                        .await
                        .map_err(|e| QueryError::Execution {
                            message: format!(
                                "Failed to get table for navigate target {}: {}",
                                target_table, e
                            ),
                        })?;
                let target_source = provider_as_source(target_provider);
                let target_builder = LogicalPlanBuilder::scan(target_table, target_source, None)
                    .map_err(|e| QueryError::Execution {
                        message: format!("Failed to scan navigate target {}: {}", target_table, e),
                    })?;

                // Build the current (source) plan as the semi-join filter side
                let source_plan = builder.build().map_err(|e| QueryError::Execution {
                    message: format!("Failed to build source plan for navigate: {}", e),
                })?;

                // target LEFT LeftSemi JOIN source RIGHT on join_key
                builder = target_builder
                    .join(
                        source_plan,
                        JoinType::LeftSemi,
                        (vec![join_key], vec![join_key]),
                        None,
                    )
                    .map_err(|e| QueryError::Execution {
                        message: format!("Failed to build navigate LeftSemi JOIN: {}", e),
                    })?;

                current_signal = nav.target;
            }
            Stage::Merge(merge) => {
                // Compile Merge as an auxiliary plan root; primary plan is unchanged.
                let primary_plan = builder.build().map_err(|e| QueryError::Execution {
                    message: format!("Failed to build primary plan for merge: {}", e),
                })?;
                let aux = compile_merge_aux(merge, &primary_plan, current_signal, ctx).await?;
                auxiliary_plans.extend(aux);
                // Restore builder from primary plan (primary is unchanged by Merge)
                builder = LogicalPlanBuilder::from(primary_plan);
            }
            Stage::Patterns(_) => {
                // Patterns stage is a no-op (was unimplemented)
            }
        }
    }

    let plan = builder.build().map_err(|e| QueryError::Execution {
        message: format!("Failed to build logical plan: {}", e),
    })?;

    Ok((plan, auxiliary_plans))
}

/// Recursively compile a `MergeStage` into one or more auxiliary `LogicalPlan`s.
///
/// The auxiliary plan scans the merge target and filters it to only rows whose
/// `join_key` appears in the parent plan's result. Inner stages (filter, sort,
/// limit, nested merges) are applied to the auxiliary plan.
fn compile_merge_aux<'a>(
    merge: &'a MergeStage,
    parent_plan: &'a LogicalPlan,
    parent_signal: Signal,
    ctx: &'a SessionContext,
) -> MergeAuxFuture<'a> {
    Box::pin(async move {
        let join_key =
            merge_join_key(&parent_signal, &merge.target).ok_or_else(|| QueryError::Execution {
                message: format!(
                    "No merge path from {:?} to {:?}",
                    parent_signal, merge.target
                ),
            })?;

        // Build target table scan
        let target_table = signal_to_table_name(merge.target);
        let target_provider =
            ctx.table_provider(target_table)
                .await
                .map_err(|e| QueryError::Execution {
                    message: format!(
                        "Failed to get table for merge target {}: {}",
                        target_table, e
                    ),
                })?;
        let target_source = provider_as_source(target_provider);
        let mut aux_builder =
            LogicalPlanBuilder::scan(target_table, target_source, None).map_err(|e| {
                QueryError::Execution {
                    message: format!("Failed to scan merge target {}: {}", target_table, e),
                }
            })?;

        // Project parent plan to just the join key for the semi-join filter
        let parent_key_plan = LogicalPlanBuilder::from(parent_plan.clone())
            .project(vec![col(join_key)])
            .map_err(|e| QueryError::Execution {
                message: format!("Failed to project parent join key for merge: {}", e),
            })?
            .distinct()
            .map_err(|e| QueryError::Execution {
                message: format!("Failed to deduplicate parent keys for merge: {}", e),
            })?
            .build()
            .map_err(|e| QueryError::Execution {
                message: format!("Failed to build parent key plan for merge: {}", e),
            })?;

        // LeftSemi JOIN: keep target rows where join_key appears in parent's key set
        aux_builder = aux_builder
            .join(
                parent_key_plan,
                JoinType::LeftSemi,
                (vec![join_key], vec![join_key]),
                None,
            )
            .map_err(|e| QueryError::Execution {
                message: format!("Failed to build merge LeftSemi JOIN: {}", e),
            })?;

        // Apply inner stages
        let mut current_signal = merge.target;
        let mut nested_aux_plans: Vec<(String, LogicalPlan, Signal)> = Vec::new();

        for stage in &merge.stages {
            match stage {
                Stage::Filter(filter) => {
                    aux_builder = apply_filter(aux_builder, filter, current_signal, ctx)?;
                }
                Stage::Project(project) => {
                    aux_builder = apply_project(aux_builder, project, ctx)?;
                }
                Stage::Compute(compute) => {
                    aux_builder = apply_compute(aux_builder, compute, current_signal, ctx)?;
                }
                Stage::Aggregate(aggregate) => {
                    aux_builder = apply_aggregate(aux_builder, aggregate, current_signal, ctx)?;
                }
                Stage::Sort(sort) => {
                    aux_builder = apply_sort(aux_builder, sort, current_signal, ctx)?;
                }
                Stage::Limit(limit) => {
                    aux_builder = apply_limit(aux_builder, limit)?;
                }
                Stage::Unique(unique) => {
                    aux_builder = apply_unique(aux_builder, unique)?;
                }
                Stage::TimeRange(tr) => {
                    aux_builder = apply_time_range_stage(aux_builder, current_signal, tr)?;
                }
                Stage::Navigate(nav) => {
                    // Navigate within a merge inner stage
                    let nav_join_key =
                        navigate_join_key(&current_signal, &nav.target).ok_or_else(|| {
                            QueryError::Execution {
                                message: format!(
                                    "No navigate path from {:?} to {:?} (inside merge)",
                                    current_signal, nav.target
                                ),
                            }
                        })?;
                    let nav_table = signal_to_table_name(nav.target);
                    let nav_provider =
                        ctx.table_provider(nav_table)
                            .await
                            .map_err(|e| QueryError::Execution {
                                message: format!("Failed to get table {}: {}", nav_table, e),
                            })?;
                    let nav_source = provider_as_source(nav_provider);
                    let nav_target_builder = LogicalPlanBuilder::scan(nav_table, nav_source, None)
                        .map_err(|e| QueryError::Execution {
                            message: format!("Failed to scan {}: {}", nav_table, e),
                        })?;
                    let source_plan = aux_builder.build().map_err(|e| QueryError::Execution {
                        message: format!("Failed to build source plan for merge navigate: {}", e),
                    })?;
                    aux_builder = nav_target_builder
                        .join(
                            source_plan,
                            JoinType::LeftSemi,
                            (vec![nav_join_key], vec![nav_join_key]),
                            None,
                        )
                        .map_err(|e| QueryError::Execution {
                            message: format!(
                                "Failed to build navigate LeftSemi JOIN (merge): {}",
                                e
                            ),
                        })?;
                    current_signal = nav.target;
                }
                Stage::Merge(inner_merge) => {
                    // Nested merge: build aux plans with the current aux plan as parent
                    let current_plan = aux_builder.build().map_err(|e| QueryError::Execution {
                        message: format!("Failed to build plan for nested merge: {}", e),
                    })?;
                    let inner_plans =
                        compile_merge_aux(inner_merge, &current_plan, current_signal, ctx).await?;
                    nested_aux_plans.extend(inner_plans);
                    // Restore aux builder from built plan (unchanged by nested merge)
                    aux_builder = LogicalPlanBuilder::from(current_plan);
                }
                Stage::Patterns(_) => {
                    // Patterns stage is a no-op
                }
            }
        }

        let aux_plan = aux_builder.build().map_err(|e| QueryError::Execution {
            message: format!(
                "Failed to build aux plan for merge '{}': {}",
                merge.alias, e
            ),
        })?;

        let mut result = vec![(merge.alias.clone(), aux_plan, merge.target)];
        result.extend(nested_aux_plans);
        Ok(result)
    })
}

/// Map a `Signal` to its SeqlExtension signal name (lowercase string)
fn signal_to_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Spans => "spans",
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
        Signal::SpanLinks => "span_links",
    }
}

/// Convert a `TimeRange` to the SeqlExtension protobuf `TimeRange` message
fn time_range_to_proto(time_range: &TimeRange) -> crate::seql_ext::TimeRange {
    match time_range {
        TimeRange::SlidingWindow { start_ns } => crate::seql_ext::TimeRange {
            range: Some(crate::seql_ext::time_range::Range::SlidingWindowNs(
                *start_ns,
            )),
        },
        TimeRange::Absolute { start_ns, end_ns } => crate::seql_ext::TimeRange {
            range: Some(crate::seql_ext::time_range::Range::Absolute(
                crate::seql_ext::AbsoluteRange {
                    start_ns: *start_ns,
                    end_ns: *end_ns,
                },
            )),
        },
    }
}

// ---------------------------------------------------------------------------
// Overflow attribute UDF stubs for schema-context compilation
// ---------------------------------------------------------------------------
//
// These stub types provide the correct name + return type for the overflow
// extraction UDFs so that attr.* queries on non-promoted attributes can be
// compiled to Substrait in a schema-only context.  The real implementations
// are registered in `DataFusionBackend::make_session_ctx()`.

macro_rules! overflow_stub_udf {
    ($struct_name:ident, $udf_name:expr, $sig_static:ident, $ret:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct $struct_name;

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn name(&self) -> &str {
                $udf_name
            }
            fn signature(&self) -> &Signature {
                static $sig_static: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
                $sig_static.get_or_init(|| Signature::any(2, Volatility::Immutable))
            }
            fn return_type(
                &self,
                _arg_types: &[arrow::datatypes::DataType],
            ) -> datafusion::error::Result<arrow::datatypes::DataType> {
                Ok($ret)
            }
            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> datafusion::error::Result<ColumnarValue> {
                Err(datafusion::error::DataFusionError::NotImplemented(format!(
                    "{} stub: only available in server execution context",
                    $udf_name
                )))
            }
        }
    };
}

overflow_stub_udf!(
    OverflowGetStrStub,
    "overflow_get_str",
    STUB_SIG_STR,
    arrow::datatypes::DataType::Utf8
);
overflow_stub_udf!(
    OverflowGetI64Stub,
    "overflow_get_i64",
    STUB_SIG_I64,
    arrow::datatypes::DataType::Int64
);
overflow_stub_udf!(
    OverflowGetF64Stub,
    "overflow_get_f64",
    STUB_SIG_F64,
    arrow::datatypes::DataType::Float64
);
overflow_stub_udf!(
    OverflowGetBoolStub,
    "overflow_get_bool",
    STUB_SIG_BOOL,
    arrow::datatypes::DataType::Boolean
);

/// Register overflow extraction UDF stubs for schema-context compilation.
///
/// Stubs have the correct name and return type so that `attr.*` references on
/// non-promoted attributes can be compiled to valid Substrait plans.
fn register_overflow_stubs(ctx: &SessionContext) {
    use datafusion::logical_expr::ScalarUDF;
    ctx.register_udf(ScalarUDF::new_from_impl(OverflowGetStrStub));
    ctx.register_udf(ScalarUDF::new_from_impl(OverflowGetI64Stub));
    ctx.register_udf(ScalarUDF::new_from_impl(OverflowGetF64Stub));
    ctx.register_udf(ScalarUDF::new_from_impl(OverflowGetBoolStub));
}

/// Create a schema-only SessionContext for client-side compilation
///
/// This returns a SessionContext with empty MemTable providers for all signal types,
/// using the known schemas from `sequins-types`. Remote clients can use this to compile
/// SeQL to Substrait without needing actual data access.
pub fn schema_context() -> Result<SessionContext, QueryError> {
    let ctx = SessionContext::new();
    register_overflow_stubs(&ctx);

    // Register empty MemTable for every signal type (needed for Navigate JOINs and Merge aux roots)
    register_empty_table(&ctx, "spans", sequins_types::arrow_schema::span_schema())?;
    register_empty_table(&ctx, "logs", sequins_types::arrow_schema::log_schema())?;
    register_empty_table(
        &ctx,
        "metrics",
        sequins_types::arrow_schema::metric_schema(),
    )?;
    register_empty_table(
        &ctx,
        "datapoints",
        sequins_types::arrow_schema::series_data_point_schema(),
    )?;
    register_empty_table(
        &ctx,
        "histogram_data_points",
        sequins_types::arrow_schema::histogram_series_data_point_schema(),
    )?;
    register_empty_table(
        &ctx,
        "samples",
        sequins_types::arrow_schema::profile_samples_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profiles",
        sequins_types::arrow_schema::profile_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profile_stacks",
        sequins_types::arrow_schema::profile_stacks_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profile_frames",
        sequins_types::arrow_schema::profile_frames_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profile_mappings",
        sequins_types::arrow_schema::profile_mappings_schema(),
    )?;
    register_empty_table(
        &ctx,
        "resources",
        sequins_types::arrow_schema::resource_schema(),
    )?;
    register_empty_table(&ctx, "scopes", sequins_types::arrow_schema::scope_schema())?;
    register_empty_table(
        &ctx,
        "span_links",
        sequins_types::arrow_schema::span_links_schema(),
    )?;

    Ok(ctx)
}

fn register_empty_table(
    ctx: &SessionContext,
    name: &str,
    schema: std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<(), QueryError> {
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    // Create an empty RecordBatch with the schema
    let batch = RecordBatch::new_empty(schema.clone());

    // MemTable requires at least one partition, even if empty
    let table =
        MemTable::try_new(schema, vec![vec![batch]]).map_err(|e| QueryError::Execution {
            message: format!("Failed to create MemTable for {}: {}", name, e),
        })?;

    ctx.register_table(name, std::sync::Arc::new(table))
        .map_err(|e| QueryError::Execution {
            message: format!("Failed to register table {}: {}", name, e),
        })?;

    Ok(())
}

// ── Helper functions ──────────────────────────────────────────────────────────

fn signal_to_table_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Spans => "spans",
        Signal::SpanLinks => "span_links",
        Signal::Logs => "logs",
        Signal::Datapoints => "datapoints",
        Signal::Histograms => "histogram_data_points",
        Signal::Metrics => "metrics",
        Signal::Samples => "samples",
        Signal::Traces => "spans", // Traces use the spans table with filtering
        Signal::Profiles => "profiles",
        Signal::Stacks => "profile_stacks",
        Signal::Frames => "profile_frames",
        Signal::Mappings => "profile_mappings",
        Signal::Resources => "resources",
        Signal::Scopes => "scopes",
    }
}

/// Returns the primary join key column for a signal type
#[allow(dead_code)]
fn signal_join_key(signal: Signal) -> Option<&'static str> {
    match signal {
        Signal::Spans => Some("span_id"),
        Signal::SpanLinks => Some("span_id"),
        Signal::Logs => Some("log_id"),
        Signal::Datapoints => Some("metric_id"),
        Signal::Histograms => Some("metric_id"),
        Signal::Metrics => Some("metric_id"),
        Signal::Samples => Some("profile_id"),
        Signal::Traces => Some("trace_id"),
        Signal::Profiles => Some("profile_id"),
        Signal::Stacks => Some("stack_id"),
        Signal::Frames => Some("frame_id"),
        Signal::Mappings => Some("mapping_id"),
        Signal::Resources => Some("resource_id"),
        Signal::Scopes => Some("scope_id"),
    }
}

/// Returns the primary time column name for a signal type, or `None` if the signal has no time column.
pub fn time_column_for_signal(signal: Signal) -> Option<&'static str> {
    match signal {
        Signal::Spans => Some("start_time_unix_nano"),
        Signal::Logs => Some("time_unix_nano"),
        Signal::Datapoints => Some("time_unix_nano"),
        Signal::Histograms => Some("time_unix_nano"),
        Signal::Samples => Some("time_unix_nano"),
        Signal::Traces => Some("start_time_unix_nano"),
        Signal::Profiles => Some("time_unix_nano"),
        // Deduplicated entities have no time column
        Signal::SpanLinks
        | Signal::Metrics
        | Signal::Stacks
        | Signal::Frames
        | Signal::Mappings
        | Signal::Resources
        | Signal::Scopes => None,
    }
}

fn apply_time_range_filter(
    builder: LogicalPlanBuilder,
    signal: Signal,
    time_range: &TimeRange,
) -> Result<LogicalPlanBuilder, QueryError> {
    let time_col = match time_column_for_signal(signal) {
        Some(col) => col,
        None => return Ok(builder),
    };

    match time_range {
        TimeRange::Absolute { start_ns, end_ns } => {
            // Cast timestamp column to Int64 for comparison (Vortex stores as Timestamp type)
            let start_expr = cast(col(time_col), ArrowDataType::Int64).gt_eq(lit(*start_ns as i64));

            let end_expr = cast(col(time_col), ArrowDataType::Int64).lt_eq(lit(*end_ns as i64));

            builder
                .filter(start_expr.and(end_expr))
                .map_err(|e| QueryError::Execution {
                    message: format!("Failed to apply time range filter: {}", e),
                })
        }
        TimeRange::SlidingWindow { start_ns } => {
            // For sliding window, compute end_ns = now() at compile time
            let now_ns = sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime);

            let start_expr =
                cast(col(time_col), ArrowDataType::Int64).gt_eq(lit((now_ns - start_ns) as i64));

            let end_expr = cast(col(time_col), ArrowDataType::Int64).lt_eq(lit(now_ns as i64));

            builder
                .filter(start_expr.and(end_expr))
                .map_err(|e| QueryError::Execution {
                    message: format!("Failed to apply sliding window filter: {}", e),
                })
        }
    }
}

fn apply_time_range_stage(
    builder: LogicalPlanBuilder,
    signal: Signal,
    stage: &crate::ast::TimeRangeStage,
) -> Result<LogicalPlanBuilder, QueryError> {
    let time_col = match time_column_for_signal(signal) {
        Some(col) => col,
        None => return Ok(builder),
    };

    // TimeRangeStage always uses sliding window (last N duration)
    let now_ns = sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime);

    let start_expr =
        cast(col(time_col), ArrowDataType::Int64).gt_eq(lit((now_ns - stage.duration_ns) as i64));

    let end_expr = cast(col(time_col), ArrowDataType::Int64).lt_eq(lit(now_ns as i64));

    builder
        .filter(start_expr.and(end_expr))
        .map_err(|e| QueryError::Execution {
            message: format!("Failed to apply time range stage filter: {}", e),
        })
}

pub fn apply_filter(
    builder: LogicalPlanBuilder,
    filter: &FilterStage,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<LogicalPlanBuilder, QueryError> {
    let expr = predicate_to_expr(&filter.predicate, builder.schema(), signal, ctx)?;
    builder.filter(expr).map_err(|e| QueryError::Execution {
        message: format!("Failed to apply filter: {}", e),
    })
}

pub fn apply_project(
    builder: LogicalPlanBuilder,
    project: &ProjectStage,
    ctx: &SessionContext,
) -> Result<LogicalPlanBuilder, QueryError> {
    let exprs: Result<Vec<_>, _> = project
        .fields
        .iter()
        .map(|pf| {
            let expr = field_to_expr(&pf.field, ctx)?;
            Ok(if let Some(alias) = &pf.alias {
                expr.alias(alias)
            } else {
                expr
            })
        })
        .collect();

    builder.project(exprs?).map_err(|e| QueryError::Execution {
        message: format!("Failed to apply project: {}", e),
    })
}

pub fn apply_compute(
    builder: LogicalPlanBuilder,
    compute: &ComputeStage,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<LogicalPlanBuilder, QueryError> {
    let mut current = builder;

    for derivation in &compute.derivations {
        let expr = ast_expr_to_df_expr(&derivation.expr, current.schema(), signal, ctx)?;
        let aliased = expr.alias(&derivation.alias);

        // Add as a new column using project with existing columns + new column
        let schema = current.schema();
        let mut exprs: Vec<DfExpr> = schema.fields().iter().map(|f| col(f.name())).collect();
        exprs.push(aliased);

        current = current.project(exprs).map_err(|e| QueryError::Execution {
            message: format!(
                "Failed to add computed column '{}': {}",
                derivation.alias, e
            ),
        })?;
    }

    Ok(current)
}

pub fn apply_aggregate(
    builder: LogicalPlanBuilder,
    aggregate: &AggregateStage,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<LogicalPlanBuilder, QueryError> {
    // Build group expressions
    let group_exprs: Result<Vec<_>, _> = aggregate
        .group_by
        .iter()
        .map(|ge| group_expr_to_df_expr(ge, builder.schema(), signal, ctx))
        .collect();

    // Build aggregation expressions
    let agg_exprs: Result<Vec<_>, _> = aggregate
        .aggregations
        .iter()
        .map(|agg| {
            let expr = aggregate_fn_to_df_expr(&agg.function, builder.schema(), signal, ctx)?;
            let expr = if let Some(predicate) = &agg.filter {
                let filter_expr = predicate_to_expr(predicate, builder.schema(), signal, ctx)?;
                expr.filter(filter_expr)
                    .build()
                    .map_err(|e| QueryError::Execution {
                        message: format!("Failed to apply aggregate filter: {}", e),
                    })?
            } else {
                expr
            };
            Ok(expr.alias(&agg.alias))
        })
        .collect();

    builder
        .aggregate(group_exprs?, agg_exprs?)
        .map_err(|e| QueryError::Execution {
            message: format!("Failed to apply aggregate: {}", e),
        })
}

pub fn apply_sort(
    builder: LogicalPlanBuilder,
    sort: &SortStage,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<LogicalPlanBuilder, QueryError> {
    let exprs: Result<Vec<_>, _> = sort
        .exprs
        .iter()
        .map(|se| {
            let expr = ast_expr_to_df_expr(&se.expr, builder.schema(), signal, ctx)?;
            Ok(expr.sort(se.ascending, true)) // nulls last
        })
        .collect();

    builder.sort(exprs?).map_err(|e| QueryError::Execution {
        message: format!("Failed to apply sort: {}", e),
    })
}

pub fn apply_limit(
    builder: LogicalPlanBuilder,
    limit: &LimitStage,
) -> Result<LogicalPlanBuilder, QueryError> {
    let skip = limit.offset.unwrap_or(0) as usize;
    let fetch = Some(limit.limit as usize);

    builder
        .limit(skip, fetch)
        .map_err(|e| QueryError::Execution {
            message: format!("Failed to apply limit: {}", e),
        })
}

pub fn apply_unique(
    builder: LogicalPlanBuilder,
    _unique: &UniqueStage,
) -> Result<LogicalPlanBuilder, QueryError> {
    // Unique is implemented as DISTINCT on all columns
    // TODO: Add support for unique on specific field
    builder.distinct().map_err(|e| QueryError::Execution {
        message: format!("Failed to apply unique: {}", e),
    })
}

// ── Expression translation ────────────────────────────────────────────────────

fn predicate_to_expr(
    pred: &Predicate,
    schema: &datafusion::common::DFSchemaRef,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<DfExpr, QueryError> {
    match pred {
        Predicate::And(left, right) => {
            let left_expr = predicate_to_expr(left, schema, signal, ctx)?;
            let right_expr = predicate_to_expr(right, schema, signal, ctx)?;
            Ok(left_expr.and(right_expr))
        }
        Predicate::Or(left, right) => {
            let left_expr = predicate_to_expr(left, schema, signal, ctx)?;
            let right_expr = predicate_to_expr(right, schema, signal, ctx)?;
            Ok(left_expr.or(right_expr))
        }
        Predicate::Not(inner) => {
            let inner_expr = predicate_to_expr(inner, schema, signal, ctx)?;
            Ok(inner_expr.not())
        }
        Predicate::Compare(cmp) => {
            let left = ast_expr_to_df_expr(&cmp.left, schema, signal, ctx)?;
            let right = ast_expr_to_df_expr(&cmp.right, schema, signal, ctx)?;
            Ok(match cmp.op {
                CompareOp::Eq => left.eq(right),
                CompareOp::Neq => left.not_eq(right),
                CompareOp::Gt => left.gt(right),
                CompareOp::Gte => left.gt_eq(right),
                CompareOp::Lt => left.lt(right),
                CompareOp::Lte => left.lt_eq(right),
            })
        }
        Predicate::Exists(field) => {
            let expr = field_to_expr(field, ctx)?;
            Ok(expr.is_not_null())
        }
        Predicate::NotExists(field) => {
            let expr = field_to_expr(field, ctx)?;
            Ok(expr.is_null())
        }
        Predicate::Contains { field, value } => {
            let expr = field_to_expr(field, ctx)?;
            // Use ILIKE %value% for case-insensitive matching
            Ok(expr.ilike(lit(format!("%{}%", value))))
        }
        Predicate::StartsWith { field, value } => {
            let expr = field_to_expr(field, ctx)?;
            // Use ILIKE value% for case-insensitive matching
            Ok(expr.ilike(lit(format!("{}%", value))))
        }
        Predicate::EndsWith { field, value } => {
            let expr = field_to_expr(field, ctx)?;
            // Use ILIKE %value for case-insensitive matching
            Ok(expr.ilike(lit(format!("%{}", value))))
        }
        Predicate::Matches { field, pattern } => {
            let expr = field_to_expr(field, ctx)?;
            // Use regexp_like which returns a boolean (regexp_match returns List)
            Ok(datafusion::prelude::regexp_like(
                expr,
                lit(pattern.clone()),
                None,
            ))
        }
        Predicate::In { field, values } => {
            let expr = field_to_expr(field, ctx)?;
            let lit_values: Result<Vec<_>, _> = values.iter().map(literal_to_df_lit).collect();
            Ok(expr.in_list(lit_values?, false))
        }
    }
}

fn field_to_expr(field: &FieldRef, ctx: &SessionContext) -> Result<DfExpr, QueryError> {
    use crate::ast::AttrScope;
    match field.scope {
        AttrScope::Attribute => {
            let catalog = sequins_types::arrow_schema::default_schema_catalog();
            if let Some(idx) = catalog.column_index(&field.name) {
                // Promoted attribute: use the catalog column name directly (no prefix).
                // e.g. "http.request.method" → col("http_request_method")
                Ok(col(catalog.promoted[idx].column_name))
            } else {
                // Non-promoted attribute: extract from the overflow map using the
                // registered `overflow_get_str` UDF.
                // e.g. "my.custom.key" → overflow_get_str(col("_overflow_attrs"), "my.custom.key")
                let udf = ctx.udf("overflow_get_str").map_err(|e| QueryError::Execution {
                    message: format!(
                        "overflow_get_str UDF not registered (is the session context configured?): {}",
                        e
                    ),
                })?;
                Ok(udf.call(vec![col("_overflow_attrs"), lit(field.name.clone())]))
            }
        }
        // Direct signal fields and auto-resolved names use the field name as-is
        AttrScope::Signal | AttrScope::Auto | AttrScope::Resource | AttrScope::Scope => {
            Ok(col(&field.name))
        }
    }
}

pub fn ast_expr_to_df_expr(
    expr: &AstExpr,
    _schema: &datafusion::common::DFSchemaRef,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<DfExpr, QueryError> {
    match expr {
        AstExpr::Field(field) => field_to_expr(field, ctx),
        AstExpr::Literal(lit_val) => literal_to_df_lit(lit_val),
        AstExpr::BinaryOp { left, op, right } => {
            let left_expr = ast_expr_to_df_expr(left, _schema, signal, ctx)?;
            let right_expr = ast_expr_to_df_expr(right, _schema, signal, ctx)?;
            Ok(match op {
                ArithOp::Add => left_expr + right_expr,
                ArithOp::Sub => left_expr - right_expr,
                ArithOp::Mul => left_expr * right_expr,
                ArithOp::Div => left_expr / right_expr,
                ArithOp::Mod => left_expr % right_expr,
            })
        }
        AstExpr::FunctionCall { function, args } => {
            let arg_exprs: Result<Vec<_>, _> = args
                .iter()
                .map(|arg| ast_expr_to_df_expr(arg, _schema, signal, ctx))
                .collect();
            let arg_exprs = arg_exprs?;

            match function {
                ScalarFn::Timestamp => {
                    if !arg_exprs.is_empty() {
                        return Err(QueryError::Execution {
                            message: "ts() expects no arguments".into(),
                        });
                    }
                    let col_name =
                        time_column_for_signal(signal).ok_or_else(|| QueryError::Execution {
                            message: format!("Signal {:?} has no time column", signal),
                        })?;
                    Ok(col(col_name))
                }
                ScalarFn::Abs => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "abs() expects 1 argument".into(),
                        });
                    }
                    Ok(datafusion::prelude::abs(arg_exprs[0].clone()))
                }
                ScalarFn::Round => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "round() expects 1 argument".into(),
                        });
                    }
                    // Round with 0 decimal places
                    Ok(datafusion::prelude::round(vec![
                        arg_exprs[0].clone(),
                        lit(0),
                    ]))
                }
                ScalarFn::Ceil => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "ceil() expects 1 argument".into(),
                        });
                    }
                    Ok(datafusion::prelude::ceil(arg_exprs[0].clone()))
                }
                ScalarFn::Floor => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "floor() expects 1 argument".into(),
                        });
                    }
                    Ok(datafusion::prelude::floor(arg_exprs[0].clone()))
                }
                ScalarFn::ToMillis => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "to_millis() expects 1 argument".into(),
                        });
                    }
                    // Divide nanoseconds by 1_000_000
                    Ok(arg_exprs[0].clone() / lit(1_000_000_i64))
                }
                ScalarFn::ToSeconds => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "to_seconds() expects 1 argument".into(),
                        });
                    }
                    // Divide nanoseconds by 1_000_000_000
                    Ok(arg_exprs[0].clone() / lit(1_000_000_000_i64))
                }
                ScalarFn::ToString => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "to_string() expects 1 argument".into(),
                        });
                    }
                    // Cast to string
                    Ok(cast(arg_exprs[0].clone(), ArrowDataType::Utf8))
                }
                ScalarFn::Len => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "len() expects 1 argument".into(),
                        });
                    }
                    Ok(datafusion::prelude::length(arg_exprs[0].clone()))
                }
                ScalarFn::Lower => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "lower() expects 1 argument".into(),
                        });
                    }
                    Ok(datafusion::prelude::lower(arg_exprs[0].clone()))
                }
                ScalarFn::Upper => {
                    if arg_exprs.len() != 1 {
                        return Err(QueryError::Execution {
                            message: "upper() expects 1 argument".into(),
                        });
                    }
                    Ok(datafusion::prelude::upper(arg_exprs[0].clone()))
                }
            }
        }
    }
}

fn literal_to_df_lit(lit_val: &Literal) -> Result<DfExpr, QueryError> {
    Ok(match lit_val {
        Literal::Null => lit(datafusion::scalar::ScalarValue::Null),
        Literal::Bool(b) => lit(*b),
        Literal::Int(i) => lit(*i),
        Literal::UInt(u) => lit(*u),
        Literal::Float(f) => lit(*f),
        Literal::String(s) => lit(s.clone()),
        Literal::Duration(ns) => lit(*ns as i64),
        Literal::Timestamp(ns) => lit(*ns as i64),
        Literal::Status(status) => {
            // status column is UInt8: Unset=0, Ok=1, Error=2
            let v: u8 = match status {
                crate::ast::StatusLiteral::Unset => 0,
                crate::ast::StatusLiteral::Ok => 1,
                crate::ast::StatusLiteral::Error => 2,
            };
            lit(v)
        }
        Literal::SpanKind(kind) => {
            // kind column is UInt8: Unspecified=0, Internal=1, Server=2, Client=3, Producer=4, Consumer=5
            let v: u8 = match kind {
                crate::ast::SpanKindLiteral::Internal => 1,
                crate::ast::SpanKindLiteral::Server => 2,
                crate::ast::SpanKindLiteral::Client => 3,
                crate::ast::SpanKindLiteral::Producer => 4,
                crate::ast::SpanKindLiteral::Consumer => 5,
            };
            lit(v)
        }
        Literal::Severity(severity) => {
            // Convert to string
            let s = match severity {
                crate::ast::SeverityLiteral::Trace => "TRACE",
                crate::ast::SeverityLiteral::Debug => "DEBUG",
                crate::ast::SeverityLiteral::Info => "INFO",
                crate::ast::SeverityLiteral::Warn => "WARN",
                crate::ast::SeverityLiteral::Error => "ERROR",
                crate::ast::SeverityLiteral::Fatal => "FATAL",
            };
            lit(s)
        }
    })
}

fn group_expr_to_df_expr(
    group_expr: &GroupExpr,
    schema: &datafusion::common::DFSchemaRef,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<DfExpr, QueryError> {
    let expr = ast_expr_to_df_expr(&group_expr.expr, schema, signal, ctx)?;

    // Handle time binning
    let expr = if let Some(bin_ns) = group_expr.bin_ns {
        // Bin by dividing timestamp by bin size, then multiplying back.
        // Time columns are stored as Timestamp(ns) — cast to Int64 first so
        // integer arithmetic works correctly.
        // E.g., for 5-minute bins: (cast(ts, Int64) / 300_000_000_000) * 300_000_000_000
        let bin_lit = lit(bin_ns as i64);
        let int_expr = cast(expr, ArrowDataType::Int64);
        (int_expr.clone() / bin_lit.clone()) * bin_lit
    } else {
        expr
    };

    // Apply alias if provided
    Ok(if let Some(alias) = &group_expr.alias {
        expr.alias(alias)
    } else {
        expr
    })
}

fn aggregate_fn_to_df_expr(
    agg_fn: &AggregateFn,
    schema: &datafusion::common::DFSchemaRef,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<DfExpr, QueryError> {
    match agg_fn {
        AggregateFn::Count => Ok(count(lit(1))),
        AggregateFn::Sum(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(sum(df_expr))
        }
        AggregateFn::Avg(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(avg(df_expr))
        }
        AggregateFn::Min(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(min(df_expr))
        }
        AggregateFn::Max(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(max(df_expr))
        }
        AggregateFn::P50(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            // Use median which is equivalent to P50
            Ok(datafusion_functions_aggregate::expr_fn::median(df_expr))
        }
        AggregateFn::P95(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            // Use approx_percentile_cont UDAF with percentile parameter
            let udaf =
                datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf(
                );
            Ok(Expr::AggregateFunction(
                datafusion_expr::expr::AggregateFunction::new_udf(
                    udaf,
                    vec![df_expr, lit(0.95)],
                    false,  // distinct
                    None,   // filter
                    vec![], // order_by
                    None,   // null_treatment
                ),
            ))
        }
        AggregateFn::P99(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            // Use approx_percentile_cont UDAF with percentile parameter
            let udaf =
                datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf(
                );
            Ok(Expr::AggregateFunction(
                datafusion_expr::expr::AggregateFunction::new_udf(
                    udaf,
                    vec![df_expr, lit(0.99)],
                    false,  // distinct
                    None,   // filter
                    vec![], // order_by
                    None,   // null_treatment
                ),
            ))
        }
        AggregateFn::Distinct(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(count_distinct(df_expr))
        }
        AggregateFn::First(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(first_value(df_expr, vec![]))
        }
        AggregateFn::Last(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(last_value(df_expr, vec![]))
        }
        // Domain-specific aggregates need custom handling
        AggregateFn::ErrorRate => {
            // COUNT(CASE WHEN status=2 THEN 1 END) / COUNT(*) — status is UInt8, Error=2
            let case_expr = when(col("status").eq(lit(2u8)), lit(1))
                .otherwise(lit(datafusion::scalar::ScalarValue::Null))
                .map_err(|e| QueryError::Execution {
                    message: format!("Failed to build CASE expression: {}", e),
                })?;
            let error_count = count(case_expr);
            let total_count = count(lit(1));
            Ok(error_count / total_count)
        }
        AggregateFn::Throughput => {
            // COUNT(*) / time_range_seconds
            // This requires knowing the time range, which we don't have here
            // For now, just return COUNT(*)
            // TODO: Divide by time range duration in seconds
            Ok(count(lit(1)))
        }
        AggregateFn::Heatmap(_expr) => {
            // Heatmap requires custom post-processing
            // For now, return a placeholder
            Err(QueryError::Execution {
                message: "Heatmap aggregate not yet implemented in Substrait path".into(),
            })
        }
        AggregateFn::Sample(_n) => {
            // Sample requires custom post-processing
            // For now, return a placeholder
            Err(QueryError::Execution {
                message: "Sample aggregate not yet implemented in Substrait path".into(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_schema_context_registers_all_tables() {
        let ctx = schema_context().expect("schema_context should succeed");

        // Core signal tables
        assert!(ctx.table_provider("spans").await.is_ok());
        assert!(ctx.table_provider("logs").await.is_ok());
        assert!(ctx.table_provider("metrics").await.is_ok());
        assert!(ctx.table_provider("datapoints").await.is_ok());
        assert!(ctx.table_provider("histogram_data_points").await.is_ok());

        // Profile tables
        assert!(ctx.table_provider("samples").await.is_ok());
        assert!(ctx.table_provider("profiles").await.is_ok());
        assert!(ctx.table_provider("profile_stacks").await.is_ok());
        assert!(ctx.table_provider("profile_frames").await.is_ok());
        assert!(ctx.table_provider("profile_mappings").await.is_ok());

        // Infrastructure tables
        assert!(ctx.table_provider("resources").await.is_ok());
        assert!(ctx.table_provider("scopes").await.is_ok());
        assert!(ctx.table_provider("span_links").await.is_ok());
    }

    #[test]
    fn test_signal_to_table_name() {
        assert_eq!(signal_to_table_name(Signal::Spans), "spans");
        assert_eq!(signal_to_table_name(Signal::Logs), "logs");
        assert_eq!(signal_to_table_name(Signal::Datapoints), "datapoints");
        assert_eq!(signal_to_table_name(Signal::Metrics), "metrics");
        assert_eq!(signal_to_table_name(Signal::Samples), "samples");
        assert_eq!(signal_to_table_name(Signal::Traces), "spans");
    }

    fn decode_seql_extension(bytes: &[u8]) -> crate::seql_ext::SeqlExtension {
        use datafusion_substrait::substrait::proto::Plan;
        let plan: Plan = prost::Message::decode(bytes).expect("plan bytes decode");
        let ext_any = plan
            .advanced_extensions
            .expect("no advanced_extensions")
            .enhancement
            .expect("no enhancement");
        prost::Message::decode(&ext_any.value[..]).expect("SeqlExtension decode")
    }

    #[tokio::test]
    async fn test_compile_simple_query() {
        let ctx = schema_context().expect("schema_context should succeed");
        let bytes = compile("spans last 1h", &ctx)
            .await
            .expect("compile should succeed");

        assert!(!bytes.is_empty(), "plan bytes should not be empty");

        let ext = decode_seql_extension(&bytes);
        assert_eq!(ext.signal, "spans");
        assert_eq!(ext.response_shape, "table");
        assert!(ext.auxiliary_aliases.is_empty());
    }

    #[tokio::test]
    async fn test_compile_with_filter() {
        let ctx = schema_context().expect("schema_context should succeed");
        let bytes = compile("spans last 1h | where status == 'Error'", &ctx)
            .await
            .expect("compile should succeed");

        assert!(!bytes.is_empty());
        let ext = decode_seql_extension(&bytes);
        assert_eq!(ext.signal, "spans");
    }

    #[tokio::test]
    async fn test_compile_with_aggregate() {
        let ctx = schema_context().expect("schema_context should succeed");
        let bytes = compile("spans last 1h | group by {} { count() as n }", &ctx)
            .await
            .expect("compile should succeed");

        assert!(!bytes.is_empty());
        let ext = decode_seql_extension(&bytes);
        assert_eq!(ext.response_shape, "scalar");
    }

    #[test]
    fn test_infer_shape_table() {
        use crate::ast::{QueryMode, Scan, Signal, Stage, TimeRange};
        use crate::schema::infer_shape;
        let ast = crate::ast::QueryAst {
            bindings: vec![],
            scan: Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![],
            mode: QueryMode::Snapshot,
        };
        assert_eq!(infer_shape(&ast), crate::schema::ResponseShape::Table);
        // Patterns stage is a no-op in the compiler but schema still infers PatternGroups
        let _ = Stage::Patterns; // ensure variant still exists
    }

    #[test]
    fn test_infer_shape_patterns() {
        use crate::ast::{PatternsStage, QueryMode, Scan, Signal, Stage, TimeRange};
        use crate::schema::infer_shape;
        let ast = crate::ast::QueryAst {
            bindings: vec![],
            scan: Scan {
                signal: Signal::Logs,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Patterns(PatternsStage { field: None })],
            mode: QueryMode::Snapshot,
        };
        assert_eq!(
            infer_shape(&ast),
            crate::schema::ResponseShape::PatternGroups
        );
    }

    #[tokio::test]
    async fn test_compile_with_time_range_stage() {
        let ctx = schema_context().expect("schema_context should succeed");
        let bytes = compile("spans last 1h | last 30m", &ctx)
            .await
            .expect("compile should succeed");

        assert!(!bytes.is_empty());
        let ext = decode_seql_extension(&bytes);
        assert_eq!(ext.signal, "spans");
    }

    #[test]
    fn test_time_range_stage_round_trip() {
        use crate::ast::TimeRangeStage;

        let stage = Stage::TimeRange(TimeRangeStage {
            duration_ns: 1_800_000_000_000, // 30 minutes
        });

        let json = serde_json::to_string(&stage).unwrap();
        let back: Stage = serde_json::from_str(&json).unwrap();
        assert_eq!(stage, back);
    }

    #[tokio::test]
    async fn test_compile_filter_all_compare_ops() {
        use crate::ast::{AttrScope, CompareExpr, Expr as AstExpr, FieldRef, Predicate};

        let ctx = schema_context().expect("schema_context should succeed");

        // Test each comparison operator
        let ops = [
            (CompareOp::Eq, "=="),
            (CompareOp::Neq, "!="),
            (CompareOp::Gt, ">"),
            (CompareOp::Gte, ">="),
            (CompareOp::Lt, "<"),
            (CompareOp::Lte, "<="),
        ];

        for (op, op_str) in ops {
            // Create a filter stage with this operator
            let filter = FilterStage {
                predicate: Predicate::Compare(CompareExpr {
                    left: AstExpr::Field(FieldRef {
                        scope: AttrScope::Signal,
                        name: "duration_ns".to_string(),
                    }),
                    op,
                    right: AstExpr::Literal(Literal::Int(1000)),
                }),
            };

            let ast = QueryAst {
                bindings: vec![],
                scan: crate::ast::Scan {
                    signal: Signal::Spans,
                    time_range: TimeRange::SlidingWindow {
                        start_ns: 3_600_000_000_000,
                    },
                },
                stages: vec![Stage::Filter(filter)],
                mode: QueryMode::Snapshot,
            };

            let result = ast_to_logical_plan(&ast, &ctx).await;
            assert!(
                result.is_ok(),
                "CompareOp::{:?} ({}) should compile successfully",
                op,
                op_str
            );
        }
    }

    #[tokio::test]
    async fn test_compile_aggregate_all_functions() {
        use crate::ast::{Aggregation, AttrScope, Expr as AstExpr, FieldRef};

        let ctx = schema_context().expect("schema_context should succeed");

        // Test each aggregate function that takes an expression
        let duration_expr = AstExpr::Field(FieldRef {
            scope: AttrScope::Signal,
            name: "duration_ns".to_string(),
        });

        let agg_fns = vec![
            AggregateFn::Count,
            AggregateFn::Sum(duration_expr.clone()),
            AggregateFn::Avg(duration_expr.clone()),
            AggregateFn::Min(duration_expr.clone()),
            AggregateFn::Max(duration_expr.clone()),
            AggregateFn::P50(duration_expr.clone()),
            AggregateFn::P95(duration_expr.clone()),
            AggregateFn::P99(duration_expr.clone()),
            AggregateFn::Distinct(duration_expr.clone()),
            AggregateFn::First(duration_expr.clone()),
            AggregateFn::Last(duration_expr.clone()),
            AggregateFn::ErrorRate,
            AggregateFn::Throughput,
        ];

        for agg_fn in agg_fns {
            let agg_stage = AggregateStage {
                group_by: vec![],
                aggregations: vec![Aggregation {
                    function: agg_fn.clone(),
                    alias: "result".to_string(),
                    filter: None,
                }],
            };

            let ast = QueryAst {
                bindings: vec![],
                scan: crate::ast::Scan {
                    signal: Signal::Spans,
                    time_range: TimeRange::SlidingWindow {
                        start_ns: 3_600_000_000_000,
                    },
                },
                stages: vec![Stage::Aggregate(agg_stage)],
                mode: QueryMode::Snapshot,
            };

            let result = ast_to_logical_plan(&ast, &ctx).await;

            // Heatmap and Sample are not yet implemented
            match &agg_fn {
                AggregateFn::Heatmap(_) | AggregateFn::Sample(_) => {
                    assert!(
                        result.is_err(),
                        "{:?} should return error (not yet implemented)",
                        agg_fn
                    );
                }
                _ => {
                    assert!(
                        result.is_ok(),
                        "{:?} should compile successfully: {:?}",
                        agg_fn,
                        result.err()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_compile_error_invalid_field_reference() {
        use crate::ast::{AttrScope, FieldRef};

        let ctx = schema_context().expect("schema_context should succeed");

        // Create a project stage with a field that doesn't exist in the schema
        // Note: DataFusion may not validate field names at plan-building time,
        // so this might succeed at compile time and fail at execution time
        let project = ProjectStage {
            fields: vec![crate::ast::ProjectField {
                field: FieldRef {
                    scope: AttrScope::Signal,
                    name: "nonexistent_field_xyz".to_string(),
                },
                alias: None,
            }],
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Project(project)],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast, &ctx).await;

        // DataFusion validates column names at planning time and rejects unknown fields,
        // so an invalid field reference must produce an error at compile time.
        assert!(
            result.is_err(),
            "Logical plan with unknown field should fail at planning time"
        );
    }

    #[tokio::test]
    async fn test_compile_time_range_expressions() {
        let ctx = schema_context().expect("schema_context should succeed");

        // Test absolute time range
        let ast_absolute = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::Absolute {
                    start_ns: 1_000_000_000_000,
                    end_ns: 2_000_000_000_000,
                },
            },
            stages: vec![],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast_absolute, &ctx).await;
        assert!(
            result.is_ok(),
            "Absolute time range should compile: {:?}",
            result.err()
        );

        // Test sliding window time range
        let ast_sliding = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000, // 1 hour
                },
            },
            stages: vec![],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast_sliding, &ctx).await;
        assert!(
            result.is_ok(),
            "Sliding window time range should compile: {:?}",
            result.err()
        );

        // Test time range stage in pipeline
        let ast_stage = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Logs,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::TimeRange(crate::ast::TimeRangeStage {
                duration_ns: 1_800_000_000_000, // 30 minutes
            })],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast_stage, &ctx).await;
        assert!(
            result.is_ok(),
            "Time range stage should compile: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_compile_nested_expressions() {
        use crate::ast::{AttrScope, CompareExpr, Expr as AstExpr, FieldRef, Predicate};

        let ctx = schema_context().expect("schema_context should succeed");

        // Create a complex nested predicate:
        // (status == 'Error' OR status == 'Fatal') AND duration_ns > 1000
        let status_field = FieldRef {
            scope: AttrScope::Signal,
            name: "status".to_string(),
        };

        let duration_field = FieldRef {
            scope: AttrScope::Signal,
            name: "duration_ns".to_string(),
        };

        let status_error = Predicate::Compare(CompareExpr {
            left: AstExpr::Field(status_field.clone()),
            op: CompareOp::Eq,
            right: AstExpr::Literal(Literal::String("Error".to_string())),
        });

        let status_fatal = Predicate::Compare(CompareExpr {
            left: AstExpr::Field(status_field),
            op: CompareOp::Eq,
            right: AstExpr::Literal(Literal::String("Fatal".to_string())),
        });

        let duration_gt = Predicate::Compare(CompareExpr {
            left: AstExpr::Field(duration_field),
            op: CompareOp::Gt,
            right: AstExpr::Literal(Literal::Int(1000)),
        });

        let status_or = Predicate::Or(Box::new(status_error), Box::new(status_fatal));
        let combined = Predicate::And(Box::new(status_or), Box::new(duration_gt));

        let filter = FilterStage {
            predicate: combined,
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Filter(filter)],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast, &ctx).await;
        assert!(
            result.is_ok(),
            "Nested AND/OR expressions should compile: {:?}",
            result.err()
        );

        // Test NOT predicate
        let not_predicate = Predicate::Not(Box::new(Predicate::Compare(CompareExpr {
            left: AstExpr::Field(FieldRef {
                scope: AttrScope::Signal,
                name: "status".to_string(),
            }),
            op: CompareOp::Eq,
            right: AstExpr::Literal(Literal::String("Ok".to_string())),
        })));

        let filter_not = FilterStage {
            predicate: not_predicate,
        };

        let ast_not = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Filter(filter_not)],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast_not, &ctx).await;
        assert!(
            result.is_ok(),
            "NOT predicate should compile: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_compile_sort_stage() {
        use crate::ast::{AttrScope, Expr as AstExpr, FieldRef, SortExpr};

        let ctx = schema_context().expect("schema_context should succeed");

        let sort = SortStage {
            exprs: vec![SortExpr {
                expr: AstExpr::Field(FieldRef {
                    scope: AttrScope::Signal,
                    name: "start_time_unix_nano".to_string(),
                }),
                ascending: false,
            }],
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Sort(sort)],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast, &ctx).await;
        assert!(
            result.is_ok(),
            "sort stage should compile: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_compile_project_stage() {
        use crate::ast::{AttrScope, FieldRef, ProjectField};

        let ctx = schema_context().expect("schema_context should succeed");

        let project = ProjectStage {
            fields: vec![
                ProjectField {
                    field: FieldRef {
                        scope: AttrScope::Signal,
                        name: "trace_id".to_string(),
                    },
                    alias: None,
                },
                ProjectField {
                    field: FieldRef {
                        scope: AttrScope::Signal,
                        name: "span_id".to_string(),
                    },
                    alias: None,
                },
            ],
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Project(project)],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast, &ctx).await;
        assert!(
            result.is_ok(),
            "project stage should compile: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_compile_unique_stage() {
        use crate::ast::{AttrScope, FieldRef};

        let ctx = schema_context().expect("schema_context should succeed");

        let unique = UniqueStage {
            field: FieldRef {
                scope: AttrScope::Signal,
                name: "trace_id".to_string(),
            },
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Unique(unique)],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast, &ctx).await;
        assert!(
            result.is_ok(),
            "unique stage should compile: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_compile_string_predicates() {
        use crate::ast::{AttrScope, FieldRef};

        let ctx = schema_context().expect("schema_context should succeed");

        let span_name_field = FieldRef {
            scope: AttrScope::Signal,
            name: "name".to_string(),
        };

        let predicates = [
            Predicate::Contains {
                field: span_name_field.clone(),
                value: "api".to_string(),
            },
            Predicate::StartsWith {
                field: span_name_field.clone(),
                value: "GET".to_string(),
            },
            Predicate::EndsWith {
                field: span_name_field.clone(),
                value: "health".to_string(),
            },
            Predicate::Matches {
                field: span_name_field.clone(),
                pattern: "^GET.*".to_string(),
            },
        ];

        for predicate in predicates {
            let filter = FilterStage { predicate };
            let ast = QueryAst {
                bindings: vec![],
                scan: crate::ast::Scan {
                    signal: Signal::Spans,
                    time_range: TimeRange::SlidingWindow {
                        start_ns: 3_600_000_000_000,
                    },
                },
                stages: vec![Stage::Filter(filter)],
                mode: QueryMode::Snapshot,
            };
            let result = ast_to_logical_plan(&ast, &ctx).await;
            assert!(
                result.is_ok(),
                "string predicate should compile: {:?}",
                result.err()
            );
        }
    }

    // ── ts() function tests ───────────────────────────────────────────────────

    #[test]
    fn test_time_column_for_signal_timed_signals() {
        assert_eq!(
            time_column_for_signal(Signal::Spans),
            Some("start_time_unix_nano")
        );
        assert_eq!(
            time_column_for_signal(Signal::Traces),
            Some("start_time_unix_nano")
        );
        assert_eq!(time_column_for_signal(Signal::Logs), Some("time_unix_nano"));
        assert_eq!(
            time_column_for_signal(Signal::Datapoints),
            Some("time_unix_nano")
        );
        assert_eq!(
            time_column_for_signal(Signal::Samples),
            Some("time_unix_nano")
        );
    }

    #[test]
    fn test_time_column_for_signal_timeless_signals() {
        assert_eq!(time_column_for_signal(Signal::Metrics), None);
        assert_eq!(time_column_for_signal(Signal::Resources), None);
        assert_eq!(time_column_for_signal(Signal::Scopes), None);
        assert_eq!(time_column_for_signal(Signal::Stacks), None);
    }

    #[tokio::test]
    async fn test_ts_fn_resolves_to_time_column() {
        use crate::parser;

        let ctx = schema_context().expect("schema_context should succeed");

        // ts() inside group by on spans should resolve to start_time_unix_nano
        let seql = "spans last 1h | group by { ts() bin 5m as bucket } { count() as n }";
        let plan_bytes = compile(seql, &ctx)
            .await
            .expect("should compile ts() query");
        assert!(!plan_bytes.is_empty());
        let ext = decode_seql_extension(&plan_bytes);
        assert_eq!(ext.signal, "spans");

        // ts() inside group by on logs should resolve to time_unix_nano
        let seql_logs = "logs last 1h | group by { ts() bin 5m as bucket } { count() as n }";
        let plan_logs_bytes = compile(seql_logs, &ctx)
            .await
            .expect("should compile ts() for logs");
        let ext_logs = decode_seql_extension(&plan_logs_bytes);
        assert_eq!(ext_logs.signal, "logs");

        // ts() on a timeless signal (metrics) should error at compile time
        let ast = parser::parse("metrics last 1h").unwrap();
        let expr = crate::ast::Expr::FunctionCall {
            function: ScalarFn::Timestamp,
            args: vec![],
        };
        let schema = Arc::new(datafusion::common::DFSchema::empty());
        let result = ast_expr_to_df_expr(&expr, &schema, Signal::Metrics, &ctx);
        assert!(result.is_err(), "ts() on Metrics should fail");
    }

    #[tokio::test]
    async fn test_compile_aggregate_with_filter() {
        use crate::ast::{Aggregation, AttrScope, CompareExpr, CompareOp, FieldRef, Predicate};
        use crate::ast::{Expr as AstExpr, Literal};

        let ctx = schema_context().expect("schema_context should succeed");

        let status_pred = Predicate::Compare(CompareExpr {
            left: AstExpr::Field(FieldRef {
                scope: AttrScope::Signal,
                name: "status".to_string(),
            }),
            op: CompareOp::Eq,
            right: AstExpr::Literal(Literal::Int(2)),
        });

        let agg_stage = AggregateStage {
            group_by: vec![],
            aggregations: vec![Aggregation {
                function: AggregateFn::Count,
                alias: "errors".to_string(),
                filter: Some(status_pred),
            }],
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: crate::ast::Scan {
                signal: Signal::Spans,
                time_range: TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                },
            },
            stages: vec![Stage::Aggregate(agg_stage)],
            mode: QueryMode::Snapshot,
        };

        let result = ast_to_logical_plan(&ast, &ctx).await;
        assert!(
            result.is_ok(),
            "aggregate with filter should compile successfully: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_compile_aggregate_filter_from_seql() {
        let ctx = schema_context().expect("schema_context should succeed");

        let ast = crate::parser::parse(
            "spans last 1h | group by {} { count() where status == 2 as errors, count() as total }",
        )
        .expect("parse should succeed");

        let result = ast_to_logical_plan(&ast, &ctx).await;
        assert!(
            result.is_ok(),
            "SeQL aggregate with filter should compile: {:?}",
            result.err()
        );
    }
}
