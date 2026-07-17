//! SeQL to Substrait compiler
//!
//! Converts SeQL text or AST into Substrait binary plans for execution.
//! The output is a multi-root Substrait `Plan` with `SeqlExtension` metadata
//! embedded in `advanced_extensions.enhancement`.

use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit as ArrowTimeUnit;
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
use seql_ast::ast::{
    AggregateFn, AggregateStage, ArithOp, BinSpec, CompareOp, ComputeStage, Expr as AstExpr,
    FieldRef, FilterStage, GroupExpr, LimitStage, Literal, MergeStage, Predicate, ProjectStage,
    QueryAst, QueryMode, ScalarFn, Scan, Signal, SortStage, Stage, TimeRange, UniqueStage,
    WindowFn, WindowStage,
};
use seql_ast::correlation::{merge_join_key, navigate_join_key};
use seql_ast::schema::infer_shape;
use sequins_traits::QueryError;

/// Return type for `compile_merge_aux` — list of (alias, plan, signal) tuples boxed for recursion.
type MergeAuxFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Vec<(String, LogicalPlan, Signal)>, QueryError>>
            + Send
            + 'a,
    >,
>;

/// Outcome of [`apply_common_stage`] for a single pipeline stage.
enum StageOutcome {
    /// Builder (and possibly signal) were updated; caller should continue.
    Updated(LogicalPlanBuilder, Signal),
    /// Stage is a `Stage::Merge` — the caller must handle it (builder returned unchanged).
    IsMerge(LogicalPlanBuilder, Signal),
}
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
    compile_with_range(seql, None, ctx).await
}

/// Like [`compile`], but with an optional structured time range supplied
/// out-of-band. When present it **overrides** any inline scope, so a saved
/// query template (`spans | group by { ts() bin 10% } { count() }`) can be run
/// against the dashboard's selected range. See [`compile_ast_with_range`].
pub async fn compile_with_range(
    seql: &str,
    time_range: Option<TimeRange>,
    ctx: &SessionContext,
) -> Result<Vec<u8>, QueryError> {
    let ast = seql_parser::parse(seql).map_err(|e| QueryError::InvalidAst {
        message: format!("Parse error at offset {}: {}", e.offset, e.message),
    })?;
    compile_ast_with_range(ast, time_range, ctx).await
}

/// Compile a parsed `QueryAst` into a multi-root Substrait plan with `SeqlExtension` metadata.
///
/// Unlike `compile()`, this accepts a pre-parsed AST, allowing the caller to
/// modify the AST before compilation (e.g., set `mode = QueryMode::Live`).
pub async fn compile_ast(ast: QueryAst, ctx: &SessionContext) -> Result<Vec<u8>, QueryError> {
    compile_ast_with_range(ast, None, ctx).await
}

/// Compile an AST with an optional structured time range that **overrides** the
/// scan's inline scope (if any). This is how the range becomes a separable input:
/// a scope-less template carries `scan.time_range == None` and gets its range
/// here at execution; an injected range also replaces an inline one so the
/// dashboard's selected range always wins.
pub async fn compile_ast_with_range(
    mut ast: QueryAst,
    time_range: Option<TimeRange>,
    ctx: &SessionContext,
) -> Result<Vec<u8>, QueryError> {
    if time_range.is_some() {
        ast.scan.time_range = time_range;
    }
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
    let time_range_proto = ast.scan.time_range.as_ref().map(time_range_to_proto);
    let mode_val = match ast.mode {
        QueryMode::Snapshot => crate::seql_ext::QueryMode::Snapshot as i32,
        QueryMode::Live => crate::seql_ext::QueryMode::Live as i32,
    };
    let seql_ext = crate::seql_ext::SeqlExtension {
        response_shape: response_shape.as_str().to_string(),
        signal: signal_to_name(ast.scan.signal).to_string(),
        time_range: time_range_proto,
        mode: mode_val,
        cursor: None,
        auxiliary_aliases,
        auxiliary_signals,
        auxiliary_plan_bytes,
        // Scope defaults to `All`; the distributed coordinator overrides it
        // per-leg via `set_plan_scope`.
        scope: crate::seql_ext::QueryScope::All as i32,
        aggregated: ast_is_aggregated(&ast),
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

/// Whether a query aggregates or de-duplicates rows (an `Aggregate` or `Unique`
/// stage in the main pipeline, any binding, **or any `merge` sub-pipeline**), and
/// so is not row-distributive.
///
/// The merge case matters for distribution: a query like
/// `metrics <- datapoints | group by … as datapoints` carries its aggregation
/// inside the merge's inner pipeline, not the main one. Such a query cannot be
/// correctly stream-merged across nodes (each node would produce its own partial
/// aggregate) — it must go through the coordinator's gather-and-re-aggregate
/// path, which requires this flag to be set.
fn ast_is_aggregated(ast: &QueryAst) -> bool {
    fn stage_aggregates(stage: &Stage) -> bool {
        match stage {
            Stage::Aggregate(_) | Stage::Unique(_) => true,
            // Recurse into a merge's inner sub-pipeline (which may itself merge).
            Stage::Merge(merge) => has_agg(&merge.stages),
            _ => false,
        }
    }
    fn has_agg(stages: &[Stage]) -> bool {
        stages.iter().any(stage_aggregates)
    }
    has_agg(&ast.stages) || ast.bindings.iter().any(|b| has_agg(&b.stages))
}

/// Override the query scope stamped in a compiled plan's `SeqlExtension`.
///
/// The distributed query coordinator uses this to derive a `HotOnly` plan for
/// peer fan-out and a `ColdOnly` plan for its own shared-cold read from a single
/// client-supplied plan. Auxiliary plans need no change: they execute against
/// the same (scoped) session context as the primary plan.
pub fn set_plan_scope(
    plan_bytes: &[u8],
    scope: crate::seql_ext::QueryScope,
) -> Result<Vec<u8>, QueryError> {
    rewrite_seql_ext(plan_bytes, |ext| ext.scope = scope as i32)
}

/// Override the query mode (live vs snapshot) stamped in a compiled plan.
///
/// The coordinator forces its cold-tier leg to snapshot mode even for a live
/// query — cold data is historical and never streams, and a live cold leg would
/// otherwise re-emit the local hot broadcast (duplicating the hot leg).
pub fn set_plan_mode(plan_bytes: &[u8], live: bool) -> Result<Vec<u8>, QueryError> {
    let mode = if live {
        crate::seql_ext::QueryMode::Live
    } else {
        crate::seql_ext::QueryMode::Snapshot
    };
    rewrite_seql_ext(plan_bytes, |ext| ext.mode = mode as i32)
}

/// Decode a plan, mutate its `SeqlExtension` in place via `f`, and re-encode.
fn rewrite_seql_ext(
    plan_bytes: &[u8],
    f: impl FnOnce(&mut crate::seql_ext::SeqlExtension),
) -> Result<Vec<u8>, QueryError> {
    use datafusion_substrait::substrait::proto::Plan;

    let mut plan: Plan = Message::decode(plan_bytes).map_err(|e| QueryError::Execution {
        message: format!("rewrite_seql_ext: failed to decode plan: {e}"),
    })?;
    let ext_any = plan
        .advanced_extensions
        .as_mut()
        .and_then(|adv| adv.enhancement.as_mut())
        .ok_or_else(|| QueryError::Execution {
            message: "rewrite_seql_ext: plan missing SeqlExtension enhancement".to_string(),
        })?;
    let mut ext = crate::seql_ext::SeqlExtension::decode(&ext_any.value[..]).map_err(|e| {
        QueryError::Execution {
            message: format!("rewrite_seql_ext: failed to decode SeqlExtension: {e}"),
        }
    })?;
    f(&mut ext);
    ext_any.value = ext.encode_to_vec().into();
    Ok(plan.encode_to_vec())
}

/// Routing metadata read from a compiled plan's `SeqlExtension` — enough for the
/// distributed query coordinator to decide fan-out strategy without a full
/// Substrait decode.
#[derive(Debug, Clone)]
pub struct PlanMeta {
    /// Which tiers the plan scans (`All` for a client query; a coordinator sets
    /// `HotOnly`/`ColdOnly` on the legs it fans out).
    pub scope: crate::seql_ext::QueryScope,
    /// True for a live (streaming) query, false for a one-shot snapshot.
    pub live: bool,
    /// True when the query aggregates/de-duplicates (needs coordinator recompute).
    pub aggregated: bool,
    /// Primary signal name (e.g. `"logs"`).
    pub signal: String,
}

/// Decode a compiled plan's [`PlanMeta`].
pub fn decode_plan_meta(plan_bytes: &[u8]) -> Result<PlanMeta, QueryError> {
    use datafusion_substrait::substrait::proto::Plan;

    let plan: Plan = Message::decode(plan_bytes).map_err(|e| QueryError::Execution {
        message: format!("decode_plan_meta: failed to decode plan: {e}"),
    })?;
    let ext_any = plan
        .advanced_extensions
        .as_ref()
        .and_then(|adv| adv.enhancement.as_ref())
        .ok_or_else(|| QueryError::Execution {
            message: "decode_plan_meta: plan missing SeqlExtension enhancement".to_string(),
        })?;
    let ext = crate::seql_ext::SeqlExtension::decode(&ext_any.value[..]).map_err(|e| {
        QueryError::Execution {
            message: format!("decode_plan_meta: failed to decode SeqlExtension: {e}"),
        }
    })?;
    Ok(PlanMeta {
        scope: crate::seql_ext::QueryScope::try_from(ext.scope)
            .unwrap_or(crate::seql_ext::QueryScope::All),
        live: ext.mode == crate::seql_ext::QueryMode::Live as i32,
        aggregated: ext.aggregated,
        signal: ext.signal,
    })
}

/// Decode a compiled plan's full [`SeqlExtension`](crate::seql_ext::SeqlExtension)
/// — primary signal, time range, auxiliary plans, scope and mode.
///
/// The distributed query coordinator needs the signal + time range to build a
/// raw-scan plan (via [`raw_scan_plan`]) for gathering rows to re-aggregate, and
/// the auxiliary-plan list to decide whether an aggregating query is a simple
/// primary aggregation (distributable) or a merge/navigate (kept node-local).
pub fn decode_plan_ext(plan_bytes: &[u8]) -> Result<crate::seql_ext::SeqlExtension, QueryError> {
    use datafusion_substrait::substrait::proto::Plan;

    let plan: Plan = Message::decode(plan_bytes).map_err(|e| QueryError::Execution {
        message: format!("decode_plan_ext: failed to decode plan: {e}"),
    })?;
    let ext_any = plan
        .advanced_extensions
        .as_ref()
        .and_then(|adv| adv.enhancement.as_ref())
        .ok_or_else(|| QueryError::Execution {
            message: "decode_plan_ext: plan missing SeqlExtension enhancement".to_string(),
        })?;
    crate::seql_ext::SeqlExtension::decode(&ext_any.value[..]).map_err(|e| QueryError::Execution {
        message: format!("decode_plan_ext: failed to decode SeqlExtension: {e}"),
    })
}

/// Convert a primary-signal table name (as stored in `SeqlExtension.signal`)
/// back to an AST [`Signal`]. Inverse of [`signal_to_name`].
fn signal_from_name(name: &str) -> Option<Signal> {
    Some(match name {
        "spans" => Signal::Spans,
        "logs" => Signal::Logs,
        "datapoints" => Signal::Datapoints,
        "histograms" => Signal::Histograms,
        "metrics" => Signal::Metrics,
        "samples" => Signal::Samples,
        "traces" => Signal::Traces,
        "profiles" => Signal::Profiles,
        "stacks" => Signal::Stacks,
        "frames" => Signal::Frames,
        "mappings" => Signal::Mappings,
        "resources" => Signal::Resources,
        "scopes" => Signal::Scopes,
        "span_links" => Signal::SpanLinks,
        _ => return None,
    })
}

/// Map a primary/auxiliary signal name (as stored in `SeqlExtension.signal` /
/// `.auxiliary_signals`) to the registration table name that a plan's `read`
/// relation references — e.g. `"histograms"` → `"histogram_data_points"`,
/// `"traces"` → `"spans"`. The distributed coordinator registers gathered rows
/// under this name so the re-run plan resolves them. Returns `None` for an
/// unknown signal name.
pub fn signal_table_name(signal_name: &str) -> Option<&'static str> {
    signal_from_name(signal_name).map(signal_to_table_name)
}

/// Convert a `SeqlExtension` protobuf `TimeRange` back to an AST [`TimeRange`].
/// Inverse of [`time_range_to_proto`].
fn proto_to_time_range(tr: &crate::seql_ext::TimeRange) -> Option<TimeRange> {
    match tr.range.as_ref()? {
        crate::seql_ext::time_range::Range::SlidingWindowNs(ns) => {
            Some(TimeRange::SlidingWindow { start_ns: *ns })
        }
        crate::seql_ext::time_range::Range::Absolute(a) => Some(TimeRange::Absolute {
            start_ns: a.start_ns,
            end_ns: a.end_ns,
        }),
    }
}

/// Compile a **raw-scan** plan — `<signal> <time_range>` with no pipeline stages
/// and `aggregated = false` — for the distributed coordinator to gather the raw
/// rows of `signal` from every node so it can re-run an aggregation over the
/// union.
///
/// The coordinator stamps the returned plan with `HotOnly`/`ColdOnly` scope (via
/// [`set_plan_scope`]) per leg. `live` selects streaming mode so peers emit
/// `Append` deltas the coordinator can use as change signals. When `time_range`
/// is absent a one-hour sliding window is used as a safety bound.
pub async fn raw_scan_plan(
    signal_name: &str,
    time_range: Option<&crate::seql_ext::TimeRange>,
    live: bool,
) -> Result<Vec<u8>, QueryError> {
    let signal = signal_from_name(signal_name).ok_or_else(|| QueryError::InvalidAst {
        message: format!("raw_scan_plan: unknown signal '{signal_name}'"),
    })?;
    let time_range = time_range.and_then(proto_to_time_range).unwrap_or(
        // Default: last hour — a safety bound so a coordinator never gathers the
        // entire hot+cold history when the plan carries no explicit range.
        TimeRange::SlidingWindow {
            start_ns: 3_600_000_000_000,
        },
    );
    let ast = QueryAst {
        bindings: vec![],
        scan: Scan {
            signal,
            time_range: Some(time_range),
        },
        stages: vec![],
        mode: if live {
            QueryMode::Live
        } else {
            QueryMode::Snapshot
        },
    };
    let ctx = schema_context()?;
    compile_ast(ast, &ctx).await
}

/// Apply one pipeline stage that is common to both the primary plan and merge-aux paths.
///
/// Returns [`StageOutcome::Updated`] with the new builder and current signal for all handled
/// stages, or [`StageOutcome::IsMerge`] when the stage is `Stage::Merge` (caller must handle).
///
/// `nav_context` is included in Navigate-related error messages (e.g. `"(inside merge)"`).
async fn apply_common_stage(
    mut builder: LogicalPlanBuilder,
    stage: &Stage,
    mut current_signal: Signal,
    ctx: &SessionContext,
    nav_context: &str,
    window_ns: Option<u64>,
) -> Result<StageOutcome, QueryError> {
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
            builder = apply_aggregate(builder, aggregate, current_signal, ctx, window_ns)?;
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
            let join_key = navigate_join_key(&current_signal, &nav.target).ok_or_else(|| {
                QueryError::Execution {
                    message: format!(
                        "No navigate path from {:?} to {:?}{}",
                        current_signal,
                        nav.target,
                        if nav_context.is_empty() {
                            String::new()
                        } else {
                            format!(" {}", nav_context)
                        }
                    ),
                }
            })?;

            let target_table = signal_to_table_name(nav.target);
            let target_provider =
                ctx.table_provider(target_table)
                    .await
                    .map_err(|e| QueryError::Execution {
                        message: format!(
                            "Failed to get table for navigate target {}{}: {}",
                            target_table,
                            if nav_context.is_empty() {
                                String::new()
                            } else {
                                format!(" {}", nav_context)
                            },
                            e
                        ),
                    })?;
            let target_source = provider_as_source(target_provider);
            let target_builder = LogicalPlanBuilder::scan(target_table, target_source, None)
                .map_err(|e| QueryError::Execution {
                    message: format!("Failed to scan navigate target {}: {}", target_table, e),
                })?;

            let source_plan = builder.build().map_err(|e| QueryError::Execution {
                message: format!("Failed to build source plan for navigate: {}", e),
            })?;

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
        Stage::Merge(_) => {
            return Ok(StageOutcome::IsMerge(builder, current_signal));
        }
        Stage::Patterns(_) => {
            // Patterns stage is a no-op
        }
        Stage::Window(window) => {
            builder = apply_window(builder, window, current_signal, ctx)?;
        }
    }

    Ok(StageOutcome::Updated(builder, current_signal))
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
    builder = apply_time_range_filter(builder, current_signal, ast.scan.time_range.as_ref())?;

    // The concrete window this scan covers — threaded into aggregation so
    // time-relative features (throughput rate, `ts() bin N%`) scale to it.
    // `None` for a scope-less template with no injected range.
    let window_ns = effective_window_ns(ast.scan.time_range.as_ref());

    // Apply each stage in order
    for stage in &ast.stages {
        match apply_common_stage(builder, stage, current_signal, ctx, "", window_ns).await? {
            StageOutcome::Updated(b, s) => {
                builder = b;
                current_signal = s;
            }
            StageOutcome::IsMerge(b, s) => {
                builder = b;
                current_signal = s;
                // Compile Merge as an auxiliary plan root; primary plan is unchanged.
                let Stage::Merge(merge) = stage else {
                    unreachable!()
                };
                let primary_plan = builder.build().map_err(|e| QueryError::Execution {
                    message: format!("Failed to build primary plan for merge: {}", e),
                })?;
                let aux = compile_merge_aux(
                    merge,
                    &primary_plan,
                    current_signal,
                    ast.scan.time_range.as_ref(),
                    ctx,
                )
                .await?;
                auxiliary_plans.extend(aux);
                // Restore builder from primary plan (primary is unchanged by Merge)
                builder = LogicalPlanBuilder::from(primary_plan);
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
/// The auxiliary plan scans the merge target, applies the outer query's time range
/// filter so only the relevant window is fetched (avoiding full-table scans on cold
/// tier Vortex files), and then filters to rows whose `join_key` appears in the parent
/// plan's result. Inner stages (filter, sort, limit, nested merges) are applied next.
fn compile_merge_aux<'a>(
    merge: &'a MergeStage,
    parent_plan: &'a LogicalPlan,
    parent_signal: Signal,
    time_range: Option<&'a TimeRange>,
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

        // Apply the outer query's time range to the auxiliary table so we only fetch
        // data within the requested window. This is critical for cold-tier performance:
        // Vortex zone maps can skip files outside the range, avoiding full-table scans.
        aux_builder = apply_time_range_filter(aux_builder, merge.target, time_range)?;

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

        let merge_window_ns = effective_window_ns(time_range);
        for stage in &merge.stages {
            match apply_common_stage(
                aux_builder,
                stage,
                current_signal,
                ctx,
                "(inside merge)",
                merge_window_ns,
            )
            .await?
            {
                StageOutcome::Updated(b, s) => {
                    aux_builder = b;
                    current_signal = s;
                }
                StageOutcome::IsMerge(b, s) => {
                    aux_builder = b;
                    current_signal = s;
                    // Nested merge: build aux plans with the current aux plan as parent
                    let Stage::Merge(inner_merge) = stage else {
                        unreachable!()
                    };
                    let current_plan = aux_builder.build().map_err(|e| QueryError::Execution {
                        message: format!("Failed to build plan for nested merge: {}", e),
                    })?;
                    let inner_plans = compile_merge_aux(
                        inner_merge,
                        &current_plan,
                        current_signal,
                        time_range,
                        ctx,
                    )
                    .await?;
                    nested_aux_plans.extend(inner_plans);
                    // Restore aux builder from built plan (unchanged by nested merge)
                    aux_builder = LogicalPlanBuilder::from(current_plan);
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
    register_empty_table(
        &ctx,
        "spans",
        sequins_arrow_schema::arrow_schema::span_schema(),
    )?;
    register_empty_table(
        &ctx,
        "logs",
        sequins_arrow_schema::arrow_schema::log_schema(),
    )?;
    register_empty_table(
        &ctx,
        "metrics",
        sequins_arrow_schema::arrow_schema::metric_schema(),
    )?;
    register_empty_table(
        &ctx,
        "datapoints",
        sequins_arrow_schema::arrow_schema::series_data_point_schema(),
    )?;
    register_empty_table(
        &ctx,
        "histogram_data_points",
        sequins_arrow_schema::arrow_schema::histogram_series_data_point_schema(),
    )?;
    register_empty_table(
        &ctx,
        "samples",
        sequins_arrow_schema::arrow_schema::profile_samples_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profiles",
        sequins_arrow_schema::arrow_schema::profile_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profile_stacks",
        sequins_arrow_schema::arrow_schema::profile_stacks_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profile_frames",
        sequins_arrow_schema::arrow_schema::profile_frames_schema(),
    )?;
    register_empty_table(
        &ctx,
        "profile_mappings",
        sequins_arrow_schema::arrow_schema::profile_mappings_schema(),
    )?;
    register_empty_table(
        &ctx,
        "resources",
        sequins_arrow_schema::arrow_schema::resource_schema(),
    )?;
    register_empty_table(
        &ctx,
        "scopes",
        sequins_arrow_schema::arrow_schema::scope_schema(),
    )?;
    register_empty_table(
        &ctx,
        "span_links",
        sequins_arrow_schema::arrow_schema::span_links_schema(),
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

/// The concrete duration (in nanoseconds) that a scan's time scope covers.
///
/// Used to make time-relative query features scale to the selected range:
/// `throughput` divides by it, and `ts() bin N%` derives the bucket size from it.
/// For an absolute range it's `end - start`; for a sliding window it's the offset.
fn effective_window_ns(time_range: Option<&TimeRange>) -> Option<u64> {
    time_range.map(|tr| match tr {
        TimeRange::Absolute { start_ns, end_ns } => end_ns.saturating_sub(*start_ns),
        TimeRange::SlidingWindow { start_ns } => *start_ns,
    })
}

/// Resolve a [`BinSpec`] to a concrete bucket width in nanoseconds, given the
/// query's effective window. `Percent`/`Auto` scale with `window_ns` so a saved
/// time-series re-buckets sensibly as the selected range changes. Always ≥ 1ns.
///
/// `Percent`/`Auto` require a known window; on a scope-less template with no
/// range supplied they error rather than guess.
fn resolve_bin_ns(bin: &BinSpec, window_ns: Option<u64>) -> Result<u64, QueryError> {
    let need_window = || {
        window_ns.ok_or_else(|| QueryError::InvalidAst {
            message: "`ts() bin N%`/`bin auto` needs a time range: add a scope \
                      (e.g. `last 1h`) or supply one at execution"
                .to_string(),
        })
    };
    let ns = match bin {
        BinSpec::Fixed(ns) => *ns,
        // `bin 10%` → 10 buckets: width = window * 10/100. `max(0.0001)` guards a
        // degenerate/zero percentage from collapsing the divisor.
        BinSpec::Percent(pct) => ((need_window()? as f64) * (pct.max(0.0001) / 100.0)) as u64,
        BinSpec::Auto => nice_bin_ns(need_window()?),
    };
    Ok(ns.max(1))
}

/// Pick a "nice" bucket width (~100 buckets) for `bin auto`, snapped to a human
/// duration ladder so time axes land on round intervals. Mirrors the client's
/// `MetricsViewModel.binSeconds(for:)` ladder.
fn nice_bin_ns(window_ns: u64) -> u64 {
    const S: u64 = 1_000_000_000;
    const LADDER: &[u64] = &[
        S,        // 1s
        5 * S,    // 5s
        10 * S,   // 10s
        15 * S,   // 15s
        30 * S,   // 30s
        60 * S,   // 1m
        300 * S,  // 5m
        600 * S,  // 10m
        900 * S,  // 15m
        1800 * S, // 30m
        3600 * S, // 1h
        3 * 3600 * S,
        6 * 3600 * S,
        12 * 3600 * S,
        86_400 * S, // 1d
    ];
    let target = (window_ns / 100).max(1);
    LADDER
        .iter()
        .copied()
        .find(|&step| step >= target)
        .unwrap_or_else(|| *LADDER.last().unwrap())
}

fn apply_time_range_filter(
    builder: LogicalPlanBuilder,
    signal: Signal,
    time_range: Option<&TimeRange>,
) -> Result<LogicalPlanBuilder, QueryError> {
    let time_col = match time_column_for_signal(signal) {
        Some(col) => col,
        // Signals with no time column (e.g. resources/scopes) are never
        // time-filtered, so a missing range is fine.
        None => return Ok(builder),
    };

    // A time-scoped signal with no inline scope and no injected range is a
    // template that was executed without a range — reject it clearly.
    let time_range = time_range.ok_or_else(|| QueryError::InvalidAst {
        message: format!(
            "query on `{}` has no time range: add a scope (e.g. `last 1h`) \
             or supply one at execution",
            signal_to_name(signal)
        ),
    })?;

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
    stage: &seql_ast::ast::TimeRangeStage,
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
    window_ns: Option<u64>,
) -> Result<LogicalPlanBuilder, QueryError> {
    // Divisor (seconds) for rate aggregates like `throughput`: a per-bucket rate
    // when the group keys include a time bin, otherwise the whole query window.
    // `None` when neither is known (scope-less template, no injected range).
    let bucket_ns: Option<u64> = match aggregate.group_by.iter().find_map(|ge| ge.bin.as_ref()) {
        Some(bin) => Some(resolve_bin_ns(bin, window_ns)?),
        None => None,
    };
    let rate_divisor_secs: Option<f64> = bucket_ns
        .or(window_ns)
        .map(|ns| (ns as f64 / 1e9).max(f64::MIN_POSITIVE));

    // Build group expressions
    let group_exprs: Result<Vec<_>, _> = aggregate
        .group_by
        .iter()
        .map(|ge| group_expr_to_df_expr(ge, builder.schema(), signal, ctx, window_ns))
        .collect();

    // Build aggregation expressions
    let agg_exprs: Result<Vec<_>, _> = aggregate
        .aggregations
        .iter()
        .map(|agg| {
            let expr = aggregate_fn_to_df_expr(
                &agg.function,
                builder.schema(),
                signal,
                ctx,
                rate_divisor_secs,
            )?;
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

/// Apply a `window { … }` stage: compute window functions over the result,
/// ordered by the first temporal column (the `ts()` bucket for a time series)
/// or the first column otherwise. Each item appends one aliased column.
pub fn apply_window(
    builder: LogicalPlanBuilder,
    window: &WindowStage,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<LogicalPlanBuilder, QueryError> {
    let schema = builder.schema();
    // Order by the first Timestamp column (the time bucket), else the first column.
    let order_field = schema
        .fields()
        .iter()
        .find(|f| matches!(f.data_type(), ArrowDataType::Timestamp(_, _)))
        .or_else(|| schema.fields().first())
        .ok_or_else(|| QueryError::Execution {
            message: "window stage requires at least one column to order by".to_string(),
        })?;
    let order_by = vec![col(order_field.name()).sort(true, false)];

    let window_exprs: Vec<DfExpr> = window
        .items
        .iter()
        .map(|item| {
            let e = window_fn_to_df_expr(&item.function, &order_by, schema, signal, ctx)?;
            Ok(e.alias(&item.alias))
        })
        .collect::<Result<_, QueryError>>()?;

    builder
        .window(window_exprs)
        .map_err(|e| QueryError::Execution {
            message: format!("Failed to apply window stage: {}", e),
        })
}

/// Build a DataFusion window expression for one [`WindowFn`], ordered by
/// `order_by`. Uses only `avg`/`sum` aggregate UDFs over row frames — including
/// `delta`, which reads the previous row via a `[1 preceding, 1 preceding]`
/// frame — so no separate lag window UDF dependency is needed.
fn window_fn_to_df_expr(
    fun: &WindowFn,
    order_by: &[datafusion_expr::SortExpr],
    schema: &datafusion::common::DFSchemaRef,
    signal: Signal,
    ctx: &SessionContext,
) -> Result<DfExpr, QueryError> {
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::expr::WindowFunction;
    use datafusion_expr::window_frame::{WindowFrame, WindowFrameBound, WindowFrameUnits};
    use datafusion_expr::{ExprFunctionExt, WindowFunctionDefinition};
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;

    let build = |udaf, arg: DfExpr, frame: WindowFrame| -> Result<DfExpr, QueryError> {
        Expr::WindowFunction(Box::new(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(udaf),
            vec![arg],
        )))
        .order_by(order_by.to_vec())
        .window_frame(frame)
        .build()
        .map_err(|e| QueryError::Execution {
            message: format!("Failed to build window function: {}", e),
        })
    };

    match fun {
        WindowFn::MovingAvg(expr, n) => {
            let arg = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            // Trailing frame of n rows: [n-1 preceding, current].
            let frame = WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n.saturating_sub(1)))),
                WindowFrameBound::CurrentRow,
            );
            build(avg_udaf(), arg, frame)
        }
        WindowFn::Cumulative(expr) => {
            let arg = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            // Running total: [unbounded preceding, current] (null bound = unbounded).
            let frame = WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            );
            build(sum_udaf(), arg, frame)
        }
        WindowFn::Delta(expr) => {
            let arg = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            // Previous row's value via sum over a single-row [1 preceding, 1 preceding]
            // frame; delta = current - previous.
            let prev_frame = WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            );
            let prev = build(sum_udaf(), arg.clone(), prev_frame)?;
            Ok(arg - prev)
        }
    }
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
    use seql_ast::ast::AttrScope;
    match field.scope {
        AttrScope::Attribute => {
            let catalog = sequins_arrow_schema::arrow_schema::default_schema_catalog();
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
                seql_ast::ast::StatusLiteral::Unset => 0,
                seql_ast::ast::StatusLiteral::Ok => 1,
                seql_ast::ast::StatusLiteral::Error => 2,
            };
            lit(v)
        }
        Literal::SpanKind(kind) => {
            // kind column is UInt8: Unspecified=0, Internal=1, Server=2, Client=3, Producer=4, Consumer=5
            let v: u8 = match kind {
                seql_ast::ast::SpanKindLiteral::Internal => 1,
                seql_ast::ast::SpanKindLiteral::Server => 2,
                seql_ast::ast::SpanKindLiteral::Client => 3,
                seql_ast::ast::SpanKindLiteral::Producer => 4,
                seql_ast::ast::SpanKindLiteral::Consumer => 5,
            };
            lit(v)
        }
        Literal::Severity(severity) => {
            // Convert to string
            let s = match severity {
                seql_ast::ast::SeverityLiteral::Trace => "TRACE",
                seql_ast::ast::SeverityLiteral::Debug => "DEBUG",
                seql_ast::ast::SeverityLiteral::Info => "INFO",
                seql_ast::ast::SeverityLiteral::Warn => "WARN",
                seql_ast::ast::SeverityLiteral::Error => "ERROR",
                seql_ast::ast::SeverityLiteral::Fatal => "FATAL",
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
    window_ns: Option<u64>,
) -> Result<DfExpr, QueryError> {
    let expr = ast_expr_to_df_expr(&group_expr.expr, schema, signal, ctx)?;

    // Handle time binning
    let expr = if let Some(bin) = &group_expr.bin {
        let bin_ns = resolve_bin_ns(bin, window_ns)?;
        // Bin by dividing timestamp by bin size, then multiplying back.
        // Time columns are stored as Timestamp(ns) — cast to Int64 first so
        // integer arithmetic works correctly.
        // E.g., for 5-minute bins: (cast(ts, Int64) / 300_000_000_000) * 300_000_000_000
        let bin_lit = lit(bin_ns as i64);
        let int_expr = cast(expr, ArrowDataType::Int64);
        let binned = (int_expr.clone() / bin_lit.clone()) * bin_lit;
        // Wrap the binned epoch-nanoseconds back into a Timestamp so the bucket column is
        // a real temporal type: clients render a proper time axis (Arrow Timestamp →
        // Swift `Date`) instead of a giant integer, and downstream keeps time semantics.
        cast(
            binned,
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
        )
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

/// `approx_percentile_cont(expr, q)` for a quantile `q` in (0,1). Shared by
/// `p95`/`p99` and the general `percentile(col, q)` aggregate.
fn approx_percentile_expr(df_expr: DfExpr, q: f64) -> DfExpr {
    let udaf =
        datafusion_functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf();
    Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
        udaf,
        vec![df_expr, lit(q)],
        false,  // distinct
        None,   // filter
        vec![], // order_by
        None,   // null_treatment
    ))
}

fn aggregate_fn_to_df_expr(
    agg_fn: &AggregateFn,
    schema: &datafusion::common::DFSchemaRef,
    signal: Signal,
    ctx: &SessionContext,
    rate_divisor_secs: Option<f64>,
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
        AggregateFn::P95(expr) => Ok(approx_percentile_expr(
            ast_expr_to_df_expr(expr, schema, signal, ctx)?,
            0.95,
        )),
        AggregateFn::P99(expr) => Ok(approx_percentile_expr(
            ast_expr_to_df_expr(expr, schema, signal, ctx)?,
            0.99,
        )),
        AggregateFn::Percentile(expr, q) => {
            // Clamp to the open interval (0,1) approx_percentile_cont accepts.
            let q = q.clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON);
            Ok(approx_percentile_expr(
                ast_expr_to_df_expr(expr, schema, signal, ctx)?,
                q,
            ))
        }
        AggregateFn::Stddev(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(datafusion_functions_aggregate::expr_fn::stddev(df_expr))
        }
        AggregateFn::Variance(expr) => {
            let df_expr = ast_expr_to_df_expr(expr, schema, signal, ctx)?;
            Ok(datafusion_functions_aggregate::expr_fn::var_sample(df_expr))
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
            // COUNT(*) / seconds — a per-second rate. `rate_divisor_secs` is the
            // time-bin width when the query is bucketed by `ts()`, else the whole
            // query window, so the rate scales with the selected time range.
            let secs = rate_divisor_secs.ok_or_else(|| QueryError::InvalidAst {
                message: "`throughput()` needs a time range: add a scope \
                          (e.g. `last 1h`) or supply one at execution"
                    .to_string(),
            })?;
            Ok(count(lit(1)) / lit(secs))
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

    #[test]
    fn test_effective_window_ns() {
        assert_eq!(
            effective_window_ns(Some(&TimeRange::SlidingWindow {
                start_ns: 3_600_000_000_000
            })),
            Some(3_600_000_000_000)
        );
        assert_eq!(
            effective_window_ns(Some(&TimeRange::Absolute {
                start_ns: 1_000,
                end_ns: 61_000
            })),
            Some(60_000)
        );
        // No range → no window (a scope-less template).
        assert_eq!(effective_window_ns(None), None);
    }

    /// A non-bucketed `throughput()` is `count(*) / window_seconds` (1h → 3600s),
    /// not the old bare `count(*)` stub.
    #[tokio::test]
    async fn test_throughput_divides_by_window_seconds() {
        let ctx = schema_context().expect("schema_context");
        let ast = seql_parser::parse("spans last 1h | group by {} { throughput() as tps }")
            .expect("parse");
        let (plan, _) = ast_to_logical_plan(&ast, &ctx).await.expect("plan");
        let text = format!("{}", plan.display_indent());
        assert!(
            text.contains("Float64(3600"),
            "throughput over a 1h window should divide by 3600s; plan was:\n{text}"
        );
    }

    #[test]
    fn test_resolve_bin_ns() {
        let hour = Some(3_600_000_000_000u64);
        // `bin 10%` of a 1h window → 6-minute buckets.
        assert_eq!(
            resolve_bin_ns(&BinSpec::Percent(10.0), hour).unwrap(),
            360_000_000_000
        );
        // Fixed passes through unchanged — and needs no window.
        assert_eq!(
            resolve_bin_ns(&BinSpec::Fixed(60_000_000_000), None).unwrap(),
            60_000_000_000
        );
        // `bin auto` snaps to a nice ladder step (1% of 1h = 36s → 60s).
        assert_eq!(
            resolve_bin_ns(&BinSpec::Auto, hour).unwrap(),
            60_000_000_000
        );
        // Never collapses to zero.
        assert!(resolve_bin_ns(&BinSpec::Percent(0.0), hour).unwrap() >= 1);
        // Percent/Auto without a window is a clear error, not a guess.
        assert!(resolve_bin_ns(&BinSpec::Percent(10.0), None).is_err());
        assert!(resolve_bin_ns(&BinSpec::Auto, None).is_err());
    }

    /// `ts() bin 10%` derives the bucket width from the query window so a saved
    /// time-series re-buckets when the range changes (10% of 1h = 6m).
    #[tokio::test]
    async fn test_bin_percent_scales_with_window() {
        let ctx = schema_context().expect("schema_context");
        let ast = seql_parser::parse(
            "spans last 1h | group by { ts() bin 10% as bucket } { count() as n }",
        )
        .expect("parse");
        let (plan, _) = ast_to_logical_plan(&ast, &ctx).await.expect("plan");
        let text = format!("{}", plan.display_indent());
        assert!(
            text.contains("360000000000"),
            "10% of a 1h window should bin at 6m (360000000000ns); plan was:\n{text}"
        );
    }

    /// A `window { … }` stage on a time series compiles AND round-trips through
    /// the backend's custom Substrait consumer (so the daemon can execute it).
    #[tokio::test]
    async fn test_window_stage_compiles_and_consumes() {
        use datafusion_substrait::extensions::Extensions;
        use datafusion_substrait::logical_plan::consumer::{
            DefaultSubstraitConsumer, SubstraitConsumer,
        };
        use datafusion_substrait::substrait::proto::{plan_rel, Plan};

        let ctx = schema_context().expect("schema_context");
        let bytes = compile(
            "spans last 1h | group by { ts() bin 5m as bucket } { count() as n } \
             | window { moving_avg(n, 3) as ma, cumulative(n) as run, delta(n) as d }",
            &ctx,
        )
        .await
        .expect("window query should compile");
        assert!(!bytes.is_empty());

        // Consume the primary relation back, exactly as the backend does.
        let plan: Plan = prost::Message::decode(&bytes[..]).expect("decode");
        let extensions = Extensions::try_from(&plan.extensions).expect("extensions");
        let state = ctx.state();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let rel = match plan.relations[0].rel_type.as_ref().expect("rel_type") {
            plan_rel::RelType::Root(root) => root.input.as_ref().expect("root input"),
            plan_rel::RelType::Rel(rel) => rel,
        };
        let consumed = consumer.consume_rel(rel).await;
        assert!(
            consumed.is_ok(),
            "backend consumer rejected the window plan: {:?}",
            consumed.err()
        );
    }

    /// The new statistical aggregates (stddev/variance/arbitrary percentile)
    /// parse and compile.
    /// SPIKE (Phase 4): does DataFusion's Substrait producer serialize a window
    /// function at all? If this errors, the `window { … }` stage must carry its
    /// spec in the SeqlExtension and apply it on the backend instead.
    #[tokio::test]
    async fn spike_window_fn_substrait_producer() {
        let ctx = schema_context().expect("schema_context");
        let sql = "SELECT trace_id, \
                   avg(duration_ns) OVER (ORDER BY start_time_unix_nano \
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma \
                   FROM spans";
        let df = ctx.sql(sql).await.expect("window SQL should plan");
        let plan = df.into_optimized_plan().expect("optimize");
        let produced = to_substrait_plan(&plan, &ctx.state());
        // Intentional: surfaces the producer's verdict in the test log.
        assert!(
            produced.is_ok(),
            "Substrait producer rejected a window function: {:?}",
            produced.err()
        );
    }

    /// SPIKE (Phase 4): full round-trip — does the backend's *custom* consumer
    /// (`DefaultSubstraitConsumer`) accept a window function back? If yes, the
    /// `window { … }` stage can go through the normal compile→Substrait→consume
    /// path with no fallback.
    #[tokio::test]
    async fn spike_window_fn_substrait_round_trip() {
        use datafusion_substrait::extensions::Extensions;
        use datafusion_substrait::logical_plan::consumer::{
            DefaultSubstraitConsumer, SubstraitConsumer,
        };
        use datafusion_substrait::substrait::proto::plan_rel;

        let ctx = schema_context().expect("schema_context");
        let sql = "SELECT trace_id, \
                   avg(duration_ns) OVER (ORDER BY start_time_unix_nano \
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma \
                   FROM spans";
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .into_optimized_plan()
            .expect("optimize");
        let substrait = *to_substrait_plan(&plan, &ctx.state()).expect("produce");

        let extensions = Extensions::try_from(&substrait.extensions).expect("extensions");
        let state = ctx.state();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let rel = match substrait.relations[0].rel_type.as_ref().expect("rel_type") {
            plan_rel::RelType::Root(root) => root.input.as_ref().expect("root input"),
            plan_rel::RelType::Rel(rel) => rel,
        };
        let consumed = consumer.consume_rel(rel).await;
        assert!(
            consumed.is_ok(),
            "custom consumer rejected a window function: {:?}",
            consumed.err()
        );
    }

    #[tokio::test]
    async fn test_new_statistical_aggregates_compile() {
        let ctx = schema_context().expect("schema_context");
        let bytes = compile(
            "spans last 1h | group by {} { \
             stddev(duration_ns) as sd, \
             variance(duration_ns) as v, \
             percentile(duration_ns, 0.9) as p90 }",
            &ctx,
        )
        .await
        .expect("stddev/variance/percentile should compile");
        assert!(!bytes.is_empty());
    }

    /// `percentile(col, 90)` (a 0–100 form) is normalized to the 0..1 quantile.
    #[test]
    fn test_percentile_parses_0_100_form() {
        let ast = seql_parser::parse(
            "spans last 1h | group by {} { percentile(duration_ns, 90) as p90 }",
        )
        .expect("parse");
        let Some(Stage::Aggregate(agg)) = ast.stages.first() else {
            panic!("expected aggregate stage");
        };
        match &agg.aggregations[0].function {
            AggregateFn::Percentile(_, q) => assert!((*q - 0.90).abs() < 1e-9, "q={q}"),
            other => panic!("expected Percentile, got {other:?}"),
        }
    }

    /// A scope-less template parses (no inline time scope) and, when a range is
    /// injected at compile time, compiles successfully.
    #[tokio::test]
    async fn test_template_scope_less_with_injected_range() {
        let ctx = schema_context().expect("schema_context");
        let ast =
            seql_parser::parse("spans | group by { ts() bin 10% as bucket } { count() as n }")
                .expect("scope-less template should parse");
        assert_eq!(
            ast.scan.time_range, None,
            "template carries no inline scope"
        );
        let bytes = compile_ast_with_range(
            ast,
            Some(TimeRange::SlidingWindow {
                start_ns: 3_600_000_000_000,
            }),
            &ctx,
        )
        .await
        .expect("template + injected range should compile");
        assert!(!bytes.is_empty());
    }

    /// A scope-less template on a time-scoped signal errors if executed without a
    /// range (rather than silently scanning everything).
    #[tokio::test]
    async fn test_template_without_range_errors() {
        let ctx = schema_context().expect("schema_context");
        let ast = seql_parser::parse("spans | group by {} { count() as n }").expect("parse");
        assert!(
            compile_ast(ast, &ctx).await.is_err(),
            "a scope-less spans query with no injected range must error"
        );
    }

    /// An injected range overrides an inline scope — the dashboard's selected
    /// range wins. Injecting 1h into a `last 5m` query makes `bin 10%` = 6m
    /// (360000000000ns), proving the 1h window, not the inline 5m, was used.
    #[tokio::test]
    async fn test_injected_range_overrides_inline_scope() {
        let ctx = schema_context().expect("schema_context");
        let mut ast = seql_parser::parse(
            "spans last 5m | group by { ts() bin 10% as bucket } { count() as n }",
        )
        .expect("parse");
        // Injection is `scan.time_range = Some(range)` (as compile_ast_with_range does).
        ast.scan.time_range = Some(TimeRange::SlidingWindow {
            start_ns: 3_600_000_000_000,
        });
        let (plan, _) = ast_to_logical_plan(&ast, &ctx).await.expect("plan");
        let text = format!("{}", plan.display_indent());
        assert!(
            text.contains("360000000000") && !text.contains("30000000000"),
            "injected 1h window should drive bin 10% to 6m, not the inline 5m; plan:\n{text}"
        );
    }

    /// A time-bucketed `throughput()` is a per-bucket rate: divide by the bin
    /// width (1m → 60s), not the whole query window.
    #[tokio::test]
    async fn test_throughput_bucketed_divides_by_bin_seconds() {
        let ctx = schema_context().expect("schema_context");
        let ast = seql_parser::parse(
            "spans last 1h | group by { ts() bin 1m as bucket } { throughput() as tps }",
        )
        .expect("parse");
        let (plan, _) = ast_to_logical_plan(&ast, &ctx).await.expect("plan");
        let text = format!("{}", plan.display_indent());
        assert!(
            text.contains("Float64(60") && !text.contains("Float64(3600"),
            "bucketed throughput should divide by the 60s bin, not the window; plan was:\n{text}"
        );
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
        use seql_ast::ast::{QueryMode, Scan, Signal, Stage, TimeRange};
        use seql_ast::schema::infer_shape;
        let ast = QueryAst {
            bindings: vec![],
            scan: Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
            },
            stages: vec![],
            mode: QueryMode::Snapshot,
        };
        assert_eq!(infer_shape(&ast), seql_ast::schema::ResponseShape::Table);
        // Patterns stage is a no-op in the compiler but schema still infers PatternGroups
        let _ = Stage::Patterns; // ensure variant still exists
    }

    #[test]
    fn test_infer_shape_patterns() {
        use seql_ast::ast::{PatternsStage, QueryMode, Scan, Signal, Stage, TimeRange};
        use seql_ast::schema::infer_shape;
        let ast = QueryAst {
            bindings: vec![],
            scan: Scan {
                signal: Signal::Logs,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
            },
            stages: vec![Stage::Patterns(PatternsStage { field: None })],
            mode: QueryMode::Snapshot,
        };
        assert_eq!(
            infer_shape(&ast),
            seql_ast::schema::ResponseShape::PatternGroups
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
        use seql_ast::ast::TimeRangeStage;

        let stage = Stage::TimeRange(TimeRangeStage {
            duration_ns: 1_800_000_000_000, // 30 minutes
        });

        let json = serde_json::to_string(&stage).unwrap();
        let back: Stage = serde_json::from_str(&json).unwrap();
        assert_eq!(stage, back);
    }

    #[tokio::test]
    async fn test_compile_filter_all_compare_ops() {
        use seql_ast::ast::{AttrScope, CompareExpr, Expr as AstExpr, FieldRef, Predicate};

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
                scan: seql_ast::ast::Scan {
                    signal: Signal::Spans,
                    time_range: Some(TimeRange::SlidingWindow {
                        start_ns: 3_600_000_000_000,
                    }),
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
        use seql_ast::ast::{AttrScope, Expr as AstExpr, FieldRef};

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
                aggregations: vec![seql_ast::ast::Aggregation {
                    function: agg_fn.clone(),
                    alias: "result".to_string(),
                    filter: None,
                }],
            };

            let ast = QueryAst {
                bindings: vec![],
                scan: seql_ast::ast::Scan {
                    signal: Signal::Spans,
                    time_range: Some(TimeRange::SlidingWindow {
                        start_ns: 3_600_000_000_000,
                    }),
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
        use seql_ast::ast::{AttrScope, FieldRef};

        let ctx = schema_context().expect("schema_context should succeed");

        // Create a project stage with a field that doesn't exist in the schema
        // Note: DataFusion may not validate field names at plan-building time,
        // so this might succeed at compile time and fail at execution time
        let project = ProjectStage {
            fields: vec![seql_ast::ast::ProjectField {
                field: FieldRef {
                    scope: AttrScope::Signal,
                    name: "nonexistent_field_xyz".to_string(),
                },
                alias: None,
            }],
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
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
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::Absolute {
                    start_ns: 1_000_000_000_000,
                    end_ns: 2_000_000_000_000,
                }),
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
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000, // 1 hour
                }),
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
            scan: seql_ast::ast::Scan {
                signal: Signal::Logs,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
            },
            stages: vec![Stage::TimeRange(seql_ast::ast::TimeRangeStage {
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
        use seql_ast::ast::{AttrScope, CompareExpr, Expr as AstExpr, FieldRef, Predicate};

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
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
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
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
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
        use seql_ast::ast::{AttrScope, Expr as AstExpr, FieldRef, SortExpr};

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
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
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
        use seql_ast::ast::{AttrScope, FieldRef, ProjectField};

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
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
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
        use seql_ast::ast::{AttrScope, FieldRef};

        let ctx = schema_context().expect("schema_context should succeed");

        let unique = UniqueStage {
            field: FieldRef {
                scope: AttrScope::Signal,
                name: "trace_id".to_string(),
            },
        };

        let ast = QueryAst {
            bindings: vec![],
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
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
        use seql_ast::ast::{AttrScope, FieldRef};

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
                scan: seql_ast::ast::Scan {
                    signal: Signal::Spans,
                    time_range: Some(TimeRange::SlidingWindow {
                        start_ns: 3_600_000_000_000,
                    }),
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
        let _ast = seql_parser::parse("metrics last 1h").unwrap();
        let expr = AstExpr::FunctionCall {
            function: ScalarFn::Timestamp,
            args: vec![],
        };
        let schema = Arc::new(datafusion::common::DFSchema::empty());
        let result = ast_expr_to_df_expr(&expr, &schema, Signal::Metrics, &ctx);
        assert!(result.is_err(), "ts() on Metrics should fail");
    }

    #[tokio::test]
    async fn test_compile_aggregate_with_filter() {
        use seql_ast::ast::{Aggregation, AttrScope, CompareExpr, CompareOp, FieldRef, Predicate};
        use seql_ast::ast::{Expr as AstExpr, Literal};

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
            scan: seql_ast::ast::Scan {
                signal: Signal::Spans,
                time_range: Some(TimeRange::SlidingWindow {
                    start_ns: 3_600_000_000_000,
                }),
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

        let ast = seql_parser::parse(
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
