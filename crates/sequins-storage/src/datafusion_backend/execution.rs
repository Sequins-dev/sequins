//! Query execution logic

use super::exec_err;
use super::Storage;
use arrow::array::AsArray;
use arrow::compute::{concat_batches, filter_record_batch};
use arrow_flight::FlightData;
use datafusion::common::DFSchema;
use datafusion::execution::context::{ExecutionProps, SessionContext};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{DefaultSubstraitConsumer, SubstraitConsumer};
use datafusion_substrait::substrait::proto::{plan_rel, Plan};
use futures::stream;
use futures::StreamExt;
use prost::Message;
use sequins_query::ast::{QueryMode, Signal};
use sequins_query::error::QueryError;
use sequins_query::flight::{
    append_flight_data, complete_flight_data, data_flight_data, replace_flight_data,
    schema_flight_data,
};
use sequins_query::frame::QueryStats;
use sequins_query::seql_ext;
use sequins_query::SeqlStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Execute a compiled Substrait plan (as raw bytes), returning a `SeqlStream` of `FlightData`.
///
/// The `plan_bytes` must be a serialized Substrait `Plan` with `SeqlExtension` embedded
/// in `advanced_extensions.enhancement`. Supports both snapshot and live modes.
#[tracing::instrument(skip_all, fields(mode))]
pub(super) async fn execute_plan(
    storage: &Arc<Storage>,
    plan_bytes: Vec<u8>,
    make_session_ctx: impl std::future::Future<Output = Result<SessionContext, QueryError>>,
) -> Result<SeqlStream, QueryError> {
    // Decode the plan to extract the SeqlExtension metadata
    let plan: Plan = Message::decode(&plan_bytes[..])
        .map_err(|e| exec_err(format!("Failed to decode Substrait plan: {}", e)))?;

    let ext = extract_seql_extension(&plan)?;

    let mode = if ext.mode == seql_ext::QueryMode::Live as i32 {
        QueryMode::Live
    } else {
        QueryMode::Snapshot
    };

    match mode {
        QueryMode::Live => execute_live(storage, plan_bytes, make_session_ctx).await,
        QueryMode::Snapshot => execute_snapshot(storage, plan_bytes, make_session_ctx).await,
    }
}

/// Extract and decode the `SeqlExtension` from a Substrait plan.
fn extract_seql_extension(plan: &Plan) -> Result<seql_ext::SeqlExtension, QueryError> {
    let ext_any = plan
        .advanced_extensions
        .as_ref()
        .and_then(|ae| ae.enhancement.as_ref())
        .ok_or_else(|| exec_err("Plan missing SeqlExtension in advanced_extensions.enhancement"))?;
    seql_ext::SeqlExtension::decode(&ext_any.value[..])
        .map_err(|e| exec_err(format!("Failed to decode SeqlExtension: {}", e)))
}

/// Extract the actual `Rel` from a `PlanRel` (handles both Rel and Root variants).
fn get_rel(plan_rel: &plan_rel::RelType) -> Option<&datafusion_substrait::substrait::proto::Rel> {
    match plan_rel {
        plan_rel::RelType::Rel(rel) => Some(rel),
        plan_rel::RelType::Root(root) => root.input.as_ref(),
    }
}

/// Convert a signal name string back to `Signal` enum.
///
/// Used in the live path to subscribe to the correct WAL broadcast channel.
fn signal_from_str(s: &str) -> Signal {
    match s {
        "spans" | "traces" => Signal::Spans,
        "logs" => Signal::Logs,
        "datapoints" => Signal::Datapoints,
        "histograms" => Signal::Histograms,
        "metrics" => Signal::Metrics,
        "samples" => Signal::Samples,
        "profiles" => Signal::Profiles,
        "stacks" => Signal::Stacks,
        "frames" => Signal::Frames,
        "mappings" => Signal::Mappings,
        "resources" => Signal::Resources,
        "scopes" => Signal::Scopes,
        "span_links" => Signal::SpanLinks,
        _ => Signal::Spans, // fallback
    }
}

/// Execute all plan roots as a snapshot, returning FlightData messages for each root.
///
/// Each root produces:
/// - `schema_flight_data(table, ...)` — schema header
/// - `data_flight_data(table, &batch)` — result batch (if non-empty)
///
/// The primary root uses `table = None`. Auxiliary roots use `table = Some(alias)`.
/// Each auxiliary plan is decoded independently with its own extensions context,
/// avoiding the "function reference not found" error that occurred when auxiliary
/// relations were merged into the primary plan (dropping their extensions).
#[tracing::instrument(skip_all)]
async fn execute_snapshot(
    storage: &Arc<Storage>,
    plan_bytes: Vec<u8>,
    make_session_ctx: impl std::future::Future<Output = Result<SessionContext, QueryError>>,
) -> Result<SeqlStream, QueryError> {
    let start = Instant::now();
    let ctx = make_session_ctx.await?;

    let plan: Plan = Message::decode(&plan_bytes[..])
        .map_err(|e| exec_err(format!("Failed to decode Substrait plan: {}", e)))?;
    let ext = extract_seql_extension(&plan)?;
    let response_shape = sequins_query::schema::ResponseShape::from_shape_str(&ext.response_shape)
        .unwrap_or(sequins_query::schema::ResponseShape::Table);

    let primary_extensions = Extensions::try_from(&plan.extensions)
        .map_err(|e| exec_err(format!("Failed to parse Substrait extensions: {}", e)))?;

    let watermark = storage.wal().last_seq();

    let mut all_frames: Vec<Result<FlightData, QueryError>> = Vec::new();
    let mut total_rows: u64 = 0;

    // Execute primary plan root (relations[0]) with the primary plan's extensions
    if let Some(plan_rel) = plan.relations.first() {
        if let Some(rel_type) = &plan_rel.rel_type {
            if let Some(rel) = get_rel(rel_type) {
                let ctx_state = ctx.state();
                let consumer = DefaultSubstraitConsumer::new(&primary_extensions, &ctx_state);
                let logical_plan = consumer.consume_rel(rel).await.map_err(|e| {
                    exec_err(format!("Failed to consume Substrait relation[0]: {}", e))
                })?;

                let (arrow_schema, batch) =
                    execute_logical_plan_to_batch(&ctx, logical_plan).await?;
                let col_defs = super::arrow_convert::schema_to_col_defs(&arrow_schema);

                all_frames.push(Ok(schema_flight_data(
                    None,
                    arrow_schema,
                    response_shape,
                    col_defs,
                    watermark,
                )));
                total_rows += batch.num_rows() as u64;
                if batch.num_rows() > 0 {
                    all_frames.push(Ok(data_flight_data(None, &batch)));
                }
            }
        }
    }

    // Execute each auxiliary plan independently using its own extensions context.
    // Each entry in auxiliary_plan_bytes is a complete serialized Substrait Plan.
    for (idx, aux_bytes) in ext.auxiliary_plan_bytes.iter().enumerate() {
        let alias = ext.auxiliary_aliases.get(idx).map(String::as_str);

        let aux_plan: Plan = Message::decode(&aux_bytes[..])
            .map_err(|e| exec_err(format!("Failed to decode auxiliary plan[{}]: {}", idx, e)))?;

        let aux_extensions = Extensions::try_from(&aux_plan.extensions).map_err(|e| {
            exec_err(format!(
                "Failed to parse auxiliary extensions[{}]: {}",
                idx, e
            ))
        })?;

        let rel = aux_plan
            .relations
            .first()
            .and_then(|pr| pr.rel_type.as_ref())
            .and_then(|rt| get_rel(rt));
        let Some(rel) = rel else { continue };

        let ctx_state = ctx.state();
        let consumer = DefaultSubstraitConsumer::new(&aux_extensions, &ctx_state);
        let logical_plan = consumer.consume_rel(rel).await.map_err(|e| {
            exec_err(format!(
                "Failed to consume auxiliary Substrait relation[{}]: {}",
                idx, e
            ))
        })?;

        let (arrow_schema, batch) = execute_logical_plan_to_batch(&ctx, logical_plan).await?;
        let col_defs = super::arrow_convert::schema_to_col_defs(&arrow_schema);

        all_frames.push(Ok(schema_flight_data(
            alias,
            arrow_schema,
            sequins_query::schema::ResponseShape::Table,
            col_defs,
            watermark,
        )));
        total_rows += batch.num_rows() as u64;
        if batch.num_rows() > 0 {
            all_frames.push(Ok(data_flight_data(alias, &batch)));
        }
    }

    let elapsed_us = start.elapsed().as_micros() as u64;
    all_frames.push(Ok(complete_flight_data(QueryStats {
        execution_time_us: elapsed_us,
        rows_scanned: total_rows,
        bytes_read: 0,
        rows_returned: total_rows,
        warning_count: 0,
    })));

    Ok(Box::pin(stream::iter(all_frames)))
}

/// Execute a `LogicalPlan` and collect all results into a single concatenated `RecordBatch`.
#[tracing::instrument(skip_all)]
async fn execute_logical_plan_to_batch(
    ctx: &SessionContext,
    logical_plan: LogicalPlan,
) -> Result<
    (
        Arc<arrow::datatypes::Schema>,
        arrow::record_batch::RecordBatch,
    ),
    QueryError,
> {
    let df = ctx
        .execute_logical_plan(logical_plan)
        .await
        .map_err(|e| exec_err(format!("Failed to execute logical plan: {}", e)))?;

    let arrow_schema = Arc::new(df.schema().as_arrow().clone());

    let batches = df
        .collect()
        .await
        .map_err(|e| exec_err(format!("Execution error: {}", e)))?;

    let batch = if batches.is_empty() {
        arrow::record_batch::RecordBatch::new_empty(arrow_schema.clone())
    } else {
        concat_batches(&arrow_schema, &batches)
            .map_err(|e| exec_err(format!("Failed to concat batches: {}", e)))?
    };

    Ok((arrow_schema, batch))
}

/// Execute a live (streaming) query.
///
/// Phase 1: Executes the historical snapshot for all plan roots (Schema + Data frames).
/// Phase 2: Subscribes to WAL broadcasts for the primary signal and emits live deltas.
///
/// Aggregated primary plans use the Replace strategy (re-execute on each broadcast).
/// Non-aggregated primary plans use the Append strategy (emit new batches as they arrive).
///
/// Note: Auxiliary table live updates (for Merge queries) are not yet implemented.
/// Auxiliary tables are returned only in the historical snapshot phase.
#[tracing::instrument(skip_all, fields(signal = tracing::field::Empty))]
async fn execute_live(
    storage: &Arc<Storage>,
    plan_bytes: Vec<u8>,
    make_session_ctx: impl std::future::Future<Output = Result<SessionContext, QueryError>>,
) -> Result<SeqlStream, QueryError> {
    let ctx = make_session_ctx.await?;

    let plan: Plan = Message::decode(&plan_bytes[..])
        .map_err(|e| exec_err(format!("Failed to decode Substrait plan: {}", e)))?;
    let ext = extract_seql_extension(&plan)?;
    tracing::Span::current().record("signal", ext.signal.as_str());
    let primary_signal = signal_from_str(&ext.signal);
    let response_shape = sequins_query::schema::ResponseShape::from_shape_str(&ext.response_shape)
        .unwrap_or(sequins_query::schema::ResponseShape::Table);

    let mut auxiliary_signal_map: Vec<(Signal, String)> = Vec::new();
    for (idx, sig_name) in ext.auxiliary_signals.iter().enumerate() {
        if let Some(alias) = ext.auxiliary_aliases.get(idx) {
            auxiliary_signal_map.push((signal_from_str(sig_name), alias.clone()));
        }
    }

    let primary_extensions = Extensions::try_from(&plan.extensions)
        .map_err(|e| exec_err(format!("Failed to parse Substrait extensions: {}", e)))?;

    let watermark = storage.wal().last_seq();

    // --- Phase 1: Historical snapshot (primary + auxiliary plan roots) ---
    let mut historical_frames: Vec<Result<FlightData, QueryError>> = Vec::new();
    let mut has_aggregation = false;
    let mut primary_filter: Option<Arc<dyn PhysicalExpr>> = None;

    // Execute primary plan root with the primary plan's extensions
    if let Some(plan_rel) = plan.relations.first() {
        if let Some(rel_type) = &plan_rel.rel_type {
            if let Some(rel) = get_rel(rel_type) {
                let ctx_state = ctx.state();
                let consumer = DefaultSubstraitConsumer::new(&primary_extensions, &ctx_state);
                let logical_plan = consumer.consume_rel(rel).await.map_err(|e| {
                    exec_err(format!("Failed to consume Substrait relation[0]: {}", e))
                })?;

                has_aggregation = plan_has_aggregation(&logical_plan);
                // Optimize before extracting filter so TypeCoercionRule inserts CAST nodes.
                // Without this, numeric literals (e.g. resource_id = 12345) are typed as
                // Float64 in the Substrait-deserialized plan, causing UInt32 == Float64
                // evaluation errors that silently drop every live batch.
                let optimized_for_filter = ctx
                    .state()
                    .optimize(&logical_plan)
                    .unwrap_or_else(|_| logical_plan.clone());
                let time_col = sequins_query::compiler::time_column_for_signal(primary_signal);
                primary_filter =
                    extract_filter_predicates(&optimized_for_filter).and_then(|(expr, schema)| {
                        let live_expr = strip_time_upper_bound(expr, time_col);
                        build_physical_filter(&live_expr, &schema).ok()
                    });

                let (arrow_schema, batch) =
                    execute_logical_plan_to_batch(&ctx, logical_plan).await?;
                let col_defs = super::arrow_convert::schema_to_col_defs(&arrow_schema);

                historical_frames.push(Ok(schema_flight_data(
                    None,
                    arrow_schema,
                    response_shape.clone(),
                    col_defs,
                    watermark,
                )));
                if batch.num_rows() > 0 {
                    historical_frames.push(Ok(data_flight_data(None, &batch)));
                }
            }
        }
    }

    // Execute each auxiliary plan independently using its own extensions context
    for (idx, aux_bytes) in ext.auxiliary_plan_bytes.iter().enumerate() {
        let alias = ext.auxiliary_aliases.get(idx).map(String::as_str);

        let aux_plan: Plan = Message::decode(&aux_bytes[..])
            .map_err(|e| exec_err(format!("Failed to decode auxiliary plan[{}]: {}", idx, e)))?;

        let aux_extensions = Extensions::try_from(&aux_plan.extensions).map_err(|e| {
            exec_err(format!(
                "Failed to parse auxiliary extensions[{}]: {}",
                idx, e
            ))
        })?;

        let rel = aux_plan
            .relations
            .first()
            .and_then(|pr| pr.rel_type.as_ref())
            .and_then(|rt| get_rel(rt));
        let Some(rel) = rel else { continue };

        let ctx_state = ctx.state();
        let consumer = DefaultSubstraitConsumer::new(&aux_extensions, &ctx_state);
        let logical_plan = consumer.consume_rel(rel).await.map_err(|e| {
            exec_err(format!(
                "Failed to consume auxiliary Substrait relation[{}]: {}",
                idx, e
            ))
        })?;

        let (arrow_schema, batch) = execute_logical_plan_to_batch(&ctx, logical_plan).await?;
        let col_defs = super::arrow_convert::schema_to_col_defs(&arrow_schema);

        historical_frames.push(Ok(schema_flight_data(
            alias,
            arrow_schema,
            sequins_query::schema::ResponseShape::Table,
            col_defs,
            watermark,
        )));
        if batch.num_rows() > 0 {
            historical_frames.push(Ok(data_flight_data(alias, &batch)));
        }
    }

    let historical_stream: SeqlStream = Box::pin(stream::iter(historical_frames));

    // Extract sliding window duration for live expiry scheduling.
    let window_ns: Option<u64> = ext.time_range.as_ref().and_then(|tr| match &tr.range {
        Some(sequins_query::seql_ext::time_range::Range::SlidingWindowNs(ns)) => Some(*ns),
        _ => None,
    });

    // --- Phase 2: Live stream for primary table ---
    let live_stream = if has_aggregation {
        execute_live_replace_stream(
            storage,
            plan_bytes,
            primary_signal,
            ctx,
            ext.auxiliary_plan_bytes.clone(),
            auxiliary_signal_map,
        )
    } else {
        execute_live_append_stream(
            storage,
            primary_signal,
            auxiliary_signal_map,
            primary_filter,
            window_ns,
        )
    };

    Ok(Box::pin(historical_stream.chain(live_stream)))
}

/// Build the Append-delta live stream for non-aggregated queries.
///
/// Subscribes to the WAL broadcast channel. On each new batch for the primary
/// signal, applies the optional `primary_filter` (a compiled `PhysicalExpr` from the
/// `where` clause), then emits an Append FlightData message. Filtered-out batches
/// (zero rows after filtering) are silently dropped.
/// On each new batch for an auxiliary signal, emits an Append with the alias.
///
/// When `window_ns` is Some (sliding window query), a per-batch expiry timer fires
/// an `Expire` message for each batch once its oldest row ages out of the window.
#[tracing::instrument(skip_all, fields(signal = ?primary_signal, has_filter = primary_filter.is_some()))]
fn execute_live_append_stream(
    storage: &Arc<Storage>,
    primary_signal: Signal,
    auxiliary_signals: Vec<(Signal, String)>,
    primary_filter: Option<Arc<dyn PhysicalExpr>>,
    window_ns: Option<u64>,
) -> SeqlStream {
    let broadcast_tx = storage.live_broadcast_tx();
    let mut broadcast_rx = broadcast_tx.subscribe();

    let heartbeat_emitter = Arc::new(crate::live_query::HeartbeatEmitter::new(
        Duration::from_secs(5),
        Arc::clone(storage.wal()),
    ));

    // Name of the time column for this signal — used to extract timestamps for expiry.
    let time_col =
        sequins_query::compiler::time_column_for_signal(primary_signal).map(|s| s.to_owned());

    Box::pin(async_stream::stream! {
        let hb_stream = heartbeat_emitter.start();
        tokio::pin!(hb_stream);

        let mut seq: u64 = 0;
        // (expiry_instant, start_row_id) for each pending Append batch.
        let mut expiry_queue: std::collections::VecDeque<(tokio::time::Instant, u64)> = std::collections::VecDeque::new();

        loop {
            // Compute the next expiry deadline (if any) for use in select!.
            let next_expiry = expiry_queue.front().map(|(t, _)| *t);

            tokio::select! {
                result = broadcast_rx.recv() => {
                    match result {
                        Ok((signal, batch)) => {
                            if signal == primary_signal {
                                tracing::debug!(signal = ?signal, rows = batch.num_rows(), filter = primary_filter.is_some(), "live_append broadcast");
                                let batch = (*batch).clone();
                                let batch = if let Some(ref filter) = primary_filter {
                                    match filter.evaluate(&batch) {
                                        Ok(col_val) => match col_val.into_array(batch.num_rows()) {
                                            Ok(arr) => {
                                                let mask = arr.as_boolean();
                                                match filter_record_batch(&batch, mask) {
                                                    Ok(filtered) if filtered.num_rows() == 0 => { tracing::debug!(total = batch.num_rows(), "live_append filter: 0 rows, skip"); continue },
                                                    Ok(filtered) => { tracing::debug!(kept = filtered.num_rows(), total = batch.num_rows(), "live_append filter: rows kept"); filtered },
                                                    Err(e) => { tracing::warn!(error = %e, "live_append filter_record_batch error"); continue },
                                                }
                                            }
                                            Err(e) => { tracing::warn!(error = %e, "live_append into_array error"); continue },
                                        },
                                        Err(e) => { tracing::warn!(error = %e, "live_append evaluate error"); continue },
                                    }
                                } else {
                                    tracing::debug!(rows = batch.num_rows(), "live_append no filter, passing all rows");
                                    batch
                                };

                                let batch_seq = seq;
                                seq += batch.num_rows() as u64;
                                tracing::debug!(rows = batch.num_rows(), "live_append yielding Append");
                                yield Ok(append_flight_data(None, &batch, batch_seq, batch_seq));

                                // Schedule expiry for this batch if we have a sliding window.
                                if let (Some(wns), Some(ref col)) = (window_ns, &time_col) {
                                    if let Some(expiry_instant) = batch_expiry_instant(&batch, col, wns) {
                                        expiry_queue.push_back((expiry_instant, batch_seq));
                                    }
                                }
                            } else if let Some(alias) = auxiliary_signals.iter()
                                .find(|(s, _)| *s == signal)
                                .map(|(_, a)| a.as_str())
                            {
                                yield Ok(append_flight_data(Some(alias), &(*batch).clone(), 0, 0));
                            }
                        }
                        Err(_) => continue,
                    }
                }
                hb = hb_stream.next() => {
                    if let Some(fd) = hb {
                        yield Ok(fd);
                    }
                }
                // Expiry timer: fire when the oldest queued batch has aged out.
                _ = async {
                    match next_expiry {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Drain all batches whose expiry has passed.
                    let now = tokio::time::Instant::now();
                    while let Some((expiry, row_id)) = expiry_queue.front().copied() {
                        if expiry <= now {
                            expiry_queue.pop_front();
                            tracing::debug!(row_id, "live_append expiring batch");
                            yield Ok(sequins_query::flight::expire_flight_data(None, row_id, 0));
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    })
}

/// Compute the `tokio::time::Instant` at which a batch should expire from a sliding window.
///
/// Finds the minimum (oldest) timestamp in the batch's time column, then returns
/// `Instant::now() + remaining_ns` where `remaining_ns = min_time_ns + window_ns - now_ns`.
/// Returns `None` if the batch has no time column, contains no rows, or has already expired.
fn batch_expiry_instant(
    batch: &arrow::record_batch::RecordBatch,
    time_col: &str,
    window_ns: u64,
) -> Option<tokio::time::Instant> {
    use arrow::array::TimestampNanosecondArray;
    use arrow::compute::min as arrow_min;

    let col = batch.column_by_name(time_col)?;
    let ts_array = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
    let min_ns = arrow_min(ts_array)? as u64;

    let now_ns = sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime);
    let expiry_ns = min_ns.saturating_add(window_ns);
    if expiry_ns <= now_ns {
        // Already expired — fire immediately (next loop iteration).
        return Some(tokio::time::Instant::now());
    }
    let remaining = Duration::from_nanos(expiry_ns - now_ns);
    Some(tokio::time::Instant::now() + remaining)
}

/// Debounce interval for coalescing rapid WAL broadcasts before re-executing.
const DEBOUNCE_DURATION: Duration = Duration::from_millis(50);

/// Build the Replace-delta live stream for aggregated queries.
///
/// Subscribes to the WAL broadcast channel. On each primary-signal broadcast (debounced
/// at 50 ms), re-executes the primary plan root and emits a Replace FlightData message.
/// Also re-executes auxiliary plans and emits Replace for each auxiliary table.
#[tracing::instrument(skip_all, fields(signal = ?primary_signal))]
fn execute_live_replace_stream(
    storage: &Arc<Storage>,
    plan_bytes: Vec<u8>,
    primary_signal: Signal,
    ctx: SessionContext,
    auxiliary_plan_bytes: Vec<Vec<u8>>,
    auxiliary_signals: Vec<(Signal, String)>,
) -> SeqlStream {
    let broadcast_tx = storage.live_broadcast_tx();
    let mut broadcast_rx = broadcast_tx.subscribe();

    let heartbeat_emitter = Arc::new(crate::live_query::HeartbeatEmitter::new(
        Duration::from_secs(5),
        Arc::clone(storage.wal()),
    ));

    Box::pin(async_stream::stream! {
        let hb_stream = heartbeat_emitter.start();
        tokio::pin!(hb_stream);

        let mut pending = false;
        let mut debounce_deadline: Option<tokio::time::Instant> = None;

        loop {
            let maybe_deadline = debounce_deadline;

            tokio::select! {
                result = broadcast_rx.recv() => {
                    match result {
                        Ok((signal, _batch)) => {
                            let is_relevant = signal == primary_signal
                                || auxiliary_signals.iter().any(|(s, _)| *s == signal);
                            if !is_relevant {
                                continue;
                            }
                            pending = true;
                            debounce_deadline = Some(
                                tokio::time::Instant::now() + DEBOUNCE_DURATION,
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(lagged_by = n, "live_replace broadcast lagged");
                            pending = true;
                            debounce_deadline = Some(
                                tokio::time::Instant::now() + DEBOUNCE_DURATION,
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                _ = async {
                    if let Some(d) = maybe_deadline {
                        tokio::time::sleep_until(d).await;
                    } else {
                        futures::future::pending::<()>().await;
                    }
                }, if pending => {
                    debounce_deadline = None;
                    pending = false;

                    // Re-execute primary plan root against hot+cold.
                    let result: Result<arrow::record_batch::RecordBatch, QueryError> = async {
                        let plan: Plan = Message::decode(&plan_bytes[..])
                            .map_err(|e| exec_err(format!("Substrait decode: {}", e)))?;
                        let extensions = Extensions::try_from(&plan.extensions)
                            .map_err(|e| exec_err(format!("Extensions parse: {}", e)))?;
                        let rel_type = plan.relations.first()
                            .and_then(|pr| pr.rel_type.as_ref())
                            .ok_or_else(|| exec_err("Plan has no relations"))?;
                        let rel = get_rel(rel_type)
                            .ok_or_else(|| exec_err("First relation has no Rel"))?;
                        let ctx_state = ctx.state();
                        let consumer = DefaultSubstraitConsumer::new(&extensions, &ctx_state);
                        let logical_plan = consumer.consume_rel(rel).await
                            .map_err(|e| exec_err(format!("consume_rel: {}", e)))?;
                        let (_schema, batch) = execute_logical_plan_to_batch(&ctx, logical_plan).await?;
                        Ok(batch)
                    }.await;

                    match result {
                        Ok(batch) => yield Ok(replace_flight_data(None, &batch, 0)),
                        Err(e) => tracing::warn!(error = %e, "live_replace primary replace failed"),
                    }

                    // Re-execute auxiliary plans to refresh reference data.
                    for (idx, aux_bytes) in auxiliary_plan_bytes.iter().enumerate() {
                        let alias = auxiliary_signals.get(idx).map(|(_, a)| a.as_str());
                        let result: Result<arrow::record_batch::RecordBatch, QueryError> = async {
                            let aux_plan: Plan = Message::decode(&aux_bytes[..])
                                .map_err(|e| exec_err(format!("Aux plan decode: {}", e)))?;
                            let aux_extensions = Extensions::try_from(&aux_plan.extensions)
                                .map_err(|e| exec_err(format!("Aux extensions: {}", e)))?;
                            let rel = aux_plan.relations.first()
                                .and_then(|pr| pr.rel_type.as_ref())
                                .and_then(|rt| get_rel(rt))
                                .ok_or_else(|| exec_err("Aux plan has no relation"))?;
                            let ctx_state = ctx.state();
                            let consumer = DefaultSubstraitConsumer::new(&aux_extensions, &ctx_state);
                            let logical_plan = consumer.consume_rel(rel).await
                                .map_err(|e| exec_err(format!("Aux consume_rel: {}", e)))?;
                            let (_schema, batch) = execute_logical_plan_to_batch(&ctx, logical_plan).await?;
                            Ok(batch)
                        }.await;

                        match result {
                            Ok(batch) if batch.num_rows() > 0 => {
                                yield Ok(replace_flight_data(alias, &batch, 0));
                            }
                            Ok(_) => {}
                            Err(e) => tracing::warn!(error = %e, idx = idx, "live_replace aux replace failed"),
                        }
                    }
                }
                Some(fd) = hb_stream.next() => {
                    yield Ok(fd);
                }
            }
        }
    })
}

/// Check whether a `LogicalPlan` contains an `Aggregate` node anywhere in the tree.
fn plan_has_aggregation(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Aggregate(_))
        || plan.inputs().iter().any(|p| plan_has_aggregation(p))
}

/// Extract filter predicates from a `LogicalPlan`, combining multiple `Filter` nodes with AND.
///
/// Returns the combined predicate expression and an `Arc<DFSchema>` from the filter's input
/// (not the projected output schema, since predicates reference source table columns).
/// Returns `None` if the plan contains no `Filter` nodes.
/// Remove compile-time time-range upper bounds from a filter expression before it is used
/// as the live-path `primary_filter`.
///
/// The SeQL compiler emits a sliding-window filter as:
///   `CAST(time_col AS Int64) >= lit(start_ns) AND CAST(time_col AS Int64) <= lit(now_ns)`
///
/// `now_ns` is captured at query compile time.  Any batch arriving **after** that instant
/// will have `time_col > now_ns` and will be incorrectly rejected by the upper bound.
/// For live streaming there is no meaningful upper bound — new data is always "now" — so
/// we strip `<= lit(...)` predicates on the time column before the filter is compiled into
/// a `PhysicalExpr`.
fn strip_time_upper_bound(expr: Expr, time_col: Option<&str>) -> Expr {
    let Some(col_name) = time_col else {
        return expr;
    };

    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = strip_time_upper_bound(*binary.left, time_col);
            let right = strip_time_upper_bound(*binary.right, time_col);
            left.and(right)
        }
        Expr::BinaryExpr(ref binary) if binary.op == Operator::LtEq => {
            if is_cast_of_col(&binary.left, col_name) && matches!(*binary.right, Expr::Literal(..))
            {
                // Replace the upper-bound predicate with a tautology so the AND tree stays valid.
                Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)), None)
            } else {
                expr
            }
        }
        other => other,
    }
}

/// Returns true if `expr` is `CAST(<col_name> AS <any type>)` or just `Column(<col_name>)`.
fn is_cast_of_col(expr: &Expr, col_name: &str) -> bool {
    match expr {
        Expr::Cast(cast) => matches!(cast.expr.as_ref(), Expr::Column(col) if col.name == col_name),
        Expr::Column(col) => col.name == col_name,
        _ => false,
    }
}

fn extract_filter_predicates(plan: &LogicalPlan) -> Option<(Expr, Arc<DFSchema>)> {
    fn walk(plan: &LogicalPlan, acc: &mut Option<(Expr, Arc<DFSchema>)>) {
        if let LogicalPlan::Filter(filter) = plan {
            let schema = filter.input.schema().clone();
            let pred = filter.predicate.clone();
            *acc = Some(match acc.take() {
                Some((existing, s)) => (existing.and(pred), s),
                None => (pred, schema),
            });
        }
        for input in plan.inputs() {
            walk(input, acc);
        }
    }
    let mut combined = None;
    walk(plan, &mut combined);
    combined
}

/// Convert a logical `Expr` and its `DFSchema` into a `PhysicalExpr` for batch evaluation.
fn build_physical_filter(
    expr: &Expr,
    df_schema: &DFSchema,
) -> Result<Arc<dyn PhysicalExpr>, QueryError> {
    create_physical_expr(expr, df_schema, &ExecutionProps::new())
        .map_err(|e| exec_err(format!("Failed to build physical filter: {}", e)))
}

#[cfg(test)]
mod tests {
    use crate::datafusion_backend::DataFusionBackend;
    use crate::test_fixtures::{
        make_test_otlp_logs, make_test_otlp_logs_at, make_test_otlp_metrics, make_test_otlp_traces,
        make_test_otlp_traces_at, now_ns, TestStorageBuilder,
    };
    use arrow_flight::FlightData;
    use futures::StreamExt;
    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    use sequins_query::QueryApi;
    use sequins_types::ingest::OtlpIngest;
    use std::sync::Arc;
    use std::time::Duration;

    fn get_metadata(fd: &FlightData) -> SeqlMetadata {
        decode_metadata(&fd.app_metadata).expect("FlightData must have SeqlMetadata")
    }

    fn is_schema(fd: &FlightData) -> bool {
        matches!(get_metadata(fd), SeqlMetadata::Schema { .. })
    }

    fn is_data(fd: &FlightData) -> bool {
        matches!(get_metadata(fd), SeqlMetadata::Data { .. })
    }

    fn is_complete(fd: &FlightData) -> bool {
        matches!(get_metadata(fd), SeqlMetadata::Complete { .. })
    }

    fn is_append(fd: &FlightData) -> bool {
        matches!(get_metadata(fd), SeqlMetadata::Append { .. })
    }

    fn is_replace(fd: &FlightData) -> bool {
        matches!(get_metadata(fd), SeqlMetadata::Replace { .. })
    }

    fn is_heartbeat(fd: &FlightData) -> bool {
        matches!(get_metadata(fd), SeqlMetadata::Heartbeat { .. })
    }

    #[tokio::test]
    async fn test_query_execution_against_hot_tier() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let request = make_test_otlp_traces(1, 10);
        storage.ingest_traces(request).await.unwrap();

        let hot_tier = &storage.hot_tier;
        assert_eq!(hot_tier.spans.row_count(), 10);

        let backend = DataFusionBackend::new(Arc::new(storage));
        let query = "spans last 1h LIMIT 5";
        let mut stream = backend.query(query).await.unwrap();

        let mut frames: Vec<FlightData> = Vec::new();
        while let Some(result) = stream.next().await {
            frames.push(result.unwrap());
        }

        assert!(
            frames.len() >= 3,
            "Should have at least schema + data + complete frames"
        );
        assert!(is_schema(&frames[0]), "first frame must be Schema");
        assert!(is_data(&frames[1]), "second frame must be Data");
        assert!(is_complete(&frames[2]), "third frame must be Complete");
    }

    #[tokio::test]
    async fn test_query_execution_against_cold_tier() {
        let (storage, _temp) = TestStorageBuilder::new()
            .flush_interval(sequins_types::models::Duration::from_millis(100))
            .build()
            .await;

        let request = make_test_otlp_traces(1, 5);
        storage.ingest_traces(request).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let backend = DataFusionBackend::new(Arc::new(storage));
        let query = "spans last 1h LIMIT 10";
        let mut stream = backend.query(query).await.unwrap();

        let mut frame_count = 0;
        while let Some(result) = stream.next().await {
            result.unwrap();
            frame_count += 1;
        }

        assert!(frame_count >= 1, "Should have at least one frame");
    }

    #[tokio::test]
    async fn test_query_execution_with_filters() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let logs_request = make_test_otlp_logs(1, 20);
        storage.ingest_logs(logs_request).await.unwrap();

        let metrics_request = make_test_otlp_metrics(1, 3, 5);
        storage.ingest_metrics(metrics_request).await.unwrap();

        let backend = DataFusionBackend::new(Arc::new(storage));
        let query = "logs last 1h WHERE severity = 'INFO' LIMIT 10";
        let mut stream = backend.query(query).await.unwrap();

        let mut frames: Vec<FlightData> = Vec::new();
        while let Some(result) = stream.next().await {
            frames.push(result.unwrap());
        }

        assert!(!frames.is_empty(), "Should have frames from filtered query");

        // Complete frame should have execution stats
        if let Some(last) = frames.last() {
            if let SeqlMetadata::Complete { stats } = get_metadata(last) {
                assert!(stats.execution_time_us > 0, "Should have execution time");
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Live query tests
    // ─────────────────────────────────────────────────────────────────────────

    /// Receive the next FlightData from a stream, with a 2-second wall-clock timeout.
    async fn next_fd(stream: &mut sequins_query::SeqlStream) -> FlightData {
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("timeout waiting for next frame")
            .expect("stream ended unexpectedly")
            .expect("frame error")
    }

    /// Collect all historical frames (Schema + any Data) until the stream would block.
    async fn collect_historical(stream: &mut sequins_query::SeqlStream) -> Vec<FlightData> {
        let mut frames = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                Ok(Some(Ok(fd))) => frames.push(fd),
                Ok(Some(Err(e))) => panic!("frame error: {e}"),
                Ok(None) => break,
                Err(_timeout) => break,
            }
        }
        frames
    }

    #[tokio::test]
    async fn test_live_query_spans_emits_schema_and_data() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let request = make_test_otlp_traces(1, 5);
        storage.ingest_traces(request).await.unwrap();

        let backend = DataFusionBackend::new(Arc::new(storage));
        let mut stream = backend.query_live("spans last 1h").await.unwrap();

        let frames = collect_historical(&mut stream).await;

        assert!(!frames.is_empty(), "expected at least a Schema frame");
        assert!(
            is_schema(&frames[0]),
            "first frame must be Schema, got {:?}",
            get_metadata(&frames[0])
        );

        let data_count = frames.iter().filter(|f| is_data(f)).count();
        assert!(
            data_count >= 1,
            "expected at least one Data frame for historical spans"
        );
    }

    #[tokio::test]
    async fn test_live_query_empty_historical_still_streams() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let backend = DataFusionBackend::new(Arc::new(storage));
        let mut stream = backend.query_live("spans last 1h").await.unwrap();

        let frames = collect_historical(&mut stream).await;

        assert!(
            !frames.is_empty(),
            "should still emit Schema frame for empty storage"
        );
        assert!(is_schema(&frames[0]), "first frame must be Schema");

        let data_count = frames.iter().filter(|f| is_data(f)).count();
        assert_eq!(data_count, 0, "no Data frames expected for empty hot tier");
    }

    #[tokio::test]
    async fn test_live_query_spans_append_delta() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let storage = Arc::new(storage);

        let backend = DataFusionBackend::new(Arc::clone(&storage));
        let mut stream = backend.query_live("spans last 1h").await.unwrap();

        let _hist = collect_historical(&mut stream).await;

        let past_ns = now_ns().saturating_sub(30_000_000_000);
        let request = make_test_otlp_traces_at(1, 3, past_ns);
        storage.ingest_traces(request).await.unwrap();

        let fd = next_fd(&mut stream).await;

        if is_append(&fd) {
            // good
        } else if is_heartbeat(&fd) {
            let fd2 = next_fd(&mut stream).await;
            assert!(is_append(&fd2), "expected Append after Heartbeat");
        } else {
            panic!("expected Append frame, got {:?}", get_metadata(&fd));
        }
    }

    #[tokio::test]
    async fn test_live_query_resources_append_delta() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let storage = Arc::new(storage);

        let backend = DataFusionBackend::new(Arc::clone(&storage));
        let mut stream = backend.query_live("resources last 24h").await.unwrap();

        let _hist = collect_historical(&mut stream).await;

        storage
            .ingest_traces(make_test_otlp_traces(1, 1))
            .await
            .unwrap();

        let fd = next_fd(&mut stream).await;

        if is_append(&fd) {
            // good
        } else if is_heartbeat(&fd) {
            let fd2 = next_fd(&mut stream).await;
            assert!(is_append(&fd2), "expected Append after Heartbeat");
        } else {
            panic!(
                "expected Append frame for resources query, got {:?}",
                get_metadata(&fd)
            );
        }
    }

    #[tokio::test]
    async fn test_live_query_aggregated_replace_delta() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let storage = Arc::new(storage);

        storage
            .ingest_traces(make_test_otlp_traces(1, 5))
            .await
            .unwrap();

        let backend = DataFusionBackend::new(Arc::clone(&storage));
        let mut stream = backend
            .query_live("spans last 1h | group by { name } { count() as count }")
            .await
            .unwrap();

        let _hist = collect_historical(&mut stream).await;

        storage
            .ingest_traces(make_test_otlp_traces(1, 2))
            .await
            .unwrap();

        let mut found_replace = false;
        for _ in 0..5 {
            let fd = match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
                Ok(Some(Ok(f))) => f,
                _ => break,
            };
            if is_replace(&fd) {
                found_replace = true;
                break;
            }
            if is_heartbeat(&fd) {
                continue;
            }
            break;
        }
        assert!(
            found_replace,
            "expected Replace delta for aggregated live query"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_live_query_heartbeat_deterministic() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let backend = DataFusionBackend::new(Arc::new(storage));
        let mut stream = backend.query_live("spans last 1h").await.unwrap();

        let _hist = collect_historical(&mut stream).await;

        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::task::yield_now().await;

        let fd = match tokio::time::timeout(Duration::from_secs(1), stream.next()).await {
            Ok(Some(Ok(f))) => f,
            Ok(Some(Err(e))) => panic!("stream error: {e}"),
            Ok(None) => panic!("stream ended"),
            Err(_) => panic!("timeout: no Heartbeat after advance(6s)"),
        };

        assert!(
            is_heartbeat(&fd),
            "expected Heartbeat after 6s, got {:?}",
            get_metadata(&fd)
        );
    }

    /// Confirm that `body contains 'text'` actually filters live Append batches.
    ///
    /// The test fixture generates logs with body `"Test log message N"`.
    /// A query containing `'nomatch'` should yield 0 rows; `'message'` should pass all rows.
    #[tokio::test]
    async fn test_live_query_text_filter_correctly_applied() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;
        let storage = Arc::new(storage);

        // ── query that should match nothing ──────────────────────────────────
        let backend = DataFusionBackend::new(Arc::clone(&storage));
        let mut no_match_stream = backend
            .query_live("logs last 1h | where body contains 'nomatch'")
            .await
            .unwrap();
        let _hist = collect_historical(&mut no_match_stream).await;

        let past_ns = now_ns().saturating_sub(30_000_000_000);
        storage
            .ingest_logs(make_test_otlp_logs_at(1, 5, past_ns))
            .await
            .unwrap();

        // Drain a short window — no Append should arrive because none of the
        // 5 log bodies contain "nomatch".
        let mut append_rows = 0u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
        while std::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(50), no_match_stream.next())
                .await
            {
                Ok(Some(Ok(fd))) => {
                    if let SeqlMetadata::Append { .. } = get_metadata(&fd) {
                        // Count the rows decoded from the IPC body.
                        if !fd.data_body.is_empty() {
                            use arrow::ipc::reader::StreamReader;
                            let cursor = std::io::Cursor::new(&fd.data_body[..]);
                            if let Ok(reader) = StreamReader::try_new(cursor, None) {
                                for batch in reader.flatten() {
                                    append_rows += batch.num_rows() as u64;
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        assert_eq!(
            append_rows, 0,
            "body contains 'nomatch' should yield 0 live rows, got {}",
            append_rows
        );

        // ── query that should match all logs ─────────────────────────────────
        let backend2 = DataFusionBackend::new(Arc::clone(&storage));
        let mut match_stream = backend2
            .query_live("logs last 1h | where body contains 'message'")
            .await
            .unwrap();
        let _hist2 = collect_historical(&mut match_stream).await;

        storage
            .ingest_logs(make_test_otlp_logs_at(1, 5, now_ns()))
            .await
            .unwrap();

        let fd = next_fd(&mut match_stream).await;
        let meta = get_metadata(&fd);
        assert!(
            matches!(meta, SeqlMetadata::Append { .. }),
            "expected Append for matching search, got {:?}",
            meta
        );
    }
}
