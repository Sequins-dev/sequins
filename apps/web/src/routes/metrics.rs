//! Metrics tab — server-rendered SVG time-series charts.
//!
//! Single endpoint pattern (same as logs/traces/health). The data-effect on #main-area
//! in base.html manages the SSE lifecycle; auto-cancels old connection on re-fire.
//!
//! Live mode:  query_live → persistent SSE + tick-based data refresh
//! Paused mode: query → snapshot → render once → SSE closes

use async_stream::stream;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use datastar::axum::ReadSignals;
use datastar::consts::ElementPatchMode;
use datastar::patch_elements::PatchElements;
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::Infallible;

use crate::state::AppState;
use crate::stream::html_escape;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::frame::ipc_to_batch;
use sequins_query::QueryApi;

use arrow::array::{
    Array, Float64Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

#[derive(Deserialize, Default)]
pub struct MetricsSignals {
    #[serde(default)]
    pub service: String,
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default)]
    pub search: String,
    #[serde(default = "default_live")]
    pub live: bool,
}

fn default_duration() -> String {
    "5m".to_string()
}
fn default_live() -> bool {
    true
}

fn render_tab_controls(params: &MetricsSignals, bin_label: &str) -> String {
    let search_val = html_escape(&params.search);
    // Search input updates $search signal only — no @get — to avoid competing SSE streams.
    // The data-effect on #main-area is the single source of truth for starting new requests.
    format!(
        r#"<input type="text" class="px-2 py-1 text-xs border border-zinc-600 rounded bg-zinc-950 text-zinc-300 placeholder-zinc-500 focus:outline-none focus:border-blue-400 w-40 shrink-0" placeholder="Search metrics…" value="{search_val}" data-bind:search>
<div class="w-px h-4 bg-zinc-700 shrink-0 mx-0.5"></div>
<span class="text-xs text-zinc-500 whitespace-nowrap shrink-0">Bucket: {bin_label}</span>"#
    )
}

pub async fn handler(
    State(state): State<AppState>,
    ReadSignals(params): ReadSignals<MetricsSignals>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let backend = state.backend.clone();
    let tmpl = state.templates.clone();

    // Generation counter: increment on each new request so stale server-side loops
    // detect the change and terminate (same pattern as logs).
    let my_gen = {
        let mut val = 0u64;
        state.metrics_gen_tx.send_modify(|g| {
            *g += 1;
            val = *g;
        });
        val
    };
    let mut gen_rx = state.metrics_gen_tx.subscribe();

    let s = stream! {
        let dur_secs = duration_to_secs(&params.duration);
        let bin_secs = bin_size_secs(dur_secs);
        let bin_ns = bin_secs as i64 * 1_000_000_000;
        let bin_label = bin_size_label(bin_secs);
        let window_ns = dur_secs as i64 * 1_000_000_000;

        let page_html = match tmpl.render("partials/metrics.html", minijinja::context! {}) {
            Ok(h) => h,
            Err(e) => { tracing::error!("Metrics template error: {e}"); return; }
        };
        yield Ok(Event::from(
            PatchElements::new(page_html).selector("#content").mode(ElementPatchMode::Inner),
        ));
        yield Ok(Event::from(
            PatchElements::new(render_tab_controls(&params, &bin_label))
                .selector("#tab-controls")
                .mode(ElementPatchMode::Inner),
        ));

        let q = if params.service.is_empty() {
            format!(
                "metrics last {} | where metric_type != 'histogram' <- (datapoints | group by {{ ts() bin {} as bucket, metric_id }} {{ avg(value) as val }}) as datapoints",
                params.duration, bin_label
            )
        } else {
            format!(
                "metrics last {} | where resource_id = '{}' | where metric_type != 'histogram' <- (datapoints | group by {{ ts() bin {} as bucket, metric_id }} {{ avg(value) as val }}) as datapoints",
                params.duration, params.service.replace('\'', "''"), bin_label
            )
        };

        tracing::debug!("Metrics query (live={}): {q}", params.live);

        // State held across the lifetime of this SSE connection
        let mut metric_rows: HashMap<String, MetricRow> = HashMap::new();
        // metric_id → bucket_ns → avg_val
        let mut dp: HashMap<String, HashMap<i64, f64>> = HashMap::new();

        // Single query — execute_live Phase 1 delivers both metric descriptors (table=None)
        // and historical datapoints (table=Some("datapoints")) before the live stream begins.
        let raw_stream_result = if params.live {
            backend.query_live(&q).await
        } else {
            backend.query(&q).await
        };

        let mut raw_stream = match raw_stream_result {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Metrics query error: {e}");
                yield Ok(Event::from(
                    PatchElements::new(format!(r#"<div class="flex items-center justify-center h-24 text-red-400 text-sm">{}</div>"#, html_escape(&e.to_string())))
                        .selector("#metric-cards")
                        .mode(ElementPatchMode::Inner)
                ));
                return;
            }
        };

        let mut initial_rendered = false;

        if params.live {
            // Live mode: tick every 2s to refresh data and slide the x-axis.
            // Visual smoothness between ticks comes from client-side requestAnimationFrame.
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // skip the immediate first tick

            loop {
                tokio::select! {
                    biased;
                    // Newer request started — stop this stream.
                    _ = gen_rx.changed() => {
                        if *gen_rx.borrow_and_update() > my_gen { return; }
                    }

                    item = raw_stream.next() => {
                        let fd = match item {
                            Some(Ok(fd)) => fd,
                            Some(Err(e)) => { tracing::warn!("Metrics stream error: {e}"); continue; }
                            None => break,
                        };

                        if fd.data_body.is_empty() {
                            let meta = match decode_metadata(&fd.app_metadata) { Some(m) => m, None => continue };
                            match meta {
                                SeqlMetadata::Heartbeat { .. } if !initial_rendered => {
                                    initial_rendered = true;
                                    let cutoff = now_cutoff_ns(window_ns);
                                    let html = render_all_cards_html(&metric_rows, &dp, cutoff, window_ns, &params);
                                    yield Ok(Event::from(
                                        PatchElements::new(html).selector("#metric-cards").mode(ElementPatchMode::Inner),
                                    ));
                                }
                                SeqlMetadata::Complete { .. } => {
                                    if !initial_rendered {
                                        let cutoff = now_cutoff_ns(window_ns);
                                        let html = render_all_cards_html(&metric_rows, &dp, cutoff, window_ns, &params);
                                        yield Ok(Event::from(
                                            PatchElements::new(html).selector("#metric-cards").mode(ElementPatchMode::Inner),
                                        ));
                                    }
                                    break;
                                }
                                _ => {}
                            }
                            continue;
                        }

                        let meta = match decode_metadata(&fd.app_metadata) { Some(m) => m, None => continue };
                        let batch = match ipc_to_batch(&fd.data_body) {
                            Ok(b) => b,
                            Err(e) => { tracing::warn!("Metrics IPC decode: {e}"); continue; }
                        };

                        let (table, is_replace) = match &meta {
                            SeqlMetadata::Data { table } => (table.clone(), false),
                            SeqlMetadata::Append { table, .. } => (table.clone(), false),
                            SeqlMetadata::Replace { table, .. } => (table.clone(), true),
                            SeqlMetadata::Update { table, .. } => (table.clone(), false),
                            _ => continue,
                        };

                        if table.is_none() {
                            collect_metric_rows(&batch, &mut metric_rows);
                        } else {
                            let affected = apply_datapoints(&batch, &mut dp, is_replace, bin_ns);
                            if initial_rendered {
                                let cutoff = now_cutoff_ns(window_ns);
                                for metric_id in &affected {
                                    if let Some(row) = metric_rows.get(metric_id) {
                                        let pts = metric_points(&dp, metric_id, cutoff);
                                        let safe = safe_metric_id(metric_id);
                                        let now_sec = now_sec();
                                        let svg = render_line_chart(&pts, &row.metric_type, &row.unit, window_ns, &safe, now_sec);
                                        yield Ok(Event::from(
                                            PatchElements::new(svg)
                                                .selector(format!("#metric-chart-{safe}"))
                                                .mode(ElementPatchMode::Inner),
                                        ));
                                    }
                                }
                            }
                        }

                        // Render as soon as we have both metric descriptors and datapoints
                        // from execute_live's historical Phase 1 — don't wait for Heartbeat.
                        if !initial_rendered && !metric_rows.is_empty() && !dp.is_empty() {
                            initial_rendered = true;
                            let cutoff = now_cutoff_ns(window_ns);
                            let html = render_all_cards_html(&metric_rows, &dp, cutoff, window_ns, &params);
                            yield Ok(Event::from(
                                PatchElements::new(html).selector("#metric-cards").mode(ElementPatchMode::Inner),
                            ));
                        }
                    }

                    _ = tick.tick() => {
                        if initial_rendered {
                            let cutoff = now_cutoff_ns(window_ns);
                            // Prune buckets outside the window
                            for buckets in dp.values_mut() {
                                buckets.retain(|&t, _| t >= cutoff);
                            }
                            // Re-render all SVGs with fresh render time (client JS picks up new
                            // data-render-time and resets the slide animation from 0).
                            let now_s = now_sec();
                            for (metric_id, row) in &metric_rows {
                                let pts = metric_points(&dp, metric_id, cutoff);
                                let safe = safe_metric_id(metric_id);
                                let svg = render_line_chart(&pts, &row.metric_type, &row.unit, window_ns, &safe, now_s);
                                yield Ok(Event::from(
                                    PatchElements::new(svg)
                                        .selector(format!("#metric-chart-{safe}"))
                                        .mode(ElementPatchMode::Inner),
                                ));
                            }
                        }
                    }
                }
            }
        } else {
            // Paused mode: consume snapshot, render once, done.
            while let Some(item) = raw_stream.next().await {
                let fd = match item {
                    Ok(fd) => fd,
                    Err(e) => { tracing::warn!("Metrics snapshot error: {e}"); continue; }
                    };
                if fd.data_body.is_empty() {
                    let meta = match decode_metadata(&fd.app_metadata) { Some(m) => m, None => continue };
                    if let SeqlMetadata::Complete { .. } = meta {
                        let cutoff = now_cutoff_ns(window_ns);
                        let html = render_all_cards_html(&metric_rows, &dp, cutoff, window_ns, &params);
                        yield Ok(Event::from(
                            PatchElements::new(html).selector("#metric-cards").mode(ElementPatchMode::Inner),
                        ));
                        break;
                    }
                    continue;
                }
                let meta = match decode_metadata(&fd.app_metadata) { Some(m) => m, None => continue };
                let batch = match ipc_to_batch(&fd.data_body) {
                    Ok(b) => b,
                    Err(e) => { tracing::warn!("Metrics IPC decode: {e}"); continue; }
                };
                match &meta {
                    SeqlMetadata::Data { table } if table.is_none() => {
                        collect_metric_rows(&batch, &mut metric_rows);
                    }
                    SeqlMetadata::Data { .. } => {
                        apply_datapoints(&batch, &mut dp, false, bin_ns);
                    }
                    _ => {}
                }
            }
        }
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}

// ---------------------------------------------------------------------------
// State helpers
// ---------------------------------------------------------------------------

struct MetricRow {
    name: String,
    unit: String,
    metric_type: String,
}

fn now_cutoff_ns(window_ns: i64) -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
        - window_ns
}

fn now_sec() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn metric_points(
    dp: &HashMap<String, HashMap<i64, f64>>,
    metric_id: &str,
    cutoff_ns: i64,
) -> Vec<(i64, f64)> {
    let Some(buckets) = dp.get(metric_id) else {
        return vec![];
    };
    let mut pts: Vec<(i64, f64)> = buckets
        .iter()
        .filter(|(&t, _)| t >= cutoff_ns)
        .map(|(&t, &v)| (t, v))
        .collect();
    pts.sort_by_key(|&(t, _)| t);
    pts
}

fn safe_metric_id(id: &str) -> String {
    id.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn collect_metric_rows(batch: &RecordBatch, out: &mut HashMap<String, MetricRow>) {
    for row in 0..batch.num_rows() {
        let metric_id = match col_str(batch, "metric_id", row) {
            Some(id) => id,
            None => continue,
        };
        let name = col_str(batch, "name", row)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| metric_id.clone());
        let unit = col_str(batch, "unit", row).unwrap_or_default();
        let metric_type = col_str(batch, "metric_type", row).unwrap_or_default();
        out.insert(
            metric_id,
            MetricRow {
                name,
                unit,
                metric_type,
            },
        );
    }
}

fn col_to_ns(col: &dyn Array, row: usize) -> Option<i64> {
    if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Some(a.value(row) * 1_000);
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Some(a.value(row) * 1_000_000);
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampSecondArray>() {
        return Some(a.value(row) * 1_000_000_000);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row));
    }
    None
}

fn apply_datapoints(
    batch: &RecordBatch,
    dp: &mut HashMap<String, HashMap<i64, f64>>,
    replace_affected: bool,
    bin_ns: i64,
) -> Vec<String> {
    let fields = batch.schema();
    let metric_id_idx = fields.fields().iter().position(|f| f.name() == "metric_id");

    let aggregated = fields.fields().iter().any(|f| f.name() == "val");

    let (val_idx, time_idx) = if aggregated {
        let v = fields.fields().iter().position(|f| f.name() == "val");
        let t = fields.fields().iter().position(|f| f.name() == "bucket");
        (v, t)
    } else {
        let v = fields.fields().iter().position(|f| f.name() == "value");
        let t = fields
            .fields()
            .iter()
            .position(|f| f.name() == "time_unix_nano");
        (v, t)
    };

    let (Some(val_idx), Some(time_idx)) = (val_idx, time_idx) else {
        tracing::warn!(
            "Datapoints frame missing val/bucket or value/time_unix_nano. Schema: {:?}",
            fields
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        );
        return vec![];
    };

    let val_arr = match batch
        .column(val_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
    {
        Some(a) => a,
        None => return vec![],
    };
    let time_col = batch.column(time_idx);

    if replace_affected {
        let mut to_clear: std::collections::HashSet<String> = std::collections::HashSet::new();
        for row in 0..batch.num_rows() {
            if let Some(id) = extract_metric_id(batch, metric_id_idx, row) {
                to_clear.insert(id);
            }
        }
        for id in &to_clear {
            dp.remove(id);
        }
    }

    let mut affected: Vec<String> = Vec::new();
    for row in 0..batch.num_rows() {
        if val_arr.is_null(row) {
            continue;
        }
        let metric_id = match extract_metric_id(batch, metric_id_idx, row) {
            Some(id) => id,
            None => continue,
        };
        let val = val_arr.value(row);
        let time_ns = match col_to_ns(time_col.as_ref(), row) {
            Some(t) => t,
            None => continue,
        };
        let bucket_ns = if aggregated {
            time_ns
        } else {
            (time_ns / bin_ns) * bin_ns
        };
        let buckets = dp.entry(metric_id.clone()).or_default();
        buckets.insert(bucket_ns, val);
        if !affected.contains(&metric_id) {
            affected.push(metric_id);
        }
    }
    affected
}

fn extract_metric_id(batch: &RecordBatch, idx: Option<usize>, row: usize) -> Option<String> {
    col_str(batch, batch.schema().field(idx?).name(), row)
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Build one big HTML string for all matching metric cards, then send as a
/// single Inner-mode PatchElements. This clears any previous content (including
/// the loading spinner or stale cards from a prior stream).
fn render_all_cards_html(
    metric_rows: &HashMap<String, MetricRow>,
    dp: &HashMap<String, HashMap<i64, f64>>,
    cutoff_ns: i64,
    window_ns: i64,
    params: &MetricsSignals,
) -> String {
    let mut sorted_ids: Vec<&String> = metric_rows.keys().collect();
    sorted_ids.sort_by_key(|id| metric_rows[*id].name.as_str());

    let now_s = now_sec();
    let mut html = String::new();
    let mut count = 0usize;

    for metric_id in sorted_ids {
        let row = &metric_rows[metric_id];
        if !params.search.is_empty()
            && !row
                .name
                .to_lowercase()
                .contains(&params.search.to_lowercase())
        {
            continue;
        }
        let pts = metric_points(dp, metric_id, cutoff_ns);
        html.push_str(&render_metric_card(
            metric_id,
            &row.name,
            &row.unit,
            &row.metric_type,
            &pts,
            window_ns,
            now_s,
        ));
        count += 1;
    }

    if count == 0 {
        html.push_str(r#"<div class="flex items-center justify-center h-24 text-zinc-500 text-sm">No metrics found</div>"#);
    }

    html
}

fn render_metric_card(
    metric_id: &str,
    name: &str,
    unit: &str,
    metric_type: &str,
    points: &[(i64, f64)],
    window_ns: i64,
    now_sec: f64,
) -> String {
    let safe = safe_metric_id(metric_id);
    let svg = render_line_chart(points, metric_type, unit, window_ns, &safe, now_sec);
    format!(
        r#"<article class="metric-card" id="metric-card-{safe}">
  <header class="metric-card-header">
    <span class="metric-card-name">{name}</span>
    <span class="metric-type-badge">{metric_type}</span>
    {unit_span}
  </header>
  <div class="metric-chart" id="metric-chart-{safe}">{svg}</div>
</article>"#,
        safe = safe,
        name = html_escape(name),
        metric_type = html_escape(metric_type),
        unit_span = if unit.is_empty() {
            String::new()
        } else {
            format!(r#"<span class="metric-unit">{}</span>"#, html_escape(unit))
        },
        svg = svg,
    )
}

fn render_line_chart(
    points: &[(i64, f64)],
    metric_type: &str,
    unit: &str,
    window_ns: i64,
    safe_id: &str,
    render_time_sec: f64,
) -> String {
    const W: f64 = 400.0;
    const H: f64 = 120.0;
    const PAD_L: f64 = 38.0;
    const PAD_R: f64 = 6.0;
    const PAD_T: f64 = 6.0;
    const PAD_B: f64 = 16.0;
    let chart_w = W - PAD_L - PAD_R;
    let chart_h = H - PAD_T - PAD_B;

    let now_ns = (render_time_sec * 1e9) as i64;
    let window_start_ns = now_ns - window_ns;
    let window_sec = window_ns as f64 / 1e9;
    // Pixels per second for client-side animation
    let slide_rate = chart_w / window_sec;

    let clip_id = format!("clip-{safe_id}");

    if points.is_empty() {
        return format!(
            r##"<svg viewBox="0 0 {W} {H}" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:{H}px;display:block" data-pts="[]">
  <text x="{cx}" y="{cy}" text-anchor="middle" fill="#555" font-size="11">No data</text>
</svg>"##,
            cx = W / 2.0,
            cy = H / 2.0,
        );
    }

    let min_t = window_start_ns;
    let max_t = now_ns;
    let min_v = points
        .iter()
        .map(|&(_, v)| v)
        .fold(f64::INFINITY, f64::min)
        .min(0.0);
    let max_v = points
        .iter()
        .map(|&(_, v)| v)
        .fold(f64::NEG_INFINITY, f64::max);
    let max_v = if (max_v - min_v).abs() < 1e-10 {
        min_v + 1.0
    } else {
        max_v
    };

    let t_range = (max_t - min_t).max(1) as f64;
    let v_range = (max_v - min_v).max(1e-10);

    let sx = |t: i64| -> f64 { PAD_L + (t - min_t) as f64 / t_range * chart_w };
    let sy = |v: f64| -> f64 { PAD_T + (1.0 - (v - min_v) / v_range) * chart_h };

    let poly_pts: String = points
        .iter()
        .map(|&(t, v)| format!("{:.1},{:.1}", sx(t), sy(v)))
        .collect::<Vec<_>>()
        .join(" ");

    // data-pts: [[svg_x, svg_y, value, timestamp_sec], ...] for JS tooltip + hover dot
    let data_pts: String = {
        let entries: Vec<String> = points
            .iter()
            .map(|&(t, v)| format!("[{:.1},{:.1},{},{}]", sx(t), sy(v), v, t / 1_000_000_000))
            .collect();
        format!("[{}]", entries.join(","))
    };
    let data_unit = html_escape(unit);

    let color = if metric_type == "counter" {
        "#74c7ec"
    } else {
        "#a6e3a1"
    };

    let first_x = sx(points[0].0);
    let last_x = sx(points.last().unwrap().0);
    let bottom = sy(min_v.min(0.0));
    let area_path = format!(
        "M{:.1},{:.1} {} L{:.1},{:.1} L{:.1},{:.1} Z",
        first_x,
        sy(points[0].1),
        poly_pts,
        last_x,
        bottom,
        first_x,
        bottom
    );

    let y_top = format_value(max_v);
    let y_mid = format_value((max_v + min_v) / 2.0);
    let y_bot = format_value(min_v);
    let mid_y = PAD_T + chart_h / 2.0;

    format!(
        r##"<svg viewBox="0 0 {W} {H}" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg"
  style="width:100%;height:{H}px;display:block;cursor:crosshair"
  data-pts='{data_pts}' data-unit="{data_unit}"
  data-render-time="{render_time_sec:.3}" data-slide-rate="{slide_rate:.4}">
  <defs>
    <clipPath id="{clip_id}">
      <rect x="{pl}" y="{pt}" width="{cw}" height="{ch}"/>
    </clipPath>
  </defs>
  <line x1="{pl}" y1="{pt}" x2="{pr}" y2="{pt}" stroke="#2a2a2a" stroke-width="1"/>
  <line x1="{pl}" y1="{mid_y:.1}" x2="{pr}" y2="{mid_y:.1}" stroke="#2a2a2a" stroke-width="1"/>
  <line x1="{pl}" y1="{pb}" x2="{pr}" y2="{pb}" stroke="#2a2a2a" stroke-width="1"/>
  <line x1="{pl}" y1="{pt}" x2="{pl}" y2="{pb}" stroke="#444" stroke-width="1"/>
  <text x="{label_x:.1}" y="{pt_text:.1}" text-anchor="end" fill="#666" font-size="9" font-family="monospace">{y_top}</text>
  <text x="{label_x:.1}" y="{mid_text:.1}" text-anchor="end" fill="#666" font-size="9" font-family="monospace">{y_mid}</text>
  <text x="{label_x:.1}" y="{pb_text:.1}" text-anchor="end" fill="#666" font-size="9" font-family="monospace">{y_bot}</text>
  <g clip-path="url(#{clip_id})" class="chart-slide">
    <path d="{area_path}" fill="{color}" fill-opacity="0.15"/>
    <polyline points="{poly_pts}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>
  </g>
  <circle class="hover-dot" cx="-100" cy="-100" r="3.5" fill="white" stroke="{color}" stroke-width="1.5" pointer-events="none"/>
  <rect x="{pl}" y="{pt}" width="{cw}" height="{ch}" fill="transparent" pointer-events="all"/>
</svg>"##,
        W = W,
        H = H,
        clip_id = clip_id,
        color = color,
        pl = PAD_L,
        pt = PAD_T,
        pr = PAD_L + chart_w,
        pb = PAD_T + chart_h,
        mid_y = mid_y,
        poly_pts = poly_pts,
        area_path = area_path,
        label_x = PAD_L - 2.0,
        pt_text = PAD_T + 4.0,
        mid_text = mid_y + 3.0,
        pb_text = PAD_T + chart_h + 3.0,
        y_top = y_top,
        y_mid = y_mid,
        y_bot = y_bot,
        cw = chart_w,
        ch = chart_h,
        data_pts = data_pts,
        data_unit = data_unit,
        render_time_sec = render_time_sec,
        slide_rate = slide_rate,
    )
}

fn format_value(v: f64) -> String {
    if v.abs() >= 1_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if v.abs() >= 1_000.0 {
        format!("{:.1}k", v / 1_000.0)
    } else if v.fract() == 0.0 {
        format!("{:.0}", v)
    } else {
        format!("{:.2}", v)
    }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn col_str(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    use arrow::array::LargeStringArray;

    let col = batch.column_by_name(name)?;
    if col.is_null(row) {
        return None;
    }

    if let DataType::Dictionary(_, value_type) = col.data_type() {
        match value_type.as_ref() {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                let cast = arrow::compute::cast(col, &DataType::LargeUtf8).ok()?;
                let arr = cast.as_any().downcast_ref::<LargeStringArray>()?;
                if arr.is_null(row) {
                    return None;
                }
                return Some(arr.value(row).to_string());
            }
            _ => {}
        }
    }

    if let Some(a) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = col
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
    {
        return Some(a.value(row).to_string());
    }
    None
}

fn duration_to_secs(dur: &str) -> u64 {
    let dur = dur.trim();
    if let Some(n) = dur.strip_suffix('s') {
        n.parse().unwrap_or(300)
    } else if let Some(n) = dur.strip_suffix('m') {
        n.parse::<u64>().unwrap_or(5) * 60
    } else if let Some(n) = dur.strip_suffix('h') {
        n.parse::<u64>().unwrap_or(1) * 3600
    } else if let Some(n) = dur.strip_suffix('d') {
        n.parse::<u64>().unwrap_or(1) * 86400
    } else {
        300
    }
}

/// Snap to nice bin interval targeting ~20 data points per window (duration/20, min 5s).
fn bin_size_secs(duration_secs: u64) -> u64 {
    let target = ((duration_secs as f64 / 20.0).max(5.0)) as i64;
    const NICE: &[u64] = &[
        1, 2, 3, 5, 10, 15, 20, 30, 60, 90, 120, 180, 240, 300, 600, 900, 1200, 1800, 3600,
    ];
    *NICE
        .iter()
        .min_by_key(|&&n| (n as i64 - target).abs())
        .unwrap_or(&3600)
}

fn bin_size_label(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        format!("{}h", secs / 3600)
    }
}
