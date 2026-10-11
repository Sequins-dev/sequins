//! Traces tab — 4-pane layout matching the macOS app.
//!
//! Top: waterfall for the selected trace.
//! Bottom-left: trace list. Bottom-center: trace info. Bottom-right: span details.

use async_stream::stream;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use datastar::axum::ReadSignals;
use datastar::consts::ElementPatchMode;
use datastar::patch_elements::PatchElements;
use datastar::patch_signals::PatchSignals;
use futures::StreamExt;
use sequins_query::QueryApi;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::Infallible;

use crate::state::AppState;
use crate::stream::html_escape;
use sequins_view::{TableStrategy, ViewDelta, ViewStrategy};

#[derive(Deserialize, Default)]
pub struct TraceSignals {
    #[serde(default)]
    pub service: String,
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default)]
    pub search: String,
    #[serde(default = "default_sort")]
    pub sort: String,
    #[serde(default, rename = "errorsOnly")]
    pub errors_only: bool,
    #[serde(default, rename = "traceId")]
    pub trace_id: String,
    #[serde(default, rename = "spanId")]
    pub _span_id: String,
}

fn default_duration() -> String {
    "5m".to_string()
}
fn default_sort() -> String {
    "time".to_string()
}

fn render_tab_controls(params: &TraceSignals) -> String {
    let search_val = html_escape(&params.search);
    let sort = &params.sort;
    let errors_active = if params.errors_only { " active" } else { "" };
    let time_active = if sort == "time" { " active" } else { "" };
    let dur_active = if sort == "duration" { " active" } else { "" };
    let svc_active = if sort == "service" { " active" } else { "" };
    let pill = r#"inline-flex items-center px-2.5 py-1 text-xs border border-zinc-600 rounded bg-transparent text-zinc-300 hover:border-zinc-400 cursor-pointer whitespace-nowrap shrink-0"#;
    format!(
        r#"<input type="text" class="px-2 py-1 text-xs border border-zinc-600 rounded bg-zinc-950 text-zinc-300 placeholder-zinc-500 focus:outline-none focus:border-blue-400 w-40 shrink-0" placeholder="Search traces…" value="{search_val}" data-bind:search data-on:input__debounce.300ms="@get('/traces/view', {{retry: 'never'}})">
<div class="w-px h-4 bg-zinc-700 shrink-0 mx-0.5"></div>
<button class="filter-pill{time_active} {pill}" data-class:active="$sort==='time'" data-on:click="$sort='time'; @get('/traces/view', {{retry: 'never'}})">Start Time</button>
<button class="filter-pill{dur_active} {pill}" data-class:active="$sort==='duration'" data-on:click="$sort='duration'; @get('/traces/view', {{retry: 'never'}})">Duration</button>
<button class="filter-pill{svc_active} {pill}" data-class:active="$sort==='service'" data-on:click="$sort='service'; @get('/traces/view', {{retry: 'never'}})">Service</button>
<div class="w-px h-4 bg-zinc-700 shrink-0 mx-0.5"></div>
<button class="filter-pill{errors_active} {pill}" data-class:active="$errorsOnly" data-on:click="$errorsOnly=!$errorsOnly; @get('/traces/view', {{retry: 'never'}})">Errors only</button>"#
    )
}

pub async fn handler(
    State(state): State<AppState>,
    ReadSignals(params): ReadSignals<TraceSignals>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let backend = state.backend.clone();
    let tmpl = state.templates.clone();

    let s = stream! {
        // Render layout skeleton
        let page_html = match tmpl.render("partials/traces.html", minijinja::context! {}) {
            Ok(h) => h,
            Err(e) => { tracing::error!("Traces template error: {e}"); return; }
        };
        yield Ok(Event::from(
            PatchElements::new(page_html).selector("#content").mode(ElementPatchMode::Inner),
        ));
        yield Ok(Event::from(
            PatchElements::new(render_tab_controls(&params))
                .selector("#tab-controls")
                .mode(ElementPatchMode::Inner),
        ));
        yield Ok(Event::from(PatchSignals::new(r#"{"loading": true}"#)));

        // Build query
        let mut q = format!("spans last {}", params.duration);
        if !params.service.is_empty() {
            q.push_str(&format!(" | where resource_id = '{}'", params.service));
        }
        if params.errors_only {
            q.push_str(" | where status = 2");
        }
        if !params.search.is_empty() {
            q.push_str(&format!(
                " | where name contains '{}'",
                params.search.replace('\'', "''")
            ));
        }
        q.push_str(" | take 2000");

        let raw_stream = match backend.query(&q).await {
            Ok(s) => s,
            Err(e) => {
                yield Ok(error_event("#trace-list", &e.to_string()));
                yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                return;
            }
        };

        let strategy = TableStrategy::new();
        let mut delta_stream = strategy.transform(raw_stream).await;
        let mut all_spans: Vec<SpanRow> = Vec::new();

        while let Some(delta) = delta_stream.next().await {
            match delta {
                ViewDelta::RowsAppended { ipc, .. } => {
                    if let Ok(objects) = crate::query::ipc_to_objects(&ipc) {
                        for obj in objects {
                            if let Some(span) = parse_span_row(&obj) {
                                all_spans.push(span);
                            }
                        }
                    }
                }
                ViewDelta::Error { message } => {
                    yield Ok(error_event("#trace-list", &message));
                    yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                    return;
                }
                _ => {}
            }
        }

        // Sort and determine initial selection (first trace if none already valid)
        let trace_ids = sorted_trace_ids(&all_spans, &params.sort);
        let initial_trace_id = if !params.trace_id.is_empty() && trace_ids.contains(&params.trace_id) {
            params.trace_id.clone()
        } else {
            trace_ids.first().cloned().unwrap_or_default()
        };

        // Build per-trace span map once
        let mut trace_map: HashMap<&str, Vec<&SpanRow>> = HashMap::new();
        for s in &all_spans {
            trace_map.entry(s.trace_id.as_str()).or_default().push(s);
        }

        // Find root span of initial trace for auto-selection
        let initial_span_id = if !initial_trace_id.is_empty() {
            if let Some(ss) = trace_map.get(initial_trace_id.as_str()) {
                let span_ids: std::collections::HashSet<&str> = ss.iter().map(|s| s.span_id.as_str()).collect();
                ss.iter()
                    .filter(|s| s.parent_span_id.as_deref().map(|p| !span_ids.contains(p)).unwrap_or(true))
                    .min_by_key(|s| s.start_ns)
                    .or_else(|| ss.iter().min_by_key(|s| s.start_ns))
                    .map(|s| s.span_id.clone())
                    .unwrap_or_default()
            } else {
                String::new()
            }
        } else {
            String::new()
        };

        // Trace list (reactive selection via data-class, no @get on click)
        yield Ok(Event::from(
            PatchElements::new(render_trace_list(&all_spans, &trace_ids, &trace_map))
                .selector("#trace-list")
                .mode(ElementPatchMode::Inner),
        ));

        // All waterfalls — one per trace, toggled by $traceId
        yield Ok(Event::from(
            PatchElements::new(render_all_waterfalls(&trace_ids, &trace_map))
                .selector("#trace-waterfall")
                .mode(ElementPatchMode::Inner),
        ));

        // All trace infos — one per trace, toggled by $traceId
        yield Ok(Event::from(
            PatchElements::new(render_all_trace_infos(&trace_ids, &trace_map))
                .selector("#trace-info")
                .mode(ElementPatchMode::Inner),
        ));

        // All span details — flat across all traces, toggled by $spanId
        yield Ok(Event::from(
            PatchElements::new(render_all_span_details_flat(&all_spans, &trace_map))
                .selector("#span-details")
                .mode(ElementPatchMode::Inner),
        ));

        // Set initial signal values so the right panes light up immediately
        let init_json = format!(
            r#"{{"loading":false,"traceId":"{}","spanId":"{}"}}"#,
            initial_trace_id.replace('"', "\\\""),
            initial_span_id.replace('"', "\\\""),
        );
        yield Ok(Event::from(PatchSignals::new(init_json)));
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

struct SpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    service_name: String,
    start_ns: i64,
    end_ns: i64,
    status: i64,
    span_kind: i64,
    attributes: Vec<(String, String)>,
}

fn format_attr_value(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Array(arr) => Some(
            arr.iter()
                .filter_map(format_attr_value)
                .collect::<Vec<_>>()
                .join(", "),
        ),
        serde_json::Value::Object(_) => None, // skip nested objects
    }
}

fn parse_span_row(obj: &serde_json::Map<String, serde_json::Value>) -> Option<SpanRow> {
    let trace_id = obj
        .get("trace_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if trace_id.is_empty() {
        return None;
    }

    // Collect promoted attr_* columns
    let mut attributes: Vec<(String, String)> = obj
        .iter()
        .filter_map(|(k, v)| {
            let name = k.strip_prefix("attr_")?;
            let s = format_attr_value(v)?;
            Some((name.to_string(), s))
        })
        .collect();

    // Collect overflow attributes (_overflow_attrs is now a JSON object)
    if let Some(serde_json::Value::Object(overflow)) = obj.get("_overflow_attrs") {
        for (k, v) in overflow {
            if let Some(s) = format_attr_value(v) {
                attributes.push((k.clone(), s));
            }
        }
    }

    attributes.sort_by(|a, b| a.0.cmp(&b.0));

    Some(SpanRow {
        trace_id,
        span_id: obj
            .get("span_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        parent_span_id: obj
            .get("parent_span_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()),
        name: obj
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        service_name: obj
            .get("service_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        start_ns: obj
            .get("start_time_unix_nano")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        end_ns: obj
            .get("end_time_unix_nano")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        status: obj.get("status_code").and_then(|v| v.as_i64()).unwrap_or(0),
        span_kind: obj.get("span_kind").and_then(|v| v.as_i64()).unwrap_or(0),
        attributes,
    })
}

// ---------------------------------------------------------------------------
// Sorting / grouping helpers
// ---------------------------------------------------------------------------

fn sorted_trace_ids(spans: &[SpanRow], sort: &str) -> Vec<String> {
    let mut trace_map: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, s) in spans.iter().enumerate() {
        trace_map.entry(s.trace_id.clone()).or_default().push(i);
    }
    let mut ids: Vec<String> = trace_map.keys().cloned().collect();
    match sort {
        "duration" => ids.sort_by(|a, b| {
            let dur = |id: &str| -> i64 {
                let indices = &trace_map[id];
                let start = indices
                    .iter()
                    .map(|&i| spans[i].start_ns)
                    .min()
                    .unwrap_or(0);
                let end = indices.iter().map(|&i| spans[i].end_ns).max().unwrap_or(0);
                end - start
            };
            dur(b).cmp(&dur(a))
        }),
        "service" => ids.sort_by(|a, b| {
            let svc = |id: &str| {
                trace_map[id]
                    .first()
                    .map(|&i| spans[i].service_name.as_str())
                    .unwrap_or("")
            };
            svc(a).cmp(svc(b))
        }),
        _ => ids.sort_by(|a, b| {
            let start = |id: &str| {
                trace_map[id]
                    .iter()
                    .map(|&i| spans[i].start_ns)
                    .min()
                    .unwrap_or(0)
            };
            start(b).cmp(&start(a))
        }),
    }
    ids
}

// ---------------------------------------------------------------------------
// Trace list panel
// ---------------------------------------------------------------------------

fn render_trace_list(
    spans: &[SpanRow],
    trace_ids: &[String],
    trace_map: &HashMap<&str, Vec<&SpanRow>>,
) -> String {
    if spans.is_empty() {
        return r#"<div class="flex items-center justify-center flex-1 text-zinc-500 text-xs">No traces found</div>"#.to_string();
    }

    // Stats header
    let total = trace_ids.len();
    let errors: usize = trace_ids
        .iter()
        .filter(|id| {
            trace_map
                .get(id.as_str())
                .map(|ss| ss.iter().any(|s| s.status == 2))
                .unwrap_or(false)
        })
        .count();
    let avg_dur_ms: i64 = if total > 0 {
        trace_ids
            .iter()
            .map(|id| {
                let ss = &trace_map[id.as_str()];
                let start = ss.iter().map(|s| s.start_ns).min().unwrap_or(0);
                let end = ss.iter().map(|s| s.end_ns).max().unwrap_or(0);
                (end - start) / 1_000_000
            })
            .sum::<i64>()
            / total as i64
    } else {
        0
    };

    let error_color = if errors > 0 {
        "color:#f87171"
    } else {
        "color:#a1a1aa"
    };
    let mut html = format!(
        r#"<div class="flex items-center justify-around px-2 py-2 border-b border-zinc-700 bg-zinc-900 shrink-0 text-center">
  <div><div class="text-sm font-bold text-zinc-100 font-mono">{total}</div><div class="text-[10px] text-zinc-500">traces</div></div>
  <div><div class="text-sm font-bold font-mono" style="{error_color}">{errors}</div><div class="text-[10px] text-zinc-500">errors</div></div>
  <div><div class="text-sm font-bold text-zinc-100 font-mono">{avg_dur_ms}ms</div><div class="text-[10px] text-zinc-500">avg dur</div></div>
</div>
<div class="flex-1 overflow-y-auto">"#,
    );

    for trace_id in trace_ids.iter().take(200) {
        let ss = match trace_map.get(trace_id.as_str()) {
            Some(v) => v,
            None => continue,
        };
        let trace_start = ss.iter().map(|s| s.start_ns).min().unwrap_or(0);
        let trace_end = ss.iter().map(|s| s.end_ns).max().unwrap_or(0);
        let dur_ms = (trace_end - trace_start) / 1_000_000;
        let has_error = ss.iter().any(|s| s.status == 2);
        let dur_color = if dur_ms >= 1000 {
            "color:#fb923c"
        } else {
            "color:#a1a1aa"
        };
        let health_color = if has_error { "#f87171" } else { "#4ade80" };

        let span_ids: std::collections::HashSet<&str> =
            ss.iter().map(|s| s.span_id.as_str()).collect();
        let root = ss
            .iter()
            .filter(|s| {
                s.parent_span_id
                    .as_deref()
                    .map(|p| !span_ids.contains(p))
                    .unwrap_or(true)
            })
            .min_by_key(|s| s.start_ns)
            .or_else(|| ss.iter().min_by_key(|s| s.start_ns));

        let op_name = root.map(|s| s.name.as_str()).unwrap_or("(unknown)");
        let timestamp = format_ns_time(trace_start);
        let span_count = ss.len();

        html.push_str(&format!(
            r#"<div class="trace-row" data-class:tr-selected="$traceId==='{tid}'" data-on:click="$traceId='{tid}'; $spanId=''">
  <div style="display:flex;gap:6px;align-items:center">
    <div style="flex:0 0 52px;text-align:center;padding:2px 4px">
      <div style="font-family:monospace;font-size:10px;{dur_color}">{dur_ms}ms</div>
      <div style="height:3px;border-radius:1px;background:{health_color};margin-top:3px"></div>
    </div>
    <div style="flex:1;min-width:0">
      <div style="font-family:monospace;font-size:11px;color:#e4e4e7;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{op}</div>
      <div style="font-size:10px;color:#71717a">{ts} · {span_count} spans</div>
    </div>
  </div>
</div>"#,
            tid = html_escape(trace_id),
            dur_ms = dur_ms,
            dur_color = dur_color,
            health_color = health_color,
            op = html_escape(&truncate_str(op_name, 28)),
            ts = ts_short(&timestamp),
            span_count = span_count,
        ));
    }

    html.push_str("</div>");
    html
}

fn render_all_waterfalls(trace_ids: &[String], trace_map: &HashMap<&str, Vec<&SpanRow>>) -> String {
    let empty = r#"<div class="flex items-center justify-center flex-1 text-zinc-500 text-xs" data-show="$traceId===''">Select a trace to view the waterfall</div>"#;
    let waterfalls: String = trace_ids.iter().take(200).map(|tid| {
        let spans = match trace_map.get(tid.as_str()) { Some(v) => v, None => return String::new() };
        let wf = render_waterfall(spans);
        format!(r#"<div class="flex flex-col flex-1 overflow-hidden" data-show="$traceId==='{tid}'">{wf}</div>"#,
            tid = html_escape(tid), wf = wf)
    }).collect();
    format!("{empty}{waterfalls}")
}

fn render_all_trace_infos(
    trace_ids: &[String],
    trace_map: &HashMap<&str, Vec<&SpanRow>>,
) -> String {
    let empty = r#"<div class="flex flex-col items-center justify-center h-full text-zinc-500 text-xs" data-show="$traceId===''">Select a trace to view details</div>"#;
    let infos: String = trace_ids
        .iter()
        .take(200)
        .map(|tid| {
            let spans = match trace_map.get(tid.as_str()) {
                Some(v) => v,
                None => return String::new(),
            };
            let info = render_trace_info(spans, tid);
            format!(
                r#"<div data-show="$traceId==='{tid}'">{info}</div>"#,
                tid = html_escape(tid),
                info = info
            )
        })
        .collect();
    format!("{empty}{infos}")
}

fn render_all_span_details_flat(
    all_spans: &[SpanRow],
    trace_map: &HashMap<&str, Vec<&SpanRow>>,
) -> String {
    let empty = r#"<div class="flex flex-col items-center justify-center h-full text-zinc-500 text-xs" data-show="$spanId===''">Select a span</div>"#;
    let details: String = all_spans
        .iter()
        .map(|span| {
            let trace_start = trace_map
                .get(span.trace_id.as_str())
                .and_then(|ss| ss.iter().map(|s| s.start_ns).min())
                .unwrap_or(0);
            let inner = render_span_details(span, trace_start);
            format!(
                r#"<div data-show="$spanId==='{sid}'">{inner}</div>"#,
                sid = html_escape(&span.span_id),
                inner = inner
            )
        })
        .collect();
    format!("{empty}{details}")
}

// ---------------------------------------------------------------------------
// Waterfall panel
// ---------------------------------------------------------------------------

fn render_waterfall(spans: &[&SpanRow]) -> String {
    if spans.is_empty() {
        return r#"<div class="flex items-center justify-center flex-1 text-zinc-500 text-xs">No spans</div>"#.to_string();
    }

    let trace_start = spans.iter().map(|s| s.start_ns).min().unwrap_or(0);
    let trace_end = spans.iter().map(|s| s.end_ns).max().unwrap_or(0);
    let trace_dur_ns = (trace_end - trace_start).max(1);

    // Build tree structure
    let span_ids: std::collections::HashSet<&str> =
        spans.iter().map(|s| s.span_id.as_str()).collect();
    let mut children: HashMap<Option<String>, Vec<&SpanRow>> = HashMap::new();
    for &s in spans {
        let parent = s
            .parent_span_id
            .as_deref()
            .filter(|p| span_ids.contains(*p))
            .map(|p| p.to_string());
        children.entry(parent).or_default().push(s);
    }
    for kids in children.values_mut() {
        kids.sort_by_key(|s| s.start_ns);
    }

    let mut ordered: Vec<(&SpanRow, u32)> = Vec::new();
    assign_depths(None, &children, 0, &mut ordered);
    if ordered.is_empty() {
        let mut flat: Vec<&SpanRow> = spans.to_vec();
        flat.sort_by_key(|s| s.start_ns);
        for s in flat {
            ordered.push((s, 0));
        }
    }

    let mut rows_html = String::new();
    for &(span, _depth) in &ordered {
        let offset_ns = span.start_ns - trace_start;
        let dur_ns = (span.end_ns - span.start_ns).max(0);
        let offset_pct = (offset_ns as f64 / trace_dur_ns as f64 * 100.0).clamp(0.0, 99.9);
        let width_pct =
            (dur_ns as f64 / trace_dur_ns as f64 * 100.0).clamp(0.1, 100.0 - offset_pct);
        let dur_ms = dur_ns / 1_000_000;
        let color = service_color(&span.service_name);
        let error_class = if span.status == 2 { " wf-error" } else { "" };
        let tooltip = html_escape(&format!("{} — {}ms", span.name, dur_ms));

        rows_html.push_str(&format!(
            r#"<div class="wf-row{error_class}" data-class:wf-selected="$spanId==='{sid}'" data-on:click="$spanId='{sid}'">
  <div class="wf-timeline">
    <div class="wf-bar" style="left:{offset_pct:.2}%;width:{width_pct:.2}%;background:{color}" title="{tooltip}">
      <span class="wf-bar-label">{name}</span>
    </div>
  </div>
</div>"#,
            error_class = error_class,
            sid = html_escape(&span.span_id),
            offset_pct = offset_pct,
            width_pct = width_pct,
            color = color,
            tooltip = tooltip,
            name = html_escape(&span.name),
        ));
    }

    // Timeline ruler (5 ticks)
    let ruler = render_ruler(trace_dur_ns);

    format!(
        r#"{ruler}<div class="flex-1 overflow-y-auto">{rows}</div>"#,
        rows = rows_html,
        ruler = ruler,
    )
}

fn render_ruler(dur_ns: i64) -> String {
    let mut ticks = String::new();
    for i in 0..=4 {
        let pct = i as f64 * 25.0;
        let t_ns = dur_ns as f64 * pct / 100.0;
        let label = format_ns_duration(t_ns as i64);
        ticks.push_str(&format!(
            r#"<span class="wf-ruler-tick" style="left:{pct}%">{label}</span>"#,
            pct = pct,
            label = label,
        ));
    }
    format!(r#"<div class="wf-ruler">{ticks}</div>"#)
}

// ---------------------------------------------------------------------------
// Trace info panel
// ---------------------------------------------------------------------------

fn render_trace_info(spans: &[&SpanRow], trace_id: &str) -> String {
    if spans.is_empty() {
        return r#"<div class="text-zinc-500 text-xs text-center py-8">No trace data</div>"#
            .to_string();
    }

    let trace_start = spans.iter().map(|s| s.start_ns).min().unwrap_or(0);
    let trace_end = spans.iter().map(|s| s.end_ns).max().unwrap_or(0);
    let dur_ms = (trace_end - trace_start) / 1_000_000;

    let span_ids: std::collections::HashSet<&str> =
        spans.iter().map(|s| s.span_id.as_str()).collect();
    let root = spans
        .iter()
        .filter(|s| {
            s.parent_span_id
                .as_deref()
                .map(|p| !span_ids.contains(p))
                .unwrap_or(true)
        })
        .min_by_key(|s| s.start_ns)
        .or_else(|| spans.iter().min_by_key(|s| s.start_ns));

    let op_name = root.map(|s| s.name.as_str()).unwrap_or("(unknown)");
    let error_count = spans.iter().filter(|s| s.status == 2).count();
    let span_count = spans.len();

    // Unique services
    let mut services: Vec<&str> = spans.iter().map(|s| s.service_name.as_str()).collect();
    services.sort_unstable();
    services.dedup();
    let svc_count = services.len();

    let error_badge = if error_count > 0 {
        format!(
            r#"<span class="text-[10px] text-red-400 font-mono">⚠ {error_count} error{s}</span>"#,
            error_count = error_count,
            s = if error_count == 1 { "" } else { "s" }
        )
    } else {
        String::new()
    };

    // Service legend
    let svc_legend: String = services.iter().map(|svc| {
        let color = service_color(svc);
        let svc_spans = spans.iter().filter(|s| s.service_name.as_str() == *svc).count();
        format!(
            r#"<div class="flex items-center gap-1.5 py-0.5">
  <span style="width:10px;height:10px;border-radius:2px;background:{color};display:inline-block;flex-shrink:0"></span>
  <span class="text-xs text-zinc-200 font-mono overflow-hidden text-ellipsis whitespace-nowrap">{svc}</span>
  <span class="ml-auto text-[10px] text-zinc-500 font-mono">{svc_spans}</span>
</div>"#,
            color = color,
            svc = html_escape(svc),
            svc_spans = svc_spans,
        )
    }).collect();

    // Error spans
    let error_spans_html: String = spans.iter()
        .filter(|s| s.status == 2)
        .map(|s| format!(
            r#"<div class="px-2 py-1 rounded text-[11px] font-mono" style="background:rgba(239,68,68,0.1)">
  <div class="text-red-300">{name}</div>
  <div class="text-zinc-500">{svc}</div>
</div>"#,
            name = html_escape(&s.name),
            svc = html_escape(&s.service_name),
        )).collect();

    let errors_section = if error_count > 0 {
        format!(
            r#"<div class="mt-4">
  <div class="text-[10px] font-semibold uppercase tracking-wider text-red-400 mb-1">Errors</div>
  <div class="flex flex-col gap-1">{error_spans_html}</div>
</div>"#
        )
    } else {
        String::new()
    };

    let trace_short = &trace_id[..trace_id.len().min(16)];

    format!(
        r#"<div class="text-xs">
  <div class="font-semibold text-zinc-100 text-sm mb-2 font-mono overflow-hidden text-ellipsis whitespace-nowrap">{op_name}</div>
  <div class="flex items-center gap-3 text-zinc-400 text-[11px] mb-4">
    <span class="font-mono">{span_count} spans</span>
    <span class="font-mono">{svc_count} services</span>
    {error_badge}
  </div>
  <div class="flex flex-col gap-1.5 text-[11px]">
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Trace ID</span><span class="font-mono text-zinc-300 overflow-hidden text-ellipsis">{trace_short}</span></div>
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Duration</span><span class="font-mono text-zinc-300">{dur_ms}ms</span></div>
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Start</span><span class="font-mono text-zinc-300">{start_time}</span></div>
  </div>
  <div class="mt-4">
    <div class="text-[10px] font-semibold uppercase tracking-wider text-zinc-500 mb-1">Services</div>
    <div class="flex flex-col">{svc_legend}</div>
  </div>
  {errors_section}
</div>"#,
        op_name = html_escape(op_name),
        span_count = span_count,
        svc_count = svc_count,
        error_badge = error_badge,
        trace_short = html_escape(trace_short),
        dur_ms = dur_ms,
        start_time = format_ns_time(trace_start),
        svc_legend = svc_legend,
        errors_section = errors_section,
    )
}

// ---------------------------------------------------------------------------
// Span details panel
// ---------------------------------------------------------------------------

fn render_span_details(span: &SpanRow, trace_start: i64) -> String {
    let dur_ns = (span.end_ns - span.start_ns).max(0);
    let dur_ms = dur_ns / 1_000_000;
    let offset_ms = (span.start_ns - trace_start) / 1_000_000;

    let status_label = match span.status {
        1 => {
            r#"<span class="px-2 py-0.5 rounded-full text-[10px] font-bold" style="background:#166534;color:#4ade80">OK</span>"#
        }
        2 => {
            r#"<span class="px-2 py-0.5 rounded-full text-[10px] font-bold" style="background:#7f1d1d;color:#f87171">Error</span>"#
        }
        _ => {
            r#"<span class="px-2 py-0.5 rounded-full text-[10px] font-bold" style="background:#27272a;color:#a1a1aa">—</span>"#
        }
    };

    let kind_label = match span.span_kind {
        1 => "Internal",
        2 => "Server",
        3 => "Client",
        4 => "Producer",
        5 => "Consumer",
        _ => "Unspecified",
    };

    let parent_row = if let Some(ref p) = span.parent_span_id {
        format!(
            r#"<div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Parent</span><span class="font-mono text-zinc-300 overflow-hidden text-ellipsis text-[10px]">{}</span></div>"#,
            html_escape(p)
        )
    } else {
        String::new()
    };

    let span_short = &span.span_id[..span.span_id.len().min(16)];

    let attrs_section = if span.attributes.is_empty() {
        String::new()
    } else {
        let rows: String = span
            .attributes
            .iter()
            .map(|(k, v)| {
                format!(
                    r#"<div class="flex gap-2 py-px">
  <span class="text-zinc-500 font-mono shrink-0 text-right" style="min-width:80px">{k}</span>
  <span class="text-zinc-300 font-mono break-all">{v}</span>
</div>"#,
                    k = html_escape(k),
                    v = html_escape(v),
                )
            })
            .collect();
        format!(
            r#"<div class="mt-3">
  <div class="text-[10px] font-semibold uppercase tracking-wider text-zinc-500 mb-1">Attributes</div>
  <div class="flex flex-col text-[11px]">{rows}</div>
</div>"#
        )
    };

    format!(
        r#"<div class="text-xs">
  <div class="flex items-center justify-between mb-3">
    <span class="font-semibold text-zinc-100 text-[11px]">Span Details</span>
    {status_label}
  </div>
  <div class="flex flex-col gap-1.5 text-[11px]">
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Operation</span><span class="font-mono text-zinc-300 overflow-hidden text-ellipsis whitespace-nowrap">{name}</span></div>
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Service</span><span class="font-mono text-zinc-300">{svc}</span></div>
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Span ID</span><span class="font-mono text-zinc-300 text-[10px]">{span_short}</span></div>
    {parent_row}
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Kind</span><span class="font-mono text-zinc-300">{kind}</span></div>
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Offset</span><span class="font-mono text-zinc-300">+{offset_ms}ms</span></div>
    <div class="flex gap-2"><span class="text-zinc-500 w-20 shrink-0">Duration</span><span class="font-mono text-zinc-300">{dur_ms}ms</span></div>
  </div>
  {attrs_section}
</div>"#,
        status_label = status_label,
        name = html_escape(&span.name),
        svc = html_escape(&span.service_name),
        span_short = html_escape(span_short),
        parent_row = parent_row,
        kind = kind_label,
        offset_ms = offset_ms,
        dur_ms = dur_ms,
        attrs_section = attrs_section,
    )
}

// ---------------------------------------------------------------------------
// Tree helpers
// ---------------------------------------------------------------------------

fn assign_depths<'a>(
    parent_key: Option<String>,
    children: &HashMap<Option<String>, Vec<&'a SpanRow>>,
    depth: u32,
    result: &mut Vec<(&'a SpanRow, u32)>,
) {
    if let Some(kids) = children.get(&parent_key) {
        for &kid in kids {
            result.push((kid, depth));
            assign_depths(Some(kid.span_id.clone()), children, depth + 1, result);
        }
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_ns_time(ns: i64) -> String {
    if ns == 0 {
        return "—".to_string();
    }
    // Format as HH:MM:SS.mmm using simple math
    let secs = ns / 1_000_000_000;
    let ms = (ns % 1_000_000_000) / 1_000_000;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
}

fn ts_short(s: &str) -> &str {
    // "HH:MM:SS.mmm" → show "HH:MM:SS"
    if s.len() >= 8 {
        &s[..8]
    } else {
        s
    }
}

fn format_ns_duration(ns: i64) -> String {
    if ns == 0 {
        return "0".to_string();
    }
    if ns < 1_000 {
        return format!("{ns}ns");
    }
    if ns < 1_000_000 {
        return format!("{:.1}µs", ns as f64 / 1_000.0);
    }
    if ns < 1_000_000_000 {
        return format!("{:.1}ms", ns as f64 / 1_000_000.0);
    }
    format!("{:.2}s", ns as f64 / 1_000_000_000.0)
}

fn service_color(service: &str) -> &'static str {
    let h = service.bytes().fold(5381u32, |acc, b| {
        acc.wrapping_mul(33).wrapping_add(b as u32)
    });
    const COLORS: &[&str] = &[
        "#4dabf7", "#74c7ec", "#a6e3a1", "#94e2d5", "#89dceb", "#cba6f7", "#f38ba8", "#fab387",
        "#f9e2af", "#a6adc8",
    ];
    COLORS[(h as usize) % COLORS.len()]
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let t: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{}…", t)
}

fn error_event(selector: &str, msg: &str) -> Event {
    let selector = selector.to_string();
    Event::from(
        PatchElements::new(format!(
            r#"<div role="alert" class="m-2 p-3 bg-red-950 text-red-400 border-l-4 border-red-500 rounded text-sm">{}</div>"#,
            html_escape(msg)
        ))
        .selector(&selector)
        .mode(ElementPatchMode::Inner),
    )
}
