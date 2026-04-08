//! Logs tab — live-streaming SSE endpoint.
//!
//! Streams log rows as they arrive, prepending new rows to the log list.
//! Query parameters (read via Datastar ReadSignals):
//!   - service:  resource_id to filter by (empty = all services)
//!   - duration: relative time window, e.g. "5m", "1h" (default "5m")
//!   - search:   text filter applied via `body contains '...'`
//!   - live:     true = live stream, false = snapshot

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
use std::convert::Infallible;

use crate::state::AppState;
use crate::stream::{html_escape, table_delta_to_events};
use sequins_view::{TableStrategy, ViewStrategy};

#[derive(Deserialize, Default)]
pub struct LogSignals {
    #[serde(default)]
    pub service: String,
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default = "default_live")]
    pub live: bool,
    #[serde(default)]
    pub search: String,
    #[serde(default = "default_true")]
    pub sev1: bool,
    #[serde(default = "default_true")]
    pub sev5: bool,
    #[serde(default = "default_true")]
    pub sev9: bool,
    #[serde(default = "default_true")]
    pub sev13: bool,
    #[serde(default = "default_true")]
    pub sev17: bool,
    #[serde(default = "default_true")]
    pub sev21: bool,
}

fn default_duration() -> String {
    "5m".to_string()
}
fn default_live() -> bool {
    true
}
fn default_true() -> bool {
    true
}

/// Build the tab-specific controls HTML for the logs tab.
fn render_tab_controls(params: &LogSignals) -> String {
    let search_val = html_escape(&params.search);

    let make_toggle = |label: &str, css_class: &str, sig: &str, active: bool| -> String {
        let active_class = if active { " active" } else { "" };
        format!(
            r#"<button class="sev-toggle{active_class} px-2 py-px rounded text-[11px] font-bold border cursor-pointer shrink-0 whitespace-nowrap sev-{css_class}" data-class:active="${sig}" data-on:click="${sig}=!${sig}; @get('/logs/view', {{retry: 'never'}})">{label}</button>"#
        )
    };

    format!(
        r#"<input type="text" class="px-2 py-1 text-xs border border-zinc-600 rounded bg-zinc-950 text-zinc-300 placeholder-zinc-500 focus:outline-none focus:border-blue-400 w-40 shrink-0" placeholder="Search logs…" value="{search_val}" data-bind:search data-on:input__debounce.300ms="@get('/logs/view', {{retry: 'never'}})"><div class="w-px h-4 bg-zinc-700 shrink-0 mx-0.5"></div>{error}{warn}{info}{debug}{trace}{fatal}"#,
        error = make_toggle("Error", "error", "sev17", params.sev17),
        warn = make_toggle("Warn", "warn", "sev13", params.sev13),
        info = make_toggle("Info", "info", "sev9", params.sev9),
        debug = make_toggle("Debug", "debug", "sev5", params.sev5),
        trace = make_toggle("Trace", "trace", "sev1", params.sev1),
        fatal = make_toggle("Fatal", "fatal", "sev21", params.sev21),
    )
}

pub async fn handler(
    State(state): State<AppState>,
    ReadSignals(params): ReadSignals<LogSignals>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let backend = state.backend.clone();
    let tmpl = state.templates.clone();

    // Claim a generation slot. The previous live-streaming loop (if any) will
    // detect that the generation has advanced and terminate itself, closing the
    // old gRPC stream before the new one starts sending rows.
    let my_gen = {
        let mut val = 0u64;
        state.logs_gen_tx.send_modify(|g| {
            *g += 1;
            val = *g;
        });
        val
    };
    let mut gen_rx = state.logs_gen_tx.subscribe();

    let s = stream! {
        // Render the full logs page into #content
        let page_html = match tmpl.render("partials/logs.html", minijinja::context! {
            service => &params.service,
            duration => &params.duration,
            live => params.live,
            search => &params.search,
        }) {
            Ok(h) => h,
            Err(e) => { tracing::error!("Logs template error: {e}"); return; }
        };
        yield Ok(Event::from(
            PatchElements::new(page_html).selector("#content").mode(ElementPatchMode::Inner),
        ));
        // Inject tab-specific filter controls
        yield Ok(Event::from(
            PatchElements::new(render_tab_controls(&params))
                .selector("#tab-controls")
                .mode(ElementPatchMode::Inner),
        ));

        // Signal: loading started
        yield Ok(Event::from(PatchSignals::new(r#"{"loading": true}"#)));

        // Build the SeQL query
        let seql = build_logs_query(&params);

        if params.live {
            // Live streaming mode
            let stream_result = backend.query_live(&seql).await;
            let raw_stream = match stream_result {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Logs query error: {e}");
                    yield Ok(error_event(&e.to_string()));
                    yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                    return;
                }
            };

            let strategy = TableStrategy::new();
            let mut delta_stream = strategy.transform(raw_stream).await;

            loop {
                tokio::select! {
                    biased;
                    // A newer connection claimed the generation slot — abort.
                    _ = gen_rx.changed() => {
                        if *gen_rx.borrow_and_update() > my_gen {
                            return;
                        }
                    }
                    delta = delta_stream.next() => {
                        match delta {
                            Some(d) => {
                                for event in table_delta_to_events(d, "#log-rows", render_log_row) {
                                    yield Ok(event);
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        } else {
            // Snapshot mode
            let stream_result = backend.query(&seql).await;
            let raw_stream = match stream_result {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Logs snapshot error: {e}");
                    yield Ok(error_event(&e.to_string()));
                    yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                    return;
                }
            };

            let strategy = TableStrategy::new();
            let mut delta_stream = strategy.transform(raw_stream).await;

            while let Some(delta) = delta_stream.next().await {
                for event in table_delta_to_events(delta, "#log-rows", render_log_row) {
                    yield Ok(event);
                }
            }
            yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
        }
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}

fn build_logs_query(params: &LogSignals) -> String {
    let mut q = format!("logs last {}", params.duration);
    if !params.service.is_empty() {
        q.push_str(&format!(
            " | where resource_id = '{}'",
            params.service.replace('\'', "''")
        ));
    }
    // Severity filter — only add if not all severities selected
    let mut sevs: Vec<u32> = vec![];
    if params.sev1 {
        sevs.push(1);
    }
    if params.sev5 {
        sevs.push(5);
    }
    if params.sev9 {
        sevs.push(9);
    }
    if params.sev13 {
        sevs.push(13);
    }
    if params.sev17 {
        sevs.push(17);
    }
    if params.sev21 {
        sevs.push(21);
    }
    if sevs.len() < 6 {
        if sevs.is_empty() {
            q.push_str(" | where 1 = 0");
        } else {
            let nums: Vec<String> = sevs.iter().map(|n| n.to_string()).collect();
            q.push_str(&format!(
                " | where severity_number in [{}]",
                nums.join(", ")
            ));
        }
    }
    if !params.search.is_empty() {
        q.push_str(&format!(
            " | where body contains '{}'",
            params.search.replace('\'', "''")
        ));
    }
    q.push_str(" | take 500");
    q
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
        serde_json::Value::Object(_) => None,
    }
}

fn render_log_row(obj: &serde_json::Map<String, serde_json::Value>) -> String {
    let time = obj
        .get("time_unix_nano")
        .and_then(|v| v.as_i64())
        .map(format_time_ns)
        .unwrap_or_default();
    let severity = obj
        .get("severity_number")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let severity_label = severity_label(severity);
    let severity_class = severity_class(severity);
    let body = obj
        .get("body")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Collect promoted attr_* columns
    let mut attributes: Vec<(String, String)> = obj
        .iter()
        .filter_map(|(k, v)| {
            let name = k.strip_prefix("attr_")?;
            let s = format_attr_value(v)?;
            Some((name.to_string(), s))
        })
        .collect();

    // Collect overflow attributes (_overflow_attrs is decoded to a JSON object by col_value_to_json)
    if let Some(serde_json::Value::Object(overflow)) = obj.get("_overflow_attrs") {
        for (k, v) in overflow {
            if let Some(s) = format_attr_value(v) {
                attributes.push((k.clone(), s));
            }
        }
    }
    attributes.sort_by(|a, b| a.0.cmp(&b.0));

    let has_attrs = !attributes.is_empty();

    let attrs_html = if has_attrs {
        let rows: String = attributes
            .iter()
            .map(|(k, v)| {
                format!(
                    r#"<div style="display:contents"><span style="text-align:right;color:#71717a;font-family:monospace;font-size:11px;padding:1px 0">{key}</span><span style="color:#d4d4d8;font-family:monospace;font-size:11px;word-break:break-all;padding:1px 0">{val}</span></div>"#,
                    key = html_escape(k),
                    val = html_escape(v),
                )
            })
            .collect();
        format!(
            r#"<div style="display:grid;grid-template-columns:auto 1fr;gap:1px 12px;padding:6px 12px 8px 12px;background:#18181b;border-top:1px solid #27272a">{rows}</div>"#
        )
    } else {
        String::new()
    };

    // Only make the row a <details> if there are attributes to show
    if has_attrs {
        format!(
            r#"<details class="group">
  <summary class="flex items-center gap-2 px-3 py-1 cursor-pointer hover:bg-zinc-900 list-none">
    <span style="font-family:monospace;font-size:11px;color:#a1a1aa;white-space:nowrap">{time}</span>
    <span class="inline-block px-1.5 py-px rounded font-bold font-mono {severity_class}-bg" style="font-size:11px">{severity_label}</span>
    <span style="font-family:monospace;font-size:12px;color:#f4f4f5;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1">{body_escaped}</span>
  </summary>
  {attrs_html}
</details>"#,
            time = time,
            severity_class = severity_class,
            severity_label = severity_label,
            body_escaped = html_escape(&body),
            attrs_html = attrs_html,
        )
    } else {
        format!(
            r#"<div class="flex items-center gap-2 px-3 py-1 hover:bg-zinc-900">
  <span style="font-family:monospace;font-size:11px;color:#a1a1aa;white-space:nowrap">{time}</span>
  <span class="inline-block px-1.5 py-px rounded font-bold font-mono {severity_class}-bg" style="font-size:11px">{severity_label}</span>
  <span style="font-family:monospace;font-size:12px;color:#f4f4f5;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1">{body_escaped}</span>
</div>"#,
            time = time,
            severity_class = severity_class,
            severity_label = severity_label,
            body_escaped = html_escape(&body),
        )
    }
}

fn error_event(msg: &str) -> Event {
    Event::from(
        PatchElements::new(format!(
            r#"<div role="alert" class="m-2 p-3 bg-red-950 text-red-400 border-l-4 border-red-500 rounded text-sm">{}</div>"#,
            html_escape(msg)
        ))
        .selector("#log-rows")
        .mode(ElementPatchMode::Before),
    )
}

fn format_time_ns(ns: i64) -> String {
    use std::time::{Duration, UNIX_EPOCH};
    let d = UNIX_EPOCH + Duration::from_nanos(ns.unsigned_abs());
    let secs = ns / 1_000_000_000;
    let ms = (ns % 1_000_000_000) / 1_000_000;
    // Use UTC time via basic calculation
    let total_secs = secs.unsigned_abs();
    let h = (total_secs / 3600) % 24;
    let m = (total_secs / 60) % 60;
    let s = total_secs % 60;
    let _ = d; // suppress unused warning
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

fn severity_label(n: i64) -> &'static str {
    match n {
        1..=4 => "TRACE",
        5..=8 => "DEBUG",
        9..=12 => "INFO",
        13..=16 => "WARN",
        17..=20 => "ERROR",
        21..=24 => "FATAL",
        _ => "?",
    }
}

fn severity_class(n: i64) -> &'static str {
    match n {
        1..=4 => "severity-trace",
        5..=8 => "severity-debug",
        9..=12 => "severity-info",
        13..=16 => "severity-warn",
        17..=20 => "severity-error",
        21..=24 => "severity-fatal",
        _ => "severity-unknown",
    }
}
