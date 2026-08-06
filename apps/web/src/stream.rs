use axum::response::sse::Event;
use datastar::consts::ElementPatchMode;
use datastar::patch_elements::PatchElements;
use datastar::patch_signals::PatchSignals;
use sequins_view::ViewDelta;

use crate::query::ipc_to_objects;

/// Convert a ViewDelta into zero or more SSE events for a table (logs, spans).
///
/// `selector` is the CSS selector of the container element that rows are appended to.
/// `row_template` is a function that renders a single JSON object row to HTML.
pub fn table_delta_to_events<F>(delta: ViewDelta, selector: &str, row_template: F) -> Vec<Event>
where
    F: Fn(&serde_json::Map<String, serde_json::Value>) -> String,
{
    match delta {
        ViewDelta::RowsAppended { ipc, .. } => {
            if let Ok(objects) = ipc_to_objects(&ipc) {
                if objects.is_empty() {
                    return vec![];
                }
                let html: String = objects.iter().map(row_template).collect();
                vec![Event::from(
                    PatchElements::new(html)
                        .selector(selector)
                        .mode(ElementPatchMode::Prepend),
                )]
            } else {
                vec![]
            }
        }
        ViewDelta::TableReplaced { ipc, .. } => {
            if let Ok(objects) = ipc_to_objects(&ipc) {
                let html: String = objects.iter().map(row_template).collect();
                vec![Event::from(
                    PatchElements::new(html)
                        .selector(selector)
                        .mode(ElementPatchMode::Inner),
                )]
            } else {
                vec![]
            }
        }
        ViewDelta::RowsExpired { expired_count, .. } => {
            // Remove the last `expired_count` children of the container
            let js = format!(
                "const c=document.querySelector('{}');for(let i=0;i<{};i++){{if(c&&c.lastElementChild)c.lastElementChild.remove();}}",
                selector.replace('\'', "\\'"),
                expired_count
            );
            vec![Event::from(datastar::execute_script::ExecuteScript::new(
                js,
            ))]
        }
        ViewDelta::Ready => {
            // Signal that initial data load is complete
            vec![Event::from(PatchSignals::new(r#"{"loading": false}"#))]
        }
        ViewDelta::Error { message } => {
            let html = format!(
                r#"<div class="error-banner" role="alert">{}</div>"#,
                html_escape(&message),
            );
            vec![Event::from(
                PatchElements::new(html)
                    .selector(selector)
                    .mode(ElementPatchMode::Before),
            )]
        }
        _ => vec![],
    }
}

/// Convert a ViewDelta from an aggregate strategy into SSE events that replace a stats panel.
#[allow(dead_code)]
pub fn aggregate_delta_to_event(
    delta: ViewDelta,
    selector: &str,
    render: impl Fn(Vec<serde_json::Map<String, serde_json::Value>>) -> String,
) -> Vec<Event> {
    match delta {
        ViewDelta::TableReplaced { ipc, .. } | ViewDelta::RowsAppended { ipc, .. } => {
            if let Ok(objects) = ipc_to_objects(&ipc) {
                let html = render(objects);
                vec![Event::from(
                    PatchElements::new(html)
                        .selector(selector)
                        .mode(ElementPatchMode::Inner),
                )]
            } else {
                vec![]
            }
        }
        _ => vec![],
    }
}

/// Minimal HTML escaping to prevent XSS in rendered content.
pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Format a nanosecond timestamp into a human-readable UTC time string.
#[allow(dead_code)]
pub fn format_timestamp_ns(ns: i64) -> String {
    let total_secs = (ns / 1_000_000_000).unsigned_abs();
    let ms = (ns.abs() % 1_000_000_000) / 1_000_000;
    let h = (total_secs / 3600) % 24;
    let m = (total_secs / 60) % 60;
    let s = total_secs % 60;
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}
