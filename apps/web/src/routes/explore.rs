//! Explore tab — free-form SeQL query editor.
//!
//! POST /explore/query with JSON body containing all signals.
//! Always renders the explore template, then runs the query if non-empty.

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
use crate::stream::html_escape;
use sequins_view::{TableStrategy, ViewDelta, ViewStrategy};

/// GET /explore/view — renders the explore template without running a query.
pub async fn view_handler(
    State(state): State<AppState>,
    ReadSignals(params): ReadSignals<ExploreSignals>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let tmpl = state.templates.clone();
    let query_str = params.query.clone();

    let s = stream! {
        let page_html = match tmpl.render("partials/explore.html", minijinja::context! { query => &query_str }) {
            Ok(h) => h,
            Err(e) => { tracing::error!("Explore template error: {e}"); return; }
        };
        yield Ok(Event::from(
            PatchElements::new(page_html).selector("#content").mode(ElementPatchMode::Inner),
        ));
        // Clear tab controls (explore uses the in-content query editor)
        yield Ok(Event::from(
            PatchElements::new("").selector("#tab-controls").mode(ElementPatchMode::Inner),
        ));
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}

#[derive(Deserialize, Default)]
pub struct ExploreSignals {
    #[serde(default = "default_query")]
    pub query: String,
}

fn default_query() -> String {
    "spans last 1h".to_string()
}

pub async fn handler(
    State(state): State<AppState>,
    ReadSignals(params): ReadSignals<ExploreSignals>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let backend = state.backend.clone();
    let tmpl = state.templates.clone();
    let query_str = params.query.clone();

    let s = stream! {
        // Always render the explore template
        let page_html = match tmpl.render("partials/explore.html", minijinja::context! { query => &query_str }) {
            Ok(h) => h,
            Err(e) => { tracing::error!("Explore template error: {e}"); return; }
        };
        yield Ok(Event::from(
            PatchElements::new(page_html).selector("#content").mode(ElementPatchMode::Inner),
        ));

        // Clear tab controls
        yield Ok(Event::from(
            PatchElements::new("").selector("#tab-controls").mode(ElementPatchMode::Inner),
        ));

        if query_str.trim().is_empty() {
            return;
        }

        yield Ok(Event::from(PatchSignals::new(r#"{"loading": true}"#)));
        // Clear previous results
        yield Ok(Event::from(
            PatchElements::new("").selector("#explore-results").mode(ElementPatchMode::Inner),
        ));

        let raw_stream = match backend.query(&query_str).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Explore query error: {e}");
                yield Ok(Event::from(
                    PatchElements::new(format!(
                        r#"<div role="alert" class="m-2 p-3 bg-red-950 text-red-400 border-l-4 border-red-500 rounded text-sm">{}</div>"#,
                        html_escape(&e.to_string())
                    ))
                    .selector("#explore-results")
                    .mode(ElementPatchMode::Inner),
                ));
                yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                return;
            }
        };

        let strategy = TableStrategy::new();
        let mut delta_stream = strategy.transform(raw_stream).await;
        let mut row_count: usize = 0;
        let mut headers_rendered = false;

        while let Some(delta) = delta_stream.next().await {
            match delta {
                ViewDelta::RowsAppended { ipc, .. } => {
                    if let Ok(objects) = crate::query::ipc_to_objects(&ipc) {
                        if objects.is_empty() {
                            continue;
                        }
                        if !headers_rendered {
                            headers_rendered = true;
                            let headers: Vec<&str> = objects[0].keys().map(|k| k.as_str()).collect();
                            let header_html = headers.iter()
                                .map(|h| format!(r#"<th class="border border-zinc-700 px-2 py-1 text-left bg-zinc-900 font-semibold text-zinc-400 sticky top-0">{}</th>"#, html_escape(h)))
                                .collect::<String>();
                            yield Ok(Event::from(
                                PatchElements::new(format!(
                                    r#"<table class="w-full border-collapse text-xs font-mono"><thead><tr>{}</tr></thead><tbody id="explore-rows"></tbody></table>"#,
                                    header_html
                                ))
                                .selector("#explore-results")
                                .mode(ElementPatchMode::Inner),
                            ));
                        }
                        let rows_html: String = objects.iter().map(|obj| {
                            let cells: String = obj.values().map(|v| {
                                let s = match v {
                                    serde_json::Value::String(s) => s.clone(),
                                    serde_json::Value::Null => String::new(),
                                    other => other.to_string(),
                                };
                                format!(r#"<td class="border border-zinc-700 px-2 py-1 text-zinc-200 max-w-[30ch] overflow-hidden text-ellipsis whitespace-nowrap">{}</td>"#, html_escape(&s))
                            }).collect();
                            format!("<tr>{}</tr>", cells)
                        }).collect();
                        row_count += objects.len();
                        yield Ok(Event::from(
                            PatchElements::new(rows_html)
                                .selector("#explore-rows")
                                .mode(ElementPatchMode::Append),
                        ));
                        yield Ok(Event::from(PatchSignals::new(format!(r#"{{"row_count": {}}}"#, row_count))));
                    }
                }
                ViewDelta::Ready => {
                    yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                    return;
                }
                ViewDelta::Error { message } => {
                    yield Ok(Event::from(
                        PatchElements::new(format!(
                            r#"<div role="alert" class="m-2 p-3 bg-red-950 text-red-400 border-l-4 border-red-500 rounded text-sm">{}</div>"#,
                            html_escape(&message)
                        ))
                        .selector("#explore-results")
                        .mode(ElementPatchMode::Before),
                    ));
                    yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                    return;
                }
                _ => {}
            }
        }
        yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}
