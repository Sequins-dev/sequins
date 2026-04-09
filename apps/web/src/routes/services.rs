//! Sidebar service list — SSE endpoint.
//!
//! Returns a Datastar PatchElements event that replaces the #service-list contents.
//! Called via `data-init="@get('/services')"` on page load.

use async_stream::stream;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use datastar::consts::ElementPatchMode;
use datastar::patch_elements::PatchElements;
use futures::StreamExt;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::frame::ipc_to_batch;
use sequins_query::QueryApi;
use std::convert::Infallible;

use crate::query::batch_to_objects;
use crate::state::AppState;
use crate::stream::html_escape;

pub async fn handler(
    State(state): State<AppState>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let backend = state.backend.clone();
    let tmpl = state.templates.clone();

    let s = stream! {
        let mut raw_stream = match backend.query("resources last 24h").await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Services query error: {e}");
                let html = format!(
                    r#"<li class="px-4 py-2 text-xs text-red-400">Error: {}</li>"#,
                    html_escape(&e.to_string())
                );
                yield Ok(Event::from(
                    PatchElements::new(html).selector("#service-list").mode(ElementPatchMode::Inner),
                ));
                return;
            }
        };

        let mut all_objects = Vec::new();
        while let Some(item) = raw_stream.next().await {
            let fd = match item {
                Ok(fd) => fd,
                Err(e) => { tracing::warn!("Services stream error: {e}"); continue; }
            };
            if fd.data_body.is_empty() { continue; }
            let meta = match decode_metadata(&fd.app_metadata) {
                Some(m) => m,
                None => continue,
            };
            match meta {
                SeqlMetadata::Data { .. } | SeqlMetadata::Append { .. } | SeqlMetadata::Replace { .. } => {
                    if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                        all_objects.extend(batch_to_objects(&batch));
                    }
                }
                SeqlMetadata::Complete { .. } => break,
                _ => {}
            }
        }

        let html = if all_objects.is_empty() {
            r#"<li class="px-4 py-2 text-xs text-zinc-500">No services found</li>"#.to_string()
        } else {
            match tmpl.render("partials/sidebar.html", minijinja::context! { services => &all_objects }) {
                Ok(h) => h,
                Err(e) => {
                    tracing::error!("Sidebar template error: {e}");
                    "<li>Template error</li>".to_string()
                }
            }
        };

        yield Ok(Event::from(
            PatchElements::new(html).selector("#service-list").mode(ElementPatchMode::Inner),
        ));
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}
