use axum::extract::State;
use axum::http::header;
use axum::response::{Html, IntoResponse};

use crate::query::snapshot_objects;
use crate::state::AppState;

pub async fn handler(State(state): State<AppState>) -> impl IntoResponse {
    // Fetch services to set the initial $service signal — avoids the race condition
    // where data-init on #main-area fires before the services SSE sets the service.
    let services = snapshot_objects(&state.backend, "resources last 24h")
        .await
        .unwrap_or_default();

    let initial_service = services
        .first()
        .and_then(|s| s.get("resource_id"))
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .unwrap_or_default();

    match state.render(
        "base.html",
        minijinja::context! { initial_service => &initial_service },
    ) {
        Ok(html) => (
            [(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")],
            Html(html),
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Template render error: {e}");
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
