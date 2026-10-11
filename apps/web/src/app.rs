use crate::routes;
use crate::state::{AppState, Templates};
use axum::routing::{get, post};
use axum::Router;
use minijinja::Environment;
use sequins_client::RemoteClient;
use std::sync::Arc;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

pub fn build(backend: Arc<RemoteClient>) -> Router {
    let templates = make_templates();
    let (logs_gen_tx, _) = tokio::sync::watch::channel(0u64);
    let (metrics_gen_tx, _) = tokio::sync::watch::channel(0u64);
    let (health_gen_tx, _) = tokio::sync::watch::channel(0u64);
    let state = AppState {
        backend,
        templates,
        logs_gen_tx: Arc::new(logs_gen_tx),
        metrics_gen_tx: Arc::new(metrics_gen_tx),
        health_gen_tx: Arc::new(health_gen_tx),
    };

    let assets_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("assets");

    Router::new()
        .route("/", get(routes::index::handler))
        .route("/services", get(routes::services::handler))
        .route("/logs/view", get(routes::logs::handler))
        .route("/traces/view", get(routes::traces::handler))
        .route("/health/view", get(routes::health::handler))
        .route("/metrics/view", get(routes::metrics::handler))
        .route("/profiles/view", get(routes::profiles::handler))
        .route("/explore/view", get(routes::explore::view_handler))
        .route("/explore/query", post(routes::explore::handler))
        .nest_service("/assets", ServeDir::new(assets_dir))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn make_templates() -> Templates {
    // Dev: load from disk on every render so edits take effect without a server restart.
    // Prod: use compiled-in templates cached once at startup.
    let templates_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
    if templates_dir.exists() {
        return Templates::Dev(Arc::new(templates_dir));
    }

    // Compiled-in fallback for deployed binaries.
    let mut env = Environment::new();
    env.add_template("base.html", include_str!("../templates/base.html"))
        .expect("base.html");
    env.add_template(
        "partials/sidebar.html",
        include_str!("../templates/partials/sidebar.html"),
    )
    .expect("partials/sidebar.html");
    env.add_template(
        "partials/logs.html",
        include_str!("../templates/partials/logs.html"),
    )
    .expect("partials/logs.html");
    env.add_template(
        "partials/traces.html",
        include_str!("../templates/partials/traces.html"),
    )
    .expect("partials/traces.html");
    env.add_template(
        "partials/health.html",
        include_str!("../templates/partials/health.html"),
    )
    .expect("partials/health.html");
    env.add_template(
        "partials/metrics.html",
        include_str!("../templates/partials/metrics.html"),
    )
    .expect("partials/metrics.html");
    env.add_template(
        "partials/profiles.html",
        include_str!("../templates/partials/profiles.html"),
    )
    .expect("partials/profiles.html");
    env.add_template(
        "partials/explore.html",
        include_str!("../templates/partials/explore.html"),
    )
    .expect("partials/explore.html");
    Templates::Prod(Arc::new(env))
}
