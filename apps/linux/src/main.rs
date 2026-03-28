//! Sequins Linux desktop app
//!
//! OTLP observability viewer powered by Relm4 + libadwaita.
//! Depends directly on sequins-* Rust crates — no C FFI boundary.

// Widget struct fields are kept alive (GObject ref-counting) even if not read.
#![allow(dead_code)]
// Cairo-rs 0.20 returns () for many functions; let _ = () is intentional.
#![allow(clippy::let_unit_value)]

mod app;
mod components;
mod config;
mod data;
mod drawing;
mod time_range;

use relm4::RelmApp;

fn main() {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sequins_linux=debug,sequins=debug,warn".parse().unwrap()),
        )
        .init();

    tracing::info!("Starting Sequins desktop");

    // Default to the Cairo software renderer so the app runs correctly in
    // container environments without GPU access (e.g. Fedora Toolbox).
    // These are only set if not already in the environment, so a user with a
    // working GPU can still override by setting the vars before launching.
    // SAFETY: single-threaded before GTK initialisation.
    #[allow(unused_unsafe)]
    unsafe {
        if std::env::var("GSK_RENDERER").is_err() {
            std::env::set_var("GSK_RENDERER", "cairo");
        }
        if std::env::var("GDK_GL").is_err() {
            std::env::set_var("GDK_GL", "disabled");
        }
    }

    let app = RelmApp::new("com.sequins.desktop");
    app.run::<app::AppModel>(());
}
