//! Logging initializer for the FFI layer.
//!
//! Writes structured logs to `~/Library/Logs/Sequins/rust.YYYY-MM-DD.log`
//! using a non-blocking file appender. The `WorkerGuard` is stored in a
//! `OnceCell` to ensure it lives for the process lifetime.
//!
//! Call `init()` once at startup (e.g. from `sequins_data_source_new_local`).

use once_cell::sync::OnceCell;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, EnvFilter};

static GUARD: OnceCell<WorkerGuard> = OnceCell::new();

/// Initialize the global tracing subscriber.
///
/// - Writes to `~/Library/Logs/Sequins/rust.YYYY-MM-DD.log` (rolling daily).
/// - Filter: all `sequins` crates at `debug`, everything else at `warn`.
/// - Safe to call multiple times — only the first call has any effect.
pub fn init() {
    GUARD.get_or_init(|| {
        // Resolve ~/Library/Logs/Sequins/
        let log_dir = dirs_for_log_dir();

        // Create the directory if it doesn't exist (best-effort)
        let _ = std::fs::create_dir_all(&log_dir);

        let file_appender = tracing_appender::rolling::daily(&log_dir, "rust");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let filter = EnvFilter::new("sequins=debug,warn");

        let _ = fmt::Subscriber::builder()
            .with_env_filter(filter)
            .with_writer(non_blocking)
            .with_ansi(false)
            .try_init();

        guard
    });
}

/// Return the path to `~/Library/Logs/Sequins/`.
///
/// Falls back to `/tmp/sequins-logs/` if the home directory cannot be resolved.
fn dirs_for_log_dir() -> std::path::PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = std::path::PathBuf::from(home);
        p.push("Library/Logs/Sequins");
        p
    } else {
        std::path::PathBuf::from("/tmp/sequins-logs")
    }
}
