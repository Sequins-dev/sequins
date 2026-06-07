//! Logging initializer for the FFI layer.
//!
//! Writes structured logs to a platform-appropriate directory using a
//! non-blocking rolling file appender.  The `WorkerGuard` is stored in a
//! `OnceCell` to ensure it lives for the process lifetime.
//!
//! Call `init()` once at startup (e.g. from `sequins_data_source_new_local`),
//! or call `init_with_log_dir` to supply a custom path.

use once_cell::sync::OnceCell;
use std::path::PathBuf;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, EnvFilter};

static GUARD: OnceCell<WorkerGuard> = OnceCell::new();

/// Initialize the global tracing subscriber using the platform log directory.
///
/// - macOS:   `~/Library/Logs/Sequins/`
/// - Linux:   `$XDG_DATA_HOME/sequins/logs/` (default `~/.local/share/sequins/logs/`)
/// - Windows: `%APPDATA%\Sequins\Logs\`
/// - Fallback: `/tmp/sequins-logs/`
///
/// Filter: all `sequins` crates at `debug`, everything else at `warn`.
/// Safe to call multiple times — only the first call has any effect.
pub fn init() {
    let log_dir = platform_log_dir();
    init_with_log_dir(&log_dir);
}

/// Initialize the global tracing subscriber writing logs to `log_dir`.
///
/// Safe to call multiple times — only the first call has any effect.
pub fn init_with_log_dir(log_dir: &std::path::Path) {
    GUARD.get_or_init(|| {
        let _ = std::fs::create_dir_all(log_dir);

        let file_appender = tracing_appender::rolling::daily(log_dir, "rust");
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

fn platform_log_dir() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        if let Some(home) = std::env::var_os("HOME") {
            let mut p = PathBuf::from(home);
            p.push("Library/Logs/Sequins");
            return p;
        }
    }

    #[cfg(target_os = "linux")]
    {
        // Respect XDG_DATA_HOME if set, otherwise fall back to ~/.local/share
        let base = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|h| {
                    let mut p = PathBuf::from(h);
                    p.push(".local/share");
                    p
                })
            });
        if let Some(mut p) = base {
            p.push("sequins/logs");
            return p;
        }
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(appdata) = std::env::var_os("APPDATA") {
            let mut p = PathBuf::from(appdata);
            p.push("Sequins\\Logs");
            return p;
        }
    }

    PathBuf::from("/tmp/sequins-logs")
}
