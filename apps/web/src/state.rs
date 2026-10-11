use minijinja::Environment;
use sequins_client::RemoteClient;
use std::path::PathBuf;
use std::sync::Arc;

/// Template renderer.
///
/// In development (templates directory exists on disk), creates a fresh minijinja
/// `Environment` on every `render()` call so that edits to HTML/Jinja files take
/// effect immediately without restarting the server.
///
/// In production (directory absent, compiled-in fallback), uses a cached
/// `Arc<Environment<'static>>` built once at startup.
#[derive(Clone)]
pub enum Templates {
    /// Dev: load from disk on every render.
    Dev(Arc<PathBuf>),
    /// Prod: use compiled-in cached environment.
    Prod(Arc<Environment<'static>>),
}

impl Templates {
    pub fn render(
        &self,
        template: &str,
        ctx: impl serde::Serialize,
    ) -> Result<String, minijinja::Error> {
        match self {
            Templates::Dev(dir) => {
                let mut env = Environment::new();
                env.set_loader(minijinja::path_loader(dir.as_path()));
                env.get_template(template)?.render(ctx)
            }
            Templates::Prod(env) => env.get_template(template)?.render(ctx),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub backend: Arc<RemoteClient>,
    pub templates: Templates,
    /// Incremented each time a new live-logs SSE connection starts.
    /// Old streaming loops watch for a change and terminate when superseded.
    pub logs_gen_tx: Arc<tokio::sync::watch::Sender<u64>>,
    /// Same pattern for metrics.
    pub metrics_gen_tx: Arc<tokio::sync::watch::Sender<u64>>,
    /// Same pattern for health.
    pub health_gen_tx: Arc<tokio::sync::watch::Sender<u64>>,
}

impl AppState {
    pub fn render(
        &self,
        template: &str,
        ctx: impl serde::Serialize,
    ) -> Result<String, minijinja::Error> {
        self.templates.render(template, ctx)
    }
}
