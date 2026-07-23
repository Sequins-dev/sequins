use anyhow::{Context, Result};
use clap::Parser;
use sequins_client::RemoteClient;
use std::sync::Arc;
use tracing::info;

mod app;
mod error;
mod query;
mod routes;
mod state;
mod stream;

#[derive(Parser, Debug)]
#[command(name = "sequins-web")]
#[command(about = "Sequins Web UI — connects to a running Sequins daemon")]
struct Cli {
    /// Arrow Flight SQL URL of the Sequins daemon
    #[arg(long, default_value = "http://localhost:4319")]
    query_url: String,

    /// HTTP bind address
    #[arg(long, default_value = "0.0.0.0:3000")]
    bind: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let backend =
        Arc::new(RemoteClient::new(&cli.query_url).context("Failed to create remote client")?);

    let app = app::build(backend);

    info!("Sequins Web UI listening on http://{}", cli.bind);
    let listener = tokio::net::TcpListener::bind(&cli.bind)
        .await
        .with_context(|| format!("Failed to bind to {}", cli.bind))?;

    axum::serve(listener, app).await.context("Web server error")
}
