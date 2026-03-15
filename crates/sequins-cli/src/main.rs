//! Sequins CLI — query, ingest, and daemon operations

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use sequins_server::{flight_service_server, ManagementServer, OtlpServer};
use sequins_storage::{DataFusionBackend, Storage, StorageConfig};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

mod ingest;
mod query;

#[derive(Parser, Debug)]
#[command(name = "sequins")]
#[command(about = "Sequins CLI - query, ingest, and daemon operations")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the daemon with all servers
    Start {
        /// Path to storage configuration file
        #[arg(short, long, default_value = "sequins-storage.yaml")]
        config: PathBuf,

        /// Arrow Flight SQL gRPC bind address
        #[arg(long, default_value = "0.0.0.0:4319")]
        flight_addr: String,

        /// Management API bind address
        #[arg(long, default_value = "0.0.0.0:8081")]
        management_addr: String,

        /// OTLP gRPC bind address
        #[arg(long, default_value = "0.0.0.0:4317")]
        otlp_grpc_addr: String,

        /// OTLP HTTP bind address
        #[arg(long, default_value = "0.0.0.0:4318")]
        otlp_http_addr: String,
    },

    /// Execute a SeQL query
    Query {
        /// SeQL query string
        query: String,

        /// Database path (local) or query endpoint URL (remote)
        #[arg(short, long)]
        target: String,

        /// Output format
        #[arg(short, long, default_value = "table")]
        format: OutputFormat,
    },

    /// Ingest telemetry data from OTLP files
    Ingest {
        /// Path to OTLP data file
        file: PathBuf,

        /// Signal type (auto-detected if not specified)
        #[arg(short, long, value_enum)]
        signal: Option<SignalType>,

        /// Format (auto-detected if not specified)
        #[arg(short, long, value_enum)]
        format: Option<IngestFormat>,

        /// Database path (local) or OTLP endpoint URL (remote)
        #[arg(short, long)]
        target: String,
    },

    /// Generate a default configuration file
    Init {
        /// Output path
        #[arg(short, long, default_value = "sequins-storage.yaml")]
        output: PathBuf,

        /// Overwrite existing file
        #[arg(short, long)]
        force: bool,
    },

    /// Validate a configuration file
    Validate {
        /// Path to configuration file
        #[arg(short, long, default_value = "sequins-storage.yaml")]
        config: PathBuf,
    },

    /// Show version information
    Version,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable table
    Table,
    /// JSON output
    Json,
    /// JSON Lines (one object per line)
    Jsonl,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum SignalType {
    /// Trace spans
    Traces,
    /// Log entries
    Logs,
    /// Metrics
    Metrics,
    /// Profiles
    Profiles,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum IngestFormat {
    /// Protocol Buffers (binary)
    Protobuf,
    /// JSON
    Json,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Start {
            config,
            flight_addr,
            management_addr,
            otlp_grpc_addr,
            otlp_http_addr,
        } => {
            info!("Starting Sequins Daemon");

            let storage_config = load_config(&config)?;
            let storage = Arc::new(
                Storage::new(storage_config)
                    .await
                    .context("Failed to initialize storage")?,
            );

            let flush_handle = Storage::start_background_flush(Arc::clone(&storage));

            // Wrap storage in DataFusion backend for query execution
            let backend: Arc<dyn sequins_query::QueryExec> =
                Arc::new(DataFusionBackend::new(Arc::clone(&storage)));

            let flight_svc = flight_service_server(backend);
            let management_server = ManagementServer::new(Arc::clone(&storage));
            let otlp_server = OtlpServer::new(Arc::clone(&storage));

            info!("Flight SQL gRPC on {}", flight_addr);
            info!("Management API on {}", management_addr);
            info!("OTLP gRPC on {}", otlp_grpc_addr);
            info!("OTLP HTTP on {}", otlp_http_addr);

            let flight_listener = tokio::net::TcpListener::bind(&flight_addr)
                .await
                .with_context(|| format!("Failed to bind Flight SQL to {}", flight_addr))?;
            let flight_incoming = tokio_stream::wrappers::TcpListenerStream::new(flight_listener);

            tokio::select! {
                result = tonic::transport::Server::builder()
                    .add_service(flight_svc)
                    .serve_with_incoming(flight_incoming) =>
                {
                    error!("Flight SQL server stopped: {:?}", result);
                    result.context("Flight SQL server error")?;
                }
                result = management_server.serve(&management_addr) => {
                    error!("Management server stopped: {:?}", result);
                    result.context("Management server error")?;
                }
                result = otlp_server.serve(&otlp_grpc_addr, &otlp_http_addr) => {
                    error!("OTLP server stopped: {:?}", result);
                    result.context("OTLP server error")?;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Shutdown signal received");
                    storage.shutdown();
                    let _ = flush_handle.await;
                    info!("Shutdown complete");
                }
            }

            Ok(())
        }

        Command::Query {
            query: query_str,
            target,
            format,
        } => query::execute(query_str, target, format).await,

        Command::Ingest {
            file,
            signal,
            format,
            target,
        } => ingest::execute(file, signal, format, target).await,

        Command::Init { output, force } => {
            if output.exists() && !force {
                anyhow::bail!(
                    "Configuration file already exists at {}. Use --force to overwrite.",
                    output.display()
                );
            }

            let config = StorageConfig::default();
            let yaml =
                serde_yaml::to_string(&config).context("Failed to serialize configuration")?;
            fs::write(&output, yaml)
                .with_context(|| format!("Failed to write to {}", output.display()))?;

            println!("Created configuration at {}", output.display());
            Ok(())
        }

        Command::Validate { config } => {
            let _ = load_config(&config)?;
            println!("Configuration is valid: {}", config.display());
            Ok(())
        }

        Command::Version => {
            println!("sequins {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

fn load_config(path: &PathBuf) -> Result<StorageConfig> {
    let yaml =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    StorageConfig::from_yaml(&yaml).map_err(|e| anyhow::anyhow!("Invalid configuration: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parse_start_command() {
        let args = vec![
            "sequins",
            "start",
            "--config",
            "test-config.yaml",
            "--flight-addr",
            "127.0.0.1:4319",
        ];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok(), "Expected start command to parse successfully");

        match cli.unwrap().command {
            Command::Start {
                config,
                flight_addr,
                ..
            } => {
                assert_eq!(config, PathBuf::from("test-config.yaml"));
                assert_eq!(flight_addr, "127.0.0.1:4319");
            }
            _ => panic!("Expected Start command"),
        }
    }

    #[test]
    fn test_cli_parse_query_command() {
        let args = vec![
            "sequins",
            "query",
            "spans | select trace_id",
            "--target",
            "http://localhost:8080",
            "--format",
            "json",
        ];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok(), "Expected query command to parse successfully");

        match cli.unwrap().command {
            Command::Query {
                query,
                target,
                format,
            } => {
                assert_eq!(query, "spans | select trace_id");
                assert_eq!(target, "http://localhost:8080");
                assert!(matches!(format, OutputFormat::Json));
            }
            _ => panic!("Expected Query command"),
        }
    }

    #[test]
    fn test_cli_parse_ingest_command() {
        let args = vec![
            "sequins",
            "ingest",
            "traces.json",
            "--signal",
            "traces",
            "--format",
            "json",
            "--target",
            "/path/to/db",
        ];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok(), "Expected ingest command to parse successfully");

        match cli.unwrap().command {
            Command::Ingest {
                file,
                signal,
                format,
                target,
            } => {
                assert_eq!(file, PathBuf::from("traces.json"));
                assert!(matches!(signal, Some(SignalType::Traces)));
                assert!(matches!(format, Some(IngestFormat::Json)));
                assert_eq!(target, "/path/to/db");
            }
            _ => panic!("Expected Ingest command"),
        }
    }

    #[test]
    fn test_cli_parse_init_command() {
        let args = vec![
            "sequins",
            "init",
            "--output",
            "custom-config.yaml",
            "--force",
        ];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_ok(), "Expected init command to parse successfully");

        match cli.unwrap().command {
            Command::Init { output, force } => {
                assert_eq!(output, PathBuf::from("custom-config.yaml"));
                assert!(force);
            }
            _ => panic!("Expected Init command"),
        }
    }

    #[test]
    fn test_cli_parse_validate_command() {
        let args = vec!["sequins", "validate", "--config", "my-config.yaml"];
        let cli = Cli::try_parse_from(args);
        assert!(
            cli.is_ok(),
            "Expected validate command to parse successfully"
        );

        match cli.unwrap().command {
            Command::Validate { config } => {
                assert_eq!(config, PathBuf::from("my-config.yaml"));
            }
            _ => panic!("Expected Validate command"),
        }
    }

    #[test]
    fn test_cli_parse_version_command() {
        let args = vec!["sequins", "version"];
        let cli = Cli::try_parse_from(args);
        assert!(
            cli.is_ok(),
            "Expected version command to parse successfully"
        );

        assert!(matches!(cli.unwrap().command, Command::Version));
    }

    #[test]
    fn test_cli_parse_invalid_arguments() {
        // Missing required argument
        let args = vec!["sequins", "query"];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_err(), "Expected error when query string is missing");

        // Invalid subcommand
        let args = vec!["sequins", "invalid-command"];
        let cli = Cli::try_parse_from(args);
        assert!(cli.is_err(), "Expected error for invalid subcommand");
    }
}
