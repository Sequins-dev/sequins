//! Sequins CLI — query, ingest, and configuration operations
//!
//! The query-serving daemon lives in the production distribution (`sequins-pro`);
//! this CLI provides client and local-storage utilities only.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use sequins_storage::StorageConfig;
use std::fs;
use std::path::PathBuf;

mod ingest;
mod query;
mod storage;

#[derive(Parser, Debug)]
#[command(name = "sequins")]
#[command(about = "Sequins CLI - query, ingest, and configuration operations")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
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
