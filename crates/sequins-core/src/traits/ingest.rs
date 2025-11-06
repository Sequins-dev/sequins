use crate::error::Result;
use crate::models::{LogEntry, Metric, Profile, Span};

/// Trait for OTLP protocol ingestion operations
///
/// This trait defines the interface for ingesting telemetry data from OTLP sources.
/// It is implemented by `TieredStorage` for local storage operations.
///
/// **Not implemented by remote clients** - OTLP data goes directly to the daemon's
/// OTLP endpoints (ports 4317/4318), not through the Query/Management APIs.
#[async_trait::async_trait]
pub trait OtlpIngest: Send + Sync {
    /// Ingest trace spans
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()>;

    /// Ingest log entries
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_logs(&self, logs: Vec<LogEntry>) -> Result<()>;

    /// Ingest metrics
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()>;

    /// Ingest profiling data
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_profiles(&self, profiles: Vec<Profile>) -> Result<()>;
}
