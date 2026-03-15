use crate::error::Result;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    profiles::v1development::{ExportProfilesServiceRequest, ExportProfilesServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};

/// Trait for OTLP protocol ingestion operations
///
/// This trait defines the interface for ingesting telemetry data from OTLP sources.
/// It accepts OTLP proto types directly and returns OTLP response types.
/// All decomposition and normalization is handled by the storage layer.
///
/// This is implemented by `TieredStorage` for local storage operations.
///
/// **Not implemented by remote clients** - OTLP data goes directly to the daemon's
/// OTLP endpoints (ports 4317/4318), not through the Query/Management APIs.
#[async_trait::async_trait]
pub trait OtlpIngest: Send + Sync {
    /// Ingest trace data from OTLP request
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_traces(
        &self,
        request: ExportTraceServiceRequest,
    ) -> Result<ExportTraceServiceResponse>;

    /// Ingest log data from OTLP request
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_logs(
        &self,
        request: ExportLogsServiceRequest,
    ) -> Result<ExportLogsServiceResponse>;

    /// Ingest metrics data from OTLP request
    ///
    /// Handles all metric types (gauges, counters, histograms, summaries)
    /// and performs decomposition into normalized storage format.
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_metrics(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> Result<ExportMetricsServiceResponse>;

    /// Ingest profiling data from OTLP request
    ///
    /// Decomposes profiles into normalized tables (frames, stacks, samples, mappings)
    /// with full deduplication.
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn ingest_profiles(
        &self,
        request: ExportProfilesServiceRequest,
    ) -> Result<ExportProfilesServiceResponse>;
}
