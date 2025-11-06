use crate::error::Result;
use crate::models::{
    LogEntry, LogId, LogQuery, Metric, MetricId, MetricQuery, Profile, ProfileId, ProfileQuery,
    QueryTrace, Service, Span, SpanId, TraceId, TraceQuery,
};

/// Trait for read-only data access operations
///
/// This trait provides the interface for querying telemetry data from storage.
/// It is implemented by both `TieredStorage` (for local access) and `QueryClient`
/// (for remote HTTP access to the daemon's Query API server on port 8080).
///
/// **Design principle:** The app UI should be identical whether running in local
/// or remote mode - it just calls QueryApi methods. The implementation details
/// (direct storage vs HTTP) are hidden behind the trait.
#[async_trait::async_trait]
pub trait QueryApi: Send + Sync {
    /// Get list of services with span/log counts
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_services(&self) -> Result<Vec<Service>>;

    /// Query traces with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<QueryTrace>>;

    /// Get all spans for a specific trace
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>>;

    /// Get a single span by trace ID and span ID
    ///
    /// Span IDs are only unique within a trace context, so both IDs are required
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>>;

    /// Query log entries with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>>;

    /// Get a single log entry by ID
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_log(&self, log_id: LogId) -> Result<Option<LogEntry>>;

    /// Query metrics with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn query_metrics(&self, query: MetricQuery) -> Result<Vec<Metric>>;

    /// Get a single metric by ID
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_metric(&self, metric_id: MetricId) -> Result<Option<Metric>>;

    /// Query profiles with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_profiles(&self, query: ProfileQuery) -> Result<Vec<Profile>>;

    /// Get a single profile by ID
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_profile(&self, profile_id: ProfileId) -> Result<Option<Profile>>;
}
