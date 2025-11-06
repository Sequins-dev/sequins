use crate::error::Result;
use crate::models::{
    LogEntry, LogId, LogQuery, Metric, MetricId, MetricQuery, Profile, ProfileId, ProfileQuery,
    QueryTrace, Span, SpanId, Timestamp, TraceId, TraceQuery,
};

/// Internal trait for read operations on storage sources
///
/// This trait provides a unified interface for querying telemetry data from
/// different storage tiers (hot, cold) and future remote nodes. It enables
/// the QueryAggregator to treat all sources uniformly.
///
/// # Design
///
/// - **Async**: All operations return futures for non-blocking I/O
/// - **Generic**: Works with in-memory, Parquet, and remote sources
/// - **Query-focused**: Optimized for filter-based queries, not full scans
///
/// # Implementors
///
/// - `HotTier`: In-memory storage (fast, recent data)
/// - `ColdTier`: Parquet files (slower, historical data)
/// - `RemoteNode`: HTTP queries to other nodes (future)
#[allow(async_fn_in_trait)]
pub trait StorageRead: Send + Sync {
    /// Query traces with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn query_traces(&self, query: &TraceQuery) -> Result<Vec<QueryTrace>>;

    /// Query log entries with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn query_logs(&self, query: &LogQuery) -> Result<Vec<LogEntry>>;

    /// Query metrics with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn query_metrics(&self, query: &MetricQuery) -> Result<Vec<Metric>>;

    /// Query profiles with filtering
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn query_profiles(&self, query: &ProfileQuery) -> Result<Vec<Profile>>;

    /// Get all spans for a specific trace
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>>;

    /// Get a single span by trace ID and span ID
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>>;

    /// Get a single log entry by ID
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_log(&self, log_id: LogId) -> Result<Option<LogEntry>>;

    /// Get a single metric by ID
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_metric(&self, metric_id: MetricId) -> Result<Option<Metric>>;

    /// Get a single profile by ID
    ///
    /// # Errors
    ///
    /// Returns an error if storage read fails
    async fn get_profile(&self, profile_id: ProfileId) -> Result<Option<Profile>>;
}

/// Internal trait for write operations on storage destinations
///
/// This trait provides a unified interface for writing telemetry data to
/// different storage backends. It separates write concerns from read concerns.
///
/// # Design
///
/// - **Batch-oriented**: Takes vectors for efficient bulk writes
/// - **Async**: Non-blocking I/O for Parquet and remote writes
/// - **Error-tolerant**: Returns Result for recoverable failures
///
/// # Implementors
///
/// - `ColdTier`: Writes to Parquet files on object storage
/// - `RemoteNode`: Forwards writes to other nodes (future)
///
/// Note: `HotTier` doesn't implement this as it uses direct insert methods
#[allow(async_fn_in_trait)]
pub trait StorageWrite: Send + Sync {
    /// Write spans to storage
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn write_spans(&self, spans: Vec<Span>) -> Result<()>;

    /// Write logs to storage
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn write_logs(&self, logs: Vec<LogEntry>) -> Result<()>;

    /// Write metrics to storage
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn write_metrics(&self, metrics: Vec<Metric>) -> Result<()>;

    /// Write profiles to storage
    ///
    /// # Errors
    ///
    /// Returns an error if storage write fails
    async fn write_profiles(&self, profiles: Vec<Profile>) -> Result<()>;
}

/// Metadata about a storage tier for query routing
///
/// This trait provides information that the QueryAggregator uses to
/// intelligently route queries to the most appropriate sources.
///
/// # Query Routing Strategy
///
/// 1. **Priority**: Query sources in priority order (0 = highest)
/// 2. **Time Range**: Skip sources that don't cover the query time range
/// 3. **Early Exit**: Stop querying when limit is reached
///
/// # Example
///
/// ```ignore
/// // Hot tier: Priority 0, covers last 15 minutes
/// impl TierMetadata for HotTier {
///     fn tier_id(&self) -> &str { "hot" }
///     fn priority(&self) -> u8 { 0 }
///     fn covers_time_range(&self, start, end) -> bool {
///         let now = Timestamp::now().unwrap();
///         let hot_cutoff = now - Duration::from_minutes(15);
///         start >= hot_cutoff
///     }
/// }
///
/// // Cold tier: Priority 10, covers all time
/// impl TierMetadata for ColdTier {
///     fn tier_id(&self) -> &str { "cold" }
///     fn priority(&self) -> u8 { 10 }
///     fn covers_time_range(&self, _start, _end) -> bool {
///         true  // Has all data
///     }
/// }
/// ```
pub trait TierMetadata {
    /// Unique identifier for this tier
    ///
    /// Used for logging and debugging
    fn tier_id(&self) -> &str;

    /// Query priority (lower = higher priority)
    ///
    /// The QueryAggregator queries sources in priority order:
    /// - 0: Highest priority (e.g., hot tier - fast, recent data)
    /// - 10: Medium priority (e.g., cold tier - slower, historical)
    /// - 100: Low priority (e.g., remote nodes - slow, distributed)
    fn priority(&self) -> u8;

    /// Check if this tier covers the given time range
    ///
    /// Used to skip querying tiers that definitely don't have relevant data.
    /// For example, the hot tier only covers the last 15 minutes, so queries
    /// for older data can skip it entirely.
    ///
    /// # Arguments
    ///
    /// * `start` - Start of query time range
    /// * `end` - End of query time range
    ///
    /// # Returns
    ///
    /// `true` if this tier *might* have data in the range, `false` if it
    /// definitely doesn't.
    fn covers_time_range(&self, start: Timestamp, end: Timestamp) -> bool;
}
