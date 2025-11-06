use crate::models::{SpanId, Timestamp, TraceId};
use serde::{Deserialize, Serialize};

/// Query parameters for fetching traces
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceQuery {
    /// Filter by service name (optional)
    pub service: Option<String>,

    /// Time range start (inclusive)
    pub start_time: Timestamp,

    /// Time range end (inclusive)
    pub end_time: Timestamp,

    /// Filter by minimum duration (optional)
    pub min_duration: Option<i64>,

    /// Filter by maximum duration (optional)
    pub max_duration: Option<i64>,

    /// Filter to only traces with errors (optional)
    pub has_error: Option<bool>,

    /// Limit number of results
    pub limit: Option<usize>,
}

/// Query parameters for fetching logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogQuery {
    /// Filter by service name (optional)
    pub service: Option<String>,

    /// Time range start (inclusive)
    pub start_time: Timestamp,

    /// Time range end (inclusive)
    pub end_time: Timestamp,

    /// Filter by severity level (optional)
    pub severity: Option<String>,

    /// Full-text search in log body (optional)
    pub search: Option<String>,

    /// Filter by trace ID (optional)
    pub trace_id: Option<TraceId>,

    /// Limit number of results
    pub limit: Option<usize>,
}

/// Query parameters for fetching metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricQuery {
    /// Filter by metric name (optional)
    pub name: Option<String>,

    /// Filter by service name (optional)
    pub service: Option<String>,

    /// Time range start (inclusive)
    pub start_time: Timestamp,

    /// Time range end (inclusive)
    pub end_time: Timestamp,

    /// Limit number of results
    pub limit: Option<usize>,
}

/// Query parameters for fetching profiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileQuery {
    /// Filter by service name (optional)
    pub service: Option<String>,

    /// Filter by profile type (optional)
    pub profile_type: Option<String>,

    /// Time range start (inclusive)
    pub start_time: Timestamp,

    /// Time range end (inclusive)
    pub end_time: Timestamp,

    /// Filter by trace ID (optional)
    pub trace_id: Option<TraceId>,

    /// Limit number of results
    pub limit: Option<usize>,
}

/// Simplified service information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    /// Service name
    pub name: String,

    /// Number of spans from this service
    pub span_count: usize,

    /// Number of logs from this service
    pub log_count: usize,
}

/// Complete trace with all spans
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    /// Trace ID
    pub trace_id: TraceId,

    /// Root span ID
    pub root_span_id: SpanId,

    /// All spans in the trace
    pub spans: Vec<crate::models::Span>,

    /// Total duration of the trace
    pub duration: i64,

    /// Whether the trace contains any errors
    pub has_error: bool,
}
