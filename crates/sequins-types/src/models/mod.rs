/// ID types (TraceId, SpanId, LogId, MetricId, ProfileId)
pub mod ids;
/// Log entry models and severity levels
pub mod logs;
/// Metric models, data points, and histogram types
pub mod metrics;
/// Profile models and types
pub mod profiles;
/// Resource model (deduplicated entity producing telemetry)
pub mod resource;
/// InstrumentationScope model (instrumentation library metadata)
pub mod scope;
/// Storage-related types (retention policies, statistics)
pub mod storage;
/// Time types (Timestamp, Duration, TimeRange, TimeWindow)
pub mod time;
/// Trace and span models
pub mod traces;

// Re-export for convenience
pub use ids::{SpanId, TraceId};
pub use logs::{LogEntry, LogId, LogSeverity};
pub use metrics::{
    Exemplar, ExponentialHistogramDataPoint, GroupingPattern, HistogramDataPoint, Metric,
    MetricDataPoint, MetricGroup, MetricId, MetricType, VisualizationType, PROM_HISTOGRAM_SUFFIXES,
    PROM_SUMMARY_SUFFIXES, STAT_SUFFIXES,
};
pub use profiles::{
    Profile, ProfileFrame, ProfileId, ProfileMapping, ProfileSample, ProfileStack, ProfileType,
    StackFrame,
};
pub use resource::Resource;
pub use scope::InstrumentationScope;
pub use storage::{MaintenanceStats, RetentionPolicy, StorageStats};
pub use time::{Duration, TimeRange, TimeWindow, Timestamp};
pub use traces::{
    AttributeValue, Span, SpanEvent, SpanKind, SpanLink, SpanStatus, Trace, TraceStatus,
};
