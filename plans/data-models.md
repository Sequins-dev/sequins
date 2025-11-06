# Data Models

[← Back to Index](INDEX.md)

**Related Documentation:** [database.md](database.md) | [otlp-ingestion.md](otlp-ingestion.md) | [ui-design.md](ui-design.md)

---

## Core ID Types

```rust
use uuid::Uuid;

/// Strongly-typed ID for Services (internal identifier, not from OTLP)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServiceId(Uuid);

/// Strongly-typed ID for Log entries (internal identifier, not from OTLP)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogId(Uuid);

/// Strongly-typed ID for Metrics (internal identifier, not from OTLP)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetricId(Uuid);

/// Strongly-typed ID for Profiles (internal identifier, not from OTLP)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProfileId(Uuid);

impl ServiceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl LogId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl MetricId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl ProfileId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}
```

## Service Model

```rust
pub struct Service {
    pub id: ServiceId,           // Strongly-typed UUID
    pub name: String,            // e.g., "api-gateway" (from OTLP resource.service.name)
    pub instance_id: String,     // e.g., "api-gateway-7f8d9c-abc123" (arbitrary format)
    pub first_seen: Timestamp,   // When first seen (nanosecond precision)
    pub last_seen: Timestamp,    // Most recent activity (nanosecond precision)
    pub attributes: HashMap<String, String>, // Resource attributes from OTLP
}
```

## Trace & Span Models

```rust
use opentelemetry::trace::{TraceId, SpanId};

pub struct Trace {
    pub trace_id: TraceId,       // OpenTelemetry 16-byte trace ID
    pub root_span_id: SpanId,    // Entry span of the trace
    pub service_name: String,    // Root service
    pub start_time: Timestamp,   // Earliest span start (nanosecond precision)
    pub end_time: Timestamp,     // Latest span end (nanosecond precision)
    pub duration: Duration,      // Total duration (calculated as end_time - start_time)
    pub span_count: usize,       // Number of spans
    pub status: TraceStatus,     // Ok, Error, Unset
}

pub struct Span {
    pub span_id: SpanId,         // OpenTelemetry 8-byte span ID
    pub trace_id: TraceId,       // Parent trace
    pub parent_span_id: Option<SpanId>, // Parent span (None for root)
    pub service_name: String,    // Service that created this span
    pub name: String,            // Operation name
    pub kind: SpanKind,          // Server, Client, Internal, etc.
    pub start_time: Timestamp,   // Span start (nanosecond precision)
    pub end_time: Timestamp,     // Span end (nanosecond precision)
    pub duration: Duration,      // Calculated duration (end_time - start_time)
    pub status: SpanStatus,      // Ok, Error, Unset
    pub attributes: HashMap<String, AttributeValue>,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
}

pub enum SpanKind {
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

pub struct SpanEvent {
    pub name: String,
    pub timestamp: Timestamp,    // Event time (nanosecond precision)
    pub attributes: HashMap<String, AttributeValue>,
}
```

## Log Model

```rust
use opentelemetry::trace::{TraceId, SpanId};

pub struct LogEntry {
    pub id: LogId,               // Strongly-typed UUID (internal, not from OTLP)
    pub timestamp: Timestamp,    // Log timestamp (nanosecond precision, from OTLP)
    pub observed_timestamp: Timestamp, // When we received it (nanosecond precision, internal)
    pub service_name: String,    // Source service (from OTLP resource)
    pub severity: LogSeverity,   // Debug, Info, Warn, Error, Fatal (from OTLP)
    pub body: String,            // Log message (from OTLP)
    pub attributes: HashMap<String, AttributeValue>, // From OTLP
    pub trace_id: Option<TraceId>, // Link to trace (from OTLP trace context)
    pub span_id: Option<SpanId>,  // Link to span (from OTLP trace context)
    pub resource: HashMap<String, String>, // Resource attributes (from OTLP)
}

pub enum LogSeverity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}
```

## Metric Models

```rust
pub struct Metric {
    pub id: MetricId,            // Strongly-typed UUID (internal, not from OTLP)
    pub name: String,            // e.g., "http.server.duration" (from OTLP)
    pub description: String,     // From OTLP metric descriptor
    pub unit: String,            // e.g., "ms", "bytes" (from OTLP)
    pub metric_type: MetricType, // From OTLP metric type
    pub service_name: String,    // From OTLP resource
}

pub enum MetricType {
    Gauge,
    Counter,
    Histogram,
    Summary,
}

pub struct MetricDataPoint {
    pub metric_id: MetricId,     // Strongly-typed reference to parent Metric
    pub timestamp: Timestamp,    // Data point time (nanosecond precision, from OTLP)
    pub value: f64,              // For gauge/counter (from OTLP)
    pub attributes: HashMap<String, String>, // From OTLP
}

pub struct HistogramDataPoint {
    pub metric_id: MetricId,     // Strongly-typed reference to parent Metric
    pub timestamp: Timestamp,    // Data point time (nanosecond precision, from OTLP)
    pub count: u64,              // From OTLP histogram
    pub sum: f64,                // From OTLP histogram
    pub bucket_counts: Vec<u64>, // From OTLP histogram
    pub explicit_bounds: Vec<f64>, // From OTLP histogram
    pub exemplars: Vec<Exemplar>, // From OTLP exemplars
    pub attributes: HashMap<String, String>, // From OTLP
}
```

## Profile Model

```rust
pub struct Profile {
    pub id: ProfileId,           // Strongly-typed UUID (internal, not from OTLP)
    pub timestamp: Timestamp,    // When profile was captured (nanosecond precision, from profiling data)
    pub service_name: String,    // Source service (from resource attributes)
    pub profile_type: String,    // "cpu", "memory", "goroutine" (from pprof data)
    pub sample_type: String,     // "samples", "count", "bytes" (from pprof data)
    pub sample_unit: String,     // "count", "nanoseconds", "bytes" (from pprof data)
    pub data: Vec<u8>,           // pprof binary data (vendor-specific format)
    pub trace_id: Option<TraceId>, // Link to trace if available (from trace context)
}

pub struct FlameGraphNode {
    pub name: String,            // Function name
    pub file: Option<String>,    // Source file
    pub line: Option<u32>,       // Line number
    pub value: i64,              // Sample count
    pub children: Vec<FlameGraphNode>,
}
```

## Type Conventions

### ID Types: OTLP vs Internal

**OTLP-Defined IDs** (from OpenTelemetry specification):
- `TraceId` - 16-byte (128-bit) trace identifier from OTLP
- `SpanId` - 8-byte (64-bit) span identifier from OTLP
- These are the ONLY fixed-format IDs defined in the OTLP spec
- We use `opentelemetry::trace::{TraceId, SpanId}` types directly

**Internal IDs** (generated by Sequins, not from OTLP):
- `ServiceId(Uuid)` - Internal identifier for service entities
- `LogId(Uuid)` - Internal identifier for log entries (OTLP logs have no ID)
- `MetricId(Uuid)` - Internal identifier for metric definitions
- `ProfileId(Uuid)` - Internal identifier for profiles (not in core OTLP)

**Why strongly-typed internal IDs?**
- **Type safety:** Cannot accidentally use `ServiceId` where `MetricId` is expected
- **Efficient storage:** UUID is 16 bytes (same as storing hex string but more efficient)
- **Self-documenting:** Code clearly shows relationships (e.g., `MetricDataPoint.metric_id: MetricId`)
- **Compiler-enforced:** Rust prevents ID type confusion at compile time

### OpenTelemetry Types

The models use OpenTelemetry's native types for trace and span identifiers:
- `opentelemetry::trace::TraceId` - 16-byte trace identifier
- `opentelemetry::trace::SpanId` - 8-byte span identifier

These types provide:
- Standard formatting (hex strings for display)
- Parsing from hex strings
- Efficient binary representation
- Type safety preventing ID confusion

### Timestamps and Durations

All timestamps and durations use **newtype wrappers around i64 nanoseconds** for both efficiency and ergonomics:

```rust
use chrono::{DateTime, Utc};

/// Nanosecond-precision duration (wraps i64 for efficiency)
/// Can be negative (useful for time differences and calculations)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Duration(i64);

impl Duration {
    /// Create from nanoseconds
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Get raw nanoseconds (for Parquet storage)
    pub fn as_nanos(&self) -> i64 {
        self.0
    }

    /// Convert to milliseconds (common for display)
    pub fn as_millis(&self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }

    /// Convert to seconds
    pub fn as_secs(&self) -> f64 {
        self.0 as f64 / 1_000_000_000.0
    }

    /// Create from milliseconds
    pub fn from_millis(millis: f64) -> Self {
        Self((millis * 1_000_000.0) as i64)
    }

    /// Create from seconds
    pub fn from_secs(secs: f64) -> Self {
        Self((secs * 1_000_000_000.0) as i64)
    }

    /// Zero duration
    pub fn zero() -> Self {
        Self(0)
    }

    /// Check if negative
    pub fn is_negative(&self) -> bool {
        self.0 < 0
    }

    /// Absolute value
    pub fn abs(&self) -> Self {
        Self(self.0.abs())
    }

    /// Human-readable display (e.g., "1.5s", "250ms", "50µs")
    pub fn display(&self) -> String {
        let abs_nanos = self.0.abs();
        if abs_nanos >= 1_000_000_000 {
            format!("{:.2}s", self.as_secs())
        } else if abs_nanos >= 1_000_000 {
            format!("{:.2}ms", self.as_millis())
        } else if abs_nanos >= 1_000 {
            format!("{:.2}µs", abs_nanos as f64 / 1_000.0)
        } else {
            format!("{}ns", abs_nanos)
        }
    }
}

impl From<i64> for Duration {
    fn from(nanos: i64) -> Self {
        Self(nanos)
    }
}

impl From<std::time::Duration> for Duration {
    fn from(d: std::time::Duration) -> Self {
        Self(d.as_nanos() as i64)
    }
}

/// Nanosecond-precision timestamp (wraps i64 for efficiency)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Create from nanoseconds (OTLP format)
    pub fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Get raw nanoseconds (for OTLP and Parquet)
    pub fn as_nanos(&self) -> i64 {
        self.0
    }

    /// Convert to DateTime for display and time arithmetic
    pub fn as_datetime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_nanos(self.0)
    }

    /// Create from DateTime
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self(dt.timestamp_nanos_opt().unwrap())
    }

    /// Current time
    pub fn now() -> Self {
        Self::from_datetime(Utc::now())
    }

    /// Add duration
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration.as_nanos()).map(Self)
    }

    /// Subtract duration
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        self.0.checked_sub(duration.as_nanos()).map(Self)
    }

    /// Calculate duration since earlier timestamp
    pub fn duration_since(&self, earlier: Timestamp) -> Duration {
        Duration::from_nanos(self.0 - earlier.0)
    }
}

impl From<i64> for Timestamp {
    fn from(nanos: i64) -> Self {
        Self(nanos)
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Self::from_datetime(dt)
    }
}

/// Time window for queries (start/end range)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeWindow {
    start: Timestamp,
    end: Timestamp,
}

impl TimeWindow {
    /// Create new time window (validates end >= start)
    pub fn new(start: Timestamp, end: Timestamp) -> Result<Self, String> {
        if end < start {
            return Err(format!(
                "Invalid time window: end ({}) before start ({})",
                end.as_datetime(), start.as_datetime()
            ));
        }
        Ok(Self { start, end })
    }

    /// Create unchecked (use when you know start <= end)
    pub fn new_unchecked(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Get start timestamp
    pub fn start(&self) -> Timestamp {
        self.start
    }

    /// Get end timestamp
    pub fn end(&self) -> Timestamp {
        self.end
    }

    /// Calculate duration of the window
    pub fn duration(&self) -> Duration {
        self.end.duration_since(self.start)
    }

    /// Check if timestamp falls within this window (inclusive)
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }

    /// Check if this window overlaps with another
    pub fn overlaps(&self, other: &TimeWindow) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    /// Create window for last N minutes
    pub fn last_minutes(minutes: u32) -> Self {
        let end = Timestamp::now();
        let start = end.checked_sub(Duration::from_secs((minutes as f64) * 60.0)).unwrap();
        Self { start, end }
    }

    /// Create window for last hour
    pub fn last_hour() -> Self {
        Self::last_minutes(60)
    }

    /// Create window for last 24 hours
    pub fn last_day() -> Self {
        Self::last_minutes(24 * 60)
    }

    /// Create window for last 7 days
    pub fn last_week() -> Self {
        Self::last_minutes(7 * 24 * 60)
    }

    /// Create window for entire retention period
    pub fn all(retention_hours: u32) -> Self {
        let end = Timestamp::now();
        let start = end.checked_sub(Duration::from_secs((retention_hours as f64) * 3600.0)).unwrap();
        Self { start, end }
    }
}
```

**Benefits of Timestamp, Duration & TimeWindow:**

**Timestamp:**
- **Storage efficiency:** Only 8 bytes (same as i64)
- **Zero-cost OTLP conversion:** Direct field access (`timestamp.as_nanos()`)
- **Efficient Parquet:** Stored as INT64 without conversion
- **Ergonomic API:** Provides helper methods when needed
- **Type safety:** Cannot accidentally use Duration where Timestamp expected
- **Lazy conversion:** Only convert to DateTime when actually needed for display

**Duration:**
- **Storage efficiency:** Only 8 bytes (same as i64)
- **Type safety:** Cannot accidentally add two timestamps when you meant timestamp + duration
- **Supports negative values:** Unlike `std::time::Duration`, useful for time differences
- **Human-readable display:** Automatic formatting (1.5s, 250ms, 50µs)
- **Unit conversion helpers:** Easy conversion between ns/µs/ms/s
- **Perfect for latency metrics:** P50, P95, P99 calculations with sorted `Vec<Duration>`

**TimeWindow:**
- **Type safety:** Single parameter instead of separate start/end (prevents argument order mistakes)
- **Validation:** Ensures end >= start at construction time
- **Ergonomic API:** `window.contains(timestamp)`, `window.overlaps(other)`
- **Common patterns:** `TimeWindow::last_hour()`, `TimeWindow::last_day()`
- **Query clarity:** `query_traces(window)` is clearer than `query_traces(start, end)`
- **Copy semantics:** Only 16 bytes (2 × Timestamp), cheap to pass around

**Performance:**
- Comparisons: Same as i64 (implements Ord)
- Storage: Same as i64 (8 bytes, serde transparent)
- Arithmetic: Fast integer math with helper methods
- Display: Converts to DateTime only when needed

**Comparison:**

| Aspect | Bare i64 | DateTime/std::Duration | **Timestamp/Duration (Newtype)** |
|--------|----------|------------------------|----------------------------------|
| Size | 8 bytes | 12 bytes | **8 bytes** ✅ |
| OTLP Conversion | None | Required | **None** ✅ |
| Parquet I/O | Direct | Conversion | **Direct** ✅ |
| Range Queries | Fast | Fast | **Fast** ✅ |
| Memory (1M records) | 16 MB | 24 MB | **16 MB** ✅ |
| Type Safety | ❌ No distinction | ✅ Type-safe | **✅ Type-safe + distinguishes Timestamp vs Duration** |
| Ergonomic API | ❌ | ✅ | **✅** |
| Unit Mistakes | Easy | Impossible | **Impossible** ✅ |
| Negative Durations | ✅ | ❌ std::Duration can't be negative | **✅ Useful for time diffs** |
| Display Formatting | Manual | Built-in | **Built-in + automatic unit selection** ✅ |

**Usage Examples:**

```rust
// === Timestamp Examples ===

// OTLP ingestion - zero conversion
let timestamp = Timestamp::from_nanos(otlp_log.time_unix_nano);

// Parquet write - zero conversion
parquet_writer.write_i64(log.timestamp.as_nanos());

// Range query - fast integer comparison
logs.iter()
    .filter(|log| log.timestamp >= start && log.timestamp <= end)
    .collect()

// Display in UI - convert only when needed
println!("{}", log.timestamp.as_datetime().format("%Y-%m-%d %H:%M:%S"));

// Time arithmetic with Duration
let one_hour = Duration::from_secs(3600.0);
let one_hour_ago = Timestamp::now().checked_sub(one_hour).unwrap();

// === Duration Examples ===

// Calculate span duration
let span_duration = span.end_time.duration_since(span.start_time);

// Or use precomputed duration field
println!("Span took {}", span.duration.display());  // "1.5s", "250ms", etc.

// Filter slow traces
traces.iter()
    .filter(|trace| trace.duration >= Duration::from_millis(100.0))
    .collect()

// Aggregate metrics - p95 latency
let mut durations: Vec<Duration> = spans.iter().map(|s| s.duration).collect();
durations.sort();
let p95 = durations[(durations.len() * 95) / 100];
println!("P95 latency: {}", p95.display());

// Display human-readable durations
Duration::from_nanos(1_500_000_000).display()  // "1.50s"
Duration::from_nanos(250_000_000).display()    // "250.00ms"
Duration::from_nanos(50_000).display()         // "50.00µs"
Duration::from_nanos(100).display()            // "100ns"
```

### String Identifiers

Some identifiers remain as `String` because they have **arbitrary formats**:
- `service.instance_id` - Arbitrary format defined by the service (e.g., "pod-abc123")
- `service.name` - Human-readable name from OTLP resource (e.g., "api-gateway")
- Metric names - Dot-separated paths (e.g., "http.server.duration")
- Span names - Operation names (e.g., "GET /users/:id")

### Attributes

Attributes use `HashMap<String, AttributeValue>` where `AttributeValue` is an enum supporting:
- String values
- Integer values
- Float values
- Boolean values
- Array values

This matches the OpenTelemetry attribute model.

---

**Last Updated:** 2025-11-05
