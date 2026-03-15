/// Test fixtures for OTLP data types
use sequins::models::{
    logs::{LogEntry, LogId, LogSeverity},
    metrics::{HistogramDataPoint, Metric, MetricDataPoint, MetricId, MetricType},
    profiles::{Profile, ProfileId, ProfileType},
    traces::{AttributeValue, Span, SpanEvent, SpanKind, SpanStatus},
    Duration, SpanId, Timestamp, TraceId,
};
use std::collections::HashMap;

/// OTLP test fixtures
pub struct OtlpFixtures;

impl OtlpFixtures {
    // ============================================================================
    // TRACE FIXTURES
    // ============================================================================

    /// Create a valid trace with multiple spans
    pub fn valid_trace() -> Vec<Span> {
        let trace_id = Self::test_trace_id(1);
        let root_span_id = Self::test_span_id(1);
        let child_span_id = Self::test_span_id(2);

        vec![
            Self::valid_span_with_id(trace_id, root_span_id, None, "GET /api/users"),
            Self::valid_span_with_id(
                trace_id,
                child_span_id,
                Some(root_span_id),
                "query_database",
            ),
        ]
    }

    /// Create a valid span with default values
    pub fn valid_span() -> Span {
        let trace_id = Self::test_trace_id(1);
        let span_id = Self::test_span_id(1);
        Self::valid_span_with_id(trace_id, span_id, None, "test-operation")
    }

    /// Create a valid span with specific IDs
    pub fn valid_span_with_id(
        trace_id: TraceId,
        span_id: SpanId,
        parent_span_id: Option<SpanId>,
        operation_name: &str,
    ) -> Span {
        let start_time = Timestamp::from_secs(1700000000);
        let duration = Duration::from_millis(100);
        let end_time = start_time + duration;

        Span {
            trace_id,
            span_id,
            parent_span_id,
            service_name: "test-service".to_string(),
            operation_name: operation_name.to_string(),
            start_time,
            end_time,
            duration,
            attributes: Self::sample_attributes(),
            events: vec![Self::sample_span_event()],
            status: SpanStatus::Ok,
            span_kind: SpanKind::Server,
            resource: Self::sample_resource(),
        }
    }

    /// Create a span with HTTP semantic conventions
    pub fn span_with_http_attributes() -> Span {
        let mut span = Self::valid_span();
        span.attributes.insert(
            "http.method".to_string(),
            AttributeValue::String("GET".to_string()),
        );
        span.attributes
            .insert("http.status_code".to_string(), AttributeValue::Int(200));
        span.attributes.insert(
            "http.url".to_string(),
            AttributeValue::String("https://api.example.com/users".to_string()),
        );
        span
    }

    /// Create a span with unicode attributes
    #[allow(dead_code)]
    pub fn span_with_unicode() -> Span {
        let mut span = Self::valid_span();
        span.service_name = "テストサービス".to_string();
        span.operation_name = "测试操作".to_string();
        span.attributes.insert(
            "emoji".to_string(),
            AttributeValue::String("🚀 Rocket Launch 🎉".to_string()),
        );
        span.attributes.insert(
            "chinese".to_string(),
            AttributeValue::String("你好世界".to_string()),
        );
        span
    }

    /// Create a span with special characters that might break SQL/JSON
    pub fn span_with_special_chars() -> Span {
        let mut span = Self::valid_span();
        span.attributes.insert(
            "quotes".to_string(),
            AttributeValue::String(r#"It's a "test" with 'quotes'"#.to_string()),
        );
        span.attributes.insert(
            "backslashes".to_string(),
            AttributeValue::String(r"C:\Users\Test\Path".to_string()),
        );
        span.attributes.insert(
            "sql_injection".to_string(),
            AttributeValue::String("'; DROP TABLE spans; --".to_string()),
        );
        span.attributes.insert(
            "newlines".to_string(),
            AttributeValue::String("line1\nline2\r\nline3".to_string()),
        );
        span
    }

    fn sample_span_event() -> SpanEvent {
        SpanEvent {
            timestamp: Timestamp::from_secs(1700000050),
            name: "cache_miss".to_string(),
            attributes: {
                let mut attrs = HashMap::new();
                attrs.insert(
                    "cache.key".to_string(),
                    AttributeValue::String("user:123".to_string()),
                );
                attrs
            },
        }
    }

    // ============================================================================
    // LOG FIXTURES
    // ============================================================================

    /// Create a valid log entry
    pub fn valid_log() -> LogEntry {
        LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::from_secs(1700000000),
            observed_timestamp: Timestamp::from_secs(1700000001),
            service_name: "test-service".to_string(),
            severity: LogSeverity::Info,
            body: "Test log message".to_string(),
            attributes: Self::sample_attributes(),
            trace_id: None,
            span_id: None,
            resource: Self::sample_resource(),
        }
    }

    /// Create a log with trace correlation
    pub fn log_with_trace_correlation() -> LogEntry {
        let mut log = Self::valid_log();
        log.trace_id = Some(Self::test_trace_id(1));
        log.span_id = Some(Self::test_span_id(1));
        log
    }

    /// Create a log with unicode content
    #[allow(dead_code)]
    pub fn log_with_unicode() -> LogEntry {
        let mut log = Self::valid_log();
        log.body = "ユーザー登録完了: 用户注册成功 🎉".to_string();
        log
    }

    // ============================================================================
    // METRIC FIXTURES
    // ============================================================================

    /// Create a valid gauge metric
    pub fn valid_gauge() -> (Metric, Vec<MetricDataPoint>) {
        let metric_id = MetricId::new();
        let metric = Metric {
            id: metric_id,
            name: "system.cpu.usage".to_string(),
            description: "CPU usage percentage".to_string(),
            unit: "%".to_string(),
            metric_type: MetricType::Gauge,
            service_name: "test-service".to_string(),
            is_generated: false,
        };

        let data_points = vec![
            MetricDataPoint {
                metric_id,
                timestamp: Timestamp::from_secs(1700000000),
                value: 45.5,
                attributes: Self::sample_simple_attributes(),
            },
            MetricDataPoint {
                metric_id,
                timestamp: Timestamp::from_secs(1700000060),
                value: 52.3,
                attributes: Self::sample_simple_attributes(),
            },
        ];

        (metric, data_points)
    }

    /// Create a valid counter metric
    #[allow(dead_code)]
    pub fn valid_counter() -> (Metric, Vec<MetricDataPoint>) {
        let metric_id = MetricId::new();
        let metric = Metric {
            id: metric_id,
            name: "http.requests.total".to_string(),
            description: "Total HTTP requests".to_string(),
            unit: "1".to_string(),
            metric_type: MetricType::Counter,
            service_name: "test-service".to_string(),
            is_generated: false,
        };

        let data_points = vec![
            MetricDataPoint {
                metric_id,
                timestamp: Timestamp::from_secs(1700000000),
                value: 100.0,
                attributes: Self::sample_simple_attributes(),
            },
            MetricDataPoint {
                metric_id,
                timestamp: Timestamp::from_secs(1700000060),
                value: 150.0,
                attributes: Self::sample_simple_attributes(),
            },
        ];

        (metric, data_points)
    }

    /// Create a valid histogram metric
    pub fn valid_histogram() -> (Metric, Vec<HistogramDataPoint>) {
        let metric_id = MetricId::new();
        let metric = Metric {
            id: metric_id,
            name: "http.request.duration".to_string(),
            description: "HTTP request duration".to_string(),
            unit: "ms".to_string(),
            metric_type: MetricType::Histogram,
            service_name: "test-service".to_string(),
            is_generated: false,
        };

        let histogram_data = HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::from_secs(1700000000),
            count: 100,
            sum: 5000.0,
            bucket_counts: vec![50, 30, 15, 5],
            // Note: In OpenTelemetry histograms, the last bucket is implicitly [last_bound, +inf)
            // so we don't need to include f64::INFINITY as it serializes to null in JSON
            explicit_bounds: vec![100.0, 250.0, 500.0, 1000.0],
            exemplars: vec![],
            attributes: Self::sample_simple_attributes(),
        };

        (metric, vec![histogram_data])
    }

    // ============================================================================
    // PROFILE FIXTURES
    // ============================================================================

    /// Create a valid CPU profile
    pub fn valid_cpu_profile() -> Profile {
        Profile {
            id: ProfileId::new(),
            timestamp: Timestamp::from_secs(1700000000),
            service_name: "test-service".to_string(),
            profile_type: ProfileType::Cpu,
            sample_type: "cpu".to_string(),
            sample_unit: "nanoseconds".to_string(),
            data: Self::sample_pprof_data(),
            trace_id: None,
        }
    }

    /// Create a valid memory profile
    #[allow(dead_code)]
    pub fn valid_memory_profile() -> Profile {
        Profile {
            id: ProfileId::new(),
            timestamp: Timestamp::from_secs(1700000000),
            service_name: "test-service".to_string(),
            profile_type: ProfileType::Memory,
            sample_type: "alloc_objects".to_string(),
            sample_unit: "count".to_string(),
            data: Self::sample_pprof_data(),
            trace_id: None,
        }
    }

    /// Create a profile with trace correlation
    #[allow(dead_code)]
    pub fn profile_with_trace_correlation() -> Profile {
        let mut profile = Self::valid_cpu_profile();
        profile.trace_id = Some(Self::test_trace_id(1));
        profile
    }

    fn sample_pprof_data() -> Vec<u8> {
        // Minimal valid pprof data (in reality this would be a proper protobuf)
        vec![
            0x1f, 0x8b, 0x08, 0x00, // gzip header
            0x00, 0x00, 0x00, 0x00, // timestamp
            0x00, 0xff, // compression
        ]
    }

    // ============================================================================
    // SHARED HELPERS
    // ============================================================================

    fn sample_attributes() -> HashMap<String, AttributeValue> {
        let mut attrs = HashMap::new();
        attrs.insert(
            "environment".to_string(),
            AttributeValue::String("test".to_string()),
        );
        attrs.insert(
            "version".to_string(),
            AttributeValue::String("1.0.0".to_string()),
        );
        attrs.insert("enabled".to_string(), AttributeValue::Bool(true));
        attrs.insert("count".to_string(), AttributeValue::Int(42));
        attrs
    }

    fn sample_simple_attributes() -> HashMap<String, String> {
        let mut attrs = HashMap::new();
        attrs.insert("environment".to_string(), "test".to_string());
        attrs.insert("version".to_string(), "1.0.0".to_string());
        attrs.insert("method".to_string(), "GET".to_string());
        attrs
    }

    fn sample_resource() -> HashMap<String, String> {
        let mut resource = HashMap::new();
        resource.insert("host.name".to_string(), "test-host".to_string());
        resource.insert("service.namespace".to_string(), "test-ns".to_string());
        resource
    }

    pub fn test_trace_id(n: u8) -> TraceId {
        TraceId::from_bytes([n, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    }

    pub fn test_span_id(n: u8) -> SpanId {
        SpanId::from_bytes([0, 0, 0, 0, 0, 0, 0, n])
    }

    // ============================================================================
    // INVALID/EDGE CASE FIXTURES
    // ============================================================================

    /// Create a batch of spans for performance testing
    pub fn large_span_batch(count: usize) -> Vec<Span> {
        (0..count)
            .map(|i| {
                let trace_id = Self::test_trace_id((i % 256) as u8);
                let span_id = Self::test_span_id((i % 256) as u8);
                Self::valid_span_with_id(trace_id, span_id, None, &format!("operation-{}", i))
            })
            .collect()
    }

    /// Generate a batch of logs with unique IDs
    #[allow(dead_code)]
    pub fn large_log_batch(count: usize) -> Vec<LogEntry> {
        (0..count)
            .map(|i| {
                let mut log = Self::valid_log();
                log.body = format!("Log message {}", i);
                log
            })
            .collect()
    }

    /// Log with error severity
    #[allow(dead_code)]
    pub fn log_with_error_severity() -> LogEntry {
        LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::now().unwrap(),
            observed_timestamp: Timestamp::now().unwrap(),
            service_name: "test-service".to_string(),
            severity: LogSeverity::Error,
            body: "An error occurred".to_string(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        }
    }

    /// Create a log with a specific severity level
    #[allow(dead_code)]
    pub fn log_with_severity(severity: LogSeverity) -> LogEntry {
        LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::from_secs(1700000000),
            observed_timestamp: Timestamp::from_secs(1700000001),
            service_name: "test-service".to_string(),
            severity,
            body: format!("Log with severity {:?}", severity),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        }
    }

    /// Create a batch of logs for performance testing - DEPRECATED, use large_log_batch instead
    fn _large_log_batch_old(count: usize) -> Vec<LogEntry> {
        (0..count)
            .map(|i| {
                let mut log = Self::valid_log();
                log.body = format!("Log message {}", i);
                log
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_trace_has_parent_child_relationship() {
        let spans = OtlpFixtures::valid_trace();
        assert_eq!(spans.len(), 2);

        let root = &spans[0];
        let child = &spans[1];

        assert!(root.is_root());
        assert!(!child.is_root());
        assert_eq!(root.trace_id, child.trace_id);
        assert_eq!(child.parent_span_id, Some(root.span_id));
    }

    #[test]
    fn test_valid_span_has_all_required_fields() {
        let span = OtlpFixtures::valid_span();
        assert!(!span.service_name.is_empty());
        assert!(!span.operation_name.is_empty());
        assert!(span.start_time.as_nanos() > 0);
        assert!(span.end_time.as_nanos() > span.start_time.as_nanos());
    }

    #[test]
    fn test_span_with_http_attributes_contains_http_fields() {
        let span = OtlpFixtures::span_with_http_attributes();
        assert!(span.attributes.contains_key("http.method"));
        assert!(span.attributes.contains_key("http.status_code"));
    }

    #[test]
    fn test_valid_log_has_all_required_fields() {
        let log = OtlpFixtures::valid_log();
        assert!(!log.body.is_empty());
        assert!(!log.service_name.is_empty());
        assert!(log.timestamp.as_nanos() > 0);
    }

    #[test]
    fn test_log_with_trace_correlation_has_trace_context() {
        let log = OtlpFixtures::log_with_trace_correlation();
        assert!(log.has_trace_context());
        assert!(log.trace_id.is_some());
        assert!(log.span_id.is_some());
    }

    #[test]
    fn test_valid_gauge_has_data_points() {
        let (metric, data_points) = OtlpFixtures::valid_gauge();
        assert!(metric.is_gauge());
        assert!(!data_points.is_empty());
    }

    #[test]
    fn test_valid_histogram_has_buckets() {
        let (metric, data_points) = OtlpFixtures::valid_histogram();
        assert!(metric.is_histogram());
        assert!(!data_points.is_empty());
    }

    #[test]
    fn test_valid_profile_has_data() {
        let profile = OtlpFixtures::valid_cpu_profile();
        assert!(!profile.data.is_empty());
        assert_eq!(profile.profile_type, ProfileType::Cpu);
    }

    #[test]
    fn test_large_span_batch_creates_correct_count() {
        let batch = OtlpFixtures::large_span_batch(100);
        assert_eq!(batch.len(), 100);
    }

    #[test]
    fn test_special_chars_span_contains_dangerous_strings() {
        let span = OtlpFixtures::span_with_special_chars();
        let sql_injection = span.get_attribute("sql_injection");
        assert!(sql_injection.is_some());
    }
}
