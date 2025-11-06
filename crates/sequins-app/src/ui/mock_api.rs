/// Mock implementation of QueryApi for UI development
use sequins_core::{
    error::Result,
    models::{
        AttributeValue, Duration, LogEntry, LogId, LogQuery, Metric, MetricId, MetricQuery,
        Profile, ProfileId, ProfileQuery, QueryTrace, Service, Span, SpanId, SpanKind, SpanStatus,
        Timestamp, TraceId, TraceQuery,
    },
    traits::QueryApi,
};
use std::collections::HashMap;

/// Mock API for UI development
///
/// This implements QueryApi with in-memory mock data, allowing the UI to be developed
/// and tested without requiring a real storage backend. When ready, this can be swapped
/// out for TursoStorage or RemoteClient with no changes to the UI code.
#[derive(Debug, Clone)]
pub struct MockApi;

impl MockApi {
    pub fn new() -> Self {
        Self
    }

    /// Generate mock trace IDs
    fn mock_trace_id(n: u8) -> TraceId {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        TraceId::from_bytes(bytes)
    }

    /// Generate mock span IDs
    fn mock_span_id(n: u8) -> SpanId {
        let mut bytes = [0u8; 8];
        bytes[7] = n;
        SpanId::from_bytes(bytes)
    }

    /// Generate mock spans for a trace
    fn generate_mock_spans(trace_id: TraceId) -> Vec<Span> {
        let now = Timestamp::now().expect("Failed to get current timestamp");
        let base_time = now - Duration::from_secs(60);

        vec![
            Span {
                trace_id,
                span_id: Self::mock_span_id(1),
                parent_span_id: None,
                service_name: "api-gateway".to_string(),
                operation_name: "GET /api/users".to_string(),
                start_time: base_time,
                end_time: base_time + Duration::from_millis(234),
                duration: Duration::from_millis(234),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert(
                        "http.method".to_string(),
                        AttributeValue::String("GET".to_string()),
                    );
                    attrs.insert(
                        "http.route".to_string(),
                        AttributeValue::String("/api/users".to_string()),
                    );
                    attrs.insert("http.status_code".to_string(), AttributeValue::Int(200));
                    attrs
                },
                events: vec![],
                status: SpanStatus::Ok,
                span_kind: SpanKind::Server,
            },
            Span {
                trace_id,
                span_id: Self::mock_span_id(2),
                parent_span_id: Some(Self::mock_span_id(1)),
                service_name: "auth-service".to_string(),
                operation_name: "authenticate".to_string(),
                start_time: base_time + Duration::from_millis(5),
                end_time: base_time + Duration::from_millis(50),
                duration: Duration::from_millis(45),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert(
                        "auth.method".to_string(),
                        AttributeValue::String("jwt".to_string()),
                    );
                    attrs.insert(
                        "user.id".to_string(),
                        AttributeValue::String("12345".to_string()),
                    );
                    attrs
                },
                events: vec![],
                status: SpanStatus::Ok,
                span_kind: SpanKind::Internal,
            },
            Span {
                trace_id,
                span_id: Self::mock_span_id(3),
                parent_span_id: Some(Self::mock_span_id(1)),
                service_name: "user-service".to_string(),
                operation_name: "query users".to_string(),
                start_time: base_time + Duration::from_millis(52),
                end_time: base_time + Duration::from_millis(208),
                duration: Duration::from_millis(156),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert(
                        "db.system".to_string(),
                        AttributeValue::String("postgresql".to_string()),
                    );
                    attrs.insert(
                        "db.statement".to_string(),
                        AttributeValue::String("SELECT * FROM users LIMIT 100".to_string()),
                    );
                    attrs
                },
                events: vec![],
                status: SpanStatus::Ok,
                span_kind: SpanKind::Client,
            },
            Span {
                trace_id,
                span_id: Self::mock_span_id(4),
                parent_span_id: Some(Self::mock_span_id(3)),
                service_name: "database".to_string(),
                operation_name: "db.query".to_string(),
                start_time: base_time + Duration::from_millis(60),
                end_time: base_time + Duration::from_millis(158),
                duration: Duration::from_millis(98),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert(
                        "db.connection_string".to_string(),
                        AttributeValue::String("postgres://localhost".to_string()),
                    );
                    attrs.insert("db.rows_returned".to_string(), AttributeValue::Int(42));
                    attrs
                },
                events: vec![],
                status: SpanStatus::Ok,
                span_kind: SpanKind::Server,
            },
            Span {
                trace_id,
                span_id: Self::mock_span_id(5),
                parent_span_id: Some(Self::mock_span_id(1)),
                service_name: "cache".to_string(),
                operation_name: "cache.get".to_string(),
                start_time: base_time + Duration::from_millis(212),
                end_time: base_time + Duration::from_millis(224),
                duration: Duration::from_millis(12),
                attributes: {
                    let mut attrs = HashMap::new();
                    attrs.insert(
                        "cache.key".to_string(),
                        AttributeValue::String("user:session:abc123".to_string()),
                    );
                    attrs.insert("cache.hit".to_string(), AttributeValue::Bool(true));
                    attrs
                },
                events: vec![],
                status: SpanStatus::Ok,
                span_kind: SpanKind::Client,
            },
        ]
    }
}

impl Default for MockApi {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl QueryApi for MockApi {
    async fn get_services(&self) -> Result<Vec<Service>> {
        Ok(vec![
            Service {
                name: "api-gateway".to_string(),
                span_count: 245,
                log_count: 1203,
            },
            Service {
                name: "auth-service".to_string(),
                span_count: 189,
                log_count: 456,
            },
            Service {
                name: "user-service".to_string(),
                span_count: 412,
                log_count: 892,
            },
            Service {
                name: "order-service".to_string(),
                span_count: 324,
                log_count: 678,
            },
            Service {
                name: "payment-service".to_string(),
                span_count: 156,
                log_count: 234,
            },
            Service {
                name: "notification-service".to_string(),
                span_count: 98,
                log_count: 145,
            },
            Service {
                name: "database".to_string(),
                span_count: 567,
                log_count: 89,
            },
            Service {
                name: "cache".to_string(),
                span_count: 234,
                log_count: 45,
            },
        ])
    }

    async fn query_traces(&self, _query: TraceQuery) -> Result<Vec<QueryTrace>> {
        let _now = Timestamp::now().expect("Failed to get current timestamp");
        let _base_time = _now - Duration::from_secs(60);

        Ok(vec![
            QueryTrace {
                trace_id: Self::mock_trace_id(1),
                root_span_id: Self::mock_span_id(1),
                spans: Self::generate_mock_spans(Self::mock_trace_id(1)),
                duration: 234,
                has_error: false,
            },
            QueryTrace {
                trace_id: Self::mock_trace_id(2),
                root_span_id: Self::mock_span_id(1),
                spans: {
                    let trace_id = Self::mock_trace_id(2);
                    let mut spans = Self::generate_mock_spans(trace_id);
                    // Make one span have an error
                    spans[2].status = SpanStatus::Error;
                    spans
                },
                duration: 1205,
                has_error: true,
            },
            QueryTrace {
                trace_id: Self::mock_trace_id(3),
                root_span_id: Self::mock_span_id(1),
                spans: Self::generate_mock_spans(Self::mock_trace_id(3)),
                duration: 145,
                has_error: false,
            },
            QueryTrace {
                trace_id: Self::mock_trace_id(4),
                root_span_id: Self::mock_span_id(1),
                spans: Self::generate_mock_spans(Self::mock_trace_id(4)),
                duration: 567,
                has_error: false,
            },
            QueryTrace {
                trace_id: Self::mock_trace_id(5),
                root_span_id: Self::mock_span_id(1),
                spans: Self::generate_mock_spans(Self::mock_trace_id(5)),
                duration: 89,
                has_error: false,
            },
        ])
    }

    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>> {
        Ok(Self::generate_mock_spans(trace_id))
    }

    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>> {
        let spans = Self::generate_mock_spans(trace_id);
        Ok(spans.into_iter().find(|s| s.span_id == span_id))
    }

    async fn query_logs(&self, _query: LogQuery) -> Result<Vec<LogEntry>> {
        // Return empty for now - can be implemented later
        Ok(vec![])
    }

    async fn get_log(&self, _log_id: LogId) -> Result<Option<LogEntry>> {
        Ok(None)
    }

    async fn query_metrics(&self, _query: MetricQuery) -> Result<Vec<Metric>> {
        // Return empty for now - can be implemented later
        Ok(vec![])
    }

    async fn get_metric(&self, _metric_id: MetricId) -> Result<Option<Metric>> {
        Ok(None)
    }

    async fn get_profiles(&self, _query: ProfileQuery) -> Result<Vec<Profile>> {
        // Return empty for now - can be implemented later
        Ok(vec![])
    }

    async fn get_profile(&self, _profile_id: ProfileId) -> Result<Option<Profile>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_api_get_services() {
        let api = MockApi::new();
        let services = api.get_services().await.unwrap();

        assert_eq!(services.len(), 8);
        assert!(services.iter().any(|s| s.name == "api-gateway"));
        assert!(services.iter().any(|s| s.name == "auth-service"));
        assert!(services.iter().all(|s| s.span_count > 0));
    }

    #[tokio::test]
    async fn test_mock_api_query_traces() {
        let api = MockApi::new();
        let now = Timestamp::now().unwrap();
        let query = TraceQuery {
            service: None,
            start_time: now - Duration::from_hours(1),
            end_time: now,
            min_duration: None,
            max_duration: None,
            has_error: None,
            limit: Some(10),
        };

        let traces = api.query_traces(query).await.unwrap();

        assert_eq!(traces.len(), 5);
        assert!(traces.iter().any(|t| t.has_error));
        assert!(traces.iter().any(|t| !t.has_error));
    }

    #[tokio::test]
    async fn test_mock_api_get_spans() {
        let api = MockApi::new();
        let trace_id = MockApi::mock_trace_id(1);

        let spans = api.get_spans(trace_id).await.unwrap();

        assert_eq!(spans.len(), 5);
        assert!(spans.iter().any(|s| s.parent_span_id.is_none())); // Has root span
        assert!(spans.iter().any(|s| s.parent_span_id.is_some())); // Has child spans
    }

    #[tokio::test]
    async fn test_mock_api_get_span() {
        let api = MockApi::new();
        let trace_id = MockApi::mock_trace_id(1);
        let span_id = MockApi::mock_span_id(1);

        let span = api.get_span(trace_id, span_id).await.unwrap();

        assert!(span.is_some());
        let span = span.unwrap();
        assert_eq!(span.span_id, span_id);
        assert_eq!(span.trace_id, trace_id);
        assert_eq!(span.service_name, "api-gateway");
    }
}
