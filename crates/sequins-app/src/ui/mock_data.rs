/// Mock data for UI development
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub instance_count: usize,
    pub health: HealthStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone)]
pub struct TraceInfo {
    pub trace_id: String,
    pub root_span_name: String,
    pub duration_ms: u64,
    pub status: TraceStatus,
    pub timestamp: SystemTime,
    pub service: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceStatus {
    Ok,
    Error,
}

#[derive(Debug, Clone)]
pub struct SpanInfo {
    pub span_id: String,
    pub name: String,
    pub service: String,
    pub duration_ms: u64,
    pub offset_ms: u64, // Offset from trace start
    pub depth: usize,    // Nesting depth
    pub attributes: Vec<(String, String)>,
}

/// Generate mock service list
pub fn mock_services() -> Vec<ServiceInfo> {
    vec![
        ServiceInfo {
            name: "api-gateway".to_string(),
            instance_count: 3,
            health: HealthStatus::Healthy,
        },
        ServiceInfo {
            name: "auth-service".to_string(),
            instance_count: 2,
            health: HealthStatus::Healthy,
        },
        ServiceInfo {
            name: "user-service".to_string(),
            instance_count: 5,
            health: HealthStatus::Degraded,
        },
        ServiceInfo {
            name: "order-service".to_string(),
            instance_count: 4,
            health: HealthStatus::Healthy,
        },
        ServiceInfo {
            name: "payment-service".to_string(),
            instance_count: 2,
            health: HealthStatus::Unhealthy,
        },
        ServiceInfo {
            name: "notification-service".to_string(),
            instance_count: 3,
            health: HealthStatus::Healthy,
        },
        ServiceInfo {
            name: "database".to_string(),
            instance_count: 1,
            health: HealthStatus::Healthy,
        },
        ServiceInfo {
            name: "cache".to_string(),
            instance_count: 4,
            health: HealthStatus::Healthy,
        },
    ]
}

/// Generate mock trace list
pub fn mock_traces() -> Vec<TraceInfo> {
    let now = SystemTime::now();
    vec![
        TraceInfo {
            trace_id: "7f8d9c1e4b2a3f5d".to_string(),
            root_span_name: "GET /api/users".to_string(),
            duration_ms: 234,
            status: TraceStatus::Ok,
            timestamp: now,
            service: "api-gateway".to_string(),
        },
        TraceInfo {
            trace_id: "a3b5c7d9e1f2g4h6".to_string(),
            root_span_name: "POST /api/orders".to_string(),
            duration_ms: 1205,
            status: TraceStatus::Error,
            timestamp: now,
            service: "api-gateway".to_string(),
        },
        TraceInfo {
            trace_id: "3f5d7b9c1e4a2d6f".to_string(),
            root_span_name: "GET /api/products".to_string(),
            duration_ms: 145,
            status: TraceStatus::Ok,
            timestamp: now,
            service: "api-gateway".to_string(),
        },
        TraceInfo {
            trace_id: "9c1e4b2a3f5d7f8d".to_string(),
            root_span_name: "PUT /api/users/123".to_string(),
            duration_ms: 567,
            status: TraceStatus::Ok,
            timestamp: now,
            service: "api-gateway".to_string(),
        },
        TraceInfo {
            trace_id: "5d7f8d9c1e4b2a3f".to_string(),
            root_span_name: "DELETE /api/sessions/abc".to_string(),
            duration_ms: 89,
            status: TraceStatus::Ok,
            timestamp: now,
            service: "auth-service".to_string(),
        },
    ]
}

/// Generate mock span hierarchy for a trace
pub fn mock_spans_for_trace(_trace_id: &str) -> Vec<SpanInfo> {
    vec![
        SpanInfo {
            span_id: "span-1".to_string(),
            name: "GET /api/users".to_string(),
            service: "api-gateway".to_string(),
            duration_ms: 234,
            offset_ms: 0,
            depth: 0,
            attributes: vec![
                ("http.method".to_string(), "GET".to_string()),
                ("http.route".to_string(), "/api/users".to_string()),
                ("http.status_code".to_string(), "200".to_string()),
            ],
        },
        SpanInfo {
            span_id: "span-2".to_string(),
            name: "authenticate".to_string(),
            service: "auth-service".to_string(),
            duration_ms: 45,
            offset_ms: 5,
            depth: 1,
            attributes: vec![
                ("auth.method".to_string(), "jwt".to_string()),
                ("user.id".to_string(), "12345".to_string()),
            ],
        },
        SpanInfo {
            span_id: "span-3".to_string(),
            name: "query users".to_string(),
            service: "user-service".to_string(),
            duration_ms: 156,
            offset_ms: 52,
            depth: 1,
            attributes: vec![
                ("db.system".to_string(), "postgresql".to_string()),
                ("db.statement".to_string(), "SELECT * FROM users LIMIT 100".to_string()),
            ],
        },
        SpanInfo {
            span_id: "span-4".to_string(),
            name: "db.query".to_string(),
            service: "database".to_string(),
            duration_ms: 98,
            offset_ms: 60,
            depth: 2,
            attributes: vec![
                ("db.connection_string".to_string(), "postgres://localhost".to_string()),
                ("db.rows_returned".to_string(), "42".to_string()),
            ],
        },
        SpanInfo {
            span_id: "span-5".to_string(),
            name: "cache.get".to_string(),
            service: "cache".to_string(),
            duration_ms: 12,
            offset_ms: 212,
            depth: 1,
            attributes: vec![
                ("cache.key".to_string(), "user:session:abc123".to_string()),
                ("cache.hit".to_string(), "true".to_string()),
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_services_returns_expected_count() {
        let services = mock_services();
        assert_eq!(services.len(), 8, "Should return 8 mock services");
    }

    #[test]
    fn test_mock_services_have_valid_names() {
        let services = mock_services();
        for service in &services {
            assert!(!service.name.is_empty(), "Service name should not be empty");
            assert!(service.instance_count > 0, "Instance count should be greater than 0");
        }
    }

    #[test]
    fn test_mock_services_have_different_health_statuses() {
        let services = mock_services();
        let has_healthy = services.iter().any(|s| s.health == HealthStatus::Healthy);
        let has_degraded = services.iter().any(|s| s.health == HealthStatus::Degraded);
        let has_unhealthy = services.iter().any(|s| s.health == HealthStatus::Unhealthy);

        assert!(has_healthy, "Should have at least one healthy service");
        assert!(has_degraded, "Should have at least one degraded service");
        assert!(has_unhealthy, "Should have at least one unhealthy service");
    }

    #[test]
    fn test_mock_traces_returns_expected_count() {
        let traces = mock_traces();
        assert_eq!(traces.len(), 5, "Should return 5 mock traces");
    }

    #[test]
    fn test_mock_traces_have_valid_data() {
        let traces = mock_traces();
        for trace in &traces {
            assert!(!trace.trace_id.is_empty(), "Trace ID should not be empty");
            assert!(!trace.root_span_name.is_empty(), "Root span name should not be empty");
            assert!(trace.duration_ms > 0, "Duration should be greater than 0");
            assert!(!trace.service.is_empty(), "Service name should not be empty");
        }
    }

    #[test]
    fn test_mock_traces_have_different_statuses() {
        let traces = mock_traces();
        let has_ok = traces.iter().any(|t| t.status == TraceStatus::Ok);
        let has_error = traces.iter().any(|t| t.status == TraceStatus::Error);

        assert!(has_ok, "Should have at least one OK trace");
        assert!(has_error, "Should have at least one error trace");
    }

    #[test]
    fn test_mock_spans_returns_expected_count() {
        let spans = mock_spans_for_trace("test-trace-id");
        assert_eq!(spans.len(), 5, "Should return 5 mock spans");
    }

    #[test]
    fn test_mock_spans_have_valid_data() {
        let spans = mock_spans_for_trace("test-trace-id");
        for span in &spans {
            assert!(!span.span_id.is_empty(), "Span ID should not be empty");
            assert!(!span.name.is_empty(), "Span name should not be empty");
            assert!(!span.service.is_empty(), "Service name should not be empty");
            assert!(span.duration_ms > 0, "Duration should be greater than 0");
            assert!(!span.attributes.is_empty(), "Span should have attributes");
        }
    }

    #[test]
    fn test_mock_spans_have_correct_depth_hierarchy() {
        let spans = mock_spans_for_trace("test-trace-id");

        // Root span should have depth 0
        assert_eq!(spans[0].depth, 0, "First span should be at depth 0");

        // Should have spans at different depths
        let max_depth = spans.iter().map(|s| s.depth).max().unwrap();
        assert!(max_depth >= 2, "Should have spans nested at least 2 levels deep");
    }

    #[test]
    fn test_mock_spans_timing_is_consistent() {
        let spans = mock_spans_for_trace("test-trace-id");

        for span in &spans {
            // Offset + duration should be reasonable (not overflow or negative)
            let end_time = span.offset_ms + span.duration_ms;
            assert!(end_time >= span.offset_ms, "End time should be after start time");
        }
    }

    #[test]
    fn test_health_status_equality() {
        assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_trace_status_equality() {
        assert_eq!(TraceStatus::Ok, TraceStatus::Ok);
        assert_ne!(TraceStatus::Ok, TraceStatus::Error);
    }
}
