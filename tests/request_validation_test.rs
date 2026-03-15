//! Request validation tests for server endpoints
//!
//! Tests server handling of:
//! - Malformed JSON in POST bodies
//! - Missing required fields
//! - Invalid query parameters
//! - Oversized request bodies
//! - Type mismatches

use reqwest::StatusCode;
use sequins::server::QueryServer;
use sequins::{
    error::Result,
    models::{
        HistogramDataPointQuery, HistogramDataPointQueryResult, LogEntry, LogId, LogQuery,
        LogQueryResult, Metric, MetricDataPointQuery, MetricDataPointQueryResult, MetricGroup,
        MetricId, MetricQuery, MetricQueryResult, Profile, ProfileId, ProfileQuery,
        ProfileQueryResult, ProfileSample, ProfileSampleQuery, ProfileSampleQueryResult,
        QueryCursor, QueryTrace, Service, Span, SpanId, SpanQuery, SpanQueryResult, Timestamp,
        TraceId, TraceQuery, TraceQueryResult,
    },
    traits::QueryApi,
};
use serial_test::serial;
use std::pin::Pin;
use std::sync::Arc;

/// Minimal mock for validation tests
#[derive(Clone)]
struct ValidationMockQueryApi;

#[async_trait::async_trait]
impl QueryApi for ValidationMockQueryApi {
    async fn get_services(&self) -> Result<Vec<Service>> {
        Ok(vec![])
    }

    async fn query_traces(&self, _query: TraceQuery) -> Result<TraceQueryResult> {
        Ok(TraceQueryResult {
            traces: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_traces(
        &self,
        _query: TraceQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = QueryTrace> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_spans(&self, _trace_id: TraceId) -> Result<Vec<Span>> {
        Ok(vec![])
    }

    async fn get_span(&self, _trace_id: TraceId, _span_id: SpanId) -> Result<Option<Span>> {
        Ok(None)
    }

    async fn query_spans(&self, _query: SpanQuery) -> Result<SpanQueryResult> {
        Ok(SpanQueryResult {
            spans: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_spans(
        &self,
        _query: SpanQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Span> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_log(&self, _log_id: LogId) -> Result<Option<LogEntry>> {
        Ok(None)
    }

    async fn query_logs(&self, _query: LogQuery) -> Result<LogQueryResult> {
        Ok(LogQueryResult {
            logs: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_logs(
        &self,
        _query: LogQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = LogEntry> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_metric(&self, _metric_id: MetricId) -> Result<Option<Metric>> {
        Ok(None)
    }

    async fn query_metrics(&self, _query: MetricQuery) -> Result<MetricQueryResult> {
        Ok(MetricQueryResult {
            metrics: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_metrics(
        &self,
        _query: MetricQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Metric> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn query_metric_data_points(
        &self,
        _query: MetricDataPointQuery,
    ) -> Result<MetricDataPointQueryResult> {
        Ok(MetricDataPointQueryResult {
            data_points: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_metric_data_points(
        &self,
        _query: MetricDataPointQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = sequins::models::MetricDataPoint> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn query_histogram_data_points(
        &self,
        _query: HistogramDataPointQuery,
    ) -> Result<HistogramDataPointQueryResult> {
        Ok(HistogramDataPointQueryResult {
            data_points: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn get_profile(&self, _profile_id: ProfileId) -> Result<Option<Profile>> {
        Ok(None)
    }

    async fn query_profiles(&self, _query: ProfileQuery) -> Result<ProfileQueryResult> {
        Ok(ProfileQueryResult {
            profiles: vec![],
            cursor: QueryCursor::now().unwrap(),
        })
    }

    async fn subscribe_profiles(
        &self,
        _query: ProfileQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = Profile> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn query_profile_samples(
        &self,
        _query: ProfileSampleQuery,
    ) -> Result<ProfileSampleQueryResult> {
        Ok(ProfileSampleQueryResult {
            samples: vec![],
            cursor: QueryCursor::new(Timestamp::from_secs(0)),
        })
    }

    async fn subscribe_profile_samples(
        &self,
        _query: ProfileSampleQuery,
        _cursor: QueryCursor,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = ProfileSample> + Send>>> {
        Ok(Box::pin(futures::stream::iter(vec![])))
    }

    async fn get_metric_groups(&self, _service_name: Option<&str>) -> Result<Vec<MetricGroup>> {
        Ok(vec![])
    }

    async fn get_metric_group(
        &self,
        _base_name: &str,
        _service_name: &str,
    ) -> Result<Option<MetricGroup>> {
        Ok(None)
    }
}

async fn start_validation_server() -> tokio::task::JoinHandle<()> {
    let mock = ValidationMockQueryApi;
    let server = QueryServer::new(Arc::new(mock));

    tokio::spawn(async move {
        let app = server.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:18087")
            .await
            .unwrap();
        axum::serve(listener, app).await.unwrap();
    })
}

// ============================================================================
// Malformed JSON Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_malformed_json_post_body() {
    let _handle = start_validation_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let response = client
        .post("http://127.0.0.1:18087/api/traces")
        .header("Content-Type", "application/json")
        .body("{this is not valid json}")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial]
async fn test_empty_post_body() {
    let _handle = start_validation_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let response = client
        .post("http://127.0.0.1:18087/api/traces")
        .header("Content-Type", "application/json")
        .body("")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// Invalid Query Parameter Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_invalid_query_parameter_format() {
    let _handle = start_validation_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    let response = client
        .get("http://127.0.0.1:18087/api/metrics/valid-metric-id-12345678/data")
        .query(&[
            ("start_time", "not-a-number"),
            ("end_time", "also-not-a-number"),
        ])
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ============================================================================
// Type Mismatch Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_type_mismatch_in_json() {
    let _handle = start_validation_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    // Send string where number expected
    let response = client
        .post("http://127.0.0.1:18087/api/traces")
        .json(&serde_json::json!({
            "service": "test",
            "start_time": "not-a-timestamp",
            "end_time": 2000,
            "limit": 10,
            "live": false
        }))
        .send()
        .await
        .unwrap();

    // Axum returns 422 Unprocessable Entity for deserialization errors
    assert!(
        response.status() == StatusCode::BAD_REQUEST
            || response.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

// ============================================================================
// Large Request Body Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_very_large_request_body() {
    let _handle = start_validation_server().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = reqwest::Client::new();
    // Create very large body (1MB)
    let large_string = "x".repeat(1_000_000);
    let response = client
        .post("http://127.0.0.1:18087/api/traces")
        .header("Content-Type", "application/json")
        .body(large_string)
        .send()
        .await
        .unwrap();

    // Should be rejected due to malformed JSON
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
