//! Tests for metric data point queries
//!
//! These tests verify that RemoteClient correctly implements metric data point
//! retrieval methods from the QueryApi trait.

use sequins::client::RemoteClient;
use sequins::{
    models::{
        HistogramDataPoint, HistogramDataPointQuery, HistogramDataPointQueryResult,
        MetricDataPoint, MetricDataPointQuery, MetricDataPointQueryResult, MetricId, QueryCursor,
        Timestamp,
    },
    traits::QueryApi,
};
use wiremock::{
    matchers::{method, path, query_param},
    Mock, MockServer, ResponseTemplate,
};

// ============================================================================
// Metric Data Points Tests
// ============================================================================

#[tokio::test]
async fn test_query_metric_data_points_success() {
    let mock_server = MockServer::start().await;
    let metric_id = MetricId::new();

    let data_points = vec![
        MetricDataPoint {
            metric_id,
            timestamp: Timestamp::from_secs(1000),
            value: 42.0,
            attributes: std::collections::HashMap::new(),
        },
        MetricDataPoint {
            metric_id,
            timestamp: Timestamp::from_secs(2000),
            value: 84.0,
            attributes: std::collections::HashMap::new(),
        },
    ];

    let response = MetricDataPointQueryResult {
        data_points,
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("GET"))
        .and(path(format!("/api/metrics/{}/data", metric_id.to_hex())))
        .and(query_param("start_time", "1000000000000"))
        .and(query_param("end_time", "3000000000000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = MetricDataPointQuery {
        metric_id,
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(3000),
        bucket_duration: None,
    };
    let result = client.query_metric_data_points(query).await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.data_points.len(), 2);
    assert_eq!(query_result.data_points[0].value, 42.0);
    assert_eq!(query_result.data_points[1].value, 84.0);
}

#[tokio::test]
async fn test_query_metric_data_points_empty() {
    let mock_server = MockServer::start().await;
    let metric_id = MetricId::new();

    let response = MetricDataPointQueryResult {
        data_points: vec![],
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("GET"))
        .and(path(format!("/api/metrics/{}/data", metric_id.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = MetricDataPointQuery {
        metric_id,
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(3000),
        bucket_duration: None,
    };
    let result = client.query_metric_data_points(query).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap().data_points.len(), 0);
}

#[tokio::test]
async fn test_query_metric_data_points_network_error() {
    let client = RemoteClient::new("http://localhost:1", "http://localhost:1").unwrap();
    let query = MetricDataPointQuery {
        metric_id: MetricId::new(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(3000),
        bucket_duration: None,
    };
    let result = client.query_metric_data_points(query).await;

    assert!(result.is_err());
}

// ============================================================================
// Histogram Data Points Tests
// ============================================================================

#[tokio::test]
async fn test_query_histogram_data_points_success() {
    let mock_server = MockServer::start().await;
    let metric_id = MetricId::new();

    let histogram_points = vec![
        HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::from_secs(1000),
            count: 100,
            sum: 5000.0,
            bucket_counts: vec![20, 50, 30],
            explicit_bounds: vec![50.0, 100.0, 200.0],
            exemplars: vec![],
            attributes: std::collections::HashMap::new(),
        },
        HistogramDataPoint {
            metric_id,
            timestamp: Timestamp::from_secs(2000),
            count: 150,
            sum: 7500.0,
            bucket_counts: vec![30, 70, 50],
            explicit_bounds: vec![50.0, 100.0, 200.0],
            exemplars: vec![],
            attributes: std::collections::HashMap::new(),
        },
    ];

    let response = HistogramDataPointQueryResult {
        data_points: histogram_points,
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("GET"))
        .and(path(format!(
            "/api/metrics/{}/histogram",
            metric_id.to_hex()
        )))
        .and(query_param("start_time", "1000000000000"))
        .and(query_param("end_time", "3000000000000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = HistogramDataPointQuery {
        metric_id,
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(3000),
        bucket_duration: None,
    };
    let result = client.query_histogram_data_points(query).await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.data_points.len(), 2);
    assert_eq!(query_result.data_points[0].count, 100);
    assert_eq!(query_result.data_points[0].sum, 5000.0);
    assert_eq!(query_result.data_points[1].count, 150);
    assert_eq!(query_result.data_points[1].sum, 7500.0);
}

#[tokio::test]
async fn test_query_histogram_data_points_empty() {
    let mock_server = MockServer::start().await;
    let metric_id = MetricId::new();

    let response = HistogramDataPointQueryResult {
        data_points: vec![],
        cursor: QueryCursor::now().unwrap(),
    };

    Mock::given(method("GET"))
        .and(path(format!(
            "/api/metrics/{}/histogram",
            metric_id.to_hex()
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(&response))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = RemoteClient::new(&mock_server.uri(), &mock_server.uri()).unwrap();
    let query = HistogramDataPointQuery {
        metric_id,
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(3000),
        bucket_duration: None,
    };
    let result = client.query_histogram_data_points(query).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap().data_points.len(), 0);
}

#[tokio::test]
async fn test_query_histogram_data_points_network_error() {
    let client = RemoteClient::new("http://localhost:1", "http://localhost:1").unwrap();
    let query = HistogramDataPointQuery {
        metric_id: MetricId::new(),
        start_time: Timestamp::from_secs(1000),
        end_time: Timestamp::from_secs(3000),
        bucket_duration: None,
    };
    let result = client.query_histogram_data_points(query).await;

    assert!(result.is_err());
}
