//! Health Metric Generation
//!
//! This module provides functionality for generating health metrics based on
//! telemetry data (spans and logs). Health metrics are stored as regular metrics
//! with `is_generated: true` to distinguish them from OTLP-reported metrics.

use crate::models::metrics::{Metric, MetricDataPoint, MetricId, MetricType};
use crate::models::time::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Health metric name constants
pub mod names {
    /// Span error rate (0.0-1.0)
    pub const SPAN_ERROR_RATE: &str = "sequins.health.span_error_rate";
    /// HTTP error rate from status codes (0.0-1.0)
    pub const HTTP_ERROR_RATE: &str = "sequins.health.http_error_rate";
    /// HTTP 2xx success rate (0.0-1.0)
    pub const HTTP_2XX_RATE: &str = "sequins.health.http_2xx_rate";
    /// HTTP 4xx client error rate (0.0-1.0)
    pub const HTTP_4XX_RATE: &str = "sequins.health.http_4xx_rate";
    /// HTTP 5xx server error rate (0.0-1.0)
    pub const HTTP_5XX_RATE: &str = "sequins.health.http_5xx_rate";
    /// Latency p50 percentile (nanoseconds)
    pub const LATENCY_P50: &str = "sequins.health.latency_p50";
    /// Latency p95 percentile (nanoseconds)
    pub const LATENCY_P95: &str = "sequins.health.latency_p95";
    /// Latency p99 percentile (nanoseconds)
    pub const LATENCY_P99: &str = "sequins.health.latency_p99";
    /// Throughput (spans per minute)
    pub const THROUGHPUT: &str = "sequins.health.throughput";
    /// Error log rate (logs per minute)
    pub const ERROR_LOG_RATE: &str = "sequins.health.error_log_rate";
}

/// Raw health statistics computed from telemetry data
#[derive(Debug, Clone, Default)]
pub struct HealthStats {
    /// Total number of spans in the time window
    pub total_span_count: u64,
    /// Number of spans with error status
    pub error_span_count: u64,
    /// Number of HTTP 2xx responses
    pub http_2xx_count: u64,
    /// Number of HTTP 3xx responses
    pub http_3xx_count: u64,
    /// Number of HTTP 4xx responses
    pub http_4xx_count: u64,
    /// Number of HTTP 5xx responses
    pub http_5xx_count: u64,
    /// Latency values for percentile calculation (nanoseconds)
    pub latencies: Vec<u64>,
    /// Total number of logs in the time window
    pub total_log_count: u64,
    /// Number of ERROR and FATAL logs
    pub error_log_count: u64,
    /// Duration of the time window in minutes
    pub window_minutes: f64,
}

impl HealthStats {
    /// Calculate span error rate (0.0-1.0)
    pub fn span_error_rate(&self) -> f64 {
        if self.total_span_count == 0 {
            0.0
        } else {
            self.error_span_count as f64 / self.total_span_count as f64
        }
    }

    /// Calculate HTTP error rate (4xx + 5xx) / total HTTP (0.0-1.0)
    pub fn http_error_rate(&self) -> f64 {
        let total_http = self.total_http_requests();
        if total_http == 0 {
            0.0
        } else {
            (self.http_4xx_count + self.http_5xx_count) as f64 / total_http as f64
        }
    }

    /// Total HTTP requests
    fn total_http_requests(&self) -> u64 {
        self.http_2xx_count + self.http_3xx_count + self.http_4xx_count + self.http_5xx_count
    }

    /// Calculate HTTP 2xx success rate (0.0-1.0)
    pub fn http_2xx_rate(&self) -> f64 {
        let total_http = self.total_http_requests();
        if total_http == 0 {
            0.0
        } else {
            self.http_2xx_count as f64 / total_http as f64
        }
    }

    /// Calculate HTTP 3xx redirect rate (0.0-1.0)
    pub fn http_3xx_rate(&self) -> f64 {
        let total_http = self.total_http_requests();
        if total_http == 0 {
            0.0
        } else {
            self.http_3xx_count as f64 / total_http as f64
        }
    }

    /// Calculate HTTP 4xx client error rate (0.0-1.0)
    pub fn http_4xx_rate(&self) -> f64 {
        let total_http = self.total_http_requests();
        if total_http == 0 {
            0.0
        } else {
            self.http_4xx_count as f64 / total_http as f64
        }
    }

    /// Calculate HTTP 5xx server error rate (0.0-1.0)
    pub fn http_5xx_rate(&self) -> f64 {
        let total_http = self.total_http_requests();
        if total_http == 0 {
            0.0
        } else {
            self.http_5xx_count as f64 / total_http as f64
        }
    }

    /// Calculate throughput (spans per minute)
    pub fn throughput(&self) -> f64 {
        if self.window_minutes <= 0.0 {
            0.0
        } else {
            self.total_span_count as f64 / self.window_minutes
        }
    }

    /// Calculate error log rate (logs per minute)
    pub fn error_log_rate(&self) -> f64 {
        if self.window_minutes <= 0.0 {
            0.0
        } else {
            self.error_log_count as f64 / self.window_minutes
        }
    }

    /// Calculate latency percentile
    pub fn latency_percentile(&self, percentile: f64) -> u64 {
        if self.latencies.is_empty() {
            return 0;
        }

        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();

        let index = ((percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[index.min(sorted.len() - 1)]
    }

    /// Calculate p50 latency (median)
    pub fn latency_p50(&self) -> u64 {
        self.latency_percentile(50.0)
    }

    /// Calculate p95 latency
    pub fn latency_p95(&self) -> u64 {
        self.latency_percentile(95.0)
    }

    /// Calculate p99 latency
    pub fn latency_p99(&self) -> u64 {
        self.latency_percentile(99.0)
    }
}

/// Configuration for health metric generation timing
#[derive(Debug, Clone)]
pub struct HealthGenerationConfig {
    /// How often to compute health metrics
    pub interval: Duration,
    /// Rolling window for aggregation
    pub window: Duration,
}

impl Default for HealthGenerationConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60), // 1 minute
            window: Duration::from_secs(300),  // 5 minutes
        }
    }
}

/// A rule defining how a metric contributes to health status
///
/// Each rule specifies warning and error thresholds for a metric,
/// as well as the weight it contributes to the overall health score.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthMetricRule {
    /// Metric name (exact match, e.g., "sequins.health.span_error_rate" or "my.custom.metric")
    pub metric_name: String,

    /// Service filter (None = applies to all services, Some = specific service only)
    pub service_name: Option<String>,

    /// Warning threshold - above this value = degraded/warning status
    pub warning_threshold: f64,

    /// Error threshold - above this value = unhealthy/error status
    pub error_threshold: f64,

    /// Comparison direction: true = higher is worse (default), false = lower is worse
    #[serde(default = "default_higher_is_worse")]
    pub higher_is_worse: bool,

    /// Weight for overall health score (0.0-1.0, will be normalized)
    #[serde(default = "default_weight")]
    pub weight: f64,

    /// Human-readable display name
    pub display_name: String,
}

fn default_higher_is_worse() -> bool {
    true
}

fn default_weight() -> f64 {
    1.0
}

/// Configuration for health threshold rules
///
/// This defines which metrics contribute to health and their thresholds.
/// Stored in the config table with key "health_thresholds".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthThresholdConfig {
    /// List of metric rules that define health
    pub rules: Vec<HealthMetricRule>,
}

impl Default for HealthThresholdConfig {
    fn default() -> Self {
        Self {
            rules: vec![
                HealthMetricRule {
                    metric_name: names::SPAN_ERROR_RATE.to_string(),
                    service_name: None,
                    warning_threshold: 0.01, // 1%
                    error_threshold: 0.05,   // 5%
                    higher_is_worse: true,
                    weight: 0.40,
                    display_name: "Span Error Rate".to_string(),
                },
                HealthMetricRule {
                    metric_name: names::HTTP_ERROR_RATE.to_string(),
                    service_name: None,
                    warning_threshold: 0.05, // 5%
                    error_threshold: 0.15,   // 15%
                    higher_is_worse: true,
                    weight: 0.25,
                    display_name: "HTTP Error Rate".to_string(),
                },
                HealthMetricRule {
                    metric_name: names::LATENCY_P95.to_string(),
                    service_name: None,
                    warning_threshold: 200_000_000.0, // 200ms in ns
                    error_threshold: 500_000_000.0,   // 500ms in ns
                    higher_is_worse: true,
                    weight: 0.20,
                    display_name: "Latency (P95)".to_string(),
                },
                HealthMetricRule {
                    metric_name: names::ERROR_LOG_RATE.to_string(),
                    service_name: None,
                    warning_threshold: 5.0, // 5/min
                    error_threshold: 20.0,  // 20/min
                    higher_is_worse: true,
                    weight: 0.15,
                    display_name: "Error Log Rate".to_string(),
                },
            ],
        }
    }
}

/// Generates health metrics from telemetry data
pub struct HealthMetricGenerator {
    config: HealthGenerationConfig,
}

impl HealthMetricGenerator {
    /// Create a new health metric generator with default configuration
    pub fn new() -> Self {
        Self {
            config: HealthGenerationConfig::default(),
        }
    }

    /// Create a health metric generator with custom configuration
    pub fn with_config(config: HealthGenerationConfig) -> Self {
        Self { config }
    }

    /// Get the generation interval
    pub fn interval(&self) -> Duration {
        self.config.interval
    }

    /// Get the aggregation window
    pub fn window(&self) -> Duration {
        self.config.window
    }

    /// Create health metric definitions for a service
    pub fn create_metric_definitions(&self, service_name: &str) -> Vec<Metric> {
        vec![
            self.create_metric(
                names::SPAN_ERROR_RATE,
                "Span error rate (0.0-1.0)",
                "1",
                service_name,
            ),
            self.create_metric(
                names::HTTP_ERROR_RATE,
                "HTTP error rate from status codes (0.0-1.0)",
                "1",
                service_name,
            ),
            self.create_metric(
                names::HTTP_2XX_RATE,
                "HTTP 2xx success rate (0.0-1.0)",
                "1",
                service_name,
            ),
            self.create_metric(
                names::HTTP_4XX_RATE,
                "HTTP 4xx client error rate (0.0-1.0)",
                "1",
                service_name,
            ),
            self.create_metric(
                names::HTTP_5XX_RATE,
                "HTTP 5xx server error rate (0.0-1.0)",
                "1",
                service_name,
            ),
            self.create_metric(
                names::LATENCY_P50,
                "Latency p50 percentile",
                "ns",
                service_name,
            ),
            self.create_metric(
                names::LATENCY_P95,
                "Latency p95 percentile",
                "ns",
                service_name,
            ),
            self.create_metric(
                names::LATENCY_P99,
                "Latency p99 percentile",
                "ns",
                service_name,
            ),
            self.create_metric(
                names::THROUGHPUT,
                "Throughput (spans per minute)",
                "{spans}/min",
                service_name,
            ),
            self.create_metric(
                names::ERROR_LOG_RATE,
                "Error log rate (logs per minute)",
                "{logs}/min",
                service_name,
            ),
        ]
    }

    /// Create health metric data points from computed stats
    pub fn create_data_points(
        &self,
        service_name: &str,
        stats: &HealthStats,
        timestamp: Timestamp,
    ) -> Vec<MetricDataPoint> {
        vec![
            self.create_data_point(
                names::SPAN_ERROR_RATE,
                service_name,
                stats.span_error_rate(),
                timestamp,
            ),
            self.create_data_point(
                names::HTTP_ERROR_RATE,
                service_name,
                stats.http_error_rate(),
                timestamp,
            ),
            self.create_data_point(
                names::HTTP_2XX_RATE,
                service_name,
                stats.http_2xx_rate(),
                timestamp,
            ),
            self.create_data_point(
                names::HTTP_4XX_RATE,
                service_name,
                stats.http_4xx_rate(),
                timestamp,
            ),
            self.create_data_point(
                names::HTTP_5XX_RATE,
                service_name,
                stats.http_5xx_rate(),
                timestamp,
            ),
            self.create_data_point(
                names::LATENCY_P50,
                service_name,
                stats.latency_p50() as f64,
                timestamp,
            ),
            self.create_data_point(
                names::LATENCY_P95,
                service_name,
                stats.latency_p95() as f64,
                timestamp,
            ),
            self.create_data_point(
                names::LATENCY_P99,
                service_name,
                stats.latency_p99() as f64,
                timestamp,
            ),
            self.create_data_point(
                names::THROUGHPUT,
                service_name,
                stats.throughput(),
                timestamp,
            ),
            self.create_data_point(
                names::ERROR_LOG_RATE,
                service_name,
                stats.error_log_rate(),
                timestamp,
            ),
        ]
    }

    fn create_metric(
        &self,
        name: &str,
        description: &str,
        unit: &str,
        service_name: &str,
    ) -> Metric {
        Metric {
            id: MetricId::from_name_and_service(name, service_name),
            name: name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metric_type: MetricType::Gauge as u8,
            aggregation_temporality: 0, // Unspecified for health metrics
            resource_id: 0,             // FIXME: Need proper resource registry
            scope_id: 0,
            is_generated: true,
        }
    }

    fn create_data_point(
        &self,
        metric_name: &str,
        service_name: &str,
        value: f64,
        timestamp: Timestamp,
    ) -> MetricDataPoint {
        MetricDataPoint {
            metric_id: MetricId::from_name_and_service(metric_name, service_name),
            timestamp,
            start_time: None,
            value,
            attributes: HashMap::new(),
            resource_id: 0, // FIXME: Need proper resource registry
        }
    }
}

impl Default for HealthMetricGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_stats_calculations() {
        let stats = HealthStats {
            total_span_count: 100,
            error_span_count: 5,
            http_2xx_count: 80,
            http_3xx_count: 5,
            http_4xx_count: 10,
            http_5xx_count: 5,
            latencies: vec![100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            total_log_count: 50,
            error_log_count: 3,
            window_minutes: 5.0,
        };

        assert!((stats.span_error_rate() - 0.05).abs() < 0.001);
        assert!((stats.http_error_rate() - 0.15).abs() < 0.001);
        assert!((stats.throughput() - 20.0).abs() < 0.001);
        assert!((stats.error_log_rate() - 0.6).abs() < 0.001);

        // Latency percentiles
        assert_eq!(stats.latency_p50(), 600); // ~median
        assert!(stats.latency_p95() >= 900);
        assert!(stats.latency_p99() >= 900);
    }

    #[test]
    fn test_health_stats_empty() {
        let stats = HealthStats::default();

        assert_eq!(stats.span_error_rate(), 0.0);
        assert_eq!(stats.http_error_rate(), 0.0);
        assert_eq!(stats.throughput(), 0.0);
        assert_eq!(stats.error_log_rate(), 0.0);
        assert_eq!(stats.latency_p50(), 0);
    }

    #[test]
    fn test_metric_definitions() {
        let generator = HealthMetricGenerator::new();
        let metrics = generator.create_metric_definitions("test-service");

        assert_eq!(metrics.len(), 10);
        for metric in &metrics {
            assert!(metric.is_generated);
            assert!(metric.name.starts_with("sequins.health."));
            assert_eq!(metric.resource_id, 0); // Service name moved to resource registry
            assert_eq!(metric.get_metric_type(), MetricType::Gauge);
        }
    }

    #[test]
    fn test_data_points() {
        let generator = HealthMetricGenerator::new();
        let stats = HealthStats {
            total_span_count: 100,
            error_span_count: 10,
            http_2xx_count: 90,
            http_3xx_count: 0,
            http_4xx_count: 5,
            http_5xx_count: 5,
            latencies: vec![100, 200, 300],
            total_log_count: 50,
            error_log_count: 5,
            window_minutes: 5.0,
        };

        let timestamp = Timestamp::from_nanos(1234567890000000000);
        let data_points = generator.create_data_points("test-service", &stats, timestamp);

        assert_eq!(data_points.len(), 10);
        for dp in &data_points {
            assert_eq!(dp.timestamp, timestamp);
        }
    }
}
