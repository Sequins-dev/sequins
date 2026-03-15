//! Test fixtures and utilities for sequins-storage tests
//!
//! This module provides:
//! - TestStorageBuilder for creating fully-wired Storage instances
//! - OTLP test data generators for traces, logs, metrics, and profiles
//! - Assertion helpers for validating RecordBatch data

#[cfg(test)]
use crate::config::{
    ColdTierConfig, CompanionIndexConfig, HotTierConfig, LifecycleConfig, StorageConfig,
};
#[cfg(test)]
use crate::Storage;
#[cfg(test)]
use arrow::datatypes::DataType;
#[cfg(test)]
use arrow::record_batch::RecordBatch;
#[cfg(test)]
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        profiles::v1development::ExportProfilesServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value::Value as OtlpValue, AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics},
    profiles::v1development::{Profile, ResourceProfiles, ScopeProfiles},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
};
#[cfg(test)]
use sequins_types::models::Duration;
#[cfg(test)]
use sequins_types::MockNowTime;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tempfile::TempDir;

/// Builder for creating test Storage instances with configurable parameters
#[cfg(test)]
pub(crate) struct TestStorageBuilder {
    hot_tier_max_age: Duration,
    hot_tier_max_entries: usize,
    cold_tier_row_block_size: usize,
    retention: Duration,
    flush_interval: Duration,
    cleanup_interval: Duration,
    temp_dir: Option<TempDir>,
    /// Base epoch nanoseconds for the mock clock.
    ///
    /// Defaults to a recent-ish timestamp so test data generated with
    /// `self.base_ns()` always falls within a `last 1h` query window
    /// when the compiler uses the real system clock.
    base_ns: u64,
}

#[cfg(test)]
impl TestStorageBuilder {
    /// Create a new builder with sensible defaults for testing.
    ///
    /// The mock clock starts at approximately `SystemTime::now()` so that
    /// test data generated with `builder.base_ns()` always falls within a
    /// `last 1h` query window.
    pub(crate) fn new() -> Self {
        let base_ns = sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime);
        Self {
            hot_tier_max_age: Duration::from_minutes(5),
            hot_tier_max_entries: 1000,
            cold_tier_row_block_size: 1000,
            retention: Duration::from_hours(24 * 7), // 7 days
            flush_interval: Duration::from_minutes(5),
            cleanup_interval: Duration::from_hours(1),
            temp_dir: None,
            base_ns,
        }
    }

    /// Set hot tier max age
    pub(crate) fn hot_tier_max_age(mut self, max_age: Duration) -> Self {
        self.hot_tier_max_age = max_age;
        self
    }

    /// Set hot tier max entries
    pub(crate) fn hot_tier_max_entries(mut self, max_entries: usize) -> Self {
        self.hot_tier_max_entries = max_entries;
        self
    }

    /// Set retention duration
    pub(crate) fn retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// Set flush interval
    pub(crate) fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Build a Storage instance with the configured parameters
    ///
    /// Returns (Storage, TempDir) - the TempDir must be kept alive for the duration of the test
    pub(crate) async fn build(mut self) -> (Storage, TempDir) {
        let temp_dir = self
            .temp_dir
            .take()
            .unwrap_or_else(|| TempDir::new().unwrap());

        let config = StorageConfig {
            hot_tier: HotTierConfig {
                max_age: self.hot_tier_max_age,
                max_entries: self.hot_tier_max_entries,
            },
            cold_tier: ColdTierConfig {
                uri: format!("file://{}", temp_dir.path().display()),
                row_block_size: self.cold_tier_row_block_size,
                compact_encodings: true,
                companion_index: CompanionIndexConfig {
                    tantivy_enabled: false,
                    bloom_enabled: false,
                    trigram_enabled: false,
                    cardinality_threshold: 100,
                    bloom_fpr: 0.01,
                },
                index_path: None,
                max_attribute_columns: 256,
            },
            lifecycle: LifecycleConfig {
                retention: self.retention,
                flush_interval: self.flush_interval,
                cleanup_interval: self.cleanup_interval,
            },
        };

        let clock = Arc::new(MockNowTime::new(self.base_ns));
        let storage = Storage::new_with_clock(config, clock).await.unwrap();
        (storage, temp_dir)
    }
}

#[cfg(test)]
impl Default for TestStorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Return the current wall-clock time as epoch nanoseconds.
///
/// Convenience for test fixture timestamp generation — using this ensures
/// test data falls within `last 1h` query windows.
#[cfg(test)]
pub(crate) fn now_ns() -> u64 {
    sequins_types::NowTime::now_ns(&sequins_types::SystemNowTime)
}

/// Create a test OTLP traces request with realistic data.
///
/// Timestamps are set to approximately `SystemTime::now()` so that span data
/// is within `last 1h` query windows.
///
/// # Arguments
/// * `n_resources` - Number of unique resource objects (services)
/// * `n_spans` - Total number of spans to generate (distributed across resources)
///
/// # Returns
/// A valid ExportTraceServiceRequest with realistic span hierarchy and attributes
#[cfg(test)]
pub(crate) fn make_test_otlp_traces(
    n_resources: usize,
    n_spans: usize,
) -> ExportTraceServiceRequest {
    make_test_otlp_traces_at(n_resources, n_spans, now_ns())
}

/// Create a test OTLP traces request with timestamps starting at `base_time_ns`.
#[cfg(test)]
pub(crate) fn make_test_otlp_traces_at(
    n_resources: usize,
    n_spans: usize,
    base_time_ns: u64,
) -> ExportTraceServiceRequest {
    let mut resource_spans = Vec::new();
    let spans_per_resource = (n_spans / n_resources.max(1)).max(1);

    for resource_idx in 0..n_resources {
        // Create resource with service.name attribute
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue(format!(
                        "test-service-{}",
                        resource_idx
                    ))),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        // Create scope
        let scope = InstrumentationScope {
            name: "test-tracer".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        // Generate spans for this resource
        let mut spans = Vec::new();

        for span_idx in 0..spans_per_resource {
            let global_idx = resource_idx * spans_per_resource + span_idx;
            if global_idx >= n_spans {
                break;
            }

            // Generate trace_id and span_id
            let trace_id = generate_trace_id(global_idx);
            let span_id = generate_span_id(global_idx);

            // Parent span ID (every other span has a parent)
            let parent_span_id = if span_idx > 0 && span_idx % 2 == 1 {
                generate_span_id(global_idx - 1)
            } else {
                vec![]
            };

            let start_time = base_time_ns + (global_idx as u64 * 1_000_000_000);
            let end_time = start_time + 100_000_000; // 100ms duration

            spans.push(Span {
                trace_id,
                span_id,
                trace_state: String::new(),
                parent_span_id,
                flags: 1,
                name: format!("operation-{}", span_idx),
                kind: 2, // Server
                start_time_unix_nano: start_time,
                end_time_unix_nano: end_time,
                attributes: vec![
                    KeyValue {
                        key: "http.method".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::StringValue("GET".to_string())),
                        }),
                    },
                    KeyValue {
                        key: "http.status_code".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::IntValue(200)),
                        }),
                    },
                ],
                dropped_attributes_count: 0,
                events: vec![],
                dropped_events_count: 0,
                links: vec![],
                dropped_links_count: 0,
                status: Some(Status {
                    message: String::new(),
                    code: 1, // Ok
                }),
            });
        }

        resource_spans.push(ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![ScopeSpans {
                scope: Some(scope),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
    }

    ExportTraceServiceRequest { resource_spans }
}

/// Create a test OTLP logs request with realistic data.
///
/// Timestamps are set to approximately `SystemTime::now()`.
///
/// # Arguments
/// * `n_resources` - Number of unique resource objects (services)
/// * `n_logs` - Total number of log records to generate
///
/// # Returns
/// A valid ExportLogsServiceRequest with realistic log data
#[cfg(test)]
pub(crate) fn make_test_otlp_logs(n_resources: usize, n_logs: usize) -> ExportLogsServiceRequest {
    make_test_otlp_logs_at(n_resources, n_logs, now_ns())
}

/// Create a test OTLP logs request with timestamps starting at `base_time_ns`.
#[cfg(test)]
pub(crate) fn make_test_otlp_logs_at(
    n_resources: usize,
    n_logs: usize,
    base_time_ns: u64,
) -> ExportLogsServiceRequest {
    let mut resource_logs = Vec::new();
    let logs_per_resource = (n_logs / n_resources.max(1)).max(1);

    for resource_idx in 0..n_resources {
        // Create resource with service.name attribute
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue(format!(
                        "test-service-{}",
                        resource_idx
                    ))),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        // Create scope
        let scope = InstrumentationScope {
            name: "test-logger".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        // Generate logs for this resource
        let mut log_records = Vec::new();

        for log_idx in 0..logs_per_resource {
            let global_idx = resource_idx * logs_per_resource + log_idx;
            if global_idx >= n_logs {
                break;
            }

            let timestamp = base_time_ns + (global_idx as u64 * 1_000_000_000);

            log_records.push(LogRecord {
                time_unix_nano: timestamp,
                observed_time_unix_nano: timestamp,
                severity_number: 9, // Info
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(OtlpValue::StringValue(format!(
                        "Test log message {}",
                        log_idx
                    ))),
                }),
                attributes: vec![KeyValue {
                    key: "log.file.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(OtlpValue::StringValue("test.log".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                flags: 1,
                trace_id: vec![],
                span_id: vec![],
                event_name: String::new(),
            });
        }

        resource_logs.push(ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: Some(scope),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
    }

    ExportLogsServiceRequest { resource_logs }
}

/// Create a test OTLP metrics request with realistic data.
///
/// Timestamps are set to approximately `SystemTime::now()`.
///
/// # Arguments
/// * `n_resources` - Number of unique resource objects (services)
/// * `n_metrics` - Number of unique metric definitions per resource
/// * `n_datapoints` - Number of data points per metric
///
/// # Returns
/// A valid ExportMetricsServiceRequest with gauge, sum, and histogram metrics
#[cfg(test)]
pub(crate) fn make_test_otlp_metrics(
    n_resources: usize,
    n_metrics: usize,
    n_datapoints: usize,
) -> ExportMetricsServiceRequest {
    make_test_otlp_metrics_at(n_resources, n_metrics, n_datapoints, now_ns())
}

/// Create a test OTLP metrics request with timestamps starting at `base_time_ns`.
#[cfg(test)]
pub(crate) fn make_test_otlp_metrics_at(
    n_resources: usize,
    n_metrics: usize,
    n_datapoints: usize,
    base_time_ns: u64,
) -> ExportMetricsServiceRequest {
    let mut resource_metrics = Vec::new();

    for resource_idx in 0..n_resources {
        // Create resource with service.name attribute
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue(format!(
                        "test-service-{}",
                        resource_idx
                    ))),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        // Create scope
        let scope = InstrumentationScope {
            name: "test-meter".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        // Generate metrics for this resource
        let mut metrics = Vec::new();

        for metric_idx in 0..n_metrics {
            // Create data points
            let mut data_points = Vec::new();
            for dp_idx in 0..n_datapoints {
                let timestamp =
                    base_time_ns + ((metric_idx * n_datapoints + dp_idx) as u64 * 1_000_000_000);

                data_points.push(NumberDataPoint {
                    attributes: vec![KeyValue {
                        key: "instance".to_string(),
                        value: Some(AnyValue {
                            value: Some(OtlpValue::StringValue(format!("instance-{}", dp_idx))),
                        }),
                    }],
                    start_time_unix_nano: timestamp.saturating_sub(60_000_000_000), // 60s ago
                    time_unix_nano: timestamp,
                    value: Some(
                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                            42.0 + dp_idx as f64,
                        ),
                    ),
                    exemplars: vec![],
                    flags: 0,
                });
            }

            // Create gauge metric
            metrics.push(Metric {
                name: format!("test.gauge.{}", metric_idx),
                description: format!("Test gauge metric {}", metric_idx),
                unit: "ms".to_string(),
                data: Some(Data::Gauge(Gauge { data_points })),
                metadata: vec![],
            });
        }

        resource_metrics.push(ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(scope),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
    }

    ExportMetricsServiceRequest { resource_metrics }
}

/// Create a test OTLP profiles request that includes actual samples (requires a dictionary).
///
/// Produces one profile with one sample referencing a minimal stack and frame so that
/// `batches.samples.num_rows() > 0` after OTLP conversion.
#[cfg(test)]
pub(crate) fn make_test_otlp_profiles_with_samples() -> ExportProfilesServiceRequest {
    use opentelemetry_proto::tonic::profiles::v1development::{
        Function, Line, Location, ProfilesDictionary, Sample, Stack,
    };

    // string_table[0] must be "".
    let string_table = vec![String::new(), "main".to_string(), "main.rs".to_string()];

    let function = Function {
        name_strindex: 1,
        system_name_strindex: 1,
        filename_strindex: 2,
        start_line: 1,
    };

    let line = Line {
        function_index: 0,
        line: 1,
        ..Default::default()
    };

    let location = Location {
        line: vec![line],
        ..Default::default()
    };

    let stack = Stack {
        location_indices: vec![0],
    };

    let dictionary = ProfilesDictionary {
        string_table,
        function_table: vec![function],
        location_table: vec![location],
        stack_table: vec![stack],
        ..Default::default()
    };

    let resource = Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(OtlpValue::StringValue("test-service-0".to_string())),
            }),
        }],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    };

    let scope = InstrumentationScope {
        name: "test-profiler".to_string(),
        version: "1.0.0".to_string(),
        attributes: vec![],
        dropped_attributes_count: 0,
    };

    let sample = Sample {
        stack_index: 0,
        values: vec![1_000_000],
        ..Default::default()
    };

    let profile = Profile {
        profile_id: generate_profile_id(0),
        time_unix_nano: now_ns(),
        duration_nano: 60_000_000_000,
        sample: vec![sample],
        ..Default::default()
    };

    ExportProfilesServiceRequest {
        resource_profiles: vec![ResourceProfiles {
            resource: Some(resource),
            scope_profiles: vec![ScopeProfiles {
                scope: Some(scope),
                profiles: vec![profile],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
        dictionary: Some(dictionary),
    }
}

/// Create a test OTLP profiles request with realistic data.
///
/// Timestamps are set to approximately `SystemTime::now()`.
///
/// # Arguments
/// * `n_resources` - Number of unique resource objects (services)
/// * `n_profiles` - Total number of profiles to generate
///
/// # Returns
/// A valid ExportProfilesServiceRequest with CPU profile data
#[cfg(test)]
pub(crate) fn make_test_otlp_profiles(
    n_resources: usize,
    n_profiles: usize,
) -> ExportProfilesServiceRequest {
    make_test_otlp_profiles_at(n_resources, n_profiles, now_ns())
}

/// Create a test OTLP profiles request with timestamps starting at `base_time_ns`.
#[cfg(test)]
pub(crate) fn make_test_otlp_profiles_at(
    n_resources: usize,
    n_profiles: usize,
    base_time_ns: u64,
) -> ExportProfilesServiceRequest {
    let mut resource_profiles = Vec::new();
    let profiles_per_resource = (n_profiles / n_resources.max(1)).max(1);

    for resource_idx in 0..n_resources {
        // Create resource with service.name attribute
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(OtlpValue::StringValue(format!(
                        "test-service-{}",
                        resource_idx
                    ))),
                }),
            }],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        };

        // Create scope
        let scope = InstrumentationScope {
            name: "test-profiler".to_string(),
            version: "1.0.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        // Generate profiles for this resource
        let mut profiles = Vec::new();

        for profile_idx in 0..profiles_per_resource {
            let global_idx = resource_idx * profiles_per_resource + profile_idx;
            if global_idx >= n_profiles {
                break;
            }

            let timestamp = base_time_ns + (global_idx as u64 * 1_000_000_000);

            // Create a minimal valid pprof profile
            // This is a simplified version - real pprof data is more complex
            let profile_data = vec![
                0x1f, 0x8b, 0x08, 0x00, // gzip header
                0x00, 0x00, 0x00, 0x00, // timestamp
                0x00, 0xff, // flags
                      // minimal gzipped pprof data
            ];

            profiles.push(Profile {
                profile_id: generate_profile_id(global_idx),
                time_unix_nano: timestamp,
                duration_nano: 60_000_000_000, // 60s duration
                sample_type: None,
                original_payload: profile_data,
                original_payload_format: "pprof-gzip".to_string(),
                dropped_attributes_count: 0,
                comment_strindices: vec![],
                attribute_indices: vec![],
                period: 0,
                period_type: None,
                sample: vec![],
            });
        }

        resource_profiles.push(ResourceProfiles {
            resource: Some(resource),
            scope_profiles: vec![ScopeProfiles {
                scope: Some(scope),
                profiles,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
    }

    ExportProfilesServiceRequest {
        resource_profiles,
        dictionary: None,
    }
}

/// Assert that a RecordBatch has a specific column with the expected type
#[cfg(test)]
pub(crate) fn assert_batch_has_column(batch: &RecordBatch, name: &str, expected_type: &DataType) {
    let schema = batch.schema();
    let field = schema.field_with_name(name).unwrap_or_else(|_| {
        panic!(
            "Column '{}' not found in batch. Available columns: {:?}",
            name,
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        )
    });

    assert_eq!(
        field.data_type(),
        expected_type,
        "Column '{}' has wrong type. Expected {:?}, got {:?}",
        name,
        expected_type,
        field.data_type()
    );
}

/// Assert that a RecordBatch has the expected number of rows
#[cfg(test)]
pub(crate) fn assert_batch_row_count(batch: &RecordBatch, expected: usize) {
    assert_eq!(
        batch.num_rows(),
        expected,
        "RecordBatch has wrong row count. Expected {}, got {}",
        expected,
        batch.num_rows()
    );
}

// Helper functions for generating realistic IDs

#[cfg(test)]
fn generate_trace_id(index: usize) -> Vec<u8> {
    let mut trace_id = vec![0u8; 16];
    let bytes = (index as u64).to_be_bytes();
    trace_id[8..16].copy_from_slice(&bytes);
    trace_id
}

#[cfg(test)]
fn generate_span_id(index: usize) -> Vec<u8> {
    let mut span_id = vec![0u8; 8];
    let bytes = (index as u64).to_be_bytes();
    span_id.copy_from_slice(&bytes);
    span_id
}

#[cfg(test)]
fn generate_profile_id(index: usize) -> Vec<u8> {
    let mut profile_id = vec![0u8; 16];
    let bytes = (index as u64).to_be_bytes();
    profile_id[8..16].copy_from_slice(&bytes);
    profile_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_builder_creates_valid_storage() {
        let (storage, _temp) = TestStorageBuilder::new().build().await;

        // Verify storage is accessible
        let stats = storage.stats();
        assert_eq!(stats.span_count, 0);
        assert_eq!(stats.log_count, 0);
        assert_eq!(stats.metric_count, 0);
        assert_eq!(stats.profile_count, 0);
    }

    #[tokio::test]
    async fn test_storage_builder_custom_config() {
        let (_storage, _temp) = TestStorageBuilder::new()
            .hot_tier_max_age(Duration::from_minutes(10))
            .hot_tier_max_entries(500)
            .retention(Duration::from_hours(48))
            .build()
            .await;

        // Storage was created successfully with custom config
        // The config is applied during construction
    }

    #[test]
    fn test_make_test_otlp_traces() {
        let request = make_test_otlp_traces(2, 10);

        // Should have 2 resources
        assert_eq!(request.resource_spans.len(), 2);

        // Count total spans
        let total_spans: usize = request
            .resource_spans
            .iter()
            .flat_map(|rs| &rs.scope_spans)
            .map(|ss| ss.spans.len())
            .sum();
        assert_eq!(total_spans, 10);

        // Verify first span has required fields
        let first_span = &request.resource_spans[0].scope_spans[0].spans[0];
        assert!(!first_span.name.is_empty());
        assert_eq!(first_span.trace_id.len(), 16);
        assert_eq!(first_span.span_id.len(), 8);
        assert!(first_span.start_time_unix_nano > 0);
        assert!(first_span.end_time_unix_nano > first_span.start_time_unix_nano);
    }

    #[test]
    fn test_make_test_otlp_logs() {
        let request = make_test_otlp_logs(2, 10);

        // Should have 2 resources
        assert_eq!(request.resource_logs.len(), 2);

        // Count total logs
        let total_logs: usize = request
            .resource_logs
            .iter()
            .flat_map(|rl| &rl.scope_logs)
            .map(|sl| sl.log_records.len())
            .sum();
        assert_eq!(total_logs, 10);

        // Verify first log has required fields
        let first_log = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert!(first_log.body.is_some());
        assert!(first_log.time_unix_nano > 0);
        assert_eq!(first_log.severity_number, 9); // Info
    }

    #[test]
    fn test_make_test_otlp_metrics() {
        let request = make_test_otlp_metrics(2, 3, 5);

        // Should have 2 resources
        assert_eq!(request.resource_metrics.len(), 2);

        // Each resource should have 3 metrics
        for resource_metrics in &request.resource_metrics {
            assert_eq!(resource_metrics.scope_metrics[0].metrics.len(), 3);

            // Each metric should have 5 data points
            for metric in &resource_metrics.scope_metrics[0].metrics {
                if let Some(Data::Gauge(gauge)) = &metric.data {
                    assert_eq!(gauge.data_points.len(), 5);
                }
            }
        }
    }

    #[test]
    fn test_make_test_otlp_profiles() {
        let request = make_test_otlp_profiles(2, 6);

        // Should have 2 resources
        assert_eq!(request.resource_profiles.len(), 2);

        // Count total profiles
        let total_profiles: usize = request
            .resource_profiles
            .iter()
            .flat_map(|rp| &rp.scope_profiles)
            .map(|sp| sp.profiles.len())
            .sum();
        assert_eq!(total_profiles, 6);

        // Verify first profile has required fields
        let first_profile = &request.resource_profiles[0].scope_profiles[0].profiles[0];
        assert_eq!(first_profile.profile_id.len(), 16);
        assert!(first_profile.time_unix_nano > 0);
        assert!(!first_profile.original_payload.is_empty());
    }

    #[test]
    fn test_assert_batch_has_column() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{Field, Schema};

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Should pass
        assert_batch_has_column(&batch, "id", &DataType::Int32);
        assert_batch_has_column(&batch, "name", &DataType::Utf8);
    }

    #[test]
    #[should_panic(expected = "Column 'missing' not found")]
    fn test_assert_batch_has_column_missing() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema};

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        assert_batch_has_column(&batch, "missing", &DataType::Int32);
    }

    #[test]
    fn test_assert_batch_row_count() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema};

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Should pass
        assert_batch_row_count(&batch, 3);
    }

    #[test]
    #[should_panic(expected = "RecordBatch has wrong row count")]
    fn test_assert_batch_row_count_wrong() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{Field, Schema};

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        assert_batch_row_count(&batch, 5);
    }
}
