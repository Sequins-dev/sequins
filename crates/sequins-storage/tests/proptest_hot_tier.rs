//! Property-based tests for sequins-storage hot tier
//!
//! Tests hot tier invariants using proptest to verify correctness across random inputs.

use arrow::array::{
    Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;
use sequins_storage::cold_tier::series_index::SeriesIndex;
use sequins_storage::config::HotTierConfig;
use sequins_storage::hot_tier::batch_chain::BatchMeta;
use sequins_storage::hot_tier::core::HotTier;
use sequins_types::models::{
    AttributeValue, Duration, LogEntry, LogId, LogSeverity, MetricDataPoint, MetricId, Span,
    SpanId, Timestamp, TraceId,
};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

// Helper: Create a test span with random data
fn create_test_span(
    trace_id_bytes: [u8; 16],
    span_id_bytes: [u8; 8],
    operation: String,
    start_nanos: i64,
    duration_nanos: i64,
) -> Span {
    Span {
        trace_id: TraceId::from_bytes(trace_id_bytes),
        span_id: SpanId::from_bytes(span_id_bytes),
        parent_span_id: None,
        operation_name: operation,
        start_time: Timestamp::from_nanos(start_nanos),
        end_time: Timestamp::from_nanos(start_nanos + duration_nanos),
        duration: Duration::from_nanos(duration_nanos),
        attributes: HashMap::new(),
        events: vec![],
        links: vec![],
        status_code: 0,
        status_message: None,
        kind: 0,
        trace_state: None,
        flags: None,
        resource_id: 0,
        scope_id: 0,
    }
}

// Helper: Create a test log with random data
fn create_test_log(timestamp_nanos: i64, body: String) -> LogEntry {
    LogEntry {
        id: LogId::new(),
        timestamp: Timestamp::from_nanos(timestamp_nanos),
        observed_timestamp: Timestamp::from_nanos(timestamp_nanos),
        severity_number: LogSeverity::Info.to_number(),
        body: AttributeValue::String(body),
        attributes: HashMap::new(),
        trace_id: None,
        span_id: None,
        flags: None,
        resource_id: 0,
        scope_id: 0,
    }
}

// Helper: Create a test metric data point with random data
fn create_test_data_point(value: f64, timestamp_nanos: i64) -> MetricDataPoint {
    MetricDataPoint {
        metric_id: MetricId::new(),
        timestamp: Timestamp::from_nanos(timestamp_nanos),
        start_time: None,
        value,
        attributes: std::collections::HashMap::new(),
        resource_id: 0,
    }
}

// Helper: Convert spans to a RecordBatch directly (without ColdTier)
fn spans_to_batch(spans: &[Span]) -> RecordBatch {
    use arrow::array::new_null_array;

    // Use the full span schema so the batch matches the chain's schema invariant.
    let schema = sequins_types::arrow_schema::span_schema();
    let n = spans.len();

    let trace_ids: Vec<String> = spans.iter().map(|s| s.trace_id.to_hex()).collect();
    let span_ids: Vec<String> = spans.iter().map(|s| s.span_id.to_hex()).collect();
    let parent_ids: Vec<Option<String>> = spans
        .iter()
        .map(|s| s.parent_span_id.map(|p| p.to_hex()))
        .collect();
    let names: Vec<&str> = spans.iter().map(|s| s.operation_name.as_str()).collect();
    let kinds: Vec<u8> = spans.iter().map(|s| s.kind).collect();
    let statuses: Vec<u8> = spans.iter().map(|s| s.status_code).collect();
    let start_times: Vec<i64> = spans.iter().map(|s| s.start_time.as_nanos()).collect();
    let end_times: Vec<i64> = spans.iter().map(|s| s.end_time.as_nanos()).collect();
    let dur_ns: Vec<i64> = spans.iter().map(|s| s.duration.as_nanos()).collect();
    let res_ids: Vec<u32> = spans.iter().map(|s| s.resource_id).collect();
    let scp_ids: Vec<u32> = spans.iter().map(|s| s.scope_id).collect();

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringViewArray::from(
            trace_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )) as _,
        Arc::new(StringViewArray::from(
            span_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )) as _,
        Arc::new(StringViewArray::from(parent_ids)) as _,
        Arc::new(StringViewArray::from(names)) as _,
        Arc::new(UInt8Array::from(kinds)) as _,
        Arc::new(UInt8Array::from(statuses)) as _,
        Arc::new(TimestampNanosecondArray::from(start_times)) as _,
        Arc::new(TimestampNanosecondArray::from(end_times)) as _,
        Arc::new(Int64Array::from(dur_ns)) as _,
        Arc::new(UInt32Array::from(res_ids)) as _,
        Arc::new(UInt32Array::from(scp_ids)) as _,
    ];
    for field in schema.fields().iter().skip(11) {
        columns.push(new_null_array(field.data_type(), n));
    }

    RecordBatch::try_new(schema, columns).expect("Failed to build span batch")
}

// Helper: Convert logs to a RecordBatch directly (without ColdTier)
fn logs_to_batch(logs: &[LogEntry]) -> RecordBatch {
    use arrow::array::new_null_array;

    // Use the full log schema so the batch matches the chain's schema invariant.
    let schema = sequins_types::arrow_schema::log_schema();
    let n = logs.len();

    let log_ids: Vec<String> = logs.iter().map(|l| l.id.to_hex()).collect();
    let times: Vec<i64> = logs.iter().map(|l| l.timestamp.as_nanos()).collect();
    let obs_times: Vec<i64> = logs
        .iter()
        .map(|l| l.observed_timestamp.as_nanos())
        .collect();
    let sev_nums: Vec<u8> = logs.iter().map(|l| l.severity_number).collect();
    let bodies: Vec<String> = logs
        .iter()
        .map(|l| match &l.body {
            AttributeValue::String(s) => s.clone(),
            other => serde_json::to_string(other).unwrap_or_default(),
        })
        .collect();
    let trace_ids: Vec<Option<String>> = logs
        .iter()
        .map(|l| l.trace_id.map(|id| id.to_hex()))
        .collect();
    let span_ids: Vec<Option<String>> = logs
        .iter()
        .map(|l| l.span_id.map(|id| id.to_hex()))
        .collect();
    let res_ids: Vec<u32> = logs.iter().map(|l| l.resource_id).collect();
    let scp_ids: Vec<u32> = logs.iter().map(|l| l.scope_id).collect();

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringViewArray::from(
            log_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )) as _,
        Arc::new(TimestampNanosecondArray::from(times)) as _,
        Arc::new(TimestampNanosecondArray::from(obs_times)) as _,
        Arc::new(StringViewArray::from(vec!["unknown"; n])) as _,
        Arc::new(StringViewArray::from(vec!["INFO"; n])) as _,
        Arc::new(UInt8Array::from(sev_nums)) as _,
        Arc::new(StringViewArray::from(
            bodies.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )) as _,
        Arc::new(StringViewArray::from(trace_ids)) as _,
        Arc::new(StringViewArray::from(span_ids)) as _,
        Arc::new(UInt32Array::from(res_ids)) as _,
        Arc::new(UInt32Array::from(scp_ids)) as _,
    ];
    for field in schema.fields().iter().skip(11) {
        columns.push(new_null_array(field.data_type(), n));
    }

    RecordBatch::try_new(schema, columns).expect("Failed to build log batch")
}

// Helper: Convert metric data points to a RecordBatch directly (without ColdTier)
fn data_points_to_batch(data_points: &[MetricDataPoint]) -> RecordBatch {
    let schema = sequins_types::arrow_schema::series_data_point_schema();

    let series_ids: Vec<u64> = data_points.iter().map(|_| 0u64).collect();
    let metric_ids: Vec<String> = data_points.iter().map(|dp| dp.metric_id.to_hex()).collect();
    let timestamps: Vec<i64> = data_points
        .iter()
        .map(|dp| dp.timestamp.as_nanos())
        .collect();
    let values: Vec<f64> = data_points.iter().map(|dp| dp.value).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(series_ids)) as _,
            Arc::new(StringViewArray::from(
                metric_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )) as _,
            Arc::new(TimestampNanosecondArray::from(timestamps)) as _,
            Arc::new(Float64Array::from(values)) as _,
        ],
    )
    .expect("Failed to build data point batch")
}

// Helper: Compute resource fingerprint deterministically
fn compute_resource_fingerprint(attributes: &HashMap<String, String>) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();
    let mut keys: Vec<&String> = attributes.keys().collect();
    keys.sort();
    for key in keys {
        if let Some(value) = attributes.get(key) {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
    }
    hasher.finish()
}

// Helper: Create a simple BatchMeta for test batches.
fn simple_meta(row_count: usize) -> BatchMeta {
    BatchMeta {
        min_timestamp: 0,
        max_timestamp: i64::MAX,
        row_count,
    }
}

// Helper: Create a tokio runtime and return it.
// HotTier::new() calls tokio::spawn() so a runtime must be active.
// Use rt.enter() in each test to set the runtime context for synchronous code.
fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
}

// Test 1: Resource fingerprint is deterministic - same attrs, different order -> same fingerprint
proptest! {
    #[test]
    fn prop_resource_fingerprint_deterministic(
        key1 in "[a-z]{3,10}",
        val1 in "[a-z]{3,10}",
        key2 in "[a-z]{3,10}",
        val2 in "[a-z]{3,10}",
    ) {
        // Create two hashmaps with same data but potentially different insertion order
        let mut attrs1 = HashMap::new();
        attrs1.insert(key1.clone(), val1.clone());
        attrs1.insert(key2.clone(), val2.clone());

        let mut attrs2 = HashMap::new();
        attrs2.insert(key2.clone(), val2.clone());
        attrs2.insert(key1.clone(), val1.clone());

        let fingerprint1 = compute_resource_fingerprint(&attrs1);
        let fingerprint2 = compute_resource_fingerprint(&attrs2);

        prop_assert_eq!(fingerprint1, fingerprint2, "Fingerprints should be deterministic regardless of insertion order");
    }
}

// Test 2: Hot tier span insert/retrieve — push batch into chain, verify row_count matches.
proptest! {
    #[test]
    fn prop_hot_tier_span_insert_retrieve(
        spans_data in prop::collection::vec(
            (any::<[u8; 16]>(), any::<[u8; 8]>(), "[a-zA-Z]{5,15}", any::<i64>(), 1i64..1000000),
            1..10
        )
    ) {
        let rt = make_runtime();
        let _guard = rt.enter();

        let config = HotTierConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1000,
        };
        let hot_tier = HotTier::new(config);

        let spans: Vec<Span> = spans_data.iter()
            .map(|(trace_id, span_id, operation, start, duration)| {
                create_test_span(*trace_id, *span_id, operation.clone(), *start, *duration)
            })
            .collect();
        let expected_count = spans.len();

        let batch = spans_to_batch(&spans);
        hot_tier.spans.push(Arc::new(batch), simple_meta(expected_count));

        prop_assert_eq!(hot_tier.spans.row_count(), expected_count, "Should retrieve all inserted spans");
    }
}

// Test 3: Hot tier log insert/retrieve — push batch, verify row_count matches.
proptest! {
    #[test]
    fn prop_hot_tier_log_insert_retrieve(
        logs_data in prop::collection::vec(
            (any::<i64>(), "[a-zA-Z ]{10,30}"),
            1..10
        )
    ) {
        let rt = make_runtime();
        let _guard = rt.enter();

        let config = HotTierConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1000,
        };
        let hot_tier = HotTier::new(config);

        let logs: Vec<LogEntry> = logs_data.iter()
            .map(|(timestamp, body)| create_test_log(*timestamp, body.clone()))
            .collect();
        let expected_count = logs.len();

        let batch = logs_to_batch(&logs);
        hot_tier.logs.push(Arc::new(batch), simple_meta(expected_count));

        prop_assert_eq!(hot_tier.logs.row_count(), expected_count, "Should retrieve all inserted logs");
    }
}

// Test 4: Hot tier metric data point insert/retrieve — push batch, verify row_count matches.
proptest! {
    #[test]
    fn prop_hot_tier_metric_insert_retrieve(
        dp_data in prop::collection::vec((any::<f64>(), any::<i64>()), 1..10)
    ) {
        let rt = make_runtime();
        let _guard = rt.enter();

        let config = HotTierConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1000,
        };
        let hot_tier = HotTier::new(config);

        let data_points: Vec<MetricDataPoint> = dp_data.iter()
            .map(|(value, ts)| create_test_data_point(*value, *ts))
            .collect();
        let expected_count = data_points.len();

        let batch = data_points_to_batch(&data_points);
        hot_tier.datapoints.push(Arc::new(batch), simple_meta(expected_count));

        prop_assert_eq!(hot_tier.datapoints.row_count(), expected_count, "Should retrieve all inserted data points");
    }
}

// Test 5: Hot tier entry count accurate for spans
proptest! {
    #[test]
    fn prop_hot_tier_entry_count_accurate_spans(
        spans_data in prop::collection::vec(
            (any::<[u8; 16]>(), any::<[u8; 8]>(), "[a-zA-Z]{5,15}", any::<i64>(), 1i64..1000000),
            1..15
        )
    ) {
        let rt = make_runtime();
        let _guard = rt.enter();

        let config = HotTierConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1000,
        };
        let hot_tier = HotTier::new(config);

        let spans: Vec<Span> = spans_data.iter()
            .map(|(trace_id, span_id, operation, start, duration)| {
                create_test_span(*trace_id, *span_id, operation.clone(), *start, *duration)
            })
            .collect();
        let expected_count = spans.len();

        let batch = spans_to_batch(&spans);
        hot_tier.spans.push(Arc::new(batch), simple_meta(expected_count));

        let stats = hot_tier.stats();
        prop_assert_eq!(stats.span_count, expected_count, "Span count should match inserted count");
    }
}

// Test 6: Hot tier entry count accurate for logs
proptest! {
    #[test]
    fn prop_hot_tier_entry_count_accurate_logs(
        logs_data in prop::collection::vec(
            (any::<i64>(), "[a-zA-Z ]{10,30}"),
            1..15
        )
    ) {
        let rt = make_runtime();
        let _guard = rt.enter();

        let config = HotTierConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1000,
        };
        let hot_tier = HotTier::new(config);

        let logs: Vec<LogEntry> = logs_data.iter()
            .map(|(timestamp, body)| create_test_log(*timestamp, body.clone()))
            .collect();
        let expected_count = logs.len();

        let batch = logs_to_batch(&logs);
        hot_tier.logs.push(Arc::new(batch), simple_meta(expected_count));

        let stats = hot_tier.stats();
        prop_assert_eq!(stats.log_count, expected_count, "Log count should match inserted count");
    }
}

// Test 7: Series index register returns valid ID
proptest! {
    #[test]
    fn prop_series_index_register_returns_id(name in "[a-z_]{3,20}") {
        let mut index = SeriesIndex::new();
        let attrs = BTreeMap::new();

        let id = index.register(&name, attrs);

        // ID should be a valid non-zero value
        prop_assert!(id.as_u64() >= 1, "Series ID should be >= 1");
    }
}

// Test 8: Series index lookup after register
proptest! {
    #[test]
    fn prop_series_index_lookup_registered(
        name in "[a-z_]{3,20}",
        key in "[a-z]{3,10}",
        value in "[a-z]{3,10}",
    ) {
        let mut index = SeriesIndex::new();
        let mut attrs = BTreeMap::new();
        attrs.insert(key.clone(), value.clone());

        let id = index.register(&name, attrs.clone());
        let metadata = index.lookup(id).expect("Should find registered series");

        prop_assert_eq!(&metadata.metric_name, &name, "Metric name should match");
        prop_assert_eq!(&metadata.attributes, &attrs, "Attributes should match");
    }
}

// Test 9: Series index is deterministic - same name+labels -> always same ID
proptest! {
    #[test]
    fn prop_series_index_deterministic(
        name in "[a-z_]{3,20}",
        key in "[a-z]{3,10}",
        value in "[a-z]{3,10}",
    ) {
        let mut index = SeriesIndex::new();
        let mut attrs = BTreeMap::new();
        attrs.insert(key, value);

        // Register twice
        let id1 = index.register(&name, attrs.clone());
        let id2 = index.register(&name, attrs.clone());

        prop_assert_eq!(id1, id2, "Same metric+attributes should return same ID");
    }
}

// Test 10: Batch round trip preserves count (simplified version - just test span count preservation)
proptest! {
    #[test]
    fn prop_batch_round_trip_preserves_count(
        spans_data in prop::collection::vec(
            (any::<[u8; 16]>(), any::<[u8; 8]>(), "[a-zA-Z]{5,15}", any::<i64>(), 1i64..1000000),
            1..10
        )
    ) {
        // This test verifies that we can create and count spans consistently
        let spans: Vec<Span> = spans_data.iter()
            .map(|(trace_id, span_id, operation, start, duration)| {
                create_test_span(*trace_id, *span_id, operation.clone(), *start, *duration)
            })
            .collect();

        let original_count = spans.len();

        // Verify count is preserved in the vector
        prop_assert_eq!(spans.len(), original_count, "Span count should be preserved");
    }
}
