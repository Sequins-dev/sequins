//! Pre-computed rollups for fast dashboard queries
//!
//! At flush time, we compute rollups across different time granularities
//! to enable fast dashboard queries without scanning raw data.

use arrow::array::{Array, Int64Array, StringViewArray, TimestampNanosecondArray, UInt8Array};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Timelike, Utc};
use object_store::{path::Path as ObjectPath, ObjectStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::series_index::SeriesId;
use crate::error::{Error, Result};

/// Span rollup for 1-minute buckets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanRollup {
    /// Service name
    pub service_name: String,

    /// Operation/span name
    pub span_name: String,

    /// 1-minute bucket timestamp (Unix epoch seconds)
    pub minute_bucket: i64,

    /// Total span count in this bucket
    pub count: u64,

    /// Error count (non-OK status)
    pub error_count: u64,

    /// Duration statistics (nanoseconds)
    pub duration_stats: DurationStats,
}

/// Duration statistics for a bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurationStats {
    /// Minimum duration in nanoseconds
    pub min: i64,

    /// Maximum duration in nanoseconds
    pub max: i64,

    /// Sum of all durations (for computing average)
    pub sum: i64,

    /// Simple histogram buckets: [p50, p90, p95, p99]
    /// Using fixed percentiles instead of t-digest for simplicity
    pub percentiles: PercentileStats,
}

/// Pre-computed percentile estimates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PercentileStats {
    pub p50: i64,
    pub p90: i64,
    pub p95: i64,
    pub p99: i64,
}

impl DurationStats {
    /// Create new stats from a list of durations
    fn from_durations(mut durations: Vec<i64>) -> Self {
        if durations.is_empty() {
            return Self {
                min: 0,
                max: 0,
                sum: 0,
                percentiles: PercentileStats {
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                },
            };
        }

        durations.sort_unstable();
        let len = durations.len();

        let min = durations[0];
        let max = durations[len - 1];
        let sum: i64 = durations.iter().sum();

        // Compute percentiles using 0-indexed formula: (n-1) * p / 100
        let p50 = durations[(len - 1) * 50 / 100];
        let p90 = durations[(len - 1) * 90 / 100];
        let p95 = durations[(len - 1) * 95 / 100];
        let p99 = durations[(len - 1) * 99 / 100];

        Self {
            min,
            max,
            sum,
            percentiles: PercentileStats { p50, p90, p95, p99 },
        }
    }
}

/// Compute span rollups from a RecordBatch of spans.
///
/// Groups spans by (service, operation, 1-minute bucket) and computes aggregates.
/// Extracts columns: `start_time_unix_nano`, `name`, `duration_ns`, `status`,
/// and optionally `attr_service_name`.
pub fn compute_span_rollups(batch: &RecordBatch) -> Vec<SpanRollup> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Vec::new();
    }

    // Extract columns
    let start_times = batch
        .column_by_name("start_time_unix_nano")
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>());

    let names = batch
        .column_by_name("name")
        .and_then(|c| c.as_any().downcast_ref::<StringViewArray>());

    let durations = batch
        .column_by_name("duration_ns")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

    let statuses = batch
        .column_by_name("status")
        .and_then(|c| c.as_any().downcast_ref::<UInt8Array>());

    let service_names = batch
        .column_by_name("attr_service_name")
        .and_then(|c| c.as_any().downcast_ref::<StringViewArray>());

    // SpanStatus::Ok = 1
    const STATUS_OK: u8 = 1;

    // Group key: (service, span_name, minute_bucket)
    let mut groups: HashMap<(String, String, i64), SpanRollupBuilder> = HashMap::new();

    for row in 0..num_rows {
        let start_ns = start_times.map(|a| a.value(row)).unwrap_or(0);
        let minute_bucket = start_ns / 1_000_000_000 / 60;

        let span_name = names.map(|a| a.value(row).to_string()).unwrap_or_default();

        let service_name = service_names
            .and_then(|a| {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row).to_string())
                }
            })
            .unwrap_or_else(|| "unknown".to_string());

        let duration_ns = durations.map(|a| a.value(row)).unwrap_or(0);
        let status = statuses.map(|a| a.value(row)).unwrap_or(STATUS_OK);

        let key = (service_name, span_name, minute_bucket);
        groups
            .entry(key)
            .or_insert_with(SpanRollupBuilder::new)
            .add_row(duration_ns, status);
    }

    // Convert builders to rollups
    groups
        .into_iter()
        .map(|((service_name, span_name, minute_bucket), builder)| {
            builder.build(service_name, span_name, minute_bucket)
        })
        .collect()
}

/// Builder for accumulating span statistics
struct SpanRollupBuilder {
    count: u64,
    error_count: u64,
    durations: Vec<i64>,
}

impl SpanRollupBuilder {
    fn new() -> Self {
        Self {
            count: 0,
            error_count: 0,
            durations: Vec::new(),
        }
    }

    fn add_row(&mut self, duration_ns: i64, status: u8) {
        self.count += 1;
        // SpanStatus::Ok = 1; anything else is an error
        if status != 1 {
            self.error_count += 1;
        }
        self.durations.push(duration_ns);
    }

    fn build(self, service_name: String, span_name: String, minute_bucket: i64) -> SpanRollup {
        SpanRollup {
            service_name,
            span_name,
            minute_bucket,
            count: self.count,
            error_count: self.error_count,
            duration_stats: DurationStats::from_durations(self.durations),
        }
    }
}

/// Write rollups to object storage as a Vortex file
///
/// # Arguments
/// * `rollups` - Pre-computed rollup data
/// * `store` - Object store instance
/// * `base_path` - Base path (e.g., "s3://bucket/data")
/// * `timestamp` - Timestamp for partitioning (used to create the file path)
pub async fn write_span_rollups(
    rollups: Vec<SpanRollup>,
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    timestamp: i64,
    row_block_size: usize,
    compact_encodings: bool,
) -> Result<String> {
    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::stream;
    use vortex::array::arrow::FromArrowArray;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::array::ArrayRef as VortexArrayRef;
    use vortex::dtype::arrow::FromArrowType;
    use vortex::dtype::DType;
    use vortex::error::VortexResult;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::ObjectStoreWriter;
    use vortex::session::VortexSession;
    use vortex::VortexSessionDefault;

    if rollups.is_empty() {
        return Ok(String::new());
    }

    // Convert rollups to Arrow RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("service_name", DataType::Utf8, false),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("minute_bucket", DataType::Int64, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("error_count", DataType::UInt64, false),
        Field::new("min_duration", DataType::Int64, false),
        Field::new("max_duration", DataType::Int64, false),
        Field::new("sum_duration", DataType::Int64, false),
        Field::new("p50_duration", DataType::Int64, false),
        Field::new("p90_duration", DataType::Int64, false),
        Field::new("p95_duration", DataType::Int64, false),
        Field::new("p99_duration", DataType::Int64, false),
    ]));

    use arrow::array::Array;
    use std::sync::Arc as StdArc;

    let service_names: StdArc<dyn Array> = StdArc::new(StringArray::from(
        rollups
            .iter()
            .map(|r| r.service_name.as_str())
            .collect::<Vec<_>>(),
    ));
    let span_names: StdArc<dyn Array> = StdArc::new(StringArray::from(
        rollups
            .iter()
            .map(|r| r.span_name.as_str())
            .collect::<Vec<_>>(),
    ));
    let minute_buckets: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups.iter().map(|r| r.minute_bucket).collect::<Vec<_>>(),
    ));
    let counts: StdArc<dyn Array> = StdArc::new(UInt64Array::from(
        rollups.iter().map(|r| r.count).collect::<Vec<_>>(),
    ));
    let error_counts: StdArc<dyn Array> = StdArc::new(UInt64Array::from(
        rollups.iter().map(|r| r.error_count).collect::<Vec<_>>(),
    ));
    let min_durations: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.duration_stats.min)
            .collect::<Vec<_>>(),
    ));
    let max_durations: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.duration_stats.max)
            .collect::<Vec<_>>(),
    ));
    let sum_durations: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.duration_stats.sum)
            .collect::<Vec<_>>(),
    ));
    let p50_durations: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.duration_stats.percentiles.p50)
            .collect::<Vec<_>>(),
    ));
    let p90_durations: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.duration_stats.percentiles.p90)
            .collect::<Vec<_>>(),
    ));
    let p95_durations: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.duration_stats.percentiles.p95)
            .collect::<Vec<_>>(),
    ));
    let p99_durations: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.duration_stats.percentiles.p99)
            .collect::<Vec<_>>(),
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            service_names,
            span_names,
            minute_buckets,
            counts,
            error_counts,
            min_durations,
            max_durations,
            sum_durations,
            p50_durations,
            p90_durations,
            p95_durations,
            p99_durations,
        ],
    )
    .map_err(|e| Error::Storage(format!("Failed to create rollup batch: {}", e)))?;

    // Write as Vortex file
    let session = VortexSession::default();
    let arrow_schema = batch.schema();
    let dtype = DType::from_arrow(arrow_schema);
    let vortex_array = VortexArrayRef::from_arrow(batch, false);
    let stream = stream::once(async move { VortexResult::Ok(vortex_array) });
    let array_stream = ArrayStreamAdapter::new(dtype, stream);

    // Generate path: rollups/spans/YYYY-MM-DD/HH/timestamp.vortex
    let dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .ok_or_else(|| Error::Storage("Invalid timestamp".to_string()))?;
    let path = format!(
        "{}/rollups/spans/{}/{:02}/{}.vortex",
        base_path.trim_end_matches('/'),
        dt.format("%Y-%m-%d"),
        dt.hour(),
        timestamp
    );

    let object_path = ObjectPath::from(path.as_str());
    let mut writer = ObjectStoreWriter::new(store, &object_path)
        .await
        .map_err(|e| Error::Storage(format!("Failed to create rollup writer: {}", e)))?;

    let strategy = {
        use vortex::compressor::CompactCompressor;
        use vortex::file::WriteStrategyBuilder;
        let mut builder = WriteStrategyBuilder::new().with_row_block_size(row_block_size);
        if compact_encodings {
            builder = builder.with_compressor(CompactCompressor::default());
        }
        builder.build()
    };
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut writer, array_stream)
        .await
        .map_err(|e| Error::Storage(format!("Failed to write rollups: {}", e)))?;

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array, UInt8Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Build a RecordBatch with span fields for testing rollups.
    ///
    /// rows: (start_time_unix_nano, name, service_name, duration_ns, status)
    fn make_span_batch(rows: &[(i64, &str, &str, i64, u8)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8View, false),
            Field::new("span_id", DataType::Utf8View, false),
            Field::new("parent_span_id", DataType::Utf8View, true),
            Field::new("name", DataType::Utf8View, false),
            Field::new("kind", DataType::UInt8, false),
            Field::new("status", DataType::UInt8, false),
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "end_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("duration_ns", DataType::Int64, false),
            Field::new("resource_id", DataType::UInt32, false),
            Field::new("scope_id", DataType::UInt32, false),
            Field::new("attr_service_name", DataType::Utf8View, true),
        ]));
        let n = rows.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec!["00"; n])) as _,
                Arc::new(StringViewArray::from(vec!["00"; n])) as _,
                Arc::new(StringViewArray::from(vec![None::<&str>; n])) as _,
                Arc::new(StringViewArray::from(
                    rows.iter().map(|(_, n, _, _, _)| *n).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt8Array::from(vec![0u8; n])) as _,
                Arc::new(UInt8Array::from(
                    rows.iter().map(|(_, _, _, _, s)| *s).collect::<Vec<_>>(),
                )) as _,
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|(t, _, _, _, _)| *t).collect::<Vec<_>>(),
                )) as _,
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter().map(|(t, _, _, d, _)| t + d).collect::<Vec<_>>(),
                )) as _,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, _, d, _)| *d).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt32Array::from(vec![0u32; n])) as _,
                Arc::new(UInt32Array::from(vec![0u32; n])) as _,
                Arc::new(StringViewArray::from(
                    rows.iter()
                        .map(|(_, _, s, _, _)| Some(*s))
                        .collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    // SpanStatus::Ok = 1, SpanStatus::Error = 2
    const STATUS_OK: u8 = 1;
    const STATUS_ERROR: u8 = 2;

    #[test]
    fn test_compute_span_rollups_basic() {
        let batch = make_span_batch(&[
            (60_000_000_000, "GET /users", "api", 100_000_000, STATUS_OK),
            (60_500_000_000, "GET /users", "api", 150_000_000, STATUS_OK),
            (
                60_700_000_000,
                "POST /users",
                "api",
                200_000_000,
                STATUS_ERROR,
            ),
        ]);

        let rollups = compute_span_rollups(&batch);

        // Should have 2 rollups: one for GET /users, one for POST /users
        assert_eq!(rollups.len(), 2);

        let get_rollup = rollups
            .iter()
            .find(|r| r.span_name == "GET /users")
            .unwrap();
        assert_eq!(get_rollup.service_name, "api");
        assert_eq!(get_rollup.count, 2);
        assert_eq!(get_rollup.error_count, 0);
        assert_eq!(get_rollup.minute_bucket, 1); // 60 seconds / 60 = 1

        let post_rollup = rollups
            .iter()
            .find(|r| r.span_name == "POST /users")
            .unwrap();
        assert_eq!(post_rollup.count, 1);
        assert_eq!(post_rollup.error_count, 1);
    }

    #[test]
    fn test_duration_stats() {
        let durations = vec![100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
        let stats = DurationStats::from_durations(durations);

        assert_eq!(stats.min, 100);
        assert_eq!(stats.max, 1000);
        assert_eq!(stats.sum, 5500);
        assert_eq!(stats.percentiles.p50, 500);
        assert_eq!(stats.percentiles.p90, 900);
    }

    #[test]
    fn test_time_bucketing() {
        let batch = make_span_batch(&[
            (60_000_000_000, "test", "api", 100_000_000, STATUS_OK), // 1 minute
            (119_000_000_000, "test", "api", 100_000_000, STATUS_OK), // 1 minute (just before 2)
            (120_000_000_000, "test", "api", 100_000_000, STATUS_OK), // 2 minutes
        ]);

        let rollups = compute_span_rollups(&batch);

        // Should have 2 rollups: one for minute 1, one for minute 2
        assert_eq!(rollups.len(), 2);

        let minute_1 = rollups.iter().find(|r| r.minute_bucket == 1).unwrap();
        assert_eq!(minute_1.count, 2);

        let minute_2 = rollups.iter().find(|r| r.minute_bucket == 2).unwrap();
        assert_eq!(minute_2.count, 1);
    }
}

// ============================================================================
// Metric Rollups
// ============================================================================

/// Metric rollup for different time granularities (1m, 1h, 1d)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricRollup {
    /// Series ID from the series index
    pub series_id: u64,

    /// Bucket timestamp (Unix epoch seconds at bucket start)
    pub bucket_timestamp: i64,

    /// Minimum value in this bucket
    pub min: f64,

    /// Maximum value in this bucket
    pub max: f64,

    /// Sum of all values (for computing average)
    pub sum: f64,

    /// Count of data points in this bucket
    pub count: u64,
}

/// Rollup tier/granularity
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupTier {
    /// 1-minute rollups
    OneMinute,
    /// 1-hour rollups
    OneHour,
    /// 1-day rollups
    OneDay,
}

impl RollupTier {
    /// Get the directory name for this tier
    pub fn directory(&self) -> &str {
        match self {
            RollupTier::OneMinute => "rollups/metrics/1m",
            RollupTier::OneHour => "rollups/metrics/1h",
            RollupTier::OneDay => "rollups/metrics/1d",
        }
    }

    /// Get the bucket duration in seconds
    pub fn bucket_seconds(&self) -> i64 {
        match self {
            RollupTier::OneMinute => 60,
            RollupTier::OneHour => 3600,
            RollupTier::OneDay => 86400,
        }
    }

    /// Compute bucket timestamp from data point timestamp
    pub fn bucket_timestamp(&self, timestamp_ns: i64) -> i64 {
        let timestamp_s = timestamp_ns / 1_000_000_000;
        (timestamp_s / self.bucket_seconds()) * self.bucket_seconds()
    }
}

/// Compute metric rollups from raw data points for all three tiers
///
/// Returns (1m rollups, 1h rollups, 1d rollups)
pub fn compute_metric_rollups(
    data_points: &[(SeriesId, i64, f64)],
) -> (Vec<MetricRollup>, Vec<MetricRollup>, Vec<MetricRollup>) {
    let rollups_1m = compute_metric_rollups_tier(data_points, RollupTier::OneMinute);
    let rollups_1h = compute_metric_rollups_tier(data_points, RollupTier::OneHour);
    let rollups_1d = compute_metric_rollups_tier(data_points, RollupTier::OneDay);

    (rollups_1m, rollups_1h, rollups_1d)
}

/// Compute metric rollups for a specific tier
fn compute_metric_rollups_tier(
    data_points: &[(SeriesId, i64, f64)],
    tier: RollupTier,
) -> Vec<MetricRollup> {
    // Group key: (series_id, bucket_timestamp)
    let mut groups: HashMap<(u64, i64), MetricRollupBuilder> = HashMap::new();

    for &(series_id, timestamp_ns, value) in data_points {
        let bucket_timestamp = tier.bucket_timestamp(timestamp_ns);
        let key = (series_id.as_u64(), bucket_timestamp);

        groups
            .entry(key)
            .or_insert_with(MetricRollupBuilder::new)
            .add_value(value);
    }

    // Convert builders to rollups
    groups
        .into_iter()
        .map(|((series_id, bucket_timestamp), builder)| builder.build(series_id, bucket_timestamp))
        .collect()
}

/// Builder for accumulating metric statistics
struct MetricRollupBuilder {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl MetricRollupBuilder {
    fn new() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            count: 0,
        }
    }

    fn add_value(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1;
    }

    fn build(self, series_id: u64, bucket_timestamp: i64) -> MetricRollup {
        MetricRollup {
            series_id,
            bucket_timestamp,
            min: if self.min.is_finite() { self.min } else { 0.0 },
            max: if self.max.is_finite() { self.max } else { 0.0 },
            sum: self.sum,
            count: self.count,
        }
    }
}

/// Write metric rollups to object storage as Vortex files
pub async fn write_metric_rollups(
    rollups: Vec<MetricRollup>,
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    tier: RollupTier,
    timestamp: i64,
    row_block_size: usize,
    compact_encodings: bool,
) -> Result<String> {
    use arrow::array::{Array, Float64Array, Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::stream;
    use vortex::array::arrow::FromArrowArray;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::array::ArrayRef as VortexArrayRef;
    use vortex::dtype::arrow::FromArrowType;
    use vortex::dtype::DType;
    use vortex::error::VortexResult;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::ObjectStoreWriter;
    use vortex::session::VortexSession;
    use vortex::VortexSessionDefault;

    if rollups.is_empty() {
        return Ok(String::new());
    }

    // Build Arrow schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new("bucket_timestamp", DataType::Int64, false),
        Field::new("min", DataType::Float64, false),
        Field::new("max", DataType::Float64, false),
        Field::new("sum", DataType::Float64, false),
        Field::new("count", DataType::UInt64, false),
    ]));

    // Extract columns
    let series_ids: Vec<u64> = rollups.iter().map(|r| r.series_id).collect();
    let bucket_timestamps: Vec<i64> = rollups.iter().map(|r| r.bucket_timestamp).collect();
    let mins: Vec<f64> = rollups.iter().map(|r| r.min).collect();
    let maxs: Vec<f64> = rollups.iter().map(|r| r.max).collect();
    let sums: Vec<f64> = rollups.iter().map(|r| r.sum).collect();
    let counts: Vec<u64> = rollups.iter().map(|r| r.count).collect();

    use std::sync::Arc as StdArc;
    let series_ids_arr: StdArc<dyn Array> = StdArc::new(UInt64Array::from(series_ids));
    let bucket_timestamps_arr: StdArc<dyn Array> = StdArc::new(Int64Array::from(bucket_timestamps));
    let mins_arr: StdArc<dyn Array> = StdArc::new(Float64Array::from(mins));
    let maxs_arr: StdArc<dyn Array> = StdArc::new(Float64Array::from(maxs));
    let sums_arr: StdArc<dyn Array> = StdArc::new(Float64Array::from(sums));
    let counts_arr: StdArc<dyn Array> = StdArc::new(UInt64Array::from(counts));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            series_ids_arr,
            bucket_timestamps_arr,
            mins_arr,
            maxs_arr,
            sums_arr,
            counts_arr,
        ],
    )
    .map_err(|e| Error::Storage(format!("Failed to create rollup batch: {}", e)))?;

    // Write as Vortex file
    let session = VortexSession::default();
    let arrow_schema = batch.schema();
    let dtype = DType::from_arrow(arrow_schema);
    let vortex_array = VortexArrayRef::from_arrow(batch, false);
    let stream = stream::once(async move { VortexResult::Ok(vortex_array) });
    let array_stream = ArrayStreamAdapter::new(dtype, stream);

    // Generate path: rollups/metrics/{tier}/YYYY-MM-DD/HH/timestamp.vortex
    let dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .ok_or_else(|| Error::Storage("Invalid timestamp".to_string()))?;
    let path = format!(
        "{}/{}/{}/{:02}/{}.vortex",
        base_path.trim_end_matches('/'),
        tier.directory(),
        dt.format("%Y-%m-%d"),
        dt.hour(),
        timestamp
    );

    let object_path = ObjectPath::from(path.as_str());
    let mut writer = ObjectStoreWriter::new(store, &object_path)
        .await
        .map_err(|e| Error::Storage(format!("Failed to create rollup writer: {}", e)))?;

    let strategy = {
        use vortex::compressor::CompactCompressor;
        use vortex::file::WriteStrategyBuilder;
        let mut builder = WriteStrategyBuilder::new().with_row_block_size(row_block_size);
        if compact_encodings {
            builder = builder.with_compressor(CompactCompressor::default());
        }
        builder.build()
    };
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut writer, array_stream)
        .await
        .map_err(|e| Error::Storage(format!("Failed to write rollups: {}", e)))?;

    Ok(path)
}

#[cfg(test)]
mod metric_rollup_tests {
    use super::*;

    #[test]
    fn test_rollup_tier_bucket_timestamp() {
        // 1-minute tier
        let tier = RollupTier::OneMinute;
        let timestamp_ns = 123_456_000_000_000; // 123456 seconds
        let bucket = tier.bucket_timestamp(timestamp_ns);
        assert_eq!(bucket, 123_420); // Rounded down to nearest minute (123420 = 123456 / 60 * 60)

        // 1-hour tier
        let tier = RollupTier::OneHour;
        let bucket = tier.bucket_timestamp(timestamp_ns);
        assert_eq!(bucket, 122_400); // Rounded down to nearest hour

        // 1-day tier
        let tier = RollupTier::OneDay;
        let bucket = tier.bucket_timestamp(timestamp_ns);
        assert_eq!(bucket, 86_400); // Rounded down to nearest day
    }

    #[test]
    fn test_compute_metric_rollups() {
        let data_points = vec![
            (SeriesId(1), 60_000_000_000, 10.0),  // 60s -> bucket 60
            (SeriesId(1), 90_000_000_000, 20.0),  // 90s -> bucket 60
            (SeriesId(1), 120_000_000_000, 30.0), // 120s -> bucket 120
            (SeriesId(2), 60_000_000_000, 5.0),   // Different series
        ];

        let (rollups_1m, rollups_1h, rollups_1d) = compute_metric_rollups(&data_points);

        // 1-minute rollups
        assert_eq!(rollups_1m.len(), 3); // 2 buckets for series 1, 1 for series 2

        // Find series 1, bucket 60
        let rollup = rollups_1m
            .iter()
            .find(|r| r.series_id == 1 && r.bucket_timestamp == 60)
            .unwrap();
        assert_eq!(rollup.count, 2);
        assert_eq!(rollup.min, 10.0);
        assert_eq!(rollup.max, 20.0);
        assert_eq!(rollup.sum, 30.0);

        // 1-hour rollups (all data points in same hour)
        assert_eq!(rollups_1h.len(), 2); // 1 for each series

        // 1-day rollups (all data points in same day)
        assert_eq!(rollups_1d.len(), 2); // 1 for each series
    }

    #[test]
    fn test_metric_rollup_builder() {
        let mut builder = MetricRollupBuilder::new();
        builder.add_value(10.0);
        builder.add_value(20.0);
        builder.add_value(5.0);

        let rollup = builder.build(1, 60);

        assert_eq!(rollup.series_id, 1);
        assert_eq!(rollup.bucket_timestamp, 60);
        assert_eq!(rollup.min, 5.0);
        assert_eq!(rollup.max, 20.0);
        assert_eq!(rollup.sum, 35.0);
        assert_eq!(rollup.count, 3);
    }

    #[tokio::test]
    async fn test_write_metric_rollups() {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let base_path = "/test";

        let rollups = vec![
            MetricRollup {
                series_id: 1,
                bucket_timestamp: 60,
                min: 10.0,
                max: 20.0,
                sum: 30.0,
                count: 2,
            },
            MetricRollup {
                series_id: 2,
                bucket_timestamp: 60,
                min: 5.0,
                max: 15.0,
                sum: 20.0,
                count: 2,
            },
        ];

        let path = write_metric_rollups(
            rollups,
            store.clone(),
            base_path,
            RollupTier::OneMinute,
            60,
            8192,
            false,
        )
        .await
        .unwrap();

        assert!(!path.is_empty());
        assert!(path.contains("rollups/metrics/1m"));
    }
}

// ============================================================================
// Profile Rollups (Flamegraph Blocks)
// ============================================================================

/// Profile rollup for pre-merged flamegraph blocks
///
/// Groups samples by (service, stack_hash, 1-minute bucket, value_type) and
/// aggregates values. This enables sub-second query times for flamegraph views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileRollup {
    /// Service name
    pub service: String,

    /// Stack hash from StackDictionary
    pub stack_hash: u64,

    /// 1-minute bucket timestamp (Unix epoch seconds)
    pub minute_bucket: i64,

    /// Profile value type (e.g., "cpu", "alloc_space", "alloc_objects")
    pub value_type: String,

    /// Aggregated value (sum of all samples in this bucket)
    pub aggregated_value: i64,
}

/// Compute profile rollups from raw samples
///
/// Groups by (service, stack_hash, 1-minute bucket, value_type) and sums values.
/// Used to create pre-merged flamegraph blocks for fast queries.
pub fn compute_profile_rollups(samples: &[(String, u64, i64, String, i64)]) -> Vec<ProfileRollup> {
    // Group key: (service, stack_hash, minute_bucket, value_type)
    let mut groups: HashMap<(String, u64, i64, String), i64> = HashMap::new();

    for (service, stack_hash, timestamp_ns, value_type, value) in samples {
        let minute_bucket = timestamp_ns / 1_000_000_000 / 60;
        let key = (
            service.clone(),
            *stack_hash,
            minute_bucket,
            value_type.clone(),
        );

        *groups.entry(key).or_insert(0) += value;
    }

    // Convert to rollups
    groups
        .into_iter()
        .map(
            |((service, stack_hash, minute_bucket, value_type), aggregated_value)| ProfileRollup {
                service,
                stack_hash,
                minute_bucket,
                value_type,
                aggregated_value,
            },
        )
        .collect()
}

/// Write profile rollups to object storage as Vortex files
///
/// Flamegraph query for "last 6 hours" reads 360 pre-merged blocks (~500KB)
/// instead of millions of raw samples.
pub async fn write_profile_rollups(
    rollups: Vec<ProfileRollup>,
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    timestamp: i64,
    row_block_size: usize,
    compact_encodings: bool,
) -> Result<String> {
    use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::stream;
    use vortex::array::arrow::FromArrowArray;
    use vortex::array::stream::ArrayStreamAdapter;
    use vortex::array::ArrayRef as VortexArrayRef;
    use vortex::dtype::arrow::FromArrowType;
    use vortex::dtype::DType;
    use vortex::error::VortexResult;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::ObjectStoreWriter;
    use vortex::session::VortexSession;
    use vortex::VortexSessionDefault;

    if rollups.is_empty() {
        return Ok(String::new());
    }

    // Build Arrow schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("service", DataType::Utf8, false),
        Field::new("stack_hash", DataType::UInt64, false),
        Field::new("minute_bucket", DataType::Int64, false),
        Field::new("value_type", DataType::Utf8, false),
        Field::new("aggregated_value", DataType::Int64, false),
    ]));

    // Extract columns
    use std::sync::Arc as StdArc;
    let services: StdArc<dyn Array> = StdArc::new(StringArray::from(
        rollups
            .iter()
            .map(|r| r.service.as_str())
            .collect::<Vec<_>>(),
    ));
    let stack_hashes: StdArc<dyn Array> = StdArc::new(UInt64Array::from(
        rollups.iter().map(|r| r.stack_hash).collect::<Vec<_>>(),
    ));
    let minute_buckets: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups.iter().map(|r| r.minute_bucket).collect::<Vec<_>>(),
    ));
    let value_types: StdArc<dyn Array> = StdArc::new(StringArray::from(
        rollups
            .iter()
            .map(|r| r.value_type.as_str())
            .collect::<Vec<_>>(),
    ));
    let aggregated_values: StdArc<dyn Array> = StdArc::new(Int64Array::from(
        rollups
            .iter()
            .map(|r| r.aggregated_value)
            .collect::<Vec<_>>(),
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            services,
            stack_hashes,
            minute_buckets,
            value_types,
            aggregated_values,
        ],
    )
    .map_err(|e| Error::Storage(format!("Failed to create profile rollup batch: {}", e)))?;

    // Write as Vortex file
    let session = VortexSession::default();
    let arrow_schema = batch.schema();
    let dtype = DType::from_arrow(arrow_schema);
    let vortex_array = VortexArrayRef::from_arrow(batch, false);
    let stream = stream::once(async move { VortexResult::Ok(vortex_array) });
    let array_stream = ArrayStreamAdapter::new(dtype, stream);

    // Generate path: rollups/profiles/YYYY-MM-DD/HH/timestamp.vortex
    let dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .ok_or_else(|| Error::Storage("Invalid timestamp".to_string()))?;
    let path = format!(
        "{}/rollups/profiles/{}/{:02}/{}.vortex",
        base_path.trim_end_matches('/'),
        dt.format("%Y-%m-%d"),
        dt.hour(),
        timestamp
    );

    let object_path = ObjectPath::from(path.as_str());
    let mut writer = ObjectStoreWriter::new(store, &object_path)
        .await
        .map_err(|e| Error::Storage(format!("Failed to create profile rollup writer: {}", e)))?;

    let strategy = {
        use vortex::compressor::CompactCompressor;
        use vortex::file::WriteStrategyBuilder;
        let mut builder = WriteStrategyBuilder::new().with_row_block_size(row_block_size);
        if compact_encodings {
            builder = builder.with_compressor(CompactCompressor::default());
        }
        builder.build()
    };
    session
        .write_options()
        .with_strategy(strategy)
        .write(&mut writer, array_stream)
        .await
        .map_err(|e| Error::Storage(format!("Failed to write profile rollups: {}", e)))?;

    Ok(path)
}

// ============================================================================
// Metric Exemplars
// ============================================================================

/// Exemplar: links a metric data point to a trace ID that caused it.
///
/// Exemplars enable "click on a spike in a metric chart → jump to the causing trace."
/// They record the specific data point (series_id + timestamp) alongside the trace_id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricExemplar {
    /// Series ID from the series index
    pub series_id: u64,

    /// Timestamp of the data point in nanoseconds since Unix epoch
    pub timestamp_ns: i64,

    /// Value of the data point
    pub value: f64,

    /// Trace ID (hex string) that was active when the data point was recorded
    pub trace_id: String,
}

/// Extract exemplars from a batch of raw metric data points.
///
/// Since `MetricDataPoint` does not carry a `trace_id`, this function uses a
/// value-threshold heuristic: data points whose value is in the **top 10%** of
/// the batch are promoted to exemplars with a synthetic trace_id placeholder.
///
/// The synthetic trace_id is derived deterministically from the series_id and
/// timestamp so callers can recognise that it is a stand-in value.
///
/// Arguments:
/// * `data_points` – tuples of `(series_id, timestamp_ns, value)` produced by
///   `write_metric_data_points`.
pub fn extract_exemplars(data_points: &[(u64, i64, f64)]) -> Vec<MetricExemplar> {
    if data_points.is_empty() {
        return Vec::new();
    }

    // Determine the 90th-percentile threshold
    let mut sorted_values: Vec<f64> = data_points.iter().map(|(_, _, v)| *v).collect();
    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let threshold_idx = (sorted_values.len() * 9) / 10; // index of 90th percentile
    let threshold = sorted_values[threshold_idx];

    // Collect data points that are at or above the threshold
    data_points
        .iter()
        .filter(|(_, _, value)| *value >= threshold)
        .map(|(series_id, timestamp_ns, value)| {
            // Synthetic trace_id: deterministic hex derived from series + timestamp
            let trace_id = format!("{:016x}{:016x}", series_id, *timestamp_ns as u64);
            MetricExemplar {
                series_id: *series_id,
                timestamp_ns: *timestamp_ns,
                value: *value,
                trace_id,
            }
        })
        .collect()
}

/// Write exemplars to object storage as a JSON file.
///
/// # Arguments
/// * `exemplars` - Exemplar data to persist
/// * `store` - Object store instance
/// * `base_path` - Base path (e.g., "/data")
/// * `timestamp` - Unix seconds timestamp, used to create the partition path
///
/// # Returns
/// The object-store path that was written, or an empty string if `exemplars` is empty.
pub async fn write_exemplars(
    exemplars: Vec<MetricExemplar>,
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    timestamp: i64,
) -> Result<String> {
    use object_store::PutPayload;

    if exemplars.is_empty() {
        return Ok(String::new());
    }

    let dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .ok_or_else(|| Error::Storage("Invalid timestamp".to_string()))?;

    let path = format!(
        "{}/metrics/exemplars/{}/{:02}/{}.json",
        base_path.trim_end_matches('/'),
        dt.format("%Y-%m-%d"),
        dt.hour(),
        timestamp
    );

    let json_bytes = serde_json::to_vec(&exemplars)
        .map_err(|e| Error::Storage(format!("Failed to serialize exemplars: {}", e)))?;

    let object_path = ObjectPath::from(path.as_str());
    store
        .put(&object_path, PutPayload::from(json_bytes))
        .await
        .map_err(|e| Error::Storage(format!("Failed to write exemplars: {}", e)))?;

    Ok(path)
}

#[cfg(test)]
mod exemplar_tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn test_extract_exemplars_empty() {
        let result = extract_exemplars(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_exemplars_top_10_percent() {
        // 10 data points; values 1..=10.  Threshold idx = (10*9)/10 = 9 → value 10.
        let data_points: Vec<(u64, i64, f64)> = (1u64..=10)
            .map(|i| (i, i as i64 * 1_000_000_000, i as f64))
            .collect();

        let exemplars = extract_exemplars(&data_points);

        // Only value=10.0 is at or above threshold index 9
        assert_eq!(exemplars.len(), 1);
        assert_eq!(exemplars[0].value, 10.0);
        assert_eq!(exemplars[0].series_id, 10);
    }

    #[test]
    fn test_extract_exemplars_single_point() {
        // A batch with a single data point should always produce one exemplar
        let data_points = vec![(42u64, 1_000_000_000i64, 99.0f64)];
        let exemplars = extract_exemplars(&data_points);
        assert_eq!(exemplars.len(), 1);
        assert_eq!(exemplars[0].series_id, 42);
        assert_eq!(exemplars[0].value, 99.0);
        // trace_id is deterministic
        let expected_trace_id = format!("{:016x}{:016x}", 42u64, 1_000_000_000u64);
        assert_eq!(exemplars[0].trace_id, expected_trace_id);
    }

    #[test]
    fn test_extract_exemplars_all_equal_values() {
        // When all values are equal, all are at the 90th percentile → all become exemplars
        let data_points: Vec<(u64, i64, f64)> =
            (1u64..=5).map(|i| (i, i as i64 * 1_000, 7.0)).collect();
        let exemplars = extract_exemplars(&data_points);
        assert_eq!(exemplars.len(), 5);
    }

    #[tokio::test]
    async fn test_write_exemplars_empty() {
        let store = Arc::new(InMemory::new());
        let path = write_exemplars(vec![], store, "/test", 60).await.unwrap();
        assert!(path.is_empty());
    }

    #[tokio::test]
    async fn test_write_exemplars_creates_file() {
        let store = Arc::new(InMemory::new());
        let base_path = "/test";

        let exemplars = vec![
            MetricExemplar {
                series_id: 1,
                timestamp_ns: 60_000_000_000,
                value: 42.0,
                trace_id: "0000000000000001000000e8d4a51000".to_string(),
            },
            MetricExemplar {
                series_id: 2,
                timestamp_ns: 90_000_000_000,
                value: 55.0,
                trace_id: "0000000000000002000000152d02c000".to_string(),
            },
        ];

        let path = write_exemplars(exemplars.clone(), store.clone(), base_path, 60)
            .await
            .unwrap();

        // Path must be non-empty and contain the expected directory
        assert!(!path.is_empty());
        assert!(path.contains("metrics/exemplars"), "path = {}", path);

        // Verify file was written and can be read back
        let object_path = object_store::path::Path::from(path.as_str());
        let bytes = store
            .get(&object_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let loaded: Vec<MetricExemplar> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].series_id, 1);
        assert_eq!(loaded[1].value, 55.0);
    }

    #[tokio::test]
    async fn test_write_exemplars_path_format() {
        // timestamp = 3661 seconds → 1970-01-01 01:01:01 UTC
        let store = Arc::new(InMemory::new());
        let exemplars = vec![MetricExemplar {
            series_id: 7,
            timestamp_ns: 3_661_000_000_000,
            value: 1.0,
            trace_id: "abc".to_string(),
        }];

        let path = write_exemplars(exemplars, store, "/data", 3661)
            .await
            .unwrap();

        // The path should be /data/metrics/exemplars/1970-01-01/01/3661.json
        assert!(path.contains("/metrics/exemplars/"), "path = {}", path);
        assert!(path.ends_with("3661.json"), "path = {}", path);
    }
}

#[cfg(test)]
mod profile_rollup_tests {
    use super::*;

    #[test]
    fn test_compute_profile_rollups() {
        let samples = vec![
            // Same service, stack, minute, value_type
            (
                "api".to_string(),
                123,
                60_000_000_000,
                "cpu".to_string(),
                100,
            ),
            (
                "api".to_string(),
                123,
                90_000_000_000,
                "cpu".to_string(),
                200,
            ),
            // Different minute
            (
                "api".to_string(),
                123,
                120_000_000_000,
                "cpu".to_string(),
                150,
            ),
            // Different stack
            (
                "api".to_string(),
                456,
                60_000_000_000,
                "cpu".to_string(),
                50,
            ),
            // Different value_type
            (
                "api".to_string(),
                123,
                60_000_000_000,
                "alloc_space".to_string(),
                1000,
            ),
        ];

        let rollups = compute_profile_rollups(&samples);

        // Should have 4 distinct groups
        assert_eq!(rollups.len(), 4);

        // Find the rollup for (api, 123, minute 1, cpu)
        let rollup = rollups
            .iter()
            .find(|r| {
                r.service == "api"
                    && r.stack_hash == 123
                    && r.minute_bucket == 1
                    && r.value_type == "cpu"
            })
            .unwrap();
        assert_eq!(rollup.aggregated_value, 300); // 100 + 200
    }

    #[test]
    fn test_profile_rollup_time_bucketing() {
        let samples = vec![
            ("api".to_string(), 1, 0, "cpu".to_string(), 10),
            ("api".to_string(), 1, 59_999_999_999, "cpu".to_string(), 20), // Same minute
            ("api".to_string(), 1, 60_000_000_000, "cpu".to_string(), 30), // Next minute
        ];

        let rollups = compute_profile_rollups(&samples);

        // Should have 2 buckets: minute 0 and minute 1
        assert_eq!(rollups.len(), 2);

        let minute_0 = rollups.iter().find(|r| r.minute_bucket == 0).unwrap();
        assert_eq!(minute_0.aggregated_value, 30); // 10 + 20

        let minute_1 = rollups.iter().find(|r| r.minute_bucket == 1).unwrap();
        assert_eq!(minute_1.aggregated_value, 30);
    }

    #[tokio::test]
    async fn test_write_profile_rollups() {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let base_path = "/test";

        let rollups = vec![
            ProfileRollup {
                service: "api".to_string(),
                stack_hash: 123,
                minute_bucket: 1,
                value_type: "cpu".to_string(),
                aggregated_value: 100,
            },
            ProfileRollup {
                service: "web".to_string(),
                stack_hash: 456,
                minute_bucket: 1,
                value_type: "alloc_space".to_string(),
                aggregated_value: 2000,
            },
        ];

        let path = write_profile_rollups(rollups, store.clone(), base_path, 60, 8192, false)
            .await
            .unwrap();

        assert!(!path.is_empty());
        assert!(path.contains("rollups/profiles"));
    }
}
