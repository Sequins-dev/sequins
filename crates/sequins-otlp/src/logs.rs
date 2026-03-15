//! Direct OTLP log → Arrow RecordBatch conversion

use crate::overflow_map::build_overflow_column;
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray,
    UInt32Array, UInt8Array,
};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use sequins_types::arrow_schema::log_schema_with_catalog;
use sequins_types::models::{LogId, LogSeverity, SpanId, Timestamp, TraceId};
use sequins_types::schema_catalog::AttributeValueType;
use std::sync::Arc;

/// Convert a batch of OTLP log records directly to an Arrow `RecordBatch`.
///
/// `items` contains `(LogRecord, resource_id, scope_id, service_name)` tuples.
/// `service_name` is extracted from the resource before calling this function.
///
/// The output schema is `log_schema_with_catalog(catalog)`.
pub fn otlp_logs_to_batch(
    items: Vec<(LogRecord, u32, u32, String)>,
    catalog: &sequins_types::SchemaCatalog,
) -> Result<RecordBatch, String> {
    let schema = log_schema_with_catalog(catalog);
    let n = items.len();
    if n == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Core column buffers
    let mut log_ids: Vec<String> = Vec::with_capacity(n);
    let mut timestamps: Vec<i64> = Vec::with_capacity(n);
    let mut observed_timestamps: Vec<i64> = Vec::with_capacity(n);
    let mut service_names: Vec<String> = Vec::with_capacity(n);
    let mut severity_texts: Vec<String> = Vec::with_capacity(n);
    let mut severity_numbers: Vec<u8> = Vec::with_capacity(n);
    let mut bodies: Vec<String> = Vec::with_capacity(n);
    let mut trace_ids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut span_ids_col: Vec<Option<String>> = Vec::with_capacity(n);
    let mut resource_ids_col: Vec<u32> = Vec::with_capacity(n);
    let mut scope_ids_col: Vec<u32> = Vec::with_capacity(n);

    // Promoted attribute column buffers
    let num_promoted = catalog.len();
    let mut col_strings: Vec<Vec<Option<String>>> = vec![Vec::new(); num_promoted];
    let mut col_i64: Vec<Vec<Option<i64>>> = vec![Vec::new(); num_promoted];
    let mut col_f64: Vec<Vec<Option<f64>>> = vec![Vec::new(); num_promoted];
    let mut col_bool: Vec<Vec<Option<bool>>> = vec![Vec::new(); num_promoted];

    let mut overflow_rows: Vec<Vec<&opentelemetry_proto::tonic::common::v1::KeyValue>> =
        Vec::with_capacity(n);

    for (row_idx, (otlp_log, resource_id, scope_id, service_name)) in items.iter().enumerate() {
        log_ids.push(LogId::new().to_hex());

        // Timestamps — use current time when time_unix_nano is 0 per OTLP spec
        let ts_ns: i64 = if otlp_log.time_unix_nano == 0 {
            Timestamp::now().map(|t| t.as_nanos()).unwrap_or(0)
        } else {
            otlp_log.time_unix_nano as i64
        };
        let obs_ns: i64 = if otlp_log.observed_time_unix_nano == 0 {
            ts_ns
        } else {
            otlp_log.observed_time_unix_nano as i64
        };
        timestamps.push(ts_ns);
        observed_timestamps.push(obs_ns);
        service_names.push(service_name.clone());

        // Severity
        severity_texts.push(otlp_log.severity_text.clone());
        let sev_num = match otlp_log.severity_number {
            1..=4 => LogSeverity::Trace.to_number(),
            5..=8 => LogSeverity::Debug.to_number(),
            9..=12 => LogSeverity::Info.to_number(),
            13..=16 => LogSeverity::Warn.to_number(),
            17..=20 => LogSeverity::Error.to_number(),
            21..=24 => LogSeverity::Fatal.to_number(),
            _ => LogSeverity::Info.to_number(),
        };
        severity_numbers.push(sev_num);

        // Body
        let body = otlp_log
            .body
            .as_ref()
            .and_then(|b| b.value.as_ref())
            .and_then(|v| {
                if let OtlpValue::StringValue(s) = v {
                    Some(s.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        bodies.push(body);

        // Trace context (optional)
        let trace_id_hex = if otlp_log.trace_id.len() == 16 {
            let arr: [u8; 16] = otlp_log.trace_id.as_slice().try_into().unwrap();
            Some(TraceId::from_bytes(arr).to_hex())
        } else {
            None
        };
        let span_id_hex = if otlp_log.span_id.len() == 8 {
            let arr: [u8; 8] = otlp_log.span_id.as_slice().try_into().unwrap();
            Some(SpanId::from_bytes(arr).to_hex())
        } else {
            None
        };
        trace_ids.push(trace_id_hex);
        span_ids_col.push(span_id_hex);

        resource_ids_col.push(*resource_id);
        scope_ids_col.push(*scope_id);

        for (col_idx, attr) in catalog.promoted_columns().enumerate() {
            match attr.value_type {
                AttributeValueType::String => col_strings[col_idx].push(None),
                AttributeValueType::Int64 => col_i64[col_idx].push(None),
                AttributeValueType::Float64 => col_f64[col_idx].push(None),
                AttributeValueType::Boolean => col_bool[col_idx].push(None),
            }
        }

        let mut row_overflow: Vec<&opentelemetry_proto::tonic::common::v1::KeyValue> = Vec::new();
        for kv in &otlp_log.attributes {
            let routed = if let Some(col_idx) = catalog.column_index(&kv.key) {
                let attr = &catalog.promoted[col_idx];
                if let Some(av) = &kv.value {
                    if let Some(val) = &av.value {
                        match (&attr.value_type, val) {
                            (AttributeValueType::String, OtlpValue::StringValue(s)) => {
                                col_strings[col_idx][row_idx] = Some(s.clone());
                                true
                            }
                            (AttributeValueType::Int64, OtlpValue::IntValue(i)) => {
                                col_i64[col_idx][row_idx] = Some(*i);
                                true
                            }
                            (AttributeValueType::Float64, OtlpValue::DoubleValue(f)) => {
                                col_f64[col_idx][row_idx] = Some(*f);
                                true
                            }
                            (AttributeValueType::Boolean, OtlpValue::BoolValue(b)) => {
                                col_bool[col_idx][row_idx] = Some(*b);
                                true
                            }
                            _ => false,
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };
            if !routed {
                row_overflow.push(kv);
            }
        }
        overflow_rows.push(row_overflow);
    }

    // Build Arrow arrays in schema order: 11 core + N promoted + 1 overflow
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(11 + num_promoted + 1);

    arrays.push(Arc::new(StringViewArray::from(log_ids)) as ArrayRef);
    arrays.push(Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef);
    arrays.push(Arc::new(TimestampNanosecondArray::from(observed_timestamps)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(service_names)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(severity_texts)) as ArrayRef);
    arrays.push(Arc::new(UInt8Array::from(severity_numbers)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(bodies)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(trace_ids)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(span_ids_col)) as ArrayRef);
    arrays.push(Arc::new(UInt32Array::from(resource_ids_col)) as ArrayRef);
    arrays.push(Arc::new(UInt32Array::from(scope_ids_col)) as ArrayRef);

    for (col_idx, attr) in catalog.promoted_columns().enumerate() {
        let arr: ArrayRef = match attr.value_type {
            AttributeValueType::String => {
                let vals: Vec<Option<&str>> =
                    col_strings[col_idx].iter().map(|s| s.as_deref()).collect();
                Arc::new(StringViewArray::from(vals)) as ArrayRef
            }
            AttributeValueType::Int64 => {
                Arc::new(Int64Array::from(col_i64[col_idx].clone())) as ArrayRef
            }
            AttributeValueType::Float64 => {
                Arc::new(Float64Array::from(col_f64[col_idx].clone())) as ArrayRef
            }
            AttributeValueType::Boolean => {
                Arc::new(BooleanArray::from(col_bool[col_idx].clone())) as ArrayRef
            }
        };
        arrays.push(arr);
    }
    arrays.push(build_overflow_column(&overflow_rows));

    debug_assert!(
        arrays.iter().all(|a| a.len() == n),
        "otlp_logs_to_batch: column length mismatch; expected {} rows, got lengths: {:?}",
        n,
        arrays.iter().map(|a| a.len()).collect::<Vec<_>>()
    );

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use opentelemetry_proto::tonic::common::v1::AnyValue as OtlpAnyValue;
    use opentelemetry_proto::tonic::logs::v1::LogRecord;
    use sequins_types::schema_catalog::SchemaCatalog;

    fn empty_catalog() -> SchemaCatalog {
        SchemaCatalog::new(vec![])
    }

    fn make_log(
        ts_ns: u64,
        body: &str,
        severity_number: i32,
        severity_text: &str,
        trace_id: Option<Vec<u8>>,
        span_id: Option<Vec<u8>>,
    ) -> LogRecord {
        LogRecord {
            time_unix_nano: ts_ns,
            observed_time_unix_nano: ts_ns,
            severity_number,
            severity_text: severity_text.to_string(),
            body: Some(OtlpAnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        body.to_string(),
                    ),
                ),
            }),
            trace_id: trace_id.unwrap_or_default(),
            span_id: span_id.unwrap_or_default(),
            ..Default::default()
        }
    }

    #[test]
    fn test_otlp_logs_to_batch_basic() {
        let log = make_log(1_000_000_000, "hello world", 9, "INFO", None, None);
        let items = vec![(log, 1u32, 2u32, "my-service".to_string())];
        let catalog = empty_catalog();
        let batch = otlp_logs_to_batch(items, &catalog).unwrap();

        assert_eq!(batch.num_rows(), 1);
        // 11 core + 0 promoted + 1 overflow = 12
        assert_eq!(batch.num_columns(), 12);

        // service_name column (index 3)
        let services = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(services.value(0), "my-service");

        // body column (index 6)
        let bodies = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(bodies.value(0), "hello world");

        // timestamp column (index 1)
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(timestamps.value(0), 1_000_000_000i64);
    }

    #[test]
    fn test_otlp_logs_to_batch_trace_context() {
        // Log with trace context
        let with_trace = make_log(
            1_000_000_000,
            "traced",
            9,
            "INFO",
            Some(vec![1u8; 16]),
            Some(vec![2u8; 8]),
        );
        // Log without trace context
        let without_trace = make_log(2_000_000_000, "untraced", 9, "INFO", None, None);

        let items = vec![
            (with_trace, 1u32, 1u32, "svc".to_string()),
            (without_trace, 1u32, 1u32, "svc".to_string()),
        ];
        let catalog = empty_catalog();
        let batch = otlp_logs_to_batch(items, &catalog).unwrap();

        assert_eq!(batch.num_rows(), 2);

        // trace_id column (index 7)
        let trace_ids = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert!(!trace_ids.is_null(0), "should have trace_id");
        assert!(trace_ids.is_null(1), "should have no trace_id");

        // span_id column (index 8)
        let span_ids = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert!(!span_ids.is_null(0), "should have span_id");
        assert!(span_ids.is_null(1), "should have no span_id");
    }

    #[test]
    fn test_otlp_logs_to_batch_severity_mapping() {
        // Test each severity range maps to the right number
        // 1-4 → TRACE(1), 5-8 → DEBUG(5), 9-12 → INFO(9), 13-16 → WARN(13),
        // 17-20 → ERROR(17), 21-24 → FATAL(21), other → INFO(9) fallback
        let test_cases: Vec<(i32, u8)> = vec![
            (1, LogSeverity::Trace.to_number()),
            (4, LogSeverity::Trace.to_number()),
            (5, LogSeverity::Debug.to_number()),
            (9, LogSeverity::Info.to_number()),
            (13, LogSeverity::Warn.to_number()),
            (17, LogSeverity::Error.to_number()),
            (21, LogSeverity::Fatal.to_number()),
            (0, LogSeverity::Info.to_number()), // fallback
        ];

        for (sev_num, expected) in &test_cases {
            let log = make_log(1_000_000_000, "msg", *sev_num, "", None, None);
            let items = vec![(log, 1u32, 1u32, "svc".to_string())];
            let batch = otlp_logs_to_batch(items, &empty_catalog()).unwrap();
            let sev_col = batch
                .column(5) // severity_number column
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .unwrap();
            assert_eq!(
                sev_col.value(0),
                *expected,
                "severity_number={sev_num} should map to {expected}"
            );
        }
    }
}
