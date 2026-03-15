//! Direct OTLP span event → Arrow RecordBatch conversion

use crate::overflow_map::build_overflow_column;
use arrow::array::{ArrayRef, StringViewArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use sequins_types::arrow_schema::span_events_schema;
use std::sync::Arc;

/// Convert OTLP span events to an Arrow `RecordBatch` (span_events_schema).
///
/// `items` contains `(trace_id_hex, span_id_hex, event)` tuples where the IDs
/// are the parent span's hex-encoded identifiers.
pub fn otlp_span_events_to_batch(
    items: Vec<(
        String,
        String,
        opentelemetry_proto::tonic::trace::v1::span::Event,
    )>,
) -> Result<RecordBatch, String> {
    let schema = span_events_schema();
    let n = items.len();
    if n == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut trace_ids: Vec<String> = Vec::with_capacity(n);
    let mut span_ids: Vec<String> = Vec::with_capacity(n);
    let mut timestamps: Vec<i64> = Vec::with_capacity(n);
    let mut names: Vec<String> = Vec::with_capacity(n);
    let mut overflow_rows: Vec<Vec<&KeyValue>> = Vec::with_capacity(n);

    for (trace_id_hex, span_id_hex, event) in &items {
        trace_ids.push(trace_id_hex.clone());
        span_ids.push(span_id_hex.clone());
        timestamps.push(event.time_unix_nano as i64);
        names.push(event.name.clone());
        overflow_rows.push(event.attributes.iter().collect());
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringViewArray::from(trace_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(span_ids)) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
        Arc::new(StringViewArray::from(names)) as ArrayRef,
        build_overflow_column(&overflow_rows),
    ];

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::trace::v1::span::Event;
    use sequins_types::arrow_schema::span_events_schema;

    fn make_event(name: &str, ts_ns: u64) -> Event {
        Event {
            name: name.to_string(),
            time_unix_nano: ts_ns,
            ..Default::default()
        }
    }

    #[test]
    fn test_otlp_span_events_to_batch_basic() {
        let trace_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let span_id = "bbbbbbbbbbbbbbbb".to_string();
        let items = vec![
            (
                trace_id.clone(),
                span_id.clone(),
                make_event("exception", 1_000_000_000),
            ),
            (
                trace_id.clone(),
                span_id.clone(),
                make_event("retry", 2_000_000_000),
            ),
        ];
        let batch = otlp_span_events_to_batch(items).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema(), span_events_schema());

        // trace_id column (index 0)
        let trace_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(trace_ids.value(0), trace_id);
        assert_eq!(trace_ids.value(1), trace_id);

        // name column (index 3)
        let names = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(names.value(0), "exception");
        assert_eq!(names.value(1), "retry");

        // timestamp column (index 2)
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_000_000_000i64);

        // Empty input returns empty batch with correct schema
        let empty = otlp_span_events_to_batch(vec![]).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), span_events_schema());
    }
}
