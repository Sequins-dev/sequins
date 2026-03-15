//! Direct OTLP span link → Arrow RecordBatch conversion

use crate::overflow_map::build_overflow_column;
use arrow::array::{ArrayRef, StringViewArray};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use sequins_types::arrow_schema::span_links_schema;
use sequins_types::models::{SpanId, TraceId};
use std::sync::Arc;

/// Convert OTLP span links to an Arrow `RecordBatch` (span_links_schema).
///
/// `items` contains `(source_trace_id_hex, source_span_id_hex, link)` tuples
/// where the first two fields identify the span that owns the link.
pub fn otlp_span_links_to_batch(
    items: Vec<(
        String,
        String,
        opentelemetry_proto::tonic::trace::v1::span::Link,
    )>,
) -> Result<RecordBatch, String> {
    let schema = span_links_schema();
    let n = items.len();
    if n == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut src_trace_ids: Vec<String> = Vec::with_capacity(n);
    let mut src_span_ids: Vec<String> = Vec::with_capacity(n);
    let mut tgt_trace_ids: Vec<String> = Vec::with_capacity(n);
    let mut tgt_span_ids: Vec<String> = Vec::with_capacity(n);
    let mut trace_states: Vec<Option<String>> = Vec::with_capacity(n);
    let mut overflow_rows: Vec<Vec<&KeyValue>> = Vec::with_capacity(n);

    for (src_trace_id_hex, src_span_id_hex, link) in &items {
        src_trace_ids.push(src_trace_id_hex.clone());
        src_span_ids.push(src_span_id_hex.clone());

        let tgt_trace_hex = if link.trace_id.len() == 16 {
            let arr: [u8; 16] = link.trace_id.as_slice().try_into().unwrap();
            TraceId::from_bytes(arr).to_hex()
        } else {
            String::new()
        };
        let tgt_span_hex = if link.span_id.len() == 8 {
            let arr: [u8; 8] = link.span_id.as_slice().try_into().unwrap();
            SpanId::from_bytes(arr).to_hex()
        } else {
            String::new()
        };
        tgt_trace_ids.push(tgt_trace_hex);
        tgt_span_ids.push(tgt_span_hex);

        let ts = if link.trace_state.is_empty() {
            None
        } else {
            Some(link.trace_state.clone())
        };
        trace_states.push(ts);

        overflow_rows.push(link.attributes.iter().collect());
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringViewArray::from(src_trace_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(src_span_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(tgt_trace_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(tgt_span_ids)) as ArrayRef,
        Arc::new(StringViewArray::from(trace_states)) as ArrayRef,
        build_overflow_column(&overflow_rows),
    ];

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use opentelemetry_proto::tonic::trace::v1::span::Link;
    use sequins_types::arrow_schema::span_links_schema;

    fn make_link(target_trace: Vec<u8>, target_span: Vec<u8>, trace_state: &str) -> Link {
        Link {
            trace_id: target_trace,
            span_id: target_span,
            trace_state: trace_state.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_otlp_span_links_to_batch_basic() {
        let src_trace = "aaaa".to_string();
        let src_span = "bbbb".to_string();
        let link_with_state = make_link(vec![3u8; 16], vec![4u8; 8], "key=val");
        let link_no_state = make_link(vec![5u8; 16], vec![6u8; 8], "");

        let items = vec![
            (src_trace.clone(), src_span.clone(), link_with_state),
            (src_trace.clone(), src_span.clone(), link_no_state),
        ];
        let batch = otlp_span_links_to_batch(items).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema(), span_links_schema());

        // src_trace_id column (index 0)
        let src_traces = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(src_traces.value(0), src_trace);

        // target trace and span IDs should be hex-encoded (index 2, 3)
        let tgt_traces = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert!(
            !tgt_traces.value(0).is_empty(),
            "target trace_id should be hex encoded"
        );

        // trace_state column (index 4): Some for first row, None for second
        let trace_states = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert!(!trace_states.is_null(0), "trace_state should be Some");
        assert_eq!(trace_states.value(0), "key=val");
        assert!(trace_states.is_null(1), "empty trace_state should be None");

        // Empty input returns empty batch with correct schema
        let empty = otlp_span_links_to_batch(vec![]).unwrap();
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), span_links_schema());
    }
}
