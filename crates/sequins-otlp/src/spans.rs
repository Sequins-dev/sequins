//! Direct OTLP span → Arrow RecordBatch conversion

use crate::helpers::PromotedAttrBuilder;
use arrow::array::{
    ArrayRef, Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array, UInt8Array,
};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::trace::v1::Span as OtlpSpan;
use sequins_arrow_schema::arrow_schema::span_schema_with_catalog;
use sequins_attribute_codec::build_overflow_column;
use sequins_types::models::{SpanId, TraceId};
use std::sync::Arc;

/// Convert a batch of OTLP spans directly to an Arrow `RecordBatch`.
///
/// Attributes are routed to either promoted first-class columns (if the key is
/// in the `SchemaCatalog`) or the CBOR-encoded `_overflow_attrs` map column.
///
/// The output schema is `span_schema_with_catalog(catalog)`.
pub fn otlp_spans_to_batch(
    items: Vec<(OtlpSpan, u32, u32)>,
    catalog: &sequins_arrow_schema::SchemaCatalog,
) -> Result<RecordBatch, String> {
    let schema = span_schema_with_catalog(catalog);
    let n = items.len();
    if n == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    // Core column buffers
    let mut trace_ids: Vec<String> = Vec::with_capacity(n);
    let mut span_ids: Vec<String> = Vec::with_capacity(n);
    let mut parent_span_ids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut names: Vec<String> = Vec::with_capacity(n);
    let mut kinds: Vec<u8> = Vec::with_capacity(n);
    let mut statuses: Vec<u8> = Vec::with_capacity(n);
    let mut start_times: Vec<i64> = Vec::with_capacity(n);
    let mut end_times: Vec<i64> = Vec::with_capacity(n);
    let mut durations: Vec<i64> = Vec::with_capacity(n);
    let mut resource_ids_col: Vec<u32> = Vec::with_capacity(n);
    let mut scope_ids_col: Vec<u32> = Vec::with_capacity(n);

    let num_promoted = catalog.len();
    let mut attr_builder = PromotedAttrBuilder::new(catalog);

    let mut overflow_rows: Vec<Vec<&opentelemetry_proto::tonic::common::v1::KeyValue>> =
        Vec::with_capacity(n);

    for (otlp_span, resource_id, scope_id) in items.iter() {
        let trace_id_hex = TraceId::from_bytes(
            otlp_span
                .trace_id
                .as_slice()
                .try_into()
                .unwrap_or([0u8; 16]),
        )
        .to_hex();
        let span_id_hex =
            SpanId::from_bytes(otlp_span.span_id.as_slice().try_into().unwrap_or([0u8; 8]))
                .to_hex();
        let parent_id_hex = if otlp_span.parent_span_id.is_empty() {
            None
        } else {
            Some(
                SpanId::from_bytes(
                    otlp_span
                        .parent_span_id
                        .as_slice()
                        .try_into()
                        .unwrap_or([0u8; 8]),
                )
                .to_hex(),
            )
        };

        trace_ids.push(trace_id_hex);
        span_ids.push(span_id_hex);
        parent_span_ids.push(parent_id_hex);
        names.push(otlp_span.name.clone());

        let kind = match otlp_span.kind {
            1 => 1u8,
            2 => 2u8,
            3 => 3u8,
            4 => 4u8,
            5 => 5u8,
            _ => 0u8,
        };
        kinds.push(kind);

        let status = if let Some(ref s) = otlp_span.status {
            match s.code {
                1 => 1u8,
                2 => 2u8,
                _ => 0u8,
            }
        } else {
            0u8
        };
        statuses.push(status);

        start_times.push(otlp_span.start_time_unix_nano as i64);
        end_times.push(otlp_span.end_time_unix_nano as i64);
        durations.push(
            (otlp_span.end_time_unix_nano as i64)
                .saturating_sub(otlp_span.start_time_unix_nano as i64),
        );
        resource_ids_col.push(*resource_id);
        scope_ids_col.push(*scope_id);

        let row_overflow = attr_builder.push_row(&otlp_span.attributes);
        overflow_rows.push(row_overflow);
    }

    // Build Arrow arrays in schema order: 11 core + N promoted + 1 overflow
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(11 + num_promoted + 1);

    arrays.push(Arc::new(StringViewArray::from(trace_ids)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(span_ids)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(parent_span_ids)) as ArrayRef);
    arrays.push(Arc::new(StringViewArray::from(names)) as ArrayRef);
    arrays.push(Arc::new(UInt8Array::from(kinds)) as ArrayRef);
    arrays.push(Arc::new(UInt8Array::from(statuses)) as ArrayRef);
    arrays.push(Arc::new(TimestampNanosecondArray::from(start_times)) as ArrayRef);
    arrays.push(Arc::new(TimestampNanosecondArray::from(end_times)) as ArrayRef);
    arrays.push(Arc::new(Int64Array::from(durations)) as ArrayRef);
    arrays.push(Arc::new(UInt32Array::from(resource_ids_col)) as ArrayRef);
    arrays.push(Arc::new(UInt32Array::from(scope_ids_col)) as ArrayRef);

    arrays.extend(attr_builder.finish());
    arrays.push(build_overflow_column(&overflow_rows));

    debug_assert!(
        arrays.iter().all(|a| a.len() == n),
        "otlp_spans_to_batch: column length mismatch; expected {} rows, got lengths: {:?}",
        n,
        arrays.iter().map(|a| a.len()).collect::<Vec<_>>()
    );

    RecordBatch::try_new(schema, arrays).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use opentelemetry_proto::tonic::common::v1::{
        any_value::Value as OtlpValue, AnyValue, KeyValue,
    };
    use opentelemetry_proto::tonic::trace::v1::{span::SpanKind, Status};
    use sequins_arrow_schema::schema_catalog::{
        AttributeValueType, EncodingHint, PromotedAttribute, SchemaCatalog,
    };

    fn make_kv(
        key: &str,
        val: opentelemetry_proto::tonic::common::v1::any_value::Value,
    ) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(val) }),
        }
    }

    fn make_span(
        name: &str,
        kind: i32,
        start_ns: u64,
        attrs: Vec<KeyValue>,
        parent: Option<Vec<u8>>,
    ) -> OtlpSpan {
        OtlpSpan {
            trace_id: vec![1u8; 16],
            span_id: vec![2u8; 8],
            parent_span_id: parent.unwrap_or_default(),
            name: name.to_string(),
            kind,
            start_time_unix_nano: start_ns,
            end_time_unix_nano: start_ns + 1_000_000,
            attributes: attrs,
            status: Some(Status {
                code: 1, // STATUS_CODE_OK
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn empty_catalog() -> SchemaCatalog {
        SchemaCatalog::new(vec![])
    }

    fn catalog_with_method() -> SchemaCatalog {
        SchemaCatalog::new(vec![PromotedAttribute {
            key: "http.request.method",
            column_name: "http_request_method",
            value_type: AttributeValueType::String,
            encoding_hint: EncodingHint::DictionaryEncoded,
        }])
    }

    #[test]
    fn test_otlp_spans_to_batch_basic() {
        let span = make_span(
            "GET /users",
            SpanKind::Server as i32,
            1_000_000_000,
            vec![],
            None,
        );
        let items = vec![(span, 1u32, 2u32)];
        let catalog = empty_catalog();
        let batch = otlp_spans_to_batch(items, &catalog).unwrap();

        assert_eq!(batch.num_rows(), 1);
        // 11 core + 0 promoted + 1 overflow = 12
        assert_eq!(batch.num_columns(), 12);

        // Check name column (index 3)
        let names = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(names.value(0), "GET /users");

        // Check kind column (index 4)
        let kinds = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        assert_eq!(kinds.value(0), 2); // SpanKind::Server

        // Check start_time column (index 6)
        let starts = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(starts.value(0), 1_000_000_000i64);
    }

    #[test]
    fn test_otlp_spans_to_batch_promoted_attributes() {
        let attr = make_kv(
            "http.request.method",
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                "GET".to_string(),
            ),
        );
        let span = make_span(
            "handler",
            SpanKind::Server as i32,
            2_000_000_000,
            vec![attr],
            None,
        );
        let items = vec![(span, 1u32, 1u32)];
        let catalog = catalog_with_method();
        let batch = otlp_spans_to_batch(items, &catalog).unwrap();

        // 11 core + 1 promoted + 1 overflow = 13
        assert_eq!(batch.num_columns(), 13);
        assert_eq!(batch.num_rows(), 1);

        // Promoted column is at index 11 (after 11 core columns)
        let method_col = batch
            .column(11)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(method_col.value(0), "GET");
    }

    #[test]
    fn test_otlp_spans_to_batch_underscore_key_promotion() {
        // Underscore-keyed attributes (e.g. from opentelemetry-appender-tracing)
        // should be promoted to the correct column via the underscore alias.
        let attr = make_kv(
            "http_request_method", // underscore form
            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                "POST".to_string(),
            ),
        );
        let span = make_span(
            "post_handler",
            SpanKind::Server as i32,
            3_000_000_000,
            vec![attr],
            None,
        );
        let items = vec![(span, 1u32, 1u32)];
        let catalog = catalog_with_method();
        let batch = otlp_spans_to_batch(items, &catalog).unwrap();

        assert_eq!(batch.num_rows(), 1);
        // Promoted column at index 11
        let method_col = batch
            .column(11)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert_eq!(
            method_col.value(0),
            "POST",
            "underscore key should be promoted"
        );
    }

    #[test]
    fn test_otlp_spans_to_batch_empty_input() {
        let catalog = empty_catalog();
        let batch = otlp_spans_to_batch(vec![], &catalog).unwrap();

        assert_eq!(batch.num_rows(), 0);
        // Schema should still be present (12 columns with empty catalog)
        assert_eq!(batch.num_columns(), 12);
    }

    #[test]
    fn test_otlp_spans_to_batch_parent_span() {
        // Span with no parent
        let root = make_span("root", SpanKind::Server as i32, 1_000_000_000, vec![], None);
        // Span with a parent
        let child = make_span(
            "child",
            SpanKind::Internal as i32,
            1_000_500_000,
            vec![],
            Some(vec![2u8; 8]),
        );
        let catalog = empty_catalog();
        let items = vec![(root, 1u32, 1u32), (child, 1u32, 1u32)];
        let batch = otlp_spans_to_batch(items, &catalog).unwrap();

        assert_eq!(batch.num_rows(), 2);
        // parent_span_id column is at index 2
        let parent_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        assert!(parent_ids.is_null(0), "root span should have null parent");
        assert!(
            !parent_ids.is_null(1),
            "child span should have non-null parent"
        );
    }

    #[test]
    fn test_unknown_span_kind_value_maps_to_default() {
        // kind=99 is not a valid SpanKind — should map to 0 (Unspecified)
        let span = make_span("op", 99, 1_000_000_000, vec![], None);
        let batch = otlp_spans_to_batch(vec![(span, 1u32, 1u32)], &empty_catalog()).unwrap();
        let kinds = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        assert_eq!(kinds.value(0), 0, "unknown span kind should map to 0");
    }

    #[test]
    fn test_unknown_status_code_maps_to_default() {
        // status.code=99 — should map to 0 (Unset)
        let mut span = make_span("op", SpanKind::Server as i32, 1_000_000_000, vec![], None);
        span.status = Some(Status {
            code: 99,
            ..Default::default()
        });
        let batch = otlp_spans_to_batch(vec![(span, 1u32, 1u32)], &empty_catalog()).unwrap();
        let statuses = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .unwrap();
        assert_eq!(statuses.value(0), 0, "unknown status code should map to 0");
    }

    #[test]
    fn test_promoted_attribute_type_mismatch_goes_to_overflow() {
        // Promoted attribute expects String but we send an IntValue → goes to overflow
        let catalog = catalog_with_method();
        let wrong_type_attr = make_kv(
            "http.request.method",
            OtlpValue::IntValue(42), // wrong type
        );
        let span = make_span(
            "op",
            SpanKind::Server as i32,
            1_000_000_000,
            vec![wrong_type_attr],
            None,
        );
        let batch = otlp_spans_to_batch(vec![(span, 1u32, 1u32)], &catalog).unwrap();

        // The promoted column should be null (value went to overflow)
        // The promoted column is at index 11 (11 core + 0 catalog = 11, then promoted[0] = 11)
        assert_eq!(batch.num_rows(), 1);
        let promoted_col = batch.column(11); // first promoted column
        assert!(
            promoted_col.is_null(0),
            "type-mismatched attribute should produce null promoted column"
        );
    }
}
