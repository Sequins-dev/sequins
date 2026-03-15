//! Cross-crate schema consistency tests.
//!
//! Verifies that Arrow schemas produced by `sequins-otlp` OTLP converters
//! exactly match the canonical schemas exposed by `SignalType::schema()` in
//! `sequins-types`.  If these diverge the hot tier, cold tier, DataFusion
//! backend, and OTLP ingest path will all silently disagree about column
//! layouts.

use sequins_otlp::{
    otlp_datapoints_to_batch, otlp_exp_histograms_to_batch, otlp_histograms_to_batch,
    otlp_logs_to_batch, otlp_metrics_to_batch, otlp_spans_to_batch,
};
use sequins_types::schema_catalog::SchemaCatalog;
use sequins_types::SignalType;

// ── 5A.1: OTLP batch schemas match SignalType schemas ────────────────────────

/// Schema produced by each OTLP converter must exactly match the canonical
/// schema returned by the corresponding `SignalType::schema()`.
///
/// Empty-item batches are used so `RecordBatch::new_empty(schema)` is
/// returned — the schema is built identically regardless of row count.
#[test]
fn test_otlp_batch_schema_matches_signal_type_schema() {
    // Use the same catalog that SignalType::schema() uses internally.
    let catalog = SchemaCatalog::default_catalog();

    // — Spans —
    let batch = otlp_spans_to_batch(vec![], &catalog).unwrap();
    assert_eq!(
        batch.schema(),
        SignalType::Spans.schema(),
        "Spans: OTLP batch schema must match SignalType::Spans.schema()"
    );

    // — Logs —
    let batch = otlp_logs_to_batch(vec![], &catalog).unwrap();
    assert_eq!(
        batch.schema(),
        SignalType::Logs.schema(),
        "Logs: OTLP batch schema must match SignalType::Logs.schema()"
    );

    // — Metrics metadata (no catalog — function uses metric_schema() directly) —
    let batch = otlp_metrics_to_batch(&[]).unwrap();
    assert_eq!(
        batch.schema(),
        SignalType::MetricsMetadata.schema(),
        "MetricsMetadata: OTLP batch schema must match SignalType::MetricsMetadata.schema()"
    );

    // — Datapoints (gauge / sum) — no catalog —
    let batch = otlp_datapoints_to_batch(&[]).unwrap();
    assert_eq!(
        batch.schema(),
        SignalType::Metrics.schema(),
        "Metrics (datapoints): OTLP batch schema must match SignalType::Metrics.schema()"
    );

    // — Explicit histograms — no catalog —
    let batch = otlp_histograms_to_batch(&[]).unwrap();
    assert_eq!(
        batch.schema(),
        SignalType::Histograms.schema(),
        "Histograms: OTLP batch schema must match SignalType::Histograms.schema()"
    );

    // — Exponential histograms — no catalog —
    let batch = otlp_exp_histograms_to_batch(&[]).unwrap();
    assert_eq!(
        batch.schema(),
        SignalType::ExpHistograms.schema(),
        "ExpHistograms: OTLP batch schema must match SignalType::ExpHistograms.schema()"
    );
}

// ── 5A.2: SignalType schema invariants ──────────────────────────────────────

/// Every SignalType variant must have a non-empty schema with unique field names.
/// A schema with zero fields or duplicate field names would cause silent
/// DataFusion registration failures.
#[test]
fn test_all_signal_type_schemas_have_unique_non_empty_fields() {
    for signal in SignalType::all() {
        let schema = signal.schema();

        assert!(
            !schema.fields().is_empty(),
            "SignalType::{:?} schema has no fields — every signal must have at least one column",
            signal
        );

        let mut seen = std::collections::HashSet::new();
        for field in schema.fields() {
            assert!(
                seen.insert(field.name().clone()),
                "SignalType::{:?} schema has duplicate field name: '{}'",
                signal,
                field.name()
            );
        }
    }
}

/// `SignalType::schema()` must be idempotent — calling it twice produces equal schemas.
/// The schemas are stored as `Arc<Schema>`, so this also verifies pointer or content equality.
#[test]
fn test_signal_type_schema_is_stable_across_calls() {
    for signal in SignalType::all() {
        let s1 = signal.schema();
        let s2 = signal.schema();
        assert_eq!(
            s1, s2,
            "SignalType::{:?} schema is not stable across repeated calls",
            signal
        );
    }
}

/// Every SignalType that declares a time column must have that column present
/// in its schema with a Timestamp type.
#[test]
fn test_signal_type_time_column_exists_in_schema() {
    use arrow::datatypes::DataType;

    for signal in SignalType::all() {
        if let Some(time_col) = signal.time_column() {
            let schema = signal.schema();
            let field = schema.field_with_name(time_col).unwrap_or_else(|_| {
                panic!(
                    "SignalType::{:?} declares time_column='{}' but it is absent from schema",
                    signal, time_col
                )
            });
            assert!(
                matches!(
                    field.data_type(),
                    DataType::Timestamp(_, _) | DataType::Int64 | DataType::UInt64
                ),
                "SignalType::{:?} time column '{}' has unexpected type {:?}",
                signal,
                time_col,
                field.data_type()
            );
        }
    }
}
