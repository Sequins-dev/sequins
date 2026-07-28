//! SeQL aliases must become the output column names, and a `ts() bin` bucket must be a
//! real Timestamp column — both are what let clients render clean labels and time axes.

use arrow::datatypes::DataType;
use futures::StreamExt;
use sequins_datafusion_backend::DataFusionBackend;
use sequins_flight::{decode_metadata, SeqlMetadata};
use sequins_storage::test_fixtures::{make_test_otlp_traces, TestStorageBuilder};
use sequins_traits::{OtlpIngest, QueryApi};
use std::sync::Arc;

/// Run `seql` and return the primary result's `(field_name, data_type)` list.
async fn schema_of(backend: &DataFusionBackend, seql: &str) -> Vec<(String, DataType)> {
    let mut stream = backend.query(seql).await.expect("query failed");
    while let Some(frame) = stream.next().await {
        let frame = frame.expect("frame error");
        // The Data frame for the primary table carries the schema in its Arrow batch.
        if let Some(SeqlMetadata::Data { table: None }) = decode_metadata(&frame.app_metadata) {
            let batch = sequins_flight::ipc_to_batch(&frame.data_body).unwrap();
            return batch
                .schema()
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().clone()))
                .collect();
        }
    }
    panic!("no primary data frame for `{seql}`");
}

#[tokio::test]
async fn scalar_aggregation_uses_alias_name() {
    let (storage, _t) = TestStorageBuilder::new().build().await;
    storage
        .ingest_traces(make_test_otlp_traces(1, 40))
        .await
        .unwrap();
    let backend = DataFusionBackend::new(Arc::new(storage));

    let schema = schema_of(
        &backend,
        "spans last 1h | group by {} { count() as total_spans }",
    )
    .await;
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].0, "total_spans", "alias must be the column name");
}

#[tokio::test]
async fn timeseries_bucket_is_named_and_timestamped() {
    let (storage, _t) = TestStorageBuilder::new().build().await;
    storage
        .ingest_traces(make_test_otlp_traces(1, 60))
        .await
        .unwrap();
    let backend = DataFusionBackend::new(Arc::new(storage));

    let schema = schema_of(
        &backend,
        "spans last 1h | group by { ts() bin 1m as bucket } { count() as n }",
    )
    .await;

    let names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"bucket"), "group-key alias, got {names:?}");
    assert!(names.contains(&"n"), "aggregation alias, got {names:?}");

    let bucket = schema.iter().find(|(n, _)| n == "bucket").unwrap();
    assert!(
        matches!(bucket.1, DataType::Timestamp(_, _)),
        "bucket must be a Timestamp, got {:?}",
        bucket.1
    );
}
