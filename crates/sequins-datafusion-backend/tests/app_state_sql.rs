//! `execute_app_state_sql` reads the app-state tables from a telemetry-free context,
//! so chat/dashboard reads never depend on (or trigger) signal/cold-tier machinery.

use futures::StreamExt;
use sequins_datafusion_backend::DataFusionBackend;
use sequins_flight::{decode_metadata, SeqlMetadata};
use sequins_metadata::ConversationItem;
use sequins_storage::test_fixtures::{make_test_otlp_traces, TestStorageBuilder};
use sequins_traits::OtlpIngest;
use std::sync::Arc;

fn user_item(text: &str) -> ConversationItem {
    ConversationItem {
        response_id: Some("resp_1".into()),
        position: 0,
        role: "user".into(),
        item_type: "message".into(),
        text: Some(text.into()),
        tool_name: None,
        tool_arguments: None,
        tool_output: None,
        created_at_ns: 0,
    }
}

/// Count the rows returned by an app-state SQL read.
async fn app_state_rows(backend: &DataFusionBackend, sql: &str) -> usize {
    let mut stream = backend
        .execute_app_state_sql(sql)
        .await
        .expect("execute_app_state_sql");
    let mut rows = 0;
    while let Some(frame) = stream.next().await {
        let frame = frame.expect("frame error");
        if let Some(SeqlMetadata::Data { .. }) = decode_metadata(&frame.app_metadata) {
            if let Ok(batch) = sequins_flight::ipc_to_batch(&frame.data_body) {
                rows += batch.num_rows();
            }
        }
    }
    rows
}

#[tokio::test]
async fn app_state_sql_reads_conversations() {
    let (storage, _t) = TestStorageBuilder::new().build().await;
    // Ingest telemetry too, to prove the app-state read is independent of the tiers.
    storage
        .ingest_traces(make_test_otlp_traces(1, 10))
        .await
        .unwrap();
    storage
        .app_state()
        .append_response(
            None,
            Some("First chat".into()),
            vec![user_item("hello")],
            vec![],
            "resp_1".into(),
        )
        .await
        .unwrap();

    let storage = Arc::new(storage);
    let backend = DataFusionBackend::new(storage);

    let rows = app_state_rows(
        &backend,
        "SELECT id, title, updated_at_ns, item_count FROM conversations",
    )
    .await;
    assert_eq!(rows, 1, "the one persisted conversation should be returned");

    // Messages of that conversation are readable too.
    let msg_rows = app_state_rows(&backend, "SELECT role, text FROM messages").await;
    assert_eq!(msg_rows, 1, "the single user message should be returned");
}
