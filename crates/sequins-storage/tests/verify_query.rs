use futures::StreamExt;
use sequins_query::QueryApi;
use sequins_storage::{DataFusionBackend, Storage, StorageConfig};
use std::sync::Arc;

#[tokio::test]
async fn verify_hot_tier_query() {
    eprintln!("\n🔧 Setting up storage...");

    let mut config = StorageConfig::default();
    config.cold_tier.uri = format!(
        "{}/Library/Application Support/SequinsSwift/sequins.db",
        std::env::var("HOME").unwrap()
    );

    let storage = Arc::new(Storage::new(config).await.unwrap());
    let backend = DataFusionBackend::new(storage.clone());

    // Check hot tier
    eprintln!("\n Hot tier contents:");
    let hot = storage.hot_tier_arc();
    let span_count = hot.spans.row_count();
    eprintln!("  Total spans in hot tier chain: {}", span_count);

    // Try query
    eprintln!("\n🔍 Executing: spans last 1h | take 5");

    let mut stream = backend
        .query("spans last 1h | take 5")
        .await
        .expect("Execute failed");

    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    let mut row_count = 0;
    while let Some(frame_result) = stream.next().await {
        match frame_result {
            Ok(frame) => {
                if let Some(SeqlMetadata::Data { .. }) = decode_metadata(&frame.app_metadata) {
                    let batch = sequins_query::frame::ipc_to_batch(&frame.data_body).unwrap();
                    row_count = batch.num_rows();
                    eprintln!(
                        "\n✅ Got {} rows, {} columns",
                        batch.num_rows(),
                        batch.num_columns()
                    );
                }
            }
            Err(e) => {
                eprintln!("❌ Error: {:?}", e);
                panic!("Query failed: {:?}", e);
            }
        }
    }

    eprintln!(
        "\n Returned {} rows (hot tier chain has {} rows)",
        row_count, span_count
    );
}
