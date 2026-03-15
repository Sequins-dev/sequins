use futures::StreamExt;
use sequins_query::QueryApi;
use sequins_storage::{DataFusionBackend, Storage, StorageConfig};
use std::sync::Arc;

#[tokio::test]
async fn check_vortex_query() {
    let mut config = StorageConfig::default();
    config.cold_tier.uri = format!(
        "{}/Library/Application Support/SequinsSwift/sequins.db",
        std::env::var("HOME").unwrap()
    );

    let storage = Arc::new(Storage::new(config).await.unwrap());
    let backend = DataFusionBackend::new(storage);

    eprintln!("\n🔍 Executing query for last 24 hours");

    // Use SeQL query string instead of constructing AST
    let query_str = "spans last 24h | take 5";

    eprintln!("Executing...");
    let mut stream = match backend.query(query_str).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Failed: {:?}", e);
            panic!("{:?}", e);
        }
    };

    use sequins_query::flight::{decode_metadata, SeqlMetadata};
    while let Some(frame_result) = stream.next().await {
        match frame_result {
            Ok(frame) => {
                if let Some(SeqlMetadata::Data { .. }) = decode_metadata(&frame.app_metadata) {
                    let batch = sequins_query::frame::ipc_to_batch(&frame.data_body).unwrap();
                    eprintln!("✅ Got {} rows", batch.num_rows());
                }
            }
            Err(e) => {
                eprintln!("❌ Error: {:?}", e);
                panic!("{:?}", e);
            }
        }
    }
}
