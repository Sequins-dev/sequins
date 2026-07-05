//! QueryScope selector — hot-only / cold-only / all tier isolation.
//!
//! Verifies that a plan stamped with `HotOnly` scans only the in-memory hot
//! tier, `ColdOnly` scans only shared cold storage, and `All` unions both. This
//! is the foundation for distributed fan-out: peers answer `HotOnly`, the
//! coordinator reads shared cold once via `ColdOnly`.

use futures::StreamExt;
use seql_substrait::seql_ext::QueryScope;
use seql_substrait::{compile, schema_context, set_plan_scope};
use sequins_datafusion_backend::DataFusionBackend;
use sequins_flight::{decode_metadata, SeqlMetadata};
use sequins_storage::test_fixtures::{make_test_otlp_logs, TestStorageBuilder};
use sequins_traits::{OtlpIngest, QueryExec};
use std::sync::Arc;

/// Execute a plan and count the rows delivered in `Data` frames.
async fn count_rows(backend: &DataFusionBackend, plan: Vec<u8>) -> usize {
    use arrow::ipc::reader::StreamReader;
    let mut stream = backend.execute(plan).await.expect("execute plan");
    let mut rows = 0usize;
    while let Some(fd) = stream.next().await {
        let fd = fd.expect("flight data");
        if let Some(SeqlMetadata::Data { .. }) = decode_metadata(&fd.app_metadata) {
            if !fd.data_body.is_empty() {
                if let Ok(reader) =
                    StreamReader::try_new(std::io::Cursor::new(&fd.data_body[..]), None)
                {
                    for batch in reader.flatten() {
                        rows += batch.num_rows();
                    }
                }
            }
        }
    }
    rows
}

#[tokio::test]
async fn test_query_scope_isolates_tiers() {
    // Ingest 5 logs — they land in the hot tier and are not flushed to cold.
    let (storage, _tmp) = TestStorageBuilder::new().build().await;
    storage
        .ingest_logs(make_test_otlp_logs(1, 5))
        .await
        .unwrap();
    let backend = DataFusionBackend::new(Arc::new(storage));

    // Compile once, then stamp each leg with its scope.
    let ctx = schema_context().unwrap();
    let plan = compile("logs last 1h", &ctx).await.unwrap();
    let hot = set_plan_scope(&plan, QueryScope::HotOnly).unwrap();
    let cold = set_plan_scope(&plan, QueryScope::ColdOnly).unwrap();
    let all = set_plan_scope(&plan, QueryScope::All).unwrap();

    let (h, c, a) = (
        count_rows(&backend, hot).await,
        count_rows(&backend, cold).await,
        count_rows(&backend, all).await,
    );

    // Scope isolation: hot-only sees the un-flushed hot data, cold-only sees
    // nothing (nothing has been flushed), and All equals hot ∪ (empty cold).
    assert!(
        h > 0,
        "hot-only should see the un-flushed hot logs, got {h}"
    );
    assert_eq!(
        c, 0,
        "cold-only should see nothing — nothing has been flushed to cold"
    );
    assert_eq!(a, h, "all should equal hot-only when cold is empty");
}
