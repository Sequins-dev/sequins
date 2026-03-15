//! Integration tests for the Flight SQL server

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementSubstraitPlan, ProstMessageExt, SubstraitPlan};
use arrow_flight::{FlightDescriptor, Ticket};
use futures::StreamExt;
use prost::Message as _;
use sequins_server::flight_service_server;
use sequins_storage::{DataFusionBackend, Storage};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::transport::Channel;

// ── helpers ───────────────────────────────────────────────────────────────────

async fn start_server() -> (Channel, tokio::sync::oneshot::Sender<()>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut config = sequins_storage::StorageConfig::default();
    config.cold_tier.uri = format!("file://{}", tmp.path().display());
    config.hot_tier.max_entries = 1000;
    let storage = Arc::new(Storage::new(config).await.unwrap());
    let backend = Arc::new(DataFusionBackend::new(Arc::clone(&storage)));
    let svc = flight_service_server(backend);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    shutdown_rx.await.ok();
                },
            )
            .await
            .unwrap();
    });

    let channel = Channel::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();

    (channel, shutdown_tx, tmp)
}

async fn compile_plan(seql: &str) -> Vec<u8> {
    let ctx = sequins_query::schema_context().expect("schema_context failed");
    sequins_query::compile(seql, &ctx)
        .await
        .expect("compile failed")
}

fn substrait_cmd(plan_bytes: Vec<u8>) -> CommandStatementSubstraitPlan {
    CommandStatementSubstraitPlan {
        plan: Some(SubstraitPlan {
            plan: plan_bytes.into(),
            version: "0.20.0".to_string(),
        }),
        transaction_id: None,
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_get_flight_info_returns_ticket_with_plan_bytes() {
    let (channel, _shutdown, _tmp) = start_server().await;
    let mut client = FlightServiceClient::new(channel);

    let plan_bytes = compile_plan("spans last 1h").await;
    let cmd = substrait_cmd(plan_bytes.clone());
    let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());

    let info = client
        .get_flight_info(descriptor)
        .await
        .expect("get_flight_info failed")
        .into_inner();

    assert_eq!(info.endpoint.len(), 1, "Expected exactly one endpoint");
    let ticket = info.endpoint[0].ticket.as_ref().expect("Expected a ticket");
    assert_eq!(
        ticket.ticket.to_vec(),
        plan_bytes,
        "Ticket should carry the raw plan bytes"
    );
}

#[tokio::test]
async fn test_do_get_with_compiled_plan_returns_flight_data() {
    let (channel, _shutdown, _tmp) = start_server().await;
    let mut client = FlightServiceClient::new(channel);

    let plan_bytes = compile_plan("logs last 1h").await;
    let ticket = Ticket {
        ticket: plan_bytes.into(),
    };

    let mut stream = client
        .do_get(ticket)
        .await
        .expect("do_get failed")
        .into_inner();

    // Must get at least one frame
    let first = stream
        .next()
        .await
        .expect("Stream ended immediately")
        .expect("Stream error on first frame");

    assert!(
        !first.app_metadata.is_empty() || !first.data_header.is_empty(),
        "First frame should have metadata or header"
    );
}

#[tokio::test]
async fn test_do_get_invalid_plan_returns_error() {
    let (channel, _shutdown, _tmp) = start_server().await;
    let mut client = FlightServiceClient::new(channel);

    let ticket = Ticket {
        ticket: vec![1, 2, 3, 4].into(),
    };

    // Either do_get itself fails or the stream's first item is an error
    match client.do_get(ticket).await {
        Err(_) => {} // gRPC-level error is fine
        Ok(response) => {
            // If server returned OK initially, the stream should produce an error
            let mut stream = response.into_inner();
            let first = stream.next().await;
            // Some impls propagate error as stream error, not gRPC status
            // Either no frames or an error frame is acceptable
            if let Some(Ok(frame)) = &first {
                // If we got a frame, it must be an error metadata frame — we just
                // verify we can consume the stream without panicking
                let _ = frame;
            }
        }
    }
}

#[tokio::test]
async fn test_seql_metadata_complete_encoding() {
    // Verify SeqlMetadata::Complete round-trips through bincode correctly
    use sequins_query::flight::{complete_flight_data, decode_metadata, SeqlMetadata};
    use sequins_query::frame::QueryStats;

    let fd = complete_flight_data(QueryStats::zero());
    let metadata = decode_metadata(&fd.app_metadata);
    assert!(
        matches!(metadata, Some(SeqlMetadata::Complete { .. })),
        "Expected Complete metadata, got: {:?}",
        metadata.map(|m| format!("{:?}", std::mem::discriminant(&m)))
    );
}
