//! sequins-client — Remote SeQL client (Arrow Flight SQL)
//!
//! Compiles SeQL to Substrait client-side and sends the plan to the server via
//! Arrow Flight SQL (`DoGet` with the raw plan bytes as a `Ticket`).
//! Returns a `SeqlStream` of `FlightData` for the caller to process.

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementSubstraitPlan, ProstMessageExt, SubstraitPlan};
use arrow_flight::{FlightDescriptor, Ticket};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use futures::StreamExt;
use prost::Message as _;
use sequins_query::ast::QueryMode;
use sequins_query::error::QueryError;
use sequins_query::{compile, compile_ast, schema_context, QueryApi, QueryExec, SeqlStream};
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};

/// Remote SeQL client — compiles SeQL client-side and sends Substrait plan
/// bytes to the server via Arrow Flight SQL's `DoGet`.
pub struct RemoteClient {
    /// The target endpoint — stores the URI without opening a connection.
    /// The gRPC channel is created lazily inside `execute_plan()`, where a
    /// tokio reactor is always available.
    endpoint: Endpoint,
    schema_ctx: Arc<SessionContext>,
}

impl RemoteClient {
    /// Create a new remote client pointing at `addr` (e.g. `"http://localhost:4319"`).
    ///
    /// Construction is synchronous and safe to call from non-async contexts
    /// (e.g. FFI).  The TCP connection is established on first use.
    pub fn new(addr: &str) -> Result<Self, ClientError> {
        let addr = addr.trim_end_matches('/').to_string();
        let endpoint = Channel::from_shared(addr)
            .map_err(|e| ClientError::Connect(format!("Invalid address: {e}")))?;

        let schema_ctx = schema_context().map_err(|e| ClientError::Query(e.to_string()))?;

        Ok(Self {
            endpoint,
            schema_ctx: Arc::new(schema_ctx),
        })
    }

    /// Execute a SeQL query in live streaming mode.
    ///
    /// Forces `QueryMode::Live` before compiling — the server will push
    /// incremental Append/Update/Expire/Replace/Heartbeat frames.
    pub async fn query_live(&self, seql: &str) -> Result<SeqlStream, QueryError> {
        let mut ast = sequins_query::parser::parse(seql).map_err(|e| QueryError::InvalidAst {
            message: format!("Parse error at offset {}: {}", e.offset, e.message),
        })?;
        ast.mode = QueryMode::Live;
        let plan_bytes = compile_ast(ast, &self.schema_ctx).await?;
        self.execute_plan(plan_bytes).await
    }

    /// Create a client pointing at the default localhost Flight SQL address.
    pub fn localhost() -> Result<Self, ClientError> {
        Self::new("http://localhost:4319")
    }

    /// Send Substrait plan bytes to the server via `DoGet` and return the
    /// resulting `SeqlStream`.
    ///
    /// # Protocol
    ///
    /// 1. Call `GetFlightInfo` with a `CommandStatementSubstraitPlan` descriptor.
    /// 2. Extract the ticket from the first endpoint.
    /// 3. Call `DoGet(ticket)` to open the streaming result.
    async fn execute_plan(&self, plan_bytes: Vec<u8>) -> Result<SeqlStream, QueryError> {
        // connect_lazy() must be called from an async context (needs a tokio reactor).
        // execute_plan() is always called from an async context, so this is safe.
        let channel = self.endpoint.clone().connect_lazy();
        let mut client =
            FlightServiceClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);

        // Step 1: GetFlightInfo → get ticket
        let cmd = CommandStatementSubstraitPlan {
            plan: Some(SubstraitPlan {
                plan: plan_bytes.clone().into(),
                version: "0.20.0".to_string(),
            }),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let info = client
            .get_flight_info(descriptor)
            .await
            .map_err(|e| QueryError::Execution {
                message: format!("GetFlightInfo failed: {e}"),
            })?
            .into_inner();

        let ticket = info
            .endpoint
            .into_iter()
            .next()
            .and_then(|ep| ep.ticket)
            .ok_or_else(|| QueryError::Execution {
                message: "Server returned no endpoints".to_string(),
            })?;

        // Step 2: DoGet → streaming FlightData
        let streaming = client
            .do_get(Ticket {
                ticket: ticket.ticket,
            })
            .await
            .map_err(|e| QueryError::Execution {
                message: format!("DoGet failed: {e}"),
            })?
            .into_inner();

        // Map tonic Status → QueryError
        let stream = streaming.map(|r| {
            r.map_err(|e| QueryError::Execution {
                message: format!("Stream error: {e}"),
            })
        });

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl QueryApi for RemoteClient {
    async fn query(&self, seql: &str) -> Result<SeqlStream, QueryError> {
        let plan_bytes = compile(seql, &self.schema_ctx).await?;
        self.execute_plan(plan_bytes).await
    }
}

#[async_trait]
impl QueryExec for RemoteClient {
    async fn execute(&self, plan_bytes: Vec<u8>) -> Result<SeqlStream, QueryError> {
        self.execute_plan(plan_bytes).await
    }
}

/// Errors from the remote client
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    Connect(String),
    #[error("Query error: {0}")]
    Query(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_error_types() {
        let error = ClientError::Connect("Connection refused".to_string());
        assert_eq!(error.to_string(), "Connection error: Connection refused");

        let error = ClientError::Query("parse error".to_string());
        assert_eq!(error.to_string(), "Query error: parse error");
    }

    /// Verify that a URI with invalid characters (space) gives a ClientError.
    #[test]
    fn test_client_connect_invalid_url() {
        // Spaces are not valid in URIs — from_shared rejects them
        let result = RemoteClient::new("http://local host:4319");
        assert!(
            result.is_err(),
            "Expected error for URI with invalid characters"
        );
    }

    /// Verify that construction succeeds for valid URIs even with no server running,
    /// and that execution fails on unreachable servers.
    #[tokio::test]
    async fn test_client_execute_fails_on_unreachable_server() {
        // Port 1 is almost certainly not listening — endpoint is lazy, so new() succeeds
        let client = RemoteClient::new("http://localhost:1").expect("endpoint should construct");
        // Channel connects lazily; execute should fail when the gRPC call is made
        let result = client.execute_plan(vec![]).await;
        assert!(
            result.is_err(),
            "Expected error executing against unreachable server"
        );
    }
}
