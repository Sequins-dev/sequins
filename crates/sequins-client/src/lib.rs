//! sequins-client — Remote SeQL client (Arrow Flight SQL)
//!
//! Compiles SeQL to Substrait client-side and sends the plan to the server via
//! Arrow Flight SQL (`DoGet` with the raw plan bytes as a `Ticket`).
//! Returns a `SeqlStream` of `FlightData` for the caller to process.

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{
    CommandStatementQuery, CommandStatementSubstraitPlan, ProstMessageExt, SubstraitPlan,
};
use arrow_flight::{FlightDescriptor, Ticket};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use futures::StreamExt;
use prost::Message as _;
use seql_ast::ast::QueryMode;
use seql_substrait::{compile_ast_with_range, compile_with_range, schema_context};
use sequins_traits::QueryError;
use sequins_traits::{QueryApi, QueryExec, SeqlStream};
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
    /// Optional bearer token sent as `authorization: Bearer <token>` on every
    /// request. `None` = no auth header (the default; a keyless server ignores it).
    token: Option<String>,
    /// Base URL of the Pro management API (e.g. `http://host:8081`), used for the
    /// dashboard REST endpoints. `None` disables the remote dashboard interface.
    management_url: Option<String>,
    /// HTTP client for the management REST endpoints (dashboards).
    http: reqwest::Client,
}

impl RemoteClient {
    /// Create a new remote client pointing at `addr` (e.g. `"http://localhost:4319"`).
    ///
    /// Construction is synchronous and safe to call from non-async contexts
    /// (e.g. FFI).  The TCP connection is established on first use.
    pub fn new(addr: &str) -> Result<Self, ClientError> {
        Self::with_token(addr, None)
    }

    /// Like [`Self::new`], but attaches a bearer `token` (an API key) to every
    /// request. Pass `None` for no auth. Used to authenticate to a secured
    /// Sequins Pro daemon, including inter-node fan-out.
    pub fn with_token(addr: &str, token: Option<String>) -> Result<Self, ClientError> {
        let addr = addr.trim_end_matches('/').to_string();
        let endpoint = Channel::from_shared(addr)
            .map_err(|e| ClientError::Connect(format!("Invalid address: {e}")))?;

        let schema_ctx = schema_context().map_err(|e| ClientError::Query(e.to_string()))?;

        Ok(Self {
            endpoint,
            schema_ctx: Arc::new(schema_ctx),
            token,
            management_url: None,
            http: reqwest::Client::new(),
        })
    }

    /// Configure the Pro management API base URL (e.g. `http://host:8081`) so the
    /// dashboard REST endpoints work. Without it, [`DashboardApi`] calls error.
    pub fn with_management_url(mut self, url: Option<String>) -> Self {
        self.management_url = url.map(|u| u.trim_end_matches('/').to_string());
        self
    }

    /// Wrap `msg` in a request, adding the `authorization: Bearer <token>` header
    /// when a token is configured.
    fn authed<T>(&self, msg: T) -> Result<tonic::Request<T>, QueryError> {
        let mut req = tonic::Request::new(msg);
        if let Some(token) = &self.token {
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| QueryError::Execution {
                    message: "invalid API token (not a valid header value)".to_string(),
                })?;
            req.metadata_mut().insert("authorization", value);
        }
        Ok(req)
    }

    /// Execute a SeQL query in live streaming mode.
    ///
    /// Forces `QueryMode::Live` before compiling — the server will push
    /// incremental Append/Update/Expire/Replace/Heartbeat frames.
    pub async fn query_live(&self, seql: &str) -> Result<SeqlStream, QueryError> {
        self.query_live_with_range(seql, None).await
    }

    /// Live streaming with an optional structured time range that overrides the
    /// query's inline scope (for scope-less templates run against a picker range).
    pub async fn query_live_with_range(
        &self,
        seql: &str,
        time_range: Option<seql_ast::ast::TimeRange>,
    ) -> Result<SeqlStream, QueryError> {
        let mut ast = seql_parser::parse(seql).map_err(|e| QueryError::InvalidAst {
            message: format!("Parse error at offset {}: {}", e.offset, e.message),
        })?;
        ast.mode = QueryMode::Live;
        let plan_bytes = compile_ast_with_range(ast, time_range, &self.schema_ctx).await?;
        self.execute_plan(plan_bytes).await
    }

    /// Snapshot query with an optional structured time range (see above).
    pub async fn query_with_range(
        &self,
        seql: &str,
        time_range: Option<seql_ast::ast::TimeRange>,
    ) -> Result<SeqlStream, QueryError> {
        let plan_bytes = compile_with_range(seql, time_range, &self.schema_ctx).await?;
        self.execute_plan(plan_bytes).await
    }

    /// Create a client pointing at the default localhost Flight SQL address.
    pub fn localhost() -> Result<Self, ClientError> {
        Self::new("http://localhost:4319")
    }

    /// Execute a **read-only** plain SQL query over Flight SQL's standard
    /// `CommandStatementQuery` path and return the resulting `SeqlStream`.
    async fn execute_sql(&self, sql: String) -> Result<SeqlStream, QueryError> {
        let cmd = CommandStatementQuery {
            query: sql,
            transaction_id: None,
        };
        self.get_flight_info_and_do_get(cmd.as_any().encode_to_vec())
            .await
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
        let cmd = CommandStatementSubstraitPlan {
            plan: Some(SubstraitPlan {
                plan: plan_bytes.clone().into(),
                version: "0.20.0".to_string(),
            }),
            transaction_id: None,
        };
        self.get_flight_info_and_do_get(cmd.as_any().encode_to_vec())
            .await
    }

    /// Shared Flight SQL round-trip: `GetFlightInfo(cmd)` → first endpoint ticket →
    /// `DoGet(ticket)` → mapped `SeqlStream`.
    async fn get_flight_info_and_do_get(&self, cmd_any: Vec<u8>) -> Result<SeqlStream, QueryError> {
        // connect_lazy() must be called from an async context (needs a tokio reactor).
        // This is always called from an async context, so this is safe.
        let channel = self.endpoint.clone().connect_lazy();
        let mut client =
            FlightServiceClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);

        // Step 1: GetFlightInfo → get ticket
        let descriptor = FlightDescriptor::new_cmd(cmd_any);
        let info = client
            .get_flight_info(self.authed(descriptor)?)
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
            .do_get(self.authed(Ticket {
                ticket: ticket.ticket,
            })?)
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
        self.query_with_range(seql, None).await
    }

    async fn sql(&self, sql: &str) -> Result<SeqlStream, QueryError> {
        self.execute_sql(sql.to_string()).await
    }
}

#[async_trait]
impl QueryExec for RemoteClient {
    async fn execute(&self, plan_bytes: Vec<u8>) -> Result<SeqlStream, QueryError> {
        self.execute_plan(plan_bytes).await
    }
}

// ---- Remote dashboards (REST over the management API) ----------------------

impl RemoteClient {
    fn dashboards_url(&self, suffix: &str) -> sequins_metadata::Result<String> {
        let base = self.management_url.as_ref().ok_or_else(|| {
            sequins_metadata::MetadataError::Other(
                "remote management URL not configured".to_string(),
            )
        })?;
        Ok(format!("{base}/api/dashboards{suffix}"))
    }

    fn bearer(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }
}

fn meta_err<E: std::fmt::Display>(e: E) -> sequins_metadata::MetadataError {
    sequins_metadata::MetadataError::Other(e.to_string())
}

#[async_trait::async_trait]
impl sequins_metadata::DashboardApi for RemoteClient {
    async fn list_dashboards(&self) -> sequins_metadata::Result<Vec<sequins_metadata::Dashboard>> {
        let url = self.dashboards_url("")?;
        let resp = self
            .bearer(self.http.get(&url))
            .send()
            .await
            .map_err(meta_err)?
            .error_for_status()
            .map_err(meta_err)?;
        resp.json().await.map_err(meta_err)
    }

    async fn get_dashboard(
        &self,
        id: &str,
    ) -> sequins_metadata::Result<Option<sequins_metadata::Dashboard>> {
        let url = self.dashboards_url(&format!("/{id}"))?;
        let resp = self
            .bearer(self.http.get(&url))
            .send()
            .await
            .map_err(meta_err)?
            .error_for_status()
            .map_err(meta_err)?;
        resp.json().await.map_err(meta_err)
    }

    async fn save_dashboard(
        &self,
        dashboard: sequins_metadata::Dashboard,
    ) -> sequins_metadata::Result<sequins_metadata::Dashboard> {
        let url = self.dashboards_url("")?;
        let resp = self
            .bearer(self.http.post(&url).json(&dashboard))
            .send()
            .await
            .map_err(meta_err)?
            .error_for_status()
            .map_err(meta_err)?;
        resp.json().await.map_err(meta_err)
    }

    async fn delete_dashboard(&self, id: &str) -> sequins_metadata::Result<()> {
        let url = self.dashboards_url(&format!("/{id}"))?;
        self.bearer(self.http.delete(&url))
            .send()
            .await
            .map_err(meta_err)?
            .error_for_status()
            .map_err(meta_err)?;
        Ok(())
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
