//! MCP server surface — exposes the **same** [`crate::tools`] operation layer over
//! the Model Context Protocol (streamable HTTP), so bring-your-own-model clients
//! (Claude Desktop, Cursor, …) can drive the Sequins tools with their own model.
//!
//! Tool metadata and dispatch both come from [`crate::tools::registry`], so the MCP
//! surface can never drift from what the in-process Rig agent exposes.

use std::sync::Arc;

use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, InitializeResult, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::ErrorData as McpError;

use crate::tools::{registry, Tools};

/// An MCP [`ServerHandler`] backed by the Sequins tool operation layer.
#[derive(Clone)]
pub struct SequinsMcpServer {
    tools: Tools,
}

impl SequinsMcpServer {
    /// Build the MCP handler over the given tools.
    pub fn new(tools: Tools) -> Self {
        Self { tools }
    }
}

impl ServerHandler for SequinsMcpServer {
    fn get_info(&self) -> ServerInfo {
        InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions(
            "Sequins telemetry tools: explore signal tables (spans, logs, metrics, profiles) and \
             run read-only SQL or SeQL over your observability data.",
        )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let tools = registry::specs()
            .into_iter()
            .map(|spec| {
                let schema = spec.parameters.as_object().cloned().unwrap_or_default();
                Tool::new(spec.name, spec.description, Arc::new(schema))
            })
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let name = request.name.to_string();
        let args = request
            .arguments
            .map(serde_json::Value::Object)
            .unwrap_or(serde_json::Value::Null);

        // A tool-level failure is returned as a readable error result (not a
        // protocol error), so the caller's model sees the message and can retry.
        match registry::invoke(&self.tools, &name, args).await {
            Ok(text) => Ok(CallToolResult::success(vec![Content::text(text)])),
            Err(message) => Ok(CallToolResult::error(vec![Content::text(message)])),
        }
    }
}

/// The MCP streamable-HTTP tower service over the Sequins tools. Mount it into an
/// HTTP server (the daemon nests it under a path); it handles the MCP protocol.
pub fn mcp_service(tools: Tools) -> StreamableHttpService<SequinsMcpServer, LocalSessionManager> {
    StreamableHttpService::new(
        move || Ok(SequinsMcpServer::new(tools.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    )
}

/// An axum router serving the MCP endpoint under `/mcp`.
pub fn mcp_router(tools: Tools) -> axum::Router {
    axum::Router::new().nest_service("/mcp", mcp_service(tools))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_datafusion_backend::DataFusionBackend;
    use sequins_storage::test_fixtures::{make_test_otlp_traces, TestStorageBuilder};
    use sequins_traits::OtlpIngest;

    async fn seeded_server() -> (SequinsMcpServer, tempfile::TempDir) {
        let (storage, temp) = TestStorageBuilder::new().build().await;
        storage
            .ingest_traces(make_test_otlp_traces(1, 4))
            .await
            .unwrap();
        let backend = Arc::new(DataFusionBackend::new(Arc::new(storage)));
        (SequinsMcpServer::new(Tools::new(backend)), temp)
    }

    #[test]
    fn tools_list_matches_registry() {
        // The MCP tool list is built from the same registry the agent uses.
        let names: Vec<&str> = registry::specs().iter().map(|s| s.name).collect();
        assert!(names.contains(&"run_sql"));
        assert!(names.contains(&"run_seql"));
        assert_eq!(names.len(), registry::TOOL_NAMES.len());
    }

    #[tokio::test]
    async fn call_tool_run_sql_hits_ops() {
        let (server, _t) = seeded_server().await;
        let mut args = serde_json::Map::new();
        args.insert(
            "sql".to_string(),
            serde_json::Value::String("SELECT count(*) AS c FROM spans".to_string()),
        );
        let result = registry::invoke(&server.tools, "run_sql", serde_json::Value::Object(args))
            .await
            .unwrap();
        assert!(result.contains("row(s)"));
    }
}
