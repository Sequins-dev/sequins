//! Trait implementations for DataFusionBackend

use crate::execution::execute_plan;
use crate::DataFusionBackend;
use async_trait::async_trait;
use seql_ast::ast::QueryMode;
use seql_substrait::{compile, compile_ast};
use sequins_traits::QueryError;
use sequins_traits::{QueryApi, QueryExec, SeqlStream};

impl DataFusionBackend {
    /// Execute a SeQL query in live streaming mode.
    pub async fn query_live(&self, seql: &str) -> Result<SeqlStream, QueryError> {
        let ctx = self.make_session_ctx().await?;
        let mut ast = seql_parser::parse(seql).map_err(|e| QueryError::InvalidAst {
            message: format!("Parse error at offset {}: {}", e.offset, e.message),
        })?;
        ast.mode = QueryMode::Live;
        let plan_bytes = compile_ast(ast, &ctx).await?;
        self.execute(plan_bytes).await
    }
}

#[async_trait]
impl QueryApi for DataFusionBackend {
    async fn query(&self, seql: &str) -> Result<SeqlStream, QueryError> {
        let ctx = self.make_session_ctx().await?;
        let plan_bytes = compile(seql, &ctx).await?;
        self.execute(plan_bytes).await
    }
}

#[async_trait]
impl QueryExec for DataFusionBackend {
    async fn execute(&self, plan_bytes: Vec<u8>) -> Result<SeqlStream, QueryError> {
        // The plan's scope selects which storage tiers to scan (hot / cold / both).
        let scope = crate::execution::decode_plan_scope(&plan_bytes);
        let storage = self.storage.clone();
        execute_plan(&storage, plan_bytes, self.session_ctx_for_scope(scope)).await
    }
}
