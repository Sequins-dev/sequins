//! Trait implementations for DataFusionBackend

use super::execution::execute_plan;
use super::DataFusionBackend;
use async_trait::async_trait;
use sequins_query::ast::QueryMode;
use sequins_query::error::QueryError;
use sequins_query::{compile, compile_ast, QueryApi, QueryExec, SeqlStream};

impl DataFusionBackend {
    /// Execute a SeQL query in live streaming mode.
    pub async fn query_live(&self, seql: &str) -> Result<SeqlStream, QueryError> {
        let ctx = self.make_session_ctx().await?;
        let mut ast = sequins_query::parser::parse(seql).map_err(|e| QueryError::InvalidAst {
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
        let storage = self.storage.clone();
        execute_plan(&storage, plan_bytes, self.make_session_ctx()).await
    }
}
