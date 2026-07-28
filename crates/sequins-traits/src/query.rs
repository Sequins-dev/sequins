use arrow_flight::FlightData;
use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;

/// Errors that can occur during SeQL query execution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Error)]
pub enum QueryError {
    /// A field referenced in the query does not exist on the signal
    #[error("unknown field: {field}")]
    UnknownField { field: String },
    /// The query AST is structurally invalid
    #[error("invalid AST: {message}")]
    InvalidAst { message: String },
    /// A pipeline stage is not supported by this backend
    #[error("unsupported stage: {stage}")]
    UnsupportedStage { stage: String },
    /// A resource limit was exceeded
    #[error("resource limit exceeded: {limit}")]
    ResourceLimit { limit: String },
    /// An error occurred during query execution
    #[error("execution error: {message}")]
    Execution { message: String },
}

/// Warning codes that may accompany query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WarningCode {
    /// The result set was truncated due to a row limit
    ResultTruncated,
    /// The query took longer than expected
    SlowQuery,
    /// The result is approximate (e.g. from sampling or HLL)
    ApproximateResult,
    /// Schema resolution fell back to a generic type
    SchemaResolutionFallback,
}

/// A pinned, boxed stream of Arrow Flight messages.
///
/// All query results — snapshot and live — are delivered as `FlightData` streams.
/// Each message carries `app_metadata` with a bincode-serialized `SeqlMetadata`
/// that identifies the message type and which table it belongs to.
pub type SeqlStream = Pin<Box<dyn Stream<Item = Result<FlightData, QueryError>> + Send + 'static>>;

/// Client-facing query API (compiles SeQL to Substrait and executes)
#[async_trait]
pub trait QueryApi: Send + Sync {
    /// Execute a SeQL query string and return a FlightData stream
    async fn query(&self, seql: &str) -> Result<SeqlStream, QueryError>;

    /// Execute a **read-only** plain SQL query (SELECT only) and return a framed
    /// `FlightData` stream (shape `Table`). Used to read the app-state tables
    /// (`conversations`/`messages`/`dashboards`) and other DataFusion tables that
    /// SeQL does not address. The default errors; backends that support SQL override it.
    async fn sql(&self, sql: &str) -> Result<SeqlStream, QueryError> {
        let _ = sql;
        Err(QueryError::UnsupportedStage {
            stage: "raw SQL".to_string(),
        })
    }
}

/// Server-side query executor (executes pre-compiled Substrait plans)
#[async_trait]
pub trait QueryExec: Send + Sync {
    /// Execute a Substrait Plan (with SeqlExtension) and return a FlightData stream
    async fn execute(&self, plan_bytes: Vec<u8>) -> Result<SeqlStream, QueryError>;

    /// Execute a **read-only** plain SQL query and return a framed `FlightData`
    /// stream. Backs the Flight SQL `CommandStatementQuery` path. The default
    /// errors; executors that support SQL override it.
    async fn execute_sql(&self, sql: String) -> Result<SeqlStream, QueryError> {
        let _ = sql;
        Err(QueryError::UnsupportedStage {
            stage: "raw SQL".to_string(),
        })
    }
}
