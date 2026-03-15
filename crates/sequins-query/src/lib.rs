//! sequins-query — SeQL unified pipeline query language
//!
//! Provides SeQL parser, AST, Substrait compiler, and query execution traits.
//!
//! # Architecture
//!
//! ```text
//! Client:
//!   SeQL text ──► compile() ──► Substrait Plan bytes ──► Flight SQL CommandStatementSubstraitPlan
//!
//! Server:
//!   Substrait Plan bytes ──► QueryExec::execute() ──► SeqlStream<FlightData>
//! ```

/// SeQL abstract syntax tree types
pub mod ast;
/// SeqlExtension protobuf types (generated from proto/seql_extension.proto)
pub mod seql_ext {
    include!(concat!(env!("OUT_DIR"), "/seql_extension.rs"));
}
/// SeQL to Substrait compiler
pub mod compiler;
/// Correlation key tables for navigate/merge operations
pub mod correlation;
/// Query errors and warning codes
pub mod error;
/// Flight protocol helpers — SeqlMetadata, FlightData builders
pub mod flight;
/// IPC helpers — batch_to_ipc, ipc_to_batch, QueryStats
pub mod frame;
/// SeQL text parser
pub mod parser;
/// Frame reducer — converts FlightData streams into typed sink callbacks
pub mod reducer;
/// Result schema and column type definitions
pub mod schema;

pub use ast::QueryAst;
pub use compiler::{ast_to_logical_plan, compile, compile_ast, schema_context};
pub use error::QueryError;

use arrow_flight::FlightData;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;

/// A pinned, boxed stream of Arrow Flight messages
///
/// All query results — snapshot and live — are delivered as `FlightData` streams.
/// Each message carries `app_metadata` with a bincode-serialized `SeqlMetadata`
/// that identifies the message type and which table it belongs to.
pub type SeqlStream = Pin<Box<dyn Stream<Item = Result<FlightData, QueryError>> + Send + 'static>>;

/// Client-facing query API (compiles SeQL to Substrait and executes)
#[async_trait]
pub trait QueryApi: Send + Sync {
    /// Execute a SeQL query string and return a FlightData stream
    ///
    /// Compiles the SeQL text to a multi-root Substrait plan (with SeqlExtension
    /// embedded in `advanced_extensions.enhancement`) and dispatches to the backend.
    async fn query(&self, seql: &str) -> Result<SeqlStream, QueryError>;
}

/// Server-side query executor (executes pre-compiled Substrait plans)
#[async_trait]
pub trait QueryExec: Send + Sync {
    /// Execute a Substrait Plan (with SeqlExtension) and return a FlightData stream
    ///
    /// `plan_bytes` is a serialized Substrait `Plan` protobuf containing:
    /// - `relations[0]`: primary query (with Navigate compiled as LeftSemi JOINs)
    /// - `relations[1..N]`: auxiliary tables from Merge stages
    /// - `advanced_extensions.enhancement`: `SeqlExtension` protobuf Any
    async fn execute(&self, plan_bytes: Vec<u8>) -> Result<SeqlStream, QueryError>;
}
