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

/// SeQL abstract syntax tree types (re-exported from seql-ast)
pub mod ast {
    pub use seql_ast::ast::*;
}
/// SeqlExtension protobuf types (re-exported from seql-substrait)
pub mod seql_ext {
    pub use seql_substrait::seql_ext::*;
}
/// SeQL to Substrait compiler (re-exported from seql-substrait)
pub mod compiler;
/// Correlation key tables for navigate/merge operations (re-exported from seql-ast)
pub mod correlation {
    pub use seql_ast::correlation::*;
}
/// Query errors and warning codes (re-exported from sequins-traits)
pub mod error {
    pub use sequins_traits::{QueryError, WarningCode};
}
/// Flight protocol helpers — SeqlMetadata, FlightData builders (re-exported from sequins-flight)
pub mod flight;
/// IPC helpers — batch_to_ipc, ipc_to_batch, QueryStats (re-exported from sequins-flight)
pub mod frame;
/// SeQL text parser (re-exported from seql-parser)
pub mod parser {
    pub use seql_parser::parse;
    pub use seql_parser::ParseError;
}
/// Frame reducer — converts FlightData streams into typed sink callbacks (re-exported from seql-substrait)
pub mod reducer;
/// Result schema and column type definitions (re-exported from seql-ast)
pub mod schema {
    pub use seql_ast::schema::*;
}

pub use ast::QueryAst;
pub use compiler::{ast_to_logical_plan, compile, compile_ast, schema_context};
pub use error::QueryError;

// Re-export traits and stream type from sequins-traits for backward compatibility
pub use sequins_traits::{QueryApi, QueryExec, SeqlStream};
