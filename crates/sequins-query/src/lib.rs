//! Compatibility facade for SeQL-to-Substrait compilation.
//!
//! Prefer using the owning crates directly:
//! - `seql-parser` for text parsing.
//! - `seql-ast` for AST and result schema types.
//! - `seql-substrait` for compilation.
//! - `sequins-flight` for Flight/Arrow stream framing.
//! - `sequins-traits` for query execution traits and errors.

mod compiler;

pub use compiler::{compile, compile_ast, schema_context};
