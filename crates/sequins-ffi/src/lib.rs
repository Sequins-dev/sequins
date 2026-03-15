//! sequins-ffi — C FFI for Swift/macOS embedding
//!
//! Provides C-compatible functions for:
//! - `DataSource` lifecycle (local storage or remote client)
//! - SeQL query parsing and execution via vtable callbacks
//! - Management API operations

#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod compat;
pub mod data_source;
pub mod logging;
pub mod management;
mod runtime;
pub mod seql;
pub mod types;

pub use data_source::*;
pub use management::*;
pub use runtime::RUNTIME;
pub use seql::*;
pub use types::*;
