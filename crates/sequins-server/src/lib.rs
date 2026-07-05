//! sequins-server — OTLP ingest server for Sequins
//!
//! Provides:
//! - `OtlpServer` — OTLP/gRPC (port 4317) + OTLP/HTTP (port 4318) ingest receiver
//!
//! This crate deliberately contains **only** the OTLP ingest surface. The
//! embedded/desktop app (via `sequins-ffi`) starts this server and nothing else;
//! it queries the engine in-process. The query-serving (Arrow Flight SQL) and
//! management HTTP servers live in the production distribution (`sequins-pro`).

pub mod error;
pub mod otlp;

pub use error::{Error, Result};
pub use otlp::OtlpServer;
