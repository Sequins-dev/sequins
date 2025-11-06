//! Protocol server implementations for Sequins
//!
//! This crate provides HTTP and gRPC server wrappers around the three core traits:
//! - `QueryServer` - HTTP REST API for queries (port 8080)
//! - `ManagementServer` - HTTP REST API for management (port 8081)
//! - `OtlpServer` - Full OTLP support with gRPC (port 4317) and HTTP (port 4318)
//!
//! These servers are **pure protocol adapters** with no business logic. All logic is
//! implemented in the trait implementations (typically `Storage` from `sequins-storage`).
//!
//! # Architecture
//!
//! The servers currently use trait objects for simplicity:
//! ```rust,ignore
//! QueryServer { query_api: Arc<dyn QueryApi> }
//! ```
//!
//! This may be optimized to use generics in the future for zero-cost abstractions.
//!
//! # Usage Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use sequins_storage::Storage;
//! use sequins_server::{QueryServer, ManagementServer, OtlpServer};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create storage
//! let storage = Arc::new(Storage::new(Default::default())?);
//!
//! // Start all three servers (generic over the same Storage instance)
//! let query_server = QueryServer::new(Arc::clone(&storage));
//! let mgmt_server = ManagementServer::new(Arc::clone(&storage));
//! let otlp_server = OtlpServer::new(Arc::clone(&storage));
//!
//! tokio::select! {
//!     _ = query_server.serve("0.0.0.0:8080") => {},
//!     _ = mgmt_server.serve("0.0.0.0:8081") => {},
//!     _ = otlp_server.serve("0.0.0.0:4317", "0.0.0.0:4318") => {},  // gRPC + HTTP
//! }
//! # Ok(())
//! # }
//! ```

mod error;
mod management;
mod otlp;
mod otlp_conversions;
mod query;

pub use error::{Error, Result};
pub use management::ManagementServer;
pub use otlp::OtlpServer;
pub use query::QueryServer;
