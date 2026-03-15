//! sequins-server — Network servers for Sequins
//!
//! Provides:
//! - `OtlpServer` — OTLP/HTTP ingest server (port 4318)
//! - `ManagementServer` — HTTP management API (port 8081)
//! - `FlightSqlServer` — Arrow Flight SQL gRPC server (port 4319)

pub mod error;
pub mod flight;
pub mod management;
pub mod otlp;

pub use error::{Error, Result};
pub use flight::{flight_service_server, SequinsFlightSqlService};
pub use management::ManagementServer;
pub use otlp::OtlpServer;
