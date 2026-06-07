//! Live query subscription management and heartbeat streaming.
//!
//! # Architecture
//!
//! Live query execution runs entirely inside `sequins-storage`:
//! `DataFusionBackend::execute()` → `execute_live()` uses an explicit
//! `tokio::select!` loop over the WAL broadcast channel.  This crate owns
//! only the pieces that are logically separate from query execution:
//!
//! - `LiveQueryManager` — subscription accounting and resource limits.
//! - `HeartbeatEmitter` — periodic WAL-watermark FlightData messages.

pub mod error;
pub mod heartbeat;
pub mod manager;

pub use error::{Error, Result};
pub use heartbeat::HeartbeatEmitter;
pub use manager::{LiveQueryConfig, LiveQueryManager, SubscriptionGuard};
