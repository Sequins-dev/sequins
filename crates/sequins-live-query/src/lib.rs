//! Live query orchestration
//!
//! This crate provides live query infrastructure including:
//! - `LiveQueryManager` — subscription accounting and resource limiting
//! - `LiveTableProvider` / `LiveSourceExec` — DataFusion table provider backed by
//!   the WAL broadcast channel so DataFusion reads new rows in real time
//! - `LiveStreamWrapper` — wraps a `SendableRecordBatchStream` and produces
//!   `SeqlStream` (FlightData append messages + heartbeats)
//! - Supporting types: `HeartbeatEmitter`, `DeltaEmitter`
//!
//! # Architecture
//!
//! ```text
//! Client:
//!   SeQL query (live mode)
//!     ↓
//! DataFusionBackend::execute(plan)
//!     ↓
//! execute_live(storage, plan, ctx):
//!   - Overrides signal table with LiveTableProvider (reads from WAL broadcast)
//!   - Decodes Substrait → LogicalPlan, strips Limit node
//!   - Executes plan → SendableRecordBatchStream
//!   - Wraps with LiveStreamWrapper → SeqlStream<FlightData>
//!     ↓
//! SeqlStream<FlightData>:
//!   - Append FlightData (new rows as Arrow IPC batches arrive from WAL)
//!   - Heartbeat FlightData (keepalive / watermark)
//! ```

pub mod delta_emitter;
pub mod error;
pub mod heartbeat;
pub mod live_source_exec;
pub mod live_stream_wrapper;
pub mod live_table_provider;
pub mod manager;

pub use delta_emitter::DeltaEmitter;
pub use error::{Error, Result};
pub use heartbeat::HeartbeatEmitter;
pub use live_source_exec::LiveSourceExec;
pub use live_stream_wrapper::LiveStreamWrapper;
pub use live_table_provider::LiveTableProvider;
pub use manager::{LiveQueryConfig, LiveQueryManager, SubscriptionGuard};
