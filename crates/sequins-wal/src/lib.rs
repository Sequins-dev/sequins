//! Write-Ahead Log (WAL) for durable, ordered ingestion
//!
//! The WAL provides:
//! - Monotonic sequence numbers for all ingested data
//! - Durable append-only storage backed by object_store
//! - Live broadcast to subscribers for real-time queries
//! - Segment-based compaction for cleanup
//!
//! Architecture:
//! - `WalEntry`: Single entry with seq, timestamp, payload
//! - `WalPayload`: Discriminated union of signal types
//! - `WalSegment`: Append-only file with length-prefixed entries
//! - `WalWriter`: Buffered writer with automatic rotation
//! - `WalSubscriber`: Stream for live broadcast subscription
//! - `Wal`: Main coordinator for append/subscribe/compact
//! - `WalSignal`: WAL-local signal kind (no DataFusion dependency)

mod coordinator;
mod entry;
mod error;
mod payload;
mod segment;
mod signal;
mod subscriber;
mod writer;

pub use coordinator::{Wal, WalConfig};
pub use entry::WalEntry;
pub use error::{Result, WalError};
pub use payload::WalPayload;
pub use segment::{WalSegment, WalSegmentMeta};
pub use signal::WalSignal;
pub use subscriber::WalSubscriber;
pub use writer::{WalWriter, WriterConfig};
