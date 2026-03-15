//! ViewDelta — the reactive update unit delivered to frontends.
//!
//! Each delta targets a single entity or table and carries only what changed.
//! Rust owns all domain logic (routing, joins, binning, expiry).
//! Frontends are dumb delta appliers.

/// A single reactive update emitted by a [`ViewStrategy`].
///
/// Deltas are batched and delivered to the FFI callback together to reduce
/// round-trip overhead. Frontends apply each delta to their `@Observable` graph.
#[derive(Debug)]
pub enum ViewDelta {
    // ── Table-level (logs, spans, health) ────────────────────────────────────
    /// New rows appended to a table (historical Data frame or live Append).
    RowsAppended {
        /// `None` = primary table; `Some(alias)` = auxiliary table.
        table: Option<String>,
        /// Arrow IPC bytes carrying the new rows.
        ipc: Vec<u8>,
    },

    /// Rows have expired from the live time window.
    RowsExpired {
        /// `None` = primary table; `Some(alias)` = auxiliary table.
        table: Option<String>,
        /// Number of rows that left the window.
        expired_count: u64,
    },

    /// The entire table was replaced (e.g. aggregation refresh).
    TableReplaced {
        /// `None` = primary table; `Some(alias)` = auxiliary table.
        table: Option<String>,
        /// Arrow IPC bytes carrying the replacement rows.
        ipc: Vec<u8>,
    },

    // ── Entity-level (metrics, flamegraph nodes) ──────────────────────────────
    /// A new entity was created.
    ///
    /// `descriptor_ipc` carries the immutable metadata (function name, depth, etc.).
    /// `data_ipc` carries the initial mutable values (total_value, self_value, etc.).
    EntityCreated {
        /// Unique entity key (e.g. path_key for flamegraph, metric_id for metrics).
        key: String,
        /// Arrow IPC bytes — immutable metadata schema.
        descriptor_ipc: Vec<u8>,
        /// Arrow IPC bytes — mutable values schema.
        data_ipc: Vec<u8>,
    },

    /// An existing entity's mutable data changed.
    ///
    /// Only the changed mutable fields are included. The frontend MUST use
    /// `if x != newX { x = newX }` guards to avoid spurious re-renders.
    EntityDataReplaced {
        /// Entity key.
        key: String,
        /// Arrow IPC bytes — mutable values schema.
        data_ipc: Vec<u8>,
    },

    /// An entity was removed (e.g. all samples expired, metric retired).
    EntityRemoved {
        /// Entity key.
        key: String,
    },

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    /// Initial data load complete. Live updates are now streaming.
    Ready,

    /// Periodic keepalive advancing the time watermark.
    Heartbeat {
        /// Current time watermark in nanoseconds since epoch.
        watermark_ns: u64,
    },

    /// A non-fatal warning occurred.
    Warning { code: u32, message: String },

    /// A fatal error terminated the stream.
    Error { message: String },
}
