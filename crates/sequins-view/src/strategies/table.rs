//! TableStrategy and AggregateStrategy — maps raw SeQL deltas to table-level ViewDeltas.
//!
//! Both strategies share the same stream scaffold; they differ only in how
//! Data/Append/Replace frames are routed:
//!
//! - `RoutingMode::AppendBased` (TableStrategy): Append/Data → RowsAppended,
//!   Expire → RowsExpired, Replace → TableReplaced.
//! - `RoutingMode::ReplaceBased` (AggregateStrategy): every data frame → TableReplaced,
//!   Expire ignored.

use crate::delta::ViewDelta;
use crate::strategy::{ViewDeltaStream, ViewStrategy};
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use sequins_flight::{decode_metadata, SeqlMetadata};
use sequins_traits::SeqlStream;

// ── Routing mode ──────────────────────────────────────────────────────────────

/// Controls how `Data`, `Append`, `Replace`, and `Expire` frames are translated.
#[derive(Clone, Copy, Debug)]
enum RoutingMode {
    /// Append/Data → RowsAppended; Expire → RowsExpired; Replace → TableReplaced.
    /// Used for logs, spans, and any flat-list signal with row-level identity.
    AppendBased,
    /// All data frames → TableReplaced; Expire ignored.
    /// Used for health and aggregate signals where the full result set is replaced on each update.
    ReplaceBased,
}

// ── Shared strategy implementation ───────────────────────────────────────────

struct TableStrategyImpl {
    mode: RoutingMode,
}

#[async_trait]
impl ViewStrategy for TableStrategyImpl {
    async fn transform(&self, mut stream: SeqlStream) -> ViewDeltaStream {
        let mode = self.mode;

        Box::pin(stream! {
            let mut ready_sent = false;

            while let Some(result) = stream.next().await {
                let fd = match result {
                    Ok(fd) => fd,
                    Err(e) => {
                        yield ViewDelta::Error { message: e.to_string() };
                        return;
                    }
                };

                let metadata = match decode_metadata(&fd.app_metadata) {
                    Some(m) => m,
                    None => continue,
                };

                match metadata {
                    SeqlMetadata::Data { table } => {
                        if !fd.data_body.is_empty() {
                            yield match mode {
                                RoutingMode::AppendBased => ViewDelta::RowsAppended {
                                    table,
                                    ipc: fd.data_body.to_vec(),
                                },
                                RoutingMode::ReplaceBased => ViewDelta::TableReplaced {
                                    table,
                                    ipc: fd.data_body.to_vec(),
                                },
                            };
                        }
                    }
                    SeqlMetadata::Append { table, .. } => {
                        if !fd.data_body.is_empty() {
                            yield match mode {
                                RoutingMode::AppendBased => ViewDelta::RowsAppended {
                                    table,
                                    ipc: fd.data_body.to_vec(),
                                },
                                RoutingMode::ReplaceBased => ViewDelta::TableReplaced {
                                    table,
                                    ipc: fd.data_body.to_vec(),
                                },
                            };
                        }
                    }
                    SeqlMetadata::Replace { table, .. } => {
                        if !fd.data_body.is_empty() {
                            // Both modes produce TableReplaced for an explicit Replace frame.
                            yield ViewDelta::TableReplaced {
                                table,
                                ipc: fd.data_body.to_vec(),
                            };
                        }
                    }
                    SeqlMetadata::Expire { table, .. } => {
                        if matches!(mode, RoutingMode::AppendBased) {
                            yield ViewDelta::RowsExpired {
                                table,
                                expired_count: 1,
                            };
                        }
                    }
                    SeqlMetadata::Complete { .. } => {
                        yield ViewDelta::Ready;
                        return;
                    }
                    SeqlMetadata::Heartbeat { watermark_ns } => {
                        #[allow(unused_assignments)]
                        if !ready_sent {
                            ready_sent = true;
                            yield ViewDelta::Ready;
                        }
                        yield ViewDelta::Heartbeat { watermark_ns };
                    }
                    SeqlMetadata::Warning { code, message } => {
                        yield ViewDelta::Warning { code, message };
                    }
                    SeqlMetadata::Schema { .. } | SeqlMetadata::Update { .. } => {
                        // Schema frames are informational; handled implicitly by IPC.
                        // Update deltas are not used by table/aggregate strategies.
                    }
                }
            }
        })
    }
}

// ── Public strategy types ─────────────────────────────────────────────────────

/// Routes table-shaped SeQL deltas to [`ViewDelta`] table operations.
///
/// Suitable for logs, spans, and any signal where the frontend renders a flat
/// list of rows with row-level identity.
pub struct TableStrategy;

impl TableStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TableStrategy {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ViewStrategy for TableStrategy {
    async fn transform(&self, stream: SeqlStream) -> ViewDeltaStream {
        TableStrategyImpl {
            mode: RoutingMode::AppendBased,
        }
        .transform(stream)
        .await
    }
}

/// Routes aggregate SeQL deltas to [`ViewDelta::TableReplaced`].
///
/// Suitable for health checks and single-value aggregation results where
/// the entire result set is replaced on each heartbeat.
pub struct AggregateStrategy;

impl AggregateStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AggregateStrategy {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ViewStrategy for AggregateStrategy {
    async fn transform(&self, stream: SeqlStream) -> ViewDeltaStream {
        TableStrategyImpl {
            mode: RoutingMode::ReplaceBased,
        }
        .transform(stream)
        .await
    }
}
