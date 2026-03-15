//! TableStrategy — maps raw SeQL deltas to table-level ViewDeltas.
//!
//! Used for logs and spans. Each Append/Data frame becomes `RowsAppended`,
//! each Expire becomes `RowsExpired`, each Replace becomes `TableReplaced`.

use crate::delta::ViewDelta;
use crate::strategy::{ViewDeltaStream, ViewStrategy};
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::SeqlStream;

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
    async fn transform(&self, mut stream: SeqlStream) -> ViewDeltaStream {
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
                            yield ViewDelta::RowsAppended {
                                table,
                                ipc: fd.data_body.to_vec(),
                            };
                        }
                    }
                    SeqlMetadata::Append { table, .. } => {
                        if !fd.data_body.is_empty() {
                            yield ViewDelta::RowsAppended {
                                table,
                                ipc: fd.data_body.to_vec(),
                            };
                        }
                    }
                    SeqlMetadata::Expire { table, .. } => {
                        yield ViewDelta::RowsExpired {
                            table,
                            expired_count: 1,
                        };
                    }
                    SeqlMetadata::Replace { table, .. } => {
                        if !fd.data_body.is_empty() {
                            yield ViewDelta::TableReplaced {
                                table,
                                ipc: fd.data_body.to_vec(),
                            };
                        }
                    }
                    SeqlMetadata::Complete { .. } => {
                        // Snapshot queries end with Complete; emit Ready then stop.
                        yield ViewDelta::Ready;
                        return;
                    }
                    SeqlMetadata::Heartbeat { watermark_ns } => {
                        // Live queries: emit Ready on the first heartbeat only.
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
                        // Update deltas are not used by TableStrategy.
                    }
                }
            }
        })
    }
}
