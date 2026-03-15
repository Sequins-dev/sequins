//! AggregateStrategy — maps SeQL deltas to full-replace table ViewDeltas.
//!
//! Used for health and other signals where the result is a single aggregate
//! batch that fully replaces the previous state on each update.

use crate::delta::ViewDelta;
use crate::strategy::{ViewDeltaStream, ViewStrategy};
use async_stream::stream;
use async_trait::async_trait;
use futures::StreamExt;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::SeqlStream;

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
                            yield ViewDelta::TableReplaced {
                                table,
                                ipc: fd.data_body.to_vec(),
                            };
                        }
                    }
                    SeqlMetadata::Replace { table, .. } | SeqlMetadata::Append { table, .. } => {
                        if !fd.data_body.is_empty() {
                            yield ViewDelta::TableReplaced {
                                table,
                                ipc: fd.data_body.to_vec(),
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
                    SeqlMetadata::Schema { .. }
                    | SeqlMetadata::Update { .. }
                    | SeqlMetadata::Expire { .. } => {}
                }
            }
        })
    }
}
