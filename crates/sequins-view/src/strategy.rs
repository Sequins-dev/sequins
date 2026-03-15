//! ViewStrategy — transforms a raw SeqlStream into a ViewDeltaStream.

use crate::delta::ViewDelta;
use async_trait::async_trait;
use futures::Stream;
use sequins_query::SeqlStream;
use std::pin::Pin;

/// A pinned, boxed stream of [`ViewDelta`] items.
///
/// Emitted by [`ViewStrategy::transform`] and polled by the FFI layer, which
/// delivers batches of deltas to the C callback.
pub type ViewDeltaStream = Pin<Box<dyn Stream<Item = ViewDelta> + Send + 'static>>;

/// Transforms a raw [`SeqlStream`] into a [`ViewDeltaStream`].
///
/// Each implementation owns all domain logic for its signal type:
/// table routing, entity management, binning, expiry. Frontends receive
/// only targeted, ready-to-apply deltas.
#[async_trait]
pub trait ViewStrategy: Send + Sync {
    /// Consume the raw flight stream and produce a reactive delta stream.
    ///
    /// The returned stream is polled by the FFI layer. Each item is a single
    /// [`ViewDelta`] that the frontend applies atomically.
    async fn transform(&self, stream: SeqlStream) -> ViewDeltaStream;
}
