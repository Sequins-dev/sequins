//! Bridge between async ViewDeltaStream and the Relm4 component message system.
//!
//! `ComponentSender::input()` is thread-safe — it enqueues the message on
//! the GLib main loop. This lets a tokio background task safely drive Relm4
//! component updates.

use futures::StreamExt;
use relm4::{Component, ComponentSender};
use sequins_view::{ViewDelta, ViewDeltaStream};
use tokio::task::JoinHandle;

/// Bridge a `ViewDeltaStream` to a Relm4 component.
///
/// Spawns a tokio task that polls `stream` and calls
/// `sender.input(map_fn(delta))` for each delta. Abort the returned
/// `JoinHandle` to cancel the stream.
pub fn bridge_view_stream<M, F>(
    stream: ViewDeltaStream,
    sender: ComponentSender<M>,
    map_fn: F,
) -> JoinHandle<()>
where
    M: Component + 'static,
    M::Input: Send,
    F: Fn(ViewDelta) -> M::Input + Send + 'static,
{
    tokio::spawn(async move {
        futures::pin_mut!(stream);
        while let Some(delta) = stream.next().await {
            sender.input(map_fn(delta));
        }
        tracing::debug!("ViewDeltaStream ended");
    })
}
