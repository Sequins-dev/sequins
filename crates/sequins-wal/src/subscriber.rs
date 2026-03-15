use crate::entry::WalEntry;
use futures::stream::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// A subscriber to WAL entries from a given sequence
pub struct WalSubscriber {
    /// Starting sequence number
    start_seq: u64,
    /// Wrapped broadcast stream
    stream: BroadcastStream<Arc<WalEntry>>,
    /// Next expected sequence number
    next_seq: u64,
}

impl WalSubscriber {
    /// Create a new subscriber
    pub fn new(start_seq: u64, rx: broadcast::Receiver<Arc<WalEntry>>) -> Self {
        Self {
            start_seq,
            stream: BroadcastStream::new(rx),
            next_seq: start_seq,
        }
    }

    /// Get the starting sequence number
    pub fn start_seq(&self) -> u64 {
        self.start_seq
    }
}

impl Stream for WalSubscriber {
    type Item = Result<Arc<WalEntry>, tokio_stream::wrappers::errors::BroadcastStreamRecvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Poll the inner stream
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(entry))) => {
                    // Check if this entry is in our range
                    if entry.seq >= self.next_seq {
                        self.next_seq = entry.seq + 1;
                        return Poll::Ready(Some(Ok(entry)));
                    }
                    // Entry is before our start, loop to get next one
                }
                Poll::Ready(Some(Err(e))) => {
                    // Channel error (lagged)
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    // Stream ended
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    // No entries available yet
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::WalPayload;
    use futures::StreamExt;

    #[tokio::test]
    async fn subscriber_receives_entries() {
        let (tx, rx) = broadcast::channel(10);
        let mut subscriber = WalSubscriber::new(1, rx);

        // Send entries
        let entry1 = Arc::new(WalEntry::new(
            1,
            1000,
            WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ),
        ));
        let entry2 = Arc::new(WalEntry::new(
            2,
            2000,
            WalPayload::Logs(
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::default(
                ),
            ),
        ));

        tx.send(entry1.clone()).unwrap();
        tx.send(entry2.clone()).unwrap();

        // Receive entries
        let received1 = subscriber.next().await.unwrap().unwrap();
        assert_eq!(received1.seq, 1);

        let received2 = subscriber.next().await.unwrap().unwrap();
        assert_eq!(received2.seq, 2);
    }

    #[tokio::test]
    async fn subscriber_skips_old_entries() {
        let (tx, rx) = broadcast::channel(10);
        let mut subscriber = WalSubscriber::new(5, rx); // Start from seq 5

        // Send entries before start_seq
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        let entry1 = Arc::new(WalEntry::new(
            1,
            1000,
            WalPayload::Traces(ExportTraceServiceRequest::default()),
        ));
        let entry2 = Arc::new(WalEntry::new(
            5,
            5000,
            WalPayload::Traces(ExportTraceServiceRequest::default()),
        ));

        tx.send(entry1).unwrap();
        tx.send(entry2.clone()).unwrap();

        // Should only receive entry 5
        let received = subscriber.next().await.unwrap().unwrap();
        assert_eq!(received.seq, 5);
    }
}
