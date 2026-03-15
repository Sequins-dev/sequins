use crate::payload::WalPayload;
use crate::signal::WalSignal;
use serde::{Deserialize, Serialize};

/// A single WAL entry with monotonic sequence number
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonic sequence number (becomes RAFT commit index in multi-node)
    pub seq: u64,
    /// Timestamp when this entry was written (nanoseconds since epoch)
    pub timestamp_ns: u64,
    /// The ingested data payload
    pub payload: WalPayload,
}

impl WalEntry {
    /// Create a new WAL entry
    pub fn new(seq: u64, timestamp_ns: u64, payload: WalPayload) -> Self {
        Self {
            seq,
            timestamp_ns,
            payload,
        }
    }

    /// Get the signal type for this entry
    pub fn signal(&self) -> WalSignal {
        self.payload.signal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_signal() {
        let entry = WalEntry::new(
            1,
            1000,
            WalPayload::Traces(
                opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::default(),
            ),
        );
        assert_eq!(entry.signal(), WalSignal::Traces);
    }

    #[test]
    fn entry_serialize_roundtrip() {
        let entry = WalEntry::new(
            42,
            2000,
            WalPayload::Logs(
                opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::default(
                ),
            ),
        );
        let bytes = bincode::serialize(&entry).unwrap();
        let decoded: WalEntry = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.timestamp_ns, 2000);
    }
}
