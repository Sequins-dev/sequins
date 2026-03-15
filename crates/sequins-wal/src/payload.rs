use crate::signal::WalSignal;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    profiles::v1development::ExportProfilesServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use prost::Message;
use serde::de::{self, Deserializer, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// Discriminated union of all ingestable data types
///
/// Now stores OTLP proto types instead of domain types.
/// Uses custom serde implementation with protobuf encoding to avoid bincode size issues.
#[derive(Debug, Clone)]
pub enum WalPayload {
    /// OTLP trace service request
    Traces(ExportTraceServiceRequest),
    /// OTLP logs service request
    Logs(ExportLogsServiceRequest),
    /// OTLP metrics service request
    Metrics(ExportMetricsServiceRequest),
    /// OTLP profiles service request
    Profiles(ExportProfilesServiceRequest),
}

// Custom Serialize implementation using protobuf encoding
impl Serialize for WalPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeTuple;

        match self {
            WalPayload::Traces(req) => {
                let bytes = req.encode_to_vec();
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&0u8)?; // variant tag
                tuple.serialize_element(&bytes)?;
                tuple.end()
            }
            WalPayload::Logs(req) => {
                let bytes = req.encode_to_vec();
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&1u8)?;
                tuple.serialize_element(&bytes)?;
                tuple.end()
            }
            WalPayload::Metrics(req) => {
                let bytes = req.encode_to_vec();
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&2u8)?;
                tuple.serialize_element(&bytes)?;
                tuple.end()
            }
            WalPayload::Profiles(req) => {
                let bytes = req.encode_to_vec();
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&3u8)?;
                tuple.serialize_element(&bytes)?;
                tuple.end()
            }
        }
    }
}

// Custom Deserialize implementation using protobuf decoding
impl<'de> Deserialize<'de> for WalPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WalPayloadVisitor;

        impl<'de> Visitor<'de> for WalPayloadVisitor {
            type Value = WalPayload;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a WalPayload tuple")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let tag: u8 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let bytes: Vec<u8> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                match tag {
                    0 => {
                        let req = ExportTraceServiceRequest::decode(&bytes[..])
                            .map_err(de::Error::custom)?;
                        Ok(WalPayload::Traces(req))
                    }
                    1 => {
                        let req = ExportLogsServiceRequest::decode(&bytes[..])
                            .map_err(de::Error::custom)?;
                        Ok(WalPayload::Logs(req))
                    }
                    2 => {
                        let req = ExportMetricsServiceRequest::decode(&bytes[..])
                            .map_err(de::Error::custom)?;
                        Ok(WalPayload::Metrics(req))
                    }
                    3 => {
                        let req = ExportProfilesServiceRequest::decode(&bytes[..])
                            .map_err(de::Error::custom)?;
                        Ok(WalPayload::Profiles(req))
                    }
                    _ => Err(de::Error::custom(format!(
                        "Unknown WalPayload tag: {}",
                        tag
                    ))),
                }
            }
        }

        deserializer.deserialize_tuple(2, WalPayloadVisitor)
    }
}

impl WalPayload {
    /// Get the signal type for this payload
    pub fn signal(&self) -> WalSignal {
        match self {
            Self::Traces(_) => WalSignal::Traces,
            Self::Logs(_) => WalSignal::Logs,
            Self::Metrics(_) => WalSignal::Metrics,
            Self::Profiles(_) => WalSignal::Profiles,
        }
    }

    /// Get the approximate number of items in this payload
    ///
    /// Note: For metrics, this counts resource_metrics entries, not individual data points.
    pub fn len(&self) -> usize {
        match self {
            Self::Traces(req) => req.resource_spans.len(),
            Self::Logs(req) => req.resource_logs.len(),
            Self::Metrics(req) => req.resource_metrics.len(),
            Self::Profiles(req) => req.resource_profiles.len(),
        }
    }

    /// Returns true if the payload is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_signal() {
        let traces = WalPayload::Traces(ExportTraceServiceRequest::default());
        let logs = WalPayload::Logs(ExportLogsServiceRequest::default());
        let metrics = WalPayload::Metrics(ExportMetricsServiceRequest::default());
        let profiles = WalPayload::Profiles(ExportProfilesServiceRequest::default());

        assert_eq!(traces.signal(), WalSignal::Traces);
        assert_eq!(logs.signal(), WalSignal::Logs);
        assert_eq!(metrics.signal(), WalSignal::Metrics);
        assert_eq!(profiles.signal(), WalSignal::Profiles);
    }

    #[test]
    fn payload_len() {
        let empty = WalPayload::Traces(ExportTraceServiceRequest::default());
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn payload_serialize_roundtrip() {
        let payload = WalPayload::Logs(ExportLogsServiceRequest::default());
        let bytes = bincode::serialize(&payload).unwrap();
        let decoded: WalPayload = bincode::deserialize(&bytes).unwrap();
        assert!(matches!(decoded, WalPayload::Logs(_)));
    }

    #[test]
    fn test_all_payload_variants_roundtrip() {
        let payloads: Vec<WalPayload> = vec![
            WalPayload::Traces(ExportTraceServiceRequest::default()),
            WalPayload::Logs(ExportLogsServiceRequest::default()),
            WalPayload::Metrics(
                opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::default(),
            ),
            WalPayload::Profiles(
                opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest::default(),
            ),
        ];

        for payload in payloads {
            let signal = payload.signal();
            let bytes = bincode::serialize(&payload).unwrap();
            assert!(!bytes.is_empty(), "serialized bytes must not be empty");
            let decoded: WalPayload = bincode::deserialize(&bytes).unwrap();
            assert_eq!(
                decoded.signal(),
                signal,
                "roundtrip must preserve signal type"
            );
        }
    }

    #[test]
    fn test_invalid_payload_tag_produces_error() {
        // Manually craft a (tag=99, payload_bytes) tuple and verify deserialization fails.
        // Tag 99 is unknown → should produce a descriptive error.
        let bad_bytes: Vec<u8> = bincode::serialize(&(99u8, Vec::<u8>::new())).unwrap();
        let result: Result<WalPayload, _> = bincode::deserialize(&bad_bytes);
        assert!(
            result.is_err(),
            "unknown tag should produce a deserialize error"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("99"),
            "error message should mention the unknown tag"
        );
    }
}
