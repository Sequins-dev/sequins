use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// 16-byte trace identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(opentelemetry::trace::TraceId);

impl TraceId {
    /// Create from 16 bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(opentelemetry::trace::TraceId::from_bytes(bytes))
    }

    /// Get bytes
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.to_bytes())
    }

    /// Parse from hex string
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not valid hexadecimal or is not 32 characters
    pub fn from_hex(s: &str) -> Result<Self, String> {
        let bytes = hex::decode(s).map_err(|e| format!("Invalid hex string: {}", e))?;

        if bytes.len() != 16 {
            return Err(format!("Expected 16 bytes, got {}", bytes.len()));
        }

        let mut array = [0u8; 16];
        array.copy_from_slice(&bytes);
        Ok(Self::from_bytes(array))
    }

    /// Convert to OpenTelemetry TraceId
    pub fn to_otel(&self) -> opentelemetry::trace::TraceId {
        self.0
    }

    /// Convert from OpenTelemetry TraceId
    pub fn from_otel(trace_id: opentelemetry::trace::TraceId) -> Self {
        Self(trace_id)
    }
}

impl Serialize for TraceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for TraceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// 8-byte span identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanId(opentelemetry::trace::SpanId);

impl SpanId {
    /// Create from 8 bytes
    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(opentelemetry::trace::SpanId::from_bytes(bytes))
    }

    /// Get bytes
    pub fn to_bytes(&self) -> [u8; 8] {
        self.0.to_bytes()
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.to_bytes())
    }

    /// Parse from hex string
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not valid hexadecimal or is not 16 characters
    pub fn from_hex(s: &str) -> Result<Self, String> {
        let bytes = hex::decode(s).map_err(|e| format!("Invalid hex string: {}", e))?;

        if bytes.len() != 8 {
            return Err(format!("Expected 8 bytes, got {}", bytes.len()));
        }

        let mut array = [0u8; 8];
        array.copy_from_slice(&bytes);
        Ok(Self::from_bytes(array))
    }

    /// Convert to OpenTelemetry SpanId
    pub fn to_otel(&self) -> opentelemetry::trace::SpanId {
        self.0
    }

    /// Convert from OpenTelemetry SpanId
    pub fn from_otel(span_id: opentelemetry::trace::SpanId) -> Self {
        Self(span_id)
    }
}

impl Serialize for SpanId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for SpanId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_id_hex_round_trip() {
        // Valid 32-character hex string (16 bytes)
        let hex = "0123456789abcdef0123456789abcdef";
        let trace_id = TraceId::from_hex(hex).expect("Valid hex should parse");
        let result = trace_id.to_hex();
        assert_eq!(result, hex);
    }

    #[test]
    fn test_span_id_hex_round_trip() {
        // Valid 16-character hex string (8 bytes)
        let hex = "0123456789abcdef";
        let span_id = SpanId::from_hex(hex).expect("Valid hex should parse");
        let result = span_id.to_hex();
        assert_eq!(result, hex);
    }

    #[test]
    fn test_trace_id_from_invalid_hex() {
        // Wrong length (too short)
        let result = TraceId::from_hex("0123456789abcdef");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected 16 bytes, got 8"));

        // Wrong length (too long)
        let result = TraceId::from_hex("0123456789abcdef0123456789abcdef0123");
        assert!(result.is_err());

        // Invalid hex characters
        let result = TraceId::from_hex("0123456789abcdefghijklmnopqrstuv");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid hex string"));

        // Empty string
        let result = TraceId::from_hex("");
        assert!(result.is_err());
    }

    #[test]
    fn test_span_id_from_invalid_hex() {
        // Wrong length (too short)
        let result = SpanId::from_hex("01234567");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected 8 bytes, got 4"));

        // Wrong length (too long)
        let result = SpanId::from_hex("0123456789abcdef0123");
        assert!(result.is_err());

        // Invalid hex characters
        let result = SpanId::from_hex("0123456789abcxyz");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid hex string"));

        // Empty string
        let result = SpanId::from_hex("");
        assert!(result.is_err());
    }

    #[test]
    fn test_id_zero_and_max_values() {
        // Test all zeros
        let zero_trace =
            TraceId::from_hex("00000000000000000000000000000000").expect("Valid hex should parse");
        assert_eq!(zero_trace.to_hex(), "00000000000000000000000000000000");

        let zero_span = SpanId::from_hex("0000000000000000").expect("Valid hex should parse");
        assert_eq!(zero_span.to_hex(), "0000000000000000");

        // Test all Fs (max values)
        let max_trace =
            TraceId::from_hex("ffffffffffffffffffffffffffffffff").expect("Valid hex should parse");
        assert_eq!(max_trace.to_hex(), "ffffffffffffffffffffffffffffffff");

        let max_span = SpanId::from_hex("ffffffffffffffff").expect("Valid hex should parse");
        assert_eq!(max_span.to_hex(), "ffffffffffffffff");
    }

    #[test]
    fn test_trace_id_bytes_round_trip() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let trace_id = TraceId::from_bytes(bytes);
        assert_eq!(trace_id.to_bytes(), bytes);
    }

    #[test]
    fn test_span_id_bytes_round_trip() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let span_id = SpanId::from_bytes(bytes);
        assert_eq!(span_id.to_bytes(), bytes);
    }

    #[test]
    fn test_trace_id_equality() {
        let hex = "0123456789abcdef0123456789abcdef";
        let trace_id1 = TraceId::from_hex(hex).expect("Valid hex should parse");
        let trace_id2 = TraceId::from_hex(hex).expect("Valid hex should parse");
        assert_eq!(trace_id1, trace_id2);
    }

    #[test]
    fn test_span_id_equality() {
        let hex = "0123456789abcdef";
        let span_id1 = SpanId::from_hex(hex).expect("Valid hex should parse");
        let span_id2 = SpanId::from_hex(hex).expect("Valid hex should parse");
        assert_eq!(span_id1, span_id2);
    }

    #[test]
    fn test_trace_id_otel_conversion() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let trace_id = TraceId::from_bytes(bytes);
        let otel_id = trace_id.to_otel();
        let converted_back = TraceId::from_otel(otel_id);
        assert_eq!(trace_id, converted_back);
    }

    #[test]
    fn test_span_id_otel_conversion() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let span_id = SpanId::from_bytes(bytes);
        let otel_id = span_id.to_otel();
        let converted_back = SpanId::from_otel(otel_id);
        assert_eq!(span_id, converted_back);
    }
}
