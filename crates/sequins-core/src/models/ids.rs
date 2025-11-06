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
        let bytes = hex::decode(s)
            .map_err(|e| format!("Invalid hex string: {}", e))?;

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
        self.0.to_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TraceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <[u8; 16]>::deserialize(deserializer)?;
        Ok(Self::from_bytes(bytes))
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
        let bytes = hex::decode(s)
            .map_err(|e| format!("Invalid hex string: {}", e))?;

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
        self.0.to_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SpanId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <[u8; 8]>::deserialize(deserializer)?;
        Ok(Self::from_bytes(bytes))
    }
}
