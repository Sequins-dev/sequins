use std::fmt;
use vortex::dtype::extension::{ExtDType, ExtDTypeRef, ExtId, ExtVTable};
use vortex::dtype::{DType, Nullability, PType};
use vortex::error::VortexResult;
use vortex::scalar::ScalarValue;

/// A minimal extension-type vtable used to carry a semantic ID on top of a `UInt8`
/// storage dtype. It holds no metadata and performs no validation beyond identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SequinsEnumExt {
    id: ExtId,
}

/// Empty metadata for [`SequinsEnumExt`]. Extension vtables require a `Display`able
/// metadata type; these enum extensions carry none.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EmptyMetadata;

impl fmt::Display for EmptyMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}

impl ExtVTable for SequinsEnumExt {
    type Metadata = EmptyMetadata;
    type NativeValue<'a> = &'a ScalarValue;

    fn id(&self) -> ExtId {
        self.id
    }

    fn serialize_metadata(&self, _metadata: &Self::Metadata) -> VortexResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn deserialize_metadata(&self, _metadata: &[u8]) -> VortexResult<Self::Metadata> {
        Ok(EmptyMetadata)
    }

    fn validate_dtype(_ext_dtype: &ExtDType<Self>) -> VortexResult<()> {
        Ok(())
    }

    fn unpack_native<'a>(
        _ext_dtype: &'a ExtDType<Self>,
        storage_value: &'a ScalarValue,
    ) -> VortexResult<Self::NativeValue<'a>> {
        Ok(storage_value)
    }
}

/// Build a `UInt8`-backed extension dtype carrying the given semantic ID.
fn enum_ext_dtype(id: &str) -> ExtDTypeRef {
    ExtDType::try_with_vtable(
        SequinsEnumExt { id: ExtId::new(id) },
        EmptyMetadata,
        DType::Primitive(PType::U8, Nullability::NonNullable),
    )
    .expect("UInt8 storage dtype is always valid for a metadata-free extension type")
    .erased()
}

/// Vortex extension data types for semantic typing of enum columns
///
/// These ExtDTypes are used in Vortex files to preserve semantic type information
/// for OTLP enum fields that are stored as raw integers. The storage dtype is UInt8
/// for all of these, but the ExtDType carries the semantic meaning.
///
/// When registering tables in DataFusion, these extension types are stripped and
/// queries see plain UInt8 columns.
/// Extension dtype for SpanKind enum (Internal=1, Server=2, Client=3, Producer=4, Consumer=5)
pub fn span_kind_ext_dtype() -> ExtDTypeRef {
    enum_ext_dtype("sequins.span_kind")
}

/// Extension dtype for SpanStatus enum (Unset=0, Ok=1, Error=2)
pub fn span_status_ext_dtype() -> ExtDTypeRef {
    enum_ext_dtype("sequins.span_status")
}

/// Extension dtype for LogSeverity number (1-24 range per OTLP spec)
pub fn log_severity_ext_dtype() -> ExtDTypeRef {
    enum_ext_dtype("sequins.log_severity")
}

/// Extension dtype for MetricType enum (Gauge=0, Counter=1, Histogram=2, Summary=3)
pub fn metric_type_ext_dtype() -> ExtDTypeRef {
    enum_ext_dtype("sequins.metric_type")
}

/// Extension dtype for AggregationTemporality enum (Unspecified=0, Delta=1, Cumulative=2)
pub fn aggregation_temporality_ext_dtype() -> ExtDTypeRef {
    enum_ext_dtype("sequins.aggregation_temporality")
}

/// Extension dtype for ProfileType enum (CPU=0, Memory=1, etc.)
pub fn profile_type_ext_dtype() -> ExtDTypeRef {
    enum_ext_dtype("sequins.profile_type")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ext_dtype_creation() {
        let span_kind = span_kind_ext_dtype();
        assert_eq!(span_kind.id().to_string(), "sequins.span_kind");
        let storage_dtype = span_kind.storage_dtype();
        assert!(matches!(
            storage_dtype,
            DType::Primitive(PType::U8, Nullability::NonNullable)
        ));

        let log_severity = log_severity_ext_dtype();
        assert_eq!(log_severity.id().to_string(), "sequins.log_severity");

        let metric_type = metric_type_ext_dtype();
        assert_eq!(metric_type.id().to_string(), "sequins.metric_type");
    }

    #[test]
    fn test_span_status_ext_dtype() {
        let dtype = span_status_ext_dtype();
        assert_eq!(dtype.id().to_string(), "sequins.span_status");
        assert!(matches!(
            dtype.storage_dtype(),
            DType::Primitive(PType::U8, Nullability::NonNullable)
        ));
    }

    #[test]
    fn test_aggregation_temporality_ext_dtype() {
        let dtype = aggregation_temporality_ext_dtype();
        assert_eq!(dtype.id().to_string(), "sequins.aggregation_temporality");
        assert!(matches!(
            dtype.storage_dtype(),
            DType::Primitive(PType::U8, Nullability::NonNullable)
        ));
    }

    #[test]
    fn test_profile_type_ext_dtype() {
        let dtype = profile_type_ext_dtype();
        assert_eq!(dtype.id().to_string(), "sequins.profile_type");
        assert!(matches!(
            dtype.storage_dtype(),
            DType::Primitive(PType::U8, Nullability::NonNullable)
        ));
    }
}
