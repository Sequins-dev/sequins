use std::sync::Arc;
use vortex::dtype::{DType, ExtDType, ExtID, Nullability, PType};

/// Vortex extension data types for semantic typing of enum columns
///
/// These ExtDTypes are used in Vortex files to preserve semantic type information
/// for OTLP enum fields that are stored as raw integers. The storage dtype is UInt8
/// for all of these, but the ExtDType carries the semantic meaning.
///
/// When registering tables in DataFusion, these extension types are stripped and
/// queries see plain UInt8 columns.
/// Extension dtype for SpanKind enum (Internal=1, Server=2, Client=3, Producer=4, Consumer=5)
pub fn span_kind_ext_dtype() -> ExtDType {
    ExtDType::new(
        ExtID::new("sequins.span_kind".into()),
        Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        None,
    )
}

/// Extension dtype for SpanStatus enum (Unset=0, Ok=1, Error=2)
pub fn span_status_ext_dtype() -> ExtDType {
    ExtDType::new(
        ExtID::new("sequins.span_status".into()),
        Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        None,
    )
}

/// Extension dtype for LogSeverity number (1-24 range per OTLP spec)
pub fn log_severity_ext_dtype() -> ExtDType {
    ExtDType::new(
        ExtID::new("sequins.log_severity".into()),
        Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        None,
    )
}

/// Extension dtype for MetricType enum (Gauge=0, Counter=1, Histogram=2, Summary=3)
pub fn metric_type_ext_dtype() -> ExtDType {
    ExtDType::new(
        ExtID::new("sequins.metric_type".into()),
        Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        None,
    )
}

/// Extension dtype for AggregationTemporality enum (Unspecified=0, Delta=1, Cumulative=2)
pub fn aggregation_temporality_ext_dtype() -> ExtDType {
    ExtDType::new(
        ExtID::new("sequins.aggregation_temporality".into()),
        Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        None,
    )
}

/// Extension dtype for ProfileType enum (CPU=0, Memory=1, etc.)
pub fn profile_type_ext_dtype() -> ExtDType {
    ExtDType::new(
        ExtID::new("sequins.profile_type".into()),
        Arc::new(DType::Primitive(PType::U8, Nullability::NonNullable)),
        None,
    )
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
