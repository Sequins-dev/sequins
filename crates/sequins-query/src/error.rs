use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during SeQL query execution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Error)]
pub enum QueryError {
    /// A field referenced in the query does not exist on the signal
    #[error("unknown field: {field}")]
    UnknownField {
        /// The field name that was not found
        field: String,
    },
    /// The query AST is structurally invalid
    #[error("invalid AST: {message}")]
    InvalidAst {
        /// Description of the structural problem
        message: String,
    },
    /// A pipeline stage is not supported by this backend
    #[error("unsupported stage: {stage}")]
    UnsupportedStage {
        /// Name of the stage
        stage: String,
    },
    /// A resource limit was exceeded
    #[error("resource limit exceeded: {limit}")]
    ResourceLimit {
        /// Which limit was exceeded
        limit: String,
    },
    /// An error occurred during query execution
    #[error("execution error: {message}")]
    Execution {
        /// Description of the execution error
        message: String,
    },
}

/// Warning codes that may accompany query results
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WarningCode {
    /// The result set was truncated due to a row limit
    ResultTruncated,
    /// The query took longer than expected
    SlowQuery,
    /// The result is approximate (e.g. from sampling or HLL)
    ApproximateResult,
    /// Schema resolution fell back to a generic type
    SchemaResolutionFallback,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_round_trip() {
        let errors = vec![
            QueryError::UnknownField {
                field: "foo".into(),
            },
            QueryError::InvalidAst {
                message: "bad".into(),
            },
            QueryError::UnsupportedStage {
                stage: "Navigate".into(),
            },
            QueryError::ResourceLimit {
                limit: "rows".into(),
            },
            QueryError::Execution {
                message: "db error".into(),
            },
        ];
        for e in &errors {
            let json = serde_json::to_string(e).unwrap();
            let back: QueryError = serde_json::from_str(&json).unwrap();
            assert_eq!(e, &back);
        }
    }

    #[test]
    fn warning_code_round_trip() {
        let codes = vec![
            WarningCode::ResultTruncated,
            WarningCode::SlowQuery,
            WarningCode::ApproximateResult,
            WarningCode::SchemaResolutionFallback,
        ];
        for code in &codes {
            let json = serde_json::to_string(code).unwrap();
            let back: WarningCode = serde_json::from_str(&json).unwrap();
            assert_eq!(code, &back);
        }
    }
}
