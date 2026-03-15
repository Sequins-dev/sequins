use thiserror::Error;

/// Top-level error type for Sequins operations
#[derive(Error, Debug)]
pub enum Error {
    /// Generic error with message
    #[error("{0}")]
    Other(String),
}

/// Result type alias using Sequins Error
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_other_creation() {
        let error = Error::Other("test error".to_string());
        assert!(matches!(error, Error::Other(_)));
    }

    #[test]
    fn test_error_display_formatting() {
        let error = Error::Other("connection failed".to_string());
        let error_string = error.to_string();
        assert_eq!(error_string, "connection failed");
    }

    #[test]
    fn test_error_debug_formatting() {
        let error = Error::Other("debug test".to_string());
        let debug_string = format!("{:?}", error);
        assert!(debug_string.contains("Other"));
        assert!(debug_string.contains("debug test"));
    }

    #[test]
    fn test_error_source() {
        let error = Error::Other("test".to_string());
        // Error::Other has no underlying source
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn test_result_type_ok() {
        let result: Result<i32> = Ok(42);
        assert_eq!(result.ok(), Some(42));
    }

    #[test]
    fn test_result_type_err() {
        let result: Result<i32> = Err(Error::Other("failed".to_string()));
        assert_eq!(
            result.err().map(|e| e.to_string()),
            Some("failed".to_string())
        );
    }

    #[test]
    fn test_error_propagation() {
        fn inner_function() -> Result<()> {
            Err(Error::Other("inner error".to_string()))
        }

        fn outer_function() -> Result<()> {
            inner_function()?;
            Ok(())
        }

        let result = outer_function();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "inner error");
    }

    #[test]
    fn test_error_with_empty_message() {
        let error = Error::Other(String::new());
        assert_eq!(error.to_string(), "");
    }
}
