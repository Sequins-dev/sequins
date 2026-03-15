//! Unit tests for server error types
//!
//! These tests verify that error types convert correctly to HTTP responses
//! with appropriate status codes and error messages.

use axum::{http::StatusCode, response::IntoResponse};
use sequins::server::Error;
use std::error::Error as StdError;

#[test]
fn test_core_error_to_response() {
    let core_error = sequins::error::Error::Other("DB connection failed".to_string());
    let server_error = Error::Core(core_error);

    let response = server_error.into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn test_json_error_to_response() {
    let json_str = r#"{"invalid": json}"#;
    let json_error = serde_json::from_str::<serde_json::Value>(json_str).unwrap_err();
    let server_error = Error::Json(json_error);

    let response = server_error.into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn test_http_error_to_response() {
    let server_error = Error::Http("Server binding failed".to_string());

    let response = server_error.into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn test_grpc_error_to_response() {
    let server_error = Error::Grpc("gRPC connection error".to_string());

    let response = server_error.into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn test_invalid_request_error_to_response() {
    let server_error = Error::InvalidRequest("Invalid trace ID format".to_string());

    let response = server_error.into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn test_error_display() {
    let error = Error::InvalidRequest("test error message".to_string());
    let error_string = error.to_string();
    assert!(error_string.contains("invalid request"));
    assert!(error_string.contains("test error message"));
}

#[test]
fn test_core_error_conversion() {
    let core_error = sequins::error::Error::Other("Bad input".to_string());
    let server_error: Error = core_error.into();

    match server_error {
        Error::Core(_) => {
            // Success - correctly converted
        }
        _ => panic!("Expected Error::Core variant"),
    }
}

#[test]
fn test_json_error_conversion() {
    let json_error = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
    let server_error: Error = json_error.into();

    match server_error {
        Error::Json(_) => {
            // Success - correctly converted
        }
        _ => panic!("Expected Error::Json variant"),
    }
}

#[test]
fn test_error_source_chain() {
    let core_error = sequins::error::Error::Other("Root cause".to_string());
    let server_error = Error::Core(core_error);

    // Verify error source chain
    assert!(server_error.source().is_some());
}
