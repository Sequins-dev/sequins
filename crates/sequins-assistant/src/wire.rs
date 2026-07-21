//! Small wire-format helpers shared by the OpenAI-compatible surfaces
//! ([`crate::serve`] chat-completions and [`crate::responses`]): unix timestamps and
//! id generation, tool-argument parsing, tool-definition construction, the
//! OpenAI-shaped error bodies, and the boilerplate for a Rig [`CompletionRequest`].

use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use axum::Json;
use rig::completion::{CompletionRequest, ToolDefinition};
use rig::message::Message;
use rig::OneOrMany;
use serde_json::json;

/// Seconds since the Unix epoch (0 if the clock is before the epoch).
pub(crate) fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Nanoseconds since the Unix epoch — the entropy behind generated ids.
pub(crate) fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// A unique-ish id of the form `{prefix}_{nanos}` (e.g. `resp_1234…`).
pub(crate) fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", unix_nanos())
}

/// Parse OpenAI tool-call arguments (a JSON *string*) into a value, defaulting to
/// `null` on malformed input.
pub(crate) fn parse_arguments(arguments: &str) -> serde_json::Value {
    serde_json::from_str(arguments).unwrap_or(serde_json::Value::Null)
}

/// Build a Rig [`ToolDefinition`] from the optional description/parameters shared by
/// the chat and responses tool DTOs (empty description, `{}` schema when omitted).
pub(crate) fn tool_definition(
    name: String,
    description: Option<String>,
    parameters: Option<serde_json::Value>,
) -> ToolDefinition {
    ToolDefinition {
        name,
        description: description.unwrap_or_default(),
        parameters: parameters.unwrap_or_else(|| json!({})),
    }
}

/// An OpenAI-shaped error body.
pub(crate) fn error_body(
    status: StatusCode,
    message: String,
    kind: &str,
    code: Option<&str>,
) -> (StatusCode, Json<serde_json::Value>) {
    (
        status,
        Json(json!({ "error": { "message": message, "type": kind, "code": code } })),
    )
}

/// `404 model_not_found` for an unknown/unconfigured model id.
pub(crate) fn model_not_found(model: &str) -> (StatusCode, Json<serde_json::Value>) {
    error_body(
        StatusCode::NOT_FOUND,
        format!("The model '{model}' does not exist or is not configured."),
        "invalid_request_error",
        Some("model_not_found"),
    )
}

/// `400 invalid_request_error` for a malformed request.
pub(crate) fn bad_request(message: &str) -> (StatusCode, Json<serde_json::Value>) {
    error_body(
        StatusCode::BAD_REQUEST,
        message.to_string(),
        "invalid_request_error",
        None,
    )
}

/// A [`CompletionRequest`] with the fields both surfaces always set the same way.
///
/// `model` is `None` (the public registry id is not the backing provider's model
/// name; the middleware supplies its own) and `documents`/`max_tokens`/`tool_choice`/
/// `additional_params`/`output_schema`/`temperature` are unset. Callers that need a
/// non-default (e.g. chat forwarding `temperature`) override the field afterward.
pub(crate) fn base_completion_request(
    preamble: Option<String>,
    chat_history: OneOrMany<Message>,
    tools: Vec<ToolDefinition>,
) -> CompletionRequest {
    CompletionRequest {
        model: None,
        preamble,
        chat_history,
        documents: Vec::new(),
        tools,
        temperature: None,
        max_tokens: None,
        tool_choice: None,
        additional_params: None,
        output_schema: None,
    }
}
