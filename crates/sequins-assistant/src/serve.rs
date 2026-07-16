//! An OpenAI-compatible HTTP surface over a **named registry** of Rig
//! [`CompletionModel`]s — the generic "Rig proxying" server.
//!
//! - `GET /v1/models` lists the registry's model ids (empty when none configured).
//! - `POST /v1/chat/completions` resolves `model` in the registry and runs it,
//!   streaming (SSE) or not. An unknown/absent model id returns the standard
//!   OpenAI `model_not_found` error.
//!
//! This is domain-agnostic: it serves *any* `CompletionModel` (here a
//! [`SequinsAssistantModel`](crate::model::SequinsAssistantModel)) and is the far
//! side of the remote wire — our own app and any OpenAI client hit the same route.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use indexmap::IndexMap;
use rig::completion::{CompletionModel, CompletionRequest, CompletionResponse, ToolDefinition};
use rig::message::{
    AssistantContent, Message, Text, ToolCall, ToolFunction, ToolResult, ToolResultContent,
    UserContent,
};
use rig::OneOrMany;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Build the OpenAI-compatible router over a named registry of models.
///
/// The registry order is preserved in `GET /v1/models`. An empty registry serves
/// an empty model list and answers every completion with `model_not_found`.
pub fn completion_model_router<M>(models: IndexMap<String, M>) -> Router
where
    M: CompletionModel + Send + Sync + 'static,
{
    let state = Arc::new(AppState { models });
    Router::new()
        .route("/v1/models", get(list_models::<M>))
        .route("/v1/chat/completions", post(chat_completions::<M>))
        .with_state(state)
}

struct AppState<M> {
    models: IndexMap<String, M>,
}

// ---------------------------------------------------------------------------
// GET /v1/models
// ---------------------------------------------------------------------------

async fn list_models<M>(State(state): State<Arc<AppState<M>>>) -> Json<serde_json::Value>
where
    M: CompletionModel + Send + Sync + 'static,
{
    let created = unix_secs();
    let data: Vec<_> = state
        .models
        .keys()
        .map(|id| json!({ "id": id, "object": "model", "created": created, "owned_by": "sequins" }))
        .collect();
    Json(json!({ "object": "list", "data": data }))
}

// ---------------------------------------------------------------------------
// POST /v1/chat/completions
// ---------------------------------------------------------------------------

async fn chat_completions<M>(
    State(state): State<Arc<AppState<M>>>,
    Json(req): Json<ChatRequest>,
) -> Response
where
    M: CompletionModel + Send + Sync + 'static,
{
    tracing::debug!(
        model = %req.model,
        messages = req.messages.len(),
        caller_tools = req.tools.len(),
        stream = req.stream,
        "assistant chat request"
    );

    let Some(model) = state.models.get(&req.model).cloned() else {
        tracing::warn!(model = %req.model, "assistant chat: unknown model");
        return model_not_found(&req.model).into_response();
    };

    let completion_req = match build_completion_request(&req) {
        Ok(r) => r,
        Err(msg) => {
            tracing::warn!(model = %req.model, error = %msg, "assistant chat: bad request");
            return bad_request(&msg).into_response();
        }
    };

    let response = match model.completion(completion_req).await {
        Ok(r) => r,
        Err(e) => {
            // Surface the upstream/tool-loop failure — otherwise a bad backing key
            // or unreachable provider looks like a silent "failed to send" client-side.
            tracing::warn!(model = %req.model, error = %e, "assistant chat: upstream error");
            return upstream_error(&e.to_string()).into_response();
        }
    };

    let (text, tool_calls) = split_choice(&response);
    let model_id = req.model.clone();

    if req.stream {
        stream_response(model_id, text, tool_calls).into_response()
    } else {
        Json(build_chat_response(&model_id, text, tool_calls, &response)).into_response()
    }
}

/// Translate an OpenAI chat request into a Rig [`CompletionRequest`], merging all
/// `system` messages into the preamble and mapping the rest into chat history.
fn build_completion_request(req: &ChatRequest) -> Result<CompletionRequest, String> {
    let mut system_texts: Vec<String> = Vec::new();
    let mut history: Vec<Message> = Vec::new();

    for msg in &req.messages {
        match msg.role.as_str() {
            "system" | "developer" => {
                if let Some(c) = msg.content_text() {
                    system_texts.push(c);
                }
            }
            "user" => {
                history.push(Message::User {
                    content: OneOrMany::one(UserContent::Text(Text::new(
                        msg.content_text().unwrap_or_default(),
                    ))),
                });
            }
            "assistant" => {
                let mut parts: Vec<AssistantContent> = Vec::new();
                if let Some(c) = msg.content_text() {
                    if !c.is_empty() {
                        parts.push(AssistantContent::Text(Text::new(c)));
                    }
                }
                for tc in &msg.tool_calls {
                    parts.push(AssistantContent::ToolCall(ToolCall::new(
                        tc.id.clone(),
                        ToolFunction::new(tc.function.name.clone(), tc.function.parsed_arguments()),
                    )));
                }
                if let Ok(content) = OneOrMany::many(parts) {
                    history.push(Message::Assistant { id: None, content });
                }
            }
            "tool" => {
                let id = msg.tool_call_id.clone().unwrap_or_default();
                history.push(Message::User {
                    content: OneOrMany::one(UserContent::ToolResult(ToolResult {
                        id,
                        call_id: None,
                        content: OneOrMany::one(ToolResultContent::Text(Text::new(
                            msg.content_text().unwrap_or_default(),
                        ))),
                    })),
                });
            }
            other => return Err(format!("unsupported message role '{other}'")),
        }
    }

    let chat_history = OneOrMany::many(history)
        .map_err(|_| "at least one non-system message is required".to_string())?;

    let tools: Vec<ToolDefinition> = req
        .tools
        .iter()
        .map(|t| ToolDefinition {
            name: t.function.name.clone(),
            description: t.function.description.clone().unwrap_or_default(),
            parameters: t.function.parameters.clone().unwrap_or_else(|| json!({})),
        })
        .collect();

    let preamble = (!system_texts.is_empty()).then(|| system_texts.join("\n\n"));

    Ok(CompletionRequest {
        // No model override: `req.model` is our public registry id, not the backing
        // provider's model name. The backing model supplies its own name (the
        // middleware also clears this defensively).
        model: None,
        preamble,
        chat_history,
        documents: Vec::new(),
        tools,
        temperature: req.temperature,
        // Drop the client's max_tokens: it sizes output for our *public* model id,
        // but we route to a different backing model with its own limit. Forwarding
        // an oversized value 400s ("max_tokens is too large for this model"); letting
        // the backing model use its default is correct for a remapping proxy.
        max_tokens: None,
        tool_choice: None,
        additional_params: None,
        output_schema: None,
    })
}

/// Split a completion response's choice into concatenated text and tool calls.
fn split_choice<R>(response: &CompletionResponse<R>) -> (String, Vec<ChatToolCall>) {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for content in response.choice.iter() {
        match content {
            AssistantContent::Text(t) => text.push_str(&t.text),
            AssistantContent::ToolCall(tc) => tool_calls.push(ChatToolCall {
                id: tc.id.clone(),
                r#type: "function".to_string(),
                function: ChatToolCallFunction {
                    name: tc.function.name.clone(),
                    arguments: tc.function.arguments.to_string(),
                },
            }),
            _ => {}
        }
    }
    (text, tool_calls)
}

fn build_chat_response<R>(
    model_id: &str,
    text: String,
    tool_calls: Vec<ChatToolCall>,
    response: &CompletionResponse<R>,
) -> serde_json::Value {
    let finish_reason = if tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };
    let mut message = json!({ "role": "assistant" });
    message["content"] = if text.is_empty() && !tool_calls.is_empty() {
        serde_json::Value::Null
    } else {
        json!(text)
    };
    if !tool_calls.is_empty() {
        message["tool_calls"] = json!(tool_calls);
    }
    json!({
        "id": chat_id(),
        "object": "chat.completion",
        "created": unix_secs(),
        "model": model_id,
        "choices": [{ "index": 0, "message": message, "finish_reason": finish_reason }],
        "usage": {
            "prompt_tokens": response.usage.input_tokens,
            "completion_tokens": response.usage.output_tokens,
            "total_tokens": response.usage.total_tokens,
        }
    })
}

/// Stream the (already-computed) result as OpenAI `chat.completion.chunk` SSE
/// events: a role delta, the content/tool-call delta, a finish chunk, then `[DONE]`.
fn stream_response(
    model_id: String,
    text: String,
    tool_calls: Vec<ChatToolCall>,
) -> Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let id = chat_id();
    let created = unix_secs();
    let finish_reason = if tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };

    let base = |delta: serde_json::Value, finish: Option<&str>| {
        json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model_id,
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
        })
    };

    let mut chunks: Vec<serde_json::Value> = vec![base(json!({ "role": "assistant" }), None)];
    if !text.is_empty() {
        chunks.push(base(json!({ "content": text }), None));
    }
    if !tool_calls.is_empty() {
        let tc: Vec<_> = tool_calls
            .iter()
            .enumerate()
            .map(|(i, c)| {
                json!({
                    "index": i,
                    "id": c.id,
                    "type": "function",
                    "function": { "name": c.function.name, "arguments": c.function.arguments }
                })
            })
            .collect();
        chunks.push(base(json!({ "tool_calls": tc }), None));
    }
    chunks.push(base(json!({}), Some(finish_reason)));

    let events = chunks
        .into_iter()
        .map(|c| Ok(Event::default().data(c.to_string())))
        .chain(std::iter::once(Ok(Event::default().data("[DONE]"))));

    Sse::new(futures::stream::iter(events.collect::<Vec<_>>()))
}

// ---------------------------------------------------------------------------
// OpenAI wire types (request side)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ChatRequest {
    model: String,
    #[serde(default)]
    messages: Vec<ChatMessage>,
    #[serde(default)]
    tools: Vec<ChatTool>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    temperature: Option<f64>,
    // `max_tokens` is intentionally not captured: we route to a backing model whose
    // limit differs from the public id the client sized for, so we never forward it.
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    role: String,
    #[serde(default)]
    content: Option<serde_json::Value>,
    #[serde(default)]
    tool_calls: Vec<ChatToolCall>,
    #[serde(default)]
    tool_call_id: Option<String>,
}

impl ChatMessage {
    /// Extract plain text from an OpenAI message `content`, which may be a string
    /// or an array of `{type:"text", text}` parts.
    fn content_text(&self) -> Option<String> {
        match &self.content {
            Some(serde_json::Value::String(s)) => Some(s.clone()),
            Some(serde_json::Value::Array(parts)) => {
                let joined: String = parts
                    .iter()
                    .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                    .collect::<Vec<_>>()
                    .join("");
                Some(joined)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatToolCall {
    #[serde(default)]
    id: String,
    #[serde(default = "function_type", rename = "type")]
    r#type: String,
    function: ChatToolCallFunction,
}

fn function_type() -> String {
    "function".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatToolCallFunction {
    name: String,
    /// OpenAI encodes arguments as a JSON *string*.
    #[serde(default)]
    arguments: String,
}

impl ChatToolCallFunction {
    fn parsed_arguments(&self) -> serde_json::Value {
        serde_json::from_str(&self.arguments).unwrap_or(serde_json::Value::Null)
    }
}

#[derive(Debug, Deserialize)]
struct ChatTool {
    function: ChatToolFunction,
}

#[derive(Debug, Deserialize)]
struct ChatToolFunction {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    parameters: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Errors (OpenAI-shaped)
// ---------------------------------------------------------------------------

fn model_not_found(model: &str) -> (StatusCode, Json<serde_json::Value>) {
    error_body(
        StatusCode::NOT_FOUND,
        format!("The model '{model}' does not exist or is not configured."),
        "invalid_request_error",
        Some("model_not_found"),
    )
}

fn bad_request(message: &str) -> (StatusCode, Json<serde_json::Value>) {
    error_body(
        StatusCode::BAD_REQUEST,
        message.to_string(),
        "invalid_request_error",
        None,
    )
}

fn upstream_error(message: &str) -> (StatusCode, Json<serde_json::Value>) {
    error_body(
        StatusCode::BAD_GATEWAY,
        message.to_string(),
        "api_error",
        None,
    )
}

fn error_body(
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

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn chat_id() -> String {
    format!(
        "chatcmpl-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}
