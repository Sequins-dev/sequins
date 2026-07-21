//! OpenAI **Responses API** endpoint (`POST /v1/responses`).
//!
//! Unlike Chat Completions, the Responses API streams a sequence of typed *output
//! items*. That lets us surface **server-executed** tool activity to the client:
//! each in-process tool call (`run_sql`, `column_profile`, …) appears as a resolved
//! item the client can render even though it didn't drive the tool. Client-provided
//! tools still appear as `function_call` items for the client to execute and answer
//! (via a `function_call_output` input item on the next turn).
//!
//! Server-executed tools use a custom `sequins.tool_result` item type — OpenAI only
//! defines dedicated item types for its own built-in tools (`web_search`, …), so a
//! generic server tool needs a custom type. First-party clients render it; others
//! ignore unknown item types.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use rig::completion::{CompletionModel, CompletionRequest, ToolDefinition};
use rig::message::{
    AssistantContent, Message, Text, ToolCall, ToolFunction, ToolResult, ToolResultContent,
    UserContent,
};
use rig::OneOrMany;
use sequins_metadata::{AppStateStore, ConversationItem};
use serde::Deserialize;
use serde_json::json;

use crate::model::{AgentEvent, SequinsAssistantModel};
use crate::wire::{self, bad_request, model_not_found, new_id, parse_arguments, unix_secs};

/// Router state: the model registry plus the optional app-state store that
/// persists conversations (when `store` is set on the request).
struct ResponsesState<B: CompletionModel> {
    models: IndexMap<String, SequinsAssistantModel<B>>,
    store: Option<Arc<AppStateStore>>,
}

/// Router serving `POST /v1/responses` over the model registry, persisting
/// conversations to `store` (when provided) so history is queryable.
pub fn responses_router<B>(
    models: IndexMap<String, SequinsAssistantModel<B>>,
    store: Option<Arc<AppStateStore>>,
) -> Router
where
    B: CompletionModel + Send + Sync + 'static,
{
    Router::new()
        .route("/v1/responses", post(create_response::<B>))
        .with_state(Arc::new(ResponsesState { models, store }))
}

async fn create_response<B>(
    State(state): State<Arc<ResponsesState<B>>>,
    Json(req): Json<ResponsesRequest>,
) -> Response
where
    B: CompletionModel + Send + Sync + 'static,
{
    let Some(model) = state.models.get(&req.model).cloned() else {
        return model_not_found(&req.model).into_response();
    };

    let response_id = new_id("resp");
    let store = state.store.clone();

    // Resolve the conversation this turn belongs to (existing) — a new one is
    // created at persist time if none is referenced.
    let conversation_id = resolve_conversation(store.as_deref(), &req).await;

    let mut completion_req = match build_completion_request(&req) {
        Ok(r) => r,
        Err(msg) => return bad_request(&msg).into_response(),
    };
    // Server-side conversation state: prepend the prior visible exchange so the
    // client only needs to send the new input (previous_response_id semantics).
    if let (Some(store), Some(cid)) = (store.as_deref(), conversation_id.as_deref()) {
        if let Some(conv) = store.get_conversation(cid).await {
            prepend_history(&mut completion_req, &conv);
        }
    }

    let model_id = req.model.clone();
    let persist = PersistTurn::build(&store, &req, &conversation_id, &response_id);

    if req.stream {
        stream_response(
            model,
            model_id,
            completion_req,
            response_id,
            conversation_id,
            persist,
        )
        .into_response()
    } else {
        let events: Vec<AgentEvent> = model.run_events(completion_req).collect().await;
        let conversation_id = PersistTurn::finish(persist, &events, conversation_id).await;
        Json(build_response_object(
            &model_id,
            &response_id,
            conversation_id.as_deref(),
            &events,
        ))
        .into_response()
    }
}

/// Everything needed to persist a completed turn to the app-state store.
struct PersistTurn {
    store: Arc<AppStateStore>,
    conversation_id: Option<String>,
    title: Option<String>,
    input_items: Vec<ConversationItem>,
    response_id: String,
}

impl PersistTurn {
    /// Build the persist descriptor for a turn, or `None` when persistence is off
    /// (no store configured, or `store: false` on the request).
    fn build(
        store: &Option<Arc<AppStateStore>>,
        req: &ResponsesRequest,
        conversation_id: &Option<String>,
        response_id: &str,
    ) -> Option<Self> {
        let store = store.clone()?;
        if !req.store.unwrap_or(true) {
            return None;
        }
        Some(PersistTurn {
            store,
            conversation_id: conversation_id.clone(),
            title: derive_title(req),
            input_items: input_conversation_items(req),
            response_id: response_id.to_string(),
        })
    }

    /// Persist (if enabled) and resolve the continuation conversation id, falling back
    /// to `fallback` when persistence is off or fails.
    async fn finish(
        persist: Option<Self>,
        events: &[AgentEvent],
        fallback: Option<String>,
    ) -> Option<String> {
        match persist {
            Some(p) => p.run(events).await.or(fallback),
            None => fallback,
        }
    }

    /// Persist the turn; returns the (possibly newly created) conversation id.
    async fn run(self, events: &[AgentEvent]) -> Option<String> {
        let output_items: Vec<ConversationItem> = events
            .iter()
            .filter_map(|e| event_to_conversation_item(e, &self.response_id))
            .collect();
        match self
            .store
            .append_response(
                self.conversation_id,
                self.title,
                self.input_items,
                output_items,
                self.response_id,
            )
            .await
        {
            Ok(cid) => Some(cid),
            Err(e) => {
                tracing::warn!(error = %e, "failed to persist conversation turn");
                None
            }
        }
    }
}

/// Resolve the conversation for this request from an explicit `conversation` id or
/// a `previous_response_id`.
async fn resolve_conversation(
    store: Option<&AppStateStore>,
    req: &ResponsesRequest,
) -> Option<String> {
    if let Some(cid) = &req.conversation {
        return Some(cid.clone());
    }
    let (Some(store), Some(prev)) = (store, &req.previous_response_id) else {
        return None;
    };
    store.conversation_by_response(prev).await
}

/// Prepend a conversation's prior **visible** exchange (user + assistant text) to
/// the request so the model has context. Internal tool items are omitted — the
/// model re-explores as needed, avoiding malformed tool-call/result pairing.
fn prepend_history(request: &mut CompletionRequest, conv: &sequins_metadata::Conversation) {
    let mut prior: Vec<Message> = Vec::new();
    for item in &conv.items {
        if item.item_type != "message" {
            continue;
        }
        prior.push(message_for_role(
            &item.role,
            item.text.clone().unwrap_or_default(),
        ));
    }
    if prior.is_empty() {
        return;
    }
    prior.extend(request.chat_history.iter().cloned());
    request.chat_history =
        OneOrMany::many(prior).unwrap_or_else(|_| OneOrMany::one(Message::user("")));
}

/// Convert this turn's request input into conversation items (pre-turn, so no
/// `response_id`).
fn input_conversation_items(req: &ResponsesRequest) -> Vec<ConversationItem> {
    let mut items = Vec::new();
    match &req.input {
        ResponsesInput::Text(s) => items.push(text_item("user", "message", s.clone(), None)),
        ResponsesInput::Items(list) => {
            for item in list {
                match item {
                    InputItem::Message { role, content } => {
                        items.push(text_item(role, "message", content.text(), None));
                    }
                    InputItem::FunctionCall {
                        name, arguments, ..
                    } => items.push(ConversationItem {
                        tool_name: Some(name.clone()),
                        tool_arguments: Some(arguments.clone()),
                        ..text_item("assistant", "function_call", String::new(), None)
                    }),
                    InputItem::FunctionCallOutput { output, .. } => {
                        items.push(text_item(
                            "tool",
                            "function_call_output",
                            output.clone(),
                            None,
                        ));
                    }
                    InputItem::Other => {}
                }
            }
        }
    }
    items
}

/// Convert an `AgentEvent` into a persisted conversation item.
fn event_to_conversation_item(event: &AgentEvent, response_id: &str) -> Option<ConversationItem> {
    match event {
        AgentEvent::Text(text) => Some(text_item(
            "assistant",
            "message",
            text.clone(),
            Some(response_id),
        )),
        AgentEvent::ServerTool {
            name,
            arguments,
            output,
            ..
        } => Some(ConversationItem {
            tool_name: Some(name.clone()),
            tool_arguments: Some(arguments.to_string()),
            tool_output: Some(output.clone()),
            ..text_item(
                "tool",
                "sequins.tool_result",
                String::new(),
                Some(response_id),
            )
        }),
        AgentEvent::ClientTool {
            name, arguments, ..
        } => Some(ConversationItem {
            tool_name: Some(name.clone()),
            tool_arguments: Some(arguments.to_string()),
            ..text_item(
                "assistant",
                "function_call",
                String::new(),
                Some(response_id),
            )
        }),
        AgentEvent::Error(_) => None,
    }
}

fn text_item(
    role: &str,
    item_type: &str,
    text: String,
    response_id: Option<&str>,
) -> ConversationItem {
    ConversationItem {
        response_id: response_id.map(str::to_string),
        position: 0,
        role: role.to_string(),
        item_type: item_type.to_string(),
        text: (!text.is_empty()).then_some(text),
        tool_name: None,
        tool_arguments: None,
        tool_output: None,
        created_at_ns: 0,
    }
}

/// A short title for a new conversation, from the first user input line.
fn derive_title(req: &ResponsesRequest) -> Option<String> {
    let first = match &req.input {
        ResponsesInput::Text(s) => Some(s.clone()),
        ResponsesInput::Items(list) => list.iter().find_map(|i| match i {
            InputItem::Message { role, content } if role != "system" && role != "developer" => {
                Some(content.text())
            }
            _ => None,
        }),
    }?;
    let title: String = first.trim().chars().take(80).collect();
    (!title.is_empty()).then_some(title)
}

/// Translate a Responses request into a Rig [`CompletionRequest`]. `instructions`
/// becomes the preamble (the middleware merges it with the SeQL grounding).
fn build_completion_request(req: &ResponsesRequest) -> Result<CompletionRequest, String> {
    let mut history: Vec<Message> = Vec::new();
    match &req.input {
        ResponsesInput::Text(s) => history.push(Message::user(s.clone())),
        ResponsesInput::Items(items) => {
            for item in items {
                match item {
                    InputItem::Message { role, content } => {
                        history.push(message_for_role(role, content.text()));
                    }
                    InputItem::FunctionCall {
                        call_id,
                        name,
                        arguments,
                    } => history.push(Message::Assistant {
                        id: None,
                        content: OneOrMany::one(AssistantContent::ToolCall(ToolCall::new(
                            call_id.clone(),
                            ToolFunction::new(name.clone(), parse_arguments(arguments)),
                        ))),
                    }),
                    InputItem::FunctionCallOutput { call_id, output } => {
                        history.push(Message::User {
                            content: OneOrMany::one(UserContent::ToolResult(ToolResult {
                                id: call_id.clone(),
                                call_id: None,
                                content: OneOrMany::one(ToolResultContent::Text(Text::new(
                                    output.clone(),
                                ))),
                            })),
                        })
                    }
                    InputItem::Other => {}
                }
            }
        }
    }

    let chat_history = OneOrMany::many(history)
        .map_err(|_| "input must contain at least one message".to_string())?;
    let tools: Vec<ToolDefinition> = req
        .tools
        .iter()
        .filter_map(ResponsesTool::as_definition)
        .collect();

    // `instructions` becomes the preamble (the middleware merges it with the SeQL
    // grounding).
    let mut request = wire::base_completion_request(req.instructions.clone(), chat_history, tools);
    // A requested reasoning effort becomes `reasoning_effort` on the backing request; the
    // middleware reconciles it with the model if the value isn't supported.
    if let Some(effort) = req.reasoning.as_ref().and_then(|r| r.effort.as_deref()) {
        request.additional_params = Some(json!({ "reasoning_effort": effort }));
    }
    Ok(request)
}

/// A Rig chat [`Message`] for a plain-text item of the given role (`assistant` /
/// `system`|`developer` / everything-else-as-user). Shared by request translation and
/// server-side history replay.
fn message_for_role(role: &str, text: String) -> Message {
    match role {
        "assistant" => Message::assistant(text),
        "system" | "developer" => Message::System { content: text },
        _ => Message::User {
            content: OneOrMany::one(UserContent::Text(Text::new(text))),
        },
    }
}

/// Stream the agentic loop as Responses-API SSE events, surfacing every server tool
/// call and result as its own output item.
#[allow(clippy::too_many_arguments)]
fn stream_response<B>(
    model: SequinsAssistantModel<B>,
    model_id: String,
    request: CompletionRequest,
    response_id: String,
    conversation_id: Option<String>,
    persist: Option<PersistTurn>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>>
where
    B: CompletionModel + Send + Sync + 'static,
{
    let events = model.run_events(request);
    let body = async_stream::stream! {
        yield sse(
            "response.created",
            json!({ "type": "response.created", "response": response_object(&model_id, &response_id, conversation_id.as_deref(), "in_progress", &[], None) }),
        );

        let mut output: Vec<serde_json::Value> = Vec::new();
        let mut collected: Vec<AgentEvent> = Vec::new();
        let mut failed: Option<String> = None;
        futures::pin_mut!(events);
        while let Some(event) = events.next().await {
            if let AgentEvent::Error(msg) = &event {
                failed = Some(msg.clone());
                collected.push(event);
                continue;
            }
            let Some(item) = event_to_item(&event, output.len()) else { collected.push(event); continue };
            let index = output.len();
            yield sse(
                "response.output_item.added",
                json!({ "type": "response.output_item.added", "output_index": index, "item": item }),
            );
            if let AgentEvent::Text(text) = &event {
                yield sse(
                    "response.output_text.delta",
                    json!({ "type": "response.output_text.delta", "item_id": item["id"], "output_index": index, "delta": text }),
                );
            }
            yield sse(
                "response.output_item.done",
                json!({ "type": "response.output_item.done", "output_index": index, "item": item }),
            );
            output.push(item);
            collected.push(event);
        }

        // Persist the completed turn; the (possibly new) conversation id goes on the
        // terminal event so the client can continue via `previous_response_id`.
        let final_conversation = PersistTurn::finish(persist, &collected, conversation_id).await;

        if let Some(msg) = failed {
            let resp = response_object(&model_id, &response_id, final_conversation.as_deref(), "failed", &output, Some(&msg));
            yield sse("response.failed", json!({ "type": "response.failed", "response": resp }));
        } else {
            let resp = response_object(&model_id, &response_id, final_conversation.as_deref(), "completed", &output, None);
            yield sse("response.completed", json!({ "type": "response.completed", "response": resp }));
        }
    };
    Sse::new(body)
}

fn build_response_object(
    model_id: &str,
    response_id: &str,
    conversation_id: Option<&str>,
    events: &[AgentEvent],
) -> serde_json::Value {
    let mut output = Vec::new();
    let mut failed = None;
    for event in events {
        if let AgentEvent::Error(msg) = event {
            failed = Some(msg.clone());
            continue;
        }
        if let Some(item) = event_to_item(event, output.len()) {
            output.push(item);
        }
    }
    let status = if failed.is_some() {
        "failed"
    } else {
        "completed"
    };
    response_object(
        model_id,
        response_id,
        conversation_id,
        status,
        &output,
        failed.as_deref(),
    )
}

/// Map an [`AgentEvent`] to a Responses output item.
fn event_to_item(event: &AgentEvent, index: usize) -> Option<serde_json::Value> {
    match event {
        AgentEvent::Text(text) => Some(json!({
            "type": "message",
            "id": format!("msg_{index}"),
            "role": "assistant",
            "status": "completed",
            "content": [{ "type": "output_text", "text": text }],
        })),
        // Server-executed: a self-contained, already-resolved item (call + result).
        AgentEvent::ServerTool {
            id,
            name,
            arguments,
            output,
        } => Some(json!({
            "type": "sequins.tool_result",
            "id": id,
            "name": name,
            "arguments": arguments,
            "output": output,
            "status": "completed",
        })),
        // Client-provided: a function_call the client must execute and answer.
        AgentEvent::ClientTool {
            id,
            name,
            arguments,
        } => Some(json!({
            "type": "function_call",
            "id": format!("fc_{index}"),
            "call_id": id,
            "name": name,
            "arguments": serde_json::to_string(arguments).unwrap_or_default(),
            "status": "completed",
        })),
        AgentEvent::Error(_) => None,
    }
}

fn response_object(
    model_id: &str,
    response_id: &str,
    conversation_id: Option<&str>,
    status: &str,
    output: &[serde_json::Value],
    error: Option<&str>,
) -> serde_json::Value {
    let mut obj = json!({
        "id": response_id,
        "object": "response",
        "created_at": unix_secs(),
        "model": model_id,
        "status": status,
        "output": output,
        "usage": { "input_tokens": 0, "output_tokens": 0, "total_tokens": 0 },
    });
    if let Some(cid) = conversation_id {
        obj["conversation"] = json!(cid);
    }
    if let Some(msg) = error {
        obj["error"] = json!({ "message": msg });
    }
    obj
}

fn sse(event: &str, data: serde_json::Value) -> Result<Event, Infallible> {
    Ok(Event::default().event(event).data(data.to_string()))
}

// ---------------------------------------------------------------------------
// Assistant façade — one AgentEvent-style stream for Local and Remote, used by
// the FFI so Swift has a single interface regardless of backend.
// ---------------------------------------------------------------------------

/// A normalized chat event surfaced to native clients (the FFI). Server tool
/// activity is visible; `render_visualization` arrives as a `ClientTool`.
#[derive(Debug, Clone)]
pub enum ChatEvent {
    /// Assistant text.
    Text(String),
    /// A tool the server executed in-process (name, JSON arguments, rendered output).
    ServerTool {
        name: String,
        arguments: String,
        output: String,
    },
    /// A tool call for the client to handle (e.g. `render_visualization`).
    ClientTool { name: String, arguments: String },
    /// The turn finished; carries ids for continuation/persistence.
    Done {
        response_id: String,
        conversation_id: Option<String>,
    },
    /// The turn failed.
    Error(String),
}

/// A chat assistant that streams [`ChatEvent`]s, over Local (in-process model) or
/// Remote (the daemon's `/v1/responses`). Both are Rig `CompletionModel`s underneath.
pub enum Assistant<B: CompletionModel> {
    /// In-process: run the middleware model directly, persisting to `store`.
    Local {
        model: SequinsAssistantModel<B>,
        store: Option<Arc<AppStateStore>>,
    },
    /// Remote: POST the daemon's `/v1/responses` (base_url includes `/v1`).
    Remote {
        base_url: String,
        token: Option<String>,
        http: reqwest::Client,
    },
}

impl<B: CompletionModel> Assistant<B> {
    /// In-process assistant over `model`, persisting conversations to `store`.
    pub fn local(model: SequinsAssistantModel<B>, store: Option<Arc<AppStateStore>>) -> Self {
        Assistant::Local { model, store }
    }

    /// Remote assistant over the daemon's `/v1` at `base_url` (bearer `token`).
    pub fn remote(base_url: impl Into<String>, token: Option<String>) -> Self {
        Assistant::Remote {
            base_url: base_url.into(),
            token,
            http: reqwest::Client::new(),
        }
    }
}

impl<B: CompletionModel + Send + Sync + 'static> Assistant<B> {
    /// Run a chat turn (an OpenAI Responses-shaped request) as a `ChatEvent` stream.
    pub fn chat(
        &self,
        request: serde_json::Value,
    ) -> std::pin::Pin<Box<dyn Stream<Item = ChatEvent> + Send>> {
        match self {
            Assistant::Local { model, store } => {
                Box::pin(local_chat(model.clone(), store.clone(), request))
            }
            Assistant::Remote {
                base_url,
                token,
                http,
            } => Box::pin(remote_chat(
                base_url.clone(),
                token.clone(),
                http.clone(),
                request,
            )),
        }
    }
}

fn local_chat<B: CompletionModel + Send + Sync + 'static>(
    model: SequinsAssistantModel<B>,
    store: Option<Arc<AppStateStore>>,
    request: serde_json::Value,
) -> impl Stream<Item = ChatEvent> + Send {
    async_stream::stream! {
        let req: ResponsesRequest = match serde_json::from_value(request) {
            Ok(r) => r,
            Err(e) => { yield ChatEvent::Error(format!("invalid request: {e}")); return; }
        };
        let response_id = new_id("resp");
        let conversation_id = resolve_conversation(store.as_deref(), &req).await;
        let mut completion_req = match build_completion_request(&req) {
            Ok(r) => r,
            Err(m) => { yield ChatEvent::Error(m); return; }
        };
        if let (Some(s), Some(cid)) = (store.as_deref(), conversation_id.as_deref()) {
            if let Some(conv) = s.get_conversation(cid).await {
                prepend_history(&mut completion_req, &conv);
            }
        }
        let persist = PersistTurn::build(&store, &req, &conversation_id, &response_id);

        let events = model.run_events(completion_req);
        futures::pin_mut!(events);
        let mut collected = Vec::new();
        while let Some(event) = events.next().await {
            match &event {
                AgentEvent::Text(t) => yield ChatEvent::Text(t.clone()),
                AgentEvent::ServerTool { name, arguments, output, .. } => yield ChatEvent::ServerTool {
                    name: name.clone(),
                    arguments: arguments.to_string(),
                    output: output.clone(),
                },
                AgentEvent::ClientTool { name, arguments, .. } => yield ChatEvent::ClientTool {
                    name: name.clone(),
                    arguments: arguments.to_string(),
                },
                AgentEvent::Error(m) => yield ChatEvent::Error(m.clone()),
            }
            collected.push(event);
        }
        let final_conversation = PersistTurn::finish(persist, &collected, conversation_id).await;
        yield ChatEvent::Done { response_id, conversation_id: final_conversation };
    }
}

/// Map a single Responses `output` item (`message` / `sequins.tool_result` /
/// `function_call`) to a [`ChatEvent`]. Returns `None` for empty text or unknown
/// item types. Shared by the streaming relay and used to interpret persisted items.
fn output_item_to_event(item: &serde_json::Value) -> Option<ChatEvent> {
    match item["type"].as_str() {
        Some("message") => {
            let text: String = item["content"]
                .as_array()
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(|p| p["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("")
                })
                .unwrap_or_default();
            (!text.is_empty()).then_some(ChatEvent::Text(text))
        }
        Some("sequins.tool_result") => Some(ChatEvent::ServerTool {
            name: item["name"].as_str().unwrap_or_default().to_string(),
            arguments: item["arguments"].to_string(),
            output: item["output"].as_str().unwrap_or_default().to_string(),
        }),
        Some("function_call") => Some(ChatEvent::ClientTool {
            name: item["name"].as_str().unwrap_or_default().to_string(),
            arguments: item["arguments"].as_str().unwrap_or_default().to_string(),
        }),
        _ => None,
    }
}

/// Continuation ids from a terminal `response.completed` / `response.failed` event's
/// nested `response` object.
fn done_from_response(resp: &serde_json::Value) -> ChatEvent {
    ChatEvent::Done {
        response_id: resp["id"].as_str().unwrap_or_default().to_string(),
        conversation_id: resp["conversation"].as_str().map(str::to_string),
    }
}

/// Remote chat: a **streaming** POST to `/v1/responses` (`stream: true`), consuming the
/// daemon's Server-Sent Events and relaying each output item as a [`ChatEvent`] as it
/// arrives — so the remote path streams just like the local one. Terminal ids come from
/// the final `response.completed` / `response.failed` event.
fn remote_chat(
    base_url: String,
    token: Option<String>,
    http: reqwest::Client,
    request: serde_json::Value,
) -> impl Stream<Item = ChatEvent> + Send {
    use eventsource_stream::Eventsource;
    async_stream::stream! {
        let url = format!("{}/responses", base_url.trim_end_matches('/'));
        let mut body = request;
        body["stream"] = json!(true);

        let mut builder = http.post(&url).json(&body);
        if let Some(t) = &token {
            builder = builder.bearer_auth(t);
        }
        let resp = match builder.send().await {
            Ok(r) => match r.error_for_status() {
                Ok(r) => r,
                Err(e) => { yield ChatEvent::Error(format!("remote status: {e}")); return; }
            },
            Err(e) => { yield ChatEvent::Error(format!("remote request: {e}")); return; }
        };

        let mut events = resp.bytes_stream().eventsource();
        let mut done_emitted = false;
        while let Some(next) = events.next().await {
            let event = match next {
                Ok(e) => e,
                Err(e) => { yield ChatEvent::Error(format!("remote stream: {e}")); break; }
            };
            let data: serde_json::Value = match serde_json::from_str(&event.data) {
                Ok(v) => v,
                Err(_) => continue,
            };
            match event.event.as_str() {
                "response.output_item.done" => {
                    if let Some(ev) = output_item_to_event(&data["item"]) {
                        yield ev;
                    }
                }
                "response.completed" => {
                    yield done_from_response(&data["response"]);
                    done_emitted = true;
                    break;
                }
                "response.failed" => {
                    yield ChatEvent::Error(
                        data["response"]["error"]["message"]
                            .as_str()
                            .unwrap_or("remote error")
                            .to_string(),
                    );
                    yield done_from_response(&data["response"]);
                    done_emitted = true;
                    break;
                }
                _ => {}
            }
        }
        if !done_emitted {
            // The stream closed without a terminal event — still emit a Done so callers
            // (and the FFI stream) terminate cleanly.
            yield ChatEvent::Done { response_id: String::new(), conversation_id: None };
        }
    }
}

// ---------------------------------------------------------------------------
// Request wire types (Responses API subset)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ResponsesRequest {
    model: String,
    input: ResponsesInput,
    #[serde(default)]
    instructions: Option<String>,
    #[serde(default)]
    tools: Vec<ResponsesTool>,
    #[serde(default)]
    stream: bool,
    /// Persist this turn (default true). When false, nothing is written.
    #[serde(default)]
    store: Option<bool>,
    /// Continue the conversation containing this prior response.
    #[serde(default)]
    previous_response_id: Option<String>,
    /// Explicit conversation id to append to.
    #[serde(default)]
    conversation: Option<String>,
    /// Reasoning controls (Responses API `reasoning: { effort }`). The effort is applied
    /// as `reasoning_effort` on the backing Chat Completions request.
    #[serde(default)]
    reasoning: Option<ReasoningConfig>,
}

#[derive(Debug, Deserialize)]
struct ReasoningConfig {
    #[serde(default)]
    effort: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ResponsesInput {
    Text(String),
    Items(Vec<InputItem>),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum InputItem {
    #[serde(rename = "message")]
    Message {
        #[serde(default)]
        role: String,
        content: InputContent,
    },
    #[serde(rename = "function_call")]
    FunctionCall {
        #[serde(default)]
        call_id: String,
        name: String,
        #[serde(default)]
        arguments: String,
    },
    #[serde(rename = "function_call_output")]
    FunctionCallOutput {
        #[serde(default)]
        call_id: String,
        #[serde(default)]
        output: String,
    },
    /// Any other item type (reasoning, etc.) is ignored.
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum InputContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

impl InputContent {
    fn text(&self) -> String {
        match self {
            InputContent::Text(s) => s.clone(),
            InputContent::Parts(parts) => parts
                .iter()
                .filter_map(|p| p.text.clone())
                .collect::<Vec<_>>()
                .join(""),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ContentPart {
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponsesTool {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    parameters: Option<serde_json::Value>,
}

impl ResponsesTool {
    fn as_definition(&self) -> Option<ToolDefinition> {
        let name = self.name.clone()?;
        Some(wire::tool_definition(
            name,
            self.description.clone(),
            self.parameters.clone(),
        ))
    }
}

#[cfg(test)]
mod stream_relay_tests {
    use super::*;

    #[test]
    fn maps_message_item_to_text() {
        let item = json!({
            "type": "message",
            "content": [{ "type": "output_text", "text": "hello " }, { "text": "world" }],
        });
        match output_item_to_event(&item) {
            Some(ChatEvent::Text(t)) => assert_eq!(t, "hello world"),
            other => panic!("expected Text, got {other:?}"),
        }
        // Empty text yields nothing.
        assert!(output_item_to_event(&json!({ "type": "message", "content": [] })).is_none());
    }

    #[test]
    fn maps_server_and_client_tool_items() {
        let server = json!({
            "type": "sequins.tool_result",
            "name": "run_sql",
            "arguments": { "sql": "SELECT 1" },
            "output": "1 row",
        });
        match output_item_to_event(&server) {
            Some(ChatEvent::ServerTool {
                name,
                arguments,
                output,
            }) => {
                assert_eq!(name, "run_sql");
                assert_eq!(output, "1 row");
                assert!(arguments.contains("SELECT 1"));
            }
            other => panic!("expected ServerTool, got {other:?}"),
        }

        let client = json!({
            "type": "function_call",
            "name": "render_visualization",
            "arguments": "{\"query\":\"spans last 1h\"}",
        });
        match output_item_to_event(&client) {
            Some(ChatEvent::ClientTool { name, arguments }) => {
                assert_eq!(name, "render_visualization");
                assert_eq!(arguments, "{\"query\":\"spans last 1h\"}");
            }
            other => panic!("expected ClientTool, got {other:?}"),
        }

        assert!(output_item_to_event(&json!({ "type": "unknown" })).is_none());
    }

    #[test]
    fn done_reads_ids_from_response() {
        let resp = json!({ "id": "resp_1", "conversation": "conv_9" });
        match done_from_response(&resp) {
            ChatEvent::Done {
                response_id,
                conversation_id,
            } => {
                assert_eq!(response_id, "resp_1");
                assert_eq!(conversation_id.as_deref(), Some("conv_9"));
            }
            other => panic!("expected Done, got {other:?}"),
        }
        // Missing conversation → None.
        match done_from_response(&json!({ "id": "resp_2" })) {
            ChatEvent::Done {
                conversation_id, ..
            } => assert!(conversation_id.is_none()),
            other => panic!("expected Done, got {other:?}"),
        }
    }
}
