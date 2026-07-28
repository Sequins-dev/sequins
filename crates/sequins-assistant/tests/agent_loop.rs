//! Middleware-loop and OpenAI-endpoint tests using a **fake backing model** — no
//! network, no real LLM. They prove the tool-injecting loop routes our tools vs. a
//! caller's tools correctly, and that the `/v1` surface behaves per the OpenAI spec.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use indexmap::IndexMap;
use rig::completion::{
    CompletionError, CompletionModel, CompletionRequest, CompletionResponse, Usage,
};
use rig::message::{AssistantContent, Message, Text, ToolCall, ToolFunction, UserContent};
use rig::streaming::StreamingCompletionResponse;
use rig::OneOrMany;
use sequins_assistant::model::MiddlewareStreamResponse;
use sequins_assistant::{completion_model_router, responses_router, SequinsAssistantModel, Tools};
use sequins_datafusion_backend::DataFusionBackend;
use sequins_storage::test_fixtures::{make_test_otlp_traces, TestStorageBuilder};
use sequins_traits::OtlpIngest;
use serde_json::json;
use tower::ServiceExt;

/// A scripted backing model: each `completion` pops the next canned choice and
/// records the request it received (so tests can assert on tool/preamble injection).
#[derive(Clone)]
struct FakeModel {
    script: Arc<Mutex<VecDeque<Vec<AssistantContent>>>>,
    seen: Arc<Mutex<Vec<CompletionRequest>>>,
}

impl FakeModel {
    fn new(script: Vec<Vec<AssistantContent>>) -> Self {
        Self {
            script: Arc::new(Mutex::new(script.into())),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl CompletionModel for FakeModel {
    type Response = ();
    type StreamingResponse = MiddlewareStreamResponse;
    type Client = ();

    fn make(_client: &Self::Client, _model: impl Into<String>) -> Self {
        unreachable!("test double is constructed directly")
    }

    async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse<()>, CompletionError> {
        self.seen.lock().unwrap().push(request);
        let next = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| vec![AssistantContent::Text(Text::new("(exhausted)"))]);
        Ok(CompletionResponse {
            choice: OneOrMany::many(next).expect("non-empty canned choice"),
            usage: Usage::new(),
            raw_response: (),
            message_id: None,
        })
    }

    async fn stream(
        &self,
        _request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse<MiddlewareStreamResponse>, CompletionError> {
        unreachable!("streaming path not exercised by these tests")
    }
}

fn tool_call(id: &str, name: &str, args: serde_json::Value) -> AssistantContent {
    AssistantContent::ToolCall(ToolCall::new(
        id.into(),
        ToolFunction::new(name.into(), args),
    ))
}

fn user_request(prompt: &str) -> CompletionRequest {
    CompletionRequest {
        model: None,
        preamble: None,
        chat_history: OneOrMany::one(Message::user(prompt)),
        documents: Vec::new(),
        tools: Vec::new(),
        temperature: None,
        max_tokens: None,
        tool_choice: None,
        additional_params: None,
        output_schema: None,
    }
}

fn text_of<R>(response: &CompletionResponse<R>) -> String {
    response
        .choice
        .iter()
        .filter_map(|c| match c {
            AssistantContent::Text(t) => Some(t.text.clone()),
            _ => None,
        })
        .collect()
}

fn has_tool(response_calls: &CompletionResponse<()>, name: &str) -> bool {
    response_calls
        .choice
        .iter()
        .any(|c| matches!(c, AssistantContent::ToolCall(tc) if tc.function.name == name))
}

fn request_has_tool_result(request: &CompletionRequest) -> bool {
    request.chat_history.iter().any(|m| {
        matches!(m, Message::User { content }
            if content.iter().any(|u| matches!(u, UserContent::ToolResult(_))))
    })
}

async fn seeded_tools(n_spans: usize) -> (Tools, tempfile::TempDir) {
    let (storage, temp) = TestStorageBuilder::new().build().await;
    storage
        .ingest_traces(make_test_otlp_traces(1, n_spans))
        .await
        .unwrap();
    let backend = Arc::new(DataFusionBackend::new(Arc::new(storage)));
    (Tools::new(backend), temp)
}

#[tokio::test]
async fn our_tool_executes_then_loops_to_text() {
    let (tools, _t) = seeded_tools(5).await;
    let fake = FakeModel::new(vec![
        vec![tool_call(
            "1",
            "run_sql",
            json!({ "sql": "SELECT count(*) AS c FROM spans" }),
        )],
        vec![AssistantContent::Text(Text::new("There are spans."))],
    ]);
    let model = SequinsAssistantModel::new(fake.clone(), tools);

    let response = model
        .completion(user_request("how many spans?"))
        .await
        .unwrap();

    assert!(text_of(&response).contains("There are spans."));

    let seen = fake.seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "backing model called once per round");
    // First request had our tools + grounding injected.
    assert!(seen[0].tools.iter().any(|t| t.name == "run_sql"));
    assert!(seen[0].preamble.as_ref().unwrap().contains("Sequins"));
    // Second request carried the executed tool's result.
    assert!(request_has_tool_result(&seen[1]));
}

#[tokio::test]
async fn caller_tool_is_handed_back_not_executed() {
    let (tools, _t) = seeded_tools(1).await;
    let fake = FakeModel::new(vec![vec![tool_call(
        "x",
        "render_visualization",
        json!({ "query": "spans last 1h" }),
    )]]);
    let model = SequinsAssistantModel::new(fake.clone(), tools);

    let response = model.completion(user_request("chart it")).await.unwrap();

    assert!(has_tool(&response, "render_visualization"));
    assert_eq!(fake.seen.lock().unwrap().len(), 1, "no extra rounds");
}

#[tokio::test]
async fn mixed_turn_strips_our_tool_calls() {
    let (tools, _t) = seeded_tools(1).await;
    let fake = FakeModel::new(vec![vec![
        tool_call("a", "run_sql", json!({ "sql": "SELECT 1" })),
        tool_call("b", "render_visualization", json!({ "query": "spans" })),
    ]]);
    let model = SequinsAssistantModel::new(fake, tools);

    let response = model.completion(user_request("chart")).await.unwrap();

    assert!(has_tool(&response, "render_visualization"));
    assert!(
        !has_tool(&response, "run_sql"),
        "our tool call must be stripped from the caller-facing response"
    );
}

#[tokio::test]
async fn backing_never_sees_public_model_id_as_override() {
    // Regression: the caller's `model` is our public registry id (e.g. "default"),
    // not the backing provider's model name. If forwarded as a `request.model`
    // override, a real provider 404s with "model does not exist / no access".
    let (tools, _t) = seeded_tools(1).await;
    let fake = FakeModel::new(vec![vec![AssistantContent::Text(Text::new("ok"))]]);
    let model = SequinsAssistantModel::new(fake.clone(), tools);

    let mut req = user_request("hi");
    req.model = Some("default".into());
    let _ = model.completion(req).await.unwrap();

    assert_eq!(
        fake.seen.lock().unwrap()[0].model,
        None,
        "the public model id must be cleared before reaching the backing model"
    );
}

#[tokio::test]
async fn text_only_passes_through() {
    let (tools, _t) = seeded_tools(1).await;
    let fake = FakeModel::new(vec![vec![AssistantContent::Text(Text::new("hello"))]]);
    let model = SequinsAssistantModel::new(fake.clone(), tools);

    let response = model.completion(user_request("hi")).await.unwrap();

    assert_eq!(text_of(&response), "hello");
    assert_eq!(fake.seen.lock().unwrap().len(), 1);
}

async fn body_json(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, value)
}

fn router_with_model(tools: Tools) -> axum::Router {
    let model = SequinsAssistantModel::new(
        FakeModel::new(vec![vec![AssistantContent::Text(Text::new("pong"))]]),
        tools,
    );
    let mut models = IndexMap::new();
    models.insert("sequins".to_string(), model);
    completion_model_router(models)
}

#[tokio::test]
async fn v1_models_lists_configured_models() {
    let (tools, _t) = seeded_tools(1).await;
    let app = router_with_model(tools);

    let resp = app
        .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"][0]["id"], "sequins");
}

#[tokio::test]
async fn v1_models_empty_when_none_configured() {
    let app = completion_model_router(IndexMap::<String, SequinsAssistantModel<FakeModel>>::new());

    let resp = app
        .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (status, body) = body_json(resp).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn unknown_model_returns_model_not_found() {
    let (tools, _t) = seeded_tools(1).await;
    let app = router_with_model(tools);

    let req = Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "model": "nope", "messages": [{ "role": "user", "content": "hi" }] })
                .to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "model_not_found");
}

#[tokio::test]
async fn chat_completion_returns_assistant_text() {
    let (tools, _t) = seeded_tools(1).await;
    let app = router_with_model(tools);

    let req = Request::post("/v1/chat/completions")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "model": "sequins", "messages": [{ "role": "user", "content": "ping" }] })
                .to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["choices"][0]["message"]["content"], "pong");
    assert_eq!(body["choices"][0]["finish_reason"], "stop");
}

fn responses_router_with_server_tool(tools: Tools) -> axum::Router {
    // First round calls our run_sql tool; second round answers with text — so the
    // loop executes a *server* tool the Responses stream should surface.
    let model = SequinsAssistantModel::new(
        FakeModel::new(vec![
            vec![tool_call(
                "1",
                "run_sql",
                json!({ "sql": "SELECT count(*) AS c FROM spans" }),
            )],
            vec![AssistantContent::Text(Text::new("There are spans."))],
        ]),
        tools,
    );
    let mut models = IndexMap::new();
    models.insert("default".to_string(), model);
    responses_router(models, None)
}

#[tokio::test]
async fn responses_surfaces_server_tool_items() {
    let (tools, _t) = seeded_tools(3).await;
    let app = responses_router_with_server_tool(tools);

    let req = Request::post("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "model": "default", "input": "how many spans?" }).to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;

    assert_eq!(status, StatusCode::OK);
    let output = body["output"].as_array().expect("output array");
    assert!(
        output
            .iter()
            .any(|i| i["type"] == "sequins.tool_result" && i["name"] == "run_sql"),
        "server tool result must be surfaced: {body}"
    );
    assert!(
        output.iter().any(|i| i["type"] == "message"),
        "final message must be present: {body}"
    );
}

#[tokio::test]
async fn responses_persist_and_continue_conversation() {
    use sequins_assistant::responses_router;
    use sequins_metadata::AppStateStore;
    use std::sync::Arc;

    let (tools, _t) = seeded_tools(1).await;
    let fake = FakeModel::new(vec![
        vec![AssistantContent::Text(Text::new("Nice to meet you, Sam."))],
        vec![AssistantContent::Text(Text::new("Your name is Sam."))],
    ]);
    let model = SequinsAssistantModel::new(fake.clone(), tools);
    let mut models = IndexMap::new();
    models.insert("default".to_string(), model);

    let store = Arc::new(AppStateStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "app_state",
    ));
    let app = responses_router(models, Some(store.clone()));

    // Turn 1 — new conversation.
    let (s1, b1) = body_json(
        app.clone()
            .oneshot(
                Request::post("/v1/responses")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({ "model": "default", "input": "My name is Sam." }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    let rid1 = b1["id"].as_str().unwrap().to_string();
    let cid = b1["conversation"].as_str().unwrap().to_string();

    // Turn 2 — continue via previous_response_id.
    let (s2, b2) = body_json(
        app.clone()
            .oneshot(
                Request::post("/v1/responses")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({ "model": "default", "input": "What is my name?", "previous_response_id": rid1 })
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2["conversation"].as_str(), Some(cid.as_str()));

    // Both turns persisted (2 user inputs + 2 assistant messages).
    let conv = store.get_conversation(&cid).await.unwrap();
    assert_eq!(conv.items.len(), 4, "conv items: {:?}", conv.items);

    // Turn 2's request to the model was prepended with the prior exchange.
    let seen = fake.seen.lock().unwrap();
    let turn2 = &seen[1];
    let has_prior = turn2
        .chat_history
        .iter()
        .any(|m| matches!(m, Message::User { content }
            if content.iter().any(|c| matches!(c, UserContent::Text(t) if t.text.contains("My name is Sam")))));
    assert!(has_prior, "turn 2 should include prior history");
}

#[tokio::test]
async fn responses_streams_created_tool_and_completed() {
    let (tools, _t) = seeded_tools(3).await;
    let app = responses_router_with_server_tool(tools);

    let req = Request::post("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "model": "default", "input": "how many spans?", "stream": true }).to_string(),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let sse = String::from_utf8(bytes.to_vec()).unwrap();

    assert!(sse.contains("response.created"), "{sse}");
    assert!(sse.contains("sequins.tool_result"), "{sse}");
    assert!(sse.contains("response.completed"), "{sse}");
}

/// End-to-end: `Assistant::Remote` consumes the daemon's `/v1/responses` SSE stream
/// incrementally, relaying the server-executed tool and the assistant text as
/// `ChatEvent`s and finishing with `Done` — proving the remote path streams like local.
#[tokio::test]
async fn remote_assistant_streams_over_sse() {
    use futures::StreamExt;
    use sequins_assistant::{Assistant, ChatEvent};

    let (tools, _t) = seeded_tools(3).await;
    let app = responses_router_with_server_tool(tools);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let assistant = Assistant::<FakeModel>::remote(format!("http://{addr}/v1"), None);
    let request = json!({ "model": "default", "input": "how many spans?" });
    let mut stream = assistant.chat(request);

    let mut server_tools = 0;
    let mut texts: Vec<String> = Vec::new();
    let mut done = false;
    while let Some(event) = stream.next().await {
        match event {
            ChatEvent::ServerTool { name, .. } => {
                assert_eq!(name, "run_sql");
                server_tools += 1;
            }
            ChatEvent::Text(t) => texts.push(t),
            ChatEvent::Done { .. } => {
                done = true;
                break;
            }
            ChatEvent::Error(e) => panic!("unexpected error: {e}"),
            ChatEvent::ClientTool { .. } => {}
        }
    }

    assert_eq!(
        server_tools, 1,
        "should surface the server-executed run_sql tool"
    );
    assert!(
        texts.iter().any(|t| t.contains("spans")),
        "should stream the assistant text, got {texts:?}"
    );
    assert!(done, "should emit a terminal Done");
}
