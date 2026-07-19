//! [`SequinsAssistantModel`] — a tool-injecting middleware [`CompletionModel`].
//!
//! It wraps a real *backing* model and, on every request, injects our in-process
//! tool set + SeQL grounding, then runs the agentic loop **in-process**:
//!
//! - the backing model calls one of **our** tools → we execute it against DataFusion,
//!   append the result, and loop;
//! - the backing model calls one of the **caller's** tools (or returns text) → we hand
//!   the response straight back for the caller's own agent to run.
//!
//! Because it *is* a `CompletionModel`, it drops into any Rig `Agent` and is served
//! over the OpenAI-compatible endpoint by [`crate::serve`]. Its own tool calls are
//! stripped from returned responses so a caller only ever sees its own tools.

use rig::completion::{
    CompletionError, CompletionModel, CompletionRequest, CompletionResponse, GetTokenUsage, Usage,
};
use rig::message::{
    AssistantContent, Message, Text, ToolCall, ToolResult, ToolResultContent, UserContent,
};
use rig::streaming::{RawStreamingChoice, RawStreamingToolCall, StreamingCompletionResponse};
use rig::OneOrMany;
use serde::{Deserialize, Serialize};

use crate::tools::{registry, Tools};

/// Default cap on in-process tool-call rounds before we force a final text answer.
const DEFAULT_MAX_TOOL_TURNS: usize = 16;

/// A tool-injecting middleware model over a backing [`CompletionModel`] `B`.
#[derive(Clone)]
pub struct SequinsAssistantModel<B: CompletionModel> {
    backing: B,
    tools: Tools,
    grounding: std::sync::Arc<str>,
    max_tool_turns: usize,
}

impl<B: CompletionModel> SequinsAssistantModel<B> {
    /// Wrap `backing` with the Sequins tool set over `tools`.
    pub fn new(backing: B, tools: Tools) -> Self {
        Self {
            backing,
            tools,
            grounding: std::sync::Arc::from(default_grounding()),
            max_tool_turns: DEFAULT_MAX_TOOL_TURNS,
        }
    }

    /// Override the SeQL system grounding prepended to every request.
    pub fn with_grounding(mut self, grounding: impl Into<String>) -> Self {
        self.grounding = std::sync::Arc::from(grounding.into());
        self
    }

    /// Override the in-process tool-call round cap.
    pub fn with_max_tool_turns(mut self, turns: usize) -> Self {
        self.max_tool_turns = turns.max(1);
        self
    }

    /// Inject our grounding into the preamble and our tool defs into the request,
    /// without dropping anything the caller supplied.
    fn inject(&self, request: &mut CompletionRequest) {
        // The caller's `model` is our *public* registry id (e.g. "default"), not the
        // backing provider's model name. Clear it so the backing model uses the name
        // it was built with — otherwise Rig forwards "default" as a model override and
        // the real provider 404s with "model does not exist / no access".
        request.model = None;
        request.preamble = Some(match request.preamble.take() {
            Some(p) if !p.trim().is_empty() => format!("{}\n\n---\n\n{}", self.grounding, p),
            _ => self.grounding.to_string(),
        });
        for def in registry::tool_definitions() {
            if !request.tools.iter().any(|t| t.name == def.name) {
                request.tools.push(def);
            }
        }
    }

    /// The core agentic loop, emitting an [`AgentEvent`] for every step: interim
    /// text, each **server** tool call *with its result* (executed in-process), and
    /// each **client** tool call (foreign, for the caller to run). Returns the
    /// terminal backing response (text + client tool calls, with our tools stripped)
    /// so the chat-completions path can preserve provider usage.
    ///
    /// - `completion()` drives this with a no-op sink → server tools stay hidden.
    /// - [`run_events`](Self::run_events) drives it with a channel → the Responses
    ///   endpoint surfaces every server tool call and result to the client.
    async fn run_loop(
        &self,
        mut request: CompletionRequest,
        mut emit: impl FnMut(AgentEvent),
    ) -> Result<CompletionResponse<B::Response>, CompletionError> {
        self.inject(&mut request);

        for _ in 0..self.max_tool_turns {
            let response = self.backing.completion(request.clone()).await?;

            let mut our_calls: Vec<ToolCall> = Vec::new();
            let mut has_foreign = false;
            for content in response.choice.iter() {
                if let AssistantContent::ToolCall(tc) = content {
                    if registry::is_ours(&tc.function.name) {
                        our_calls.push(tc.clone());
                    } else {
                        has_foreign = true;
                    }
                }
            }

            // Terminal turn: a client tool call (or plain text) ends the loop.
            if has_foreign || our_calls.is_empty() {
                for content in response.choice.iter() {
                    match content {
                        AssistantContent::Text(t) if !t.text.is_empty() => {
                            emit(AgentEvent::Text(t.text.clone()));
                        }
                        AssistantContent::ToolCall(tc) if !registry::is_ours(&tc.function.name) => {
                            emit(AgentEvent::ClientTool {
                                id: tc.id.clone(),
                                name: tc.function.name.clone(),
                                arguments: tc.function.arguments.clone(),
                            });
                        }
                        _ => {}
                    }
                }
                return Ok(strip_our_tool_calls(response));
            }

            // Otherwise surface any interim text, execute our tools, and continue.
            for content in response.choice.iter() {
                if let AssistantContent::Text(t) = content {
                    if !t.text.is_empty() {
                        emit(AgentEvent::Text(t.text.clone()));
                    }
                }
            }
            let assistant_content: Vec<AssistantContent> = our_calls
                .iter()
                .cloned()
                .map(AssistantContent::ToolCall)
                .collect();
            request.chat_history.push(Message::Assistant {
                id: response_id(&response),
                content: OneOrMany::many(assistant_content)
                    .expect("our_calls is non-empty in this branch"),
            });
            for call in &our_calls {
                let rendered = registry::invoke(
                    &self.tools,
                    &call.function.name,
                    call.function.arguments.clone(),
                )
                .await
                .unwrap_or_else(|e| format!("Error: {e}"));
                emit(AgentEvent::ServerTool {
                    id: call.id.clone(),
                    name: call.function.name.clone(),
                    arguments: call.function.arguments.clone(),
                    output: rendered.clone(),
                });
                request.chat_history.push(Message::User {
                    content: OneOrMany::one(UserContent::ToolResult(ToolResult {
                        id: call.id.clone(),
                        call_id: call.call_id.clone(),
                        content: OneOrMany::one(ToolResultContent::Text(Text::new(rendered))),
                    })),
                });
            }
        }

        // Exhausted the tool-turn budget: one final call with no tools, forcing text.
        request.tools.clear();
        let response = self.backing.completion(request).await?;
        for content in response.choice.iter() {
            if let AssistantContent::Text(t) = content {
                if !t.text.is_empty() {
                    emit(AgentEvent::Text(t.text.clone()));
                }
            }
        }
        Ok(response)
    }

    /// Run the agentic loop as a live [`AgentEvent`] stream — used by the Responses
    /// endpoint to surface server-side tool activity to the client as it happens.
    pub fn run_events(
        &self,
        request: CompletionRequest,
    ) -> impl futures::Stream<Item = AgentEvent> + Send + 'static
    where
        B: 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let this = self.clone();
        tokio::spawn(async move {
            let events = tx.clone();
            let result = this
                .run_loop(request, move |event| {
                    let _ = events.send(event);
                })
                .await;
            if let Err(err) = result {
                let _ = tx.send(AgentEvent::Error(err.to_string()));
            }
        });
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
    }
}

/// A step in the assistant's agentic loop, surfaced to Responses-API clients so
/// they can render server-executed tool activity they didn't drive themselves.
#[derive(Debug, Clone)]
pub enum AgentEvent {
    /// Assistant text (interim or final).
    Text(String),
    /// A tool the **server** executed in-process, with its rendered result.
    ServerTool {
        id: String,
        name: String,
        arguments: serde_json::Value,
        output: String,
    },
    /// A tool the **client** must execute (foreign / caller-provided).
    ClientTool {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },
    /// The loop failed; carries the error message.
    Error(String),
}

impl<B: CompletionModel> CompletionModel for SequinsAssistantModel<B> {
    type Response = B::Response;
    type StreamingResponse = MiddlewareStreamResponse;
    type Client = ();

    /// Not constructible from a bare client — [`SequinsAssistantModel`] wraps an
    /// already-built backing model, so use [`SequinsAssistantModel::new`] instead.
    fn make(_client: &Self::Client, _model: impl Into<String>) -> Self {
        unreachable!("SequinsAssistantModel is built via `new`, not `CompletionModel::make`")
    }

    async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse<Self::Response>, CompletionError> {
        // The chat-completions path hides server-side tool activity (ignores events).
        self.run_loop(request, |_| {}).await
    }

    async fn stream(
        &self,
        request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse<Self::StreamingResponse>, CompletionError> {
        // Run the full (non-streamed) middleware loop, then replay the final choice
        // as streaming chunks. Token-by-token streaming is a later refinement.
        let response = self.completion(request).await?;
        let mut chunks: Vec<Result<RawStreamingChoice<MiddlewareStreamResponse>, CompletionError>> =
            Vec::new();
        for content in response.choice.iter() {
            match content {
                AssistantContent::Text(t) => {
                    chunks.push(Ok(RawStreamingChoice::Message(t.text.clone())));
                }
                AssistantContent::ToolCall(tc) => {
                    chunks.push(Ok(RawStreamingChoice::ToolCall(RawStreamingToolCall::new(
                        tc.id.clone(),
                        tc.function.name.clone(),
                        tc.function.arguments.clone(),
                    ))));
                }
                _ => {}
            }
        }
        let stream = futures::stream::iter(chunks);
        Ok(StreamingCompletionResponse::stream(Box::pin(stream)))
    }
}

/// The streaming "raw response" for [`SequinsAssistantModel`]. Carries no provider
/// usage of its own (the middleware aggregates the backing model's rounds).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MiddlewareStreamResponse;

impl GetTokenUsage for MiddlewareStreamResponse {
    fn token_usage(&self) -> Usage {
        Usage::new()
    }
}

/// Return a copy of `response` with any of *our* tool calls removed from its choice,
/// so a caller only ever sees text and its own tool calls.
fn strip_our_tool_calls<R>(response: CompletionResponse<R>) -> CompletionResponse<R> {
    let kept: Vec<AssistantContent> = response
        .choice
        .iter()
        .filter(|c| {
            !matches!(c, AssistantContent::ToolCall(tc) if registry::is_ours(&tc.function.name))
        })
        .cloned()
        .collect();
    let choice = OneOrMany::many(kept)
        .unwrap_or_else(|_| OneOrMany::one(AssistantContent::Text(Text::new(String::new()))));
    CompletionResponse {
        choice,
        usage: response.usage,
        raw_response: response.raw_response,
        message_id: response.message_id,
    }
}

/// Best-effort assistant message id (providers don't always supply one).
fn response_id<R>(_response: &CompletionResponse<R>) -> Option<String> {
    None
}

/// The default SeQL system grounding. Includes a compact SeQL syntax cheatsheet with
/// worked examples so a backing model unfamiliar with this DSL writes valid queries on
/// the first try instead of looping on `validate_seql`. The daemon can override it with
/// a fuller prompt seeded from the SeQL spec.
fn default_grounding() -> String {
    r#"You are the Sequins telemetry assistant. You explore an observability database
(spans, logs, metrics, profiles) and answer questions using **SeQL**, a pipeline query
language (NOT SQL). Do not write SQL in SeQL queries.

## SeQL syntax (follow exactly)

Every query begins with a signal and a mandatory time scope, then optional `|` stages:

    <signal> <time-scope> [ | <stage> ]*

- Signals: `spans`, `logs`, `metrics`, `datapoints`, `histograms`, `profiles`,
  `samples`, `resources`, `scopes`, `span_links`, `span_events`.
- Time scope (required, right after the signal): `last <dur>` | `today` | `yesterday` |
  `between(<start_ns>, <end_ns>)`. Durations are a single integer + unit
  (`ms`,`s`,`m`,`h`,`d`) — e.g. `15m`, `1h`, `7d`. NO compound durations like `1h30m`.
- Stages (chained with `|`):
  - `where <predicate>` — e.g. `where status == 2`, `where severity_number >= 9`,
    `where attr.http_status_code >= 500`. Combine with `and`/`or`.
  - `group by { <keys> } { <aggregations> }` — aggregation. Empty keys `{}` = one scalar
    row. Aggregations: `count()`, `avg`, `sum`, `min`, `max`, `p50/p95/p99(<col>)`,
    `percentile(<col>, <q>)` (q in 0..1, e.g. 0.90), `stddev(<col>)`, `variance(<col>)`,
    `distinct(<col>)`, `error_rate()`, `throughput()` (a per-second rate, scaled to the
    window/bucket), each aliased `as <name>`; a conditional count is
    `count() where <pred> as <name>`.
  - For a **time series** (line chart over time), bucket time in the group keys. Prefer a
    RANGE-RELATIVE bucket so the chart scales to whatever window is selected:
    `ts() bin 10% as bucket` (≈10 buckets) or `ts() bin auto as bucket`. A fixed
    `ts() bin 5m` also works but won't adapt to the range.
  - `window { <fn> as <name>, … }` — window functions over the time-ordered result (use
    AFTER a time-bucketed `group by`): `moving_avg(<col>, <n>)`, `cumulative(<col>)`,
    `delta(<col>)` (period-over-period change).
  - `take <n>` — limit rows. `select <cols>` — project columns.

Time note: on a **dashboard**, the panel's selected range and live toggle are applied for
you (they override the query's `last <dur>`), so always include a sensible leading scope
like `last 1h` and use `ts() bin 10%` — the chart then follows the dashboard's range.

## Examples

- Recent errored spans:        `spans last 1h | where status == 2 | take 100`
- Total + error count (scalar): `spans last 1h | group by {} { count() as total, count() where status == 2 as errors }`
- Count by service:            `spans last 1h | group by { service_name } { count() as n }`
- Spans per bucket (timeseries):`spans last 15m | group by { ts() bin 10% as bucket } { count() as span_count }`
- Latency percentiles:         `spans last 1h | group by {} { p95(duration_ns) as p95, percentile(duration_ns, 0.9) as p90, stddev(duration_ns) as sd }`
- Throughput by service:       `spans last 1h | group by { service_name } { throughput() as rps }`
- Error-rate moving average:   `spans last 1h | group by { ts() bin auto as bucket } { error_rate() as er } | window { moving_avg(er, 5) as er_ma }`

## Workflow

Ground names/values with `list_tables` / `describe_schema` / `column_profile` when unsure
rather than guessing. Write ONE SeQL query using the rules above. If the request is
ambiguous, state the interpretation you chose in one line and offer a refinement. If
`validate_seql` fails, read `error.message`/`offset`, FIX the query per the syntax above,
and validate AT MOST once or twice more — do not loop.

Client tools for showing results (call with the final SeQL `query`, a short `title`, and
an optional `chart_type` like `line`/`bar`/`stat`/`table`):
- `render_visualization` — show a chart INLINE in the chat. Use for a one-off answer.
- `add_to_dashboard` — SAVE a chart to a dashboard given a `dashboard` NAME (created if it
  doesn't exist). Use when the user asks to add a chart to, or build/populate, a dashboard.

The app re-runs the query to render it."#
        .to_string()
}

#[cfg(test)]
mod grounding_tests {
    /// Every SeQL example embedded in [`super::default_grounding`] must parse, so we
    /// never teach the backing model invalid syntax (the original cause of a
    /// `validate_seql` retry loop).
    #[test]
    fn grounding_example_queries_parse() {
        let examples = [
            "spans last 1h | where status == 2 | take 100",
            "spans last 1h | group by {} { count() as total, count() where status == 2 as errors }",
            "spans last 1h | group by { service_name } { count() as n }",
            "spans last 15m | group by { ts() bin 10% as bucket } { count() as span_count }",
            "spans last 1h | group by {} { p95(duration_ns) as p95, percentile(duration_ns, 0.9) as p90, stddev(duration_ns) as sd }",
            "spans last 1h | group by { service_name } { throughput() as rps }",
            "spans last 1h | group by { ts() bin auto as bucket } { error_rate() as er } | window { moving_avg(er, 5) as er_ma }",
        ];
        for q in examples {
            assert!(
                seql_parser::parse(q).is_ok(),
                "grounding example must parse: {q}"
            );
        }
    }
}
