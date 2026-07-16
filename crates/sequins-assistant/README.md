# sequins-assistant

An in-engine AI assistant for Sequins: a tool-injecting [Rig](https://github.com/0xPlaygrounds/rig)
`CompletionModel` that runs an agentic loop **in-process** over a `DataFusionBackend` — exploring
telemetry and emitting SeQL — plus a [Model Context Protocol](https://modelcontextprotocol.io) (MCP)
server exposing the same tools to bring-your-own-model clients.

## Purpose

Ask a question about your observability data; the agent explores it (schema, cardinality, time
range, ad-hoc read-only SQL), writes a SeQL query, validates and runs it, and hands the query back
to the app to render. It ships as an OpenAI-compatible endpoint so anything that speaks OpenAI (the
Sequins app, `curl`, OpenCode, Cursor) can drive it, and as an MCP server so external agents can use
the tools with their own model.

## Architecture

- **`CompletionModel` is the seam.** `SequinsAssistantModel<B>` wraps a real backing model `B`,
  injects our tool set + SeQL grounding into every request, and runs the tool loop in-process:
  when the model calls **our** tools it executes them against DataFusion and loops; when it calls
  the **caller's** own tools (or returns text) it hands the response straight back. Our own tool
  calls are stripped from returned responses, so a caller only ever sees its own tools.
- **One tool set, three surfaces.** [`tools::ops`] is the single operation layer; [`tools::registry`]
  exposes it as Rig tools (for the agent) and the [`mcp`] module exposes the same via `rmcp`.
  - **`/v1/chat/completions`** ([`serve::completion_model_router`]) — OpenAI Chat Completions over
    an `IndexMap` model registry (+ `/v1/models`). Server-side tool activity is *hidden*: the caller
    sees only the final text or its own (forwarded) tool calls.
  - **`/v1/responses`** ([`responses::responses_router`]) — the OpenAI **Responses API**, which
    streams typed output items. Every server-executed tool call is surfaced as a resolved
    `sequins.tool_result` item (name + arguments + rendered output), so a client can *render* the
    exploration it didn't drive. Client tools still appear as `function_call` items to execute.
  - **`/mcp`** ([`mcp`]) — MCP tools for bring-your-own-model clients.

  The middleware's [`model::AgentEvent`] trace is the seam: the chat path ignores `ServerTool`
  events; the Responses path emits them.

## Tools

**Explore** (read-only, over the backend's `SessionContext`): `list_tables`, `describe_schema`,
`column_profile`, `time_range`, `sample`, `explain`, `run_sql`.
**Present** (the SeQL path): `validate_seql`, `run_seql`.

Visualizations are **not** an in-process tool — the app declares its own `render_visualization`
tool, which the model calls and which is forwarded back for the app to draw.

## Usage

The daemon mounts both surfaces from an `assistant:` config (see `sequins-pro-daemon --assistant-config`).
Programmatically:

```rust
use sequins_assistant::{build_registry, completion_model_router, mcp_router, AssistantConfig, Tools};

let tools = Tools::new(backend);                       // Arc<DataFusionBackend>
let registry = build_registry(&config, tools.clone())?; // OpenAI-compatible providers
let app = completion_model_router(registry).merge(mcp_router(tools));
axum::serve(listener, app).await?;
```

API keys come from the environment (default `SEQUINS_ASSISTANT_API_KEY`), never from config.

## Testing

```bash
cargo test -p sequins-assistant
```

Tests cover the tool operation layer (against seeded storage), the middleware tool-routing loop
(with a fake backing model — no network), the OpenAI `/v1` surface, and the MCP tool dispatch.

## License

MIT OR Apache-2.0.
