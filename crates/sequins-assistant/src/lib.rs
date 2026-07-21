//! In-engine AI assistant for Sequins.
//!
//! A tool-injecting Rig [`CompletionModel`](rig::completion::CompletionModel) that runs an
//! agentic loop **in-process** over a `DataFusionBackend` — exploring telemetry and emitting
//! SeQL — plus a Model Context Protocol (MCP) server exposing the same tools to
//! bring-your-own-model clients. Both surfaces share one operation layer ([`tools::ops`]).
//!
//! See the crate design plan for the full architecture (opaque model/tools axes, the
//! `/v1/chat/completions` OpenAI-compatible endpoint, and how it mounts into the daemon).

pub mod config;
pub mod mcp;
pub mod model;
pub mod responses;
pub mod serve;
pub mod tools;
mod wire;

pub use config::{
    build_backing_model, build_registry, AssistantConfig, BackingModel, ModelConfig, ModelRegistry,
};
pub use mcp::{mcp_router, mcp_service, SequinsMcpServer};
pub use model::{AgentEvent, SequinsAssistantModel};
pub use responses::{responses_router, Assistant, ChatEvent};
pub use serve::completion_model_router;
pub use tools::{OpError, Tools};
