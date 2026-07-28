//! The assistant's tool set: a shared operation layer ([`ops`]) exposed through
//! two adapters — Rig tools for the in-process agent and MCP tools for external,
//! bring-your-own-model clients.

pub mod ops;
pub mod registry;

pub use ops::{OpError, Tools};
