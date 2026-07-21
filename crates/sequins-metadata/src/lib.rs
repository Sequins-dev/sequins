//! Durable app-state for Sequins: chat **conversations** and **dashboards**.
//!
//! Conversations are persisted as a byproduct of the standard OpenAI Responses API
//! (`store` + `previous_response_id`); dashboards through a small custom API. Both
//! live as JSON on the same shared object store the cold tier uses — so in Pro they
//! are shared across the team — and are projected into DataFusion tables (by
//! `sequins-datafusion-backend`) so they're queryable by the same engine.

pub mod error;
pub mod store;
pub mod types;

pub use error::{MetadataError, Result};
pub use store::{AppStateStore, DashboardApi};
pub use types::{
    Conversation, ConversationItem, Dashboard, DashboardRow, RowPanel, SavedVisualization,
    DEFAULT_ROW_HEIGHT,
};
