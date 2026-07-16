//! App-state model types: chat conversations and dashboards.
//!
//! Conversations are persisted as a byproduct of the standard OpenAI Responses
//! API (`store` + `previous_response_id`): each turn's input and output items are
//! appended as [`ConversationItem`]s. Dashboards are edited through a custom API.
//! Both serialize to JSON on the shared object store and are projected into
//! DataFusion tables for querying.

use serde::{Deserialize, Serialize};

/// A chat conversation — an ordered list of items across one or more turns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    #[serde(default)]
    pub title: Option<String>,
    pub created_at_ns: u64,
    pub updated_at_ns: u64,
    #[serde(default)]
    pub items: Vec<ConversationItem>,
}

/// A single item in a conversation — a user/assistant/system message, a tool call,
/// or a server-executed tool result. Mirrors the Responses API output item shapes
/// so both plain messages and tool activity round-trip for display and replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationItem {
    /// The response turn that produced this item (`None` for user-supplied input).
    #[serde(default)]
    pub response_id: Option<String>,
    /// Order within the conversation.
    pub position: u32,
    /// `user` | `assistant` | `system` | `tool`.
    pub role: String,
    /// `message` | `function_call` | `sequins.tool_result` | …
    pub item_type: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    /// Tool call arguments as a JSON string.
    #[serde(default)]
    pub tool_arguments: Option<String>,
    /// Rendered tool output (for server-executed tools).
    #[serde(default)]
    pub tool_output: Option<String>,
    pub created_at_ns: u64,
}

/// Default row height, in points.
pub const DEFAULT_ROW_HEIGHT: f64 = 280.0;

fn default_row_height() -> f64 {
    DEFAULT_ROW_HEIGHT
}

fn default_weight() -> f64 {
    1.0
}

/// A saved dashboard — a titled, ordered stack of full-width [`DashboardRow`]s.
///
/// The layout is a flexbox of rows: each row has a height and fills the full width,
/// splitting it among its panels by relative weight. Legacy free-grid dashboards
/// (`panels` with `x/y/w/h`) are migrated into rows by [`Dashboard::migrate_legacy`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub title: String,
    pub created_at_ns: u64,
    pub updated_at_ns: u64,
    #[serde(default)]
    pub rows: Vec<DashboardRow>,
    /// Legacy free-grid panels — read from old stored dashboards, migrated into
    /// `rows`, and never re-serialized once migrated.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub panels: Vec<Panel>,
}

impl Dashboard {
    /// Total number of panels across all rows.
    pub fn panel_count(&self) -> usize {
        self.rows.iter().map(|r| r.panels.len()).sum()
    }

    /// Convert any legacy `panels` (free x/y/w/h grid) into `rows`: panels are grouped
    /// into rows by their `y`, ordered left-to-right by `x`, with each panel's width
    /// `w` becoming its row weight. Idempotent; a no-op once migrated.
    pub fn migrate_legacy(&mut self) {
        if self.panels.is_empty() {
            return;
        }
        let mut legacy = std::mem::take(&mut self.panels);
        legacy.sort_by_key(|p| (p.layout.y, p.layout.x));
        let mut rows: Vec<DashboardRow> = Vec::new();
        let mut current_y: Option<u32> = None;
        for p in legacy {
            let weight = if p.layout.w == 0 {
                1.0
            } else {
                p.layout.w as f64
            };
            let row_panel = RowPanel {
                visualization: p.visualization,
                weight,
            };
            if current_y == Some(p.layout.y) {
                rows.last_mut().unwrap().panels.push(row_panel);
            } else {
                current_y = Some(p.layout.y);
                rows.push(DashboardRow {
                    height: DEFAULT_ROW_HEIGHT,
                    panels: vec![row_panel],
                });
            }
        }
        // Prepend migrated rows before any rows that already exist (there shouldn't be).
        rows.append(&mut self.rows);
        self.rows = rows;
    }
}

/// A full-width dashboard row: a fixed height and an ordered set of panels that split
/// the width by weight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardRow {
    /// Row height in points.
    #[serde(default = "default_row_height")]
    pub height: f64,
    #[serde(default)]
    pub panels: Vec<RowPanel>,
}

impl Default for DashboardRow {
    fn default() -> Self {
        Self {
            height: DEFAULT_ROW_HEIGHT,
            panels: Vec::new(),
        }
    }
}

/// A panel within a row: a saved visualization and its relative width weight. The row
/// normalizes weights across its panels to fill the full width.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowPanel {
    pub visualization: SavedVisualization,
    #[serde(default = "default_weight")]
    pub weight: f64,
}

/// A legacy free-grid panel: a saved visualization plus its old `x/y/w/h` layout.
/// Retained only to deserialize and migrate older dashboards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Panel {
    pub visualization: SavedVisualization,
    #[serde(default)]
    pub layout: Layout,
}

/// A visualization the app can re-render: a SeQL query, a title, and an optional
/// requested `ResponseShape` (as its `as_str()` form, e.g. `"timeseries"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedVisualization {
    pub seql: String,
    pub title: String,
    #[serde(default)]
    pub shape: Option<String>,
}

/// Legacy grid placement of a panel within a dashboard (pre-rows layout).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Layout {
    #[serde(default)]
    pub x: u32,
    #[serde(default)]
    pub y: u32,
    #[serde(default)]
    pub w: u32,
    #[serde(default)]
    pub h: u32,
}
