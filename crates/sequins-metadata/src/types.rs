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
/// splitting it among its panels by relative weight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub title: String,
    pub created_at_ns: u64,
    pub updated_at_ns: u64,
    #[serde(default)]
    pub rows: Vec<DashboardRow>,
}

impl Dashboard {
    /// Total number of panels across all rows.
    pub fn panel_count(&self) -> usize {
        self.rows.iter().map(|r| r.panels.len()).sum()
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

/// A visualization the app can re-render: a SeQL query, a title, and an optional
/// requested `ResponseShape` (as its `as_str()` form, e.g. `"timeseries"`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedVisualization {
    pub seql: String,
    pub title: String,
    #[serde(default)]
    pub shape: Option<String>,
    /// Optional presentation overrides (units, axis scale, stacking, …). Absent on
    /// older saved dashboards; defaults to empty.
    #[serde(default, skip_serializing_if = "VisualizationOptions::is_empty")]
    pub options: VisualizationOptions,
}

/// Presentation overrides for a [`SavedVisualization`]. Every field is optional so
/// dashboards saved before this blob existed deserialize unchanged, and an all-unset
/// value is omitted from serialized output.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VisualizationOptions {
    /// Value unit shown on axes/labels (e.g. `"ms"`, `"bytes"`, `"req/s"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// Y-axis scale: `"linear"` (default) or `"log"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y_scale: Option<String>,
    /// Force the y-axis lower bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y_min: Option<f64>,
    /// Force the y-axis upper bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub y_max: Option<f64>,
    /// Stack series instead of overlaying them (area/bar charts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stacked: Option<bool>,
    /// Show a series legend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legend: Option<bool>,
    /// Cap the number of series rendered (top-N by magnitude).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub series_limit: Option<u32>,
    /// Horizontal reference lines drawn across the plot.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thresholds: Vec<Threshold>,
}

impl VisualizationOptions {
    /// True when no override is set (used to omit the blob from serialized output).
    pub fn is_empty(&self) -> bool {
        *self == VisualizationOptions::default()
    }
}

/// A horizontal reference line on a chart (e.g. an SLO or alert boundary).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Threshold {
    /// Y-value at which the line is drawn.
    pub value: f64,
    /// Optional CSS-style color (e.g. `"#ff0000"` or `"red"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// Optional label for the line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}
