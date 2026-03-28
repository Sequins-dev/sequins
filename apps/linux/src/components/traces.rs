//! Traces tab — macOS-parity layout.
//!
//! Vertical split:
//!   Top:    Waterfall timeline (scrollable Cairo drawing, tree-indented spans)
//!   Bottom: Horizontal 3-panel split
//!     Left:   Trace list (grouped by trace_id, with stats header)
//!     Middle: Trace info (operation, span count, services, IDs, duration)
//!     Right:  Span details (shown when a span bar is clicked in the waterfall)
//!
//! Two selection levels:
//!   1. TraceSelected(trace_id)  — clicking a trace in the left panel
//!   2. SpanSelected(span_id)    — clicking a span bar in the waterfall

use crate::data::AppDataSource;
use crate::drawing::{contrasting_text_color, palette_color, status_color, ChartTheme};
use crate::time_range::TimeRange;

use arrow::array::StructArray;
use arrow::array::{
    Array, Int64Array, LargeBinaryArray, MapArray, StringArray, StringViewArray,
    TimestampNanosecondArray, UInt8Array,
};
use arrow::ipc::reader::StreamReader;
use chrono::{DateTime, TimeZone, Utc};
use gtk4::glib;
use gtk4::prelude::*;
use relm4::prelude::*;
use sequins_view::{TableStrategy, ViewDelta};
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::Arc;

// ── Row data ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct SpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: u8,
    pub status: u8,
    pub start_ns: i64,
    pub end_ns: i64,
    pub duration_ns: i64,
    pub service_name: String,
    pub attributes: Vec<(String, String)>,
}

impl SpanRow {
    pub fn start_display(&self) -> String {
        let secs = self.start_ns / 1_000_000_000;
        let nsecs = (self.start_ns % 1_000_000_000).unsigned_abs() as u32;
        Utc.timestamp_opt(secs, nsecs)
            .single()
            .map(|dt: DateTime<Utc>| dt.format("%H:%M:%S%.3f").to_string())
            .unwrap_or_else(|| self.start_ns.to_string())
    }

    pub fn duration_display(&self) -> String {
        let us = self.duration_ns / 1_000;
        if us < 1_000 {
            format!("{us}µs")
        } else if us < 1_000_000 {
            format!("{:.1}ms", us as f64 / 1_000.0)
        } else {
            format!("{:.2}s", us as f64 / 1_000_000.0)
        }
    }

    pub fn status_display(&self) -> &'static str {
        match self.status {
            1 => "OK",
            2 => "Error",
            _ => "Unset",
        }
    }

    pub fn kind_display(&self) -> &'static str {
        match self.kind {
            1 => "Internal",
            2 => "Server",
            3 => "Client",
            4 => "Producer",
            5 => "Consumer",
            _ => "Unspecified",
        }
    }

    pub fn status_css_class(&self) -> &'static str {
        match self.status {
            2 => "span-error",
            1 => "span-ok",
            _ => "span-unset",
        }
    }
}

// ── Sort enum ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Default)]
pub enum TraceSortBy {
    #[default]
    StartTime,
    Duration,
    Service,
}

// ── Trace grouping ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct TraceSummary {
    trace_id: String,
    root_span_name: String,
    root_service: String,
    span_count: usize,
    has_error: bool,
    total_duration_ns: i64,
    min_start_ns: i64,
}

impl TraceSummary {
    fn duration_display(&self) -> String {
        let us = self.total_duration_ns / 1_000;
        if us < 1_000 {
            format!("{us}µs")
        } else if us < 1_000_000 {
            format!("{:.1}ms", us as f64 / 1_000.0)
        } else {
            format!("{:.2}s", us as f64 / 1_000_000.0)
        }
    }
}

fn build_trace_summaries(rows: &[SpanRow]) -> Vec<TraceSummary> {
    let mut groups: HashMap<&str, Vec<&SpanRow>> = HashMap::new();
    for row in rows {
        groups.entry(&row.trace_id).or_default().push(row);
    }

    let mut summaries: Vec<TraceSummary> = groups
        .into_iter()
        .map(|(trace_id, spans)| {
            let min_start = spans.iter().map(|s| s.start_ns).min().unwrap_or(0);
            let max_end = spans.iter().map(|s| s.end_ns).max().unwrap_or(0);
            let has_error = spans.iter().any(|s| s.status == 2);

            // Root span = no parent, or earliest start
            let root = spans
                .iter()
                .find(|s| s.parent_span_id.is_none())
                .or_else(|| spans.iter().min_by_key(|s| s.start_ns))
                .copied();

            TraceSummary {
                trace_id: trace_id.to_string(),
                root_span_name: root.map(|s| s.name.clone()).unwrap_or_default(),
                root_service: root.map(|s| s.service_name.clone()).unwrap_or_default(),
                span_count: spans.len(),
                has_error,
                total_duration_ns: (max_end - min_start).max(0),
                min_start_ns: min_start,
            }
        })
        .collect();

    summaries.sort_unstable_by(|a, b| b.min_start_ns.cmp(&a.min_start_ns));
    summaries
}

// ── Span tree ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct SpanTreeNode {
    span: SpanRow,
    depth: usize,
}

fn build_span_tree(spans: &[SpanRow]) -> Vec<SpanTreeNode> {
    // Build children index
    let mut children: HashMap<Option<&str>, Vec<&SpanRow>> = HashMap::new();
    let span_ids: std::collections::HashSet<&str> =
        spans.iter().map(|s| s.span_id.as_str()).collect();

    for span in spans {
        let parent = span
            .parent_span_id
            .as_deref()
            .filter(|pid| span_ids.contains(pid));
        children.entry(parent).or_default().push(span);
    }

    // Sort children by start time
    for list in children.values_mut() {
        list.sort_unstable_by_key(|s| s.start_ns);
    }

    // Pre-order DFS from roots
    let mut result = Vec::new();
    let mut stack: Vec<(&SpanRow, usize)> = Vec::new();

    if let Some(roots) = children.get(&None) {
        for root in roots.iter().rev() {
            stack.push((root, 0));
        }
    }

    while let Some((span, depth)) = stack.pop() {
        let key = span.span_id.as_str();
        result.push(SpanTreeNode {
            span: span.clone(),
            depth,
        });
        if let Some(kids) = children.get(&Some(key)) {
            for child in kids.iter().rev() {
                stack.push((child, depth + 1));
            }
        }
    }

    // Append orphans (parent not in set) at the end
    let added: std::collections::HashSet<String> =
        result.iter().map(|n| n.span.span_id.clone()).collect();
    for span in spans {
        if !added.contains(&span.span_id) {
            result.push(SpanTreeNode {
                span: span.clone(),
                depth: 0,
            });
        }
    }

    result
}

// ── Waterfall drawing ─────────────────────────────────────────────────────────

const WATERFALL_ROW_H: f64 = 24.0;
const WATERFALL_MARGIN: f64 = 4.0;
const INDENT_W: f64 = 14.0;
const BASE_LABEL_W: f64 = 180.0;

#[derive(Default)]
struct WaterfallDrawState {
    tree_nodes: Vec<SpanTreeNode>,
    min_start_ns: i64,
    total_ns: i64,
    hovered_idx: Option<usize>,
    selected_span_id: Option<String>,
    /// Sequential service → palette index mapping (first seen = 0, matches macOS behaviour)
    service_colors: HashMap<String, usize>,
    /// (x, y, w, h, idx) full-row rects for hit testing
    hit_rects: Vec<(f64, f64, f64, f64, usize)>,
}

impl WaterfallDrawState {
    fn load(&mut self, nodes: Vec<SpanTreeNode>, selected_span_id: Option<String>) {
        // Build sequential service→color mapping in tree order (matches macOS ServiceColorMapper)
        self.service_colors.clear();
        for node in &nodes {
            let next = self.service_colors.len();
            self.service_colors
                .entry(node.span.service_name.clone())
                .or_insert(next);
        }
        self.tree_nodes = nodes;
        self.selected_span_id = selected_span_id;
        self.hovered_idx = None;
        self.hit_rects.clear();
        if self.tree_nodes.is_empty() {
            self.min_start_ns = 0;
            self.total_ns = 1;
            return;
        }
        self.min_start_ns = self
            .tree_nodes
            .iter()
            .map(|n| n.span.start_ns)
            .min()
            .unwrap_or(0);
        let max_end = self
            .tree_nodes
            .iter()
            .map(|n| n.span.end_ns)
            .max()
            .unwrap_or(0);
        self.total_ns = (max_end - self.min_start_ns).max(1);
    }

    fn hit_test(&self, x: f64, y: f64) -> Option<usize> {
        for &(rx, ry, rw, rh, idx) in &self.hit_rects {
            if x >= rx && x <= rx + rw && y >= ry && y <= ry + rh {
                return Some(idx);
            }
        }
        None
    }
}

fn draw_waterfall(state: &mut WaterfallDrawState, cr: &cairo::Context, width: i32, _height: i32) {
    let w = width as f64;
    let theme = ChartTheme::current();

    state.hit_rects.clear();

    // Background
    let (r, g, b) = theme.bg();
    let _ = cr.set_source_rgb(r, g, b);
    let _ = cr.paint();

    for (idx, node) in state.tree_nodes.iter().enumerate() {
        let span = &node.span;
        let y = WATERFALL_MARGIN + idx as f64 * WATERFALL_ROW_H;
        let is_hovered = state.hovered_idx == Some(idx);
        let is_selected = state.selected_span_id.as_deref() == Some(&span.span_id);
        let label_w = BASE_LABEL_W + node.depth as f64 * INDENT_W;
        let chart_x = label_w + 4.0;
        let chart_w = (w - chart_x - WATERFALL_MARGIN).max(4.0);

        // Row background for selected/hovered
        if is_selected {
            // More opaque in light mode so the highlight is actually visible
            let alpha = if theme.is_dark { 0.18 } else { 0.30 };
            let _ = cr.set_source_rgba(0.37, 0.62, 1.0, alpha);
            let _ = cr.rectangle(0.0, y, w, WATERFALL_ROW_H);
            let _ = cr.fill();
        } else if is_hovered {
            let (hr, hg, hb, ha) = theme.hover_overlay();
            let _ = cr.set_source_rgba(hr, hg, hb, ha);
            let _ = cr.rectangle(0.0, y, w, WATERFALL_ROW_H);
            let _ = cr.fill();
        }

        // Tree indent guide lines
        let (glr, glg, glb, gla) = theme.guide_line();
        for depth in 0..node.depth {
            let line_x = 6.0 + depth as f64 * INDENT_W;
            let _ = cr.set_source_rgba(glr, glg, glb, gla);
            let _ = cr.set_line_width(1.0);
            let _ = cr.move_to(line_x, y);
            let _ = cr.line_to(line_x, y + WATERFALL_ROW_H);
            let _ = cr.stroke();
        }
        // Horizontal connector from parent line to span
        if node.depth > 0 {
            let line_x = 6.0 + (node.depth - 1) as f64 * INDENT_W;
            let mid_y = y + WATERFALL_ROW_H / 2.0;
            let _ = cr.set_source_rgba(glr, glg, glb, gla);
            let _ = cr.move_to(line_x, mid_y);
            let _ = cr.line_to(line_x + INDENT_W - 2.0, mid_y);
            let _ = cr.stroke();
        }

        // Span name label (clipped to label area)
        let text_x = 4.0 + node.depth as f64 * INDENT_W;
        let _ = cr.save();
        let _ = cr.rectangle(text_x, y, label_w - text_x - 4.0, WATERFALL_ROW_H);
        let _ = cr.clip();
        // Always use theme text color — selection is just a faint tint, not a dark fill
        let (tr, tg, tb) = theme.text();
        let _ = cr.set_source_rgb(tr, tg, tb);
        let _ = cr.move_to(text_x, y + WATERFALL_ROW_H - 6.0);
        let _ = cr.show_text(&span.name);
        let _ = cr.restore();

        // Vertical separator between label and chart area
        let (sr, sg, sb, sa) = theme.separator();
        let _ = cr.set_source_rgba(sr, sg, sb, sa);
        let _ = cr.set_line_width(1.0);
        let _ = cr.move_to(chart_x - 2.0, y);
        let _ = cr.line_to(chart_x - 2.0, y + WATERFALL_ROW_H);
        let _ = cr.stroke();

        // Time bar
        let start_frac = (span.start_ns - state.min_start_ns) as f64 / state.total_ns as f64;
        let dur_frac = span.duration_ns as f64 / state.total_ns as f64;
        let bar_x = chart_x + start_frac * chart_w;
        let bar_w = (dur_frac * chart_w).max(2.0);
        let bar_y = y + 5.0;
        let bar_h = WATERFALL_ROW_H - 10.0;

        let color_idx = state
            .service_colors
            .get(&span.service_name)
            .copied()
            .unwrap_or(0);
        let (r, g, b) = palette_color(color_idx, theme.is_dark);
        let mult = if is_hovered { 1.15 } else { 1.0 };
        let br = (r * mult).min(1.0);
        let bg_c = (g * mult).min(1.0);
        let bb = (b * mult).min(1.0);
        let _ = cr.set_source_rgb(br, bg_c, bb);
        let _ = cr.rectangle(bar_x, bar_y, bar_w, bar_h);
        let _ = cr.fill();

        // Error indicator: red left edge
        if span.status == 2 {
            let (er, eg, eb) = status_color(2);
            let _ = cr.set_source_rgb(er, eg, eb);
            let _ = cr.rectangle(bar_x, bar_y, 3.0_f64.min(bar_w), bar_h);
            let _ = cr.fill();
        }

        // Duration text inside bar if wide enough — color chosen for contrast against bar
        if bar_w > 50.0 {
            let _ = cr.save();
            let _ = cr.rectangle(bar_x + 4.0, bar_y, bar_w - 8.0, bar_h);
            let _ = cr.clip();
            let (dtr, dtg, dtb) = contrasting_text_color(br, bg_c, bb);
            let _ = cr.set_source_rgb(dtr, dtg, dtb);
            let _ = cr.move_to(bar_x + 4.0, bar_y + bar_h - 2.0);
            let _ = cr.show_text(&span.duration_display());
            let _ = cr.restore();
        }

        // Full-row hit rect
        state.hit_rects.push((0.0, y, w, WATERFALL_ROW_H, idx));
    }
}

fn draw_timeline_ruler(
    cr: &cairo::Context,
    width: i32,
    height: i32,
    chart_x: f64,
    chart_w: f64,
    total_ns: i64,
) {
    let w = width as f64;
    let h = height as f64;

    let theme = ChartTheme::current();
    let (r, g, b) = theme.ruler_bg();
    let _ = cr.set_source_rgb(r, g, b);
    let _ = cr.paint();

    let (sr, sg, sb, sa) = theme.separator();
    let _ = cr.set_source_rgba(sr, sg, sb, sa);
    let _ = cr.set_line_width(1.0);
    let _ = cr.move_to(0.0, 0.0);
    let _ = cr.line_to(w, 0.0);
    let _ = cr.stroke();

    if total_ns <= 0 {
        return;
    }

    // Choose a nice tick interval
    let tick_count = 5;
    let tick_ns = total_ns / tick_count;
    if tick_ns <= 0 {
        return;
    }

    let (tr, tg, tb) = theme.ruler_text();
    let _ = cr.set_source_rgb(tr, tg, tb);
    let _ = cr.set_line_width(1.0);

    for i in 0..=tick_count {
        let ns = i * tick_ns;
        let x = chart_x + ns as f64 / total_ns as f64 * chart_w;

        // Tick mark
        let _ = cr.move_to(x, 0.0);
        let _ = cr.line_to(x, 5.0);
        let _ = cr.stroke();

        // Label
        let label = format_ns(ns);
        let _ = cr.move_to(x + 2.0, h - 3.0);
        let _ = cr.show_text(&label);
    }
}

fn format_ns(ns: i64) -> String {
    let us = ns / 1_000;
    if us < 1_000 {
        format!("{us}µs")
    } else if us < 1_000_000 {
        format!("{:.0}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.1}s", us as f64 / 1_000_000.0)
    }
}

// ── Component ──────────────────────────────────────────────────────────────────

pub struct TracesInit {
    pub data_source: Arc<AppDataSource>,
}

pub struct TracesModel {
    data_source: Arc<AppDataSource>,
    search_text: String,
    service_filter: Option<u32>,
    time_range: TimeRange,
    is_live: bool,
    rows: Vec<SpanRow>,
    trace_summaries: Vec<TraceSummary>,
    /// Incremented whenever trace_summaries is rebuilt.  `update_view` only
    /// rebuilds the trace ListBox when this differs from the rendered version.
    trace_summaries_version: u64,
    selected_trace_id: Option<String>,
    selected_detail_span_id: Option<String>,
    waterfall_nodes: Vec<SpanTreeNode>,
    sort_by: TraceSortBy,
    show_errors_only: bool,
    http_status_filters: BTreeSet<u8>,
    stream_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
pub enum TracesInput {
    RowsBatch(Vec<SpanRow>),
    Expired(u64),
    StreamReady,
    SearchChanged(String),
    ServiceFilter(Option<u32>),
    SetTimeRange(TimeRange, bool),
    TraceSelected(String),
    SpanSelected(String),
    StreamError(String),
    SortByChanged(TraceSortBy),
    ErrorsOnlyToggled,
    HttpStatusToggled(u8),
}

pub struct TracesWidgets {
    root: gtk4::Box,
    search_entry: gtk4::SearchEntry,
    // Waterfall top panel
    waterfall: gtk4::DrawingArea,
    waterfall_placeholder: gtk4::Label,
    timeline_ruler: gtk4::DrawingArea,
    draw_state: Rc<RefCell<WaterfallDrawState>>,
    // Left panel — trace list
    trace_listbox: gtk4::ListBox,
    stats_label: gtk4::Label,
    rendered_summaries_version: u64,
    // Middle panel — trace info
    trace_info_box: gtk4::Box,
    // Right panel — span details
    span_detail_box: gtk4::Box,
    // Filter bar
    sort_dropdown: gtk4::DropDown,
    errors_only_btn: gtk4::ToggleButton,
    http_status_btns: HashMap<u8, gtk4::ToggleButton>,
}

impl Component for TracesModel {
    type CommandOutput = ();
    type Init = TracesInit;
    type Input = TracesInput;
    type Output = ();
    type Root = gtk4::Box;
    type Widgets = TracesWidgets;

    fn init_root() -> Self::Root {
        gtk4::Box::new(gtk4::Orientation::Vertical, 0)
    }

    fn init(
        init: Self::Init,
        root: Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        // ── Compact filter bar ───────────────────────────────────────────────
        let search_entry = gtk4::SearchEntry::builder()
            .placeholder_text("Filter spans…")
            .width_request(200)
            .build();

        // Sort dropdown
        let sort_string_list = gtk4::StringList::new(&["Start Time", "Duration", "Service"]);
        let sort_dropdown = gtk4::DropDown::builder()
            .model(&sort_string_list)
            .selected(0)
            .build();
        {
            let s = sender.clone();
            sort_dropdown.connect_selected_notify(move |dd| {
                let sort_by = match dd.selected() {
                    1 => TraceSortBy::Duration,
                    2 => TraceSortBy::Service,
                    _ => TraceSortBy::StartTime,
                };
                s.input(TracesInput::SortByChanged(sort_by));
            });
        }

        let errors_only_btn = gtk4::ToggleButton::builder()
            .label("Errors Only")
            .css_classes(["flat"])
            .build();
        {
            let s = sender.clone();
            errors_only_btn.connect_toggled(move |_| {
                s.input(TracesInput::ErrorsOnlyToggled);
            });
        }

        let http_status_defs: &[(u8, &str)] = &[(2, "2xx"), (3, "3xx"), (4, "4xx"), (5, "5xx")];
        let mut http_status_btns: HashMap<u8, gtk4::ToggleButton> = HashMap::new();
        for &(prefix, label) in http_status_defs {
            let btn = gtk4::ToggleButton::builder()
                .label(label)
                .css_classes(["flat"])
                .build();
            let s = sender.clone();
            btn.connect_toggled(move |_| {
                s.input(TracesInput::HttpStatusToggled(prefix));
            });
            http_status_btns.insert(prefix, btn);
        }

        let toolbar = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(4)
            .margin_start(8)
            .margin_end(8)
            .build();
        toolbar.append(&search_entry);
        toolbar.append(&sort_dropdown);
        toolbar.append(&errors_only_btn);
        for &(prefix, _) in http_status_defs {
            toolbar.append(http_status_btns.get(&prefix).unwrap());
        }

        {
            let s = sender.clone();
            search_entry.connect_search_changed(move |entry| {
                s.input(TracesInput::SearchChanged(entry.text().to_string()));
            });
        }

        // Ctrl+F focuses search
        {
            let se = search_entry.clone();
            let key_ctrl = gtk4::EventControllerKey::new();
            key_ctrl.connect_key_pressed(move |_, key, _, mods| {
                if key == gtk4::gdk::Key::f && mods.contains(gtk4::gdk::ModifierType::CONTROL_MASK)
                {
                    se.grab_focus();
                    return glib::Propagation::Stop;
                }
                glib::Propagation::Proceed
            });
            root.add_controller(key_ctrl);
        }

        // ── Waterfall area (outer_paned start child) ─────────────────────────
        let draw_state: Rc<RefCell<WaterfallDrawState>> =
            Rc::new(RefCell::new(WaterfallDrawState::default()));

        let waterfall = gtk4::DrawingArea::builder()
            .hexpand(true)
            .height_request(0)
            .build();

        {
            let ds = draw_state.clone();
            waterfall.set_draw_func(move |_da, cr, width, height| {
                let mut state = ds.borrow_mut();
                draw_waterfall(&mut state, cr, width, height);
            });
        }

        // Click handler — SpanSelected
        {
            let ds = draw_state.clone();
            let s = sender.clone();
            let click = gtk4::GestureClick::new();
            click.connect_released(move |_, _, x, y| {
                let state = ds.borrow();
                if let Some(idx) = state.hit_test(x, y) {
                    if let Some(node) = state.tree_nodes.get(idx) {
                        s.input(TracesInput::SpanSelected(node.span.span_id.clone()));
                    }
                }
            });
            waterfall.add_controller(click);
        }

        // Hover controller
        {
            let ds = draw_state.clone();
            let wf = waterfall.clone();
            let motion = gtk4::EventControllerMotion::new();
            motion.connect_motion(move |_, x, y| {
                let mut state = ds.borrow_mut();
                let new_idx = state.hit_test(x, y);
                if new_idx != state.hovered_idx {
                    state.hovered_idx = new_idx;
                    wf.queue_draw();
                }
            });
            waterfall.add_controller(motion);
        }

        let waterfall_scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&waterfall)
            .build();

        let timeline_ruler = gtk4::DrawingArea::builder()
            .hexpand(true)
            .height_request(24)
            .build();

        {
            let ds = draw_state.clone();
            timeline_ruler.set_draw_func(move |_da, cr, width, height| {
                let state = ds.borrow();
                // Ruler x position matches widest span's label area
                let chart_x = BASE_LABEL_W + 4.0;
                let chart_w = (width as f64 - chart_x - WATERFALL_MARGIN).max(4.0);
                draw_timeline_ruler(cr, width, height, chart_x, chart_w, state.total_ns);
            });
        }

        let waterfall_placeholder = gtk4::Label::builder()
            .label("Select a trace to view timeline")
            .css_classes(["dim-label"])
            .halign(gtk4::Align::Center)
            .valign(gtk4::Align::Center)
            .vexpand(true)
            .build();

        let waterfall_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .build();
        waterfall_box.append(&waterfall_scroll);
        waterfall_box.append(&timeline_ruler);
        waterfall_box.append(&waterfall_placeholder);

        // ── Left panel — trace list ─────────────────────────────────────────
        let stats_label = gtk4::Label::builder()
            .label("0 traces")
            .css_classes(["caption", "dim-label"])
            .halign(gtk4::Align::Start)
            .margin_start(8)
            .margin_top(4)
            .margin_bottom(4)
            .build();

        let trace_listbox = gtk4::ListBox::builder()
            .css_classes(["navigation-sidebar"])
            .build();

        {
            let s = sender.clone();
            trace_listbox.connect_row_activated(move |_, row| {
                // trace_id stored in widget name
                let trace_id = row.widget_name().to_string();
                if !trace_id.is_empty() {
                    s.input(TracesInput::TraceSelected(trace_id));
                }
            });
        }

        let trace_list_scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&trace_listbox)
            .build();

        let left_panel = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .width_request(220)
            .build();
        left_panel.append(&stats_label);
        left_panel.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        left_panel.append(&trace_list_scroll);

        // ── Middle panel — trace info ───────────────────────────────────────
        let trace_info_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(4)
            .margin_start(8)
            .margin_end(8)
            .margin_top(8)
            .margin_bottom(8)
            .build();

        let trace_info_scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&trace_info_box)
            .build();

        let middle_panel = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .width_request(350)
            .build();

        let middle_header = gtk4::Label::builder()
            .label("Trace")
            .css_classes(["heading"])
            .halign(gtk4::Align::Start)
            .margin_start(8)
            .margin_top(4)
            .margin_bottom(4)
            .build();
        middle_panel.append(&middle_header);
        middle_panel.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        middle_panel.append(&trace_info_scroll);

        // ── Right panel — span details ──────────────────────────────────────
        let span_detail_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(4)
            .margin_start(8)
            .margin_end(8)
            .margin_top(8)
            .margin_bottom(8)
            .build();

        let span_detail_scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&span_detail_box)
            .build();

        let right_panel = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .width_request(220)
            .build();

        let right_header = gtk4::Label::builder()
            .label("Span Details")
            .css_classes(["heading"])
            .halign(gtk4::Align::Start)
            .margin_start(8)
            .margin_top(4)
            .margin_bottom(4)
            .build();
        right_panel.append(&right_header);
        right_panel.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        right_panel.append(&span_detail_scroll);

        // ── Paned layout ────────────────────────────────────────────────────
        let right_paned = gtk4::Paned::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .position(380)
            .shrink_start_child(false)
            .shrink_end_child(false)
            .build();
        right_paned.set_start_child(Some(&middle_panel));
        right_paned.set_end_child(Some(&right_panel));

        let inner_paned = gtk4::Paned::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .position(220)
            .shrink_start_child(false)
            .shrink_end_child(false)
            .build();
        inner_paned.set_start_child(Some(&left_panel));
        inner_paned.set_end_child(Some(&right_paned));

        let outer_paned = gtk4::Paned::builder()
            .orientation(gtk4::Orientation::Vertical)
            .position(320)
            .vexpand(true)
            .shrink_start_child(false)
            .shrink_end_child(false)
            .build();
        outer_paned.set_start_child(Some(&waterfall_box));
        outer_paned.set_end_child(Some(&inner_paned));

        toolbar.set_widget_name("tab-toolbar");
        root.append(&toolbar);
        root.append(&outer_paned);

        let mut model = TracesModel {
            data_source: init.data_source.clone(),
            search_text: String::new(),
            service_filter: None,
            time_range: TimeRange::default(),
            is_live: true,
            rows: Vec::new(),
            trace_summaries: Vec::new(),
            trace_summaries_version: 0,
            selected_trace_id: None,
            selected_detail_span_id: None,
            waterfall_nodes: Vec::new(),
            sort_by: TraceSortBy::default(),
            show_errors_only: false,
            http_status_filters: BTreeSet::new(),
            stream_task: None,
        };

        start_stream(&mut model, sender.clone());

        ComponentParts {
            model,
            widgets: TracesWidgets {
                root,
                search_entry,
                waterfall,
                waterfall_placeholder,
                timeline_ruler,
                draw_state,
                trace_listbox,
                stats_label,
                rendered_summaries_version: u64::MAX, // force first build
                trace_info_box,
                span_detail_box,
                sort_dropdown,
                errors_only_btn,
                http_status_btns,
            },
        }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            TracesInput::RowsBatch(rows) => {
                self.rows.extend(rows);
                // Cap at 2000 rows total
                const MAX_ROWS: usize = 2000;
                if self.rows.len() > MAX_ROWS {
                    let excess = self.rows.len() - MAX_ROWS;
                    self.rows.drain(..excess);
                }
                self.trace_summaries = self.filtered_summaries();
                self.trace_summaries_version += 1;
                // Ensure a trace is selected, then refresh the waterfall for the current trace
                self.auto_select_first_trace();
                if let Some(ref tid) = self.selected_trace_id.clone() {
                    self.rebuild_waterfall_nodes(tid);
                }
            }
            TracesInput::Expired(count) => {
                let n = (count as usize).min(self.rows.len());
                self.rows.drain(..n);
                self.trace_summaries = self.filtered_summaries();
                self.trace_summaries_version += 1;
                self.auto_select_first_trace();
            }
            TracesInput::StreamReady => {
                tracing::debug!("Traces stream ready ({} spans)", self.rows.len());
            }
            TracesInput::SearchChanged(text) => {
                if text != self.search_text {
                    self.search_text = text;
                    self.trace_summaries = self.filtered_summaries();
                    self.trace_summaries_version += 1;
                    self.auto_select_first_trace();
                }
            }
            TracesInput::ServiceFilter(filter) => {
                if filter != self.service_filter {
                    self.service_filter = filter;
                    self.reset_and_restart(sender);
                }
            }
            TracesInput::SetTimeRange(range, live) => {
                if range != self.time_range || live != self.is_live {
                    self.time_range = range;
                    self.is_live = live;
                    self.reset_and_restart(sender);
                }
            }
            TracesInput::TraceSelected(trace_id) => {
                self.selected_trace_id = Some(trace_id.clone());
                self.selected_detail_span_id = None;
                self.rebuild_waterfall_nodes(&trace_id);
            }
            TracesInput::SpanSelected(span_id) => {
                self.selected_detail_span_id = Some(span_id);
            }
            TracesInput::StreamError(e) => {
                tracing::error!("Traces stream error: {}", e);
            }
            TracesInput::SortByChanged(sort_by) => {
                self.sort_by = sort_by;
                self.trace_summaries = self.filtered_summaries();
                self.trace_summaries_version += 1;
            }
            TracesInput::ErrorsOnlyToggled => {
                self.show_errors_only = !self.show_errors_only;
                self.trace_summaries = self.filtered_summaries();
                self.trace_summaries_version += 1;
                self.auto_select_first_trace();
            }
            TracesInput::HttpStatusToggled(prefix) => {
                if self.http_status_filters.contains(&prefix) {
                    self.http_status_filters.remove(&prefix);
                } else {
                    self.http_status_filters.insert(prefix);
                }
                self.trace_summaries = self.filtered_summaries();
                self.trace_summaries_version += 1;
                self.auto_select_first_trace();
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, _sender: ComponentSender<Self>) {
        // ── Rebuild trace list only when summaries changed ────────────────────
        if self.trace_summaries_version != widgets.rendered_summaries_version {
            while let Some(child) = widgets.trace_listbox.first_child() {
                widgets.trace_listbox.remove(&child);
            }
            for summary in &self.trace_summaries {
                let row = build_trace_list_row(summary);
                widgets.trace_listbox.append(&row);
            }
            widgets.rendered_summaries_version = self.trace_summaries_version;
            // Highlight the selected trace row
            if let Some(ref tid) = self.selected_trace_id {
                let mut i = 0i32;
                while let Some(row) = widgets.trace_listbox.row_at_index(i) {
                    if row.widget_name().as_str() == tid.as_str() {
                        widgets.trace_listbox.select_row(Some(&row));
                        break;
                    }
                    i += 1;
                }
            }
        }

        let error_count = self.trace_summaries.iter().filter(|t| t.has_error).count();
        widgets.stats_label.set_text(&format!(
            "{} traces  •  {} errors",
            self.trace_summaries.len(),
            error_count
        ));

        // ── Waterfall ────────────────────────────────────────────────────────
        let show_waterfall = self.selected_trace_id.is_some() && !self.waterfall_nodes.is_empty();
        widgets.waterfall_placeholder.set_visible(!show_waterfall);
        widgets.waterfall.set_visible(show_waterfall);
        widgets.timeline_ruler.set_visible(show_waterfall);

        if show_waterfall {
            let n = self.waterfall_nodes.len();
            let h = (n as i32) * (WATERFALL_ROW_H as i32) + (WATERFALL_MARGIN as i32) * 2;
            widgets
                .waterfall
                .set_height_request(h.max(WATERFALL_ROW_H as i32));

            {
                let mut state = widgets.draw_state.borrow_mut();
                state.load(
                    self.waterfall_nodes.clone(),
                    self.selected_detail_span_id.clone(),
                );
            }

            widgets.waterfall.queue_draw();
            widgets.timeline_ruler.queue_draw();
        }

        // ── Middle panel: trace info ──────────────────────────────────────────
        while let Some(child) = widgets.trace_info_box.first_child() {
            widgets.trace_info_box.remove(&child);
        }

        if let Some(ref trace_id) = self.selected_trace_id {
            if let Some(summary) = self
                .trace_summaries
                .iter()
                .find(|t| &t.trace_id == trace_id)
            {
                let trace_spans: Vec<&SpanRow> = self
                    .rows
                    .iter()
                    .filter(|s| &s.trace_id == trace_id)
                    .collect();

                let unique_services: std::collections::HashSet<&str> = trace_spans
                    .iter()
                    .map(|s| s.service_name.as_str())
                    .collect();

                let errors: Vec<&&SpanRow> = trace_spans.iter().filter(|s| s.status == 2).collect();

                populate_trace_info(
                    &widgets.trace_info_box,
                    summary,
                    unique_services.len(),
                    &errors,
                );
            }
        } else {
            let placeholder = gtk4::Label::builder()
                .label("Select a trace")
                .css_classes(["dim-label"])
                .halign(gtk4::Align::Center)
                .build();
            widgets.trace_info_box.append(&placeholder);
        }

        // ── Right panel: span details ─────────────────────────────────────────
        while let Some(child) = widgets.span_detail_box.first_child() {
            widgets.span_detail_box.remove(&child);
        }

        if let Some(ref span_id) = self.selected_detail_span_id {
            if let Some(span) = self.rows.iter().find(|s| &s.span_id == span_id) {
                let trace_start = self
                    .waterfall_nodes
                    .iter()
                    .map(|n| n.span.start_ns)
                    .min()
                    .unwrap_or(0);
                populate_span_detail(&widgets.span_detail_box, span, trace_start);
            }
        } else {
            let placeholder = gtk4::Label::builder()
                .label("Select a span in the waterfall")
                .css_classes(["dim-label"])
                .halign(gtk4::Align::Center)
                .build();
            widgets.span_detail_box.append(&placeholder);
        }
    }
}

impl TracesModel {
    /// Extract the HTTP status code prefix (2,3,4,5) from a span's attributes.
    /// Checks both the new (`http_response_status_code`) and old (`http_status_code`)
    /// OTel semantic convention column names.
    fn http_prefix(row: &SpanRow) -> Option<u8> {
        row.attributes
            .iter()
            .find(|(k, _)| {
                // Promoted column names (underscore form)
                k == "http_response_status_code"
                    || k == "http_status_code"
                    // Overflow attribute names (dot notation, matching macOS)
                    || k == "http.response.status_code"
                    || k == "http.status_code"
            })
            .and_then(|(_, v)| v.parse::<u16>().ok())
            .map(|code| (code / 100) as u8)
    }

    fn filtered_summaries(&self) -> Vec<TraceSummary> {
        // Build complete, accurate summaries from ALL rows first — this ensures
        // span_count is correct and the root span is identified properly.
        let mut summaries = build_trace_summaries(&self.rows);

        // Apply filters at the TRACE level. For each filter, a trace passes if
        // ANY of its spans satisfies the condition.
        let needs_filter = !self.search_text.is_empty()
            || self.show_errors_only
            || !self.http_status_filters.is_empty();

        if needs_filter {
            // Build trace_id → spans lookup once, used by search + HTTP filters.
            let mut trace_spans: HashMap<&str, Vec<&SpanRow>> = HashMap::new();
            for row in &self.rows {
                trace_spans
                    .entry(row.trace_id.as_str())
                    .or_default()
                    .push(row);
            }
            let search_lower = self.search_text.to_lowercase();

            summaries.retain(|summary| {
                let spans = trace_spans
                    .get(summary.trace_id.as_str())
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                // Errors only: use pre-computed has_error flag
                if self.show_errors_only && !summary.has_error {
                    return false;
                }

                // Text search: any span in the trace matches name, service, or attrs
                if !search_lower.is_empty() {
                    let matches = spans.iter().any(|row| {
                        row.name.to_lowercase().contains(&search_lower)
                            || row.service_name.to_lowercase().contains(&search_lower)
                            || row.attributes.iter().any(|(k, v)| {
                                k.to_lowercase().contains(&search_lower)
                                    || v.to_lowercase().contains(&search_lower)
                            })
                    });
                    if !matches {
                        return false;
                    }
                }

                // HTTP status filter: any span in the trace has a matching prefix
                if !self.http_status_filters.is_empty() {
                    let has_match = spans.iter().any(|row| {
                        Self::http_prefix(row)
                            .map(|p| self.http_status_filters.contains(&p))
                            .unwrap_or(false)
                    });
                    if !has_match {
                        return false;
                    }
                }

                true
            });
        }

        match self.sort_by {
            TraceSortBy::StartTime => {
                summaries.sort_unstable_by(|a, b| b.min_start_ns.cmp(&a.min_start_ns))
            }
            TraceSortBy::Duration => {
                summaries.sort_unstable_by(|a, b| b.total_duration_ns.cmp(&a.total_duration_ns))
            }
            TraceSortBy::Service => {
                summaries.sort_unstable_by(|a, b| a.root_service.cmp(&b.root_service))
            }
        }
        summaries
    }

    fn rebuild_waterfall_nodes(&mut self, trace_id: &str) {
        let trace_spans: Vec<SpanRow> = self
            .rows
            .iter()
            .filter(|s| s.trace_id == trace_id)
            .cloned()
            .collect();
        self.waterfall_nodes = build_span_tree(&trace_spans);
        // Preserve the user's span selection if it still exists; otherwise fall back to first span
        let span_still_present = self
            .selected_detail_span_id
            .as_ref()
            .is_some_and(|sid| self.waterfall_nodes.iter().any(|n| &n.span.span_id == sid));
        if !span_still_present {
            self.selected_detail_span_id =
                self.waterfall_nodes.first().map(|n| n.span.span_id.clone());
        }
    }

    /// If the currently selected trace is gone (or nothing was selected), select the first one.
    fn auto_select_first_trace(&mut self) {
        let selected_valid = self
            .selected_trace_id
            .as_ref()
            .is_some_and(|tid| self.trace_summaries.iter().any(|t| &t.trace_id == tid));
        if !selected_valid {
            if let Some(first) = self.trace_summaries.first() {
                let tid = first.trace_id.clone();
                self.selected_trace_id = Some(tid.clone());
                self.selected_detail_span_id = None;
                self.rebuild_waterfall_nodes(&tid);
            }
        }
    }

    fn reset_and_restart(&mut self, sender: ComponentSender<Self>) {
        if let Some(h) = self.stream_task.take() {
            h.abort();
        }
        self.rows.clear();
        self.trace_summaries.clear();
        self.selected_trace_id = None;
        self.selected_detail_span_id = None;
        self.waterfall_nodes.clear();
        start_stream(self, sender);
    }
}

// ── Widget builders ───────────────────────────────────────────────────────────

fn build_trace_list_row(summary: &TraceSummary) -> gtk4::ListBoxRow {
    let row_box = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .spacing(2)
        .margin_start(8)
        .margin_end(8)
        .margin_top(4)
        .margin_bottom(4)
        .build();

    let top_row = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Horizontal)
        .spacing(4)
        .build();

    // Error indicator
    if summary.has_error {
        let err_dot = gtk4::Label::builder()
            .label("●")
            .css_classes(["span-error", "caption"])
            .build();
        top_row.append(&err_dot);
    }

    let name_label = gtk4::Label::builder()
        .label(&summary.root_span_name)
        .css_classes(["caption"])
        .halign(gtk4::Align::Start)
        .hexpand(true)
        .ellipsize(gtk4::pango::EllipsizeMode::End)
        .build();
    top_row.append(&name_label);

    let dur_label = gtk4::Label::builder()
        .label(summary.duration_display())
        .css_classes(["caption", "dim-label"])
        .halign(gtk4::Align::End)
        .build();
    top_row.append(&dur_label);

    let meta_row = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Horizontal)
        .spacing(4)
        .build();

    let span_count_label = gtk4::Label::builder()
        .label(format!("{} spans", summary.span_count))
        .css_classes(["caption", "dim-label"])
        .halign(gtk4::Align::Start)
        .build();
    meta_row.append(&span_count_label);

    row_box.append(&top_row);
    row_box.append(&meta_row);

    let row = gtk4::ListBoxRow::new();
    row.set_child(Some(&row_box));
    // Store trace_id in widget name for retrieval in row_activated
    row.set_widget_name(&summary.trace_id);
    row
}

fn detail_row(label: &str, value: &str) -> gtk4::Box {
    let row = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Horizontal)
        .spacing(8)
        .build();
    let key = gtk4::Label::builder()
        .label(label)
        .css_classes(["caption", "dim-label"])
        .width_request(100)
        .halign(gtk4::Align::End)
        .xalign(1.0)
        .build();
    let val = gtk4::Label::builder()
        .label(value)
        .css_classes(["caption"])
        .halign(gtk4::Align::Start)
        .hexpand(true)
        .selectable(true)
        .ellipsize(gtk4::pango::EllipsizeMode::End)
        .build();
    row.append(&key);
    row.append(&val);
    row
}

fn populate_trace_info(
    info_box: &gtk4::Box,
    summary: &TraceSummary,
    service_count: usize,
    errors: &[&&SpanRow],
) {
    let op_label = gtk4::Label::builder()
        .label(&summary.root_span_name)
        .css_classes(["heading"])
        .halign(gtk4::Align::Start)
        .ellipsize(gtk4::pango::EllipsizeMode::End)
        .build();
    info_box.append(&op_label);
    info_box.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));

    info_box.append(&detail_row(
        "Trace ID",
        &summary.trace_id[..summary.trace_id.len().min(24)],
    ));
    info_box.append(&detail_row("Duration", &summary.duration_display()));
    info_box.append(&detail_row("Spans", &summary.span_count.to_string()));
    info_box.append(&detail_row("Services", &service_count.to_string()));

    if summary.has_error {
        info_box.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        let err_heading = gtk4::Label::builder()
            .label(format!("{} error spans", errors.len()))
            .css_classes(["caption", "span-error"])
            .halign(gtk4::Align::Start)
            .build();
        info_box.append(&err_heading);
        for span in errors.iter().take(5) {
            let lbl = gtk4::Label::builder()
                .label(&span.name)
                .css_classes(["caption"])
                .halign(gtk4::Align::Start)
                .ellipsize(gtk4::pango::EllipsizeMode::End)
                .margin_start(8)
                .build();
            info_box.append(&lbl);
        }
    }
}

fn populate_span_detail(detail_box: &gtk4::Box, span: &SpanRow, trace_start_ns: i64) {
    let op_label = gtk4::Label::builder()
        .label(&span.name)
        .css_classes(["heading"])
        .halign(gtk4::Align::Start)
        .ellipsize(gtk4::pango::EllipsizeMode::End)
        .build();
    detail_box.append(&op_label);

    let status_label = gtk4::Label::builder()
        .label(span.status_display())
        .css_classes(["caption", span.status_css_class()])
        .halign(gtk4::Align::Start)
        .build();
    detail_box.append(&status_label);
    detail_box.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));

    detail_box.append(&detail_row("Service", &span.service_name));
    detail_box.append(&detail_row("Kind", span.kind_display()));
    detail_box.append(&detail_row("Duration", &span.duration_display()));

    let start_offset_ns = span.start_ns - trace_start_ns;
    detail_box.append(&detail_row("Start offset", &format_ns(start_offset_ns)));

    let short_span = &span.span_id[..span.span_id.len().min(16)];
    detail_box.append(&detail_row("Span ID", short_span));

    if let Some(ref pid) = span.parent_span_id {
        let short_pid = &pid[..pid.len().min(16)];
        detail_box.append(&detail_row("Parent ID", short_pid));
    }

    if !span.attributes.is_empty() {
        detail_box.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        let attrs_heading = gtk4::Label::builder()
            .label("Attributes")
            .css_classes(["heading"])
            .halign(gtk4::Align::Start)
            .build();
        detail_box.append(&attrs_heading);
        for (key, value) in &span.attributes {
            detail_box.append(&detail_row(key, value));
        }
    }
}

// ── Stream launcher ───────────────────────────────────────────────────────────

fn start_stream(model: &mut TracesModel, sender: ComponentSender<TracesModel>) {
    let ds = model.data_source.clone();
    let service = model.service_filter;
    let range = model.time_range;
    let is_live = model.is_live;
    let s = sender.input_sender().clone();
    let query = build_query(service, range);

    let handle = relm4::spawn(async move {
        let strategy = TableStrategy::new();
        match ds.live_view(&query, &strategy).await {
            Ok(stream) => {
                use futures::StreamExt;
                use tokio::time::{interval, Duration, MissedTickBehavior};
                futures::pin_mut!(stream);

                let mut pending: Vec<SpanRow> = Vec::new();
                let mut pending_expired: u64 = 0;
                let mut flush = interval(Duration::from_millis(200));
                flush.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        biased;
                        delta = stream.next() => match delta {
                            Some(ViewDelta::RowsAppended { table: None, ipc }) => {
                                if let Ok(batch) = decode_ipc(&ipc) {
                                    pending.extend(extract_span_rows(&batch));
                                }
                            }
                            Some(ViewDelta::RowsExpired { table: None, expired_count }) => {
                                pending_expired += expired_count;
                            }
                            Some(ViewDelta::Ready) => {
                                if !pending.is_empty() {
                                    let _ = s.send(TracesInput::RowsBatch(std::mem::take(&mut pending)));
                                }
                                if pending_expired > 0 {
                                    let _ = s.send(TracesInput::Expired(std::mem::replace(&mut pending_expired, 0)));
                                }
                                let _ = s.send(TracesInput::StreamReady);
                                if !is_live {
                                    break;
                                }
                            }
                            Some(ViewDelta::Error { message }) => {
                                let _ = s.send(TracesInput::StreamError(message));
                            }
                            Some(_) => {}
                            None => {
                                if !pending.is_empty() {
                                    let _ = s.send(TracesInput::RowsBatch(std::mem::take(&mut pending)));
                                }
                                break;
                            }
                        },
                        _ = flush.tick() => {
                            if !pending.is_empty() {
                                let _ = s.send(TracesInput::RowsBatch(std::mem::take(&mut pending)));
                            }
                            if pending_expired > 0 {
                                let _ = s.send(TracesInput::Expired(std::mem::replace(&mut pending_expired, 0)));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let _ = s.send(TracesInput::StreamError(format!("{:#}", e)));
            }
        }
    });
    model.stream_task = Some(handle);
}

fn build_query(service_filter: Option<u32>, range: TimeRange) -> String {
    let mut parts = vec![format!("traces {}", range.seql_window())];
    if let Some(resource_id) = service_filter {
        parts.push(format!("where resource_id = {resource_id}"));
    }
    parts.join(" | ")
}

// ── Arrow IPC decode ──────────────────────────────────────────────────────────

fn decode_ipc(ipc: &[u8]) -> Result<arrow::record_batch::RecordBatch, String> {
    use std::io::Cursor;
    let cursor = Cursor::new(ipc);
    let mut reader =
        StreamReader::try_new(cursor, None).map_err(|e| format!("IPC reader error: {e}"))?;
    reader
        .next()
        .ok_or_else(|| "Empty IPC stream".to_string())?
        .map_err(|e| format!("IPC read error: {e}"))
}

fn extract_overflow_attrs(map_array: &MapArray, row: usize) -> Vec<(String, String)> {
    if map_array.is_null(row) {
        return vec![];
    }
    let entries = map_array.value(row);
    let struct_entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
    let keys = struct_entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let vals = struct_entries
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let mut out = Vec::new();
    for i in 0..keys.len() {
        if vals.is_null(i) {
            continue;
        }
        let key = keys.value(i).to_string();
        let value = cbor_decode_display(vals.value(i));
        if !value.is_empty() {
            out.push((key, value));
        }
    }
    out
}

fn cbor_decode_display(bytes: &[u8]) -> String {
    use std::io::Cursor;
    match ciborium::from_reader::<ciborium::Value, _>(&mut Cursor::new(bytes)) {
        Ok(ciborium::Value::Text(s)) => s,
        Ok(ciborium::Value::Integer(n)) => {
            let n: i128 = n.into();
            n.to_string()
        }
        Ok(ciborium::Value::Float(f)) => f.to_string(),
        Ok(ciborium::Value::Bool(b)) => b.to_string(),
        Ok(ciborium::Value::Bytes(b)) => format!("<{} bytes>", b.len()),
        Ok(ciborium::Value::Array(arr)) => format!("[{} items]", arr.len()),
        Ok(ciborium::Value::Map(m)) => format!("{{{} fields}}", m.len()),
        Ok(other) => format!("{other:?}"),
        Err(_) => String::new(),
    }
}

fn extract_span_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<SpanRow> {
    let schema = batch.schema();
    let n = batch.num_rows();

    macro_rules! col_str {
        ($name:expr) => {
            schema
                .index_of($name)
                .ok()
                .and_then(|i| batch.column(i).as_any().downcast_ref::<StringViewArray>())
        };
    }
    macro_rules! col_ts {
        ($name:expr) => {
            schema.index_of($name).ok().and_then(|i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
            })
        };
    }

    let trace_col = col_str!("trace_id");
    let span_col = col_str!("span_id");
    let parent_col = col_str!("parent_span_id");
    let name_col = col_str!("name");
    let kind_col = schema
        .index_of("kind")
        .ok()
        .and_then(|i| batch.column(i).as_any().downcast_ref::<UInt8Array>());
    let status_col = schema
        .index_of("status")
        .ok()
        .and_then(|i| batch.column(i).as_any().downcast_ref::<UInt8Array>());
    let start_col = col_ts!("start_time_unix_nano");
    let end_col = col_ts!("end_time_unix_nano");
    let dur_col = schema
        .index_of("duration_ns")
        .ok()
        .and_then(|i| batch.column(i).as_any().downcast_ref::<Int64Array>());
    let service_col = col_str!("service_name_attr");

    // Core span column names — everything else is a promoted attribute.
    const CORE_COLS: &[&str] = &[
        "trace_id",
        "span_id",
        "parent_span_id",
        "name",
        "kind",
        "status",
        "start_time_unix_nano",
        "end_time_unix_nano",
        "duration_ns",
        "resource_id",
        "scope_id",
        "_overflow_attrs",
    ];
    let attr_cols: Vec<(String, usize)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !CORE_COLS.contains(&f.name().as_str()))
        .map(|(idx, f)| (f.name().clone(), idx))
        .collect();

    let overflow_col = schema
        .index_of("_overflow_attrs")
        .ok()
        .and_then(|i| batch.column(i).as_any().downcast_ref::<MapArray>());

    (0..n)
        .map(|i| {
            let mut attributes: Vec<(String, String)> = attr_cols
                .iter()
                .filter_map(|(key, col_idx)| {
                    let col = batch.column(*col_idx);
                    if col.is_null(i) {
                        return None;
                    }
                    let val = col
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .map(|c| c.value(i).to_string())
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<StringArray>()
                                .map(|c| c.value(i).to_string())
                        })
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<Int64Array>()
                                .map(|c| c.value(i).to_string())
                        })
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<arrow::array::Float64Array>()
                                .map(|c| c.value(i).to_string())
                        })
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<arrow::array::BooleanArray>()
                                .map(|c| c.value(i).to_string())
                        })?;
                    if val.is_empty() {
                        None
                    } else {
                        Some((key.clone(), val))
                    }
                })
                .collect();
            if let Some(map) = overflow_col {
                attributes.extend(extract_overflow_attrs(map, i));
            }
            attributes.sort_by(|a, b| a.0.cmp(&b.0));
            SpanRow {
                trace_id: trace_col
                    .map(|c| c.value(i).to_string())
                    .unwrap_or_default(),
                span_id: span_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                parent_span_id: parent_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        let v = c.value(i);
                        if v.is_empty() {
                            None
                        } else {
                            Some(v.to_string())
                        }
                    }
                }),
                name: name_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                kind: kind_col.map(|c| c.value(i)).unwrap_or(0),
                status: status_col.map(|c| c.value(i)).unwrap_or(0),
                start_ns: start_col.map(|c| c.value(i)).unwrap_or(0),
                end_ns: end_col.map(|c| c.value(i)).unwrap_or(0),
                duration_ns: dur_col.map(|c| c.value(i)).unwrap_or(0),
                service_name: service_col
                    .map(|c| c.value(i).to_string())
                    .unwrap_or_default(),
                attributes,
            }
        })
        .collect()
}
