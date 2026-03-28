//! Profiles tab — interactive icicle flamegraph chart backed by FlamegraphStrategy.
//!
//! Renders a Cairo icicle chart where each node's width is proportional to its
//! total_value fraction of its parent. Roots span the full chart width.
//!
//! Interactions:
//!   - Hover → highlight node + show detail panel
//!   - Click → zoom into subtree (click again to zoom out)
//!
//! Architecture: model owns all node data (Send). IcicleDrawState lives in
//! Widgets via Rc<RefCell<>> and is updated in update_view() before queue_draw().

use crate::data::AppDataSource;
use crate::drawing::{self, ChartTheme};
use crate::time_range::TimeRange;
use arrow::array::{Array, Int64Array, StringArray, StringViewArray, UInt32Array};
use arrow::ipc::reader::StreamReader;
use gtk4::glib;
use gtk4::prelude::*;
use relm4::prelude::*;
use sequins_view::{FlamegraphStrategy, ViewDelta};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

const RETENTION_NS: u64 = 3_600_000_000_000; // 1 hour
const ROW_HEIGHT: f64 = 24.0;
const MIN_WIDTH: f64 = 2.0;

// ── Node data ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct FlamegraphNode {
    pub path_key: String,
    pub function_name: String,
    pub filename: Option<String>,
    pub line: Option<i64>,
    pub depth: u32,
    pub parent_path_key: Option<String>,
    pub total_value: i64,
    pub self_value: i64,
}

// ── Icicle draw state (lives in Widgets, not model) ───────────────────────────

struct IcicleDrawState {
    /// All nodes, cloned from model on each update_view.
    nodes: HashMap<String, FlamegraphNode>,
    /// parent_path_key → sorted child path_keys (by total_value desc).
    children_index: HashMap<Option<String>, Vec<String>>,
    root_total: i64,
    /// Selected node key (white border + full opacity).
    selected_key: Option<String>,
    /// Hovered node key (highlight + detail panel).
    hovered_key: Option<String>,
    /// Current zoom scale (content_width = viewport_width * zoom_scale).
    zoom_scale: f64,
    /// Last known mouse X in content coordinates (for Ctrl+Scroll anchor).
    mouse_x: f64,
    /// Pre-computed layout: (path_key, x, y, w, h).
    layout: Vec<(String, f64, f64, f64, f64)>,
    /// Width the layout was computed for (recompute if changed).
    layout_width: f64,
    /// Search text for client-side highlighting (empty = no highlight).
    search_text: String,
}

impl Default for IcicleDrawState {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            children_index: HashMap::new(),
            root_total: 0,
            selected_key: None,
            hovered_key: None,
            zoom_scale: 1.0,
            mouse_x: 0.0,
            layout: Vec::new(),
            layout_width: 0.0,
            search_text: String::new(),
        }
    }
}

impl IcicleDrawState {
    /// Recompute children_index and layout for the given drawing width.
    fn recompute_layout(&mut self, width: f64) {
        // Rebuild children index
        self.children_index.clear();
        for (key, node) in &self.nodes {
            self.children_index
                .entry(node.parent_path_key.clone())
                .or_default()
                .push(key.clone());
        }
        // Sort each children list by total_value desc for consistent layout
        let nodes = &self.nodes;
        for children in self.children_index.values_mut() {
            children.sort_by(|a, b| {
                let ta = nodes.get(a).map_or(0, |n| n.total_value);
                let tb = nodes.get(b).map_or(0, |n| n.total_value);
                tb.cmp(&ta)
            });
        }

        self.layout.clear();
        self.layout_width = width;

        let roots: Vec<String> = self.children_index.get(&None).cloned().unwrap_or_default();
        let total = self.root_total.max(1);

        let mut x = 0.0;
        for root_key in &roots {
            if let Some(node) = self.nodes.get(root_key).cloned() {
                let w = (node.total_value as f64 / total as f64) * width;
                if w >= MIN_WIDTH {
                    layout_recursive(
                        &self.nodes,
                        &self.children_index,
                        root_key,
                        x,
                        0.0,
                        w,
                        &mut self.layout,
                    );
                }
                x += w;
            }
        }
    }

    fn hit_test(&self, mx: f64, my: f64) -> Option<String> {
        // Iterate in reverse — deeper nodes drawn last, take priority
        for (key, x, y, w, h) in self.layout.iter().rev() {
            if mx >= *x && mx < x + w && my >= *y && my < y + h {
                return Some(key.clone());
            }
        }
        None
    }
}

fn layout_recursive(
    nodes: &HashMap<String, FlamegraphNode>,
    children_index: &HashMap<Option<String>, Vec<String>>,
    key: &str,
    x: f64,
    y: f64,
    w: f64,
    out: &mut Vec<(String, f64, f64, f64, f64)>,
) {
    out.push((key.to_string(), x, y, w, ROW_HEIGHT));

    let parent_total = nodes.get(key).map_or(1, |n| n.total_value.max(1)) as f64;
    let empty = Vec::new();
    let children = children_index.get(&Some(key.to_string())).unwrap_or(&empty);

    let mut cx = x;
    for child_key in children {
        if let Some(child) = nodes.get(child_key) {
            let cw = (child.total_value as f64 / parent_total) * w;
            if cw >= MIN_WIDTH {
                layout_recursive(
                    nodes,
                    children_index,
                    child_key,
                    cx,
                    y + ROW_HEIGHT,
                    cw,
                    out,
                );
            }
            cx += cw;
        }
    }
}

// ── Drawing ───────────────────────────────────────────────────────────────────

fn draw_icicle(cr: &cairo::Context, width: f64, height: f64, state: &IcicleDrawState) {
    let theme = ChartTheme::current();

    // Background
    let (r, g, b) = theme.ruler_bg();
    cr.set_source_rgb(r, g, b);
    cr.rectangle(0.0, 0.0, width, height);
    let _ = cr.fill();

    if state.layout.is_empty() {
        let (pr, pg, pb, pa) = theme.placeholder();
        cr.set_source_rgba(pr, pg, pb, pa);
        let _ = cr.select_font_face("Sans", cairo::FontSlant::Normal, cairo::FontWeight::Normal);
        cr.set_font_size(13.0);
        cr.move_to(width / 2.0 - 80.0, height / 2.0);
        let _ = cr.show_text("No profile data yet…");
        return;
    }

    let search_lower = state.search_text.to_lowercase();

    for (key, x, y, w, h) in &state.layout {
        let is_hovered = state.hovered_key.as_deref() == Some(key.as_str());
        let is_selected = state.selected_key.as_deref() == Some(key.as_str());
        let node_name = state
            .nodes
            .get(key)
            .map(|n| n.function_name.as_str())
            .unwrap_or("");

        let is_match = search_lower.is_empty() || node_name.to_lowercase().contains(&search_lower);
        let alpha = if search_lower.is_empty() || is_match {
            if is_selected {
                1.0
            } else if is_hovered {
                0.85
            } else {
                0.75
            }
        } else {
            0.25_f64
        };

        // Ratio = fraction of parent's value consumed by this node (1.0 for roots).
        let node = state.nodes.get(key.as_str());
        let parent_value = node
            .and_then(|n| n.parent_path_key.as_deref())
            .and_then(|pk| state.nodes.get(pk))
            .map_or_else(|| node.map_or(1, |n| n.total_value), |p| p.total_value);
        let ratio = node.map_or(1.0, |n| {
            if parent_value > 0 {
                n.total_value as f64 / parent_value as f64
            } else {
                1.0
            }
        });

        let (r, g, b) = drawing::frame_color_for_ratio(ratio);
        cr.set_source_rgba(r, g, b, alpha);
        cr.rectangle(x + 0.5, y + 0.5, w - 1.0, h - 1.0);
        let _ = cr.fill();

        // Dark border — thicker on hover/selected
        let border_alpha = if is_hovered || is_selected { 0.6 } else { 0.2 };
        cr.set_source_rgba(0.0, 0.0, 0.0, border_alpha * alpha);
        cr.set_line_width(if is_hovered || is_selected { 1.5 } else { 0.5 });
        cr.rectangle(x + 0.5, y + 0.5, w - 1.0, h - 1.0);
        let _ = cr.stroke();

        // Selected: white highlight border (drawn on top)
        if is_selected {
            cr.set_source_rgba(1.0, 1.0, 1.0, 1.0);
            cr.set_line_width(1.5);
            cr.rectangle(x + 1.0, y + 1.0, w - 2.0, h - 2.0);
            let _ = cr.stroke();
        }

        // Search match: yellow border
        if !search_lower.is_empty() && is_match && !is_selected {
            cr.set_source_rgba(1.0, 0.85, 0.0, alpha);
            cr.set_line_width(1.0);
            cr.rectangle(x + 0.5, y + 0.5, w - 1.0, h - 1.0);
            let _ = cr.stroke();
        }

        // Label (only if wide enough) — contrast-aware text color
        if *w > 28.0 {
            let (tr, tg, tb) = drawing::contrasting_text_color(r, g, b);
            cr.set_source_rgba(tr, tg, tb, alpha);
            let _ =
                cr.select_font_face("Sans", cairo::FontSlant::Normal, cairo::FontWeight::Normal);
            cr.set_font_size(11.0);
            let _ = cr.save();
            cr.rectangle(*x, *y, *w, *h);
            let _ = cr.clip();
            cr.move_to(x + 4.0, y + h - 7.0);
            let _ = cr.show_text(node_name);
            let _ = cr.restore();
        }
    }

    // Sample count watermark
    if state.root_total > 0 {
        cr.set_source_rgba(1.0, 1.0, 1.0, 0.3);
        let _ = cr.select_font_face("Sans", cairo::FontSlant::Normal, cairo::FontWeight::Normal);
        cr.set_font_size(10.0);
        let label = format!("{} samples", state.root_total);
        cr.move_to(width - 110.0, height - 6.0);
        let _ = cr.show_text(&label);
    }
}

// ── Component ──────────────────────────────────────────────────────────────────

pub struct ProfilesInit {
    pub data_source: Arc<AppDataSource>,
}

pub struct ProfilesModel {
    data_source: Arc<AppDataSource>,
    time_range: TimeRange,
    is_live: bool,
    service_filter: Option<u32>,
    nodes: HashMap<String, FlamegraphNode>,
    root_total: i64,
    node_count: usize,
    search_text: String,
    selected_value_type: Option<String>,
    available_value_types: Vec<String>,
    stream_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
pub enum ProfilesInput {
    DeltaBatch(Vec<ViewDelta>),
    SetTimeRange(TimeRange, bool),
    ServiceFilter(Option<u32>),
    StreamError(String),
    SearchChanged(String),
    ValueTypeChanged(Option<String>),
    SetAvailableValueTypes(Vec<String>),
}

#[derive(Debug)]
pub enum ProfilesCmd {
    ValueTypesLoaded(Vec<String>),
}

pub struct ProfilesWidgets {
    root: gtk4::Box,
    search_entry: gtk4::SearchEntry,
    value_type_dropdown: gtk4::DropDown,
    value_type_string_list: gtk4::StringList,
    drawing_area: gtk4::DrawingArea,
    detail_panel: gtk4::Box,
    detail_fn_label: gtk4::Label,
    detail_loc_label: gtk4::Label,
    detail_self_label: gtk4::Label,
    detail_total_label: gtk4::Label,
    draw_state: Rc<RefCell<IcicleDrawState>>,
}

impl Component for ProfilesModel {
    type CommandOutput = ProfilesCmd;
    type Init = ProfilesInit;
    type Input = ProfilesInput;
    type Output = ();
    type Root = gtk4::Box;
    type Widgets = ProfilesWidgets;

    fn init_root() -> Self::Root {
        gtk4::Box::new(gtk4::Orientation::Vertical, 0)
    }

    fn init(
        init: Self::Init,
        root: Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        let draw_state: Rc<RefCell<IcicleDrawState>> =
            Rc::new(RefCell::new(IcicleDrawState::default()));

        // ── Compact filter bar ───────────────────────────────────────────────
        let search_entry = gtk4::SearchEntry::builder()
            .placeholder_text("Search frames…")
            .width_request(200)
            .build();
        {
            let s = sender.clone();
            search_entry.connect_search_changed(move |e| {
                s.input(ProfilesInput::SearchChanged(e.text().to_string()));
            });
        }

        // Value type dropdown — starts with just "All Types", populated async
        let value_type_string_list = gtk4::StringList::new(&["All Types"]);
        let value_type_dropdown = gtk4::DropDown::builder()
            .model(&value_type_string_list)
            .selected(0)
            .build();
        {
            let s = sender.clone();
            let vt_list = value_type_string_list.clone();
            value_type_dropdown.connect_selected_notify(move |dd| {
                let idx = dd.selected();
                if idx == 0 {
                    s.input(ProfilesInput::ValueTypeChanged(None));
                } else {
                    let selected_str = vt_list.string(idx).map(|gs| gs.to_string());
                    s.input(ProfilesInput::ValueTypeChanged(selected_str));
                }
            });
        }

        let toolbar = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(8)
            .margin_start(8)
            .margin_end(8)
            .build();
        toolbar.append(&search_entry);
        toolbar.append(&value_type_dropdown);

        // ── Drawing area ──────────────────────────────────────────────────────
        let drawing_area = gtk4::DrawingArea::builder()
            .vexpand(true)
            .hexpand(true)
            .content_height(400)
            .build();

        // Draw function — layout is computed lazily here, not in update_view.
        {
            let ds = Rc::clone(&draw_state);
            drawing_area.set_draw_func(move |_, cr, w, h| {
                let mut state = ds.borrow_mut();
                // Recompute layout if dirty or width changed.
                let needs_layout = !state.nodes.is_empty()
                    && (state.layout.is_empty() || (state.layout_width - w as f64).abs() > 1.0);
                if needs_layout {
                    state.recompute_layout(w as f64);
                }
                draw_icicle(cr, w as f64, h as f64, &state);
            });
        }

        // ── Detail panel ──────────────────────────────────────────────────────
        let detail_fn_label = gtk4::Label::builder().xalign(0.0).hexpand(true).build();
        let detail_loc_label = gtk4::Label::builder()
            .xalign(0.0)
            .css_classes(["caption", "dim-label"])
            .build();
        let detail_self_label = gtk4::Label::builder()
            .xalign(0.0)
            .css_classes(["numeric"])
            .build();
        let detail_total_label = gtk4::Label::builder()
            .xalign(0.0)
            .css_classes(["numeric"])
            .build();

        let detail_panel = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(2)
            .margin_start(12)
            .margin_end(12)
            .margin_top(6)
            .margin_bottom(6)
            .visible(false)
            .build();

        for (lbl_text, val_widget) in [
            ("Function", detail_fn_label.upcast_ref::<gtk4::Widget>()),
            ("Location", detail_loc_label.upcast_ref()),
        ] {
            let row = gtk4::Box::builder()
                .orientation(gtk4::Orientation::Horizontal)
                .spacing(8)
                .build();
            let lbl = gtk4::Label::builder()
                .label(lbl_text)
                .css_classes(["caption", "dim-label"])
                .width_chars(9)
                .xalign(0.0)
                .build();
            row.append(&lbl);
            row.append(val_widget);
            detail_panel.append(&row);
        }

        let values_row = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(20)
            .build();
        for (lbl_text, val_widget) in [
            ("Self", detail_self_label.upcast_ref::<gtk4::Widget>()),
            ("Total", detail_total_label.upcast_ref()),
        ] {
            let pair = gtk4::Box::builder()
                .orientation(gtk4::Orientation::Horizontal)
                .spacing(4)
                .build();
            pair.append(
                &gtk4::Label::builder()
                    .label(lbl_text)
                    .css_classes(["caption", "dim-label"])
                    .build(),
            );
            pair.append(val_widget);
            values_row.append(&pair);
        }
        detail_panel.append(&values_row);

        // ── Motion controller (hover) ──────────────────────────────────────────
        {
            let ds = Rc::clone(&draw_state);
            let da = drawing_area.clone();
            let dp = detail_panel.clone();
            let dfn = detail_fn_label.clone();
            let dloc = detail_loc_label.clone();
            let dself = detail_self_label.clone();
            let dtotal = detail_total_label.clone();
            let motion = gtk4::EventControllerMotion::new();
            motion.connect_motion(move |_, x, y| {
                let mut state = ds.borrow_mut();
                state.mouse_x = x; // track for Ctrl+Scroll zoom anchor
                let hit = state.hit_test(x, y);
                if hit != state.hovered_key {
                    state.hovered_key = hit.clone();
                    da.queue_draw();
                    if let Some(key) = &hit {
                        if let Some(node) = state.nodes.get(key).cloned() {
                            drop(state);
                            dp.set_visible(true);
                            dfn.set_text(&node.function_name);
                            dloc.set_text(&node_location(&node));
                            dself.set_text(&node.self_value.to_string());
                            dtotal.set_text(&node.total_value.to_string());
                        }
                    } else {
                        drop(state);
                        dp.set_visible(false);
                    }
                }
            });
            let ds2 = Rc::clone(&draw_state);
            let da2 = drawing_area.clone();
            let dp2 = detail_panel.clone();
            motion.connect_leave(move |_| {
                let mut state = ds2.borrow_mut();
                if state.hovered_key.is_some() {
                    state.hovered_key = None;
                    da2.queue_draw();
                    drop(state);
                    dp2.set_visible(false);
                }
            });
            drawing_area.add_controller(motion);
        }

        let scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Automatic)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&drawing_area)
            .build();

        // ── Click controller: zoom to frame (macOS animateZoom logic) ─────────
        {
            let ds = Rc::clone(&draw_state);
            let da = drawing_area.clone();
            let sc = scroll.clone();
            let gesture = gtk4::GestureClick::new();
            gesture.connect_released(move |_, _, x, y| {
                let mut state = ds.borrow_mut();
                let hit = state.hit_test(x, y);
                let viewport_w = sc.width() as f64;

                // Reset helper — shared logic for deselect/dezoom
                fn reset(
                    state: &mut IcicleDrawState,
                    da: &gtk4::DrawingArea,
                    sc: &gtk4::ScrolledWindow,
                ) {
                    state.selected_key = None;
                    state.zoom_scale = 1.0;
                    state.layout.clear();
                    da.set_content_width(0);
                    sc.hadjustment().set_value(0.0);
                    da.queue_draw();
                }

                match hit {
                    None => {
                        if state.zoom_scale != 1.0 || state.selected_key.is_some() {
                            reset(&mut state, &da, &sc);
                        }
                    }
                    Some(key) => {
                        if state.selected_key.as_ref() == Some(&key) {
                            // Second click on same frame → reset zoom
                            reset(&mut state, &da, &sc);
                        } else {
                            // Zoom to this frame: scale so it fills the viewport width.
                            // Compute the node's position at scale=1 by dividing by
                            // current zoom_scale (all layout coords are proportional).
                            let hit_rect = state.layout.iter().find(|(k, ..)| k == &key).cloned();
                            if let Some((_, hx, _, hw, _)) = hit_rect {
                                let cur = state.zoom_scale;
                                let node_x1 = hx / cur;
                                let node_w1 = hw / cur;
                                let new_scale = (viewport_w / node_w1.max(1.0)).clamp(1.0, 50.0);
                                let new_scroll_x = node_x1 * new_scale;

                                state.selected_key = Some(key);
                                state.zoom_scale = new_scale;
                                state.layout.clear();
                                da.set_content_width((viewport_w * new_scale) as i32);
                                da.queue_draw();
                                // Defer scroll until after GTK has relayed the new size
                                let adj = sc.hadjustment();
                                glib::idle_add_local_once(move || {
                                    adj.set_value(new_scroll_x);
                                });
                            }
                        }
                    }
                }
            });
            drawing_area.add_controller(gesture);
        }

        // ── Ctrl+Scroll: zoom anchored at cursor (mirrors macOS MagnificationGesture) ──
        {
            let ds = Rc::clone(&draw_state);
            let da = drawing_area.clone();
            let sc = scroll.clone();
            let scroll_ctrl =
                gtk4::EventControllerScroll::new(gtk4::EventControllerScrollFlags::VERTICAL);
            scroll_ctrl.connect_scroll(move |ctrl, _dx, dy| {
                let mods = ctrl.current_event_state();
                if !mods.contains(gtk4::gdk::ModifierType::CONTROL_MASK) {
                    return glib::Propagation::Proceed;
                }
                let mut state = ds.borrow_mut();
                let viewport_w = sc.width() as f64;
                let factor = if dy < 0.0 { 1.15_f64 } else { 1.0 / 1.15 };
                let new_scale = (state.zoom_scale * factor).clamp(1.0, 50.0);
                let actual_factor = new_scale / state.zoom_scale;

                // Anchor: keep the content point under the cursor fixed in the viewport.
                let mx = state.mouse_x;
                let sx = sc.hadjustment().value();
                let new_sx = (mx * (actual_factor - 1.0) + sx).max(0.0);

                state.zoom_scale = new_scale;
                state.layout.clear();
                if (new_scale - 1.0).abs() < 0.01 {
                    state.selected_key = None;
                    da.set_content_width(0);
                } else {
                    da.set_content_width((viewport_w * new_scale) as i32);
                }
                da.queue_draw();
                let adj = sc.hadjustment();
                glib::idle_add_local_once(move || {
                    adj.set_value(new_sx);
                });
                glib::Propagation::Stop
            });
            drawing_area.add_controller(scroll_ctrl);
        }

        toolbar.set_widget_name("tab-toolbar");
        root.append(&toolbar);
        root.append(&scroll);
        root.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        root.append(&detail_panel);

        let mut model = ProfilesModel {
            data_source: init.data_source.clone(),
            time_range: TimeRange::default(),
            is_live: true,
            service_filter: None,
            nodes: HashMap::new(),
            root_total: 0,
            node_count: 0,
            search_text: String::new(),
            selected_value_type: None,
            available_value_types: Vec::new(),
            stream_task: None,
        };

        start_stream(&mut model, sender.clone());

        // Fetch available value types in the background
        {
            let ds = init.data_source.clone();
            sender.command(move |out, _shutdown| async move {
                let query = "samples last 1h | summarize count() by value_type | sort count() desc | take 20";
                if let Ok(batches) = ds.snapshot_batches(query).await {
                    let types = decode_value_types(&batches);
                    if !types.is_empty() {
                        let _ = out.send(ProfilesCmd::ValueTypesLoaded(types));
                    }
                }
            });
        }

        ComponentParts {
            model,
            widgets: ProfilesWidgets {
                root,
                search_entry,
                value_type_dropdown,
                value_type_string_list,
                drawing_area,
                detail_panel,
                detail_fn_label,
                detail_loc_label,
                detail_self_label,
                detail_total_label,
                draw_state,
            },
        }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            ProfilesInput::DeltaBatch(deltas) => {
                for delta in deltas {
                    self.apply_delta(delta);
                }
            }
            ProfilesInput::SetTimeRange(range, live) => {
                if range != self.time_range || live != self.is_live {
                    self.time_range = range;
                    self.is_live = live;
                    if let Some(h) = self.stream_task.take() {
                        h.abort();
                    }
                    self.nodes.clear();
                    self.root_total = 0;
                    self.node_count = 0;
                    start_stream(self, sender);
                }
            }
            ProfilesInput::ServiceFilter(filter) => {
                if filter != self.service_filter {
                    self.service_filter = filter;
                    if let Some(h) = self.stream_task.take() {
                        h.abort();
                    }
                    self.nodes.clear();
                    self.root_total = 0;
                    self.node_count = 0;
                    start_stream(self, sender);
                }
            }
            ProfilesInput::StreamError(e) => {
                tracing::error!("Profiles stream error: {}", e);
            }
            ProfilesInput::SearchChanged(text) => {
                self.search_text = text;
                // search is client-side (highlight only), no stream restart
            }
            ProfilesInput::ValueTypeChanged(vt) => {
                if vt != self.selected_value_type {
                    self.selected_value_type = vt;
                    if let Some(h) = self.stream_task.take() {
                        h.abort();
                    }
                    self.nodes.clear();
                    self.root_total = 0;
                    self.node_count = 0;
                    start_stream(self, sender);
                }
            }
            ProfilesInput::SetAvailableValueTypes(types) => {
                self.available_value_types = types;
            }
        }
    }

    fn update_cmd(
        &mut self,
        msg: Self::CommandOutput,
        sender: ComponentSender<Self>,
        _root: &Self::Root,
    ) {
        match msg {
            ProfilesCmd::ValueTypesLoaded(types) => {
                sender.input(ProfilesInput::SetAvailableValueTypes(types));
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, _sender: ComponentSender<Self>) {
        // Update node count in status label
        // Update value type dropdown options if changed
        let current_n = widgets.value_type_string_list.n_items();
        let expected_n = (self.available_value_types.len() + 1) as u32; // +1 for "All Types"
        if current_n != expected_n {
            // Rebuild the list
            let mut items: Vec<&str> = vec!["All Types"];
            items.extend(self.available_value_types.iter().map(|s| s.as_str()));
            widgets.value_type_string_list.splice(0, current_n, &items);
            // Restore selection
            let selected_idx = self
                .selected_value_type
                .as_ref()
                .and_then(|vt| self.available_value_types.iter().position(|t| t == vt))
                .map(|i| (i + 1) as u32)
                .unwrap_or(0);
            widgets.value_type_dropdown.set_selected(selected_idx);
        }

        // Pass search text to draw state for highlighting
        {
            let mut state = widgets.draw_state.borrow_mut();
            if state.search_text != self.search_text {
                state.search_text = self.search_text.clone();
                // force layout recompute so highlighting updates
                if state.layout_width > 0.0 {
                    state.layout.clear();
                }
            }
        }

        // Copy nodes into draw state and mark layout dirty; layout is computed
        // lazily in the draw func so update_view stays cheap.
        {
            let mut state = widgets.draw_state.borrow_mut();
            state.nodes = self.nodes.clone();
            state.root_total = self.root_total;
            state.layout.clear();
            state.layout_width = 0.0;
        }

        // Resize drawing area to show full tree depth
        let max_depth = self.nodes.values().map(|n| n.depth).max().unwrap_or(0);
        let chart_h = ((max_depth + 2) as f64 * ROW_HEIGHT) as i32;
        widgets.drawing_area.set_content_height(chart_h.max(200));

        widgets.drawing_area.queue_draw();
    }
}

fn node_location(node: &FlamegraphNode) -> String {
    match (&node.filename, node.line) {
        (Some(f), Some(l)) => {
            let short = f
                .rsplit('/')
                .take(2)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("/");
            format!("{}:{}", short, l)
        }
        (Some(f), None) => f.rsplit('/').next().unwrap_or(f.as_str()).to_string(),
        _ => String::new(),
    }
}

// ── Delta application ─────────────────────────────────────────────────────────

impl ProfilesModel {
    fn apply_delta(&mut self, delta: ViewDelta) {
        match delta {
            ViewDelta::EntityCreated {
                key,
                descriptor_ipc,
                data_ipc,
            } => {
                if let (Ok(desc), Ok(data)) = (decode_ipc(&descriptor_ipc), decode_ipc(&data_ipc)) {
                    if let Some(node) = extract_node(&key, &desc, &data) {
                        if node.depth == 0 {
                            self.root_total += node.total_value;
                        }
                        self.nodes.insert(key, node);
                        self.node_count += 1;
                    }
                }
            }
            ViewDelta::EntityDataReplaced { key, data_ipc } => {
                if let Ok(data) = decode_ipc(&data_ipc) {
                    self.update_node_values(&key, &data);
                }
            }
            ViewDelta::EntityRemoved { key } => {
                if let Some(node) = self.nodes.remove(&key) {
                    if node.depth == 0 {
                        self.root_total -= node.total_value;
                    }
                    self.node_count = self.node_count.saturating_sub(1);
                }
            }
            ViewDelta::Ready => {
                tracing::debug!("Profiles stream ready ({} nodes)", self.node_count);
            }
            ViewDelta::Error { message } => {
                tracing::error!("Profiles stream error: {}", message);
            }
            _ => {}
        }
    }

    fn update_node_values(&mut self, key: &str, data: &arrow::record_batch::RecordBatch) {
        let total = data
            .schema()
            .index_of("total_value")
            .ok()
            .and_then(|i| data.column(i).as_any().downcast_ref::<Int64Array>())
            .map(|c| c.value(0))
            .unwrap_or(0);
        let self_val = data
            .schema()
            .index_of("self_value")
            .ok()
            .and_then(|i| data.column(i).as_any().downcast_ref::<Int64Array>())
            .map(|c| c.value(0))
            .unwrap_or(0);

        if let Some(node) = self.nodes.get_mut(key) {
            if node.depth == 0 {
                self.root_total += total - node.total_value;
            }
            node.total_value = total;
            node.self_value = self_val;
        }
    }
}

// ── Stream launcher ───────────────────────────────────────────────────────────

fn build_query_profiles(
    range: TimeRange,
    service_filter: Option<u32>,
    value_type: Option<&str>,
) -> String {
    let window = range.seql_window();
    let mut parts = vec![format!("samples {window}")];
    if let Some(rid) = service_filter {
        parts.push(format!("where resource_id = {rid}"));
    }
    if let Some(vt) = value_type {
        let esc = vt.replace('\'', "''");
        parts.push(format!("where value_type = '{esc}'"));
    }
    let base = parts.join(" | ");
    format!("{base} <- stacks <- frames")
}

fn start_stream(model: &mut ProfilesModel, sender: ComponentSender<ProfilesModel>) {
    let ds = model.data_source.clone();
    let range = model.time_range;
    let is_live = model.is_live;
    let service_filter = model.service_filter;
    let value_type = model.selected_value_type.clone();
    let s = sender.input_sender().clone();
    let query = build_query_profiles(range, service_filter, value_type.as_deref());

    let handle = relm4::spawn(async move {
        use futures::StreamExt;
        use tokio::time::{interval, Duration, MissedTickBehavior};

        let strategy = FlamegraphStrategy::new(RETENTION_NS);
        match ds.live_view(&query, &strategy).await {
            Ok(stream) => {
                futures::pin_mut!(stream);
                let mut pending: Vec<ViewDelta> = Vec::new();
                let mut flush = interval(Duration::from_millis(200));
                flush.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        biased;
                        delta = stream.next() => match delta {
                            Some(ViewDelta::Ready) => {
                                pending.push(ViewDelta::Ready);
                                if !pending.is_empty() {
                                    let _ = s.send(ProfilesInput::DeltaBatch(std::mem::take(&mut pending)));
                                }
                                if !is_live {
                                    break;
                                }
                            }
                            Some(d) => pending.push(d),
                            None => {
                                if !pending.is_empty() {
                                    let _ = s.send(ProfilesInput::DeltaBatch(std::mem::take(&mut pending)));
                                }
                                break;
                            }
                        },
                        _ = flush.tick() => {
                            if !pending.is_empty() {
                                let _ = s.send(ProfilesInput::DeltaBatch(std::mem::take(&mut pending)));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let _ = s.send(ProfilesInput::StreamError(format!("{:#}", e)));
            }
        }
    });
    model.stream_task = Some(handle);
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

fn decode_value_types(batches: &[Vec<u8>]) -> Vec<String> {
    let mut types = Vec::new();
    for ipc in batches {
        if let Ok(batch) = decode_ipc(ipc) {
            if let Ok(idx) = batch.schema().index_of("value_type") {
                let col = batch.column(idx);
                // Try StringViewArray first, then StringArray
                if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            types.push(arr.value(i).to_string());
                        }
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            types.push(arr.value(i).to_string());
                        }
                    }
                }
            }
        }
    }
    types
}

fn extract_node(
    key: &str,
    desc: &arrow::record_batch::RecordBatch,
    data: &arrow::record_batch::RecordBatch,
) -> Option<FlamegraphNode> {
    let desc_schema = desc.schema();
    let data_schema = data.schema();

    macro_rules! str_col {
        ($batch:expr, $schema:expr, $name:expr) => {
            $schema
                .index_of($name)
                .ok()
                .and_then(|i| $batch.column(i).as_any().downcast_ref::<StringArray>())
                .map(|c| c.value(0).to_string())
        };
    }

    let function_name = str_col!(desc, desc_schema, "function_name")?;
    let filename = str_col!(desc, desc_schema, "filename");
    let parent_path_key = str_col!(desc, desc_schema, "parent_path_key");
    let depth = desc_schema
        .index_of("depth")
        .ok()
        .and_then(|i| desc.column(i).as_any().downcast_ref::<UInt32Array>())
        .map(|c| c.value(0))
        .unwrap_or(0);
    let line = desc_schema
        .index_of("line")
        .ok()
        .and_then(|i| desc.column(i).as_any().downcast_ref::<Int64Array>())
        .and_then(|c| if c.is_null(0) { None } else { Some(c.value(0)) });

    let total_value = data_schema
        .index_of("total_value")
        .ok()
        .and_then(|i| data.column(i).as_any().downcast_ref::<Int64Array>())
        .map(|c| c.value(0))
        .unwrap_or(0);
    let self_value = data_schema
        .index_of("self_value")
        .ok()
        .and_then(|i| data.column(i).as_any().downcast_ref::<Int64Array>())
        .map(|c| c.value(0))
        .unwrap_or(0);

    Some(FlamegraphNode {
        path_key: key.to_string(),
        function_name,
        filename,
        line,
        depth,
        parent_path_key: parent_path_key.filter(|s| !s.is_empty()),
        total_value,
        self_value,
    })
}
