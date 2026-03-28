//! Metrics tab — scrollable grid of chart cards, one per metric.
//!
//! Matches macOS layout: 2-column FlowBox with three sections
//! (System Metrics, Application Metrics, Histogram Metrics).
//! Each card shows a type pill, metric name, latest value, and a 120px Cairo chart.

use crate::data::AppDataSource;
use crate::drawing::{compute_time_ticks, compute_value_ticks, format_value, ChartTheme};
use crate::time_range::{AppTimeRange, LiveRange, PausedRange, TimeRange};

use arrow::array::{Array, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray};
use arrow::ipc::reader::StreamReader;
use gtk4::glib;
use gtk4::prelude::*;
use relm4::prelude::*;
use sequins_view::{TableStrategy, ViewDelta};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

// ── Data model ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct MetricLine {
    pub metric_id: String,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub metric_type: String,
    pub service_name: String,
    pub latest_value: Option<f64>,
    pub latest_time_ns: i64,
    pub point_count: usize,
    pub data_points: Vec<(i64, f64)>,
}

impl MetricLine {
    pub fn value_display(&self) -> String {
        match self.latest_value {
            Some(v) => format_value(v, &self.unit),
            None => "—".to_string(),
        }
    }
}

// ── Classification ─────────────────────────────────────────────────────────────

#[derive(PartialEq)]
enum MetricSection {
    System,
    Application,
    Histogram,
}

fn classify_metric(line: &MetricLine) -> MetricSection {
    if line.metric_type == "histogram" {
        return MetricSection::Histogram;
    }
    let lower = line.name.to_lowercase();
    if lower.contains("cpu")
        || lower.contains("memory")
        || lower.contains("heap")
        || lower.contains("utilization")
    {
        MetricSection::System
    } else {
        MetricSection::Application
    }
}

fn metric_chart_color(name: &str) -> (f64, f64, f64) {
    let lower = name.to_lowercase();
    if lower.contains("cpu") || lower.contains("utilization") {
        (0.37, 0.62, 1.0)
    } else if lower.contains("memory") || lower.contains("heap") {
        (0.30, 0.78, 0.40)
    } else if lower.contains("delay") || lower.contains("latency") {
        (1.0, 0.60, 0.20)
    } else if lower.contains("error") {
        (0.85, 0.22, 0.22)
    } else {
        (0.65, 0.40, 0.85)
    }
}

fn metric_pill_css_class(metric_type: &str) -> &'static str {
    match metric_type {
        "gauge" => "metric-pill-gauge",
        "counter" => "metric-pill-counter",
        "histogram" => "metric-pill-histogram",
        "summary" => "metric-pill-summary",
        _ => "metric-pill-gauge",
    }
}

// ── Card draw state ───────────────────────────────────────────────────────────

struct CardDrawState {
    points: Vec<(i64, f64)>,
    color: (f64, f64, f64),
    fill: bool,
    unit: String,
    /// Duration of the visible window in nanoseconds.
    window_duration_ns: i64,
    /// When true, x bounds are computed from the wall clock on every draw for smooth animation.
    /// When false, frozen_end_ns is used instead.
    is_live: bool,
    /// Used as x_end when is_live is false (paused mode).
    frozen_end_ns: i64,
}

fn draw_card_chart(state: &CardDrawState, cr: &cairo::Context, width: i32, height: i32) {
    let w = width as f64;
    let h = height as f64;
    let theme = ChartTheme::current();

    // Margins: left for Y-axis labels, bottom for X-axis labels
    let ml = 48.0;
    let mr = 8.0;
    let mt = 6.0;
    let mb = 22.0;
    let chart_w = (w - ml - mr).max(4.0);
    let chart_h = (h - mt - mb).max(4.0);

    // Background
    let (r, g, b) = theme.bg();
    let _ = cr.set_source_rgb(r, g, b);
    let _ = cr.paint();

    // X domain: computed fresh each frame in live mode for smooth animation.
    let x_end = if state.is_live {
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    } else {
        state.frozen_end_ns
    };
    let x_start = x_end - state.window_duration_ns;
    let t_range = state.window_duration_ns.max(1) as f64;

    // Y domain: always anchored at 0
    let max_v = state.points.iter().fold(0.0_f64, |a, p| a.max(p.1.abs()));
    let v_hi = if max_v < 0.001 { 1.0 } else { max_v * 1.10 };
    let v_span = v_hi;

    let to_xy = |t: i64, v: f64| -> (f64, f64) {
        let x = ml + (t - x_start) as f64 / t_range * chart_w;
        let y = mt + chart_h * (1.0 - v / v_span);
        (x, y)
    };

    // ── Y-axis labels + horizontal grid lines ─────────────────────────────────
    cr.select_font_face("Sans", cairo::FontSlant::Normal, cairo::FontWeight::Normal);
    let _ = cr.set_font_size(9.0);
    let _ = cr.set_line_width(1.0);

    let y_ticks = compute_value_ticks(v_hi, 3, &state.unit);
    for tick in &y_ticks {
        let y = mt + chart_h * (1.0 - tick.value / v_span);
        if y < mt || y > mt + chart_h {
            continue;
        }
        // Grid line
        let (gr, gg, gb, ga) = theme.grid();
        let _ = cr.set_source_rgba(gr, gg, gb, ga);
        let _ = cr.move_to(ml, y);
        let _ = cr.line_to(ml + chart_w, y);
        let _ = cr.stroke();
        // Label
        let (lr, lg, lb, la) = theme.label();
        let _ = cr.set_source_rgba(lr, lg, lb, la);
        if let Ok(extents) = cr.text_extents(&tick.label) {
            let label_x = (ml - extents.width() - 4.0).max(0.0);
            let _ = cr.move_to(label_x, y + 3.0);
        } else {
            let _ = cr.move_to(0.0, y + 3.0);
        }
        let _ = cr.show_text(&tick.label);
    }

    // ── X-axis labels + vertical grid lines ──────────────────────────────────
    let x_ticks = compute_time_ticks(x_start, x_end, 5);
    for tick in &x_ticks {
        let x = ml + (tick.ns - x_start) as f64 / t_range * chart_w;
        if x < ml || x > ml + chart_w {
            continue;
        }
        // Vertical grid line
        let (gr, gg, gb, ga) = theme.grid();
        let _ = cr.set_source_rgba(gr, gg, gb, ga);
        let _ = cr.move_to(x, mt);
        let _ = cr.line_to(x, mt + chart_h);
        let _ = cr.stroke();
        // Label centered below chart
        let (lr, lg, lb, la) = theme.label();
        let _ = cr.set_source_rgba(lr, lg, lb, la);
        let label_x = if let Ok(extents) = cr.text_extents(&tick.label) {
            (x - extents.width() / 2.0).max(ml)
        } else {
            x
        };
        let _ = cr.move_to(label_x, mt + chart_h + 14.0);
        let _ = cr.show_text(&tick.label);
    }

    if state.points.len() < 2 {
        let (pr, pg, pb, pa) = theme.placeholder();
        let _ = cr.set_source_rgba(pr, pg, pb, pa);
        let _ = cr.move_to(ml + chart_w / 2.0 - 20.0, mt + chart_h / 2.0 + 4.0);
        let _ = cr.show_text("No data");
        return;
    }

    let (r, g, b) = state.color;

    // Area fill for counters
    if state.fill && state.points.len() >= 2 {
        let _ = cr.set_source_rgba(r, g, b, 0.15);
        let bottom_y = mt + chart_h;
        if let Some(&(t0, v0)) = state.points.first() {
            let (x0, y0) = to_xy(t0, v0);
            let _ = cr.move_to(x0, bottom_y);
            let _ = cr.line_to(x0, y0);
        }
        for &(t, v) in state.points.iter().skip(1) {
            let (x, y) = to_xy(t, v);
            let _ = cr.line_to(x, y);
        }
        if let Some(&(t_last, _)) = state.points.last() {
            let (x_last, _) = to_xy(t_last, 0.0);
            let _ = cr.line_to(x_last, bottom_y);
        }
        let _ = cr.fill();
    }

    // Line
    let _ = cr.set_source_rgb(r, g, b);
    let _ = cr.set_line_width(1.5);
    if let Some(&(t0, v0)) = state.points.first() {
        let (x, y) = to_xy(t0, v0);
        let _ = cr.move_to(x, y);
    }
    for &(t, v) in state.points.iter().skip(1) {
        let (x, y) = to_xy(t, v);
        let _ = cr.line_to(x, y);
    }
    let _ = cr.stroke();
}

// ── Component ──────────────────────────────────────────────────────────────────

pub struct MetricsInit {
    pub data_source: Arc<AppDataSource>,
}

pub struct MetricsModel {
    data_source: Arc<AppDataSource>,
    service_filter: Option<String>,
    app_time_range: AppTimeRange,
    search_text: String,
    /// Wall-clock "now" in ns, set when a query starts. Used as frozen_end_ns in paused mode.
    window_now_ns: Option<i64>,
    /// Bin size in seconds for the current time range, embedded in the query.
    bin_seconds: i64,
    /// Accumulator for client-side binning of raw live WAL datapoints.
    /// metric_id -> bucket_ns -> (sum, count)
    bin_accumulators: HashMap<String, HashMap<i64, (f64, usize)>>,
    metrics: HashMap<String, MetricLine>,
    order: Vec<String>,
    /// Buffered (time_ns, value) pairs for metric IDs whose descriptor hasn't
    /// arrived yet. Flushed into the MetricLine when the descriptor arrives.
    pending_data_points: HashMap<String, Vec<(i64, f64)>>,
    /// Incremented whenever the set of metric IDs changes (add/remove).
    /// `update_view` only rebuilds FlowBox cards when this differs from
    /// the last rendered version; otherwise it just calls `queue_draw()`.
    metrics_version: u64,
    /// Incremented whenever data_points content changes (new data arrived).
    /// `update_view` only rebuilds draw_states when this differs from the
    /// last rendered version; animation frames skip the rebuild entirely.
    data_version: u64,
    stream_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
pub enum MetricsInput {
    DeltaBatch(Vec<ViewDelta>),
    ServiceFilter(Option<String>),
    SetTimeRange(TimeRange),
    SetAppTimeRange(AppTimeRange),
    SlidingWindowTick,
    SearchChanged(String),
    StreamError(String),
    /// Sent at ~30fps in live mode to trigger smooth chart redraw.
    AnimationFrame,
}

pub struct MetricsWidgets {
    root: gtk4::Box,
    granularity_label: gtk4::Label,
    system_section: gtk4::Box,
    system_flow: gtk4::FlowBox,
    app_section: gtk4::Box,
    app_flow: gtk4::FlowBox,
    histogram_section: gtk4::Box,
    histogram_flow: gtk4::FlowBox,
    draw_states: Rc<RefCell<HashMap<String, CardDrawState>>>,
    /// One DrawingArea per metric_id — used to call queue_draw() without
    /// rebuilding the entire FlowBox when only data points changed.
    card_drawing_areas: HashMap<String, gtk4::DrawingArea>,
    rendered_metrics_version: u64,
    rendered_data_version: u64,
    /// Animation timer active in live mode for smooth chart sliding.
    animation_source: Option<glib::SourceId>,
}

impl Component for MetricsModel {
    type CommandOutput = ();
    type Init = MetricsInit;
    type Input = MetricsInput;
    type Output = ();
    type Root = gtk4::Box;
    type Widgets = MetricsWidgets;

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
            .placeholder_text("Search metrics…")
            .width_request(200)
            .build();
        {
            let s = sender.clone();
            search_entry.connect_search_changed(move |e| {
                s.input(MetricsInput::SearchChanged(e.text().to_string()));
            });
        }

        let granularity_label = gtk4::Label::builder()
            .label("")
            .css_classes(["caption", "dim-label"])
            .build();

        let toolbar = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(8)
            .margin_start(8)
            .margin_end(8)
            .build();
        toolbar.append(&search_entry);
        toolbar.append(&granularity_label);

        // ── Section builders ─────────────────────────────────────────────────
        let make_section = |title: &str| -> (gtk4::Box, gtk4::FlowBox) {
            let section = gtk4::Box::builder()
                .orientation(gtk4::Orientation::Vertical)
                .spacing(8)
                .build();

            let heading = gtk4::Label::builder()
                .label(title)
                .css_classes(["heading"])
                .halign(gtk4::Align::Start)
                .margin_start(4)
                .build();

            let flow = gtk4::FlowBox::builder()
                .orientation(gtk4::Orientation::Horizontal)
                .selection_mode(gtk4::SelectionMode::None)
                .homogeneous(true)
                .min_children_per_line(2)
                .max_children_per_line(2)
                .column_spacing(12)
                .row_spacing(12)
                .build();

            section.append(&heading);
            section.append(&flow);
            (section, flow)
        };

        let (system_section, system_flow) = make_section("System Metrics");
        let (app_section, app_flow) = make_section("Application Metrics");
        let (histogram_section, histogram_flow) = make_section("Histogram Metrics");

        // ── Content box inside scroll ────────────────────────────────────────
        let content_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(16)
            .margin_start(12)
            .margin_end(12)
            .margin_top(12)
            .margin_bottom(12)
            .build();
        content_box.append(&system_section);
        content_box.append(&app_section);
        content_box.append(&histogram_section);

        let scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&content_box)
            .build();

        toolbar.set_widget_name("tab-toolbar");
        root.append(&toolbar);
        root.append(&scroll);

        let draw_states: Rc<RefCell<HashMap<String, CardDrawState>>> =
            Rc::new(RefCell::new(HashMap::new()));

        let default_range = AppTimeRange::default();
        let initial_bin_secs = bin_seconds_for_range(&default_range);
        let mut model = MetricsModel {
            data_source: init.data_source.clone(),
            service_filter: None,
            app_time_range: default_range,
            search_text: String::new(),
            window_now_ns: Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)),
            bin_seconds: initial_bin_secs,
            bin_accumulators: HashMap::new(),
            metrics: HashMap::new(),
            order: Vec::new(),
            pending_data_points: HashMap::new(),
            metrics_version: 0,
            data_version: 0,
            stream_task: None,
        };

        start_stream(&mut model, sender.clone());

        ComponentParts {
            model,
            widgets: MetricsWidgets {
                root,
                granularity_label,
                system_section,
                system_flow,
                app_section,
                app_flow,
                histogram_section,
                histogram_flow,
                draw_states,
                card_drawing_areas: HashMap::new(),
                rendered_metrics_version: u64::MAX, // force first build
                rendered_data_version: u64::MAX,    // force first draw_states build
                animation_source: None,
            },
        }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            MetricsInput::DeltaBatch(deltas) => {
                for delta in deltas {
                    self.apply_delta(delta);
                }
            }
            MetricsInput::ServiceFilter(filter) => {
                if filter != self.service_filter {
                    self.service_filter = filter;
                    self.restart_stream(sender);
                }
            }
            MetricsInput::SetTimeRange(range) => {
                // Legacy path — convert and delegate.
                let app_range = AppTimeRange::Paused(match range {
                    TimeRange::Min15 => crate::time_range::PausedRange::Min15,
                    TimeRange::Hour1 => crate::time_range::PausedRange::Hour1,
                    TimeRange::Hour6 => crate::time_range::PausedRange::Hour6,
                    TimeRange::Hour24 => crate::time_range::PausedRange::Hour24,
                });
                if app_range != self.app_time_range {
                    self.app_time_range = app_range;
                    self.window_now_ns =
                        Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
                    self.restart_stream(sender);
                }
            }
            MetricsInput::SetAppTimeRange(range) => {
                if range != self.app_time_range {
                    self.app_time_range = range;
                    self.window_now_ns =
                        Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
                    self.restart_stream(sender);
                }
            }
            MetricsInput::SlidingWindowTick => {
                if self.app_time_range.is_live() {
                    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                    self.window_now_ns = Some(now_ns);
                    // Prune data points that have scrolled off the left edge.
                    let cutoff = now_ns - self.app_time_range.duration_ns();
                    for line in self.metrics.values_mut() {
                        line.data_points.retain(|&(t, _)| t >= cutoff);
                    }
                    self.data_version += 1;
                }
            }
            MetricsInput::SearchChanged(text) => {
                if text != self.search_text {
                    self.search_text = text;
                    self.metrics_version += 1;
                }
            }
            MetricsInput::StreamError(e) => {
                tracing::error!("Metrics stream error: {}", e);
            }
            MetricsInput::AnimationFrame => {
                // No model change — update_view will queue_draw on all cards.
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, sender: ComponentSender<Self>) {
        widgets
            .granularity_label
            .set_text(&compute_granularity(&self.app_time_range));

        // Manage the 15fps animation timer for smooth live chart sliding.
        if self.app_time_range.is_live() {
            if widgets.animation_source.is_none() {
                let s = sender.input_sender().clone();
                let src =
                    glib::timeout_add_local(std::time::Duration::from_millis(100), move || {
                        s.send(MetricsInput::AnimationFrame).ok();
                        glib::ControlFlow::Continue
                    });
                widgets.animation_source = Some(src);
            }
        } else if let Some(src) = widgets.animation_source.take() {
            src.remove();
        }

        // Only rebuild draw_states when data actually changed. Animation frames only need
        // queue_draw() since the draw function already reads Utc::now() for live x bounds.
        if self.data_version != widgets.rendered_data_version {
            let is_live = self.app_time_range.is_live();
            let window_ns = self.app_time_range.duration_ns();
            let frozen_end_ns = self
                .window_now_ns
                .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
            {
                let mut states = widgets.draw_states.borrow_mut();
                states.clear();
                for (id, line) in &self.metrics {
                    let mut points = line.data_points.clone();
                    points.sort_unstable_by_key(|p| p.0);
                    let color = metric_chart_color(&line.name);
                    let fill = line.metric_type == "counter";
                    states.insert(
                        id.clone(),
                        CardDrawState {
                            points,
                            color,
                            fill,
                            unit: line.unit.clone(),
                            window_duration_ns: window_ns,
                            is_live,
                            frozen_end_ns,
                        },
                    );
                }
            }
            widgets.rendered_data_version = self.data_version;
        }

        let search_lower = self.search_text.to_lowercase();

        if self.metrics_version != widgets.rendered_metrics_version {
            // Metric set changed — full FlowBox rebuild
            let mut system_ids: Vec<&str> = Vec::new();
            let mut app_ids: Vec<&str> = Vec::new();
            let mut histogram_ids: Vec<&str> = Vec::new();

            for id in &self.order {
                if let Some(line) = self.metrics.get(id) {
                    // Filter by search text
                    if !search_lower.is_empty() && !line.name.to_lowercase().contains(&search_lower)
                    {
                        continue;
                    }
                    match classify_metric(line) {
                        MetricSection::System => system_ids.push(id),
                        MetricSection::Application => app_ids.push(id),
                        MetricSection::Histogram => histogram_ids.push(id),
                    }
                }
            }

            widgets.card_drawing_areas.clear();
            rebuild_section(
                &widgets.system_flow,
                &system_ids,
                &self.metrics,
                &widgets.draw_states,
                &mut widgets.card_drawing_areas,
            );
            rebuild_section(
                &widgets.app_flow,
                &app_ids,
                &self.metrics,
                &widgets.draw_states,
                &mut widgets.card_drawing_areas,
            );
            rebuild_section(
                &widgets.histogram_flow,
                &histogram_ids,
                &self.metrics,
                &widgets.draw_states,
                &mut widgets.card_drawing_areas,
            );

            widgets.system_section.set_visible(!system_ids.is_empty());
            widgets.app_section.set_visible(!app_ids.is_empty());
            widgets
                .histogram_section
                .set_visible(!histogram_ids.is_empty());

            widgets.rendered_metrics_version = self.metrics_version;
        } else {
            // Only data changed — queue_draw on existing cards (no widget churn)
            for da in widgets.card_drawing_areas.values() {
                da.queue_draw();
            }
        }
    }
}

// ── Card builder ──────────────────────────────────────────────────────────────

fn rebuild_section(
    flow: &gtk4::FlowBox,
    ids: &[&str],
    metrics: &HashMap<String, MetricLine>,
    draw_states: &Rc<RefCell<HashMap<String, CardDrawState>>>,
    card_das: &mut HashMap<String, gtk4::DrawingArea>,
) {
    while let Some(child) = flow.first_child() {
        flow.remove(&child);
    }

    for &id in ids {
        if let Some(line) = metrics.get(id) {
            let (card, da) = build_metric_card(id, line, draw_states);
            flow.insert(&card, -1);
            card_das.insert(id.to_string(), da);
        }
    }
}

fn build_metric_card(
    metric_id: &str,
    line: &MetricLine,
    draw_states: &Rc<RefCell<HashMap<String, CardDrawState>>>,
) -> (gtk4::Widget, gtk4::DrawingArea) {
    let card = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .spacing(0)
        .css_classes(["card"])
        .hexpand(true)
        .build();

    // Header: pill + name + value
    let header = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Horizontal)
        .spacing(6)
        .margin_start(8)
        .margin_end(8)
        .margin_top(8)
        .margin_bottom(4)
        .build();

    let pill_class = metric_pill_css_class(&line.metric_type);
    let pill = gtk4::Label::builder()
        .label(&line.metric_type)
        .css_classes(["caption", pill_class])
        .build();

    let name_label = gtk4::Label::builder()
        .label(&line.name)
        .css_classes(["caption"])
        .halign(gtk4::Align::Start)
        .hexpand(true)
        .ellipsize(gtk4::pango::EllipsizeMode::End)
        .build();

    let value_label = gtk4::Label::builder()
        .label(line.value_display())
        .css_classes(["caption", "dim-label"])
        .halign(gtk4::Align::End)
        .build();

    header.append(&pill);
    header.append(&name_label);
    header.append(&value_label);
    card.append(&header);

    // Chart DrawingArea
    let chart = gtk4::DrawingArea::builder()
        .hexpand(true)
        .height_request(160)
        .margin_start(8)
        .margin_end(8)
        .margin_bottom(8)
        .build();

    let ds = draw_states.clone();
    let id = metric_id.to_string();
    chart.set_draw_func(move |_da, cr, width, height| {
        let states = ds.borrow();
        if let Some(state) = states.get(&id) {
            draw_card_chart(state, cr, width, height);
        } else {
            let (r, g, b) = ChartTheme::current().bg();
            let _ = cr.set_source_rgb(r, g, b);
            let _ = cr.paint();
        }
    });

    card.append(&chart);
    (card.upcast(), chart)
}

// ── Delta application ─────────────────────────────────────────────────────────

impl MetricsModel {
    fn apply_delta(&mut self, delta: ViewDelta) {
        match delta {
            ViewDelta::RowsAppended { table: None, ipc } => {
                if let Ok(batch) = decode_ipc(&ipc) {
                    self.apply_metric_descriptors(&batch);
                }
            }
            ViewDelta::RowsAppended {
                table: Some(ref t),
                ref ipc,
            } if t == "datapoints" => {
                if let Ok(batch) = decode_ipc(ipc) {
                    self.apply_datapoints(&batch);
                }
            }
            ViewDelta::RowsExpired {
                table: None,
                expired_count,
            } => {
                let n = (expired_count as usize).min(self.order.len());
                let expired: Vec<String> = self.order.drain(..n).collect();
                for id in &expired {
                    self.metrics.remove(id);
                }
                if n > 0 {
                    self.metrics_version += 1;
                }
            }
            ViewDelta::Ready => {
                tracing::debug!(
                    "Metrics stream ready: {} metrics, {} with data, {} pending ids buffered",
                    self.metrics.len(),
                    self.metrics
                        .values()
                        .filter(|l| !l.data_points.is_empty())
                        .count(),
                    self.pending_data_points.len(),
                );
            }
            ViewDelta::Error { message } => {
                tracing::error!("Metrics stream error: {}", message);
            }
            _ => {}
        }
    }

    fn apply_metric_descriptors(&mut self, batch: &arrow::record_batch::RecordBatch) {
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

        let id_col = col_str!("metric_id");
        let name_col = col_str!("name");
        let desc_col = col_str!("description");
        let unit_col = col_str!("unit");
        let type_col = col_str!("metric_type");
        let svc_col = col_str!("service_name");

        for i in 0..n {
            let id = id_col.map(|c| c.value(i).to_string()).unwrap_or_default();
            if id.is_empty() {
                continue;
            }

            if !self.metrics.contains_key(&id) {
                self.metrics_version += 1;
                self.data_version += 1;
                let mut line = MetricLine {
                    metric_id: id.clone(),
                    name: name_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                    description: desc_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                    unit: unit_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                    metric_type: type_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                    service_name: svc_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                    latest_value: None,
                    latest_time_ns: 0,
                    point_count: 0,
                    data_points: Vec::new(),
                };
                // Flush any datapoints that arrived before this descriptor.
                if let Some(pending) = self.pending_data_points.remove(&id) {
                    let flushed = pending.len();
                    for (t, v) in pending {
                        line.data_points.push((t, v));
                        if t >= line.latest_time_ns {
                            line.latest_time_ns = t;
                            line.latest_value = Some(v);
                        }
                    }
                    const MAX_POINTS: usize = 300;
                    if line.data_points.len() > MAX_POINTS {
                        let excess = line.data_points.len() - MAX_POINTS;
                        line.data_points.drain(..excess);
                    }
                    line.point_count = line.data_points.len();
                    tracing::debug!("flushed {flushed} pending points for metric {id}");
                }
                self.metrics.insert(id.clone(), line);
                self.order.push(id);
            }
        }
        tracing::debug!(
            "apply_metric_descriptors: {n} rows, {} total metrics",
            self.metrics.len()
        );
    }

    fn apply_datapoints(&mut self, batch: &arrow::record_batch::RecordBatch) {
        let schema = batch.schema();
        let n = batch.num_rows();

        // Detect binned vs raw by column name.
        // Binned (query-side aggregated): bucket, metric_id, val
        // Raw (live WAL broadcast):       series_id, metric_id, time_unix_nano, value
        // Raw live WAL schema has a "series_id" (UInt64) column; server-side
        // binned historical data does not — it's 3 cols: bucket(Int64),
        // metric_id(Utf8View), avg(Float64). The SeQL `as val` / `as bucket`
        // aliases don't survive through DataFusion to the Arrow field names,
        // so we detect by presence of "series_id" instead.
        let is_binned = schema.index_of("series_id").is_err();

        tracing::debug!(
            "apply_datapoints: {n} rows, is_binned={is_binned}, fields={:?}",
            schema
                .fields()
                .iter()
                .map(|f| format!("{}:{}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        );

        let id_col = schema
            .index_of("metric_id")
            .ok()
            .and_then(|i| batch.column(i).as_any().downcast_ref::<StringViewArray>());

        if is_binned {
            // Historical snapshot — already aggregated by server.
            // `ts() bin Xs as bucket` produces Int64 (cast→divide→multiply), not Timestamp.
            // Column names aren't reliable (DataFusion doesn't apply SeQL aliases to field names),
            // so use positional access: col 0 = bucket (Int64), col 2 = avg value (Float64).
            let bucket_col = batch.column(0).as_any().downcast_ref::<Int64Array>();
            let val_col = batch.column(2).as_any().downcast_ref::<Float64Array>();

            tracing::debug!(
                "apply_datapoints binned: id_col={}, bucket_col={}, val_col={}",
                id_col.is_some(),
                bucket_col.is_some(),
                val_col.is_some()
            );

            let mut matched = 0u32;
            let mut unmatched = 0u32;
            for i in 0..n {
                let Some(id) = id_col.map(|c| c.value(i)) else {
                    continue;
                };
                let bucket_ns = bucket_col.map(|c| c.value(i)).unwrap_or(0);
                let value = val_col.map(|c| c.value(i)).unwrap_or(0.0);

                if let Some(line) = self.metrics.get_mut(id) {
                    matched += 1;
                    line.point_count += 1;
                    line.data_points.push((bucket_ns, value));
                    const MAX_POINTS: usize = 300;
                    if line.data_points.len() > MAX_POINTS {
                        let excess = line.data_points.len() - MAX_POINTS;
                        line.data_points.drain(..excess);
                    }
                    if bucket_ns >= line.latest_time_ns {
                        line.latest_time_ns = bucket_ns;
                        line.latest_value = Some(value);
                    }
                } else {
                    unmatched += 1;
                    // Descriptor hasn't arrived yet — buffer until it does.
                    self.pending_data_points
                        .entry(id.to_string())
                        .or_default()
                        .push((bucket_ns, value));
                }
            }
            tracing::debug!(
                "apply_datapoints binned: {matched} matched, {unmatched} unmatched (known metrics: {})",
                self.metrics.len()
            );
            if matched > 0 {
                self.data_version += 1;
            }
        } else {
            // Raw live WAL datapoints — bin client-side using accumulators.
            let val_col = schema
                .index_of("value")
                .ok()
                .and_then(|i| batch.column(i).as_any().downcast_ref::<Float64Array>());
            let time_col = schema.index_of("time_unix_nano").ok().and_then(|i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
            });

            let bin_ns = self.bin_seconds * 1_000_000_000;
            if bin_ns <= 0 {
                return;
            }

            for i in 0..n {
                let Some(id) = id_col.map(|c| c.value(i)) else {
                    continue;
                };
                let time_ns = time_col.map(|c| c.value(i)).unwrap_or(0);
                let value = val_col.map(|c| c.value(i)).unwrap_or(0.0);

                // Compute bucket start (floor to bin boundary)
                let bucket_ns = (time_ns / bin_ns) * bin_ns;

                // Update accumulator
                let (sum, count) = self
                    .bin_accumulators
                    .entry(id.to_string())
                    .or_default()
                    .entry(bucket_ns)
                    .or_insert((0.0, 0));
                *sum += value;
                *count += 1;
                let avg = *sum / *count as f64;

                // Upsert into data_points
                if let Some(line) = self.metrics.get_mut(id) {
                    if let Some(pos) = line.data_points.iter().position(|&(t, _)| t == bucket_ns) {
                        line.data_points[pos] = (bucket_ns, avg);
                    } else {
                        line.data_points.push((bucket_ns, avg));
                        const MAX_POINTS: usize = 300;
                        if line.data_points.len() > MAX_POINTS {
                            let excess = line.data_points.len() - MAX_POINTS;
                            line.data_points.drain(..excess);
                        }
                    }
                    line.point_count += 1;
                    if bucket_ns >= line.latest_time_ns {
                        line.latest_time_ns = bucket_ns;
                        line.latest_value = Some(avg);
                    }
                } else {
                    // Descriptor hasn't arrived yet — buffer until it does.
                    self.pending_data_points
                        .entry(id.to_string())
                        .or_default()
                        .push((bucket_ns, avg));
                }
            }
            self.data_version += 1;
        }
    }
}

// ── Stream launcher ───────────────────────────────────────────────────────────

impl MetricsModel {
    fn restart_stream(&mut self, sender: ComponentSender<MetricsModel>) {
        if let Some(h) = self.stream_task.take() {
            h.abort();
        }
        self.metrics.clear();
        self.order.clear();
        self.bin_accumulators.clear();
        self.pending_data_points.clear();
        self.bin_seconds = bin_seconds_for_range(&self.app_time_range);
        self.metrics_version += 1;
        self.data_version += 1;
        start_stream(self, sender);
    }
}

fn start_stream(model: &mut MetricsModel, sender: ComponentSender<MetricsModel>) {
    let ds = model.data_source.clone();
    let service = model.service_filter.clone();
    let range = model.app_time_range;
    let bin_secs = bin_seconds_for_range(&range);
    model.bin_seconds = bin_secs;
    let is_live = range.is_live();
    let s = sender.input_sender().clone();
    let query = build_query(service.as_deref(), range, bin_secs);

    let handle = relm4::spawn(async move {
        use futures::StreamExt;
        use tokio::time::{interval, Duration, MissedTickBehavior};

        let strategy = TableStrategy::new();
        match ds.live_view(&query, &strategy).await {
            Ok(stream) => {
                futures::pin_mut!(stream);
                let mut pending: Vec<ViewDelta> = Vec::new();
                let mut flush = interval(Duration::from_millis(200));
                flush.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut done = false;

                loop {
                    if done {
                        if !pending.is_empty() {
                            let _ = s.send(MetricsInput::DeltaBatch(std::mem::take(&mut pending)));
                        }
                        break;
                    }
                    tokio::select! {
                        biased;
                        delta = stream.next() => match delta {
                            Some(ViewDelta::Ready) => {
                                // In paused mode, stop after the initial snapshot.
                                pending.push(ViewDelta::Ready);
                                if !is_live {
                                    done = true;
                                }
                            }
                            Some(d) => pending.push(d),
                            None => {
                                if !pending.is_empty() {
                                    let _ = s.send(MetricsInput::DeltaBatch(std::mem::take(&mut pending)));
                                }
                                break;
                            }
                        },
                        _ = flush.tick() => {
                            if !pending.is_empty() {
                                let _ = s.send(MetricsInput::DeltaBatch(std::mem::take(&mut pending)));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let _ = s.send(MetricsInput::StreamError(format!("{:#}", e)));
            }
        }
    });
    model.stream_task = Some(handle);
}

fn build_query(service_filter: Option<&str>, range: AppTimeRange, bin_secs: i64) -> String {
    let mut parts = vec![format!("metrics {}", range.seql_window())];
    if let Some(svc) = service_filter {
        let esc = svc.replace('\'', "''");
        parts.push(format!("where service_name = '{esc}'"));
    }
    parts.push("where metric_type != 'histogram'".to_string());
    let base = parts.join(" | ");
    let bin = bin_size_string(bin_secs);
    format!(
        "{base} <- (datapoints | group by {{ ts() bin {bin} as bucket, metric_id }} {{ avg(value) as val }}) as datapoints"
    )
}

// ── Granularity helpers ───────────────────────────────────────────────────────

const NICE_INTERVALS: &[i64] = &[
    1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200,
];

/// Return the bin size in seconds targeting ~100 data points for the given range.
fn bin_seconds_for_range(range: &AppTimeRange) -> i64 {
    let window_secs: i64 = match range {
        AppTimeRange::Live(lr) => match lr {
            LiveRange::Min1 => 60,
            LiveRange::Min5 => 5 * 60,
            LiveRange::Min15 => 15 * 60,
            LiveRange::Min30 => 30 * 60,
            LiveRange::Hour1 => 3600,
            LiveRange::Hour6 => 6 * 3600,
        },
        AppTimeRange::Paused(pr) => match pr {
            PausedRange::Min15 => 15 * 60,
            PausedRange::Hour1 => 3600,
            PausedRange::Hour6 => 6 * 3600,
            PausedRange::Hour24 => 24 * 3600,
            PausedRange::Day7 => 7 * 24 * 3600,
        },
    };
    let target = (window_secs / 100).max(1);
    NICE_INTERVALS
        .iter()
        .copied()
        .find(|&s| s >= target)
        .unwrap_or(7200)
}

fn bin_size_string(secs: i64) -> String {
    if secs >= 3600 {
        format!("{}h", secs / 3600)
    } else if secs >= 60 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

fn compute_granularity(app_time_range: &AppTimeRange) -> String {
    format!(
        "Interval: {}",
        bin_size_string(bin_seconds_for_range(app_time_range))
    )
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
