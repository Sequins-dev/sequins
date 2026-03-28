//! Health tab — single-service aggregate health view, matching macOS design.
//!
//! Three sections (scrollable):
//!   1. Top row: HealthSummaryCard (status + score) + HTTP breakdown card (no-data)
//!   2. Health Factors: 4 factor rows with colored score bars
//!   3. Health Metrics: 6 metric value cards in a 3×2 grid
//!
//! Two live aggregate queries (spans + logs) via AggregateStrategy. Results are
//! fed into a HealthAnalyzer that ports the macOS HealthTypes.swift scoring logic
//! exactly, producing a HealthAnalysis with per-factor scores and overall status.

use crate::data::AppDataSource;
use crate::time_range::TimeRange;
use arrow::array::{Array, Float64Array, Int64Array, UInt64Array};
use arrow::ipc::reader::StreamReader;
use gtk4::prelude::*;
use relm4::prelude::*;
use sequins_view::{AggregateStrategy, ViewDelta};
use std::collections::HashMap;
use std::sync::Arc;

// ── Health analysis types ─────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Inactive,
}

impl HealthStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Degraded => "Degraded",
            Self::Unhealthy => "Unhealthy",
            Self::Inactive => "Inactive",
        }
    }

    /// CSS color class (reuses existing health-ok/health-degraded/health-error).
    fn css_class(self) -> &'static str {
        match self {
            Self::Healthy => "health-ok",
            Self::Degraded => "health-degraded",
            Self::Unhealthy => "health-error",
            Self::Inactive => "dim-label",
        }
    }

    /// GTK symbolic icon name for this status.
    fn icon_name(self) -> &'static str {
        match self {
            Self::Healthy => "emblem-ok-symbolic",
            Self::Degraded => "dialog-warning-symbolic",
            Self::Unhealthy => "dialog-error-symbolic",
            Self::Inactive => "content-loading-symbolic",
        }
    }

    /// CSS class for ProgressBar fill color.
    fn bar_css_class(self) -> &'static str {
        match self {
            Self::Healthy => "health-bar-ok",
            Self::Degraded => "health-bar-degraded",
            Self::Unhealthy => "health-bar-error",
            Self::Inactive => "health-bar-inactive",
        }
    }

    fn severity(self) -> u8 {
        match self {
            Self::Healthy => 0,
            Self::Degraded => 1,
            Self::Unhealthy => 2,
            Self::Inactive => 3,
        }
    }
}

struct HealthMetricRule {
    metric_name: &'static str,
    display_name: &'static str,
    warning_threshold: f64,
    error_threshold: f64,
    weight: f64,
}

/// Default rules, matching HealthThresholdConfig.defaultRules in HealthTypes.swift.
const DEFAULT_RULES: [HealthMetricRule; 4] = [
    HealthMetricRule {
        metric_name: "span_error_rate",
        display_name: "Span Error Rate",
        warning_threshold: 0.01,
        error_threshold: 0.05,
        weight: 0.40,
    },
    HealthMetricRule {
        metric_name: "http_error_rate",
        display_name: "HTTP Error Rate",
        warning_threshold: 0.05,
        error_threshold: 0.15,
        weight: 0.25,
    },
    HealthMetricRule {
        metric_name: "latency_p95",
        display_name: "Latency (p95)",
        warning_threshold: 200_000_000.0, // 200 ms in ns
        error_threshold: 500_000_000.0,   // 500 ms in ns
        weight: 0.20,
    },
    HealthMetricRule {
        metric_name: "error_log_rate",
        display_name: "Error Log Rate",
        warning_threshold: 5.0,
        error_threshold: 20.0,
        weight: 0.15,
    },
];

#[derive(Clone, Debug)]
struct HealthFactor {
    display_name: &'static str,
    raw_value: Option<f64>,
    formatted_value: String,
    score: f64,
    status: HealthStatus,
    /// Normalized weight (sums to ≤1.0 across factors with data).
    weight: f64,
}

#[derive(Clone, Debug)]
struct HealthAnalysis {
    status: HealthStatus,
    factors: Vec<HealthFactor>,
    overall_score: f64,
    timestamp: String,
}

impl HealthAnalysis {
    fn has_any_data(&self) -> bool {
        self.factors.iter().any(|f| f.raw_value.is_some())
    }

    fn has_complete_data(&self) -> bool {
        self.factors.iter().all(|f| f.raw_value.is_some())
    }

    fn available_count(&self) -> usize {
        self.factors
            .iter()
            .filter(|f| f.raw_value.is_some())
            .count()
    }

    /// Worst factor among those that have data (lowest score).
    fn worst_available_factor(&self) -> Option<&HealthFactor> {
        self.factors
            .iter()
            .filter(|f| f.raw_value.is_some())
            .min_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}

// ── HealthAnalyzer ────────────────────────────────────────────────────────────

/// Port of HealthAnalyzer.analyze() from HealthTypes.swift.
fn analyze(metrics: &HashMap<&'static str, f64>) -> HealthAnalysis {
    let total_weight: f64 = DEFAULT_RULES.iter().map(|r| r.weight).sum();

    let factors: Vec<HealthFactor> = DEFAULT_RULES
        .iter()
        .map(|rule| {
            let normalized_weight = rule.weight / total_weight;
            if let Some(&value) = metrics.get(rule.metric_name) {
                evaluate_metric(value, rule, normalized_weight)
            } else {
                HealthFactor {
                    display_name: rule.display_name,
                    raw_value: None,
                    formatted_value: "\u{2014}".to_string(), // em dash
                    score: 0.0,
                    status: HealthStatus::Inactive,
                    weight: normalized_weight,
                }
            }
        })
        .collect();

    // Overall status = worst status among factors WITH data.
    let overall_status = factors
        .iter()
        .filter(|f| f.raw_value.is_some())
        .map(|f| f.status)
        .max_by_key(|s| s.severity())
        .unwrap_or(HealthStatus::Inactive);

    // Overall score = weighted average of factors with data (re-normalized).
    let available: Vec<&HealthFactor> = factors.iter().filter(|f| f.raw_value.is_some()).collect();
    let available_weight_sum: f64 = available.iter().map(|f| f.weight).sum();
    let overall_score = if available_weight_sum > 0.0 {
        available.iter().map(|f| f.score * f.weight).sum::<f64>() / available_weight_sum
    } else {
        0.0
    };

    HealthAnalysis {
        status: overall_status,
        factors,
        overall_score,
        timestamp: chrono::Utc::now().format("%H:%M:%S").to_string(),
    }
}

/// Port of HealthAnalyzer.evaluateMetric() from HealthTypes.swift (line 559).
fn evaluate_metric(value: f64, rule: &HealthMetricRule, weight: f64) -> HealthFactor {
    let (status, score) = if value <= rule.warning_threshold {
        let score = if rule.warning_threshold > 0.0 {
            1.0 - (value / rule.warning_threshold) * 0.3
        } else {
            1.0
        };
        (HealthStatus::Healthy, score)
    } else if value <= rule.error_threshold {
        let range = rule.error_threshold - rule.warning_threshold;
        let position = if range > 0.0 {
            (value - rule.warning_threshold) / range
        } else {
            1.0
        };
        (HealthStatus::Degraded, 0.7 - position * 0.4)
    } else {
        let excess = value - rule.error_threshold;
        let decay_rate = if rule.error_threshold > 0.0 {
            rule.error_threshold
        } else {
            1.0
        };
        let score = (0.3 * (-excess / decay_rate).exp()).max(0.0);
        (HealthStatus::Unhealthy, score)
    };

    HealthFactor {
        display_name: rule.display_name,
        raw_value: Some(value),
        formatted_value: format_metric_value(value, rule.metric_name),
        score,
        status,
        weight,
    }
}

/// Port of HealthAnalyzer.formatMetricValue() from HealthTypes.swift (line 621).
fn format_metric_value(value: f64, metric_name: &str) -> String {
    if metric_name.contains("latency") {
        let ms = value / 1_000_000.0;
        if ms < 1.0 {
            format!("{ms:.2}ms")
        } else if ms < 1000.0 {
            format!("{ms:.0}ms")
        } else {
            format!("{:.1}s", ms / 1000.0)
        }
    } else if metric_name.contains("rate") && value <= 1.0 {
        format!("{:.1}%", value * 100.0)
    } else if metric_name.contains("log") {
        format!("{value:.1}/min")
    } else if metric_name.contains("throughput") {
        format!("{value:.0}/min")
    } else if value >= 1000.0 {
        format!("{:.1}K", value / 1000.0)
    } else if value >= 1.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.3}")
    }
}

/// Build the status message for the summary card.
/// Ports HealthSummaryCard.statusMessage from HealthSummaryCard.swift (line 95).
fn status_message(analysis: &HealthAnalysis) -> String {
    let partial_suffix = if !analysis.has_complete_data() && analysis.has_any_data() {
        " (partial data)"
    } else {
        ""
    };

    match analysis.status {
        HealthStatus::Healthy => {
            format!("All available health indicators are within normal parameters.{partial_suffix}")
        }
        HealthStatus::Degraded => {
            if let Some(f) = analysis.worst_available_factor() {
                format!(
                    "Warning: {} is elevated at {}.{partial_suffix}",
                    f.display_name, f.formatted_value
                )
            } else {
                format!("Some health indicators are showing warning levels.{partial_suffix}")
            }
        }
        HealthStatus::Unhealthy => {
            if let Some(f) = analysis.worst_available_factor() {
                format!(
                    "Critical: {} is at {}.{partial_suffix}",
                    f.display_name, f.formatted_value
                )
            } else {
                format!("One or more health indicators are in critical range.{partial_suffix}")
            }
        }
        HealthStatus::Inactive => {
            if analysis.factors.iter().all(|f| f.raw_value.is_none()) {
                "No health data available for the selected time range.".to_string()
            } else {
                format!("Waiting for health data...{partial_suffix}")
            }
        }
    }
}

// ── Raw health data ───────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
struct HealthMetrics {
    total: f64,
    errors: f64,
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
    error_logs: f64,
    http_2xx: f64,
    http_3xx: f64,
    http_4xx: f64,
    http_5xx: f64,
    http_total: f64,
}

fn range_minutes(range: TimeRange) -> f64 {
    match range {
        TimeRange::Min15 => 15.0,
        TimeRange::Hour1 => 60.0,
        TimeRange::Hour6 => 360.0,
        TimeRange::Hour24 => 1440.0,
    }
}

// ── Component ──────────────────────────────────────────────────────────────────

pub struct HealthInit {
    pub data_source: Arc<AppDataSource>,
}

pub struct HealthModel {
    data_source: Arc<AppDataSource>,
    metrics: HealthMetrics,
    span_data_ready: bool,
    log_data_ready: bool,
    analysis: Option<HealthAnalysis>,
    service_filter: Option<u32>,
    time_range: TimeRange,
    span_task: Option<tokio::task::JoinHandle<()>>,
    log_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
pub enum HealthInput {
    SpanDeltaBatch(Vec<ViewDelta>),
    LogDeltaBatch(Vec<ViewDelta>),
    ServiceFilter(Option<u32>),
    SetTimeRange(TimeRange),
    StreamError(String),
}

struct FactorRowWidgets {
    score_bar: gtk4::ProgressBar,
    score_pct: gtk4::Label,
    status_dot: gtk4::Label,
    value_label: gtk4::Label,
}

pub struct HealthWidgets {
    root: gtk4::Box,
    // Loading state
    loading_box: gtk4::Box,
    loading_spinner: gtk4::Spinner,
    loading_label: gtk4::Label,
    // Main content (ScrolledWindow wrapping the inner Box)
    main_content: gtk4::ScrolledWindow,
    // Summary card
    status_icon: gtk4::Image,
    status_label: gtk4::Label,
    score_label: gtk4::Label,
    status_message: gtk4::Label,
    data_avail_label: gtk4::Label,
    updated_label: gtk4::Label,
    // Health factors (4 rows, one per DEFAULT_RULE)
    factor_rows: Vec<FactorRowWidgets>,
    // HTTP card
    http_no_data_box: gtk4::Box,
    http_data_box: gtk4::Box,
    http_2xx_bar: gtk4::ProgressBar,
    http_2xx_pct: gtk4::Label,
    http_3xx_bar: gtk4::ProgressBar,
    http_3xx_pct: gtk4::Label,
    http_4xx_bar: gtk4::ProgressBar,
    http_4xx_pct: gtk4::Label,
    http_5xx_bar: gtk4::ProgressBar,
    http_5xx_pct: gtk4::Label,
    // Health metric cards (6 value labels)
    metric_value_labels: Vec<gtk4::Label>,
}

/// Names and keys for the 6 metric value cards.
const METRIC_CARDS: [(&str, &str); 6] = [
    ("Span Error Rate", "span_error_rate"),
    ("Latency P50", "latency_p50"),
    ("Latency P95", "latency_p95"),
    ("Latency P99", "latency_p99"),
    ("Throughput", "throughput"),
    ("Error Log Rate", "error_log_rate"),
];

impl Component for HealthModel {
    type CommandOutput = ();
    type Init = HealthInit;
    type Input = HealthInput;
    type Output = ();
    type Root = gtk4::Box;
    type Widgets = HealthWidgets;

    fn init_root() -> Self::Root {
        gtk4::Box::new(gtk4::Orientation::Vertical, 0)
    }

    fn init(
        init: Self::Init,
        root: Self::Root,
        _sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        // ── Loading placeholder ───────────────────────────────────────────────
        let loading_spinner = gtk4::Spinner::builder().spinning(false).build();
        let loading_label = gtk4::Label::builder()
            .label("Select a service to view health data")
            .css_classes(["caption", "dim-label"])
            .build();
        let loading_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(12)
            .valign(gtk4::Align::Center)
            .halign(gtk4::Align::Center)
            .vexpand(true)
            .build();
        loading_box.append(&loading_spinner);
        loading_box.append(&loading_label);
        root.append(&loading_box);

        // ── Main content (hidden until data arrives) ──────────────────────────
        let scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .visible(false)
            .build();

        let main_content = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(24)
            .margin_start(20)
            .margin_end(20)
            .margin_top(20)
            .margin_bottom(20)
            .build();
        scroll.set_child(Some(&main_content));
        root.append(&scroll);

        // ── Section 1: Top row ────────────────────────────────────────────────
        let top_row = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(16)
            .homogeneous(true)
            .build();

        // Left: Summary card
        let summary_card = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(16)
            .css_classes(["card", "health-card"])
            .build();

        let header_row = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(12)
            .build();

        let status_icon = gtk4::Image::builder()
            .icon_name("content-loading-symbolic")
            .pixel_size(32)
            .css_classes(["dim-label"])
            .build();

        let label_col = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(4)
            .build();
        let overall_lbl = gtk4::Label::builder()
            .label("Overall Health")
            .css_classes(["caption", "dim-label"])
            .halign(gtk4::Align::Start)
            .build();
        let status_label = gtk4::Label::builder()
            .label("Inactive")
            .css_classes(["title-2", "dim-label"])
            .halign(gtk4::Align::Start)
            .build();
        label_col.append(&overall_lbl);
        label_col.append(&status_label);

        let spacer = gtk4::Box::builder().hexpand(true).build();

        let score_col = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(4)
            .halign(gtk4::Align::End)
            .build();
        let score_caption = gtk4::Label::builder()
            .label("Score")
            .css_classes(["caption", "dim-label"])
            .halign(gtk4::Align::End)
            .build();
        let score_label = gtk4::Label::builder()
            .label("\u{2014}")
            .css_classes(["title-1", "dim-label", "health-mono"])
            .halign(gtk4::Align::End)
            .build();
        score_col.append(&score_caption);
        score_col.append(&score_label);

        header_row.append(&status_icon);
        header_row.append(&label_col);
        header_row.append(&spacer);
        header_row.append(&score_col);
        summary_card.append(&header_row);

        let status_message_lbl = gtk4::Label::builder()
            .label("Waiting for health data…")
            .css_classes(["body"])
            .halign(gtk4::Align::Start)
            .wrap(true)
            .build();
        summary_card.append(&status_message_lbl);

        let footer_row = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .build();
        let data_avail_label = gtk4::Label::builder()
            .label("")
            .css_classes(["caption"])
            .halign(gtk4::Align::Start)
            .visible(false)
            .build();
        let footer_spacer = gtk4::Box::builder().hexpand(true).build();
        let updated_label = gtk4::Label::builder()
            .label("")
            .css_classes(["caption", "dim-label"])
            .halign(gtk4::Align::End)
            .build();
        footer_row.append(&data_avail_label);
        footer_row.append(&footer_spacer);
        footer_row.append(&updated_label);
        summary_card.append(&footer_row);

        // Right: HTTP breakdown card (always shows "no data")
        let http_card = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(12)
            .css_classes(["card", "health-card"])
            .build();

        let http_header = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(8)
            .build();
        let http_icon = gtk4::Image::builder()
            .icon_name("network-transmit-receive-symbolic")
            .build();
        let http_title = gtk4::Label::builder()
            .label("HTTP Response Codes")
            .css_classes(["heading"])
            .halign(gtk4::Align::Start)
            .build();
        http_header.append(&http_icon);
        http_header.append(&http_title);
        http_card.append(&http_header);

        // No-data placeholder
        let http_no_data_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(8)
            .valign(gtk4::Align::Center)
            .halign(gtk4::Align::Center)
            .vexpand(true)
            .build();
        let no_data_icon = gtk4::Image::builder()
            .icon_name("network-offline-symbolic")
            .pixel_size(32)
            .css_classes(["dim-label"])
            .build();
        let no_data_lbl = gtk4::Label::builder()
            .label("No HTTP traffic data")
            .css_classes(["caption", "dim-label"])
            .build();
        http_no_data_box.append(&no_data_icon);
        http_no_data_box.append(&no_data_lbl);
        http_card.append(&http_no_data_box);

        // Data view (hidden until http_total > 0)
        let http_data_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(10)
            .visible(false)
            .build();

        // Helper closure to build one HTTP status row
        let make_http_row = |dot_class: &str, label: &str, bar_class: &str| {
            let row = gtk4::Box::builder()
                .orientation(gtk4::Orientation::Horizontal)
                .spacing(8)
                .build();
            let dot = gtk4::Label::builder()
                .label("●")
                .css_classes([dot_class])
                .build();
            let name = gtk4::Label::builder()
                .label(label)
                .halign(gtk4::Align::Start)
                .width_chars(4)
                .build();
            let bar = gtk4::ProgressBar::builder()
                .hexpand(true)
                .css_classes([bar_class])
                .build();
            let pct = gtk4::Label::builder()
                .label("—")
                .css_classes(["health-mono"])
                .width_chars(6)
                .halign(gtk4::Align::End)
                .build();
            row.append(&dot);
            row.append(&name);
            row.append(&bar);
            row.append(&pct);
            (row, bar, pct)
        };

        let (row_2xx, http_2xx_bar, http_2xx_pct) =
            make_http_row("health-dot-ok", "2xx", "health-bar-ok");
        let (row_3xx, http_3xx_bar, http_3xx_pct) =
            make_http_row("health-dot-inactive", "3xx", "health-bar-inactive");
        let (row_4xx, http_4xx_bar, http_4xx_pct) =
            make_http_row("health-dot-degraded", "4xx", "health-bar-degraded");
        let (row_5xx, http_5xx_bar, http_5xx_pct) =
            make_http_row("health-dot-error", "5xx", "health-bar-error");

        http_data_box.append(&row_2xx);
        http_data_box.append(&row_3xx);
        http_data_box.append(&row_4xx);
        http_data_box.append(&row_5xx);
        http_card.append(&http_data_box);

        top_row.append(&summary_card);
        top_row.append(&http_card);
        main_content.append(&top_row);

        // ── Section 2: Health Factors ─────────────────────────────────────────
        let factors_section = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(12)
            .build();

        let factors_heading = gtk4::Label::builder()
            .label("Health Factors")
            .css_classes(["heading"])
            .halign(gtk4::Align::Start)
            .build();
        factors_section.append(&factors_heading);

        let factors_card = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(0)
            .css_classes(["card", "health-card"])
            .build();

        let mut factor_rows: Vec<FactorRowWidgets> = Vec::new();
        for (i, rule) in DEFAULT_RULES.iter().enumerate() {
            let row = gtk4::Box::builder()
                .orientation(gtk4::Orientation::Horizontal)
                .spacing(12)
                .margin_top(10)
                .margin_bottom(10)
                .build();

            let name_lbl = gtk4::Label::builder()
                .label(rule.display_name)
                .halign(gtk4::Align::Start)
                .width_chars(18)
                .build();

            let score_bar = gtk4::ProgressBar::builder()
                .hexpand(true)
                .css_classes(["health-bar-inactive"])
                .build();
            score_bar.set_fraction(0.0);

            let score_pct = gtk4::Label::builder()
                .label("\u{2014}")
                .css_classes(["health-mono", "dim-label"])
                .width_chars(5)
                .halign(gtk4::Align::End)
                .build();

            let status_dot = gtk4::Label::builder()
                .label("\u{25CF}") // BLACK CIRCLE
                .css_classes(["health-dot-inactive"])
                .build();

            let value_label = gtk4::Label::builder()
                .label("\u{2014}")
                .css_classes(["health-mono"])
                .width_chars(10)
                .halign(gtk4::Align::End)
                .build();

            row.append(&name_lbl);
            row.append(&score_bar);
            row.append(&score_pct);
            row.append(&status_dot);
            row.append(&value_label);

            if i > 0 {
                factors_card.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
            }
            factors_card.append(&row);

            factor_rows.push(FactorRowWidgets {
                score_bar,
                score_pct,
                status_dot,
                value_label,
            });
        }

        factors_section.append(&factors_card);
        main_content.append(&factors_section);

        // ── Section 3: Health Metrics (3×2 grid) ─────────────────────────────
        let metrics_section = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .spacing(12)
            .build();

        let metrics_heading = gtk4::Label::builder()
            .label("Health Metrics")
            .css_classes(["heading"])
            .halign(gtk4::Align::Start)
            .build();
        metrics_section.append(&metrics_heading);

        let metrics_grid = gtk4::Grid::builder()
            .column_spacing(16)
            .row_spacing(16)
            .hexpand(true)
            .column_homogeneous(true)
            .build();

        let mut metric_value_labels: Vec<gtk4::Label> = Vec::new();
        for (idx, (name, _key)) in METRIC_CARDS.iter().enumerate() {
            let col = (idx % 3) as i32;
            let row = (idx / 3) as i32;

            let card = gtk4::Box::builder()
                .orientation(gtk4::Orientation::Vertical)
                .spacing(8)
                .hexpand(true)
                .css_classes(["card", "health-card"])
                .build();

            let name_lbl = gtk4::Label::builder()
                .label(*name)
                .css_classes(["caption"])
                .halign(gtk4::Align::Start)
                .build();

            let val_lbl = gtk4::Label::builder()
                .label("\u{2014}")
                .css_classes(["title-2", "health-mono"])
                .halign(gtk4::Align::Start)
                .build();

            card.append(&name_lbl);
            card.append(&val_lbl);
            metrics_grid.attach(&card, col, row, 1, 1);
            metric_value_labels.push(val_lbl);
        }

        metrics_section.append(&metrics_grid);
        main_content.append(&metrics_section);

        let model = HealthModel {
            data_source: init.data_source,
            metrics: HealthMetrics::default(),
            span_data_ready: false,
            log_data_ready: false,
            analysis: None,
            service_filter: None,
            time_range: TimeRange::default(),
            span_task: None,
            log_task: None,
        };

        // Do NOT start streams here — wait for ServiceFilter(Some(_)) first.

        ComponentParts {
            model,
            widgets: HealthWidgets {
                root,
                loading_box,
                loading_spinner,
                loading_label,
                main_content: scroll,
                status_icon,
                status_label,
                score_label,
                status_message: status_message_lbl,
                data_avail_label,
                updated_label,
                http_no_data_box,
                http_data_box,
                http_2xx_bar,
                http_2xx_pct,
                http_3xx_bar,
                http_3xx_pct,
                http_4xx_bar,
                http_4xx_pct,
                http_5xx_bar,
                http_5xx_pct,
                factor_rows,
                metric_value_labels,
            },
        }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            HealthInput::SpanDeltaBatch(deltas) => {
                for delta in deltas {
                    self.apply_span_delta(delta);
                }
            }
            HealthInput::LogDeltaBatch(deltas) => {
                for delta in deltas {
                    self.apply_log_delta(delta);
                }
            }
            HealthInput::ServiceFilter(filter) => {
                if filter != self.service_filter {
                    self.service_filter = filter.clone();
                    if filter.is_some() {
                        self.restart_streams(sender);
                    } else {
                        // No service selected — stop streams, clear data.
                        if let Some(h) = self.span_task.take() {
                            h.abort();
                        }
                        if let Some(h) = self.log_task.take() {
                            h.abort();
                        }
                        self.metrics = HealthMetrics::default();
                        self.span_data_ready = false;
                        self.log_data_ready = false;
                        self.analysis = None;
                    }
                }
            }
            HealthInput::SetTimeRange(range) => {
                if range != self.time_range {
                    self.time_range = range;
                    self.restart_streams(sender);
                }
            }
            HealthInput::StreamError(e) => {
                tracing::error!("Health stream error: {}", e);
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, _sender: ComponentSender<Self>) {
        let Some(analysis) = &self.analysis else {
            widgets.loading_box.set_visible(true);
            widgets.main_content.set_visible(false);
            if self.service_filter.is_none() {
                widgets.loading_spinner.set_spinning(false);
                widgets
                    .loading_label
                    .set_label("Select a service to view health data");
            } else {
                widgets.loading_spinner.set_spinning(true);
                widgets.loading_label.set_label("Loading health data…");
            }
            return;
        };

        widgets.loading_box.set_visible(false);
        widgets.main_content.set_visible(true);

        // ── Summary card ──────────────────────────────────────────────────────
        let status = analysis.status;

        widgets.status_icon.set_icon_name(Some(status.icon_name()));
        widgets.status_icon.set_css_classes(&[status.css_class()]);

        widgets.status_label.set_label(status.label());
        widgets
            .status_label
            .set_css_classes(&["title-2", status.css_class()]);

        let score_pct = format!("{}%", (analysis.overall_score * 100.0) as i32);
        widgets.score_label.set_label(&score_pct);
        widgets
            .score_label
            .set_css_classes(&["title-1", "health-mono", status.css_class()]);

        widgets.status_message.set_label(&status_message(analysis));

        // Data availability indicator
        if !analysis.has_complete_data() && analysis.has_any_data() {
            let text = format!(
                "{}/{} metrics",
                analysis.available_count(),
                analysis.factors.len()
            );
            widgets.data_avail_label.set_label(&text);
            widgets.data_avail_label.set_visible(true);
        } else {
            widgets.data_avail_label.set_visible(false);
        }

        widgets
            .updated_label
            .set_label(&format!("Last updated: {}", analysis.timestamp));

        // ── Health factor rows ────────────────────────────────────────────────
        for (factor, row_widgets) in analysis.factors.iter().zip(widgets.factor_rows.iter()) {
            let fstatus = factor.status;

            row_widgets.score_bar.set_fraction(factor.score);
            row_widgets
                .score_bar
                .set_css_classes(&[fstatus.bar_css_class()]);

            if factor.raw_value.is_some() {
                row_widgets
                    .score_pct
                    .set_label(&format!("{:.0}%", factor.score * 100.0));
                row_widgets
                    .score_pct
                    .set_css_classes(&["health-mono", fstatus.css_class()]);
            } else {
                row_widgets.score_pct.set_label("\u{2014}");
                row_widgets
                    .score_pct
                    .set_css_classes(&["health-mono", "dim-label"]);
            }

            let dot_class = match fstatus {
                HealthStatus::Healthy => "health-dot-ok",
                HealthStatus::Degraded => "health-dot-degraded",
                HealthStatus::Unhealthy => "health-dot-error",
                HealthStatus::Inactive => "health-dot-inactive",
            };
            row_widgets.status_dot.set_css_classes(&[dot_class]);

            row_widgets.value_label.set_label(&factor.formatted_value);
            if factor.raw_value.is_some() {
                row_widgets
                    .value_label
                    .set_css_classes(&["health-mono", fstatus.css_class()]);
            } else {
                row_widgets
                    .value_label
                    .set_css_classes(&["health-mono", "dim-label"]);
            }
        }

        // ── HTTP Response Codes card ──────────────────────────────────────────
        let m = &self.metrics;
        if self.span_data_ready && m.http_total > 0.0 {
            widgets.http_no_data_box.set_visible(false);
            widgets.http_data_box.set_visible(true);

            let t = m.http_total;
            let set_row = |bar: &gtk4::ProgressBar, pct: &gtk4::Label, count: f64| {
                let rate = (count / t).clamp(0.0, 1.0);
                bar.set_fraction(rate);
                pct.set_label(&format!("{:.1}%", rate * 100.0));
            };
            set_row(&widgets.http_2xx_bar, &widgets.http_2xx_pct, m.http_2xx);
            set_row(&widgets.http_3xx_bar, &widgets.http_3xx_pct, m.http_3xx);
            set_row(&widgets.http_4xx_bar, &widgets.http_4xx_pct, m.http_4xx);
            set_row(&widgets.http_5xx_bar, &widgets.http_5xx_pct, m.http_5xx);
        } else {
            widgets.http_no_data_box.set_visible(true);
            widgets.http_data_box.set_visible(false);
        }

        // ── Metric value cards ────────────────────────────────────────────────
        let mins = range_minutes(self.time_range);

        let metric_display: [Option<String>; 6] = [
            // Span Error Rate
            if self.span_data_ready && m.total > 0.0 {
                Some(format!("{:.2}%", (m.errors / m.total) * 100.0))
            } else {
                None
            },
            // P50 Latency
            if self.span_data_ready && m.total > 0.0 {
                Some(format_metric_value(m.p50_ns, "latency"))
            } else {
                None
            },
            // P95 Latency
            if self.span_data_ready && m.total > 0.0 {
                Some(format_metric_value(m.p95_ns, "latency"))
            } else {
                None
            },
            // P99 Latency
            if self.span_data_ready && m.total > 0.0 {
                Some(format_metric_value(m.p99_ns, "latency"))
            } else {
                None
            },
            // Throughput
            if self.span_data_ready && m.total > 0.0 {
                Some(format_metric_value(m.total / mins, "throughput"))
            } else {
                None
            },
            // Error Log Rate
            if self.log_data_ready {
                Some(format_metric_value(m.error_logs / mins, "error_log_rate"))
            } else {
                None
            },
        ];

        for (lbl, val) in widgets
            .metric_value_labels
            .iter()
            .zip(metric_display.iter())
        {
            lbl.set_label(val.as_deref().unwrap_or("\u{2014}"));
        }
    }
}

// ── Delta application ─────────────────────────────────────────────────────────

impl HealthModel {
    fn apply_span_delta(&mut self, delta: ViewDelta) {
        match delta {
            ViewDelta::TableReplaced { table: None, ipc } => {
                if let Ok(batch) = decode_ipc(&ipc) {
                    self.metrics = extract_span_metrics(&batch, &self.metrics);
                    self.span_data_ready = true;
                    self.recompute_analysis();
                }
            }
            ViewDelta::Ready => {
                tracing::debug!("Health span stream ready");
            }
            ViewDelta::Heartbeat { .. } => {}
            ViewDelta::Error { message } => {
                tracing::error!("Health span stream error: {}", message);
            }
            _ => {}
        }
    }

    fn apply_log_delta(&mut self, delta: ViewDelta) {
        match delta {
            ViewDelta::TableReplaced { table: None, ipc } => {
                if let Ok(batch) = decode_ipc(&ipc) {
                    if let Some(logs) = extract_error_logs(&batch) {
                        self.metrics.error_logs = logs;
                        self.log_data_ready = true;
                        self.recompute_analysis();
                    }
                }
            }
            ViewDelta::Ready => {
                tracing::debug!("Health log stream ready");
            }
            ViewDelta::Heartbeat { .. } => {}
            ViewDelta::Error { message } => {
                tracing::error!("Health log stream error: {}", message);
            }
            _ => {}
        }
    }

    fn recompute_analysis(&mut self) {
        if !self.span_data_ready {
            return;
        }
        let mins = range_minutes(self.time_range);
        let m = &self.metrics;
        let mut metrics: HashMap<&'static str, f64> = HashMap::new();

        if m.total > 0.0 {
            metrics.insert("span_error_rate", m.errors / m.total);
            metrics.insert("latency_p95", m.p95_ns);
        }
        if m.http_total > 0.0 {
            metrics.insert("http_error_rate", m.http_5xx / m.http_total);
        }
        if self.log_data_ready {
            metrics.insert("error_log_rate", m.error_logs / mins);
        }

        self.analysis = Some(analyze(&metrics));
    }

    fn restart_streams(&mut self, sender: ComponentSender<Self>) {
        if let Some(h) = self.span_task.take() {
            h.abort();
        }
        if let Some(h) = self.log_task.take() {
            h.abort();
        }
        self.metrics = HealthMetrics::default();
        self.span_data_ready = false;
        self.log_data_ready = false;
        self.analysis = None;
        start_streams(self, sender);
    }
}

// ── Stream launchers ───────────────────────────────────────────────────────────

fn start_streams(model: &mut HealthModel, sender: ComponentSender<HealthModel>) {
    let ds = model.data_source.clone();
    let service = model.service_filter.clone();
    let range = model.time_range;
    let span_query = build_span_query(service, range);
    let log_query = build_log_query(service, range);

    {
        let ds = ds.clone();
        let s = sender.input_sender().clone();
        model.span_task = Some(relm4::spawn(async move {
            run_stream(ds, span_query, s, HealthInput::SpanDeltaBatch).await;
        }));
    }

    {
        let s = sender.input_sender().clone();
        model.log_task = Some(relm4::spawn(async move {
            run_stream(ds, log_query, s, HealthInput::LogDeltaBatch).await;
        }));
    }
}

async fn run_stream<F>(
    ds: Arc<AppDataSource>,
    query: String,
    sender: relm4::Sender<HealthInput>,
    make_msg: F,
) where
    F: Fn(Vec<ViewDelta>) -> HealthInput,
{
    use futures::StreamExt;
    use tokio::time::{interval, Duration, MissedTickBehavior};

    let strategy = AggregateStrategy::new();
    match ds.live_view(&query, &strategy).await {
        Ok(stream) => {
            futures::pin_mut!(stream);
            let mut pending: Vec<ViewDelta> = Vec::new();
            let mut flush = interval(Duration::from_millis(500));
            flush.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    delta = stream.next() => match delta {
                        Some(d) => pending.push(d),
                        None => {
                            if !pending.is_empty() {
                                let _ = sender.send(make_msg(std::mem::take(&mut pending)));
                            }
                            break;
                        }
                    },
                    _ = flush.tick() => {
                        if !pending.is_empty() {
                            let _ = sender.send(make_msg(std::mem::take(&mut pending)));
                        }
                    }
                }
            }
        }
        Err(e) => {
            let _ = sender.send(HealthInput::StreamError(format!("{:#}", e)));
        }
    }
}

// ── Query builders ────────────────────────────────────────────────────────────

fn build_span_query(resource_id: Option<u32>, range: TimeRange) -> String {
    let mut parts = vec![format!("spans {}", range.seql_window())];
    if let Some(rid) = resource_id {
        parts.push(format!("where resource_id = {rid}"));
    }
    parts.push(
        "group by {} { \
         count() where status == 2 as errors, \
         count() as total, \
         p50(duration_ns) as p50, \
         p95(duration_ns) as p95, \
         p99(duration_ns) as p99, \
         count() where attr.http_status_code >= 200 as http_2xx_plus, \
         count() where attr.http_status_code >= 300 as http_3xx_plus, \
         count() where attr.http_status_code >= 400 as http_4xx_plus, \
         count() where attr.http_status_code >= 500 as http_5xx, \
         count() where attr.http_status_code > 0 as http_total \
         }"
        .to_string(),
    );
    parts.join(" | ")
}

fn build_log_query(resource_id: Option<u32>, range: TimeRange) -> String {
    let mut parts = vec![format!("logs {}", range.seql_window())];
    parts.push("where severity_number >= 9".to_string());
    if let Some(rid) = resource_id {
        parts.push(format!("where resource_id = {rid}"));
    }
    parts.push("group by {} { count() as error_logs }".to_string());
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

/// Read a column as f64 regardless of whether it is Float64, Int64, or UInt64.
fn col_as_f64(batch: &arrow::record_batch::RecordBatch, name: &str) -> f64 {
    let schema = batch.schema();
    let Some(idx) = schema.index_of(name).ok() else {
        return 0.0;
    };
    let col = batch.column(idx);
    let any = col.as_any();
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        if a.is_valid(0) {
            a.value(0)
        } else {
            0.0
        }
    } else if let Some(a) = any.downcast_ref::<Int64Array>() {
        if a.is_valid(0) {
            a.value(0) as f64
        } else {
            0.0
        }
    } else if let Some(a) = any.downcast_ref::<UInt64Array>() {
        if a.is_valid(0) {
            a.value(0) as f64
        } else {
            0.0
        }
    } else {
        0.0
    }
}

/// Extract span metrics from an aggregate batch by column position.
///
/// Column order matches the query:
///   count() where status == 2 as errors  → col 0
///   count() as total                     → col 1
///   p50(duration_ns) as p50              → col 2
///   p95(duration_ns) as p95              → col 3
///   p99(duration_ns) as p99              → col 4
///
/// Substrait roundtrip strips alias names so we must use positional access.
fn extract_span_metrics(
    batch: &arrow::record_batch::RecordBatch,
    current: &HealthMetrics,
) -> HealthMetrics {
    let col_f64 = |idx: usize| -> f64 {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| if a.is_valid(0) { a.value(0) } else { 0.0 })
            .or_else(|| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| {
                        if a.is_valid(0) {
                            a.value(0) as f64
                        } else {
                            0.0
                        }
                    })
            })
            .or_else(|| {
                batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .map(|a| {
                        if a.is_valid(0) {
                            a.value(0) as f64
                        } else {
                            0.0
                        }
                    })
            })
            .unwrap_or(0.0)
    };

    if batch.num_columns() < 5 {
        tracing::warn!(
            "Health span batch has {} columns, expected 7",
            batch.num_columns()
        );
        return current.clone();
    }

    HealthMetrics {
        errors: col_f64(0),
        total: col_f64(1),
        p50_ns: col_f64(2),
        p95_ns: col_f64(3),
        p99_ns: col_f64(4),
        // Columns 5-9: cumulative ≥200, ≥300, ≥400, ≥500, and total with http.status_code.
        // Derive exact bucket counts by subtraction.
        http_2xx: if batch.num_columns() > 6 {
            (col_f64(5) - col_f64(6)).max(0.0)
        } else {
            0.0
        },
        http_3xx: if batch.num_columns() > 7 {
            (col_f64(6) - col_f64(7)).max(0.0)
        } else {
            0.0
        },
        http_4xx: if batch.num_columns() > 8 {
            (col_f64(7) - col_f64(8)).max(0.0)
        } else {
            0.0
        },
        http_5xx: if batch.num_columns() > 8 {
            col_f64(8)
        } else {
            0.0
        },
        http_total: if batch.num_columns() > 9 {
            col_f64(9)
        } else {
            0.0
        },
        error_logs: current.error_logs,
    }
}

fn extract_error_logs(batch: &arrow::record_batch::RecordBatch) -> Option<f64> {
    // col 0 = count() as error_logs
    if batch.num_columns() == 0 || batch.num_rows() == 0 {
        return None;
    }
    let col = batch.column(0);
    let val = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| {
            if a.is_valid(0) {
                a.value(0) as f64
            } else {
                0.0
            }
        })
        .or_else(|| {
            col.as_any().downcast_ref::<UInt64Array>().map(|a| {
                if a.is_valid(0) {
                    a.value(0) as f64
                } else {
                    0.0
                }
            })
        })
        .or_else(|| {
            col.as_any().downcast_ref::<Float64Array>().map(|a| {
                if a.is_valid(0) {
                    a.value(0)
                } else {
                    0.0
                }
            })
        })?;
    Some(val)
}
