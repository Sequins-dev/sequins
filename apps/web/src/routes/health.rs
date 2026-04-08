//! Health tab — live aggregate stats matching macOS HealthViewModel.
//!
//! Runs two live queries concurrently:
//!   1. spans aggregate: error/total, p50/p95/p99 latency, HTTP breakdown (2xx/3xx/4xx/5xx)
//!   2. logs aggregate:  error_logs count (severity >= 9)
//!
//! Computes health analysis using the same 4-factor weighted scoring as the macOS app.

use async_stream::stream;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use datastar::axum::ReadSignals;
use datastar::consts::ElementPatchMode;
use datastar::patch_elements::PatchElements;
use futures::StreamExt;
use serde::Deserialize;
use std::convert::Infallible;

use arrow::array::Array;

use crate::state::AppState;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::frame::ipc_to_batch;
use sequins_query::QueryApi;

#[derive(Deserialize, Default)]
pub struct HealthSignals {
    #[serde(default)]
    pub service: String,
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default = "default_live")]
    pub live: bool,
}

fn default_duration() -> String {
    "5m".to_string()
}
fn default_live() -> bool {
    true
}

// ── Health data extracted from Arrow batches ─────────────────────────────────

#[derive(Default, Clone)]
struct SpanMetrics {
    error_count: i64,
    total: i64,
    p50_ns: f64,
    p95_ns: f64,
    p99_ns: f64,
    http_2xx_plus: i64, // count() where status_code >= 200
    http_3xx_plus: i64, // count() where status_code >= 300
    http_4xx_plus: i64, // count() where status_code >= 400
    http_5xx: i64,      // count() where status_code >= 500
    http_total: i64,    // count() where status_code > 0
}

#[derive(Default, Clone)]
struct LogMetrics {
    error_logs: i64,
}

// ── Health scoring (matching macOS HealthAnalyzer) ───────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HealthStatus {
    Healthy = 0,
    Degraded = 1,
    Unhealthy = 2,
    Inactive = 3,
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
    fn color_class(self) -> &'static str {
        match self {
            Self::Healthy => "text-green-400",
            Self::Degraded => "text-yellow-400",
            Self::Unhealthy => "text-red-400",
            Self::Inactive => "text-zinc-500",
        }
    }
    fn dot_color(self) -> &'static str {
        match self {
            Self::Healthy => "#4ade80",
            Self::Degraded => "#facc15",
            Self::Unhealthy => "#f87171",
            Self::Inactive => "#71717a",
        }
    }
    fn icon_color(self) -> &'static str {
        match self {
            Self::Healthy => "#4ade80",
            Self::Degraded => "#facc15",
            Self::Unhealthy => "#f87171",
            Self::Inactive => "#71717a",
        }
    }
}

struct HealthFactor {
    name: &'static str,
    value: Option<f64>, // None = no data
    formatted: String,
    score: f64, // 0.0–1.0
    status: HealthStatus,
    weight: f64,
}

/// Scoring algorithm from macOS HealthAnalyzer.evaluateFactor.
/// warning/error are the rule thresholds, value is the raw metric.
fn evaluate_factor(value: f64, warning: f64, error_threshold: f64) -> (f64, HealthStatus) {
    if value <= warning {
        let score = 1.0 - 0.3 * (value / warning.max(f64::EPSILON));
        (score, HealthStatus::Healthy)
    } else if value <= error_threshold {
        let range = error_threshold - warning;
        let t = (value - warning) / range.max(f64::EPSILON);
        let score = 0.7 - 0.4 * t;
        (score, HealthStatus::Degraded)
    } else {
        // exponential decay below 0.3
        let excess = (value - error_threshold) / error_threshold.max(f64::EPSILON);
        let score = 0.3 * (-2.0 * excess).exp();
        (score, HealthStatus::Unhealthy)
    }
}

struct HealthAnalysis {
    status: HealthStatus,
    score: f64,
    factors: Vec<HealthFactor>,
}

fn analyze(spans: &SpanMetrics, logs: &LogMetrics, duration_mins: f64) -> HealthAnalysis {
    let has_span_data = spans.total > 0;
    let has_log_data = duration_mins > 0.0;

    // Factor 1: Span error rate
    let span_error_rate = if has_span_data {
        Some(spans.error_count as f64 / spans.total as f64)
    } else {
        None
    };

    // Factor 2: HTTP error rate (5xx / http_total)
    let http_error_rate = if spans.http_total > 0 {
        Some(spans.http_5xx as f64 / spans.http_total as f64)
    } else {
        None
    };

    // Factor 3: Latency P95 in ms
    let latency_p95 = if has_span_data && spans.p95_ns > 0.0 {
        Some(spans.p95_ns / 1_000_000.0)
    } else {
        None
    };

    // Factor 4: Error log rate per minute
    let error_log_rate = if has_log_data {
        Some(logs.error_logs as f64 / duration_mins)
    } else {
        None
    };

    let mut factors = vec![
        build_factor("Span Error Rate", span_error_rate, 0.01, 0.05, 0.40, |v| {
            format!("{:.1}%", v * 100.0)
        }),
        build_factor("HTTP Error Rate", http_error_rate, 0.05, 0.15, 0.25, |v| {
            format!("{:.1}%", v * 100.0)
        }),
        build_factor("Latency P95", latency_p95, 200.0, 500.0, 0.20, |v| {
            if v >= 1000.0 {
                format!("{:.2}s", v / 1000.0)
            } else {
                format!("{:.0}ms", v)
            }
        }),
        build_factor("Error Log Rate", error_log_rate, 5.0, 20.0, 0.15, |v| {
            format!("{:.1}/min", v)
        }),
    ];

    // Overall status = worst of factors WITH data
    let data_statuses: Vec<HealthStatus> = factors
        .iter()
        .filter(|f| f.value.is_some())
        .map(|f| f.status)
        .collect();

    let status = if data_statuses.is_empty() {
        HealthStatus::Inactive
    } else {
        *data_statuses.iter().max().unwrap()
    };

    // Overall score = weighted average of available factors
    let (score_sum, weight_sum) = factors
        .iter()
        .filter(|f| f.value.is_some())
        .fold((0.0, 0.0), |(ss, ws), f| {
            (ss + f.score * f.weight, ws + f.weight)
        });

    let score = if weight_sum > 0.0 {
        score_sum / weight_sum
    } else {
        0.0
    };

    // Sort factors by score ascending so worst shows first
    factors.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    HealthAnalysis {
        status,
        score,
        factors,
    }
}

fn build_factor(
    name: &'static str,
    value: Option<f64>,
    warning: f64,
    error_threshold: f64,
    weight: f64,
    fmt: impl Fn(f64) -> String,
) -> HealthFactor {
    match value {
        Some(v) => {
            let (score, status) = evaluate_factor(v, warning, error_threshold);
            HealthFactor {
                name,
                value: Some(v),
                formatted: fmt(v),
                score,
                status,
                weight,
            }
        }
        None => HealthFactor {
            name,
            value: None,
            formatted: "--".to_string(),
            score: 0.0,
            status: HealthStatus::Inactive,
            weight,
        },
    }
}

// ── Duration parsing ─────────────────────────────────────────────────────────

fn duration_to_minutes(d: &str) -> f64 {
    let d = d.trim();
    if let Some(n) = d.strip_suffix('d') {
        return n.parse::<f64>().unwrap_or(1.0) * 1440.0;
    }
    if let Some(n) = d.strip_suffix('h') {
        return n.parse::<f64>().unwrap_or(1.0) * 60.0;
    }
    if let Some(n) = d.strip_suffix('m') {
        return n.parse::<f64>().unwrap_or(5.0);
    }
    5.0
}

// ── Batch extraction ─────────────────────────────────────────────────────────
//
// NOTE: Substrait roundtrip strips column alias names, so we MUST use
// positional column access (column index), not named access.

fn apply_span_batch(batch: &arrow::record_batch::RecordBatch) -> SpanMetrics {
    if batch.num_rows() == 0 {
        return SpanMetrics::default();
    }
    // Query column order:
    //   0: error_count, 1: total, 2: p50, 3: p95, 4: p99,
    //   5: http_2xx_plus, 6: http_3xx_plus, 7: http_4xx_plus,
    //   8: http_5xx, 9: http_total
    let col_f64 = |idx: usize| -> f64 {
        if idx >= batch.num_columns() {
            return 0.0;
        }
        let col = batch.column(idx);
        col.as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .map(|a| if a.is_valid(0) { a.value(0) } else { 0.0 })
            .or_else(|| {
                col.as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|a| {
                        if a.is_valid(0) {
                            a.value(0) as f64
                        } else {
                            0.0
                        }
                    })
            })
            .or_else(|| {
                col.as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
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
    SpanMetrics {
        error_count: col_f64(0) as i64,
        total: col_f64(1) as i64,
        p50_ns: col_f64(2),
        p95_ns: col_f64(3),
        p99_ns: col_f64(4),
        http_2xx_plus: col_f64(5) as i64,
        http_3xx_plus: col_f64(6) as i64,
        http_4xx_plus: col_f64(7) as i64,
        http_5xx: col_f64(8) as i64,
        http_total: col_f64(9) as i64,
    }
}

fn apply_log_batch(batch: &arrow::record_batch::RecordBatch) -> LogMetrics {
    if batch.num_rows() == 0 {
        return LogMetrics::default();
    }
    // Query column order: 0: error_logs
    let col = batch.column(0);
    let error_logs = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .map(|a| if a.is_valid(0) { a.value(0) } else { 0 })
        .or_else(|| {
            col.as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|a| if a.is_valid(0) { a.value(0) as i64 } else { 0 })
        })
        .unwrap_or(0);
    LogMetrics { error_logs }
}

// ── HTML rendering ────────────────────────────────────────────────────────────

fn render_health_html(
    spans: &SpanMetrics,
    logs: &LogMetrics,
    duration_mins: f64,
    has_span_data: bool,
    has_log_data: bool,
) -> String {
    // Empty state
    if !has_span_data && !has_log_data {
        return r#"<div class="flex flex-col items-center justify-center h-48 gap-2 text-zinc-500">
  <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 0 0 2.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 0 0-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 0 0 .75-.75 2.25 2.25 0 0 0-.1-.664m-5.8 0A2.251 2.251 0 0 1 13.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25ZM6.75 12h.008v.008H6.75V12Zm0 3h.008v.008H6.75V15Zm0 3h.008v.008H6.75V18Z"/></svg>
  <span class="text-sm">No health data available</span>
</div>"#.to_string();
    }

    let analysis = analyze(spans, logs, duration_mins);

    // HTTP breakdown
    let http_html = render_http_card(spans);

    // Summary card
    let summary_html = render_summary_card(&analysis);

    // Health factors
    let factors_html = render_factors(&analysis.factors);

    // Metric cards
    let metrics_html = render_metric_cards(spans, logs, duration_mins);

    format!(
        r#"<div class="p-4 space-y-4">
  <!-- Row 1: Summary + HTTP Breakdown -->
  <div class="grid gap-4" style="grid-template-columns: 1fr 1fr">
    {summary_html}
    {http_html}
  </div>

  <!-- Health Factors -->
  <div>
    <h3 class="text-xs font-semibold uppercase tracking-widest text-zinc-500 mb-2">Health Factors</h3>
    <div class="bg-zinc-900 border border-zinc-700 rounded-lg divide-y divide-zinc-800">
      {factors_html}
    </div>
  </div>

  <!-- Health Metrics -->
  <div>
    <h3 class="text-xs font-semibold uppercase tracking-widest text-zinc-500 mb-2">Health Metrics</h3>
    <div class="grid gap-3" style="grid-template-columns: repeat(auto-fill, minmax(180px, 1fr))">
      {metrics_html}
    </div>
  </div>
</div>"#
    )
}

fn render_summary_card(analysis: &HealthAnalysis) -> String {
    let status = analysis.status;
    let score_pct = (analysis.score * 100.0).round() as i32;
    let color_class = status.color_class();
    let icon_color = status.icon_color();
    let label = status.label();

    // Status message: worst factor detail or generic
    let worst = analysis
        .factors
        .iter()
        .find(|f| f.value.is_some() && f.status != HealthStatus::Healthy);
    let message = match worst {
        Some(f) if status == HealthStatus::Unhealthy => {
            format!(
                "{} is {}: {}",
                f.name,
                f.status.label().to_lowercase(),
                f.formatted
            )
        }
        Some(f) if status == HealthStatus::Degraded => {
            format!("{} is elevated: {}", f.name, f.formatted)
        }
        _ if status == HealthStatus::Healthy => "All systems operating normally".to_string(),
        _ => "Insufficient data to determine health".to_string(),
    };

    format!(
        r#"<div class="bg-zinc-900 border border-zinc-700 rounded-lg p-4 flex flex-col gap-2">
  <div class="flex items-center gap-3">
    <svg width="32" height="32" viewBox="0 0 32 32" fill="{icon_color}">
      <circle cx="16" cy="16" r="14"/>
    </svg>
    <div>
      <div class="text-xs text-zinc-500 font-medium">Overall Health</div>
      <div class="text-sm font-semibold {color_class}">{label}</div>
    </div>
    <div class="ml-auto text-3xl font-bold font-mono {color_class}">{score_pct}%</div>
  </div>
  <div class="text-xs text-zinc-400 mt-1">{message}</div>
</div>"#
    )
}

fn render_http_card(spans: &SpanMetrics) -> String {
    if spans.http_total == 0 {
        return r#"<div class="bg-zinc-900 border border-zinc-700 rounded-lg p-4 flex flex-col gap-2">
  <div class="flex items-center gap-2 text-zinc-400 font-medium text-sm mb-1">
    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z"/></svg>
    HTTP Response Codes
  </div>
  <div class="flex flex-col items-center justify-center flex-1 gap-1 text-zinc-500 py-4">
    <svg class="w-6 h-6 mb-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M12 21a9.004 9.004 0 008.716-6.747M12 21a9.004 9.004 0 01-8.716-6.747M12 21c2.485 0 4.5-4.03 4.5-9S14.485 3 12 3m0 18c-2.485 0-4.5-4.03-4.5-9S9.515 3 12 3m0 0a8.997 8.997 0 017.843 4.582M12 3a8.997 8.997 0 00-7.843 4.582m15.686 0A11.953 11.953 0 0112 10.5c-2.998 0-5.74-1.1-7.843-2.918m15.686 0A8.959 8.959 0 0121 12c0 .778-.099 1.533-.284 2.253m0 0A17.919 17.919 0 0112 16.5a17.92 17.92 0 01-8.716-2.247m0 0A9.015 9.015 0 013 12c0-1.605.42-3.113 1.157-4.418"/></svg>
    <span class="text-xs">No HTTP traffic data</span>
  </div>
</div>"#.to_string();
    }

    let total = spans.http_total as f64;
    let http_2xx = (spans.http_2xx_plus - spans.http_3xx_plus).max(0);
    let http_3xx = (spans.http_3xx_plus - spans.http_4xx_plus).max(0);
    let http_4xx = (spans.http_4xx_plus - spans.http_5xx).max(0);
    let http_5xx = spans.http_5xx;

    let pct_2xx = http_2xx as f64 / total * 100.0;
    let pct_3xx = http_3xx as f64 / total * 100.0;
    let pct_4xx = http_4xx as f64 / total * 100.0;
    let pct_5xx = http_5xx as f64 / total * 100.0;

    // Stacked bar widths (ensure they sum to 100)
    let w2 = pct_2xx;
    let w3 = pct_3xx;
    let w4 = pct_4xx;
    let w5 = pct_5xx;

    format!(
        r#"<div class="bg-zinc-900 border border-zinc-700 rounded-lg p-4 flex flex-col gap-2">
  <div class="flex items-center gap-2 text-zinc-400 font-medium text-sm mb-1">
    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z"/></svg>
    HTTP Response Codes
  </div>
  <div class="space-y-1.5">
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-2 text-xs"><span class="w-2 h-2 rounded-full inline-block" style="background:#4ade80"></span> 2xx Success</div>
      <span class="text-xs font-mono text-zinc-300">{pct_2xx:.1}%</span>
    </div>
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-2 text-xs"><span class="w-2 h-2 rounded-full inline-block" style="background:#60a5fa"></span> 3xx Redirect</div>
      <span class="text-xs font-mono text-zinc-300">{pct_3xx:.1}%</span>
    </div>
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-2 text-xs"><span class="w-2 h-2 rounded-full inline-block" style="background:#facc15"></span> 4xx Client Error</div>
      <span class="text-xs font-mono text-zinc-300">{pct_4xx:.1}%</span>
    </div>
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-2 text-xs"><span class="w-2 h-2 rounded-full inline-block" style="background:#f87171"></span> 5xx Server Error</div>
      <span class="text-xs font-mono text-zinc-300">{pct_5xx:.1}%</span>
    </div>
  </div>
  <div class="flex h-2 rounded overflow-hidden mt-1">
    <div style="width:{w2:.1}%;background:#4ade80"></div>
    <div style="width:{w3:.1}%;background:#60a5fa"></div>
    <div style="width:{w4:.1}%;background:#facc15"></div>
    <div style="width:{w5:.1}%;background:#f87171"></div>
  </div>
</div>"#
    )
}

fn render_factors(factors: &[HealthFactor]) -> String {
    factors
        .iter()
        .map(render_factor_row)
        .collect::<Vec<_>>()
        .join("")
}

fn render_factor_row(f: &HealthFactor) -> String {
    let dot_color = f.status.dot_color();
    let status_label = f.status.label();

    if f.value.is_none() {
        return format!(
            r#"<div class="flex items-center gap-3 px-4 py-3">
  <span class="text-sm text-zinc-500 w-36 shrink-0">{name}</span>
  <div class="flex-1 h-2 rounded border border-dashed border-zinc-700"></div>
  <span class="text-xs font-mono text-zinc-600 w-10 text-right">--</span>
  <span class="w-2.5 h-2.5 rounded-full shrink-0" style="background:#3f3f46"></span>
  <span class="text-xs text-zinc-600 italic w-16">No data</span>
</div>"#,
            name = f.name
        );
    }

    // Progress bar: score maps 0→1 to bar fill left→right
    // Color: green at high scores, yellow mid, red low
    let bar_color = match f.status {
        HealthStatus::Healthy => "#4ade80",
        HealthStatus::Degraded => "#facc15",
        HealthStatus::Unhealthy => "#f87171",
        HealthStatus::Inactive => "#71717a",
    };
    let bar_pct = (f.score * 100.0).round() as i32;
    let score_pct = bar_pct;

    format!(
        r#"<div class="flex items-center gap-3 px-4 py-3">
  <span class="text-sm text-zinc-200 w-36 shrink-0">{name}</span>
  <div class="flex-1 h-2 bg-zinc-800 rounded overflow-hidden">
    <div style="width:{bar_pct}%;background:{bar_color};height:100%;border-radius:inherit"></div>
  </div>
  <span class="text-xs font-mono text-zinc-300 w-10 text-right">{score_pct}%</span>
  <span class="w-2.5 h-2.5 rounded-full shrink-0" style="background:{dot_color}"></span>
  <span class="text-xs text-zinc-400 w-16">{value} <span class="text-zinc-600 text-[10px]">{status}</span></span>
</div>"#,
        name = f.name,
        bar_pct = bar_pct,
        bar_color = bar_color,
        score_pct = score_pct,
        dot_color = dot_color,
        value = f.formatted,
        status = status_label,
    )
}

fn render_metric_cards(spans: &SpanMetrics, logs: &LogMetrics, duration_mins: f64) -> String {
    let error_rate = if spans.total > 0 {
        format!(
            "{:.1}%",
            spans.error_count as f64 / spans.total as f64 * 100.0
        )
    } else {
        "—".to_string()
    };

    let fmt_latency = |ns: f64| -> String {
        if ns <= 0.0 {
            "—".to_string()
        } else {
            let ms = ns / 1_000_000.0;
            if ms >= 1000.0 {
                format!("{:.2}s", ms / 1000.0)
            } else {
                format!("{:.0}ms", ms)
            }
        }
    };

    let throughput = if duration_mins > 0.0 && spans.total > 0 {
        format!("{:.1}/min", spans.total as f64 / duration_mins)
    } else {
        "—".to_string()
    };

    let error_log_rate = if duration_mins > 0.0 {
        format!("{:.1}/min", logs.error_logs as f64 / duration_mins)
    } else {
        "—".to_string()
    };

    let error_color = if spans.total > 0 {
        let er = spans.error_count as f64 / spans.total as f64;
        if er > 0.05 {
            "text-red-400"
        } else if er > 0.01 {
            "text-yellow-400"
        } else {
            "text-green-400"
        }
    } else {
        "text-zinc-500"
    };

    let log_color = if logs.error_logs > 0 {
        "text-yellow-400"
    } else {
        "text-green-400"
    };

    let cards = [
        (
            "Span Error Rate",
            error_rate.as_str(),
            error_color,
            "error spans / total",
        ),
        (
            "Latency P50",
            &fmt_latency(spans.p50_ns),
            "text-green-400",
            "median latency",
        ),
        (
            "Latency P95",
            &fmt_latency(spans.p95_ns),
            "text-green-400",
            "95th percentile",
        ),
        (
            "Latency P99",
            &fmt_latency(spans.p99_ns),
            "text-green-400",
            "99th percentile",
        ),
        (
            "Throughput",
            throughput.as_str(),
            "text-zinc-300",
            "spans / min",
        ),
        (
            "Error Log Rate",
            error_log_rate.as_str(),
            log_color,
            "severity ≥ ERROR",
        ),
    ];

    cards
        .iter()
        .map(|(label, value, color, subtitle)| {
            format!(
                r#"<div class="bg-zinc-900 border border-zinc-700 rounded-lg p-4">
  <div class="text-xs font-semibold uppercase tracking-wide text-zinc-500 mb-2">{label}</div>
  <div class="text-3xl font-bold font-mono {color}">{value}</div>
  <div class="text-xs text-zinc-500 mt-1">{subtitle}</div>
</div>"#
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ── SSE handler ───────────────────────────────────────────────────────────────

pub async fn handler(
    State(state): State<AppState>,
    ReadSignals(params): ReadSignals<HealthSignals>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    // Increment generation — cancels any previous health SSE for this client
    state.health_gen_tx.send_modify(|g| *g += 1);
    let mut gen_rx = state.health_gen_tx.subscribe();

    let backend = state.backend.clone();
    let tmpl = state.templates.clone();
    let duration_mins = duration_to_minutes(&params.duration);

    let s = stream! {
        // Render template shell
        let page_html = match tmpl.render("partials/health.html", minijinja::context! {}) {
            Ok(h) => h,
            Err(e) => { tracing::error!("Health template error: {e}"); return; }
        };
        yield Ok(Event::from(
            PatchElements::new(page_html).selector("#content").mode(ElementPatchMode::Inner),
        ));
        yield Ok(Event::from(
            PatchElements::new("").selector("#tab-controls").mode(ElementPatchMode::Inner),
        ));

        // Build queries
        let svc_filter = if params.service.is_empty() {
            String::new()
        } else {
            format!(" | where resource_id = '{}'", params.service.replace('\'', "''"))
        };

        let span_q = format!(
            "spans last {dur}{svc} | group by {{}} {{ \
              count() where status == 2 as error_count, \
              count() as total, \
              p50(duration_ns) as p50, \
              p95(duration_ns) as p95, \
              p99(duration_ns) as p99, \
              count() where attr.http_status_code >= 200 as http_2xx_plus, \
              count() where attr.http_status_code >= 300 as http_3xx_plus, \
              count() where attr.http_status_code >= 400 as http_4xx_plus, \
              count() where attr.http_status_code >= 500 as http_5xx, \
              count() where attr.http_status_code > 0 as http_total \
            }}",
            dur = params.duration,
            svc = svc_filter,
        );
        let log_q = format!(
            "logs last {dur} | where severity_number >= 9{svc} | group by {{}} {{ count() as error_logs }}",
            dur = params.duration,
            svc = svc_filter,
        );

        // Start live or snapshot streams
        let mut span_stream = match if params.live { backend.query_live(&span_q).await } else { backend.query(&span_q).await } {
            Ok(s) => s,
            Err(e) => { tracing::error!("Health span query error: {e}"); return; }
        };
        let mut log_stream = match if params.live { backend.query_live(&log_q).await } else { backend.query(&log_q).await } {
            Ok(s) => s,
            Err(e) => { tracing::error!("Health log query error: {e}"); return; }
        };

        let mut spans = SpanMetrics::default();
        let mut logs = LogMetrics::default();
        let mut span_ready = false;
        let mut log_ready = false;
        let mut span_done = false;
        let mut log_done = false;

        loop {
            // Check generation — abort if superseded
            if gen_rx.has_changed().unwrap_or(true) { break; }

            // Exit once both streams are done (paused/snapshot mode)
            if span_done && log_done { break; }

            tokio::select! {
                biased;
                _ = gen_rx.changed() => { break; }

                item = span_stream.next(), if !span_done => {
                    let fd = match item {
                        None => { span_done = true; continue; }
                        Some(Ok(fd)) => fd,
                        Some(Err(e)) => { tracing::warn!("Health span stream error: {e}"); span_done = true; continue; }
                    };
                    if fd.data_body.is_empty() {
                        match decode_metadata(&fd.app_metadata) {
                            Some(SeqlMetadata::Heartbeat { .. }) => {
                                span_ready = true;
                                if span_ready && log_ready {
                                    let html = render_health_html(&spans, &logs, duration_mins, spans.total > 0, true);
                                    yield Ok(Event::from(PatchElements::new(html).selector("#health-stats").mode(ElementPatchMode::Inner)));
                                }
                            }
                            Some(SeqlMetadata::Complete { .. }) => { span_done = true; }
                            _ => {}
                        }
                        continue;
                    }
                    let meta = match decode_metadata(&fd.app_metadata) { Some(m) => m, None => continue };
                    match meta {
                        SeqlMetadata::Data { .. } | SeqlMetadata::Append { .. } | SeqlMetadata::Replace { .. } => {
                            if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                                spans = apply_span_batch(&batch);
                                span_ready = true;
                                // Re-render on every Replace (live refresh)
                                if span_ready && log_ready {
                                    let html = render_health_html(&spans, &logs, duration_mins, spans.total > 0, true);
                                    yield Ok(Event::from(PatchElements::new(html).selector("#health-stats").mode(ElementPatchMode::Inner)));
                                }
                            }
                        }
                        SeqlMetadata::Complete { .. } => { span_done = true; }
                        _ => {}
                    }
                }

                item = log_stream.next(), if !log_done => {
                    let fd = match item {
                        None => { log_done = true; continue; }
                        Some(Ok(fd)) => fd,
                        Some(Err(e)) => { tracing::warn!("Health log stream error: {e}"); log_done = true; continue; }
                    };
                    if fd.data_body.is_empty() {
                        match decode_metadata(&fd.app_metadata) {
                            Some(SeqlMetadata::Heartbeat { .. }) => {
                                log_ready = true;
                                if span_ready && log_ready {
                                    let html = render_health_html(&spans, &logs, duration_mins, spans.total > 0, true);
                                    yield Ok(Event::from(PatchElements::new(html).selector("#health-stats").mode(ElementPatchMode::Inner)));
                                }
                            }
                            Some(SeqlMetadata::Complete { .. }) => { log_done = true; }
                            _ => {}
                        }
                        continue;
                    }
                    let meta = match decode_metadata(&fd.app_metadata) { Some(m) => m, None => continue };
                    match meta {
                        SeqlMetadata::Data { .. } | SeqlMetadata::Append { .. } | SeqlMetadata::Replace { .. } => {
                            if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                                logs = apply_log_batch(&batch);
                                log_ready = true;
                                // Re-render on every Replace (live refresh)
                                if span_ready && log_ready {
                                    let html = render_health_html(&spans, &logs, duration_mins, spans.total > 0, true);
                                    yield Ok(Event::from(PatchElements::new(html).selector("#health-stats").mode(ElementPatchMode::Inner)));
                                }
                            }
                        }
                        SeqlMetadata::Complete { .. } => { log_done = true; }
                        _ => {}
                    }
                }
            }
        }
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}
