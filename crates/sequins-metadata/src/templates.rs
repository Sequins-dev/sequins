//! Built-in dashboard **templates** — pre-built [`Dashboard`]s a user can instantiate
//! (or that are seeded on first run). Templates are plain `Dashboard` values whose
//! panels carry **scope-less** SeQL (the range comes from the dashboard's control bar).
//!
//! The flagship template, [`system_health`], reproduces the app's Health tab as an
//! ordinary dashboard: the per-factor **scores** and weighted **overall** are computed
//! in SeQL via `score()` (thresholds mirror `sequins_types::HealthThresholdConfig`),
//! rendered by the `scoreBar` visualization, alongside key metric cards, trend lines,
//! and an HTTP status-class breakdown.

use crate::types::{Dashboard, DashboardRow, RowPanel, SavedVisualization, VisualizationOptions};

/// The stable id of the seeded System Health dashboard (so re-seeding is idempotent).
pub const SYSTEM_HEALTH_ID: &str = "builtin-system-health";

/// A named, buildable dashboard template.
#[derive(Clone, Copy)]
pub struct DashboardTemplate {
    /// Stable template id.
    pub id: &'static str,
    /// Human-readable title (also the created dashboard's title).
    pub title: &'static str,
    /// One-line description for the template gallery.
    pub description: &'static str,
    builder: fn() -> Dashboard,
}

impl DashboardTemplate {
    /// Build a fresh [`Dashboard`] from this template (unsaved; id set for built-ins).
    pub fn build(&self) -> Dashboard {
        (self.builder)()
    }
}

/// Every built-in template, in gallery order.
pub fn builtin_templates() -> Vec<DashboardTemplate> {
    vec![DashboardTemplate {
        id: SYSTEM_HEALTH_ID,
        title: "System Health",
        description: "Service reliability at a glance — health-factor scores and a \
                      weighted overall, key latency/error/throughput metrics, trends, and \
                      an HTTP status breakdown. Mirrors the Health tab as a dashboard.",
        builder: system_health,
    }]
}

/// Look up a built-in template by id.
pub fn template_by_id(id: &str) -> Option<DashboardTemplate> {
    builtin_templates().into_iter().find(|t| t.id == id)
}

// ── Panel/option helpers ────────────────────────────────────────────────────────

fn panel(
    title: &str,
    shape: &str,
    seql: &str,
    weight: f64,
    options: VisualizationOptions,
) -> RowPanel {
    RowPanel {
        visualization: SavedVisualization {
            seql: seql.to_string(),
            title: title.to_string(),
            shape: Some(shape.to_string()),
            options,
        },
        weight,
    }
}

fn unit(u: &str) -> VisualizationOptions {
    VisualizationOptions {
        unit: Some(u.to_string()),
        ..Default::default()
    }
}

// ── System Health template ──────────────────────────────────────────────────────

/// Build the **System Health** dashboard.
pub fn system_health() -> Dashboard {
    Dashboard {
        id: SYSTEM_HEALTH_ID.to_string(),
        title: "System Health".to_string(),
        created_at_ns: 0,
        updated_at_ns: 0,
        rows: vec![
            // Overall + per-factor score bars.
            DashboardRow {
                height: 220.0,
                panels: vec![panel(
                    "Service Health",
                    "scoreBar",
                    SCORECARD_QUERY,
                    1.0,
                    VisualizationOptions::default(),
                )],
            },
            // Key metric cards.
            DashboardRow {
                height: 130.0,
                panels: vec![
                    panel("Error Rate", "stat", ERROR_RATE_QUERY, 1.0, unit("%")),
                    panel("p95 Latency", "stat", P95_QUERY, 1.0, unit("ms")),
                    panel("Throughput", "stat", THROUGHPUT_QUERY, 1.0, unit("req/s")),
                    panel(
                        "Error Logs",
                        "stat",
                        ERROR_LOG_QUERY,
                        1.0,
                        VisualizationOptions::default(),
                    ),
                ],
            },
            // Trends.
            DashboardRow {
                height: 260.0,
                panels: vec![
                    panel(
                        "Error rate over time",
                        "line",
                        ERROR_RATE_TREND_QUERY,
                        1.0,
                        unit("%"),
                    ),
                    panel(
                        "Latency over time",
                        "line",
                        LATENCY_TREND_QUERY,
                        1.0,
                        unit("ms"),
                    ),
                ],
            },
            // HTTP status breakdown.
            DashboardRow {
                height: 240.0,
                panels: vec![panel(
                    "HTTP status classes",
                    "stackedBar",
                    HTTP_BREAKDOWN_QUERY,
                    1.0,
                    VisualizationOptions::default(),
                )],
            },
        ],
    }
}

// ── Queries (scope-less templates; the dashboard supplies the range) ─────────────
//
// Thresholds/weights mirror `sequins_types::health::HealthThresholdConfig::default()`:
//   span_error_rate 0.01/0.05 w0.40 · http_error_rate 0.05/0.15 w0.25 ·
//   latency_p95 200ms/500ms w0.20 · (error_log_rate 5/20 w0.15 lives in a separate panel).
// Validated by `seql-substrait` compile+consume tests.

/// Per-factor scores + weighted overall (missing HTTP traffic drops out of the weight).
/// The final `select` keeps only the score columns (+ `__value` companions the scoreBar
/// shows as raw values) so intermediate rates don't render as extra bars.
const SCORECARD_QUERY: &str = "spans | group by {} { \
    count() where status == 2 as errs, count() as total, \
    count() where attr.http_status_code >= 500 as http_5xx, \
    count() where attr.http_status_code > 0 as http_total, \
    p95(duration_ns) as p95_ns } \
 | compute float(errs) / float(total) as span_error_rate, \
           case when http_total > 0 then float(http_5xx) / float(http_total) else null end as http_error_rate, \
           p95_ns as latency_p95 \
 | compute score(span_error_rate, 0.01, 0.05) as span_error_score, \
           case when http_error_rate != null then score(http_error_rate, 0.05, 0.15) else null end as http_error_score, \
           score(latency_p95, 200000000, 500000000) as latency_score, \
           span_error_rate * 100.0 as span_error_score__value, \
           http_error_rate * 100.0 as http_error_score__value, \
           p95_ns / 1000000.0 as latency_score__value \
 | compute (span_error_score * 0.40 + coalesce(http_error_score, 0.0) * 0.25 + latency_score * 0.20) \
           / (0.40 + case when http_error_score != null then 0.25 else 0.0 end + 0.20) as overall \
 | select overall, span_error_score, span_error_score__value, http_error_score, \
          http_error_score__value, latency_score, latency_score__value";

const ERROR_RATE_QUERY: &str =
    "spans | group by {} { count() where status == 2 as e, count() as t } \
 | compute float(e) / float(t) * 100.0 as error_rate_pct";

const P95_QUERY: &str =
    "spans | group by {} { p95(duration_ns) as p95_ns } | compute p95_ns / 1000000.0 as p95_ms";

const THROUGHPUT_QUERY: &str = "spans | group by {} { throughput() as rps }";

const ERROR_LOG_QUERY: &str =
    "logs | where severity_number >= 9 | group by {} { count() as error_logs }";

const ERROR_RATE_TREND_QUERY: &str =
    "spans | group by { ts() bin 10% as bucket } { count() where status == 2 as e, count() as t } \
 | compute float(e) / float(t) * 100.0 as error_rate_pct | select bucket, error_rate_pct";

const LATENCY_TREND_QUERY: &str = "spans | group by { ts() bin 10% as bucket } \
    { p50(duration_ns) as p50, p95(duration_ns) as p95, p99(duration_ns) as p99 } \
 | compute p50 / 1000000.0 as p50_ms, p95 / 1000000.0 as p95_ms, p99 / 1000000.0 as p99_ms \
 | select bucket, p50_ms, p95_ms, p99_ms";

const HTTP_BREAKDOWN_QUERY: &str =
    "spans | where attr.http_status_code > 0 | group by { ts() bin 10% as bucket } { \
        count() where attr.http_status_code >= 200 and attr.http_status_code < 300 as c2xx, \
        count() where attr.http_status_code >= 400 and attr.http_status_code < 500 as c4xx, \
        count() where attr.http_status_code >= 500 as c5xx }";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_health_builds_expected_shape() {
        let d = system_health();
        assert_eq!(d.id, SYSTEM_HEALTH_ID);
        assert_eq!(d.title, "System Health");
        assert_eq!(d.rows.len(), 4);
        // The first panel is the score card.
        assert_eq!(
            d.rows[0].panels[0].visualization.shape.as_deref(),
            Some("scoreBar")
        );
        assert!(d.panel_count() >= 7);
        // Panels are scope-less templates (no baked `last`/`between`).
        for row in &d.rows {
            for p in &row.panels {
                let q = &p.visualization.seql;
                assert!(!q.contains(" last "), "panel query has a baked scope: {q}");
                assert!(
                    !q.contains("between("),
                    "panel query has a baked scope: {q}"
                );
            }
        }
    }

    #[test]
    fn template_lookup() {
        assert!(template_by_id(SYSTEM_HEALTH_ID).is_some());
        assert!(template_by_id("nope").is_none());
        assert_eq!(builtin_templates().len(), 1);
    }
}
