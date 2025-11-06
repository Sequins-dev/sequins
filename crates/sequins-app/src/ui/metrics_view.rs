use gpui::*;
use sequins_core::traits::QueryApi;

use super::charts::{DataPoint, LineChart, LineChartConfig};

pub struct MetricsView<Q: QueryApi> {
    pub api: Q,
}

impl<Q: QueryApi + Clone + 'static> MetricsView<Q> {
    pub fn new(api: Q) -> Self {
        Self { api }
    }

    pub fn render_inline(&self) -> Div {
        self.render_content()
    }

    fn render_content(&self) -> Div {
        // Mock metrics data for UI development
        let mock_metrics = vec![
            ("http.requests.total", "Counter", "15,234", "+125/min"),
            ("http.request.duration_ms", "Histogram", "p50: 45ms", "p95: 230ms"),
            ("database.connections.active", "Gauge", "42", "max: 100"),
            ("cache.hit_rate", "Gauge", "87.5%", "+2.3%"),
            ("memory.usage_bytes", "Gauge", "1.2 GB", "of 2 GB"),
            ("cpu.usage_percent", "Gauge", "45%", "avg: 38%"),
        ];

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(rgb(0x0f172a))
            .child(
                // Filter bar
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap_3()
                    .px_4()
                    .py_3()
                    .bg(rgb(0x1e293b))
                    .border_b_1()
                    .border_color(rgb(0x334155))
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x94a3b8))
                            .child("⏰ Last 1h"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x94a3b8))
                            .child("Type: All ▼"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x94a3b8))
                            .child("Service: All ▼"),
                    ),
            )
            .child(
                // Metrics grid - scrollable container
                div()
                    .flex_1()
                    .size_full()
                    .id("metrics-scroll")
                    .overflow_scroll()
                    .p_4()
                    .child(
                        // Summary cards
                        div()
                            .grid()
                            .grid_cols(3)
                            .gap_4()
                            .mb_4()
                            .children(mock_metrics.iter().take(3).map(|(name, metric_type, value, detail)| {
                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_2()
                                    .p_4()
                                    .bg(rgb(0x1e293b))
                                    .border_1()
                                    .border_color(rgb(0x334155))
                                    .rounded(px(8.0))
                                    .hover(|style| style.border_color(rgb(0x3b82f6)))
                                    .cursor_pointer()
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .justify_between()
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0x94a3b8))
                                                    .child(*metric_type),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0x10b981))
                                                    .child(*detail),
                                            ),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::MEDIUM)
                                            .text_color(rgb(0xf1f5f9))
                                            .child(*name),
                                    )
                                    .child(
                                        div()
                                            .text_2xl()
                                            .font_weight(FontWeight::BOLD)
                                            .text_color(rgb(0xffffff))
                                            .child(*value),
                                    )
                            })),
                    )
                    .child(
                        // Request Rate Chart
                        div()
                            .flex()
                            .flex_col()
                            .flex_1()
                            .mb_4()
                            .bg(rgb(0x1e293b))
                            .border_1()
                            .border_color(rgb(0x334155))
                            .rounded(px(8.0))
                            .child(
                                div()
                                    .px_4()
                                    .py_3()
                                    .border_b_1()
                                    .border_color(rgb(0x334155))
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::BOLD)
                                            .text_color(rgb(0xf1f5f9))
                                            .child("Request Rate Over Time (req/min)"),
                                    ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .flex_1()
                                    .p_4()
                                    .child(
                                        LineChart::new(
                                            Self::generate_request_rate_data(),
                                            px(700.0),
                                            px(200.0),
                                            LineChartConfig {
                                                color: rgb(0x3b82f6).into(),
                                                line_width: px(2.0),
                                                show_points: false,
                                                show_grid: true,
                                                smooth: false,
                                            },
                                        )
                                        .render_chart(),
                                    ),
                            ),
                    )
                    .child(
                        // Response Time Chart
                        div()
                            .flex()
                            .flex_col()
                            .h(px(280.0))
                            .mb_4()
                            .bg(rgb(0x1e293b))
                            .border_1()
                            .border_color(rgb(0x334155))
                            .rounded(px(8.0))
                            .child(
                                div()
                                    .px_4()
                                    .py_3()
                                    .border_b_1()
                                    .border_color(rgb(0x334155))
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::BOLD)
                                            .text_color(rgb(0xf1f5f9))
                                            .child("Response Time (ms)"),
                                    ),
                            )
                            .child(
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .flex_1()
                                    .p_4()
                                    .child(
                                        LineChart::new(
                                            Self::generate_response_time_data(),
                                            px(700.0),
                                            px(160.0),
                                            LineChartConfig {
                                                color: rgb(0x10b981).into(),
                                                line_width: px(2.0),
                                                show_points: true,
                                                show_grid: true,
                                                smooth: false,
                                            },
                                        )
                                        .render_chart(),
                                    ),
                            ),
                    )
                    .child(
                        // Additional metrics list
                        div()
                            .flex()
                            .flex_col()
                            .gap_2()
                            .child(
                                div()
                                    .text_xs()
                                    .font_weight(FontWeight::BOLD)
                                    .text_color(rgb(0x94a3b8))
                                    .mb_2()
                                    .child("All Metrics"),
                            )
                            .children(mock_metrics.iter().cycle().take(30).map(|(name, metric_type, value, detail)| {
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_between()
                                    .px_4()
                                    .py_3()
                                    .bg(rgb(0x1e293b))
                                    .border_1()
                                    .border_color(rgb(0x334155))
                                    .rounded(px(6.0))
                                    .hover(|style| style.border_color(rgb(0x475569)))
                                    .cursor_pointer()
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap_3()
                                            .child(
                                                div()
                                                    .px_2()
                                                    .py_1()
                                                    .bg(rgb(0x334155))
                                                    .rounded(px(3.0))
                                                    .text_xs()
                                                    .text_color(rgb(0x94a3b8))
                                                    .child(*metric_type),
                                            )
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .text_color(rgb(0xf1f5f9))
                                                    .child(*name),
                                            ),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap_4()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::BOLD)
                                                    .text_color(rgb(0xffffff))
                                                    .child(*value),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0x10b981))
                                                    .child(*detail),
                                            ),
                                    )
                            })),
                    )
            ) // end scrollable container
    }

    /// Generate mock time series data for request rate
    fn generate_request_rate_data() -> Vec<DataPoint> {
        (0..60)
            .map(|i| {
                let x = i as f32;
                // Simulate a wave pattern with some randomness
                let base = 100.0 + 30.0 * (i as f32 / 10.0).sin();
                let noise = ((i * 7) % 20) as f32 - 10.0;
                let y = base + noise;
                DataPoint { x, y }
            })
            .collect()
    }

    /// Generate mock time series data for response time
    fn generate_response_time_data() -> Vec<DataPoint> {
        (0..60)
            .map(|i| {
                let x = i as f32;
                // Simulate response time with occasional spikes
                let base = 45.0;
                let spike = if i % 15 == 0 {
                    50.0 * (1.0 + (i as f32 / 15.0))
                } else {
                    0.0
                };
                let noise = ((i * 11) % 10) as f32;
                let y = base + spike + noise;
                DataPoint { x, y }
            })
            .collect()
    }
}

impl<Q: QueryApi + Clone + 'static> Render for MetricsView<Q> {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.render_content()
    }
}
