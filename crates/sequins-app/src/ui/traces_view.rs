use gpui::*;
use sequins_core::{
    models::{QueryTrace, Span, TraceId},
    traits::QueryApi,
};

pub struct TracesView<Q: QueryApi> {
    pub api: Q,
    pub traces: Vec<QueryTrace>,
    pub selected_trace: Option<TraceId>,
    pub spans: Vec<Span>,
}

impl<Q: QueryApi + Clone + 'static> TracesView<Q> {
    pub fn new(api: Q) -> Self {
        Self {
            api,
            traces: Vec::new(),
            selected_trace: None,
            spans: Vec::new(),
        }
    }

    pub fn render_inline(&self) -> Div {
        self.render_content()
    }

    // TODO: Make this properly async with GPUI's async patterns
    // For now, loading happens synchronously on creation
    pub fn _load_traces(&mut self, _cx: &mut Context<Self>) {
        // Placeholder - will implement proper async loading later
    }

    fn _select_trace(&mut self, _trace_id: TraceId, _cx: &mut Context<Self>) {
        // TODO: Implement trace selection with async span loading
    }

    fn status_color(has_error: bool) -> Hsla {
        if has_error {
            rgb(0xef4444).into() // red
        } else {
            rgb(0x10b981).into() // green
        }
    }

    fn status_icon(has_error: bool) -> &'static str {
        if has_error {
            "✗"
        } else {
            "✓"
        }
    }

    fn service_color(service: &str) -> Hsla {
        // Simple hash-based color generation
        let hash = service.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
        let hue = (hash % 360) as f32;
        hsla(hue / 360.0, 0.7, 0.6, 1.0)
    }

    /// Calculate span depth in the hierarchy
    fn calculate_depth(&self, span: &Span) -> usize {
        if span.parent_span_id.is_none() {
            return 0;
        }

        let mut depth = 1;
        let mut current_parent = span.parent_span_id;

        while let Some(parent_id) = current_parent {
            if let Some(parent) = self.spans.iter().find(|s| s.span_id == parent_id) {
                depth += 1;
                current_parent = parent.parent_span_id;
            } else {
                break;
            }
        }

        depth
    }

    /// Calculate span offset from trace start in milliseconds
    fn calculate_offset_ms(&self, span: &Span) -> i64 {
        if let Some(trace_start) = self.spans.iter().map(|s| s.start_time).min() {
            span.start_time.duration_since(trace_start).as_millis()
        } else {
            0
        }
    }

    fn render_timeline(&self) -> impl IntoElement {
        if self.spans.is_empty() {
            return div()
                .flex()
                .items_center()
                .justify_center()
                .h(px(150.0))
                .bg(rgb(0x1e293b))
                .border_b_1()
                .border_color(rgb(0x334155))
                .child(
                    div()
                        .text_sm()
                        .text_color(rgb(0x64748b))
                        .child("Select a trace to view timeline"),
                );
        }

        // Find the maximum timestamp to scale bars
        let max_end = self
            .spans
            .iter()
            .map(|s| {
                let offset = self.calculate_offset_ms(s);
                let duration = s.duration.as_millis();
                offset + duration
            })
            .max()
            .unwrap_or(1);

        div()
            .flex()
            .flex_col()
            .gap_1()
            .p_4()
            .bg(rgb(0x1e293b))
            .border_b_1()
            .border_color(rgb(0x334155))
            .min_h(px(150.0))
            .max_h(px(300.0))
            .child(
                div()
                    .text_xs()
                    .font_weight(FontWeight::BOLD)
                    .text_color(rgb(0x94a3b8))
                    .mb_2()
                    .child("Trace Timeline"),
            )
            .children(self.spans.iter().map(|span| {
                let depth = self.calculate_depth(span);
                let offset_ms = self.calculate_offset_ms(span);
                let duration_ms = span.duration.as_millis();

                let indent = depth * 20;
                let start_percent = (offset_ms as f32 / max_end as f32) * 100.0;
                let width_percent = (duration_ms as f32 / max_end as f32) * 100.0;

                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .mb_1()
                    .child(
                        // Service name with indentation
                        div()
                            .w(px(120.0))
                            .flex_shrink_0()
                            .pl(px(indent as f32))
                            .text_xs()
                            .text_color(rgb(0xf1f5f9))
                            .child(span.service_name.clone()),
                    )
                    .child(
                        // Timeline bar container
                        div()
                            .flex_1()
                            .h(px(20.0))
                            .relative()
                            .child(
                                // The actual span bar
                                {
                                    // Calculate bar dimensions in pixels based on container width
                                    // For now, use relative positioning - we'll improve this later
                                    div()
                                        .absolute()
                                        .left(relative(start_percent / 100.0))
                                        .w(relative((width_percent / 100.0).max(0.02))) // Min 2% width
                                        .h_full()
                                        .bg(Self::service_color(&span.service_name))
                                        .rounded(px(2.0))
                                }
                                    .child(
                                        div()
                                            .absolute()
                                            .left(px(4.0))
                                            .top(px(2.0))
                                            .text_xs()
                                            .text_color(rgb(0xffffff))
                                            .child(span.operation_name.clone()),
                                    ),
                            ),
                    )
                    .child(
                        // Duration
                        div()
                            .w(px(60.0))
                            .flex_shrink_0()
                            .text_xs()
                            .text_color(rgb(0x64748b))
                            .text_align(TextAlign::Right)
                            .child(format!("{}ms", duration_ms)),
                    )
            }))
    }

    fn render_split_pane(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .flex_1()
            .min_h(px(0.0))
            .child(
                // Left: Trace list
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h(px(0.0))
                    .w(relative(0.4))
                    .border_r_1()
                    .border_color(rgb(0x334155))
                    .child(
                        div()
                            .px_4()
                            .py_2()
                            .bg(rgb(0x1e293b))
                            .border_b_1()
                            .border_color(rgb(0x334155))
                            .text_xs()
                            .font_weight(FontWeight::BOLD)
                            .text_color(rgb(0x94a3b8))
                            .child("Trace List"),
                    )
                    .child(
                        div()
                            .flex_1()
                            .size_full()
                            .id("trace-list-scroll")
                            .overflow_scroll()
                            .children(self.traces.iter().map(|trace| {
                        let is_selected = self
                            .selected_trace
                            .as_ref()
                            .map_or(false, |t| t == &trace.trace_id);
                        let trace_id = trace.trace_id;

                        // Get root span name (find span with root_span_id)
                        let root_span_name = trace
                            .spans
                            .iter()
                            .find(|s| s.span_id == trace.root_span_id)
                            .map(|s| s.operation_name.clone())
                            .unwrap_or_else(|| "Unknown".to_string());

                        let _trace_id_copy = trace_id; // TODO: wire up click handler
                        let mut div_elem = div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .px_4()
                            .py_3()
                            .cursor_pointer()
                            .hover(|style| style.bg(rgb(0x2d3b4f)))
                            .border_b_1()
                            .border_color(rgb(0x1e293b));

                        if is_selected {
                            div_elem = div_elem.bg(rgb(0x334155));
                        }

                        div_elem
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::BOLD)
                                    .text_color(Self::status_color(trace.has_error))
                                    .child(Self::status_icon(trace.has_error)),
                            )
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .flex_1()
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(0xf1f5f9))
                                            .child(root_span_name),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(rgb(0x64748b))
                                            .child(format!("{}ms", trace.duration)),
                                    ),
                            )
                    }))
                    ) // end scrollable container
            )
            .child(
                // Right: Span details
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h(px(0.0))
                    .child(
                        div()
                            .px_4()
                            .py_2()
                            .bg(rgb(0x1e293b))
                            .border_b_1()
                            .border_color(rgb(0x334155))
                            .text_xs()
                            .font_weight(FontWeight::BOLD)
                            .text_color(rgb(0x94a3b8))
                            .child("Span Details"),
                    )
                    .child(
                        div()
                            .flex_1()
                            .size_full()
                            .id("span-details-scroll")
                            .overflow_scroll()
                            .child(if self.selected_trace.is_some() && !self.spans.is_empty() {
                                div().child(self.render_span_details())
                            } else {
                                div()
                                    .flex()
                                    .items_center()
                                    .justify_center()
                                    .h_full()
                                    .text_sm()
                                    .text_color(rgb(0x64748b))
                                    .child("Select a trace to view span details")
                            })
                    ) // end scrollable container
            )
    }

    fn render_span_details(&self) -> impl IntoElement {
        // Show details for the first span (in a real app, this would be the selected span)
        let span = &self.spans[0];

        div()
            .flex()
            .flex_col()
            .gap_3()
            .p_4()
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(0x64748b))
                            .child("Span Name"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0xf1f5f9))
                            .child(span.operation_name.clone()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(0x64748b))
                            .child("Service"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0xf1f5f9))
                            .child(span.service_name.clone()),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .child(
                        div()
                            .text_xs()
                            .text_color(rgb(0x64748b))
                            .child("Duration"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0xf1f5f9))
                            .child(format!("{}ms", span.duration.as_millis())),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::BOLD)
                            .text_color(rgb(0x94a3b8))
                            .child("Attributes"),
                    )
                    .children(span.attributes.iter().map(|(key, value)| {
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(0x64748b))
                                    .child(format!("{}:", key)),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(0xf1f5f9))
                                    .child(format!("{:?}", value)),
                            )
                    })),
            )
    }

    fn render_content(&self) -> Div {
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
                            .child("Status: All ▼"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x94a3b8))
                            .child("Duration: Any ▼"),
                    ),
            )
            .child(
                // Main content area: Timeline and split pane
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .child(self.render_timeline())
                    .child(
                        div()
                            .flex()
                            .flex_1()
                            .min_h(px(0.0))
                            .child(
                                // Left: Trace list
                                div()
                                    .flex()
                                    .flex_col()
                                    .flex_1()
                                    .min_h(px(0.0))
                                    .w(relative(0.4))
                                    .border_r_1()
                                    .border_color(rgb(0x334155))
                                    .child(
                                        div()
                                            .px_4()
                                            .py_2()
                                            .bg(rgb(0x1e293b))
                                            .border_b_1()
                                            .border_color(rgb(0x334155))
                                            .text_xs()
                                            .font_weight(FontWeight::BOLD)
                                            .text_color(rgb(0x94a3b8))
                                            .child("Trace List"),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .size_full()
                                            .id("trace-list-scroll")
                                            .overflow_scroll()
                                            .children(self.traces.iter().map(|trace| {
                                        let is_selected = self
                                            .selected_trace
                                            .as_ref()
                                            .map_or(false, |t| t == &trace.trace_id);

                                        // Get root span name
                                        let root_span_name = trace
                                            .spans
                                            .iter()
                                            .find(|s| s.span_id == trace.root_span_id)
                                            .map(|s| s.operation_name.clone())
                                            .unwrap_or_else(|| "Unknown".to_string());

                                        let mut div_elem = div()
                                            .flex()
                                            .items_center()
                                            .gap_2()
                                            .px_4()
                                            .py_3()
                                            .cursor_pointer()
                                            .hover(|style| style.bg(rgb(0x2d3b4f)))
                                            .border_b_1()
                                            .border_color(rgb(0x1e293b));

                                        if is_selected {
                                            div_elem = div_elem.bg(rgb(0x334155));
                                        }

                                        div_elem
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::BOLD)
                                                    .text_color(Self::status_color(trace.has_error))
                                                    .child(Self::status_icon(trace.has_error)),
                                            )
                                            .child(
                                                div()
                                                    .flex()
                                                    .flex_col()
                                                    .flex_1()
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .text_color(rgb(0xf1f5f9))
                                                            .child(root_span_name),
                                                    )
                                                    .child(
                                                        div()
                                                            .text_xs()
                                                            .text_color(rgb(0x64748b))
                                                            .child(format!("{}ms", trace.duration)),
                                                    ),
                                            )
                                    }))
                                    )
                            )
                            .child(
                                // Right: Span details
                                div()
                                    .flex()
                                    .flex_col()
                                    .flex_1()
                                    .min_h(px(0.0))
                                    .child(
                                        div()
                                            .px_4()
                                            .py_2()
                                            .bg(rgb(0x1e293b))
                                            .border_b_1()
                                            .border_color(rgb(0x334155))
                                            .text_xs()
                                            .font_weight(FontWeight::BOLD)
                                            .text_color(rgb(0x94a3b8))
                                            .child("Span Details"),
                                    )
                                    .child(
                                        div()
                                            .flex_1()
                                            .size_full()
                                            .id("span-details-scroll")
                                            .overflow_scroll()
                                            .child(if self.selected_trace.is_some() && !self.spans.is_empty() {
                                                div().child(self.render_span_details())
                                            } else {
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .justify_center()
                                                    .h_full()
                                                    .text_sm()
                                                    .text_color(rgb(0x64748b))
                                                    .child("Select a trace to view span details")
                                            })
                                    )
                            )
                    ),
            )
    }
}

impl<Q: QueryApi + Clone + 'static> Render for TracesView<Q> {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.render_content()
    }
}
