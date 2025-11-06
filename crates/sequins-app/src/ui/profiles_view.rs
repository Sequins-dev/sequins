use gpui::*;
use sequins_core::traits::QueryApi;

pub struct ProfilesView<Q: QueryApi> {
    pub api: Q,
}

impl<Q: QueryApi + Clone + 'static> ProfilesView<Q> {
    pub fn new(api: Q) -> Self {
        Self { api }
    }

    pub fn render_inline(&self) -> Div {
        self.render_content()
    }

    fn profile_color(name: &str) -> Hsla {
        // Generate consistent colors based on function name
        let hash = name.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
        let hue = (hash % 360) as f32;
        hsla(hue / 360.0, 0.7, 0.6, 1.0)
    }

    fn render_content(&self) -> Div {
        // Mock profile data for flame graph
        let mock_profile_stack = vec![
            ("main", 100.0, 0),
            ("http.Handler.ServeHTTP", 85.0, 1),
            ("api.processRequest", 70.0, 2),
            ("database.Query", 45.0, 3),
            ("json.Marshal", 15.0, 3),
            ("auth.Validate", 10.0, 2),
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
                            .child("Type: CPU ▼"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x94a3b8))
                            .child("Service: All ▼"),
                    ),
            )
            .child(
                // Main content
                div()
                    .flex()
                    .flex_1()
                    .min_h(px(0.0))
                    .child(
                        // Left: Profile list
                        div()
                            .flex()
                            .flex_col()
                            .flex_1()
                            .min_h(px(0.0))
                            .w(relative(0.3))
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
                                    .child("Available Profiles"),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .size_full()
                                    .id("profile-list-scroll")
                                    .overflow_scroll()
                                    .p_2()
                                    .child(
                                        div()
                                            .flex()
                                            .items_center()
                                            .justify_between()
                                            .px_3()
                                            .py_2()
                                            .mb_1()
                                            .bg(rgb(0x334155))
                                            .rounded(px(4.0))
                                            .cursor_pointer()
                                            .child(
                                                div()
                                                    .flex()
                                                    .flex_col()
                                                    .gap_1()
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .text_color(rgb(0xf1f5f9))
                                                            .child("api-gateway CPU profile"),
                                                    )
                                                    .child(
                                                        div()
                                                            .text_xs()
                                                            .text_color(rgb(0x64748b))
                                                            .child("2024-01-15 10:23:45"),
                                                    ),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0x94a3b8))
                                                    .child("2.5s"),
                                            ),
                                    )
                                    .children((0..5).map(|i| {
                                        div()
                                            .flex()
                                            .items_center()
                                            .justify_between()
                                            .px_3()
                                            .py_2()
                                            .mb_1()
                                            .hover(|style| style.bg(rgb(0x2d3b4f)))
                                            .rounded(px(4.0))
                                            .cursor_pointer()
                                            .child(
                                                div()
                                                    .flex()
                                                    .flex_col()
                                                    .gap_1()
                                                    .child(
                                                        div()
                                                            .text_sm()
                                                            .text_color(rgb(0xf1f5f9))
                                                            .child(format!("Profile #{}", 6 - i)),
                                                    )
                                                    .child(
                                                        div()
                                                            .text_xs()
                                                            .text_color(rgb(0x64748b))
                                                            .child(format!("2024-01-15 10:2{}:30", 3 - i)),
                                                    ),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0x94a3b8))
                                                    .child(format!("{}.{}s", i + 1, (i * 3) % 10)),
                                            )
                                    }))
                            ) // end scrollable container
                    )
                    .child(
                        // Right: Flame graph
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
                                    .child("Flame Graph"),
                            )
                            .child(
                                div()
                                    .flex_1()
                                    .size_full()
                                    .id("flame-graph-scroll")
                                    .overflow_scroll()
                                    .p_4()
                                    .child(
                                        // Info panel
                                        div()
                                            .flex()
                                            .items_center()
                                            .gap_4()
                                            .px_4()
                                            .py_3()
                                            .mb_2()
                                            .bg(rgb(0x1e293b))
                                            .border_1()
                                            .border_color(rgb(0x334155))
                                            .rounded(px(6.0))
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .text_color(rgb(0x94a3b8))
                                                    .child("Total Samples:"),
                                            )
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::BOLD)
                                                    .text_color(rgb(0xf1f5f9))
                                                    .child("1,234"),
                                            )
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .text_color(rgb(0x94a3b8))
                                                    .child("Duration:"),
                                            )
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::BOLD)
                                                    .text_color(rgb(0xf1f5f9))
                                                    .child("2.5s"),
                                            ),
                                    )
                                    .child(
                                        // Flame graph visualization (simplified)
                                        div()
                                            .flex()
                                            .flex_col()
                                            .flex_1()
                                            .gap_1()
                                            .p_4()
                                            .mb_2()
                                            .bg(rgb(0x1e293b))
                                            .border_1()
                                            .border_color(rgb(0x334155))
                                            .rounded(px(6.0))
                                            .children(mock_profile_stack.iter().map(|(name, width, depth)| {
                                                let indent = *depth * 20;
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .ml(px(indent as f32))
                                                    .child(
                                                        div()
                                                            .h(px(30.0))
                                                            .w(relative(*width / 100.0))
                                                            .bg(Self::profile_color(name))
                                                            .border_1()
                                                            .border_color(rgb(0x0f172a))
                                                            .rounded(px(2.0))
                                                            .flex()
                                                            .items_center()
                                                            .px_2()
                                                            .cursor_pointer()
                                                            .hover(|style| style.opacity(0.8))
                                                            .child(
                                                                div()
                                                                    .text_xs()
                                                                    .text_color(rgb(0xffffff))
                                                                    .child(format!("{} ({}%)", name, *width as i32)),
                                                            ),
                                                    )
                                            })),
                                    )
                                    .child(
                                        // Top functions
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_2()
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .font_weight(FontWeight::BOLD)
                                                    .text_color(rgb(0x94a3b8))
                                                    .mb_1()
                                                    .child("Top Functions by CPU Time"),
                                            )
                                            .children(mock_profile_stack.iter().take(5).map(|(name, percentage, _)| {
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .justify_between()
                                                    .px_3()
                                                    .py_2()
                                                    .bg(rgb(0x1e293b))
                                                    .border_1()
                                                    .border_color(rgb(0x334155))
                                                    .rounded(px(4.0))
                                                    .child(
                                                        div()
                                                            .text_xs()
                                                            .text_color(rgb(0xf1f5f9))
                                                            .child(*name),
                                                    )
                                                    .child(
                                                        div()
                                                            .text_xs()
                                                            .font_weight(FontWeight::BOLD)
                                                            .text_color(rgb(0x3b82f6))
                                                            .child(format!("{}%", *percentage as i32)),
                                                    )
                                            })),
                                    )
                            ) // end scrollable container
                    ),
            )
    }
}

impl<Q: QueryApi + Clone + 'static> Render for ProfilesView<Q> {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.render_content()
    }
}
