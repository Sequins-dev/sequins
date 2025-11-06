use gpui::{prelude::*, *};
use sequins_core::{models::Service, traits::QueryApi};

pub struct ServiceNavigator<Q: QueryApi> {
    api: Q,
    services: Vec<Service>,
}

impl<Q: QueryApi + Clone + 'static> ServiceNavigator<Q> {
    pub fn new(api: Q) -> Self {
        Self {
            api,
            services: Self::mock_services(),
        }
    }

    pub fn render_inline(&self) -> Div {
        self.render_content()
    }

    /// Generate mock services for UI development
    /// TODO: Replace with actual async loading from QueryApi
    fn mock_services() -> Vec<Service> {
        vec![
            Service {
                name: "api-gateway".to_string(),
                span_count: 245,
                log_count: 1203,
            },
            Service {
                name: "auth-service".to_string(),
                span_count: 189,
                log_count: 456,
            },
            Service {
                name: "user-service".to_string(),
                span_count: 412,
                log_count: 892,
            },
            Service {
                name: "order-service".to_string(),
                span_count: 324,
                log_count: 678,
            },
            Service {
                name: "payment-service".to_string(),
                span_count: 156,
                log_count: 234,
            },
            Service {
                name: "notification-service".to_string(),
                span_count: 98,
                log_count: 145,
            },
            Service {
                name: "database".to_string(),
                span_count: 567,
                log_count: 89,
            },
            Service {
                name: "cache".to_string(),
                span_count: 234,
                log_count: 45,
            },
        ]
    }

    fn health_color(span_count: usize) -> Hsla {
        // Simple heuristic: green for active, yellow for low activity, red for no activity
        if span_count > 100 {
            rgb(0x10b981).into() // green
        } else if span_count > 10 {
            rgb(0xf59e0b).into() // yellow/amber
        } else {
            rgb(0xef4444).into() // red
        }
    }

    fn render_content(&self) -> Div {

        div()
            .flex()
            .flex_col()
            .w(px(250.0))
            .h_full()
            .bg(rgb(0x1e293b))
            .border_r_1()
            .border_color(rgb(0x334155))
            .child(
                // Header
                div()
                    .flex_shrink_0()
                    .px_4()
                    .py_3()
                    .border_b_1()
                    .border_color(rgb(0x334155))
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::BOLD)
                            .text_color(rgb(0xf1f5f9))
                            .child("Services"),
                    )
            )
            .child(
                // Service list - scrollable
                div()
                    .flex_1()
                    .size_full()
                    .id("services-scroll")
                    .overflow_scroll()
                    .p_2()
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .children(self.services.iter().map(|service| {
                                div()
                                    .flex()
                                    .items_center()
                                    .gap_2()
                                    .px_3()
                                    .py_2()
                                    .rounded(px(6.0))
                                    .hover(|style| style.bg(rgb(0x334155)))
                                    .cursor_pointer()
                                    .child(
                                        // Health indicator dot
                                        div()
                                            .w(px(8.0))
                                            .h(px(8.0))
                                            .rounded(px(4.0))
                                            .bg(Self::health_color(service.span_count)),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_1()
                                            .items_center()
                                            .justify_between()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .text_color(rgb(0xf1f5f9))
                                                    .child(service.name.clone()),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0x64748b))
                                                    .child(format!("{}", service.span_count)),
                                            )
                                    )
                            }))
                    )
            )
    }
}

impl<Q: QueryApi + Clone + 'static> Render for ServiceNavigator<Q> {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.render_content()
    }
}
