use gpui::*;
use sequins_core::traits::QueryApi;

pub struct LogsView<Q: QueryApi> {
    api: Q,
}

impl<Q: QueryApi + Clone + 'static> LogsView<Q> {
    pub fn new(api: Q) -> Self {
        Self { api }
    }

    pub fn render_inline(&self) -> Div {
        self.render_content()
    }

    fn severity_color(severity: &str) -> Hsla {
        match severity {
            "ERROR" => rgb(0xef4444).into(),   // red
            "WARN" => rgb(0xf59e0b).into(),    // amber
            "INFO" => rgb(0x3b82f6).into(),    // blue
            "DEBUG" => rgb(0x6b7280).into(),   // gray
            _ => rgb(0x94a3b8).into(),         // default gray
        }
    }

    fn render_content(&self) -> Div {
        // Mock log entries for UI development - expanded for scrolling test
        let mock_logs = vec![
            ("2024-01-15 10:23:45.123", "ERROR", "api-gateway", "Failed to connect to database"),
            ("2024-01-15 10:23:44.892", "WARN", "auth-service", "Rate limit approaching for user 12345"),
            ("2024-01-15 10:23:44.567", "INFO", "user-service", "User authentication successful"),
            ("2024-01-15 10:23:44.234", "INFO", "order-service", "Order #98765 created successfully"),
            ("2024-01-15 10:23:43.901", "DEBUG", "payment-service", "Processing payment method validation"),
            ("2024-01-15 10:23:43.678", "INFO", "notification-service", "Email sent to user@example.com"),
            ("2024-01-15 10:23:43.456", "ERROR", "payment-service", "Payment gateway timeout after 30s"),
            ("2024-01-15 10:23:43.123", "WARN", "cache", "Cache miss rate above threshold: 45%"),
            ("2024-01-15 10:23:42.987", "ERROR", "api-gateway", "Database connection pool exhausted"),
            ("2024-01-15 10:23:42.765", "WARN", "auth-service", "Suspicious login attempt detected"),
            ("2024-01-15 10:23:42.543", "INFO", "user-service", "User profile updated successfully"),
            ("2024-01-15 10:23:42.321", "INFO", "order-service", "Order #98764 shipped"),
            ("2024-01-15 10:23:42.098", "DEBUG", "payment-service", "Validating credit card information"),
            ("2024-01-15 10:23:41.876", "INFO", "notification-service", "SMS notification sent"),
            ("2024-01-15 10:23:41.654", "ERROR", "payment-service", "Payment declined by processor"),
            ("2024-01-15 10:23:41.432", "WARN", "cache", "Redis connection unstable"),
            ("2024-01-15 10:23:41.210", "ERROR", "api-gateway", "Rate limit exceeded for IP"),
            ("2024-01-15 10:23:40.988", "WARN", "auth-service", "Password reset requested"),
            ("2024-01-15 10:23:40.766", "INFO", "user-service", "New user registration"),
            ("2024-01-15 10:23:40.544", "INFO", "order-service", "Order #98763 created"),
            ("2024-01-15 10:23:40.322", "DEBUG", "payment-service", "Processing refund request"),
            ("2024-01-15 10:23:40.100", "INFO", "notification-service", "Push notification queued"),
            ("2024-01-15 10:23:39.878", "ERROR", "payment-service", "Payment webhook validation failed"),
            ("2024-01-15 10:23:39.656", "WARN", "cache", "Memory usage approaching limit"),
            ("2024-01-15 10:23:39.434", "ERROR", "api-gateway", "Upstream service unavailable"),
            ("2024-01-15 10:23:39.212", "WARN", "auth-service", "JWT token expiring soon"),
            ("2024-01-15 10:23:38.990", "INFO", "user-service", "User preferences saved"),
            ("2024-01-15 10:23:38.768", "INFO", "order-service", "Inventory check completed"),
            ("2024-01-15 10:23:38.546", "DEBUG", "payment-service", "Fraud detection scan"),
            ("2024-01-15 10:23:38.324", "INFO", "notification-service", "Email delivery confirmed"),
            ("2024-01-15 10:23:38.102", "ERROR", "payment-service", "Transaction timeout"),
            ("2024-01-15 10:23:37.880", "WARN", "cache", "Cache warming in progress"),
            ("2024-01-15 10:23:37.658", "ERROR", "api-gateway", "Circuit breaker opened"),
            ("2024-01-15 10:23:37.436", "WARN", "auth-service", "Multiple failed login attempts"),
            ("2024-01-15 10:23:37.214", "INFO", "user-service", "Password changed successfully"),
            ("2024-01-15 10:23:36.992", "INFO", "order-service", "Order tracking updated"),
            ("2024-01-15 10:23:36.770", "DEBUG", "payment-service", "Reconciliation job started"),
            ("2024-01-15 10:23:36.548", "INFO", "notification-service", "Notification preferences updated"),
            ("2024-01-15 10:23:36.326", "ERROR", "payment-service", "Duplicate transaction detected"),
            ("2024-01-15 10:23:36.104", "WARN", "cache", "Evicting expired keys"),
            ("2024-01-15 10:23:35.882", "ERROR", "api-gateway", "SSL certificate validation failed"),
            ("2024-01-15 10:23:35.660", "WARN", "auth-service", "Session cleanup running"),
            ("2024-01-15 10:23:35.438", "INFO", "user-service", "Avatar image uploaded"),
            ("2024-01-15 10:23:35.216", "INFO", "order-service", "Return initiated"),
            ("2024-01-15 10:23:34.994", "DEBUG", "payment-service", "Batch processing started"),
            ("2024-01-15 10:23:34.772", "INFO", "notification-service", "Template rendered"),
            ("2024-01-15 10:23:34.550", "ERROR", "payment-service", "Insufficient funds"),
            ("2024-01-15 10:23:34.328", "WARN", "cache", "Slow query detected"),
            ("2024-01-15 10:23:34.106", "ERROR", "api-gateway", "Request timeout after 30s"),
            ("2024-01-15 10:23:33.884", "WARN", "auth-service", "Token refresh required"),
        ];

        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(rgb(0x0f172a))
            .child(
                // Filter bar - fixed at top
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap_3()
                    .px_4()
                    .h(px(60.0))
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
                            .child("Severity: All ▼"),
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(0x94a3b8))
                            .child("Service: All ▼"),
                    )
                    .child(
                        div()
                            .flex_1()
                            .flex()
                            .justify_end()
                            .child(
                                div()
                                    .px_3()
                                    .py_1()
                                    .bg(rgb(0x0f172a))
                                    .border_1()
                                    .border_color(rgb(0x475569))
                                    .rounded(px(4.0))
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(rgb(0x94a3b8))
                                            .child("🔍 Search logs..."),
                                    ),
                            ),
                    ),
            )
            .child(
                // Log entries - scrollable container using pattern from official example
                div()
                    .flex_1()
                    .size_full()
                    .id("logs-scroll")
                    .overflow_scroll()
                    .bg(rgb(0x0a0e1a))
                    .child(
                        // Wrapper with explicit height to force scrolling (like official example)
                        div()
                            .h(px(5000.0))
                            .flex()
                            .flex_col()
                            .bg(rgb(0x1a1f2e))
                            .p_4()
                            .children(mock_logs.iter().map(|(timestamp, severity, service, message)| {
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_3()
                                    .px_4()
                                    .py_2()
                                    .border_b_1()
                                    .border_color(rgb(0x1e293b))
                                    .hover(|style| style.bg(rgb(0x1e293b)))
                                    .cursor_pointer()
                                    .child(
                                        // Timestamp
                                        div()
                                            .w(px(180.0))
                                            .flex_shrink_0()
                                            .text_xs()
                                            .text_color(rgb(0x64748b))
                                            .child(*timestamp),
                                    )
                                    .child(
                                        // Severity badge
                                        div()
                                            .w(px(60.0))
                                            .flex_shrink_0()
                                            .child(
                                                div()
                                                    .px_2()
                                                    .py_1()
                                                    .rounded(px(3.0))
                                                    .bg(Self::severity_color(severity))
                                                    .text_xs()
                                                    .font_weight(FontWeight::BOLD)
                                                    .text_color(rgb(0xffffff))
                                                    .text_align(TextAlign::Center)
                                                    .child(*severity),
                                            ),
                                    )
                                    .child(
                                        // Service
                                        div()
                                            .w(px(150.0))
                                            .flex_shrink_0()
                                            .text_xs()
                                            .text_color(rgb(0x94a3b8))
                                            .child(*service),
                                    )
                                    .child(
                                        // Message
                                        div()
                                            .flex_1()
                                            .text_sm()
                                            .text_color(rgb(0xf1f5f9))
                                            .child(*message),
                                    )
                            }))
                    )
            )
    }
}

impl<Q: QueryApi + Clone + 'static> Render for LogsView<Q> {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.render_content()
    }
}
