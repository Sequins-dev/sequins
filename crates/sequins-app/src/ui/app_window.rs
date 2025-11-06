use gpui::*;
use sequins_core::traits::QueryApi;

use super::logs_view::LogsView;
use super::metrics_view::MetricsView;
use super::profiles_view::ProfilesView;
use super::service_navigator::ServiceNavigator;
use super::tabs::Tab;
use super::traces_view::TracesView;

pub struct AppWindow<Q: QueryApi> {
    api: Q,
    active_tab: Tab,
}

impl<Q: QueryApi + Clone + 'static> AppWindow<Q> {
    pub fn new(api: Q, _cx: &mut Context<Self>) -> Self {
        Self {
            api,
            active_tab: Tab::Logs,
        }
    }

    pub fn set_active_tab(&mut self, tab: Tab, cx: &mut Context<Self>) {
        self.active_tab = tab;
        cx.notify();
    }
}

impl<Q: QueryApi + Clone + 'static> Render for AppWindow<Q> {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {

        div()
            .flex()
            .size_full()
            .bg(rgb(0x0f172a))
            .child(
                // Service Navigator Sidebar - component instance
                ServiceNavigator::new(self.api.clone()).render_inline()
            )
            .child(
                // Right pane
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h(px(0.0))
                    .child(
                // Title bar
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .justify_between()
                    .px_4()
                    .py_2()
                    .bg(rgb(0x1e293b))
                    .border_b_1()
                    .border_color(rgb(0x334155))
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::BOLD)
                            .text_color(rgb(0xf1f5f9))
                            .child("Sequins"),
                    )
            )
            .child(
                // Tab bar
                div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap_1()
                    .px_4()
                    .py_2()
                    .bg(rgb(0x1e293b))
                    .border_b_1()
                    .border_color(rgb(0x334155))
                    .children(Tab::all().iter().map(|tab| {
                        let is_active = *tab == self.active_tab;
                        let tab_value = *tab;

                        let mut div_elem = div()
                            .px_4()
                            .py_2()
                            .rounded(px(6.0))
                            .cursor_pointer()
                            .on_mouse_down(MouseButton::Left, cx.listener(move |this, _event, _window, cx| {
                                this.set_active_tab(tab_value, cx);
                            }));

                        div_elem = if is_active {
                            div_elem.bg(rgb(0x3b82f6)).text_color(rgb(0xffffff))
                        } else {
                            div_elem
                                .text_color(rgb(0x94a3b8))
                                .hover(|style| style.bg(rgb(0x334155)).text_color(rgb(0xf1f5f9)))
                        };

                        div_elem.child(div().text_sm().font_weight(FontWeight::MEDIUM).child(tab.label()))
                    })),
            )
            .child(
                // Content area wrapper
                div()
                    .flex()
                    .flex_col()
                    .flex_1()
                    .min_h(px(0.0))
                    .child(
                        match self.active_tab {
                            Tab::Logs => LogsView::new(self.api.clone()).render_inline(),
                            Tab::Metrics => MetricsView::new(self.api.clone()).render_inline(),
                            Tab::Traces => TracesView::new(self.api.clone()).render_inline(),
                            Tab::Profiles => ProfilesView::new(self.api.clone()).render_inline(),
                        }
                    )
            )
                ) // end right pane
    }
}
