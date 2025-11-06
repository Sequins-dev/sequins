use gpui::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Logs,
    Metrics,
    Traces,
    Profiles,
}

impl Tab {
    pub fn label(&self) -> &'static str {
        match self {
            Tab::Logs => "Logs",
            Tab::Metrics => "Metrics",
            Tab::Traces => "Traces",
            Tab::Profiles => "Profiles",
        }
    }

    pub fn all() -> [Tab; 4] {
        [Tab::Metrics, Tab::Logs, Tab::Profiles, Tab::Traces]
    }
}

pub struct TabBar {
    pub active_tab: Tab,
}

impl TabBar {
    pub fn new(initial_tab: Tab) -> Self {
        Self {
            active_tab: initial_tab,
        }
    }

    pub fn active_tab(&self) -> Tab {
        self.active_tab
    }

    pub fn set_active_tab(&mut self, tab: Tab, cx: &mut Context<Self>) {
        self.active_tab = tab;
        cx.notify();
    }
}

impl Render for TabBar {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
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
            }))
    }
}
