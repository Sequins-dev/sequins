use gpui::*;

/// Data point for a bar chart
#[derive(Debug, Clone)]
pub struct BarData {
    pub label: String,
    pub value: f32,
    pub color: Option<Hsla>,
}

/// Configuration for a bar chart
#[derive(Clone)]
pub struct BarChartConfig {
    pub default_color: Hsla,
    pub bar_spacing: Pixels,
    pub show_values: bool,
    pub show_grid: bool,
}

impl Default for BarChartConfig {
    fn default() -> Self {
        Self {
            default_color: rgb(0x3b82f6).into(),
            bar_spacing: px(8.0),
            show_values: true,
            show_grid: true,
        }
    }
}

/// A bar chart component
pub struct BarChart {
    data: Vec<BarData>,
    config: BarChartConfig,
    width: Pixels,
    height: Pixels,
}

impl BarChart {
    pub fn new(
        data: Vec<BarData>,
        width: Pixels,
        height: Pixels,
        config: BarChartConfig,
    ) -> Self {
        Self {
            data,
            config,
            width,
            height,
        }
    }

    pub fn render_chart(self) -> impl IntoElement {
        let config = self.config.clone();
        let data = self.data.clone();

        canvas(
            move |_bounds, _window, _cx| {
                // Prepaint - we don't need to compute anything here
                ()
            },
            move |bounds, _state, window, _cx| {
                if data.is_empty() {
                    return;
                }

                // Calculate dimensions
                let padding = 40.0;
                let chart_width: f32 = bounds.size.width.into();
                let chart_height: f32 = bounds.size.height.into();
                let chart_width = chart_width - 2.0 * padding;
                let chart_height = chart_height - 2.0 * padding;

                let max_value = data
                    .iter()
                    .map(|d| d.value)
                    .fold(f32::NEG_INFINITY, f32::max);

                let bar_count = data.len() as f32;
                let total_spacing: f32 = config.bar_spacing.into();
                let total_spacing = total_spacing * (bar_count - 1.0);
                let bar_width = (chart_width - total_spacing) / bar_count;

                // Draw grid if enabled
                if config.show_grid {
                    let grid_color: Hsla = rgb(0x334155).into();

                    // Horizontal grid lines
                    for i in 0..5 {
                        let y = bounds.origin.y
                            + px(padding)
                            + px(chart_height * (i as f32 / 4.0));
                        let mut path = PathBuilder::stroke(px(1.0));
                        path.move_to(point(bounds.origin.x + px(padding), y));
                        path.line_to(point(
                            bounds.origin.x + px(padding + chart_width),
                            y,
                        ));
                        if let Ok(path) = path.build() {
                            window.paint_path(path, grid_color);
                        }
                    }
                }

                // Draw bars
                for (i, bar_data) in data.iter().enumerate() {
                    let bar_height = if max_value > 0.0 {
                        (bar_data.value / max_value) * chart_height
                    } else {
                        0.0
                    };

                    let x: f32 = bounds.origin.x.into();
                    let y: f32 = bounds.origin.y.into();
                    let spacing: f32 = config.bar_spacing.into();

                    let x = x + padding + (i as f32) * (bar_width + spacing);
                    let y = y + padding + chart_height - bar_height;

                    let color = bar_data.color.unwrap_or(config.default_color);

                    // Draw bar
                    window.paint_quad(quad(
                        Bounds {
                            origin: point(px(x), px(y)),
                            size: size(px(bar_width), px(bar_height)),
                        },
                        Corners {
                            top_left: px(4.0),
                            top_right: px(4.0),
                            bottom_left: px(0.0),
                            bottom_right: px(0.0),
                        },
                        color,
                        Edges::default(),
                        Hsla::transparent_black(),
                        BorderStyle::Solid,
                    ));

                    // Draw value label if enabled
                    if config.show_values {
                        // Note: Text rendering would require access to the text system
                        // For now, we skip labels - they can be added separately in the parent component
                    }
                }
            },
        )
        .w(self.width)
        .h(self.height)
    }
}
