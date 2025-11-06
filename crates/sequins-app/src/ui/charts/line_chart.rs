use gpui::*;

/// Data point for a line chart
#[derive(Debug, Clone)]
pub struct DataPoint {
    pub x: f32,
    pub y: f32,
}

/// Configuration for a line chart
#[derive(Clone)]
pub struct LineChartConfig {
    pub color: Hsla,
    pub line_width: Pixels,
    pub show_points: bool,
    pub show_grid: bool,
    pub smooth: bool,
}

impl Default for LineChartConfig {
    fn default() -> Self {
        Self {
            color: rgb(0x3b82f6).into(),
            line_width: px(2.0),
            show_points: true,
            show_grid: true,
            smooth: false,
        }
    }
}

/// A line chart component
pub struct LineChart {
    data: Vec<DataPoint>,
    config: LineChartConfig,
    width: Pixels,
    height: Pixels,
}

impl LineChart {
    pub fn new(
        data: Vec<DataPoint>,
        width: Pixels,
        height: Pixels,
        config: LineChartConfig,
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
                // Draw grid if enabled
                if config.show_grid {
                    let grid_color: Hsla = rgb(0x334155).into();

                    // Horizontal grid lines
                    for i in 0..5 {
                        let y = bounds.origin.y + bounds.size.height * (i as f32 / 4.0);
                        let mut path = PathBuilder::stroke(px(1.0));
                        path.move_to(point(bounds.origin.x, y));
                        path.line_to(point(bounds.origin.x + bounds.size.width, y));
                        if let Ok(path) = path.build() {
                            window.paint_path(path, grid_color);
                        }
                    }

                    // Vertical grid lines
                    for i in 0..5 {
                        let x = bounds.origin.x + bounds.size.width * (i as f32 / 4.0);
                        let mut path = PathBuilder::stroke(px(1.0));
                        path.move_to(point(x, bounds.origin.y));
                        path.line_to(point(x, bounds.origin.y + bounds.size.height));
                        if let Ok(path) = path.build() {
                            window.paint_path(path, grid_color);
                        }
                    }
                }

                if data.is_empty() {
                    return;
                }

                // Normalize data points to bounds
                let points = {
                    // Find min/max values
                    let min_x = data.iter().map(|p| p.x).fold(f32::INFINITY, f32::min);
                    let max_x = data.iter().map(|p| p.x).fold(f32::NEG_INFINITY, f32::max);
                    let min_y = data.iter().map(|p| p.y).fold(f32::INFINITY, f32::min);
                    let max_y = data.iter().map(|p| p.y).fold(f32::NEG_INFINITY, f32::max);

                    let width: f32 = bounds.size.width.into();
                    let height: f32 = bounds.size.height.into();

                    // Add padding
                    let padding = 20.0;
                    let chart_width = width - 2.0 * padding;
                    let chart_height = height - 2.0 * padding;

                    // Normalize to bounds
                    data.iter()
                        .map(|p| {
                            let x = if max_x > min_x {
                                padding + ((p.x - min_x) / (max_x - min_x)) * chart_width
                            } else {
                                padding + chart_width / 2.0
                            };

                            // Invert Y axis (screen coordinates go down, but charts go up)
                            let y = if max_y > min_y {
                                padding + chart_height - ((p.y - min_y) / (max_y - min_y)) * chart_height
                            } else {
                                padding + chart_height / 2.0
                            };

                            let origin_x: f32 = bounds.origin.x.into();
                            let origin_y: f32 = bounds.origin.y.into();
                            point(px(x + origin_x), px(y + origin_y))
                        })
                        .collect::<Vec<_>>()
                };

                // Draw line
                if points.len() > 1 {
                    let mut path = PathBuilder::stroke(config.line_width);

                    path.move_to(points[0]);
                    for point in &points[1..] {
                        path.line_to(*point);
                    }

                    if let Ok(path) = path.build() {
                        window.paint_path(path, config.color);
                    }
                }

                // Draw points if enabled
                if config.show_points {
                    for point in &points {
                        window.paint_quad(quad(
                            Bounds {
                                origin: Point {
                                    x: point.x - px(3.0),
                                    y: point.y - px(3.0),
                                },
                                size: size(px(6.0), px(6.0)),
                            },
                            Corners::all(px(3.0)),
                            config.color,
                            Edges::default(),
                            Hsla::transparent_black(),
                            BorderStyle::Solid,
                        ));
                    }
                }
            },
        )
        .w(self.width)
        .h(self.height)
    }
}
