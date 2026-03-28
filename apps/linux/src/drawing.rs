//! Shared Cairo drawing utilities used by chart components.

// ── Chart theme ────────────────────────────────────────────────────────────────

/// Colour values for Cairo-drawn charts, resolved from the current libadwaita
/// colour scheme at the time of construction.
///
/// Call `ChartTheme::current()` at the top of each draw function so that the
/// chart re-evaluates the scheme on every repaint (handles live theme switches).
pub struct ChartTheme {
    pub is_dark: bool,
}

impl ChartTheme {
    /// Resolve the current system colour scheme via libadwaita.
    pub fn current() -> Self {
        Self {
            is_dark: libadwaita::StyleManager::default().is_dark(),
        }
    }

    /// Chart/canvas background.
    pub fn bg(&self) -> (f64, f64, f64) {
        if self.is_dark {
            (0.10, 0.10, 0.11)
        } else {
            (0.96, 0.96, 0.97)
        }
    }

    /// Timeline ruler background (slightly different from chart bg).
    pub fn ruler_bg(&self) -> (f64, f64, f64) {
        if self.is_dark {
            (0.12, 0.12, 0.13)
        } else {
            (0.92, 0.92, 0.93)
        }
    }

    /// Grid line colour (low alpha).
    pub fn grid(&self) -> (f64, f64, f64, f64) {
        if self.is_dark {
            (1.0, 1.0, 1.0, 0.07)
        } else {
            (0.0, 0.0, 0.0, 0.10)
        }
    }

    /// Axis label / tick text (medium alpha).
    pub fn label(&self) -> (f64, f64, f64, f64) {
        if self.is_dark {
            (1.0, 1.0, 1.0, 0.5)
        } else {
            (0.0, 0.0, 0.0, 0.55)
        }
    }

    /// Primary body text (span names, node names, …).
    pub fn text(&self) -> (f64, f64, f64) {
        if self.is_dark {
            (0.80, 0.80, 0.80)
        } else {
            (0.15, 0.15, 0.15)
        }
    }

    /// "No data" / placeholder text.
    pub fn placeholder(&self) -> (f64, f64, f64, f64) {
        if self.is_dark {
            (1.0, 1.0, 1.0, 0.30)
        } else {
            (0.0, 0.0, 0.0, 0.30)
        }
    }

    /// Separator / border lines.
    pub fn separator(&self) -> (f64, f64, f64, f64) {
        if self.is_dark {
            (0.30, 0.30, 0.33, 1.0)
        } else {
            (0.75, 0.75, 0.78, 1.0)
        }
    }

    /// Hover highlight overlay.
    pub fn hover_overlay(&self) -> (f64, f64, f64, f64) {
        if self.is_dark {
            (1.0, 1.0, 1.0, 0.06)
        } else {
            (0.0, 0.0, 0.0, 0.06)
        }
    }

    /// Tree indent guide lines.
    pub fn guide_line(&self) -> (f64, f64, f64, f64) {
        if self.is_dark {
            (1.0, 1.0, 1.0, 0.12)
        } else {
            (0.0, 0.0, 0.0, 0.15)
        }
    }

    /// Ruler tick / minor text colour.
    pub fn ruler_text(&self) -> (f64, f64, f64) {
        if self.is_dark {
            (0.50, 0.50, 0.53)
        } else {
            (0.40, 0.40, 0.43)
        }
    }
}

/// Map a function/symbol name to a deterministic RGB color via FNV hash → HSV.
pub fn name_to_color(name: &str) -> (f64, f64, f64) {
    let hash = name.bytes().fold(0x811c9dc5u32, |acc, b| {
        acc.wrapping_mul(0x01000193).wrapping_add(b as u32)
    });
    let hue = (hash % 360) as f64 / 360.0;
    hsv_to_rgb(hue, 0.55, 0.82)
}

/// Flamegraph frame color — blue gradient keyed on what fraction of the parent's
/// value this node consumes. Matches the macOS `ProfileColorScheme.colorForRatio`.
///
/// - `ratio` = `node.total_value / parent.total_value` (1.0 for roots)
/// - Low ratio → dark desaturated blue; high ratio → bright vivid blue.
pub fn frame_color_for_ratio(ratio: f64) -> (f64, f64, f64) {
    let r = ratio.clamp(0.0, 1.0);
    hsv_to_rgb(0.6, 0.05 + 0.95 * r, 0.3 + 0.7 * r)
}

/// Service color palette — exact match to the macOS Sequins app.
/// Colors are assigned sequentially per service (first seen → index 0, etc.),
/// matching the macOS `ServiceColorMapper` behaviour.
pub const SPAN_PALETTE: [(f64, f64, f64); 8] = [
    (0.231, 0.510, 0.965), // blue   #3B82F6
    (0.133, 0.773, 0.369), // green  #22C55E
    (0.976, 0.451, 0.086), // orange #F97316
    (0.659, 0.333, 0.969), // purple #A855F7
    (0.078, 0.722, 0.651), // teal   #14B8A6
    (0.925, 0.282, 0.600), // pink   #EC4899
    (0.918, 0.702, 0.031), // yellow #EAB308
    (0.024, 0.714, 0.831), // cyan   #06B6D4
];

/// Return the palette color for a given sequential index.
/// Pass `is_dark = false` to get a slightly darker variant for light-mode readability.
pub fn palette_color(index: usize, is_dark: bool) -> (f64, f64, f64) {
    let (r, g, b) = SPAN_PALETTE[index % SPAN_PALETTE.len()];
    if is_dark {
        (r, g, b)
    } else {
        // Darken so `contrasting_text_color` reliably picks white text on light backgrounds
        (r * 0.78, g * 0.78, b * 0.78)
    }
}

/// Choose black or white text to maximise contrast against a given background color.
pub fn contrasting_text_color(r: f64, g: f64, b: f64) -> (f64, f64, f64) {
    // Perceived luminance (sRGB coefficients)
    let lum = 0.299 * r + 0.587 * g + 0.114 * b;
    if lum > 0.45 {
        (0.05, 0.05, 0.05) // dark text on light background
    } else {
        (1.0, 1.0, 1.0) // white text on dark background
    }
}

pub fn hsv_to_rgb(h: f64, s: f64, v: f64) -> (f64, f64, f64) {
    let i = (h * 6.0).floor() as u32;
    let f = h * 6.0 - i as f64;
    let p = v * (1.0 - s);
    let q = v * (1.0 - f * s);
    let t = v * (1.0 - (1.0 - f) * s);
    match i % 6 {
        0 => (v, t, p),
        1 => (q, v, p),
        2 => (p, v, t),
        3 => (p, q, v),
        4 => (t, p, v),
        _ => (v, p, q),
    }
}

/// Color for a span/request status value (OTLP convention: 1=OK, 2=Error, else Unset).
pub fn status_color(status: u8) -> (f64, f64, f64) {
    match status {
        1 => (0.25, 0.75, 0.35), // OK — green
        2 => (0.85, 0.22, 0.22), // Error — red
        _ => (0.35, 0.55, 0.88), // Unset — blue
    }
}

// ── Axis tick computation ──────────────────────────────────────────────────

/// A single X-axis time tick.
pub struct TimeTick {
    pub ns: i64,
    pub label: String,
}

/// Compute ~`target_count` nicely-spaced time ticks between `start_ns` and `end_ns`.
/// Ticks are anchored to clock boundaries (e.g. HH:00, HH:05) and formatted "HH:mm".
/// Ticks whose fractional position is < 0.15 are suppressed to avoid Y-axis overlap.
pub fn compute_time_ticks(start_ns: i64, end_ns: i64, target_count: usize) -> Vec<TimeTick> {
    if end_ns <= start_ns || target_count == 0 {
        return vec![];
    }
    let span_ns = end_ns - start_ns;
    let raw_step = span_ns / target_count as i64;

    // Nice intervals in nanoseconds.
    const SEC: i64 = 1_000_000_000;
    const MIN: i64 = 60 * SEC;
    const HOUR: i64 = 60 * MIN;
    let nice_steps: &[i64] = &[
        10 * SEC,
        15 * SEC,
        30 * SEC,
        MIN,
        2 * MIN,
        5 * MIN,
        10 * MIN,
        15 * MIN,
        30 * MIN,
        HOUR,
        2 * HOUR,
        6 * HOUR,
    ];
    let step = *nice_steps
        .iter()
        .find(|&&s| s >= raw_step)
        .unwrap_or(nice_steps.last().unwrap());

    // First tick: smallest multiple of step >= start_ns.
    let first = ((start_ns + step - 1) / step) * step;

    let mut ticks = Vec::new();
    let mut t = first;
    while t <= end_ns {
        let frac = (t - start_ns) as f64 / span_ns as f64;
        if frac >= 0.15 {
            // Format as "HH:mm" in UTC.
            let secs = t / SEC;
            let total_minutes = secs / 60;
            let hh = (total_minutes / 60) % 24;
            let mm = total_minutes % 60;
            ticks.push(TimeTick {
                ns: t,
                label: format!("{:02}:{:02}", hh, mm),
            });
        }
        t += step;
    }
    ticks
}

// ── Value tick computation ─────────────────────────────────────────────────

/// A single Y-axis value tick.
pub struct ValueTick {
    pub value: f64,
    pub label: String,
}

/// Compute ~`target_count` nicely-spaced value ticks from 0 up to `max_value * 1.10`.
/// `unit` controls label formatting (e.g. "ns", "By", "%").
pub fn compute_value_ticks(max_value: f64, target_count: usize, unit: &str) -> Vec<ValueTick> {
    if max_value <= 0.0 || target_count == 0 {
        return vec![];
    }
    let domain = max_value * 1.10;
    let raw_step = domain / target_count as f64;

    // Snap to 1, 2, 2.5, 5 × 10^n.
    let magnitude = raw_step.log10().floor();
    let base = 10f64.powf(magnitude);
    let candidates = [1.0_f64, 2.0, 2.5, 5.0, 10.0];
    let step = candidates
        .iter()
        .map(|&c| c * base)
        .find(|&s| s >= raw_step)
        .unwrap_or(10.0 * base);

    let mut ticks = Vec::new();
    let mut v = step; // start at step (skip 0 — it's the axis baseline)
    while v <= domain + step * 0.001 {
        let label = format_value_tick(v, unit);
        ticks.push(ValueTick { value: v, label });
        v += step;
    }
    ticks
}

fn format_value_tick(v: f64, unit: &str) -> String {
    match unit {
        "ns" | "nanoseconds" => {
            let us = v / 1_000.0;
            if us < 1_000.0 {
                format!("{:.0}µs", us)
            } else {
                format!("{:.1}ms", us / 1_000.0)
            }
        }
        "By" | "bytes" => {
            if v >= 1_073_741_824.0 {
                format!("{:.1}GB", v / 1_073_741_824.0)
            } else if v >= 1_048_576.0 {
                format!("{:.1}MB", v / 1_048_576.0)
            } else if v >= 1_024.0 {
                format!("{:.1}KB", v / 1_024.0)
            } else {
                format!("{:.0}B", v)
            }
        }
        "%" => format!("{:.0}%", v),
        _ => {
            if v >= 1_000_000.0 {
                format!("{:.1}M", v / 1_000_000.0)
            } else if v >= 1_000.0 {
                format!("{:.1}k", v / 1_000.0)
            } else if (v - v.round()).abs() < 0.001 {
                format!("{:.0}", v)
            } else {
                format!("{:.2}", v)
            }
        }
    }
}

/// Format a numeric value with human-readable suffixes based on unit.
pub fn format_value(v: f64, unit: &str) -> String {
    if unit == "ns" || unit == "nanoseconds" {
        let us = v / 1_000.0;
        if us.abs() < 1_000.0 {
            return format!("{:.0}µs", us);
        }
        return format!("{:.1}ms", us / 1_000.0);
    }
    if v.abs() >= 1_000_000.0 {
        return format!("{:.1}M", v / 1_000_000.0);
    }
    if v.abs() >= 1_000.0 {
        return format!("{:.1}k", v / 1_000.0);
    }
    if (v - v.round()).abs() < 0.001 {
        format!("{:.0}", v)
    } else {
        format!("{:.3}", v)
    }
}
