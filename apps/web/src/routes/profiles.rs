//! Profiles tab — server-rendered SVG icicle/flamegraph.
//!
//! Uses FlamegraphStrategy to build a call tree from samples + stacks + frames,
//! then renders an SVG icicle chart (top-down, root at top).

use async_stream::stream;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use datastar::axum::ReadSignals;
use datastar::consts::ElementPatchMode;
use datastar::patch_elements::PatchElements;
use datastar::patch_signals::PatchSignals;
use futures::StreamExt;
use sequins_query::QueryApi;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::Infallible;

use crate::state::AppState;
use crate::stream::html_escape;
use sequins_query::frame::ipc_to_batch;
use sequins_view::{FlamegraphStrategy, ViewDelta, ViewStrategy};

use arrow::array::{Array, Int64Array, StringArray, UInt32Array};

#[derive(Deserialize, Default)]
pub struct ProfileSignals {
    #[serde(default)]
    pub service: String,
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default)]
    pub value_type: String,
}

fn default_duration() -> String {
    "1h".to_string()
}

struct FgNode {
    path_key: String,
    function_name: String,
    depth: u32,
    parent_path_key: Option<String>,
    total_value: i64,
    self_value: i64,
    filename: Option<String>,
    line: Option<i64>,
    system_name: Option<String>,
}

pub async fn handler(
    State(state): State<AppState>,
    ReadSignals(params): ReadSignals<ProfileSignals>,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let backend = state.backend.clone();
    let tmpl = state.templates.clone();

    let s = stream! {
        let page_html = match tmpl.render("partials/profiles.html", minijinja::context! {
            service => &params.service,
            duration => &params.duration,
            value_type => &params.value_type,
        }) {
            Ok(h) => h,
            Err(e) => { tracing::error!("Profiles template error: {e}"); return; }
        };
        yield Ok(Event::from(
            PatchElements::new(page_html).selector("#content").mode(ElementPatchMode::Inner),
        ));
        yield Ok(Event::from(
            PatchElements::new(r#"<input type="text" class="px-2 py-1 text-xs border border-zinc-600 rounded bg-zinc-950 text-zinc-300 placeholder-zinc-500 focus:outline-none focus:border-blue-400 w-40 shrink-0" placeholder="Search frames…" data-bind:search data-on:input__debounce.300ms="window.flamegraphSearch&&window.flamegraphSearch($search)">"#)
                .selector("#tab-controls")
                .mode(ElementPatchMode::Inner),
        ));
        yield Ok(Event::from(PatchSignals::new(r#"{"loading": true}"#)));

        // Build query — matches macOS ProfilesViewModel
        let mut q = format!("samples last {}", params.duration);
        if !params.service.is_empty() {
            q.push_str(&format!(" | where resource_id = '{}'", params.service));
        }
        if !params.value_type.is_empty() {
            q.push_str(&format!(" | where value_type = '{}'", params.value_type.replace('\'', "''")));
        }
        q.push_str(" <- stacks <- frames");

        let retention_ns = duration_to_ns(&params.duration);

        let raw_stream = match backend.query(&q).await {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Profiles query error: {e}");
                yield Ok(error_event(&e.to_string()));
                yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                return;
            }
        };

        let strategy = FlamegraphStrategy::new(retention_ns);
        let mut delta_stream = strategy.transform(raw_stream).await;

        // Collect entity deltas into a node map
        let mut nodes: HashMap<String, FgNode> = HashMap::new();

        while let Some(delta) = delta_stream.next().await {
            match delta {
                ViewDelta::EntityCreated { key, descriptor_ipc, data_ipc } => {
                    if let Some(node) = decode_fg_node(&key, &descriptor_ipc, &data_ipc) {
                        nodes.insert(key, node);
                    }
                }
                ViewDelta::EntityDataReplaced { key, data_ipc } => {
                    if let Some(node) = nodes.get_mut(&key) {
                        if let Ok(batch) = ipc_to_batch(&data_ipc) {
                            if let Some(tv_col) = batch.column_by_name("total_value")
                                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                            {
                                if !tv_col.is_null(0) { node.total_value = tv_col.value(0); }
                            }
                            if let Some(sv_col) = batch.column_by_name("self_value")
                                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                            {
                                if !sv_col.is_null(0) { node.self_value = sv_col.value(0); }
                            }
                        }
                    }
                }
                ViewDelta::EntityRemoved { key } => {
                    nodes.remove(&key);
                }
                ViewDelta::Ready => break,
                ViewDelta::Error { message } => {
                    yield Ok(error_event(&message));
                    yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
                    return;
                }
                _ => {}
            }
        }

        let svg_html = if nodes.is_empty() {
            r#"<div class="flex items-center justify-center h-48 text-zinc-500">No profile data available</div>"#.to_string()
        } else {
            render_icicle_svg(&nodes)
        };

        yield Ok(Event::from(
            PatchElements::new(svg_html)
                .selector("#flamegraph")
                .mode(ElementPatchMode::Inner),
        ));
        yield Ok(Event::from(PatchSignals::new(r#"{"loading": false}"#)));
    };

    Sse::new(s).keep_alive(KeepAlive::default())
}

fn decode_fg_node(key: &str, descriptor_ipc: &[u8], data_ipc: &[u8]) -> Option<FgNode> {
    let desc = ipc_to_batch(descriptor_ipc).ok()?;
    let data = ipc_to_batch(data_ipc).ok()?;

    let function_name = desc
        .column_by_name("function_name")
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<StringArray>()
                .map(|a| a.value(0).to_string())
        })
        .unwrap_or_else(|| key.to_string());

    let depth = desc
        .column_by_name("depth")
        .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
        .filter(|a| !a.is_null(0))
        .map(|a| a.value(0))
        .unwrap_or(0);

    let parent_path_key = desc.column_by_name("parent_path_key").and_then(|c| {
        if c.is_null(0) {
            return None;
        }
        c.as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(0).to_string())
    });

    let filename = desc.column_by_name("filename").and_then(|c| {
        if c.is_null(0) {
            return None;
        }
        c.as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(0).to_string())
    });

    let line = desc
        .column_by_name("line")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .filter(|a| !a.is_null(0))
        .map(|a| a.value(0));

    let system_name = desc.column_by_name("system_name").and_then(|c| {
        if c.is_null(0) {
            return None;
        }
        c.as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(0).to_string())
    });

    let total_value = data
        .column_by_name("total_value")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .filter(|a| !a.is_null(0))
        .map(|a| a.value(0))
        .unwrap_or(0);

    let self_value = data
        .column_by_name("self_value")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .filter(|a| !a.is_null(0))
        .map(|a| a.value(0))
        .unwrap_or(0);

    Some(FgNode {
        path_key: key.to_string(),
        function_name,
        depth,
        parent_path_key,
        total_value,
        self_value,
        filename,
        line,
        system_name,
    })
}

fn render_icicle_svg(nodes: &HashMap<String, FgNode>) -> String {
    // Use 100.0 as the coordinate width so all x/width values are natural percentages (0–100).
    // The SVG has no viewBox — horizontal coords are rendered as "N%" (scaling with container
    // width), vertical coords are pixels (fixed row heights and font sizes regardless of width).
    const SVG_W: f64 = 100.0;
    const ROW_H: f64 = 22.0;

    // Build children map
    let mut children_map: HashMap<Option<String>, Vec<&FgNode>> = HashMap::new();
    for node in nodes.values() {
        children_map
            .entry(node.parent_path_key.clone())
            .or_default()
            .push(node);
    }
    for kids in children_map.values_mut() {
        kids.sort_by(|a, b| b.total_value.cmp(&a.total_value));
    }

    let grand_total: i64 = children_map
        .get(&None)
        .map(|roots| roots.iter().map(|n| n.total_value).sum())
        .unwrap_or(0);

    if grand_total == 0 {
        return r#"<div class="flex items-center justify-center h-48 text-zinc-500">No profile data</div>"#.to_string();
    }

    let max_depth = nodes.values().map(|n| n.depth).max().unwrap_or(0);
    let svg_h = (max_depth + 1) as f64 * ROW_H + 4.0;

    let mut rects: Vec<String> = Vec::new();
    layout_nodes(&None, 0.0, 0.0, SVG_W, ROW_H, 0, &children_map, &mut rects);

    format!(
        r#"<svg id="fg-svg" xmlns="http://www.w3.org/2000/svg" width="100%" height="{svg_h:.0}" style="display:block;cursor:pointer" data-grand-total="{grand_total}">
{rects}
</svg>"#,
        svg_h = svg_h,
        grand_total = grand_total,
        rects = rects.join("\n"),
    )
}

#[allow(clippy::too_many_arguments)]
fn layout_nodes(
    parent_key: &Option<String>,
    x: f64,
    y: f64,
    width: f64,
    row_h: f64,
    parent_total: i64,
    children_map: &HashMap<Option<String>, Vec<&FgNode>>,
    rects: &mut Vec<String>,
) {
    let kids = match children_map.get(parent_key) {
        Some(k) if !k.is_empty() => k,
        _ => return,
    };

    let total_kid_val: i64 = kids.iter().map(|n| n.total_value).sum::<i64>().max(1);

    let mut cur_x = x;
    for kid in kids.iter() {
        let w = (kid.total_value as f64 / total_kid_val as f64) * width;
        if w < 0.05 {
            cur_x += w;
            continue;
        }

        // ratio = kid / parent (1.0 for root nodes, matching macOS ProfileColorScheme)
        let ratio = if parent_total > 0 {
            (kid.total_value as f64 / parent_total as f64).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let color = ratio_to_color(ratio);

        let text = if w > 2.4 {
            let max_chars = ((w * 10.0 - 6.0) / 6.5).floor() as usize;
            let label = truncate_label(&kid.function_name, max_chars.max(1));
            format!(
                r##"  <text x="{x:.2}%" y="{ty:.1}" dx="3" font-size="11" font-family="monospace" fill="#fff" dominant-baseline="middle" clip-path="url(#cp{id})">{label}</text>"##,
                x = cur_x,
                ty = y + row_h / 2.0,
                id = rects.len(),
                label = html_escape(&label),
            )
        } else {
            String::new()
        };

        let clip_id = rects.len();
        let data_file = html_escape(kid.filename.as_deref().unwrap_or(""));
        let data_line = kid.line.map(|l| l.to_string()).unwrap_or_default();
        let data_sys = html_escape(kid.system_name.as_deref().unwrap_or(""));
        let data_parent = html_escape(kid.parent_path_key.as_deref().unwrap_or(""));
        rects.push(format!(
            r##"<g class="fg-frame" data-key="{key}" data-fn="{fn_}" data-file="{file}" data-line="{line}" data-sys="{sys}" data-self="{sv}" data-total="{tv}" data-depth="{depth}" data-parent="{parent}" data-x="{dx:.4}" data-w="{dw:.4}">
  <clipPath id="cp{clip_id}"><rect x="{x:.2}%" y="{y:.1}" width="{w:.2}%" height="{rh:.1}"/></clipPath>
  <rect x="{x:.2}%" y="{y:.1}" width="{w:.2}%" height="{rh:.1}" fill="{color}" stroke="#111" stroke-width="0.5"/>
{text}</g>"##,
            key = html_escape(&kid.path_key),
            fn_ = html_escape(&kid.function_name),
            file = data_file,
            line = data_line,
            sys = data_sys,
            sv = kid.self_value,
            tv = kid.total_value,
            depth = kid.depth,
            parent = data_parent,
            dx = cur_x, dw = w,
            clip_id = clip_id,
            x = cur_x, y = y, w = w, rh = row_h - 1.0,
            color = color, text = text,
        ));

        layout_nodes(
            &Some(kid.path_key.clone()),
            cur_x,
            y + row_h,
            w,
            row_h,
            kid.total_value,
            children_map,
            rects,
        );

        cur_x += w;
    }
}

fn truncate_label(s: &str, max_chars: usize) -> String {
    if max_chars < 2 {
        return String::new();
    }
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let t: String = s.chars().take(max_chars - 1).collect();
    format!("{}…", t)
}

/// Colors a frame using the same scheme as the macOS app's ProfileColorScheme:
/// fixed blue hue (216°), saturation and brightness scaled by the ratio of
/// this node's value to its parent's value (1.0 for root nodes).
fn ratio_to_color(ratio: f64) -> String {
    let s_v = 0.05 + 0.95 * ratio; // HSV saturation
    let v = 0.3 + 0.7 * ratio; // HSV brightness/value

    // Convert HSV → HSL for CSS
    let l = v * (1.0 - s_v / 2.0);
    let s_l = if l == 0.0 || l == 1.0 {
        0.0
    } else {
        (v - l) / f64::min(l, 1.0 - l)
    };

    format!("hsl(216,{:.1}%,{:.1}%)", s_l * 100.0, l * 100.0)
}

fn duration_to_ns(dur: &str) -> u64 {
    let dur = dur.trim();
    let secs: u64 = if let Some(n) = dur.strip_suffix('s') {
        n.parse().unwrap_or(3600)
    } else if let Some(n) = dur.strip_suffix('m') {
        n.parse::<u64>().unwrap_or(60) * 60
    } else if let Some(n) = dur.strip_suffix('h') {
        n.parse::<u64>().unwrap_or(1) * 3600
    } else if let Some(n) = dur.strip_suffix('d') {
        n.parse::<u64>().unwrap_or(1) * 86400
    } else {
        3600
    };
    secs * 1_000_000_000
}

fn error_event(msg: &str) -> Event {
    Event::from(
        PatchElements::new(format!(
            r#"<div role="alert" class="m-2 p-3 bg-red-950 text-red-400 border-l-4 border-red-500 rounded text-sm">{}</div>"#,
            html_escape(msg)
        ))
        .selector("#flamegraph")
        .mode(ElementPatchMode::Inner),
    )
}
