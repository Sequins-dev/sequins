//! Logs tab — ListBox with expandable rows backed by TableStrategy ViewDeltaStream.
//!
//! Data flow:
//!   DataSource → query_live(seql) → TableStrategy → ViewDeltaStream
//!     → tokio task → sender.input(LogsInput::Delta(...))
//!     → update() → decode IPC → gio::ListStore
//!     → ListBox rows (expandable via GtkRevealer)

use crate::data::AppDataSource;
use crate::time_range::TimeRange;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::{
    Array, LargeBinaryArray, StringArray, StringViewArray, TimestampNanosecondArray, UInt8Array,
};
use arrow::array::{MapArray, StructArray};
use arrow::ipc::reader::StreamReader;
use chrono::{DateTime, TimeZone, Utc};
use gtk4::glib;
use gtk4::prelude::*;
use relm4::prelude::*;
use sequins_view::{TableStrategy, ViewDelta};
use std::sync::Arc;

// ── Row data type ──────────────────────────────────────────────────────────────

/// Plain-Rust representation of one log row, wrapped in `BoxedAnyObject`.
#[derive(Clone)]
pub struct LogRow {
    pub timestamp_ns: i64,
    pub service_name: String,
    pub severity_text: String,
    pub severity_number: u8,
    pub body: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub attributes: Vec<(String, String)>,
}

impl LogRow {
    pub fn timestamp_display(&self) -> String {
        let secs = self.timestamp_ns / 1_000_000_000;
        let nsecs = (self.timestamp_ns % 1_000_000_000).unsigned_abs() as u32;
        Utc.timestamp_opt(secs, nsecs)
            .single()
            .map(|dt: DateTime<Utc>| dt.format("%H:%M:%S%.3f").to_string())
            .unwrap_or_else(|| self.timestamp_ns.to_string())
    }

    pub fn severity_css_class(&self) -> &'static str {
        match self.severity_number {
            1..=4 => "log-trace",
            5..=8 => "log-debug",
            9..=12 => "log-info",
            13..=16 => "log-warn",
            17..=20 => "log-error",
            _ => "log-fatal",
        }
    }
}

impl std::fmt::Debug for LogRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogRow({})", self.timestamp_ns)
    }
}

// ── Component ──────────────────────────────────────────────────────────────────

pub struct LogsInit {
    pub data_source: Arc<AppDataSource>,
}

pub struct LogsModel {
    data_source: Arc<AppDataSource>,
    store: gtk4::gio::ListStore,
    search_text: String,
    search_debounce_id: u64,
    service_filter: Option<String>,
    time_range: TimeRange,
    is_live: bool,
    selected_severities: BTreeSet<u8>, // empty = all; stores min of each range (1,5,9,13,17,21)
    sort_newest_first: bool,
    stream_task: Option<tokio::task::JoinHandle<()>>,
    /// True when the scroll view should track the active edge (top or bottom).
    snap_to_edge: bool,
    /// Incremented each time snap is disabled; SnapEngage msgs with stale ids are ignored.
    snap_engage_id: u64,
    /// Set true before programmatic set_value() calls so the value-changed signal ignores them.
    programmatic_scroll: Arc<AtomicBool>,
    /// After a sort direction flip with snap off, holds the fraction (0..1) to restore inverted.
    pending_invert_fraction: std::cell::Cell<Option<f64>>,
}

#[derive(Debug)]
pub enum LogsInput {
    /// Pre-decoded batch of rows from the background stream task.
    RowsBatch(Vec<LogRow>),
    /// Number of oldest rows to expire.
    Expired(u64),
    /// Stream has delivered the initial snapshot and is now live.
    StreamReady,
    SearchChanged(String),
    /// Fired 300ms after the last SearchChanged to actually restart the stream.
    SearchCommit(u64),
    ServiceFilter(Option<String>),
    SetTimeRange(TimeRange, bool),
    StreamError(String),
    SeverityToggled(u8), // min of severity range (1=Trace,5=Debug,9=Info,13=Warn,17=Error,21=Fatal)
    SortToggled {
        scroll_value: f64,
        scroll_upper: f64,
        scroll_page: f64,
    },
    /// Fired by the scroll adjustment's value-changed signal (user-initiated only).
    UserScrolled {
        value: f64,
        upper: f64,
        page: f64,
    },
    /// Fired after the snap-re-engage delay; id must match current snap_engage_id to activate.
    SnapEngage(u64),
}

pub struct LogsWidgets {
    root: gtk4::Box,
    search_entry: gtk4::SearchEntry,
    severity_toggles: std::collections::HashMap<u8, gtk4::ToggleButton>,
    sort_button: gtk4::ToggleButton,
    scroll: gtk4::ScrolledWindow,
}

impl Component for LogsModel {
    type CommandOutput = ();
    type Init = LogsInit;
    type Input = LogsInput;
    type Output = ();
    type Root = gtk4::Box;
    type Widgets = LogsWidgets;

    fn init_root() -> Self::Root {
        gtk4::Box::new(gtk4::Orientation::Vertical, 0)
    }

    fn init(
        init: Self::Init,
        root: Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        // ── List store ───────────────────────────────────────────────────────
        let store = gtk4::gio::ListStore::new::<glib::BoxedAnyObject>();

        // ── Compact filter bar ───────────────────────────────────────────────
        let search_entry = gtk4::SearchEntry::builder()
            .placeholder_text("Filter logs…")
            .width_request(200)
            .build();

        // Severity toggle buttons: label, severity_min, css_class
        let severity_defs: &[(&str, u8, &str)] = &[
            ("Trace", 1, "log-trace"),
            ("Debug", 5, "log-debug"),
            ("Info", 9, "log-info"),
            ("Warn", 13, "log-warn"),
            ("Error", 17, "log-error"),
            ("Fatal", 21, "log-fatal"),
        ];

        let mut severity_toggles: std::collections::HashMap<u8, gtk4::ToggleButton> =
            std::collections::HashMap::new();
        for &(label, min, _css) in severity_defs {
            let btn = gtk4::ToggleButton::builder()
                .label(label)
                .css_classes(["flat"])
                .build();
            let s = sender.clone();
            btn.connect_toggled(move |_| {
                s.input(LogsInput::SeverityToggled(min));
            });
            severity_toggles.insert(min, btn);
        }

        let sort_button = gtk4::ToggleButton::builder()
            .label("↓")
            .active(true)
            .css_classes(["flat"])
            .build();
        let toolbar = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(4)
            .margin_start(8)
            .margin_end(8)
            .build();
        toolbar.append(&search_entry);
        for &(_label, min, _css) in severity_defs {
            toolbar.append(severity_toggles.get(&min).unwrap());
        }
        toolbar.append(&sort_button);

        // ── Virtualized ListView backed by store ─────────────────────────────
        // SignalListItemFactory only creates widgets for visible rows (~20-30),
        // avoiding the O(n) widget creation cost of ListBox.bind_model.
        let factory = gtk4::SignalListItemFactory::new();
        factory.connect_setup(|_, obj| {
            let item = obj.downcast_ref::<gtk4::ListItem>().unwrap();
            item.set_child(Some(&build_log_row_widget()));
        });
        factory.connect_bind(|_, obj| {
            let item = obj.downcast_ref::<gtk4::ListItem>().unwrap();
            let boxed = item.item().and_downcast::<glib::BoxedAnyObject>().unwrap();
            let row: std::cell::Ref<LogRow> = boxed.borrow();
            let widget = item.child().and_downcast::<gtk4::Box>().unwrap();
            bind_log_row_widget(&widget, &row);
        });
        factory.connect_unbind(|_, obj| {
            let item = obj.downcast_ref::<gtk4::ListItem>().unwrap();
            let widget = item.child().and_downcast::<gtk4::Box>().unwrap();
            reset_log_row_widget(&widget);
        });

        let selection = gtk4::NoSelection::new(Some(store.clone()));
        let list_view = gtk4::ListView::builder()
            .model(&selection)
            .factory(&factory)
            .show_separators(true)
            .vexpand(true)
            .build();

        let scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&list_view)
            .build();

        // ── Sort button: capture scroll position at click time ────────────────
        {
            let s = sender.clone();
            let sc = scroll.clone();
            sort_button.connect_toggled(move |_| {
                let adj = sc.vadjustment();
                s.input(LogsInput::SortToggled {
                    scroll_value: adj.value(),
                    scroll_upper: adj.upper(),
                    scroll_page: adj.page_size(),
                });
            });
        }

        // ── Snap-to-edge: detect user scroll vs programmatic scroll ──────────
        let programmatic_scroll = Arc::new(AtomicBool::new(false));
        {
            let s = sender.clone();
            let prog = programmatic_scroll.clone();
            scroll.vadjustment().connect_value_changed(move |adj| {
                if prog.load(Ordering::Relaxed) {
                    return;
                }
                s.input(LogsInput::UserScrolled {
                    value: adj.value(),
                    upper: adj.upper(),
                    page: adj.page_size(),
                });
            });
        }

        // ── Keyboard shortcut: Ctrl+F focuses search ─────────────────────────
        {
            let se = search_entry.clone();
            let key_ctrl = gtk4::EventControllerKey::new();
            key_ctrl.connect_key_pressed(move |_, key, _, mods| {
                if key == gtk4::gdk::Key::f && mods.contains(gtk4::gdk::ModifierType::CONTROL_MASK)
                {
                    se.grab_focus();
                    return glib::Propagation::Stop;
                }
                glib::Propagation::Proceed
            });
            root.add_controller(key_ctrl);
        }

        toolbar.set_widget_name("tab-toolbar");
        root.append(&toolbar);
        root.append(&scroll);

        // ── Search debounce ─────────────────────────────────────────────────
        {
            let s = sender.clone();
            search_entry.connect_search_changed(move |entry| {
                s.input(LogsInput::SearchChanged(entry.text().to_string()));
            });
        }

        let mut model = LogsModel {
            data_source: init.data_source.clone(),
            store,
            search_text: String::new(),
            search_debounce_id: 0,
            service_filter: None,
            time_range: TimeRange::default(),
            is_live: true,
            selected_severities: BTreeSet::new(),
            sort_newest_first: true,
            stream_task: None,
            snap_to_edge: true,
            snap_engage_id: 0,
            programmatic_scroll,
            pending_invert_fraction: std::cell::Cell::new(None),
        };

        // ── Start the live logs stream ────────────────────────────────────────
        start_stream(&mut model, sender.clone());

        ComponentParts {
            model,
            widgets: LogsWidgets {
                root,
                search_entry,
                severity_toggles,
                sort_button,
                scroll,
            },
        }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            LogsInput::RowsBatch(rows) => {
                let mut boxed: Vec<glib::BoxedAnyObject> =
                    rows.into_iter().map(glib::BoxedAnyObject::new).collect();
                if self.sort_newest_first {
                    boxed.reverse(); // newest within batch at index 0
                    self.store.splice(0, 0, &boxed);
                } else {
                    let pos = self.store.n_items();
                    self.store.splice(pos, 0, &boxed);
                }
                // Keep the store bounded: if over 500 rows, drop the oldest.
                const MAX_ROWS: u32 = 500;
                let current = self.store.n_items();
                if current > MAX_ROWS {
                    let excess = current - MAX_ROWS;
                    if self.sort_newest_first {
                        // oldest are at the end
                        self.store
                            .splice(MAX_ROWS, excess, &[] as &[glib::BoxedAnyObject]);
                    } else {
                        // oldest are at the beginning
                        self.store.splice(0, excess, &[] as &[glib::BoxedAnyObject]);
                    }
                }
            }
            LogsInput::Expired(count) => {
                let n = count.min(self.store.n_items() as u64) as u32;
                if self.sort_newest_first {
                    // oldest are at the end when newest-first
                    let current = self.store.n_items();
                    let start = current.saturating_sub(n);
                    self.store.splice(start, n, &[] as &[glib::BoxedAnyObject]);
                } else {
                    self.store.splice(0, n, &[] as &[glib::BoxedAnyObject]);
                }
            }
            LogsInput::StreamReady => {}
            LogsInput::SearchChanged(text) => {
                if text != self.search_text {
                    self.search_text = text;
                    // Abort current stream immediately so stale results stop appearing.
                    if let Some(h) = self.stream_task.take() {
                        h.abort();
                    }
                    self.store.remove_all();
                    // Debounce: schedule restart after 300ms of typing silence.
                    self.search_debounce_id = self.search_debounce_id.wrapping_add(1);
                    let id = self.search_debounce_id;
                    let s = sender.input_sender().clone();
                    glib::timeout_add_local(std::time::Duration::from_millis(300), move || {
                        s.send(LogsInput::SearchCommit(id)).ok();
                        glib::ControlFlow::Break
                    });
                }
            }
            LogsInput::SearchCommit(id) => {
                if id == self.search_debounce_id {
                    start_stream(self, sender);
                }
            }
            LogsInput::ServiceFilter(filter) => {
                if filter != self.service_filter {
                    self.service_filter = filter;
                    self.snap_to_edge = true;
                    if let Some(h) = self.stream_task.take() {
                        h.abort();
                    }
                    self.store.remove_all();
                    start_stream(self, sender);
                }
            }
            LogsInput::SetTimeRange(range, live) => {
                if range != self.time_range || live != self.is_live {
                    self.time_range = range;
                    self.is_live = live;
                    self.snap_to_edge = true;
                    if let Some(h) = self.stream_task.take() {
                        h.abort();
                    }
                    self.store.remove_all();
                    start_stream(self, sender);
                }
            }
            LogsInput::StreamError(e) => {
                tracing::error!("Logs stream error: {}", e);
            }
            LogsInput::SeverityToggled(min) => {
                if self.selected_severities.contains(&min) {
                    self.selected_severities.remove(&min);
                } else {
                    self.selected_severities.insert(min);
                }
                if let Some(h) = self.stream_task.take() {
                    h.abort();
                }
                self.store.remove_all();
                start_stream(self, sender);
            }
            LogsInput::SortToggled {
                scroll_value,
                scroll_upper,
                scroll_page,
            } => {
                self.sort_newest_first = !self.sort_newest_first;
                if self.snap_to_edge {
                    // Already at the active edge; snap will move to the new edge.
                    self.snap_to_edge = true;
                    self.pending_invert_fraction.set(None);
                } else {
                    // Preserve relative position by inverting the scroll fraction.
                    let max = (scroll_upper - scroll_page).max(1.0);
                    let fraction = (scroll_value / max).clamp(0.0, 1.0);
                    self.pending_invert_fraction.set(Some(fraction));
                    self.snap_to_edge = false;
                }
                if let Some(h) = self.stream_task.take() {
                    h.abort();
                }
                self.store.remove_all();
                start_stream(self, sender);
            }
            LogsInput::UserScrolled { value, upper, page } => {
                if !self.is_live {
                    return;
                }
                // ~5 rows at ~40px each
                const SNAP_THRESHOLD_PX: f64 = 200.0;
                let dist = if self.sort_newest_first {
                    value // distance from top
                } else {
                    (upper - page - value).max(0.0) // distance from bottom
                };
                if dist < SNAP_THRESHOLD_PX {
                    // Near the active edge: schedule re-engage after a delay.
                    // The delay prevents accidental snapping when browsing near the edge.
                    let id = self.snap_engage_id.wrapping_add(1);
                    self.snap_engage_id = id;
                    let s = sender.input_sender().clone();
                    glib::timeout_add_local_once(Duration::from_millis(1200), move || {
                        s.send(LogsInput::SnapEngage(id)).ok();
                    });
                } else {
                    // Far from edge: disable snap immediately and invalidate pending re-engage.
                    self.snap_to_edge = false;
                    self.snap_engage_id = self.snap_engage_id.wrapping_add(1);
                }
            }
            LogsInput::SnapEngage(id) => {
                if id == self.snap_engage_id {
                    self.snap_to_edge = true;
                }
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, _sender: ComponentSender<Self>) {
        // Update direction arrow: ↓ = newest at top (pushes down), ↑ = newest at bottom (pushes up)
        let arrow = if self.sort_newest_first { "↓" } else { "↑" };
        widgets.sort_button.set_label(arrow);

        let adj = widgets.scroll.vadjustment();
        let max = (adj.upper() - adj.page_size()).max(0.0);

        // Apply inverted scroll position after a direction flip (only once content has loaded).
        if let Some(fraction) = self.pending_invert_fraction.get() {
            if max > 0.0 {
                let target = (1.0 - fraction) * max;
                self.programmatic_scroll.store(true, Ordering::Relaxed);
                adj.set_value(target);
                let flag = self.programmatic_scroll.clone();
                glib::idle_add_local_once(move || flag.store(false, Ordering::Relaxed));
                self.pending_invert_fraction.set(None);
            }
        } else if self.is_live && self.snap_to_edge {
            // Scroll to active edge when snap is engaged in live mode.
            let target = if self.sort_newest_first { 0.0 } else { max };
            self.programmatic_scroll.store(true, Ordering::Relaxed);
            adj.set_value(target);
            let flag = self.programmatic_scroll.clone();
            glib::idle_add_local_once(move || flag.store(false, Ordering::Relaxed));
        }
    }
}

// ── Stream launcher ───────────────────────────────────────────────────────────

fn start_stream(model: &mut LogsModel, sender: ComponentSender<LogsModel>) {
    let ds = model.data_source.clone();
    let search = model.search_text.clone();
    let service = model.service_filter.clone();
    let range = model.time_range;
    let is_live = model.is_live;
    let severities = model.selected_severities.clone();
    let s = sender.input_sender().clone();
    let query = build_query(&search, service.as_deref(), range, &severities);

    // Decode IPC and batch rows on the tokio thread pool; send pre-decoded
    // batches to the GTK thread at most every 200ms to keep the UI responsive.
    let handle = relm4::spawn(async move {
        let strategy = TableStrategy::new();
        match ds.live_view(&query, &strategy).await {
            Ok(stream) => {
                use futures::StreamExt;
                use tokio::time::{interval, Duration, MissedTickBehavior};
                futures::pin_mut!(stream);

                let mut pending: Vec<LogRow> = Vec::new();
                let mut pending_expired: u64 = 0;
                let mut flush = interval(Duration::from_millis(200));
                flush.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        biased;
                        delta = stream.next() => match delta {
                            Some(ViewDelta::RowsAppended { table: None, ipc }) => {
                                if let Ok(batch) = decode_ipc(&ipc) {
                                    pending.extend(extract_log_rows(&batch));
                                }
                            }
                            Some(ViewDelta::RowsExpired { table: None, expired_count }) => {
                                pending_expired += expired_count;
                            }
                            Some(ViewDelta::Ready) => {
                                if !pending.is_empty() {
                                    let _ = s.send(LogsInput::RowsBatch(std::mem::take(&mut pending)));
                                }
                                if pending_expired > 0 {
                                    let _ = s.send(LogsInput::Expired(std::mem::replace(&mut pending_expired, 0)));
                                }
                                let _ = s.send(LogsInput::StreamReady);
                                if !is_live {
                                    break;
                                }
                            }
                            Some(ViewDelta::Error { message }) => {
                                let _ = s.send(LogsInput::StreamError(message));
                            }
                            Some(_) => {}
                            None => {
                                if !pending.is_empty() {
                                    let _ = s.send(LogsInput::RowsBatch(std::mem::take(&mut pending)));
                                }
                                break;
                            }
                        },
                        _ = flush.tick() => {
                            if !pending.is_empty() {
                                let _ = s.send(LogsInput::RowsBatch(std::mem::take(&mut pending)));
                            }
                            if pending_expired > 0 {
                                let _ = s.send(LogsInput::Expired(std::mem::replace(&mut pending_expired, 0)));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let _ = s.send(LogsInput::StreamError(format!("{:#}", e)));
            }
        }
    });
    model.stream_task = Some(handle);
}

fn build_query(
    search: &str,
    service_filter: Option<&str>,
    range: TimeRange,
    selected_severities: &BTreeSet<u8>,
) -> String {
    let mut parts = vec![format!("logs {}", range.seql_window())];
    if let Some(svc) = service_filter {
        let esc = svc.replace('\'', "''");
        parts.push(format!("where service_name = '{esc}'"));
    }
    if !selected_severities.is_empty() {
        let nums: Vec<String> = selected_severities
            .iter()
            .flat_map(|&min| (min..min + 4).map(|n| n.to_string()))
            .collect();
        parts.push(format!("where severity_number in [{}]", nums.join(", ")));
    }
    if !search.is_empty() {
        let esc = search.replace('\'', "''");
        parts.push(format!("where body contains '{esc}'"));
    }
    parts.join(" | ")
}

// ── Overflow attribute decode ─────────────────────────────────────────────────

/// Decode all entries from a `_overflow_attrs: Map<Utf8, LargeBinary>` cell.
/// Values are CBOR-encoded; we decode them to their string representation.
fn extract_overflow_attrs(map_array: &MapArray, row: usize) -> Vec<(String, String)> {
    if map_array.is_null(row) {
        return vec![];
    }
    let entries = map_array.value(row);
    let struct_entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
    let keys = struct_entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let vals = struct_entries
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();

    let mut out = Vec::new();
    for i in 0..keys.len() {
        if vals.is_null(i) {
            continue;
        }
        let key = keys.value(i).to_string();
        let bytes = vals.value(i);
        let value = cbor_decode_display(bytes);
        if !value.is_empty() {
            out.push((key, value));
        }
    }
    out
}

/// Decode CBOR bytes to a human-readable string for display.
fn cbor_decode_display(bytes: &[u8]) -> String {
    use std::io::Cursor;
    let mut cursor = Cursor::new(bytes);
    match ciborium::from_reader::<ciborium::Value, _>(&mut cursor) {
        Ok(ciborium::Value::Text(s)) => s,
        Ok(ciborium::Value::Integer(n)) => {
            let n: i128 = n.into();
            n.to_string()
        }
        Ok(ciborium::Value::Float(f)) => f.to_string(),
        Ok(ciborium::Value::Bool(b)) => b.to_string(),
        Ok(ciborium::Value::Bytes(b)) => format!("<{} bytes>", b.len()),
        Ok(ciborium::Value::Array(arr)) => format!("[{} items]", arr.len()),
        Ok(ciborium::Value::Map(m)) => format!("{{{} fields}}", m.len()),
        Ok(other) => format!("{other:?}"),
        Err(_) => String::new(),
    }
}

// ── Arrow IPC decode ──────────────────────────────────────────────────────────

fn decode_ipc(ipc: &[u8]) -> Result<arrow::record_batch::RecordBatch, String> {
    use std::io::Cursor;
    let cursor = Cursor::new(ipc);
    let mut reader =
        StreamReader::try_new(cursor, None).map_err(|e| format!("IPC reader error: {e}"))?;
    reader
        .next()
        .ok_or_else(|| "Empty IPC stream".to_string())?
        .map_err(|e| format!("IPC read error: {e}"))
}

fn extract_log_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<LogRow> {
    let schema = batch.schema();
    let n = batch.num_rows();

    macro_rules! col_string_view {
        ($name:expr) => {
            schema
                .index_of($name)
                .ok()
                .and_then(|i| batch.column(i).as_any().downcast_ref::<StringViewArray>())
        };
    }

    let time_col = schema.index_of("time_unix_nano").ok().and_then(|i| {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
    });
    let service_col = col_string_view!("service_name");
    let severity_text_col = col_string_view!("severity_text");
    let severity_num_col = schema
        .index_of("severity_number")
        .ok()
        .and_then(|i| batch.column(i).as_any().downcast_ref::<UInt8Array>());
    let body_col = col_string_view!("body");
    let trace_col = col_string_view!("trace_id");
    let span_col = col_string_view!("span_id");

    // Core log column names — everything else is a promoted attribute.
    const CORE_COLS: &[&str] = &[
        "log_id",
        "time_unix_nano",
        "observed_time_unix_nano",
        "service_name",
        "severity_text",
        "severity_number",
        "body",
        "trace_id",
        "span_id",
        "resource_id",
        "scope_id",
        "_overflow_attrs",
    ];
    let attr_cols: Vec<(String, usize)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| !CORE_COLS.contains(&f.name().as_str()))
        .map(|(idx, f)| (f.name().clone(), idx))
        .collect();

    let overflow_col = schema
        .index_of("_overflow_attrs")
        .ok()
        .and_then(|i| batch.column(i).as_any().downcast_ref::<MapArray>());

    (0..n)
        .map(|i| {
            let mut attributes: Vec<(String, String)> = attr_cols
                .iter()
                .filter_map(|(key, col_idx)| {
                    let col = batch.column(*col_idx);
                    if col.is_null(i) {
                        return None;
                    }
                    let val = col
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .map(|c| c.value(i).to_string())
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<StringArray>()
                                .map(|c| c.value(i).to_string())
                        })
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<arrow::array::Int64Array>()
                                .map(|c| c.value(i).to_string())
                        })
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<arrow::array::Float64Array>()
                                .map(|c| c.value(i).to_string())
                        })
                        .or_else(|| {
                            col.as_any()
                                .downcast_ref::<arrow::array::BooleanArray>()
                                .map(|c| c.value(i).to_string())
                        })?;
                    if val.is_empty() {
                        None
                    } else {
                        Some((key.clone(), val))
                    }
                })
                .collect();
            if let Some(map) = overflow_col {
                attributes.extend(extract_overflow_attrs(map, i));
            }
            attributes.sort_by(|a, b| a.0.cmp(&b.0));

            LogRow {
                timestamp_ns: time_col.map(|c| c.value(i)).unwrap_or(0),
                service_name: service_col
                    .map(|c| c.value(i).to_string())
                    .unwrap_or_default(),
                severity_text: severity_text_col
                    .map(|c| c.value(i).to_string())
                    .unwrap_or_else(|| "INFO".to_string()),
                severity_number: severity_num_col.map(|c| c.value(i)).unwrap_or(9),
                body: body_col.map(|c| c.value(i).to_string()).unwrap_or_default(),
                trace_id: trace_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                }),
                span_id: span_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                }),
                attributes,
            }
        })
        .collect()
}

// ── Row widget builders ────────────────────────────────────────────────────────

/// Build the empty widget structure for a log row (called once per recycled slot).
/// Widget tree:
///   container (Box/vertical)
///     main_line (Box/horizontal): time_label, sev_label, body_label, service_label, chevron
///     revealer
fn build_log_row_widget() -> gtk4::Box {
    let container = gtk4::Box::new(gtk4::Orientation::Vertical, 0);

    let main_line = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Horizontal)
        .spacing(4)
        .margin_start(4)
        .margin_end(4)
        .margin_top(4)
        .margin_bottom(4)
        .build();

    let time_label = gtk4::Label::builder()
        .label("")
        .css_classes(["caption", "dim-label"])
        .width_request(80)
        .halign(gtk4::Align::Start)
        .build();

    let sev_label = gtk4::Label::builder()
        .label("")
        .css_classes(["caption"])
        .width_request(60)
        .halign(gtk4::Align::Start)
        .build();

    let body_label = gtk4::Label::builder()
        .label("")
        .halign(gtk4::Align::Start)
        .hexpand(true)
        .ellipsize(gtk4::pango::EllipsizeMode::End)
        .xalign(0.0)
        .build();

    let service_label = gtk4::Label::builder()
        .label("")
        .css_classes(["caption", "dim-label"])
        .width_request(120)
        .halign(gtk4::Align::Start)
        .build();

    // Chevron is always present but hidden when there are no attributes.
    let chevron = gtk4::Label::builder()
        .label("▶")
        .css_classes(["caption", "dim-label"])
        .margin_start(4)
        .visible(false)
        .build();

    main_line.append(&time_label);
    main_line.append(&sev_label);
    main_line.append(&body_label);
    main_line.append(&service_label);
    main_line.append(&chevron);

    let revealer = gtk4::Revealer::builder()
        .reveal_child(false)
        .transition_type(gtk4::RevealerTransitionType::SlideDown)
        .transition_duration(150)
        .build();

    // GestureClick is wired once; it checks chevron visibility so it's a no-op
    // when there are no attributes for the currently bound row.
    let click = gtk4::GestureClick::new();
    let r = revealer.clone();
    let c = chevron.clone();
    let bl = body_label.clone();
    click.connect_released(move |_, _, _, _| {
        if !c.is_visible() {
            return;
        }
        let expanding = !r.reveals_child();
        r.set_reveal_child(expanding);
        c.set_label(if expanding { "▼" } else { "▶" });
        // Match macOS: show full body text when expanded, truncate when collapsed.
        bl.set_ellipsize(if expanding {
            gtk4::pango::EllipsizeMode::None
        } else {
            gtk4::pango::EllipsizeMode::End
        });
        bl.set_lines(if expanding { -1 } else { 1 });
        bl.set_wrap(expanding);
    });
    container.add_controller(click);

    container.append(&main_line);
    container.append(&revealer);
    container
}

/// Fill a recycled log row widget with data from `row`.
fn bind_log_row_widget(container: &gtk4::Box, row: &LogRow) {
    // Navigate widget tree (see build_log_row_widget for structure)
    let main_line = container.first_child().and_downcast::<gtk4::Box>().unwrap();
    let revealer = main_line
        .next_sibling()
        .and_downcast::<gtk4::Revealer>()
        .unwrap();

    let time_label = main_line
        .first_child()
        .and_downcast::<gtk4::Label>()
        .unwrap();
    let sev_label = time_label
        .next_sibling()
        .and_downcast::<gtk4::Label>()
        .unwrap();
    let body_label = sev_label
        .next_sibling()
        .and_downcast::<gtk4::Label>()
        .unwrap();
    let service_label = body_label
        .next_sibling()
        .and_downcast::<gtk4::Label>()
        .unwrap();
    let chevron = service_label
        .next_sibling()
        .and_downcast::<gtk4::Label>()
        .unwrap();

    time_label.set_label(&row.timestamp_display());
    sev_label.set_label(&row.severity_text);
    sev_label.set_css_classes(&["caption", row.severity_css_class()]);
    body_label.set_label(&row.body);
    service_label.set_label(&row.service_name);

    // Reset reveal state for reused widgets
    revealer.set_reveal_child(false);
    chevron.set_label("▶");

    if row.attributes.is_empty() {
        chevron.set_visible(false);
        revealer.set_child(gtk4::Widget::NONE);
    } else {
        chevron.set_visible(true);
        revealer.set_child(Some(&build_attr_grid(&row.attributes)));
    }
}

/// Reset a recycled row widget when it scrolls off screen.
fn reset_log_row_widget(container: &gtk4::Box) {
    let main_line = container.first_child().and_downcast::<gtk4::Box>().unwrap();
    let revealer = main_line
        .next_sibling()
        .and_downcast::<gtk4::Revealer>()
        .unwrap();
    let body_label = main_line
        .first_child()
        .and_then(|c| c.next_sibling())
        .and_then(|c| c.next_sibling())
        .and_downcast::<gtk4::Label>()
        .unwrap();
    let chevron = body_label
        .next_sibling()
        .and_then(|c| c.next_sibling())
        .and_downcast::<gtk4::Label>()
        .unwrap();

    revealer.set_reveal_child(false);
    revealer.set_child(gtk4::Widget::NONE);
    chevron.set_visible(false);
    chevron.set_label("▶");
    body_label.set_ellipsize(gtk4::pango::EllipsizeMode::End);
    body_label.set_lines(1);
    body_label.set_wrap(false);
}

fn build_attr_grid(attrs: &[(String, String)]) -> gtk4::Box {
    let grid_box = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .spacing(2)
        .margin_start(40)
        .margin_end(8)
        .margin_top(4)
        .margin_bottom(6)
        .build();

    grid_box.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));

    for (key, value) in attrs {
        let row = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(8)
            .build();

        let key_label = gtk4::Label::builder()
            .label(key)
            .css_classes(["caption", "log-attr-key"])
            .width_request(160)
            .halign(gtk4::Align::End)
            .xalign(1.0)
            .build();

        let val_label = gtk4::Label::builder()
            .label(value)
            .css_classes(["caption", "log-attr-value"])
            .halign(gtk4::Align::Start)
            .hexpand(true)
            .selectable(true)
            .wrap(true)
            .xalign(0.0)
            .build();

        row.append(&key_label);
        row.append(&val_label);
        grid_box.append(&row);
    }

    grid_box
}
