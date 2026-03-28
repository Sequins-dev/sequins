//! Explore tab — freeform SeQL query editor with snapshot results.
//!
//! Provides a `GtkTextView` for SeQL input and a Run button. Results are always
//! snapshot (one-shot); the query text includes any time window expressions.
//! Columns are created dynamically from the Arrow schema of the first batch.

use crate::data::AppDataSource;
use arrow::array::ArrayRef;
use arrow::ipc::reader::StreamReader;
use gtk4::glib;
use gtk4::prelude::*;
use relm4::prelude::*;
use sequins_view::ViewDelta;
use std::sync::Arc;

// ── Component ──────────────────────────────────────────────────────────────────

pub struct ExploreInit {
    pub data_source: Arc<AppDataSource>,
}

pub struct ExploreModel {
    data_source: Arc<AppDataSource>,
    query: String,
    row_count: usize,
    status: String,
    store: gtk4::gio::ListStore,
    /// Column names from the first batch schema
    column_names: Vec<String>,
    /// Handle for the running snapshot task — abort on re-run.
    stream_task: Option<tokio::task::JoinHandle<()>>,
}

/// A single result row: one `String` value per column (pre-formatted).
#[derive(Clone)]
struct ResultRow(Vec<String>);

#[derive(Debug)]
pub enum ExploreInput {
    QueryChanged(String),
    RunQuery,
    Delta(ViewDelta),
    StreamError(String),
}

pub struct ExploreWidgets {
    root: gtk4::Box,
    row_count_label: gtk4::Label,
    status_label: gtk4::Label,
    column_view: gtk4::ColumnView,
    /// Column view columns we've created (one per result schema column)
    columns_created: bool,
}

impl Component for ExploreModel {
    type CommandOutput = ();
    type Init = ExploreInit;
    type Input = ExploreInput;
    type Output = ();
    type Root = gtk4::Box;
    type Widgets = ExploreWidgets;

    fn init_root() -> Self::Root {
        gtk4::Box::new(gtk4::Orientation::Vertical, 0)
    }

    fn init(
        init: Self::Init,
        root: Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        let store = gtk4::gio::ListStore::new::<glib::BoxedAnyObject>();

        // ── Editor toolbar ───────────────────────────────────────────────────
        let run_button = gtk4::Button::builder()
            .label("Run")
            .css_classes(["suggested-action"])
            .build();

        let status_label = gtk4::Label::builder()
            .label("Ready")
            .css_classes(["caption", "dim-label"])
            .margin_start(4)
            .build();

        let row_count_label = gtk4::Label::builder()
            .label("")
            .css_classes(["caption", "dim-label"])
            .hexpand(true)
            .halign(gtk4::Align::End)
            .build();

        let toolbar = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(4)
            .margin_start(8)
            .margin_end(8)
            .build();
        toolbar.append(&run_button);
        toolbar.append(&status_label);
        toolbar.append(&row_count_label);

        // ── SeQL editor ──────────────────────────────────────────────────────
        let text_buffer = gtk4::TextBuffer::new(None);
        text_buffer.set_text("logs last 15m | sort time_unix_nano desc | take 500");

        let text_view = gtk4::TextView::builder()
            .buffer(&text_buffer)
            .monospace(true)
            .top_margin(8)
            .bottom_margin(8)
            .left_margin(12)
            .right_margin(12)
            .build();

        let editor_scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Automatic)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .min_content_height(80)
            .max_content_height(200)
            .child(&text_view)
            .build();

        // ── Result table ─────────────────────────────────────────────────────
        let selection = gtk4::NoSelection::new(Some(store.clone()));
        let column_view = gtk4::ColumnView::builder()
            .model(&selection)
            .reorderable(true)
            .show_row_separators(true)
            .vexpand(true)
            .build();

        let result_scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Automatic)
            .vscrollbar_policy(gtk4::PolicyType::Automatic)
            .vexpand(true)
            .child(&column_view)
            .build();

        // ── Wire up events ───────────────────────────────────────────────────
        {
            let s = sender.clone();
            text_buffer.connect_changed(move |buf| {
                let text = buf.text(&buf.start_iter(), &buf.end_iter(), false);
                s.input(ExploreInput::QueryChanged(text.to_string()));
            });
        }

        {
            let s = sender.clone();
            run_button.connect_clicked(move |_| {
                s.input(ExploreInput::RunQuery);
            });
        }

        toolbar.set_widget_name("tab-toolbar");
        root.append(&toolbar);
        root.append(&editor_scroll);
        root.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        root.append(&result_scroll);

        let default_query = "logs last 15m | sort time_unix_nano desc | take 500".to_string();

        let mut model = ExploreModel {
            data_source: init.data_source.clone(),
            query: default_query.clone(),
            row_count: 0,
            status: "Ready".to_string(),
            store,
            column_names: Vec::new(),
            stream_task: None,
        };

        // Run the default query on open
        start_snapshot(&mut model, sender.clone());

        let _ = text_view; // kept alive by editor_scroll which is in root

        ComponentParts {
            model,
            widgets: ExploreWidgets {
                root,
                row_count_label,
                status_label,
                column_view,
                columns_created: false,
            },
        }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            ExploreInput::QueryChanged(q) => {
                self.query = q;
                // Debounce: cancel pending debounce task (simple flag approach)
                // Actual debounce handled in update_view via timer — here we just store query.
            }
            ExploreInput::RunQuery => {
                if let Some(h) = self.stream_task.take() {
                    h.abort();
                }
                self.store.remove_all();
                self.row_count = 0;
                self.column_names.clear();
                self.status = "Running…".to_string();
                start_snapshot(self, sender);
            }
            ExploreInput::Delta(delta) => self.apply_delta(delta),
            ExploreInput::StreamError(e) => {
                tracing::error!("Explore stream error: {}", e);
                self.status = format!("Error: {e}");
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, _sender: ComponentSender<Self>) {
        widgets.status_label.set_text(&self.status);
        if self.row_count > 0 {
            widgets
                .row_count_label
                .set_text(&format!("{} rows", self.row_count));
        } else {
            widgets.row_count_label.set_text("");
        }

        // Create columns if schema just arrived and they haven't been created yet
        if !self.column_names.is_empty() && !widgets.columns_created {
            // Remove any existing columns
            while let Some(col) = widgets.column_view.columns().item(0) {
                widgets.column_view.remove_column(
                    col.downcast_ref::<gtk4::ColumnViewColumn>()
                        .expect("column view column"),
                );
            }

            for (col_idx, col_name) in self.column_names.iter().enumerate() {
                let factory = gtk4::SignalListItemFactory::new();
                let idx = col_idx;

                factory.connect_setup(|_, obj| {
                    let list_item = obj.downcast_ref::<gtk4::ListItem>().unwrap();
                    list_item.set_child(Some(
                        &gtk4::Label::builder()
                            .halign(gtk4::Align::Start)
                            .ellipsize(gtk4::pango::EllipsizeMode::End)
                            .xalign(0.0)
                            .build(),
                    ));
                });

                factory.connect_bind(move |_, obj| {
                    let list_item = obj.downcast_ref::<gtk4::ListItem>().unwrap();
                    let Some(item) = list_item.item() else { return };
                    let Ok(boxed) = item.downcast::<glib::BoxedAnyObject>() else {
                        return;
                    };
                    let row: std::cell::Ref<ResultRow> = boxed.borrow();
                    if let Some(label) = list_item.child().and_downcast::<gtk4::Label>() {
                        let text: &str = row.0.get(idx).map(String::as_str).unwrap_or("");
                        label.set_text(text);
                    }
                });

                factory.connect_unbind(|_, obj| {
                    let list_item = obj.downcast_ref::<gtk4::ListItem>().unwrap();
                    if let Some(label) = list_item.child().and_downcast::<gtk4::Label>() {
                        label.set_text("");
                    }
                });

                let col = gtk4::ColumnViewColumn::builder()
                    .title(col_name.as_str())
                    .factory(&factory)
                    .resizable(true)
                    .expand(col_idx == self.column_names.len() - 1)
                    .build();

                if col_idx != self.column_names.len() - 1 {
                    col.set_fixed_width(140);
                }

                widgets.column_view.append_column(&col);
            }

            widgets.columns_created = true;
        }

        // Reset columns_created if we got new columns
        if self.column_names.is_empty() {
            widgets.columns_created = false;
        }
    }
}

// ── Delta application ─────────────────────────────────────────────────────────

impl ExploreModel {
    fn apply_delta(&mut self, delta: ViewDelta) {
        match delta {
            ViewDelta::RowsAppended { table: None, ipc } => {
                if let Ok(batch) = decode_ipc(&ipc) {
                    // Capture column names from first batch
                    if self.column_names.is_empty() {
                        self.column_names = batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect();
                    }
                    let n = batch.num_rows();
                    for row_idx in 0..n {
                        let values: Vec<String> = batch
                            .columns()
                            .iter()
                            .map(|col| format_cell(col, row_idx))
                            .collect();
                        self.store
                            .append(&glib::BoxedAnyObject::new(ResultRow(values)));
                    }
                    self.row_count += n;
                }
            }
            ViewDelta::TableReplaced { table: None, ipc } => {
                self.store.remove_all();
                self.row_count = 0;
                if let Ok(batch) = decode_ipc(&ipc) {
                    if self.column_names.is_empty() {
                        self.column_names = batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect();
                    }
                    let n = batch.num_rows();
                    for row_idx in 0..n {
                        let values: Vec<String> = batch
                            .columns()
                            .iter()
                            .map(|col| format_cell(col, row_idx))
                            .collect();
                        self.store
                            .append(&glib::BoxedAnyObject::new(ResultRow(values)));
                    }
                    self.row_count = n;
                }
            }
            ViewDelta::RowsExpired {
                table: None,
                expired_count,
            } => {
                let n = expired_count.min(self.store.n_items() as u64) as u32;
                self.store.splice(0, n, &[] as &[glib::BoxedAnyObject]);
                self.row_count = self.row_count.saturating_sub(n as usize);
            }
            ViewDelta::Ready => {
                self.status = format!("{} rows loaded", self.row_count);
            }
            ViewDelta::Error { message } => {
                self.status = format!("Error: {message}");
                tracing::error!("Explore query error: {}", message);
            }
            _ => {}
        }
    }
}

// ── Stream launcher ───────────────────────────────────────────────────────────

fn start_snapshot(model: &mut ExploreModel, sender: ComponentSender<ExploreModel>) {
    let ds = model.data_source.clone();
    let query = model.query.clone();
    let s = sender.input_sender().clone();

    let handle = relm4::spawn(async move {
        match ds.snapshot_batches(&query).await {
            Ok(batches) => {
                for ipc in batches {
                    s.send(ExploreInput::Delta(sequins_view::ViewDelta::RowsAppended {
                        table: None,
                        ipc,
                    }))
                    .ok();
                }
                s.send(ExploreInput::Delta(sequins_view::ViewDelta::Ready))
                    .ok();
            }
            Err(e) => {
                s.send(ExploreInput::StreamError(e.to_string())).ok();
            }
        }
    });
    model.stream_task = Some(handle);
}

// ── Arrow IPC decode + cell formatting ───────────────────────────────────────

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

fn format_cell(col: &ArrayRef, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if col.is_null(row) {
        return "null".to_string();
    }

    match col.data_type() {
        DataType::Utf8View => col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => col
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::UInt8 => col
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| format!("{:.4}", a.value(row)))
            .unwrap_or_default(),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(row).to_string())
            .unwrap_or_default(),
        DataType::Timestamp(_, _) => col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .and_then(|a| {
                use chrono::{TimeZone, Utc};
                let ns = a.value(row);
                let secs = ns / 1_000_000_000;
                let nsecs = (ns % 1_000_000_000).unsigned_abs() as u32;
                Utc.timestamp_opt(secs, nsecs)
                    .single()
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
            })
            .unwrap_or_else(|| {
                col.as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .map(|a| a.value(row).to_string())
                    .unwrap_or_default()
            }),
        _ => format!("<{:?}>", col.data_type()),
    }
}
