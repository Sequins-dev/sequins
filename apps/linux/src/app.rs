//! Root Relm4 component — AdwApplicationWindow with NavigationSplitView.
//!
//! Owns `AppDataSource` and launches tab sub-components once storage is ready.

use crate::components::explore::{ExploreInit, ExploreModel};
use crate::components::health::{HealthInit, HealthInput, HealthModel};
use crate::components::logs::{LogsInit, LogsInput, LogsModel};
use crate::components::metrics::{MetricsInit, MetricsInput, MetricsModel};
use crate::components::profiles::{ProfilesInit, ProfilesInput, ProfilesModel};
use crate::components::traces::{TracesInit, TracesInput, TracesModel};
use crate::config::{self, EnvironmentKind, EnvironmentStore};
use crate::data::{AppDataSource, DataSource, LocalServer, OtlpPorts};
use crate::time_range::{AppTimeRange, LiveRange, PausedRange, TimeRange};
use adw::prelude::*;
use arrow::array::{StringViewArray, UInt32Array};
use arrow::ipc::reader::StreamReader;
use gtk4::glib;
use gtk4::prelude::*;
use relm4::prelude::*;
use std::cell::Cell;
use std::sync::Arc;
use uuid::Uuid;

pub use tab::TabId;

// ── Model ──────────────────────────────────────────────────────────────────────

pub struct AppModel {
    pub data_source: Option<Arc<AppDataSource>>,
    pub selected_service: Option<String>,
    /// Status of the gRPC OTLP port.
    pub grpc_status: PortStatus,
    /// Status of the HTTP OTLP port.
    pub http_status: PortStatus,
    /// Status of the currently active profile's connection.
    pub profile_status: ProfileStatus,
    pub app_time_range: AppTimeRange,
    /// Persistent environment profiles.
    pub env_store: EnvironmentStore,
    /// The always-on local server (Arc so restart closures can hold a ref).
    pub local_server: Option<Arc<LocalServer>>,
    /// Whether the env popover list needs to be rebuilt.
    pub env_list_dirty: Cell<bool>,
    /// Whether tab containers need to be reset to spinners (after profile switch).
    pub tabs_reset_needed: Cell<bool>,
    /// Pending error message to show in an alert dialog (gRPC).
    pub grpc_error_pending: Cell<Option<String>>,
    /// Pending error message to show in an alert dialog (HTTP).
    pub http_error_pending: Cell<Option<String>>,
    /// Service names and their resource_ids, fetched from resources table for the sidebar.
    pub services: Vec<(String, u32)>,
    // Tab controllers — created lazily when data source is ready
    explore_controller: Option<relm4::Controller<ExploreModel>>,
    health_controller: Option<relm4::Controller<HealthModel>>,
    logs_controller: Option<relm4::Controller<LogsModel>>,
    metrics_controller: Option<relm4::Controller<MetricsModel>>,
    profiles_controller: Option<relm4::Controller<ProfilesModel>>,
    traces_controller: Option<relm4::Controller<TracesModel>>,
}

/// Status of a single OTLP port.
#[derive(Debug, Clone, PartialEq)]
pub enum PortStatus {
    Starting,
    Running(u16),
    Error(String),
}

impl PortStatus {
    fn css_class(&self) -> &'static str {
        match self {
            Self::Starting => "status-starting",
            Self::Running(_) => "status-ok",
            Self::Error(_) => "status-error",
        }
    }
}

/// Status of the currently active profile's connection.
#[derive(Debug, Clone, PartialEq)]
pub enum ProfileStatus {
    Connecting,
    Connected,
    Error(String),
}

impl ProfileStatus {
    fn label(&self, env_name: &str, env_kind: &EnvironmentKind) -> String {
        match self {
            Self::Connecting => format!("● {env_name} Connecting…"),
            Self::Connected => match env_kind {
                EnvironmentKind::Local { .. } => format!("● {env_name}"),
                EnvironmentKind::Remote { query_url } => {
                    let host = query_url
                        .trim_start_matches("http://")
                        .trim_start_matches("https://");
                    format!("● Connected to {host}")
                }
            },
            Self::Error(e) => format!("● Error: {e}"),
        }
    }

    fn css_class(&self) -> &'static str {
        match self {
            Self::Connecting => "status-starting",
            Self::Connected => "status-ok",
            Self::Error(_) => "status-error",
        }
    }
}

// ── Messages ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum AppMsg {
    ServiceSelected(String),
    ServiceCleared,
    AppTimeRangeChanged(AppTimeRange),
    /// User clicked the live/paused toggle button.
    ToggleLiveMode,
    /// Index selected in the time dropdown (mode-dependent).
    TimeRangeIndexChanged(u32),
    /// 1-second tick for sliding window (fired when live).
    SlidingWindowTick,
    /// Switch to a different environment profile.
    SwitchEnvironment(Uuid),
    /// Add a new remote environment.
    AddRemoteEnvironment {
        name: String,
        url: String,
    },
    /// Remove an environment by id.
    RemoveEnvironment(Uuid),
    /// Restart the gRPC OTLP port independently.
    RestartGrpc,
    /// Restart the HTTP OTLP port independently.
    RestartHttp,
    /// Re-show the gRPC error alert (user tapped the red dot).
    ShowGrpcError,
    /// Re-show the HTTP error alert (user tapped the red dot).
    ShowHttpError,
    /// Reconnect the active profile's data source.
    ReconnectProfile,
}

/// Async command output — result of background storage initialization.
#[derive(Debug)]
pub enum AppCmd {
    LocalServerInitialized(Arc<LocalServer>),
    GrpcReady(u16),
    GrpcError(String),
    HttpReady(u16),
    HttpError(String),
    DataSourceReady(Box<AppDataSource>),
    DataSourceError(String),
    ServicesReady(Vec<(String, u32)>),
}

// ── Widgets ───────────────────────────────────────────────────────────────────

pub struct AppWidgets {
    window: adw::ApplicationWindow,
    /// Colored dot for the gRPC port chip.
    grpc_dot: gtk4::Label,
    /// Colored dot for the HTTP port chip.
    http_dot: gtk4::Label,
    env_menu_button: gtk4::MenuButton,
    env_list_box: gtk4::ListBox,
    view_stack: gtk4::Stack,
    service_list: gtk4::ListBox,
    /// Containers for each tab page; logs page lives at index 3
    tab_containers: Vec<gtk4::Box>,
    /// Set to true once the service list has been populated.
    services_populated: bool,
    // Filter bar
    live_toggle: gtk4::ToggleButton,
    toggle_handler_id: glib::SignalHandlerId,
    time_string_list: gtk4::StringList,
    time_dropdown: gtk4::DropDown,
    dropdown_handler_id: glib::SignalHandlerId,
    /// Stack of per-tab filter controls, shown inline with the global filter bar.
    tab_controls_stack: gtk4::Stack,
    /// Set to true once tab toolbars have been reparented into tab_controls_stack.
    tab_controls_built: bool,
    /// Active glib timer source for the live sliding window tick.
    timer_source: Option<glib::SourceId>,
}

// ── Component impl ────────────────────────────────────────────────────────────

impl Component for AppModel {
    type CommandOutput = AppCmd;
    type Init = ();
    type Input = AppMsg;
    type Output = ();
    type Widgets = AppWidgets;
    type Root = adw::ApplicationWindow;

    fn init_root() -> Self::Root {
        adw::ApplicationWindow::builder()
            .title("Sequins")
            .default_width(1280)
            .default_height(800)
            .build()
    }

    fn init(
        _init: Self::Init,
        root: Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        // Load app CSS (severity colours, span status, health badges)
        let css = gtk4::CssProvider::new();
        // load_from_string is available in GTK 4.12+ (gtk4-rs 0.8+)
        // gtk4-rs 0.9 targets GTK 4.14 so this is fine
        css.load_from_string(APP_CSS);
        if let Some(display) = gtk4::gdk::Display::default() {
            gtk4::style_context_add_provider_for_display(
                &display,
                &css,
                gtk4::STYLE_PROVIDER_PRIORITY_APPLICATION,
            );
        }

        let env_store = EnvironmentStore::load().unwrap_or_default();

        let model = AppModel {
            data_source: None,
            selected_service: None,
            grpc_status: PortStatus::Starting,
            http_status: PortStatus::Starting,
            profile_status: ProfileStatus::Connecting,
            app_time_range: AppTimeRange::default(),
            env_store,
            local_server: None,
            env_list_dirty: Cell::new(true),
            tabs_reset_needed: Cell::new(false),
            grpc_error_pending: Cell::new(None),
            http_error_pending: Cell::new(None),
            services: Vec::new(),
            explore_controller: None,
            health_controller: None,
            logs_controller: None,
            metrics_controller: None,
            profiles_controller: None,
            traces_controller: None,
        };

        // ── ViewStack — one page per tab ──────────────────────────────────────
        let view_stack = gtk4::Stack::new();
        view_stack.set_vexpand(true);

        let mut tab_containers: Vec<gtk4::Box> = Vec::new();

        for (name, _icon, label) in TabId::all_tabs() {
            let container = build_placeholder_page(label);
            view_stack.add_titled(&container, Some(name), label);
            tab_containers.push(container);
        }

        // ── Content header with tab switcher ──────────────────────────────────
        let switcher = gtk4::StackSwitcher::new();
        switcher.set_stack(Some(&view_stack));

        let content_header = adw::HeaderBar::new();
        content_header.set_title_widget(Some(&switcher));

        // ── Filter bar (live/paused toggle + time dropdown) ───────────────────
        let live_toggle = gtk4::ToggleButton::builder()
            .label("● Live")
            .active(true)
            .css_classes(["flat", "filter-bar-live"])
            .build();
        let toggle_handler_id = {
            let s = sender.clone();
            live_toggle.connect_toggled(move |_| {
                s.input(AppMsg::ToggleLiveMode);
            })
        };

        // StringList so we can swap live/paused labels by splicing.
        let live_labels: Vec<&str> = LiveRange::ALL.iter().map(|r| r.label()).collect();
        let time_string_list = gtk4::StringList::new(&live_labels);
        let time_dropdown = gtk4::DropDown::builder().model(&time_string_list).build();
        // Default: LiveRange::Min15 is index 2.
        time_dropdown.set_selected(2);
        let dropdown_handler_id = {
            let s = sender.clone();
            time_dropdown.connect_selected_notify(move |dd| {
                s.input(AppMsg::TimeRangeIndexChanged(dd.selected()));
            })
        };

        let tab_controls_stack = gtk4::Stack::builder()
            .hexpand(true)
            .vhomogeneous(false)
            .transition_type(gtk4::StackTransitionType::None)
            .build();

        // Connect view_stack page changes to sync the tab_controls_stack visible page.
        {
            let stack = tab_controls_stack.clone();
            view_stack.connect_notify_local(Some("visible-child-name"), move |vs, _| {
                if let Some(name) = vs.visible_child_name() {
                    stack.set_visible_child_name(&name);
                }
            });
        }

        let vsep = gtk4::Separator::new(gtk4::Orientation::Vertical);
        vsep.set_margin_top(4);
        vsep.set_margin_bottom(4);

        let filter_bar = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .margin_start(8)
            .margin_end(8)
            .margin_top(4)
            .margin_bottom(4)
            .spacing(6)
            .build();
        filter_bar.append(&live_toggle);
        filter_bar.append(&time_dropdown);
        filter_bar.append(&vsep);
        filter_bar.append(&tab_controls_stack);

        let content_box = gtk4::Box::new(gtk4::Orientation::Vertical, 0);
        content_box.append(&content_header);
        content_box.append(&filter_bar);
        content_box.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        content_box.append(&view_stack);

        let content_page = adw::NavigationPage::builder()
            .title("Content")
            .child(&content_box)
            .build();

        // ── Sidebar ────────────────────────────────────────────────────────────

        // Environment (profile) switcher in header
        let env_list_box = gtk4::ListBox::builder()
            .selection_mode(gtk4::SelectionMode::None)
            .css_classes(["navigation-sidebar"])
            .build();

        let env_add_separator = gtk4::Separator::new(gtk4::Orientation::Horizontal);

        let add_remote_button = gtk4::Button::builder()
            .label("Add Remote…")
            .css_classes(["flat"])
            .margin_start(8)
            .margin_end(8)
            .margin_top(4)
            .margin_bottom(4)
            .build();

        // "Add Remote" inline form (hidden by default, shown on button click)
        let remote_name_entry = gtk4::Entry::builder()
            .placeholder_text("Name")
            .margin_start(8)
            .margin_end(8)
            .margin_top(4)
            .build();
        let remote_url_entry = gtk4::Entry::builder()
            .placeholder_text("http://host:4319")
            .margin_start(8)
            .margin_end(8)
            .margin_top(4)
            .build();
        let add_form_buttons = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(4)
            .margin_start(8)
            .margin_end(8)
            .margin_top(4)
            .margin_bottom(8)
            .build();
        let add_confirm_button = gtk4::Button::builder()
            .label("Add")
            .css_classes(["suggested-action"])
            .hexpand(true)
            .build();
        let add_cancel_button = gtk4::Button::builder()
            .label("Cancel")
            .css_classes(["flat"])
            .hexpand(true)
            .build();
        add_form_buttons.append(&add_cancel_button);
        add_form_buttons.append(&add_confirm_button);

        let add_remote_form = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .visible(false)
            .build();
        add_remote_form.append(&remote_name_entry);
        add_remote_form.append(&remote_url_entry);
        add_remote_form.append(&add_form_buttons);

        let popover_box = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Vertical)
            .width_request(200)
            .build();
        popover_box.append(&env_list_box);
        popover_box.append(&env_add_separator);
        popover_box.append(&add_remote_button);
        popover_box.append(&add_remote_form);

        let env_popover = gtk4::Popover::builder().child(&popover_box).build();

        let env_menu_button = gtk4::MenuButton::builder()
            .label(model.env_store.selected().name.as_str())
            .popover(&env_popover)
            .direction(gtk4::ArrowType::Down)
            .css_classes(["flat"])
            .build();

        // Wire up "Add Remote" button
        {
            let form = add_remote_form.clone();
            let btn = add_remote_button.clone();
            add_remote_button.connect_clicked(move |_| {
                btn.set_visible(false);
                form.set_visible(true);
            });
        }
        {
            let form = add_remote_form.clone();
            let btn = add_remote_button.clone();
            let name_entry = remote_name_entry.clone();
            let url_entry = remote_url_entry.clone();
            add_cancel_button.connect_clicked(move |_| {
                form.set_visible(false);
                btn.set_visible(true);
                name_entry.set_text("");
                url_entry.set_text("");
            });
        }
        {
            let s = sender.clone();
            let form = add_remote_form.clone();
            let btn = add_remote_button.clone();
            let name_entry = remote_name_entry.clone();
            let url_entry = remote_url_entry.clone();
            let popover = env_popover.clone();
            add_confirm_button.connect_clicked(move |_| {
                let name = name_entry.text().to_string();
                let url = url_entry.text().to_string();
                if !name.is_empty() && !url.is_empty() {
                    s.input(AppMsg::AddRemoteEnvironment { name, url });
                    form.set_visible(false);
                    btn.set_visible(true);
                    name_entry.set_text("");
                    url_entry.set_text("");
                    popover.popdown();
                }
            });
        }

        // ── Single status row: one chip per port + profile ────────────────────

        // Helper closure: build one chip (dot + label + icon button)
        let make_dot = || {
            gtk4::Label::builder()
                .label("●")
                .css_classes(["caption", "status-starting"])
                .build()
        };

        let grpc_dot = make_dot();
        let http_dot = make_dot();

        {
            let s = sender.clone();
            let gesture = gtk4::GestureClick::new();
            gesture.connect_released(move |_, _, _, _| s.input(AppMsg::ShowGrpcError));
            grpc_dot.add_controller(gesture);
        }
        {
            let s = sender.clone();
            let gesture = gtk4::GestureClick::new();
            gesture.connect_released(move |_, _, _, _| s.input(AppMsg::ShowHttpError));
            http_dot.add_controller(gesture);
        }

        let grpc_label = gtk4::Label::builder()
            .label("gRPC :4317")
            .css_classes(["caption"])
            .hexpand(true)
            .halign(gtk4::Align::Center)
            .build();
        let http_label = gtk4::Label::builder()
            .label("JSON :4318")
            .css_classes(["caption"])
            .hexpand(true)
            .halign(gtk4::Align::Center)
            .build();

        let make_restart_btn = || {
            gtk4::Button::builder()
                .icon_name("view-refresh-symbolic")
                .css_classes(["flat", "circular"])
                .build()
        };

        let grpc_restart_btn = make_restart_btn();
        let http_restart_btn = make_restart_btn();

        {
            let s = sender.clone();
            grpc_restart_btn.connect_clicked(move |_| {
                s.input(AppMsg::RestartGrpc);
            });
        }
        {
            let s = sender.clone();
            http_restart_btn.connect_clicked(move |_| {
                s.input(AppMsg::RestartHttp);
            });
        }

        let make_chip = |dot: &gtk4::Label, text_label: &gtk4::Label, btn: &gtk4::Button| {
            let chip = gtk4::Box::builder()
                .orientation(gtk4::Orientation::Horizontal)
                .spacing(2)
                .hexpand(true)
                .halign(gtk4::Align::Center)
                .build();
            chip.append(dot);
            chip.append(text_label);
            chip.append(btn);
            chip
        };

        let grpc_chip = make_chip(&grpc_dot, &grpc_label, &grpc_restart_btn);
        let http_chip = make_chip(&http_dot, &http_label, &http_restart_btn);

        let status_row = gtk4::Box::builder()
            .orientation(gtk4::Orientation::Horizontal)
            .spacing(0)
            .margin_start(4)
            .margin_end(4)
            .margin_top(2)
            .margin_bottom(2)
            .build();
        status_row.append(&grpc_chip);
        let sep1 = gtk4::Separator::new(gtk4::Orientation::Vertical);
        sep1.set_margin_top(4);
        sep1.set_margin_bottom(4);
        status_row.append(&sep1);
        status_row.append(&http_chip);

        let services_heading = gtk4::Label::builder()
            .label("Services")
            .css_classes(["heading"])
            .margin_start(12)
            .margin_top(8)
            .margin_bottom(4)
            .halign(gtk4::Align::Start)
            .build();

        let service_list = gtk4::ListBox::builder()
            .selection_mode(gtk4::SelectionMode::Single)
            .css_classes(["navigation-sidebar"])
            .build();

        let loading_row = gtk4::ListBoxRow::new();
        loading_row.set_sensitive(false);
        loading_row.set_child(Some(
            &gtk4::Label::builder()
                .label("Loading…")
                .css_classes(["dim-label"])
                .margin_start(12)
                .margin_top(6)
                .margin_bottom(6)
                .halign(gtk4::Align::Start)
                .build(),
        ));
        service_list.append(&loading_row);

        {
            let s = sender.clone();
            service_list.connect_row_activated(move |_, row| {
                if let Some(label) = row.child().and_downcast::<gtk4::Label>() {
                    s.input(AppMsg::ServiceSelected(label.text().to_string()));
                }
            });
        }

        let scroll = gtk4::ScrolledWindow::builder()
            .hscrollbar_policy(gtk4::PolicyType::Never)
            .vexpand(true)
            .child(&service_list)
            .build();

        let sidebar_header = adw::HeaderBar::builder()
            .show_end_title_buttons(false)
            .build();
        sidebar_header.set_title_widget(Some(&env_menu_button));
        adw::StyleManager::default().set_color_scheme(adw::ColorScheme::Default);

        let sidebar_box = gtk4::Box::new(gtk4::Orientation::Vertical, 0);
        sidebar_box.append(&sidebar_header);
        sidebar_box.append(&status_row);
        sidebar_box.append(&gtk4::Separator::new(gtk4::Orientation::Horizontal));
        sidebar_box.append(&services_heading);
        sidebar_box.append(&scroll);

        let sidebar_page = adw::NavigationPage::builder()
            .title("Sequins")
            .child(&sidebar_box)
            .build();

        // ── NavigationSplitView ───────────────────────────────────────────────
        let nav_split = adw::NavigationSplitView::builder()
            .min_sidebar_width(180.0)
            .max_sidebar_width(280.0)
            .build();
        nav_split.set_sidebar(Some(&sidebar_page));
        nav_split.set_content(Some(&content_page));

        root.set_content(Some(&nav_split));
        root.present();

        // ── Kick off async storage initialization ─────────────────────────────
        let selected_env = model.env_store.selected().clone();
        let default_ports = OtlpPorts::default();
        sender.command(move |out: relm4::Sender<AppCmd>, _shutdown| async move {
            let server = match init_local_server().await {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    let msg = e.to_string();
                    let _ = out.send(AppCmd::GrpcError(msg.clone()));
                    let _ = out.send(AppCmd::HttpError(msg));
                    return;
                }
            };
            let _ = out.send(AppCmd::LocalServerInitialized(server.clone()));

            // Start gRPC and HTTP independently so each chip updates separately.
            let s1 = server.clone();
            let s2 = server.clone();
            let out1 = out.clone();
            let out2 = out.clone();
            let (grpc_res, http_res) = tokio::join!(
                async move {
                    match s1.start_grpc(default_ports.grpc).await {
                        Ok(p) => {
                            let _ = out1.send(AppCmd::GrpcReady(p));
                            true
                        }
                        Err(e) => {
                            let _ = out1.send(AppCmd::GrpcError(e.to_string()));
                            false
                        }
                    }
                },
                async move {
                    match s2.start_http(default_ports.http).await {
                        Ok(p) => {
                            let _ = out2.send(AppCmd::HttpReady(p));
                            true
                        }
                        Err(e) => {
                            let _ = out2.send(AppCmd::HttpError(e.to_string()));
                            false
                        }
                    }
                }
            );

            if grpc_res && http_res {
                let ds_result = connect_to_env(&server, &selected_env);
                match ds_result {
                    Ok(ds) => {
                        let _ = out.send(AppCmd::DataSourceReady(Box::new(ds)));
                    }
                    Err(e) => {
                        let _ = out.send(AppCmd::DataSourceError(e.to_string()));
                    }
                }
            }
        });

        ComponentParts {
            model,
            widgets: AppWidgets {
                window: root,
                grpc_dot,
                http_dot,
                env_menu_button,
                env_list_box,
                view_stack,
                service_list,
                tab_containers,
                services_populated: false,
                live_toggle,
                toggle_handler_id,
                time_string_list,
                time_dropdown,
                dropdown_handler_id,
                tab_controls_stack,
                tab_controls_built: false,
                timer_source: None,
            },
        }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            AppMsg::ServiceSelected(service) => {
                tracing::debug!("Service selected: {}", service);
                let filter = Some(service.clone());
                self.forward_service_filter(filter);
                self.selected_service = Some(service);
            }
            AppMsg::ServiceCleared => {
                tracing::debug!("Service filter cleared");
                self.forward_service_filter(None);
                self.selected_service = None;
            }
            AppMsg::ToggleLiveMode => {
                let new_range = match self.app_time_range {
                    AppTimeRange::Live(_) => AppTimeRange::Paused(PausedRange::default()),
                    AppTimeRange::Paused(_) => AppTimeRange::Live(LiveRange::default()),
                };
                tracing::debug!("Live mode toggled: {:?}", new_range);
                self.forward_app_time_range(new_range);
                self.app_time_range = new_range;
            }
            AppMsg::TimeRangeIndexChanged(idx) => {
                let new_range = match self.app_time_range {
                    AppTimeRange::Live(_) => {
                        let lr = LiveRange::ALL
                            .get(idx as usize)
                            .copied()
                            .unwrap_or_default();
                        AppTimeRange::Live(lr)
                    }
                    AppTimeRange::Paused(_) => {
                        let pr = PausedRange::ALL
                            .get(idx as usize)
                            .copied()
                            .unwrap_or_default();
                        AppTimeRange::Paused(pr)
                    }
                };
                tracing::debug!("Time range index changed: {} → {:?}", idx, new_range);
                self.forward_app_time_range(new_range);
                self.app_time_range = new_range;
            }
            AppMsg::AppTimeRangeChanged(range) => {
                tracing::debug!("App time range changed: {:?}", range);
                self.forward_app_time_range(range);
                self.app_time_range = range;
            }
            AppMsg::SlidingWindowTick => {
                if let Some(c) = &self.metrics_controller {
                    let _ = c.sender().emit(MetricsInput::SlidingWindowTick);
                }
            }
            AppMsg::SwitchEnvironment(id) => {
                if self.env_store.select(id).is_ok() {
                    self.data_source = None;
                    self.services = Vec::new();
                    self.selected_service = None;
                    self.explore_controller = None;
                    self.health_controller = None;
                    self.logs_controller = None;
                    self.metrics_controller = None;
                    self.profiles_controller = None;
                    self.traces_controller = None;
                    self.profile_status = ProfileStatus::Connecting;
                    self.env_list_dirty.set(true);
                    self.tabs_reset_needed.set(true);

                    let selected_env = self.env_store.selected().clone();
                    let local_server_ref = self.local_server.as_ref().map(|s| s.backend());
                    let ds_result = if let Some(backend) = local_server_ref {
                        match &selected_env.kind {
                            EnvironmentKind::Local { .. } => {
                                Ok(AppDataSource::new(crate::data::DataSource::Local {
                                    backend,
                                }))
                            }
                            EnvironmentKind::Remote { query_url } => {
                                crate::data::DataSource::for_remote(query_url)
                                    .map(AppDataSource::new)
                            }
                        }
                    } else {
                        Err(anyhow::anyhow!("Local server not available"))
                    };

                    sender.command(move |out: relm4::Sender<AppCmd>, _| async move {
                        match ds_result {
                            Ok(ds) => {
                                let _ = out.send(AppCmd::DataSourceReady(Box::new(ds)));
                            }
                            Err(e) => {
                                let _ = out.send(AppCmd::DataSourceError(e.to_string()));
                            }
                        }
                    });
                }
            }
            AppMsg::AddRemoteEnvironment { name, url } => {
                if self.env_store.add_remote(name, url).is_ok() {
                    self.env_list_dirty.set(true);
                }
            }
            AppMsg::RemoveEnvironment(id) => {
                if self.env_store.remove(id).is_ok() {
                    self.env_list_dirty.set(true);
                }
            }
            AppMsg::ShowGrpcError => {
                if let PortStatus::Error(msg) = &self.grpc_status {
                    self.grpc_error_pending.set(Some(msg.clone()));
                }
            }
            AppMsg::ShowHttpError => {
                if let PortStatus::Error(msg) = &self.http_status {
                    self.http_error_pending.set(Some(msg.clone()));
                }
            }
            AppMsg::RestartGrpc => {
                self.grpc_status = PortStatus::Starting;
                if let Some(server) = self.local_server.clone() {
                    let port = OtlpPorts::default().grpc;
                    sender.command(move |out: relm4::Sender<AppCmd>, _| async move {
                        match server.start_grpc(port).await {
                            Ok(p) => {
                                let _ = out.send(AppCmd::GrpcReady(p));
                            }
                            Err(e) => {
                                let _ = out.send(AppCmd::GrpcError(e.to_string()));
                            }
                        }
                    });
                }
            }
            AppMsg::RestartHttp => {
                self.http_status = PortStatus::Starting;
                if let Some(server) = self.local_server.clone() {
                    let port = OtlpPorts::default().http;
                    sender.command(move |out: relm4::Sender<AppCmd>, _| async move {
                        match server.start_http(port).await {
                            Ok(p) => {
                                let _ = out.send(AppCmd::HttpReady(p));
                            }
                            Err(e) => {
                                let _ = out.send(AppCmd::HttpError(e.to_string()));
                            }
                        }
                    });
                }
            }
            AppMsg::ReconnectProfile => {
                self.profile_status = ProfileStatus::Connecting;
                self.data_source = None;
                self.services = Vec::new();
                self.selected_service = None;
                self.explore_controller = None;
                self.health_controller = None;
                self.logs_controller = None;
                self.metrics_controller = None;
                self.profiles_controller = None;
                self.traces_controller = None;
                self.tabs_reset_needed.set(true);

                let selected_env = self.env_store.selected().clone();
                let local_server_ref = self.local_server.as_ref().map(|s| s.backend());
                let ds_result = if let Some(backend) = local_server_ref {
                    match &selected_env.kind {
                        EnvironmentKind::Local { .. } => {
                            Ok(AppDataSource::new(DataSource::Local { backend }))
                        }
                        EnvironmentKind::Remote { query_url } => {
                            DataSource::for_remote(query_url).map(AppDataSource::new)
                        }
                    }
                } else {
                    Err(anyhow::anyhow!("Local server not available"))
                };
                sender.command(move |out: relm4::Sender<AppCmd>, _| async move {
                    match ds_result {
                        Ok(ds) => {
                            let _ = out.send(AppCmd::DataSourceReady(Box::new(ds)));
                        }
                        Err(e) => {
                            let _ = out.send(AppCmd::DataSourceError(e.to_string()));
                        }
                    }
                });
            }
        }
    }

    fn update_cmd(
        &mut self,
        cmd: Self::CommandOutput,
        sender: ComponentSender<Self>,
        _root: &Self::Root,
    ) {
        match cmd {
            AppCmd::LocalServerInitialized(server) => {
                tracing::info!("Local server initialized");
                self.local_server = Some(server);
            }
            AppCmd::GrpcReady(port) => {
                tracing::info!(port, "gRPC OTLP server ready");
                self.grpc_status = PortStatus::Running(port);
            }
            AppCmd::GrpcError(msg) => {
                tracing::error!("gRPC server failed: {}", msg);
                self.grpc_status = PortStatus::Error(msg.clone());
                self.grpc_error_pending.set(Some(msg));
            }
            AppCmd::HttpReady(port) => {
                tracing::info!(port, "HTTP OTLP server ready");
                self.http_status = PortStatus::Running(port);
            }
            AppCmd::HttpError(msg) => {
                tracing::error!("HTTP server failed: {}", msg);
                self.http_status = PortStatus::Error(msg.clone());
                self.http_error_pending.set(Some(msg));
            }
            AppCmd::DataSourceReady(ds) => {
                tracing::info!("Data source ready");
                let ds = Arc::new(*ds);
                self.profile_status = ProfileStatus::Connected;

                // Launch tab components
                let explore_controller = ExploreModel::builder()
                    .launch(ExploreInit {
                        data_source: ds.clone(),
                    })
                    .detach();
                self.explore_controller = Some(explore_controller);

                let health_controller = HealthModel::builder()
                    .launch(HealthInit {
                        data_source: ds.clone(),
                    })
                    .detach();
                self.health_controller = Some(health_controller);

                let logs_controller = LogsModel::builder()
                    .launch(LogsInit {
                        data_source: ds.clone(),
                    })
                    .detach();
                self.logs_controller = Some(logs_controller);

                let metrics_controller = MetricsModel::builder()
                    .launch(MetricsInit {
                        data_source: ds.clone(),
                    })
                    .detach();
                self.metrics_controller = Some(metrics_controller);

                let profiles_controller = ProfilesModel::builder()
                    .launch(ProfilesInit {
                        data_source: ds.clone(),
                    })
                    .detach();
                self.profiles_controller = Some(profiles_controller);

                let traces_controller = TracesModel::builder()
                    .launch(TracesInit {
                        data_source: ds.clone(),
                    })
                    .detach();
                self.traces_controller = Some(traces_controller);

                // Fetch the service list asynchronously for the sidebar
                let ds2 = ds.clone();
                sender.command(move |out, _shutdown| async move {
                    let services = fetch_service_list(&ds2).await;
                    let _ = out.send(AppCmd::ServicesReady(services));
                });

                self.data_source = Some(ds);
            }
            AppCmd::DataSourceError(msg) => {
                tracing::error!("Data source init failed: {}", msg);
                self.profile_status = ProfileStatus::Error(msg);
            }
            AppCmd::ServicesReady(services) => {
                tracing::debug!("Service list ready: {} services", services.len());
                if services.is_empty() {
                    // No data yet — retry after 5 seconds
                    if let Some(ds) = &self.data_source {
                        let ds2 = ds.clone();
                        sender.command(move |out, _shutdown| async move {
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            let services = fetch_service_list(&ds2).await;
                            let _ = out.send(AppCmd::ServicesReady(services));
                        });
                    }
                } else {
                    self.services = services;
                    // Auto-select the first service so all tabs have content immediately
                    if let Some((name, _)) = self.services.first() {
                        let name = name.clone();
                        self.selected_service = Some(name.clone());
                        self.forward_service_filter(Some(name));
                    }
                }
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, sender: ComponentSender<Self>) {
        // ── Status chips ──────────────────────────────────────────────────────
        let pointer = gtk4::gdk::Cursor::from_name("pointer", None);
        let default_cursor = gtk4::gdk::Cursor::from_name("default", None);
        for cls in [
            "status-ok",
            "status-starting",
            "status-error",
            "status-error-clickable",
            "status-stopped",
        ] {
            widgets.grpc_dot.remove_css_class(cls);
            widgets.http_dot.remove_css_class(cls);
        }
        // Use a distinct hoverable class for error dots so CSS :hover applies.
        let grpc_css = if matches!(self.grpc_status, PortStatus::Error(_)) {
            "status-error-clickable"
        } else {
            self.grpc_status.css_class()
        };
        let http_css = if matches!(self.http_status, PortStatus::Error(_)) {
            "status-error-clickable"
        } else {
            self.http_status.css_class()
        };
        widgets.grpc_dot.add_css_class(grpc_css);
        widgets.http_dot.add_css_class(http_css);
        widgets
            .grpc_dot
            .set_cursor(if matches!(self.grpc_status, PortStatus::Error(_)) {
                pointer.as_ref()
            } else {
                default_cursor.as_ref()
            });
        widgets
            .http_dot
            .set_cursor(if matches!(self.http_status, PortStatus::Error(_)) {
                pointer.as_ref()
            } else {
                default_cursor.as_ref()
            });
        widgets
            .grpc_dot
            .set_tooltip_text(if matches!(self.grpc_status, PortStatus::Error(_)) {
                Some("Click to view error")
            } else {
                None
            });
        widgets
            .http_dot
            .set_tooltip_text(if matches!(self.http_status, PortStatus::Error(_)) {
                Some("Click to view error")
            } else {
                None
            });

        // Show error dialogs for any pending port failures.
        if let Some(msg) = self.grpc_error_pending.take() {
            show_error_dialog(&widgets.window, "gRPC Server Error", &msg);
        }
        if let Some(msg) = self.http_error_pending.take() {
            show_error_dialog(&widgets.window, "HTTP Server Error", &msg);
        }

        // Update env menu button label
        widgets
            .env_menu_button
            .set_label(&self.env_store.selected().name);

        // Reset tab containers to spinners if we switched environments
        if self.tabs_reset_needed.get() {
            self.tabs_reset_needed.set(false);
            for container in &widgets.tab_containers {
                while let Some(child) = container.first_child() {
                    container.remove(&child);
                }
                let spinner = gtk4::Spinner::new();
                spinner.start();
                container.set_halign(gtk4::Align::Center);
                container.set_valign(gtk4::Align::Center);
                container.set_hexpand(false);
                container.set_vexpand(false);
                container.append(&spinner);
            }
            // Reset tab_controls_built so toolbars get re-reparented
            widgets.tab_controls_built = false;
            // Remove old tab controls from the stack
            let mut child = widgets.tab_controls_stack.first_child();
            while let Some(w) = child {
                let next = w.next_sibling();
                widgets.tab_controls_stack.remove(&w);
                child = next;
            }
            // Reset service list to loading placeholder
            while let Some(row) = widgets.service_list.first_child() {
                widgets.service_list.remove(&row);
            }
            let loading_row = gtk4::ListBoxRow::new();
            loading_row.set_sensitive(false);
            loading_row.set_child(Some(
                &gtk4::Label::builder()
                    .label("Loading…")
                    .css_classes(["dim-label"])
                    .margin_start(12)
                    .margin_top(6)
                    .margin_bottom(6)
                    .halign(gtk4::Align::Start)
                    .build(),
            ));
            widgets.service_list.append(&loading_row);
            widgets.services_populated = false;
        }

        // Rebuild env list if dirty
        if self.env_list_dirty.get() {
            self.env_list_dirty.set(false);
            while let Some(child) = widgets.env_list_box.first_child() {
                widgets.env_list_box.remove(&child);
            }
            for env in &self.env_store.environments {
                let env_id = env.id;
                let is_selected = env.id == self.env_store.selected_id;
                let row_box = gtk4::Box::builder()
                    .orientation(gtk4::Orientation::Horizontal)
                    .spacing(4)
                    .margin_start(8)
                    .margin_end(8)
                    .margin_top(4)
                    .margin_bottom(4)
                    .build();

                let dot = gtk4::Label::builder()
                    .label("●")
                    .css_classes(if is_selected {
                        vec!["status-ok"]
                    } else {
                        vec!["status-stopped"]
                    })
                    .build();

                let name_label = gtk4::Label::builder()
                    .label(&env.name)
                    .hexpand(true)
                    .halign(gtk4::Align::Start)
                    .build();

                let subtitle = gtk4::Label::builder()
                    .label(&env.kind.subtitle())
                    .css_classes(["caption", "dim-label"])
                    .build();

                row_box.append(&dot);
                row_box.append(&name_label);
                row_box.append(&subtitle);

                if !env.is_default {
                    let del_btn = gtk4::Button::builder()
                        .icon_name("user-trash-symbolic")
                        .css_classes(["flat", "circular"])
                        .build();
                    let s = sender.clone();
                    del_btn.connect_clicked(move |_| {
                        s.input(AppMsg::RemoveEnvironment(env_id));
                    });
                    row_box.append(&del_btn);
                }

                let row = gtk4::ListBoxRow::new();
                row.set_child(Some(&row_box));

                let s = sender.clone();
                let popover = widgets
                    .env_menu_button
                    .popover()
                    .and_downcast::<gtk4::Popover>();
                row.connect_activate(move |_| {
                    s.input(AppMsg::SwitchEnvironment(env_id));
                    if let Some(p) = &popover {
                        p.popdown();
                    }
                });

                widgets.env_list_box.append(&row);
            }
        }

        // ── Sync live/paused toggle ────────────────────────────────────────────
        let is_live = self.app_time_range.is_live();
        widgets.live_toggle.block_signal(&widgets.toggle_handler_id);
        widgets.live_toggle.set_active(is_live);
        widgets
            .live_toggle
            .unblock_signal(&widgets.toggle_handler_id);
        if is_live {
            widgets.live_toggle.set_label("● Live");
            widgets.live_toggle.remove_css_class("filter-bar-paused");
            widgets.live_toggle.add_css_class("filter-bar-live");
        } else {
            widgets.live_toggle.set_label("● Paused");
            widgets.live_toggle.remove_css_class("filter-bar-live");
            widgets.live_toggle.add_css_class("filter-bar-paused");
        }

        // ── Sync time dropdown labels and selection ────────────────────────────
        let (labels, selected_idx) = match self.app_time_range {
            AppTimeRange::Live(lr) => {
                let labels: Vec<&str> = LiveRange::ALL.iter().map(|r| r.label()).collect();
                let idx = LiveRange::ALL.iter().position(|&r| r == lr).unwrap_or(2) as u32;
                (labels, idx)
            }
            AppTimeRange::Paused(pr) => {
                let labels: Vec<&str> = PausedRange::ALL.iter().map(|r| r.label()).collect();
                let idx = PausedRange::ALL.iter().position(|&r| r == pr).unwrap_or(1) as u32;
                (labels, idx)
            }
        };
        let current_n = widgets.time_string_list.n_items();
        widgets
            .time_dropdown
            .block_signal(&widgets.dropdown_handler_id);
        widgets.time_string_list.splice(0, current_n, &labels);
        widgets.time_dropdown.set_selected(selected_idx);
        widgets
            .time_dropdown
            .unblock_signal(&widgets.dropdown_handler_id);

        // ── Manage 1-second sliding window timer ──────────────────────────────
        if is_live && widgets.timer_source.is_none() {
            let s = sender.clone();
            let source_id = glib::timeout_add_local(std::time::Duration::from_secs(1), move || {
                s.input(AppMsg::SlidingWindowTick);
                glib::ControlFlow::Continue
            });
            widgets.timer_source = Some(source_id);
        } else if !is_live {
            if let Some(src) = widgets.timer_source.take() {
                src.remove();
            }
        }

        // Note: the ViewSwitcher manages view_stack tab selection directly;
        // we do not call set_visible_child here to avoid overriding user navigation.

        // Populate service list once when the list is ready
        if !self.services.is_empty() && !widgets.services_populated {
            widgets.services_populated = true;
            // Remove loading placeholder
            while let Some(row) = widgets.service_list.first_child() {
                widgets.service_list.remove(&row);
            }
            for (service_name, _) in &self.services {
                let label = gtk4::Label::builder()
                    .label(service_name)
                    .margin_start(12)
                    .margin_top(6)
                    .margin_bottom(6)
                    .halign(gtk4::Align::Start)
                    .ellipsize(gtk4::pango::EllipsizeMode::End)
                    .build();
                let row = gtk4::ListBoxRow::new();
                row.set_child(Some(&label));
                widgets.service_list.append(&row);
            }
            // Highlight the auto-selected row
            if let Some(ref selected) = self.selected_service {
                let mut i = 0i32;
                while let Some(row) = widgets.service_list.row_at_index(i) {
                    if row
                        .child()
                        .and_downcast::<gtk4::Label>()
                        .map(|l| l.text().as_str() == selected.as_str())
                        .unwrap_or(false)
                    {
                        widgets.service_list.select_row(Some(&row));
                        break;
                    }
                    i += 1;
                }
            }
        }

        // Replace placeholder pages with real tab widgets once controllers are ready
        replace_placeholder(
            &widgets.tab_containers[TabId::Explore.index()],
            self.explore_controller
                .as_ref()
                .map(|c| c.widget().upcast_ref()),
        );
        replace_placeholder(
            &widgets.tab_containers[TabId::Health.index()],
            self.health_controller
                .as_ref()
                .map(|c| c.widget().upcast_ref()),
        );
        replace_placeholder(
            &widgets.tab_containers[TabId::Logs.index()],
            self.logs_controller
                .as_ref()
                .map(|c| c.widget().upcast_ref()),
        );
        replace_placeholder(
            &widgets.tab_containers[TabId::Metrics.index()],
            self.metrics_controller
                .as_ref()
                .map(|c| c.widget().upcast_ref()),
        );
        replace_placeholder(
            &widgets.tab_containers[TabId::Profiles.index()],
            self.profiles_controller
                .as_ref()
                .map(|c| c.widget().upcast_ref()),
        );
        replace_placeholder(
            &widgets.tab_containers[TabId::Traces.index()],
            self.traces_controller
                .as_ref()
                .map(|c| c.widget().upcast_ref()),
        );

        // Once all controllers are ready, reparent each tab's toolbar into the
        // unified filter bar stack so all controls appear on one line.
        if !widgets.tab_controls_built {
            let all_ready = self.logs_controller.is_some()
                && self.traces_controller.is_some()
                && self.metrics_controller.is_some()
                && self.profiles_controller.is_some()
                && self.health_controller.is_some()
                && self.explore_controller.is_some();

            if all_ready {
                let tab_roots: &[(&str, &gtk4::Box)] = &[
                    ("health", self.health_controller.as_ref().unwrap().widget()),
                    (
                        "metrics",
                        self.metrics_controller.as_ref().unwrap().widget(),
                    ),
                    ("traces", self.traces_controller.as_ref().unwrap().widget()),
                    ("logs", self.logs_controller.as_ref().unwrap().widget()),
                    (
                        "profiles",
                        self.profiles_controller.as_ref().unwrap().widget(),
                    ),
                    (
                        "explore",
                        self.explore_controller.as_ref().unwrap().widget(),
                    ),
                ];

                for (name, root) in tab_roots {
                    let mut found = false;
                    let mut child_opt = root.first_child();
                    while let Some(child) = child_opt {
                        let next = child.next_sibling();
                        if child.widget_name().as_str() == "tab-toolbar" {
                            child.unparent();
                            widgets.tab_controls_stack.add_named(&child, Some(name));
                            found = true;
                            break;
                        }
                        child_opt = next;
                    }
                    if !found {
                        let placeholder = gtk4::Box::new(gtk4::Orientation::Horizontal, 0);
                        widgets
                            .tab_controls_stack
                            .add_named(&placeholder, Some(name));
                    }
                }

                // Sync initial visible page.
                if let Some(name) = widgets.view_stack.visible_child_name() {
                    widgets.tab_controls_stack.set_visible_child_name(&name);
                }

                widgets.tab_controls_built = true;
            }
        }
    }
}

// ── AppModel inherent methods ──────────────────────────────────────────────────

impl AppModel {
    fn forward_service_filter(&self, filter: Option<String>) {
        // Traces and Profiles filter by resource_id (spans/samples store resource_id)
        let resource_id: Option<u32> = filter.as_deref().and_then(|name| {
            self.services
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, id)| *id)
        });
        if let Some(c) = &self.logs_controller {
            let _ = c.sender().emit(LogsInput::ServiceFilter(filter.clone()));
        }
        if let Some(c) = &self.traces_controller {
            let _ = c.sender().emit(TracesInput::ServiceFilter(resource_id));
        }
        if let Some(c) = &self.health_controller {
            let _ = c.sender().emit(HealthInput::ServiceFilter(resource_id));
        }
        if let Some(c) = &self.metrics_controller {
            let _ = c.sender().emit(MetricsInput::ServiceFilter(filter));
        }
        if let Some(c) = &self.profiles_controller {
            let _ = c.sender().emit(ProfilesInput::ServiceFilter(resource_id));
        }
    }

    fn forward_app_time_range(&self, range: AppTimeRange) {
        // Convert to legacy TimeRange for tabs not yet migrated.
        let legacy: TimeRange = range.into();
        let live = range.is_live();
        if let Some(c) = &self.logs_controller {
            let _ = c.sender().emit(LogsInput::SetTimeRange(legacy, live));
        }
        if let Some(c) = &self.traces_controller {
            let _ = c.sender().emit(TracesInput::SetTimeRange(legacy, live));
        }
        if let Some(c) = &self.health_controller {
            let _ = c.sender().emit(HealthInput::SetTimeRange(legacy));
        }
        if let Some(c) = &self.metrics_controller {
            let _ = c.sender().emit(MetricsInput::SetAppTimeRange(range));
        }
        if let Some(c) = &self.profiles_controller {
            let _ = c.sender().emit(ProfilesInput::SetTimeRange(legacy, live));
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Swap out a placeholder page for the real widget once a controller is ready.
///
/// The placeholder is a `gtk4::Box` whose first child is a `gtk4::Spinner`.
/// We detect this to avoid re-swapping on every `update_view` call.
fn replace_placeholder(container: &gtk4::Box, widget: Option<&gtk4::Widget>) {
    let Some(widget) = widget else { return };
    let still_placeholder = container
        .first_child()
        .map(|w| w.is::<gtk4::Spinner>())
        .unwrap_or(false);
    if still_placeholder {
        while let Some(child) = container.first_child() {
            container.remove(&child);
        }
        // Reset alignment set by build_placeholder_page so the real widget fills the page.
        container.set_halign(gtk4::Align::Fill);
        container.set_valign(gtk4::Align::Fill);
        container.set_hexpand(true);
        container.set_vexpand(true);
        container.append(widget);
    }
}

fn build_placeholder_page(tab_name: &str) -> gtk4::Box {
    let page_box = gtk4::Box::builder()
        .orientation(gtk4::Orientation::Vertical)
        .halign(gtk4::Align::Center)
        .valign(gtk4::Align::Center)
        .spacing(12)
        .build();

    let spinner = gtk4::Spinner::new();
    spinner.start();

    let label = gtk4::Label::builder()
        .label(format!("{tab_name} — loading data source…").as_str())
        .css_classes(["title-3", "dim-label"])
        .build();

    page_box.append(&spinner);
    page_box.append(&label);
    page_box
}

async fn init_local_server() -> anyhow::Result<LocalServer> {
    let storage_config = config::default_storage_config()?;
    let server = LocalServer::new(storage_config).await?;
    Ok(server)
}

fn connect_to_env(
    server: &LocalServer,
    env: &config::Environment,
) -> anyhow::Result<AppDataSource> {
    let inner = match &env.kind {
        EnvironmentKind::Local { .. } => DataSource::for_local(server),
        EnvironmentKind::Remote { query_url } => DataSource::for_remote(query_url)?,
    };
    Ok(AppDataSource::new(inner))
}

/// Query the resources table for distinct service names (with resource_id), for the sidebar.
async fn fetch_service_list(ds: &AppDataSource) -> Vec<(String, u32)> {
    let query = "resources last 24h";
    match ds.snapshot_batches(query).await {
        Ok(batches) => decode_services(&batches),
        Err(e) => {
            tracing::warn!("Service list query failed: {:#}", e);
            Vec::new()
        }
    }
}

fn decode_services(batches: &[Vec<u8>]) -> Vec<(String, u32)> {
    use std::io::Cursor;
    let mut services = Vec::new();
    for ipc in batches {
        let cursor = Cursor::new(ipc.as_slice());
        let Ok(mut reader) = StreamReader::try_new(cursor, None) else {
            continue;
        };
        while let Some(Ok(batch)) = reader.next() {
            let schema = batch.schema();
            let name_col = schema
                .index_of("service_name")
                .ok()
                .and_then(|i| batch.column(i).as_any().downcast_ref::<StringViewArray>());
            let id_col = schema
                .index_of("resource_id")
                .ok()
                .and_then(|i| batch.column(i).as_any().downcast_ref::<UInt32Array>());
            if let (Some(names), Some(ids)) = (name_col, id_col) {
                for i in 0..batch.num_rows() {
                    let name = names.value(i);
                    if !name.is_empty() {
                        services.push((name.to_string(), ids.value(i)));
                    }
                }
            }
        }
    }
    services
}

// ── CSS ────────────────────────────────────────────────────────────────────────

const APP_CSS: &str = "
.log-trace { color: alpha(currentColor, 0.4); }
.log-debug { color: alpha(currentColor, 0.65); }
.log-info  { }
.log-warn  { color: @warning_color; }
.log-error { color: @error_color; }
.log-fatal { color: @error_color; font-weight: bold; }

.span-error { color: @error_color; }
.span-ok    { color: @success_color; }
.span-unset { color: alpha(currentColor, 0.6); }

.health-error    { color: @error_color; font-weight: bold; }
.health-degraded { color: @warning_color; }
.health-ok       { color: @success_color; }

progressbar trough { min-height: 8px; }
progressbar.health-bar-ok trough progress       { background: @success_color; min-height: 8px; }
progressbar.health-bar-degraded trough progress { background: @warning_color; min-height: 8px; }
progressbar.health-bar-error trough progress    { background: @error_color;   min-height: 8px; }
progressbar.health-bar-inactive trough progress { background: alpha(currentColor, 0.2); min-height: 8px; }

.health-dot-ok       { color: @success_color; }
.health-dot-degraded { color: @warning_color; }
.health-dot-error    { color: @error_color; }
.health-dot-inactive { color: alpha(currentColor, 0.3); }

.health-mono { font-family: monospace; }

/* Inner content padding for health cards */
.health-card { padding: 16px; }

.metric-pill-gauge     { background: #5E9EFF; color: white; border-radius: 4px; padding: 1px 6px; }
.metric-pill-counter   { background: #4CC74A; color: white; border-radius: 4px; padding: 1px 6px; }
.metric-pill-histogram { background: #FF9933; color: white; border-radius: 4px; padding: 1px 6px; }
.metric-pill-summary   { background: #A566D9; color: white; border-radius: 4px; padding: 1px 6px; }

.log-attr-key   { font-family: monospace; color: alpha(currentColor, 0.6); }
.log-attr-value { font-family: monospace; }

.filter-bar-live   { color: @success_color; }
.filter-bar-paused { color: alpha(currentColor, 0.5); }

.status-ok       { color: @success_color; }
.status-starting { color: @warning_color; }
.status-stopped  { color: alpha(currentColor, 0.4); }
.status-error    { color: @error_color; }
.status-error-clickable       { color: @error_color; }
.status-error-clickable:hover { opacity: 0.6; text-decoration-line: underline; }
";

// ── TabId ──────────────────────────────────────────────────────────────────────

mod tab {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TabId {
        Health,
        Metrics,
        Traces,
        Logs,
        Profiles,
        Explore,
    }

    impl TabId {
        pub fn stack_name(self) -> &'static str {
            match self {
                Self::Health => "health",
                Self::Metrics => "metrics",
                Self::Traces => "traces",
                Self::Logs => "logs",
                Self::Profiles => "profiles",
                Self::Explore => "explore",
            }
        }

        /// Index in `all_tabs()` — used to find the container in `tab_containers`.
        pub fn index(self) -> usize {
            match self {
                Self::Health => 0,
                Self::Metrics => 1,
                Self::Traces => 2,
                Self::Logs => 3,
                Self::Profiles => 4,
                Self::Explore => 5,
            }
        }

        pub fn all_tabs() -> &'static [(&'static str, &'static str, &'static str)] {
            &[
                ("health", "heart-outline-symbolic", "Health"),
                ("metrics", "chart-line-symbolic", "Metrics"),
                ("traces", "network-transmit-receive-symbolic", "Traces"),
                ("logs", "text-x-generic-symbolic", "Logs"),
                ("profiles", "utilities-system-monitor-symbolic", "Profiles"),
                ("explore", "system-search-symbolic", "Explore"),
            ]
        }
    }
}

fn show_error_dialog(parent: &adw::ApplicationWindow, title: &str, message: &str) {
    let dialog = gtk4::MessageDialog::builder()
        .transient_for(parent)
        .modal(true)
        .message_type(gtk4::MessageType::Error)
        .buttons(gtk4::ButtonsType::Close)
        .text(title)
        .secondary_text(message)
        .build();
    dialog.connect_response(|d, _| d.destroy());
    dialog.present();
}
