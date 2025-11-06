use anyhow::Result;
use gpui::{
    point, px, size, App, AppContext, Application, Bounds, WindowBounds,
    WindowHandle, WindowOptions,
};
use image::ImageReader;
use sequins_app::ui::{AppWindow, MockApi};
use std::io::Cursor;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use tray_icon::{
    menu::{Menu as TrayMenu, MenuEvent, MenuItem as TrayMenuItem},
    TrayIconBuilder, TrayIconEvent,
};

struct SequinsApp {
    // App state - currently empty, will hold connections to storage/server later
}

impl SequinsApp {
    fn new() -> Result<Self> {
        Ok(Self {})
    }
}


// Global flag for server state
static SERVER_RUNNING: AtomicBool = AtomicBool::new(false);

/// Load the tray icon from embedded PNG
fn load_tray_icon() -> tray_icon::Icon {
    // Embed the PNG at compile time (path relative to workspace root)
    let icon_bytes = include_bytes!("../../../logo.png");

    // Decode at runtime
    let img = ImageReader::new(Cursor::new(icon_bytes))
        .with_guessed_format()
        .expect("Failed to guess image format")
        .decode()
        .expect("Failed to decode icon");

    // Resize to standard tray icon size (32x32) for consistency
    let img = img.resize_exact(32, 32, image::imageops::FilterType::Lanczos3);
    let rgba = img.to_rgba8();

    tray_icon::Icon::from_rgba(rgba.into_raw(), 32, 32)
        .expect("Failed to create icon")
}

fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    tracing::info!("Sequins starting...");

    // Initialize the app state
    let _app_state = Arc::new(SequinsApp::new().expect("Failed to initialize app"));

    tracing::info!("App state initialized, creating GPUI application...");

    // Run GPUI application - needed for macOS event loop for tray icon
    let app = Application::new();
    tracing::info!("GPUI Application created, calling run...");

    app.run(move |cx: &mut App| {
        tracing::info!("GPUI application started!");

        tracing::info!("GPUI initialized");

        // Initialize tray icon after GPUI is set up
        tracing::info!("Creating tray menu...");
        let tray_menu = TrayMenu::new();
        let show_item = TrayMenuItem::new("Show Window", true, None);
        let toggle_server_item = TrayMenuItem::new("Toggle OTLP Server", true, None);
        let quit_item = TrayMenuItem::new("Quit", true, None);
        tracing::info!("Menu items created");

        tray_menu
            .append(&show_item)
            .expect("Failed to add show item");
        tray_menu
            .append(&tray_icon::menu::PredefinedMenuItem::separator())
            .expect("Failed to add separator");
        tray_menu
            .append(&toggle_server_item)
            .expect("Failed to add toggle server item");
        tray_menu
            .append(&tray_icon::menu::PredefinedMenuItem::separator())
            .expect("Failed to add separator");
        tray_menu
            .append(&quit_item)
            .expect("Failed to add quit item");

        // Load the tray icon from embedded logo.png
        tracing::info!("Loading tray icon from logo.png...");
        let icon = load_tray_icon();
        tracing::info!("Icon loaded successfully");

        // Create tray icon and leak it intentionally so it lives for app lifetime
        tracing::info!("Building tray icon...");
        let tray_icon = TrayIconBuilder::new()
            .with_menu(Box::new(tray_menu))
            .with_tooltip("Sequins - Observability Platform")
            .with_icon(icon)
            .build()
            .expect("Failed to create tray icon");
        tracing::info!("Tray icon built successfully!");

        // Leak the tray icon so it's not dropped
        std::mem::forget(tray_icon);
        tracing::info!("Tray icon leaked (intentionally), should be visible now");

        // Create channel for window open requests
        let (window_tx, window_rx) = mpsc::channel::<()>();

        // Handle tray menu events
        let menu_channel = MenuEvent::receiver();
        let show_id = show_item.id().clone();
        let toggle_server_id = toggle_server_item.id().clone();
        let quit_id = quit_item.id().clone();

        std::thread::spawn(move || {
            while let Ok(event) = menu_channel.recv() {
                if event.id == show_id {
                    tracing::info!("Show Window clicked");
                    // Send signal to open window
                    let _ = window_tx.send(());
                } else if event.id == toggle_server_id {
                    let was_running = SERVER_RUNNING.fetch_xor(true, Ordering::SeqCst);
                    if was_running {
                        tracing::info!("Stopping OTLP ingest server...");
                        // TODO: Actually stop the OTLP server here
                    } else {
                        tracing::info!("Starting OTLP ingest server...");
                        // TODO: Actually start the OTLP server here
                    }
                } else if event.id == quit_id {
                    tracing::info!("Quitting Sequins...");
                    std::process::exit(0);
                }
            }
        });

        // Track the main window handle to prevent opening multiple windows
        let main_window: Arc<Mutex<Option<WindowHandle<AppWindow<MockApi>>>>> = Arc::new(Mutex::new(None));

        // Poll for window open requests on the main thread
        // We need to periodically check the channel and open the window when requested
        let main_window_clone = main_window.clone();
        cx.spawn(async move |cx| {
            loop {
                // Check if there's a window open request (non-blocking)
                if window_rx.try_recv().is_ok() {
                    let mut window_guard = main_window_clone.lock().unwrap();

                    // Check if we already have a window
                    let window_exists = window_guard.as_ref().map_or(false, |handle| {
                        // Check if the window is still valid by trying to read it
                        cx.read_window(handle, |_, _| {}).is_ok()
                    });

                    if window_exists {
                        tracing::info!("Window already exists, activating it...");
                        if let Some(handle) = window_guard.as_ref() {
                            // Activate the existing window
                            let _ = cx.update_window(handle.clone().into(), |_, _, cx| {
                                cx.activate(true);
                            });
                        }
                    } else {
                        tracing::info!("Opening new window on main thread...");

                        let window_size = size(px(1200.0), px(800.0));
                        let bounds = Bounds::new(point(px(100.0), px(100.0)), window_size);

                        let api = MockApi::new();
                        match cx.open_window(
                            WindowOptions {
                                window_bounds: Some(WindowBounds::Windowed(bounds)),
                                window_min_size: Some(size(px(1024.0), px(768.0))),
                                ..Default::default()
                            },
                            |_window, cx| {
                                cx.new(|cx| AppWindow::new(api.clone(), cx))
                            },
                        ) {
                            Ok(window) => {
                                tracing::info!("Window opened successfully!");
                                *window_guard = Some(window);
                            }
                            Err(e) => {
                                tracing::error!("Failed to open window: {}", e);
                            }
                        }
                    }
                }

                // Sleep briefly before checking again
                cx.background_executor()
                    .timer(std::time::Duration::from_millis(100))
                    .await;
            }
        })
        .detach();

        // Handle tray icon click events
        let icon_channel = TrayIconEvent::receiver();
        std::thread::spawn(move || {
            while let Ok(_event) = icon_channel.recv() {
                tracing::info!("Tray icon clicked");
            }
        });

        // Open window immediately at startup
        tracing::info!("Opening main window at startup...");
        let window_size = size(px(1200.0), px(800.0));
        let bounds = Bounds::new(point(px(100.0), px(100.0)), window_size);

        let api = MockApi::new();
        match cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                window_min_size: Some(size(px(1024.0), px(768.0))),
                ..Default::default()
            },
            |_window, cx| {
                cx.new(|cx| AppWindow::new(api.clone(), cx))
            },
        ) {
            Ok(window) => {
                tracing::info!("Window opened successfully at startup!");
                *main_window.lock().unwrap() = Some(window);
            }
            Err(e) => {
                tracing::error!("Failed to open window at startup: {}", e);
            }
        }

        cx.activate(true);
    });

    Ok(())
}
