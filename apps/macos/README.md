# Sequins macOS App

Native macOS application for visualizing OpenTelemetry data using the Sequins platform.

## Overview

Sequins is a local-first observability tool that provides an embedded OTLP endpoint and rich visualizations for traces, logs, metrics, and profiles. This macOS app provides a native interface to interact with Sequins telemetry data.

## Features

- **Two-Column Layout**: Service list sidebar with main content area
- **Tab-Based Navigation**: Traces, Logs, Metrics, Profiles tabs along the top
- **Traces View**: Table view of distributed traces with filtering
- **Time Range Control**: Quick presets (5m, 15m, 1h, 6h, 24h) and custom ranges
- **Service List**: Browse services with span counts and last-seen timestamps
- **Connection Modes**:
  - **Local Mode**: Embedded OTLP server with direct database access
  - **Remote Mode**: Connect to sequins-daemon for enterprise deployment
- **Settings**: Configure OTLP ports, database path, and connection settings

## Requirements

- macOS 14.0 or later
- Swift 6.2+
- sequins-ffi library (Rust FFI layer)

## Building

Build the app:

```bash
swift build
```

Run the app:

```bash
swift run
```

Build for release:

```bash
swift build -c release
```

## Architecture

The app follows a clean three-layer architecture:

```
┌─────────────────┐
│   Sequins App   │  SwiftUI views, MVVM pattern
└────────┬────────┘
         │
┌────────▼────────┐
│  SequinsData    │  Swift types, DataSource abstraction
└────────┬────────┘
         │
┌────────▼────────┐
│  sequins-ffi    │  Rust FFI layer
└─────────────────┘
```

### Project Structure

```
Sources/Sequins/
├── SequinsApp.swift          # App entry point
├── Models/
│   └── Service.swift         # Service model
├── ViewModels/
│   ├── AppStateViewModel.swift    # Global app state
│   └── TracesViewModel.swift      # Traces view logic
└── Views/
    ├── ContentView.swift          # Main navigation layout
    ├── ServiceListView.swift      # Service sidebar
    ├── TracesView.swift           # Traces table view
    ├── TimeRangeControl.swift     # Time range picker
    └── SettingsView.swift         # Settings panel
```

## Dependencies

- **SequinsData** (`../SequinsData`): Swift package wrapping sequins-ffi
- **sequins-ffi**: Rust library providing FFI interface

## Local Mode

In local mode, the app:
1. Creates an embedded database at `~/Library/Application Support/Sequins/sequins.db`
2. Starts OTLP endpoints:
   - gRPC: `localhost:4317` (default)
   - HTTP: `localhost:4318` (default)
3. Stores telemetry data locally for visualization

Configure your applications to send OTLP data:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

## Remote Mode

In remote mode, the app:
1. Connects to a sequins-daemon instance
2. Queries telemetry via HTTP API
3. No local storage or OTLP endpoints

Configure remote URLs in Settings:
- Query URL: `http://localhost:8080/query`
- Management URL: `http://localhost:8080/management`

## Current Status

**Implemented:**
- ✅ Basic app structure with NavigationSplitView
- ✅ Service list with mock data
- ✅ Traces view with Table component
- ✅ Time range control with presets
- ✅ Settings panel with connection mode
- ✅ DataSource integration (local and remote)
- ✅ Error handling with alerts

**Coming Soon:**
- 🚧 Service discovery from database
- 🚧 Real span loading and display
- 🚧 Trace detail view (waterfall diagram)
- 🚧 Logs view
- 🚧 Metrics view
- 🚧 Profiles view
- 🚧 Search and filtering
- 🚧 Real-time updates (live mode)

## Development

The app uses SwiftUI with the `@Observable` macro for state management. Key patterns:

- **MVVM Architecture**: ViewModels handle business logic, Views handle presentation
- **Environment Objects**: AppStateViewModel injected via `.environment()`
- **Async/Await**: All data loading uses Swift concurrency
- **@MainActor**: ViewModels marked with `@MainActor` for UI thread safety

## Related Projects

- **SequinsData** (`../SequinsData`): Swift package for data access
- **sequins-ffi** (`~/Code/rust/sequins/crates/ffi`): Rust FFI library
- **sequins** (`~/Code/rust/sequins`): Main Sequins project

## License

See the main Sequins project for license information.
