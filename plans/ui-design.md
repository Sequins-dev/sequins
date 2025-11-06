# UI Component Hierarchy & Design

[← Back to Index](INDEX.md)

**Related Documentation:** [state-management.md](state-management.md) | [data-models.md](data-models.md) | [architecture.md](architecture.md)

---

## Application Window Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  Sequins                                                 ●  ●  ● │
├────────────┬────────────────────────────────────────────────────┤
│            │  api-gateway │ Instance: 3 │ Uptime: 2h 34m       │ Title Bar
│            ├────────────────────────────────────────────────────┤
│            │  Logs  Metrics  Traces  Profiles                   │ Tab List
│  Services  ├────────────────────────────────────────────────────┤
│            │ ⏰ Last 1h  🔍 Search...  ⚙️ Filters ▼            │ Filter Bar (per-tab)
│ 🔍 Filter  ├────────────────────────────────────────────────────┤
│ ┌────────┐ │                                                     │
│ │gateway │ │                 Main Content View                 │
│ │api     │ │              (Selected Tab Content)               │
│ │auth    │ │                                                     │
│ │database│ │                                                     │
│ │cache   │ │                                                     │
│ │worker  │ │                                                     │
│ │...     │ │                                                     │
│ └────────┘ │                                                     │
│            │                                                     │
│            │                                                     │
└────────────┴────────────────────────────────────────────────────┘
```

## Component Tree

```
AppWindow
├── ServiceNavigator
│   ├── FilterInput
│   └── ServiceList
│       └── ServiceItem (multiple)
│           ├── ServiceName
│           └── HealthIndicator
│
└── MainPane
    ├── TitleBar
    │   ├── ServiceInfo
    │   └── WindowControls
    │
    ├── TabList
    │   ├── Tab (Logs)
    │   ├── Tab (Metrics)
    │   ├── Tab (Traces)
    │   └── Tab (Profiles)
    │
    └── ContentView (one of:)
        ├── LogsView
        │   ├── FilterBar
        │   │   ├── TimeRangePicker (shared state)
        │   │   ├── SearchInput
        │   │   └── SeverityFilter
        │   └── LogList
        │       └── LogRow (multiple)
        │           ├── LogTimestamp
        │           ├── LogMessage
        │           └── LogDetail (expandable)
        │
        ├── MetricsView
        │   ├── FilterBar
        │   │   ├── TimeRangePicker (shared state)
        │   │   └── MetricTypeFilter
        │   ├── LatencyHistogram
        │   ├── StatusCodeHistogram
        │   ├── CPUChart
        │   ├── MemoryChart
        │   └── CustomMetricCharts
        │
        ├── TracesView
        │   ├── FilterBar
        │   │   ├── TimeRangePicker (shared state)
        │   │   ├── StatusFilter
        │   │   └── DurationRangeFilter
        │   ├── TraceTimeline (full width)
        │   └── BottomPane
        │       ├── TraceList (left)
        │       │   └── TraceRow (multiple)
        │       └── SpanDetails (right)
        │
        └── ProfilesView
            ├── FilterBar
            │   ├── TimeRangePicker (shared state)
            │   └── ProfileTypeFilter
            ├── FlameGraph (full width)
            └── FrameDetails (bottom)
```

## View Specifications

### LogsView Layout

```
┌────────────────────────────────────────────────────────────────┐
│ ⏰ Last 1h  🔍 Search...  Severity: [All ▼]                   │ FilterBar
├────────────────────────────────────────────────────────────────┤
│ 2025-01-15 14:32:10.123  INFO   GET /api/users 200 32ms      │
│ 2025-01-15 14:32:10.145  ERROR  Database query failed        ▼│
│   ├─ error.type: SQLException                                  │
│   ├─ error.message: Connection timeout                         │
│   ├─ db.system: postgresql                                     │
│   └─ trace_id: 7f8d9c...                                       │
│ 2025-01-15 14:32:10.167  INFO   Cache hit for key: user:123   │
└────────────────────────────────────────────────────────────────┘
```

**Filter Bar (Logs-specific):**
- **Time Range** (shared across tabs)
- **Search** (full-text search on log body)
- **Severity** dropdown (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)

**Features:**
- Virtualized scrolling for large log volumes
- Color-coded severity levels
- Expandable log details showing attributes
- Clickable trace IDs for navigation
- Timestamp formatting with millisecond precision
- Full-text search with highlighting

**Implementation Notes:**
- Use GPUI's `List` component for virtualization
- Implement custom `LogRow` view with expand/collapse state
- Style severity levels with semantic colors:
  - TRACE: Gray
  - DEBUG: Blue
  - INFO: Green
  - WARN: Yellow
  - ERROR: Red
  - FATAL: Dark Red

### MetricsView Layout

```
┌────────────────────────────────────────────────────────────────┐
│ ⏰ Last 1h  Metric Type: [All ▼]                              │ FilterBar
├────────────────────────────────────────────────────────────────┤
│ Request Latency (p50, p95, p99)     Status Codes             │
│ [Histogram Chart]                    [Bar Chart]              │
├────────────────────────────────────────────────────────────────┤
│ CPU Usage                            Memory Usage              │
│ [Time Series]                        [Time Series]             │
├────────────────────────────────────────────────────────────────┤
│ Event Loop Lag                       GC Pause Time             │
│ [Time Series]                        [Time Series]             │
└────────────────────────────────────────────────────────────────┘
```

**Filter Bar (Metrics-specific):**
- **Time Range** (shared across tabs)
- **Metric Type** dropdown (Gauge, Counter, Histogram, Summary)

**Features:**
- Grid layout with 2 columns
- Histogram visualization for latency percentiles
- Time series charts for resource metrics
- Bar charts for categorical data (status codes)
- Real-time updates as new metrics arrive
- Customizable time range

**Chart Types:**
- **Histogram:** Latency distribution with percentile markers
- **Time Series:** Line charts for CPU, memory, custom metrics
- **Bar Chart:** Status codes, error rates
- **Gauge:** Current values with thresholds

### TracesView Layout

```
┌────────────────────────────────────────────────────────────────┐
│ ⏰ Last 1h  Status: [All ▼]  Duration: [Any ▼]               │ FilterBar
├────────────────────────────────────────────────────────────────┤
│                    Trace Timeline                              │
│ Gateway ████████████████████                200ms              │
│   API     ████████████                      150ms              │
│     DB      ████                            50ms               │
│     Cache     ██                            20ms               │
├─────────────────────────┬──────────────────────────────────────┤
│ Trace List              │ Span Details                         │
│                         │                                      │
│ ✓ GET /users (200ms)   │ Span: GET /api/users                │
│ ✗ POST /order (ERROR)   │ Service: api-gateway                │
│ ✓ GET /products (100ms) │ Duration: 200ms                     │
│                         │ Status: OK                           │
│                         │ Attributes:                          │
│                         │   http.method: GET                   │
│                         │   http.status_code: 200              │
└─────────────────────────┴──────────────────────────────────────┘
```

**Filter Bar (Traces-specific):**
- **Time Range** (shared across tabs)
- **Status** dropdown (All, OK, Error)
- **Duration Range** dropdown (Any, <100ms, 100-500ms, 500ms-1s, >1s)

**Features:**
- Waterfall timeline showing span hierarchy
- Indentation showing parent-child relationships
- Color-coded by service
- Duration bars proportional to time
- Trace list with status indicators
- Span details panel with attributes

**Implementation Notes:**
- Timeline uses horizontal bar chart
- Calculate offset and width based on timestamps
- Indent child spans under parents
- Click on span in timeline to show details
- Click on trace in list to load timeline

### ProfilesView Layout

```
┌────────────────────────────────────────────────────────────────┐
│ ⏰ Last 1h  Type: [All ▼]                                     │ FilterBar
├────────────────────────────────────────────────────────────────┤
│                     Flame Graph                                │
│ main ████████████████████████████████████████████████████████ │
│  ├─ process_request ████████████████████████████████████      │
│  │   ├─ validate_input ████                                   │
│  │   └─ execute_query ████████████████████████               │
│  └─ send_response ████                                         │
├────────────────────────────────────────────────────────────────┤
│ Frame Details                                                  │
│ Function: execute_query                                        │
│ File: src/db/query.rs:142                                     │
│ Samples: 1,234 (45.2%)                                        │
│ Self: 234 (8.5%)                                              │
└────────────────────────────────────────────────────────────────┘
```

**Filter Bar (Profiles-specific):**
- **Time Range** (shared across tabs)
- **Profile Type** dropdown (All, CPU, Memory, Goroutine)

**Features:**
- Interactive flame graph visualization
- Click to zoom into specific function
- Hover for details
- Color-coded by module or sample count
- Frame details panel
- Source file links (if available)

**Implementation Notes:**
- Parse pprof format into tree structure
- Calculate bar widths based on sample counts
- Implement zoom navigation
- Show percentage of total samples
- Distinguish "self" time vs "total" time

## Common UI Components

### TimeRangePicker

Dropdown with preset ranges (shared state across all tabs):
- Last 5 minutes
- Last 15 minutes
- Last 1 hour
- Last 6 hours
- Last 24 hours
- Custom range...

**State Management:**
- Stored in global `AppState` (not view-specific)
- Persists when switching tabs
- Updates all views when changed

### SearchInput (Logs only)

- Debounced input (300ms)
- Full-text search support
- Search syntax help tooltip
- Clear button

### SeverityFilter (Logs only)

Multi-select dropdown with:
- TRACE, DEBUG, INFO, WARN, ERROR, FATAL
- Apply/Reset buttons

### StatusFilter (Traces only)

Dropdown with:
- All, OK, Error

### DurationRangeFilter (Traces only)

Dropdown with:
- Any, <100ms, 100-500ms, 500ms-1s, >1s

### MetricTypeFilter (Metrics only)

Dropdown with:
- All, Gauge, Counter, Histogram, Summary

### ProfileTypeFilter (Profiles only)

Dropdown with:
- All, CPU, Memory, Goroutine

### ServiceNavigator

Left sidebar component showing all discovered services:

**Features:**
- Flat list of all services (sorted alphabetically)
- Text filter input at top (debounced 300ms)
- Click service to select and load its data
- Health indicator dot (green/yellow/red) for each service
- Instance count badge
- Virtualized scrolling for large service counts

**Layout:**
```
┌──────────────┐
│ 🔍 Filter... │
├──────────────┤
│ ● gateway (3)│ ← selected
│ ● api (5)    │
│ ● auth (2)   │
│ ● database(1)│
│ ● cache (4)  │
│ ● worker (8) │
└──────────────┘
```

**Implementation:**
```rust
struct ServiceNavigator {
    filter: String,
    services: Vec<ServiceInfo>,
    selected_service: Option<String>,
}

struct ServiceInfo {
    name: String,
    instance_count: usize,
    health: HealthStatus, // Green, Yellow, Red
    last_seen: DateTime,
}
```

## GPUI-Specific Considerations

### Reactive Updates

```rust
impl AppWindow {
    fn update_data(&mut self, cx: &mut ViewContext<Self>) {
        // Update data
        self.state.traces = new_traces;

        // Notify GPUI to re-render
        cx.notify();
    }
}
```

### Event Handling

```rust
impl LogRow {
    fn render(&mut self, cx: &mut ViewContext<Self>) -> impl IntoElement {
        div()
            .on_click(cx.listener(|this, _event, cx| {
                this.toggle_expanded(cx);
            }))
            .child(/* ... */)
    }

    fn toggle_expanded(&mut self, cx: &mut ViewContext<Self>) {
        self.is_expanded = !self.is_expanded;
        cx.notify();
    }
}
```

### Styling

```rust
impl LogRow {
    fn render(&mut self, cx: &mut ViewContext<Self>) -> impl IntoElement {
        div()
            .flex()
            .items_center()
            .gap_2()
            .px_4()
            .py_2()
            .bg(cx.theme().colors().surface)
            .hover(|style| style.bg(cx.theme().colors().surface_hover))
            .child(self.render_timestamp())
            .child(self.render_severity())
            .child(self.render_message())
    }
}
```

---

**Last Updated:** 2025-11-05
