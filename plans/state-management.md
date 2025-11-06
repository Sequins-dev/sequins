# State Management

[← Back to Index](INDEX.md)

**Related Documentation:** [ui-design.md](ui-design.md) | [workspace-and-crates.md](workspace-and-crates.md)

---

## Application State

```rust
use opentelemetry::trace::{TraceId, SpanId};

pub struct AppState {
    // Core state
    pub services: Vec<Service>,
    pub selected_service: Option<String>,
    pub selected_tab: Tab,

    // Shared filter state (persists across tabs)
    pub time_range: TimeRange,

    // Tab-specific filter state
    pub logs_filters: LogsFilters,
    pub traces_filters: TracesFilters,
    pub metrics_filters: MetricsFilters,
    pub profiles_filters: ProfilesFilters,

    // Data
    pub traces: Vec<Trace>,
    pub logs: Vec<LogEntry>,
    pub metrics: HashMap<String, Vec<MetricDataPoint>>,
    pub profiles: Vec<Profile>,

    // UI state
    pub selected_trace: Option<TraceId>,
    pub selected_span: Option<SpanId>,
    pub expanded_logs: HashSet<String>,
    pub selected_profile: Option<String>,
    pub selected_frame: Option<String>,

    // Settings
    pub settings: Settings,
}

pub enum Tab {
    Logs,
    Metrics,
    Traces,
    Profiles,
}

/// UI state for time range selection
/// Combines the time window with preset selection state
pub struct TimeRange {
    /// The actual time window (strongly-typed timestamps)
    pub window: TimeWindow,

    /// Which preset was selected (for UI button state)
    pub preset: TimeRangePreset,
}

impl TimeRange {
    /// Create from preset (convenience constructor)
    pub fn from_preset(preset: TimeRangePreset) -> Self {
        let window = match preset {
            TimeRangePreset::Last5Minutes => TimeWindow::last_minutes(5),
            TimeRangePreset::Last15Minutes => TimeWindow::last_minutes(15),
            TimeRangePreset::Last1Hour => TimeWindow::last_hour(),
            TimeRangePreset::Last6Hours => TimeWindow::last_minutes(6 * 60),
            TimeRangePreset::Last24Hours => TimeWindow::last_day(),
            TimeRangePreset::Custom => {
                // For custom, caller should use TimeRange::custom() instead
                TimeWindow::last_hour()
            }
        };
        Self { window, preset }
    }

    /// Create custom time range (user picked specific start/end)
    pub fn custom(window: TimeWindow) -> Self {
        Self {
            window,
            preset: TimeRangePreset::Custom,
        }
    }

    /// Convenience accessors (delegate to window)
    pub fn start(&self) -> Timestamp {
        self.window.start()
    }

    pub fn end(&self) -> Timestamp {
        self.window.end()
    }

    pub fn contains(&self, timestamp: Timestamp) -> bool {
        self.window.contains(timestamp)
    }
}

pub enum TimeRangePreset {
    Last5Minutes,
    Last15Minutes,
    Last1Hour,
    Last6Hours,
    Last24Hours,
    Custom,  // User selected custom start/end dates
}

// Tab-specific filter states
pub struct LogsFilters {
    pub search_query: String,
    pub severity: Vec<LogSeverity>, // TRACE, DEBUG, INFO, WARN, ERROR, FATAL
}

pub struct TracesFilters {
    pub status: Option<TraceStatus>, // All, OK, Error
    pub duration_range: DurationRange, // Any, <100ms, 100-500ms, etc.
}

pub struct MetricsFilters {
    pub metric_type: Option<MetricType>, // All, Gauge, Counter, Histogram, Summary
}

pub struct ProfilesFilters {
    pub profile_type: Option<ProfileType>, // All, CPU, Memory, Goroutine
}

pub enum DurationRange {
    Any,
    LessThan100ms,
    Between100And500ms,
    Between500msAnd1s,
    GreaterThan1s,
}

pub struct Settings {
    pub grpc_port: u16,
    pub http_port: u16,
    pub retention_hours: u32,
    pub theme: Theme,
    pub db_path: PathBuf,
}
```

## Filter State Architecture

Sequins uses a two-tier filter system:

### Shared Filters (Global State)

**`time_range`** - Shared across ALL tabs
- When user changes time range in Logs view, the selection persists when switching to Traces view
- Stored at top-level `AppState`
- Updates trigger data refresh for the currently visible tab
- Example: User selects "Last 1 hour" in Logs, switches to Traces, still sees "Last 1 hour"

### Tab-Specific Filters (Per-View State)

Each tab has its own filter state that is independent:

**Logs (`logs_filters`)**
- `search_query`: Full-text search
- `severity`: TRACE, DEBUG, INFO, WARN, ERROR, FATAL

**Traces (`traces_filters`)**
- `status`: All, OK, Error
- `duration_range`: Any, <100ms, 100-500ms, 500ms-1s, >1s

**Metrics (`metrics_filters`)**
- `metric_type`: All, Gauge, Counter, Histogram, Summary

**Profiles (`profiles_filters`)**
- `profile_type`: All, CPU, Memory, Goroutine

### Filter Persistence Behavior

```rust
// User flow example:
// 1. User is on Logs tab, sets severity to ERROR
app_state.set_logs_severity(vec![LogSeverity::Error], cx);
// → Only affects Logs view

// 2. User switches to Traces tab
app_state.set_tab(Tab::Traces, cx);
// → Severity filter is still ERROR (preserved), but not visible/active in Traces

// 3. User changes time range to "Last 6 hours"
app_state.set_time_range(TimeRange::from_preset(TimeRangePreset::Last6Hours), cx);
// → Affects Traces view immediately

// 4. User switches back to Logs tab
app_state.set_tab(Tab::Logs, cx);
// → Time range is still "Last 6 hours" (shared)
// → Severity is still ERROR (preserved)
```

## State Updates

```rust
impl AppState {
    pub fn select_service(&mut self, service_name: String, cx: &mut ModelContext<Self>) {
        self.selected_service = Some(service_name);
        self.refresh_data(cx);
        cx.notify();
    }

    pub fn set_tab(&mut self, tab: Tab, cx: &mut ModelContext<Self>) {
        self.selected_tab = tab;
        self.refresh_data(cx);
        cx.notify();
    }

    // Shared filter: affects ALL tabs
    pub fn set_time_range(&mut self, range: TimeRange, cx: &mut ModelContext<Self>) {
        self.time_range = range;
        self.refresh_data(cx); // Refreshes current tab's data
        cx.notify();
    }

    // Tab-specific filters: only affect their respective tab
    pub fn set_logs_severity(&mut self, severity: Vec<LogSeverity>, cx: &mut ModelContext<Self>) {
        self.logs_filters.severity = severity;
        if matches!(self.selected_tab, Tab::Logs) {
            self.refresh_data(cx);
        }
        cx.notify();
    }

    pub fn set_traces_status(&mut self, status: Option<TraceStatus>, cx: &mut ModelContext<Self>) {
        self.traces_filters.status = status;
        if matches!(self.selected_tab, Tab::Traces) {
            self.refresh_data(cx);
        }
        cx.notify();
    }

    fn refresh_data(&mut self, cx: &mut ModelContext<Self>) {
        // Spawn background task to query database
        cx.spawn(|this, mut cx| async move {
            let data = fetch_data_for_current_filters().await?;

            this.update(&mut cx, |state, cx| {
                state.update_data(data);
                cx.notify();
            })?;

            Ok(())
        }).detach();
    }
}
```

## GPUI Reactive Updates

### Model Pattern

GPUI uses a Model pattern for shared state:

```rust
// Create model
let state = cx.new_model(|_| AppState::new());

// Create view that observes model
cx.new_view(|cx| AppWindow::new(state.clone(), cx))
```

### Update Pattern

```rust
impl AppWindow {
    pub fn on_service_selected(&mut self, service_name: String, cx: &mut ViewContext<Self>) {
        // Update model
        self.state.update(cx, |state, cx| {
            state.select_service(service_name, cx);
        });
        // View automatically re-renders because it observes the model
    }
}
```

### Subscription Pattern

```rust
impl AppWindow {
    pub fn new(state: Model<AppState>, cx: &mut ViewContext<Self>) -> Self {
        // Subscribe to model updates
        cx.observe(&state, |this, _state, cx| {
            cx.notify(); // Re-render when state changes
        }).detach();

        Self { state }
    }
}
```

## Data Flow

### User Interaction Flow

```
User Action (e.g., select service)
    ↓
Event Handler in View
    ↓
Update Model State
    ↓
cx.notify() triggers re-render
    ↓
View re-renders with new state
```

### Data Refresh Flow

```
State Change (e.g., time range)
    ↓
refresh_data() spawns background task
    ↓
Query database asynchronously
    ↓
Update model with results
    ↓
cx.notify() triggers re-render
    ↓
View shows new data
```

## Async Operations

### Background Tasks

```rust
impl AppState {
    fn load_traces(&mut self, cx: &mut ModelContext<Self>) {
        let query = self.build_trace_query();

        cx.spawn(|this, mut cx| async move {
            // Async database query
            let traces = fetch_traces(query).await?;

            // Update state on main thread
            this.update(&mut cx, |state, cx| {
                state.traces = traces;
                cx.notify();
            })?;

            Ok(())
        }).detach();
    }
}
```

### Error Handling

```rust
impl AppState {
    fn load_data(&mut self, cx: &mut ModelContext<Self>) {
        cx.spawn(|this, mut cx| async move {
            match fetch_data().await {
                Ok(data) => {
                    this.update(&mut cx, |state, cx| {
                        state.update_data(data);
                        cx.notify();
                    })?;
                }
                Err(e) => {
                    this.update(&mut cx, |state, cx| {
                        state.show_error(e.to_string());
                        cx.notify();
                    })?;
                }
            }
            Ok(())
        }).detach();
    }
}
```

## State Persistence

### Settings Storage

```rust
impl Settings {
    pub fn load() -> Result<Self> {
        let path = Self::settings_path();
        let content = std::fs::read_to_string(path)?;
        let settings: Settings = serde_json::from_str(&content)?;
        Ok(settings)
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::settings_path();
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    fn settings_path() -> PathBuf {
        let config_dir = dirs::config_dir()
            .expect("Failed to get config directory");
        config_dir.join("sequins").join("settings.json")
    }
}
```

### UI State Persistence

```rust
pub struct UIState {
    pub selected_service: Option<String>,
    pub selected_tab: Tab,
    pub time_range_preset: TimeRangePreset,
    pub expanded_logs: HashSet<String>,
}

impl UIState {
    pub fn save(&self) -> Result<()> {
        let path = Self::state_path();
        let content = serde_json::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn load() -> Self {
        let path = Self::state_path();
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn state_path() -> PathBuf {
        let config_dir = dirs::config_dir()
            .expect("Failed to get config directory");
        config_dir.join("sequins").join("ui_state.json")
    }
}
```

## Performance Considerations

### Debouncing

```rust
impl SearchInput {
    fn on_input(&mut self, text: String, cx: &mut ViewContext<Self>) {
        self.pending_text = text;

        // Cancel previous debounce timer
        if let Some(task) = self.debounce_task.take() {
            task.cancel();
        }

        // Start new debounce timer
        let task = cx.spawn(|this, mut cx| async move {
            cx.background_executor().timer(Duration::from_millis(300)).await;

            this.update(&mut cx, |this, cx| {
                this.execute_search(cx);
            })?;

            Ok(())
        });

        self.debounce_task = Some(task);
    }
}
```

### Memoization

```rust
impl TracesView {
    fn render_timeline(&self, cx: &ViewContext<Self>) -> impl IntoElement {
        // Memoize expensive calculations
        let timeline_data = self.compute_timeline_data_cached(cx);

        div()
            .child(self.render_bars(timeline_data))
    }

    fn compute_timeline_data_cached(&self, cx: &ViewContext<Self>) -> TimelineData {
        // Only recompute if traces changed
        if self.timeline_cache_key == self.traces_version {
            return self.timeline_cache.clone();
        }

        let data = self.compute_timeline_data();
        self.timeline_cache = data.clone();
        self.timeline_cache_key = self.traces_version;
        data
    }
}
```

### Virtualization

```rust
impl LogList {
    fn render(&mut self, cx: &mut ViewContext<Self>) -> impl IntoElement {
        // Use GPUI's List component for virtualization
        List::new()
            .item_count(self.logs.len())
            .item_height(px(32.0))
            .render_item(cx.listener(|this, index, cx| {
                this.render_log_row(index, cx)
            }))
    }
}
```

## State Shape Best Practices

### Normalized Data

```rust
// Good: Normalized
pub struct AppState {
    pub services: HashMap<String, Service>,
    pub traces: HashMap<TraceId, Trace>,
    pub spans: HashMap<SpanId, Span>,
}

// Bad: Nested
pub struct AppState {
    pub services: Vec<Service>,
    // Each service contains nested traces, each trace contains nested spans...
}
```

### Derived State

```rust
impl AppState {
    // Compute derived state on-demand
    pub fn filtered_logs(&self) -> Vec<&LogEntry> {
        self.logs
            .iter()
            .filter(|log| self.filters.matches(log))
            .collect()
    }

    // Don't store derived state
    // pub filtered_logs: Vec<LogEntry>, // ❌
}
```

---

**Last Updated:** 2025-11-05
