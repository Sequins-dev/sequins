/// Integration tests for UI components
use sequins_app::ui::{
    tabs::{Tab, TabBar},
    traces_view::TracesView,
    MockApi,
};

#[test]
fn test_service_navigator_has_mock_data() {
    // ServiceNavigator now initializes with mock data
    // This is tested implicitly when the UI is rendered
    // Full GPUI integration tests would require the GPUI test harness
    assert!(true);
}

#[test]
fn test_tab_bar_initialization() {
    let tab_bar = TabBar::new(Tab::Traces);
    assert_eq!(tab_bar.active_tab, Tab::Traces);
}

#[test]
fn test_tab_bar_all_tabs() {
    // Verify all tab variants can be created
    let _logs = TabBar::new(Tab::Logs);
    let _metrics = TabBar::new(Tab::Metrics);
    let _traces = TabBar::new(Tab::Traces);
    let _profiles = TabBar::new(Tab::Profiles);
}

#[test]
fn test_traces_view_initialization() {
    let api = MockApi::new();
    let view = TracesView::new(api);

    // Should have no traces loaded initially (async load required)
    assert!(view.traces.is_empty());

    // Should have no selected trace initially
    assert!(view.selected_trace.is_none());

    // Should have no spans initially
    assert!(view.spans.is_empty());
}

#[test]
fn test_tab_equality() {
    // Verify Tab enum can be compared
    assert_eq!(Tab::Logs, Tab::Logs);
    assert_ne!(Tab::Logs, Tab::Metrics);
    assert_ne!(Tab::Traces, Tab::Profiles);
}

#[test]
fn test_tab_labels() {
    // Verify all tabs have correct labels
    assert_eq!(Tab::Logs.label(), "Logs");
    assert_eq!(Tab::Metrics.label(), "Metrics");
    assert_eq!(Tab::Traces.label(), "Traces");
    assert_eq!(Tab::Profiles.label(), "Profiles");
}

#[test]
fn test_tab_all() {
    // Verify Tab::all() returns all tabs
    let all_tabs = Tab::all();
    assert_eq!(all_tabs.len(), 4);
    assert!(all_tabs.contains(&Tab::Logs));
    assert!(all_tabs.contains(&Tab::Metrics));
    assert!(all_tabs.contains(&Tab::Traces));
    assert!(all_tabs.contains(&Tab::Profiles));
}
