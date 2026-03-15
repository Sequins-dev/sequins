import SwiftUI

/// Unified filter bar containing universal controls and tab-specific filters
struct UnifiedFilterBar: View {
    @Environment(AppStateViewModel.self) private var appState
    let selectedTab: NavigationItem
    @Bindable var tracesViewModel: TracesViewModel
    @Bindable var metricsViewModel: MetricsViewModel
    @Bindable var profilesViewModel: ProfilesViewModel
    @Bindable var logsViewModel: LogsViewModel
    @Bindable var healthViewModel: HealthViewModel
    @Bindable var exploreViewModel: ExploreViewModel
    @Binding var logsSearchText: String
    @Binding var metricsSearchText: String

    var body: some View {
        @Bindable var appState = appState

        HStack(spacing: 12) {
            // === UNIVERSAL CONTROLS (always visible) ===

            // Live/Pause toggle FIRST
            Button(action: { appState.isLive.toggle() }) {
                HStack(spacing: 4) {
                    Circle()
                        .fill(appState.isLive ? Color.green : Color.gray)
                        .frame(width: 6, height: 6)
                    Text(appState.isLive ? "Live" : "Paused")
                        .font(.caption)
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 4)
                .background(.quaternary)
                .cornerRadius(4)
            }
            .buttonStyle(.plain)
            .fixedSize()

            // Time range control
            TimeRangeControl()
                .fixedSize()

            // Separator between universal and tab-specific
            Divider()
                .frame(height: 20)

            // === TAB-SPECIFIC CONTROLS ===
            switch selectedTab {
            case .health:
                HealthFilterControls(viewModel: healthViewModel)
            case .traces:
                TracesFilterControls(viewModel: tracesViewModel)
            case .logs:
                LogsFilterControls(searchText: $logsSearchText, sortNewestFirst: $logsViewModel.sortNewestFirst, onExport: { logsViewModel.exportLogs() })
            case .metrics:
                MetricsFilterControls(viewModel: metricsViewModel, searchText: $metricsSearchText)
            case .profiles:
                ProfilesFilterControls(viewModel: profilesViewModel)
            case .explore:
                ExploreFilterControls(viewModel: exploreViewModel)
            }

            Spacer()
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
        .background(.quaternary.opacity(0.3))
    }
}

#Preview("UnifiedFilterBar - Traces") {
    struct PreviewWrapper: View {
        @State private var tracesVM = TracesViewModel()
        @State private var metricsVM = MetricsViewModel()
        @State private var profilesVM = ProfilesViewModel()
        @State private var logsVM = LogsViewModel()
        @State private var healthVM = HealthViewModel()
        @State private var exploreVM = ExploreViewModel()
        @State private var logsSearch = ""
        @State private var metricsSearch = ""

        var body: some View {
            UnifiedFilterBar(
                selectedTab: .traces,
                tracesViewModel: tracesVM,
                metricsViewModel: metricsVM,
                profilesViewModel: profilesVM,
                logsViewModel: logsVM,
                healthViewModel: healthVM,
                exploreViewModel: exploreVM,
                logsSearchText: $logsSearch,
                metricsSearchText: $metricsSearch
            )
            .environment(AppStateViewModel())
        }
    }
    return PreviewWrapper()
}

#Preview("UnifiedFilterBar - Metrics") {
    struct PreviewWrapper: View {
        @State private var tracesVM = TracesViewModel()
        @State private var metricsVM = MetricsViewModel()
        @State private var profilesVM = ProfilesViewModel()
        @State private var logsVM = LogsViewModel()
        @State private var healthVM = HealthViewModel()
        @State private var exploreVM = ExploreViewModel()
        @State private var logsSearch = ""
        @State private var metricsSearch = ""

        var body: some View {
            UnifiedFilterBar(
                selectedTab: .metrics,
                tracesViewModel: tracesVM,
                metricsViewModel: metricsVM,
                profilesViewModel: profilesVM,
                logsViewModel: logsVM,
                healthViewModel: healthVM,
                exploreViewModel: exploreVM,
                logsSearchText: $logsSearch,
                metricsSearchText: $metricsSearch
            )
            .environment(AppStateViewModel())
        }
    }
    return PreviewWrapper()
}
