import SwiftUI

/// Main application window containing the navigation split view and tab content
struct MainWindow: View {
    @Environment(AppStateViewModel.self) private var appState
    @State private var selectedTab: NavigationItem
    @State private var columnVisibility: NavigationSplitViewVisibility = .all

    // Shared view models for tab content
    @State private var tracesViewModel = TracesViewModel()
    @State private var metricsViewModel = MetricsViewModel()
    @State private var profilesViewModel = ProfilesViewModel()
    @State private var logsViewModel = LogsViewModel()
    @State private var healthViewModel = HealthViewModel()
    @State private var exploreViewModel = ExploreViewModel()
    @State private var logsSearchText = ""
    @State private var metricsSearchText = ""

    init(initialTab: NavigationItem = .metrics) {
        _selectedTab = State(initialValue: initialTab)
    }

    var body: some View {
        @Bindable var appState = appState

        NavigationSplitView(columnVisibility: $columnVisibility) {
            ServiceListView()
                .navigationSplitViewColumnWidth(min: 160, ideal: 220, max: 300)
        } detail: {
            VStack(spacing: 0) {
                if let service = appState.selectedService {
                    // Expandable resource attributes panel
                    if appState.isServiceAttributesExpanded {
                        ResourceAttributesPanel(service: service)
                        Divider()
                    }

                    // Unified filter bar (universal + tab-specific controls)
                    UnifiedFilterBar(
                        selectedTab: selectedTab,
                        tracesViewModel: tracesViewModel,
                        metricsViewModel: metricsViewModel,
                        profilesViewModel: profilesViewModel,
                        logsViewModel: logsViewModel,
                        healthViewModel: healthViewModel,
                        exploreViewModel: exploreViewModel,
                        logsSearchText: $logsSearchText,
                        metricsSearchText: $metricsSearchText
                    )

                    Divider()
                }

                // Content area
                DetailContentView(
                    selectedTab: selectedTab,
                    tracesViewModel: tracesViewModel,
                    metricsViewModel: metricsViewModel,
                    profilesViewModel: profilesViewModel,
                    logsViewModel: logsViewModel,
                    healthViewModel: healthViewModel,
                    exploreViewModel: exploreViewModel,
                    logsSearchText: $logsSearchText,
                    metricsSearchText: $metricsSearchText
                )
            }
            .frame(minWidth: 880) // 1100 window target − 220 sidebar ideal
        }
        .toolbar {
            // Service name with expander (left of tabs)
            ToolbarItem(placement: .navigation) {
                if let service = appState.selectedService {
                    ServiceNameView(
                        serviceName: service.name,
                        isExpanded: appState.isServiceAttributesExpanded
                    ) {
                        withAnimation(.easeInOut(duration: 0.2)) {
                            appState.isServiceAttributesExpanded.toggle()
                        }
                    }
                }
            }

            // Tab picker (center)
            ToolbarItem(placement: .principal) {
                Picker("", selection: $selectedTab) {
                    ForEach(NavigationItem.allCases) { tab in
                        Text(tab.rawValue).tag(tab)
                    }
                }
                .pickerStyle(.segmented)
                .fixedSize()
            }

            ToolbarItem(placement: .primaryAction) {
                Button(action: { appState.showServerInfo = true }) {
                    HStack(spacing: 6) {
                        Circle()
                            .fill(appState.serverStatus.statusColor)
                            .frame(width: 8, height: 8)
                        if let env = appState.environmentManager.selectedEnvironment {
                            Text(env.name)
                                .font(.system(size: 11))
                        } else {
                            Text(appState.serverStatus.statusText)
                                .font(.system(size: 11))
                        }
                        Image(systemName: "chevron.down")
                            .font(.system(size: 9))
                            .foregroundStyle(.secondary)
                    }
                }
                .popover(isPresented: $appState.showServerInfo) {
                    EnvironmentStatusView()
                }
            }
        }
        .navigationSplitViewStyle(.prominentDetail)
        .task {
            if appState.dataSource == nil && appState.dataSourceError == nil {
                appState.connectToDataSource()
            }
        }
        .alert("Connection Error", isPresented: .constant(appState.dataSourceError != nil)) {
            Button("Retry") { appState.reconnect() }
            Button("Settings") { appState.showSettings = true }
            Button("OK") { appState.dataSourceError = nil }
        } message: {
            if let error = appState.dataSourceError {
                Text(error)
            }
        }
    }
}

// MARK: - Detail Content View

/// View that switches content based on the selected tab
struct DetailContentView: View {
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
        Group {
            if appState.selectedService == nil && selectedTab != .explore {
                // No service selected - show empty state (Explore doesn't need a service)
                NoServiceSelectedView()
            } else {
                switch selectedTab {
                case .health:
                    HealthContentOnly(viewModel: healthViewModel)
                case .traces:
                    TracesContentOnly(viewModel: tracesViewModel)
                case .logs:
                    LogsContentOnly(viewModel: logsViewModel, searchText: $logsSearchText)
                case .metrics:
                    MetricsContentOnly(viewModel: metricsViewModel, searchText: $metricsSearchText)
                case .profiles:
                    ProfilesContentOnly(viewModel: profilesViewModel)
                case .explore:
                    ExploreContentOnly(viewModel: exploreViewModel)
                }
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - No Service Selected View

/// Empty state when no service is selected
struct NoServiceSelectedView: View {
    @Environment(AppStateViewModel.self) private var appState

    var body: some View {
        VStack(spacing: 20) {
            Image(systemName: "sidebar.left")
                .font(.system(size: 64))
                .foregroundStyle(.secondary)

            Text("Select a Service")
                .font(.title2)
                .foregroundStyle(.secondary)

            Text("Choose a service from the sidebar to view its telemetry data")
                .font(.body)
                .foregroundStyle(.tertiary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: 300)

            if case .running(let grpcPort, let httpPort) = appState.serverStatus {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Waiting for telemetry data...")
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    HStack(spacing: 16) {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("gRPC")
                                .font(.caption2)
                                .foregroundStyle(.tertiary)
                            Text("localhost:\(String(grpcPort))")
                                .font(.system(.caption, design: .monospaced))
                                .foregroundStyle(.secondary)
                        }

                        VStack(alignment: .leading, spacing: 4) {
                            Text("HTTP")
                                .font(.caption2)
                                .foregroundStyle(.tertiary)
                            Text("localhost:\(String(httpPort))")
                                .font(.system(.caption, design: .monospaced))
                                .foregroundStyle(.secondary)
                        }
                    }
                }
                .padding()
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Placeholder View

/// Generic placeholder view for coming-soon features
struct PlaceholderView: View {
    let title: String
    let icon: String

    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: icon)
                .font(.system(size: 64))
                .foregroundStyle(.secondary)
            Text(title)
                .font(.title)
                .foregroundStyle(.secondary)
            Text("Coming soon")
                .font(.caption)
                .foregroundStyle(.tertiary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

#Preview {
    MainWindow()
        .environment(AppStateViewModel())
        .frame(width: 1200, height: 800)
}
