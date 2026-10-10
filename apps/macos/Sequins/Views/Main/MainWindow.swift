import SwiftUI

/// Main application window containing the navigation split view and tab content
struct MainWindow: View {
    @Environment(AppStateViewModel.self) private var appState
    @Environment(\.openSettings) private var openSettings
    @State private var selectedTab: NavigationItem
    @State private var columnVisibility: NavigationSplitViewVisibility = .all

    // Shared view models for tab content
    @State private var tracesViewModel = TracesViewModel()
    @State private var metricsViewModel = MetricsViewModel()
    @State private var profilesViewModel = ProfilesViewModel()
    @State private var logsViewModel = LogsViewModel()
    @State private var healthViewModel = HealthViewModel()
    @State private var exploreViewModel = ExploreViewModel()
    @State private var assistantViewModel = AssistantViewModel()
    @State private var dashboardsViewModel = DashboardsViewModel()
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
                WelcomeBanner()
                // Expandable resource attributes panel (service tabs only)
                if let service = appState.selectedService, appState.isServiceAttributesExpanded {
                    ResourceAttributesPanel(service: service)
                    Divider()
                }

                // Unified filter bar (universal + tab-specific controls). Shown for the
                // service tabs and for Dashboards (which uses the universal live +
                // time-range controls). Not shown for Assistant.
                if appState.selectedService != nil || selectedTab == .dashboards {
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
                    assistantViewModel: assistantViewModel,
                    dashboardsViewModel: dashboardsViewModel,
                    logsSearchText: $logsSearchText,
                    metricsSearchText: $metricsSearchText
                )
            }
            .frame(minWidth: 880) // 1100 window target − 220 sidebar ideal
        }
        .toolbar {
            // Service name with expander (left of tabs)
            if #available(macOS 26.0, *) {
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
            } else {
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
        .onAppear {
            // Reload dashboards after an assistant turn that edited them server-side, so a
            // dashboard the user is viewing updates live.
            assistantViewModel.onDashboardsChanged = {
                if let ds = appState.dataSource {
                    dashboardsViewModel.refresh(dataSource: ds)
                }
            }
        }
        .alert("Connection Error", isPresented: .constant(appState.dataSourceError != nil)) {
            Button("Retry") { appState.reconnect() }
            Button("Settings") { openSettings() }
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
    @Bindable var assistantViewModel: AssistantViewModel
    @Bindable var dashboardsViewModel: DashboardsViewModel
    @Binding var logsSearchText: String
    @Binding var metricsSearchText: String

    /// Tabs that work without a selected service.
    private var serviceOptional: Bool {
        selectedTab == .explore || selectedTab == .assistant || selectedTab == .dashboards
    }

    var body: some View {
        Group {
            if appState.selectedService == nil && !serviceOptional {
                // No service selected - show empty state (some tabs don't need a service)
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
                case .assistant:
                    AssistantContentOnly(viewModel: assistantViewModel)
                case .dashboards:
                    DashboardsContentOnly(viewModel: dashboardsViewModel)
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
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        VStack(spacing: 20) {
            switch appState.serverStatus {
            case .starting:
                ProgressView("Starting Sequins…")
            case .stopped:
                statusMessage("Connect an Environment", icon: "network", message: "Open Settings to connect a local or remote environment.")
                Button("Open Environment Settings", action: openEnvironmentSettings)
            case .error(let message):
                failure(message)
            case .running, .connected:
                switch appState.serviceDiscoveryState {
                case .loading:
                    ProgressView("Loading services…")
                case .failed(let message):
                    failure(message)
                case .ready(hasServices: true):
                    statusMessage("Select a Service", icon: "sidebar.left", message: "Choose a service from the sidebar to view its telemetry data.")
                case .ready(hasServices: false):
                    gettingStarted
                }
            }
        }
        .padding(32)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var gettingStarted: some View {
        VStack(spacing: 20) {
            if case .running(let grpcPort, let httpPort) = appState.serverStatus {
                statusMessage("Send your first telemetry", icon: "antenna.radiowaves.left.and.right", message: "Sequins is ready to receive OpenTelemetry data. Point your application's exporter at one of these local endpoints. Services appear automatically when data arrives.")
                VStack(alignment: .leading, spacing: 12) {
                    endpoint("HTTP", value: "http://localhost:\(httpPort)")
                    endpoint("gRPC", value: "http://localhost:\(grpcPort)")
                    Divider()
                    Text("For an OTLP HTTP/protobuf exporter:")
                        .font(.caption).foregroundStyle(.secondary)
                    let configuration = "export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:\(httpPort)\nexport OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf"
                    Text(configuration)
                        .font(.system(.caption, design: .monospaced))
                        .fixedSize(horizontal: false, vertical: true)
                        .textSelection(.enabled)
                    Button("Copy exporter configuration") { copy(configuration) }
                }
                .padding(16)
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
            } else {
                statusMessage("Waiting for remote telemetry", icon: "network", message: "No services are available in this environment. Send telemetry to your remote Sequins daemon and verify its connection settings.")
                Button("Open Environment Settings", action: openEnvironmentSettings)
            }
            Link("OpenTelemetry instrumentation guide", destination: URL(string: "https://opentelemetry.io/docs/languages/")!)
        }
        .frame(maxWidth: 560)
    }

    private func statusMessage(_ title: String, icon: String, message: String) -> some View {
        VStack(spacing: 16) {
            Image(systemName: icon).font(.system(size: 48)).foregroundStyle(.secondary)
            Text(title).font(.title2)
            Text(message).foregroundStyle(.secondary).multilineTextAlignment(.center)
        }
    }

    private func failure(_ message: String) -> some View {
        VStack(spacing: 16) {
            statusMessage("Couldn’t Load Telemetry", icon: "exclamationmark.triangle", message: message)
            HStack {
                Button("Retry") { appState.reconnect() }
                Button("Open Environment Settings", action: openEnvironmentSettings)
            }
        }
        .frame(maxWidth: 560)
    }

    private func endpoint(_ label: String, value: String) -> some View {
        HStack {
            Text(label).frame(width: 45, alignment: .leading)
            Text(value).font(.system(.body, design: .monospaced)).textSelection(.enabled)
            Spacer()
            Button("Copy \(label) endpoint") { copy(value) }
        }
    }

    private func copy(_ value: String) {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
    }

    private func openEnvironmentSettings() {
        appState.selectedSettingsTab = .environments
        openSettings()
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
