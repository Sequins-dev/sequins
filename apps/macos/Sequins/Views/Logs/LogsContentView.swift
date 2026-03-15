import SwiftUI
import SequinsData

/// Content-only logs view (filter bar is in MainWindow's UnifiedFilterBar)
struct LogsContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: LogsViewModel
    @Binding var searchText: String
    @State private var searchDebounceTask: Task<Void, Never>?

    var body: some View {
        @Bindable var appState = appState

        let logs = viewModel.effectiveLogs.map { LogEntry(from: $0) }
        let isLoading = viewModel.isLoading

        LogTerminalView(
            logs: logs,
            isLoading: isLoading,
            sortNewestFirst: viewModel.sortNewestFirst,
            onTimeSelection: { startTime, endTime in
                // Update to custom absolute time range (clears preset)
                appState.setCustomTimeRange(start: startTime, end: endTime)
            }
        )
        .task(id: appState.dataSourceId) {
            viewModel.configure(dataSource: appState.dataSource)
            if appState.isLive {
                let severities = currentSeverities()
                viewModel.startLiveStream(
                    dataSource: appState.dataSource,
                    selectedService: appState.selectedService,
                    searchText: searchText,
                    severities: severities.isEmpty ? nil : severities
                )
            } else {
                refreshLogs()
            }
        }
        .onChange(of: appState.selectedService) { _, _ in
            if appState.isLive {
                let severities = currentSeverities()
                viewModel.startLiveStream(
                    dataSource: appState.dataSource,
                    selectedService: appState.selectedService,
                    searchText: searchText,
                    severities: severities.isEmpty ? nil : severities
                )
            } else {
                refreshLogs()
            }
        }
        .onChange(of: appState.timeRangeState.timeRange) { _, _ in
            if !appState.isLive {
                refreshLogs()
            }
        }
        .onChange(of: searchText) { _, newValue in
            viewModel.searchText = newValue
            if appState.isLive {
                // Debounce: wait 300ms after the last keystroke before restarting the live stream.
                // Without this, each character restarts the stream and the snapshot never completes.
                searchDebounceTask?.cancel()
                let severities = currentSeverities()
                let ds = appState.dataSource
                let service = appState.selectedService
                searchDebounceTask = Task { @MainActor in
                    try? await Task.sleep(nanoseconds: 300_000_000)
                    guard !Task.isCancelled else { return }
                    viewModel.startLiveStream(
                        dataSource: ds,
                        selectedService: service,
                        searchText: newValue,
                        severities: severities.isEmpty ? nil : severities
                    )
                }
            } else {
                refreshLogs()
            }
        }
        .onChange(of: appState.selectedLogLevels) { _, _ in
            if appState.isLive {
                let severities = currentSeverities()
                viewModel.startLiveStream(
                    dataSource: appState.dataSource,
                    selectedService: appState.selectedService,
                    searchText: searchText,
                    severities: severities.isEmpty ? nil : severities
                )
            } else {
                refreshLogs()
            }
        }
        .onChange(of: appState.isLive) { _, isLive in
            if isLive {
                let severities = currentSeverities()
                viewModel.startLiveStream(
                    dataSource: appState.dataSource,
                    selectedService: appState.selectedService,
                    searchText: searchText,
                    severities: severities.isEmpty ? nil : severities
                )
            } else {
                viewModel.cancel()
                refreshLogs()
            }
        }
        .onAppear {
            if appState.isLive {
                let severities = currentSeverities()
                viewModel.startLiveStream(
                    dataSource: appState.dataSource,
                    selectedService: appState.selectedService,
                    searchText: searchText,
                    severities: severities.isEmpty ? nil : severities
                )
            } else {
                refreshLogs()
            }
        }
        .onDisappear {
            searchDebounceTask?.cancel()
            viewModel.cancel()
        }
    }

    private func currentSeverities() -> [SequinsData.LogSeverity] {
        appState.selectedLogLevels.compactMap { uiSeverity in
            switch uiSeverity {
            case .trace: return .trace
            case .debug: return .debug
            case .info: return .info
            case .warn: return .warn
            case .error: return .error
            case .fatal: return .fatal
            }
        }
    }

    private func refreshLogs() {
        let severities = currentSeverities()
        viewModel.loadLogs(
            dataSource: appState.dataSource,
            selectedService: appState.selectedService,
            timeRange: appState.timeRangeState.timeRange,
            searchText: searchText,
            severities: severities.isEmpty ? nil : severities
        )
    }
}

#Preview {
    struct PreviewWrapper: View {
        @State private var viewModel = LogsViewModel()
        @State private var searchText = ""
        var body: some View {
            LogsContentOnly(viewModel: viewModel, searchText: $searchText)
                .environment(AppStateViewModel())
                .frame(width: 1000, height: 600)
        }
    }
    return PreviewWrapper()
}
