import SwiftUI
import Charts
import SequinsData

/// Content-only metrics view (filter bar is in MainWindow's UnifiedFilterBar)
struct MetricsContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: MetricsViewModel
    @Binding var searchText: String

    /// Check if a metric name matches the search text
    private func matchesSearch(_ name: String) -> Bool {
        guard !searchText.isEmpty else { return true }
        return name.localizedCaseInsensitiveContains(searchText)
    }

    var body: some View {
        metricsContent
            .environment(viewModel)
            .task(id: appState.dataSourceId) {
                viewModel.clearModelCache()
                if appState.isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    await viewModel.loadMetrics(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onChange(of: appState.selectedService) { _, _ in
                viewModel.clearModelCache()
                if appState.isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onChange(of: appState.timeRangeState.timeRange) { _, newTimeRange in
                if appState.isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: newTimeRange
                    )
                } else {
                    viewModel.updateTimeRangeOnModels(newTimeRange)
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: newTimeRange
                    )
                }
            }
            .onChange(of: appState.isLive) { _, isLive in
                if isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    viewModel.cancel()
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onAppear {
                if appState.isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onDisappear {
                viewModel.cancel()
            }
    }

    private var metricsContent: some View {
        ScrollView {
            LazyVStack(spacing: 16) {
                // Loading state
                if viewModel.isLoading && viewModel.metrics.isEmpty && viewModel.sortedHistograms.isEmpty {
                    loadingView
                }

                // Priority Metrics (CPU, Memory) - shown first if available
                if !priorityMetrics.isEmpty || !priorityMetricGroups.isEmpty {
                    priorityMetricsGrid
                }

                // All Other Discovered Metrics
                if !otherMetrics.isEmpty || !otherMetricGroups.isEmpty {
                    discoveredMetricsSection
                }

                // Histogram Metrics
                if !histogramMetrics.isEmpty {
                    histogramMetricsSection
                }

                // Empty state
                if !viewModel.isLoading && viewModel.metrics.isEmpty && viewModel.sortedHistograms.isEmpty {
                    emptyStateView
                }
            }
            .padding()
        }
    }

    // MARK: - Priority Metrics (CPU, Memory at top)

    /// Set of metric names that are part of a group (should not show as individual cards)
    private var groupedMetricNames: Set<String> {
        viewModel.groupedMetricNames
    }

    /// Metrics that should appear at the top (CPU, Memory) - excluding grouped metrics
    /// Using explicit id: \.id to ensure stable identity
    private var priorityMetrics: [SequinsData.Metric] {
        viewModel.metrics.filter { metric in
            let name = metric.name.lowercased()
            let isPriority = name.contains("cpu") || name.contains("memory") || name.contains("heap")
            let isGrouped = groupedMetricNames.contains(metric.name)
            return isPriority && !isGrouped && matchesSearch(metric.name)
        }
    }

    /// All other metrics - excluding grouped metrics
    private var otherMetrics: [SequinsData.Metric] {
        viewModel.metrics.filter { metric in
            let name = metric.name.lowercased()
            let isNotPriority = !name.contains("cpu") && !name.contains("memory") && !name.contains("heap")
            let isGrouped = groupedMetricNames.contains(metric.name)
            return isNotPriority && !isGrouped && matchesSearch(metric.name)
        }
    }

    /// Priority metric groups (CPU, Memory related)
    private var priorityMetricGroups: [SequinsData.MetricGroup] {
        viewModel.metricGroups.filter { group in
            let name = group.baseName.lowercased()
            let isPriority = name.contains("cpu") || name.contains("memory") || name.contains("heap")
            return isPriority && matchesSearch(group.baseName)
        }
    }

    /// Other metric groups
    private var otherMetricGroups: [SequinsData.MetricGroup] {
        viewModel.metricGroups.filter { group in
            let name = group.baseName.lowercased()
            let isNotPriority = !name.contains("cpu") && !name.contains("memory") && !name.contains("heap")
            return isNotPriority && matchesSearch(group.baseName)
        }
    }

    private var priorityMetricsGrid: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("System Metrics")
                .font(.headline)
                .foregroundColor(.primary)

            LazyVGrid(columns: [
                GridItem(.flexible(), spacing: 8),
                GridItem(.flexible(), spacing: 8)
            ], spacing: 8) {
                // Grouped metrics first
                ForEach(priorityMetricGroups, id: \.id) { group in
                    GroupedMetricChartCard(
                        group: group,
                        timeRange: appState.timeRangeState.timeRange,
                        onTimeRangeSelected: handleTimeRangeSelection
                    )
                }

                // Individual (non-grouped) metrics
                ForEach(priorityMetrics, id: \.name) { metric in
                    StaticMetricChartCard(
                        metric: metric,
                        timeRange: appState.timeRangeState.timeRange,
                        onTimeRangeSelected: handleTimeRangeSelection
                    )
                }
            }
        }
    }

    // MARK: - Discovered Metrics Section

    private var discoveredMetricsSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Application Metrics")
                .font(.headline)
                .foregroundColor(.primary)

            LazyVGrid(columns: [
                GridItem(.flexible(), spacing: 8),
                GridItem(.flexible(), spacing: 8)
            ], spacing: 8) {
                // Grouped metrics first
                ForEach(otherMetricGroups, id: \.id) { group in
                    GroupedMetricChartCard(
                        group: group,
                        timeRange: appState.timeRangeState.timeRange,
                        onTimeRangeSelected: handleTimeRangeSelection
                    )
                }

                // Individual (non-grouped) metrics
                ForEach(otherMetrics, id: \.name) { metric in
                    StaticMetricChartCard(
                        metric: metric,
                        timeRange: appState.timeRangeState.timeRange,
                        onTimeRangeSelected: handleTimeRangeSelection
                    )
                }
            }
        }
    }

    // MARK: - Histogram Metrics Section

    /// Histogram metrics filtered by search text.
    private var histogramMetrics: [HistogramLine] {
        viewModel.sortedHistograms.filter { matchesSearch($0.name) }
    }

    private var histogramMetricsSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Histogram Metrics")
                .font(.headline)
                .foregroundColor(.primary)

            LazyVGrid(columns: [
                GridItem(.flexible(), spacing: 8),
                GridItem(.flexible(), spacing: 8)
            ], spacing: 8) {
                ForEach(histogramMetrics, id: \.id) { line in
                    HistogramChartCard(
                        line: line,
                        timeRange: appState.timeRangeState.timeRange,
                        onTimeRangeSelected: handleTimeRangeSelection
                    )
                }
            }
        }
    }

    // MARK: - Time Range Selection Handler

    private func handleTimeRangeSelection(start: Date, end: Date) {
        // Drag selection switches to paused mode with the selected time range
        appState.setCustomTimeRange(start: start, end: end)
    }

    // MARK: - Loading View

    private var loadingView: some View {
        VStack(spacing: 12) {
            ProgressView()
            Text("Loading metrics...")
                .font(.caption)
                .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 40)
    }

    // MARK: - Empty State

    private var emptyStateView: some View {
        VStack(spacing: 12) {
            Image(systemName: "chart.line.uptrend.xyaxis")
                .font(.system(size: 48))
                .foregroundColor(.secondary)
            Text("No Metrics Data")
                .font(.headline)
            Text("Metrics will appear here when your application starts sending OpenTelemetry metrics data.")
                .font(.caption)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 60)
    }
}

#Preview {
    struct PreviewWrapper: View {
        @State private var viewModel = MetricsViewModel()
        @State private var searchText = ""
        var body: some View {
            MetricsContentOnly(viewModel: viewModel, searchText: $searchText)
                .environment(AppStateViewModel())
                .frame(width: 800, height: 600)
        }
    }
    return PreviewWrapper()
}
