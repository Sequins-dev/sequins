//
//  HealthContentView.swift
//  Sequins
//
//  Main content view for the Health tab
//

import SwiftUI
import SequinsData

/// Content view for the Health tab showing service health analysis
struct HealthContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: HealthViewModel

    /// Metrics to display as summary cards (excludes HTTP rates shown in breakdown)
    private var summaryMetricNames: [String] {
        [
            HealthMetricNames.spanErrorRate,
            HealthMetricNames.latencyP50,
            HealthMetricNames.latencyP95,
            HealthMetricNames.latencyP99,
            HealthMetricNames.throughput,
            HealthMetricNames.errorLogRate
        ]
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                if let analysis = viewModel.feed?.healthAnalysis {
                    let metricValues = viewModel.feed?.healthMetricValues ?? [:]

                    // Top row: Summary card and HTTP Status breakdown
                    HStack(alignment: .top, spacing: 16) {
                        // Summary card
                        HealthSummaryCard(analysis: analysis)
                            .frame(maxWidth: .infinity)

                        // HTTP Status breakdown (real-time from health metrics)
                        HTTPStatusBreakdownCard(healthMetricValues: metricValues)
                            .frame(maxWidth: .infinity)
                    }

                    // Health factors section
                    if !analysis.factors.isEmpty {
                        VStack(alignment: .leading, spacing: 12) {
                            Text("Health Factors")
                                .font(.headline)
                                .foregroundStyle(.primary)

                            VStack(spacing: 8) {
                                ForEach(analysis.factors) { factor in
                                    HealthFactorRow(factor: factor)
                                }
                            }
                            .padding()
                            .background(
                                RoundedRectangle(cornerRadius: 8)
                                    .fill(Color(nsColor: .controlBackgroundColor))
                            )
                        }
                    }

                    // Health metric summary cards
                    VStack(alignment: .leading, spacing: 12) {
                        Text("Health Metrics")
                            .font(.headline)
                            .foregroundStyle(.primary)

                        LazyVGrid(columns: [
                            GridItem(.flexible(), spacing: 16),
                            GridItem(.flexible(), spacing: 16),
                            GridItem(.flexible(), spacing: 16)
                        ], spacing: 16) {
                            ForEach(summaryMetricNames, id: \.self) { name in
                                HealthMetricValueCard(
                                    name: name,
                                    value: metricValues[name],
                                    previousValue: viewModel.feed?.previousMetricValues?[name]
                                )
                            }
                        }
                    }
                } else if viewModel.feed?.isLoading ?? false {
                    VStack(spacing: 16) {
                        ProgressView()
                            .scaleEffect(1.5)
                        Text("Loading health data...")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .padding(.top, 100)
                } else {
                    // No health data available yet
                    VStack(spacing: 16) {
                        Image(systemName: "heart.text.clipboard")
                            .font(.system(size: 48))
                            .foregroundStyle(.secondary)
                        Text("No health data available")
                            .font(.title3)
                            .foregroundStyle(.secondary)
                        Text("Health metrics will appear once telemetry data is being processed.")
                            .font(.subheadline)
                            .foregroundStyle(.tertiary)
                            .multilineTextAlignment(.center)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .padding(.top, 100)
                }
            }
            .padding(20)
        }
        .task(id: appState.dataSourceId) {
            viewModel.configure(dataSource: appState.dataSource)
            await refreshHealth()
        }
        .onChange(of: appState.selectedService) { _, _ in
            Task { await refreshHealth() }
        }
        .onChange(of: appState.timeRangeState.timeRange) { _, _ in
            Task { await refreshHealth() }
        }
        .onDisappear {
            viewModel.cancel()
        }
        .onChange(of: viewModel.comparisonPeriod) { _, _ in
            Task {
                await viewModel.loadComparison(dataSource: appState.dataSource)
            }
        }
        .sheet(isPresented: $viewModel.showingHealthRulesSheet) {
            HealthRulesSettingsView()
                .environment(appState)
                .onDisappear {
                    viewModel.reloadConfig(dataSource: appState.dataSource)
                    Task { await refreshHealth() }
                }
        }
    }

    private func refreshHealth() async {
        print("🏥 [HealthContentView] refreshHealth called - service: \(appState.selectedService?.name ?? "nil"), dataSource: \(appState.dataSource != nil ? "present" : "nil")")
        await viewModel.loadHealth(
            dataSource: appState.dataSource,
            selectedService: appState.selectedService,
            timeRange: appState.timeRangeState.timeRange
        )
        print("🏥 [HealthContentView] refreshHealth completed - feed: \(viewModel.feed != nil ? "present" : "nil")")
    }
}

/// Summary card showing a computed health metric value
struct HealthMetricValueCard: View {
    let name: String
    let value: Double?
    var previousValue: Double? = nil

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(displayName)
                .font(.subheadline)
                .fontWeight(.medium)
                .foregroundStyle(.primary)

            HStack(alignment: .firstTextBaseline, spacing: 8) {
                Text(value.map { formatValue($0) } ?? "--")
                    .font(.system(.title2, design: .monospaced))
                    .fontWeight(.semibold)
                    .foregroundStyle(value != nil ? .primary : .secondary)

                if let current = value, let previous = previousValue {
                    TrendIndicator(current: current, previous: previous)
                }
            }
            .frame(height: 40)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(Color(nsColor: .controlBackgroundColor))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 8)
                .stroke(Color(nsColor: .separatorColor), lineWidth: 1)
        )
    }

    private var displayName: String {
        let short = name.replacingOccurrences(of: "sequins.health.", with: "")
        return short.split(separator: "_").map { $0.capitalized }.joined(separator: " ")
    }

    private func formatValue(_ value: Double) -> String {
        if name.contains("error_rate") {
            return String(format: "%.1f%%", value * 100)
        } else if name.contains("latency") {
            let ms = value / 1_000_000
            if ms < 1 {
                return String(format: "%.2fms", ms)
            } else if ms < 1000 {
                return String(format: "%.0fms", ms)
            } else {
                return String(format: "%.1fs", ms / 1000)
            }
        } else if name.contains("throughput") {
            return String(format: "%.1f/min", value)
        } else if name.contains("log_rate") {
            return String(format: "%.1f/min", value)
        } else {
            return String(format: "%.2f", value)
        }
    }
}

/// Simple trend indicator
struct TrendIndicator: View {
    let current: Double
    let previous: Double

    var body: some View {
        let change = current - previous
        let percentChange = previous != 0 ? (change / previous) * 100 : 0

        HStack(spacing: 4) {
            Image(systemName: change >= 0 ? "arrow.up.right" : "arrow.down.right")
                .font(.caption)
            Text(String(format: "%.1f%%", abs(percentChange)))
                .font(.caption)
        }
        .foregroundStyle(trendColor)
    }

    private var trendColor: Color {
        // For error rates and latency, going up is bad
        // For throughput, going up is neutral/good
        // We'll use a simple approach: red for up, green for down
        current >= previous ? .red.opacity(0.8) : .green.opacity(0.8)
    }
}

#Preview {
    struct PreviewWrapper: View {
        @State private var viewModel = HealthViewModel()

        var body: some View {
            HealthContentOnly(viewModel: viewModel)
                .environment(AppStateViewModel())
                .frame(width: 800, height: 600)
        }
    }
    return PreviewWrapper()
}
