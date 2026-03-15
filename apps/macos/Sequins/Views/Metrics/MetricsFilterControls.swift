import SwiftUI

/// Filter controls specific to the Metrics tab
struct MetricsFilterControls: View {
    @Bindable var viewModel: MetricsViewModel
    @Binding var searchText: String

    var body: some View {
        HStack(spacing: 12) {
            // Search field for metric names/descriptions
            HStack(spacing: 4) {
                Image(systemName: "magnifyingglass")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextField("Search metrics...", text: $searchText)
                    .textFieldStyle(.plain)
                    .font(.caption)
                    .frame(width: 120)
                if !searchText.isEmpty {
                    Button(action: { searchText = "" }) {
                        Image(systemName: "xmark.circle.fill")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.quaternary)
            .cornerRadius(4)
            .fixedSize()

            // Granularity indicator (read-only)
            HStack(spacing: 4) {
                Image(systemName: "chart.dots.scatter")
                    .font(.caption2)
                    .foregroundColor(.secondary)
                Text("Interval: \(viewModel.binSizeStringForDisplay)")
                    .font(.caption2)
                    .lineLimit(1)
                    .foregroundColor(.secondary)
            }
            .fixedSize()
            .help("Data aggregation interval (automatically adjusted based on time range)")

            // Export Menu
            ExportButton {
                Button("Export as JSON") {
                    viewModel.exportAsJSON()
                }
                Button("Export as CSV") {
                    viewModel.exportAsCSV()
                }
            }
        }
    }
}

#Preview("MetricsFilterControls") {
    struct PreviewWrapper: View {
        @State private var viewModel = MetricsViewModel()
        @State private var searchText = ""

        var body: some View {
            MetricsFilterControls(viewModel: viewModel, searchText: $searchText)
                .padding()
        }
    }
    return PreviewWrapper()
}
