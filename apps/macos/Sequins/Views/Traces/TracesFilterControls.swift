import SwiftUI
import AppKit

/// Filter controls specific to the Traces tab
struct TracesFilterControls: View {
    @Bindable var viewModel: TracesViewModel

    var body: some View {
        HStack(spacing: 12) {
            // Search
            HStack(spacing: 4) {
                Image(systemName: "magnifyingglass")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextField("Search traces...", text: $viewModel.searchText)
                    .textFieldStyle(.plain)
                    .font(.caption)
                    .frame(width: 120)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.quaternary)
            .cornerRadius(4)
            .fixedSize()

            // Sort
            Menu {
                ForEach(TraceSortBy.allCases, id: \.self) { sortBy in
                    Button(action: { viewModel.sortBy = sortBy }) {
                        HStack {
                            Text(sortBy.displayName)
                            if viewModel.sortBy == sortBy {
                                Image(systemName: "checkmark")
                            }
                        }
                    }
                }
            } label: {
                HStack(spacing: 4) {
                    Text("Sort: \(viewModel.sortBy.displayName)")
                        .font(.caption)
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 4)
                .background(.quaternary)
                .cornerRadius(4)
            }
            .menuStyle(.borderlessButton)
            .fixedSize()

            // Errors only toggle
            Button(action: { viewModel.showErrorsOnly.toggle() }) {
                HStack(spacing: 4) {
                    Image(systemName: viewModel.showErrorsOnly ? "checkmark.square.fill" : "square")
                        .font(.caption)
                    Text("Errors")
                        .font(.caption)
                }
                .padding(.horizontal, 8)
                .padding(.vertical, 4)
                .background(viewModel.showErrorsOnly ? Color.red.opacity(0.2) : Color.gray.opacity(0.2))
                .foregroundStyle(viewModel.showErrorsOnly ? .red : .primary)
                .cornerRadius(4)
            }
            .buttonStyle(.plain)
            .fixedSize()

            // Status code range filters
            HStack(spacing: 4) {
                ForEach(HTTPStatusCodeRange.allCases, id: \.self) { range in
                    StatusCodeRangeButton(
                        range: range,
                        isSelected: viewModel.statusCodeRanges.contains(range),
                        action: {
                            if viewModel.statusCodeRanges.contains(range) {
                                viewModel.statusCodeRanges.remove(range)
                            } else {
                                viewModel.statusCodeRanges.insert(range)
                            }
                        }
                    )
                }
            }
            .fixedSize()

            // Export Menu
            ExportButton {
                Button("Export as JSON") {
                    viewModel.exportSpans()
                }
            }
        }
    }
}

/// Individual toggle button for a status code range
private struct StatusCodeRangeButton: View {
    let range: HTTPStatusCodeRange
    let isSelected: Bool
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Text(range.displayName)
                .font(.caption)
                .lineLimit(1)
                .padding(.horizontal, 6)
                .padding(.vertical, 4)
                .background(isSelected ? backgroundColor.opacity(0.3) : Color.gray.opacity(0.2))
                .foregroundStyle(isSelected ? backgroundColor : .primary)
                .cornerRadius(4)
        }
        .buttonStyle(.plain)
        .fixedSize()
    }

    private var backgroundColor: Color {
        switch range {
        case .success2xx: return .green
        case .redirect3xx: return .blue
        case .clientError4xx: return .orange
        case .serverError5xx: return .red
        }
    }
}

#Preview("TracesFilterControls") {
    struct PreviewWrapper: View {
        @State private var viewModel = TracesViewModel()

        var body: some View {
            TracesFilterControls(viewModel: viewModel)
                .padding()
        }
    }
    return PreviewWrapper()
}
