import SwiftUI

/// Filter controls specific to the Logs tab
struct LogsFilterControls: View {
    @Environment(AppStateViewModel.self) private var appState
    @Binding var searchText: String
    @Binding var sortNewestFirst: Bool
    var onExport: (() -> Void)?

    var body: some View {
        @Bindable var appState = appState

        HStack(spacing: 12) {
            // Search
            HStack(spacing: 4) {
                Image(systemName: "magnifyingglass")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextField("Search logs...", text: $searchText)
                    .textFieldStyle(.plain)
                    .font(.caption)
                    .frame(width: 120)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.quaternary)
            .cornerRadius(4)
            .fixedSize()

            // Level filter toggles
            HStack(spacing: 2) {
                ForEach(LogSeverity.allCases, id: \.self) { level in
                    Button(action: {
                        if appState.selectedLogLevels.contains(level) {
                            appState.selectedLogLevels.remove(level)
                        } else {
                            appState.selectedLogLevels.insert(level)
                        }
                    }) {
                        Text(level.rawValue)
                            .font(.caption2.weight(.medium))
                            .lineLimit(1)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 4)
                            .background(appState.selectedLogLevels.contains(level) ? level.color.opacity(0.3) : Color.gray.opacity(0.2))
                            .foregroundStyle(appState.selectedLogLevels.contains(level) ? level.color : .secondary)
                            .cornerRadius(4)
                    }
                    .buttonStyle(.plain)
                    .fixedSize()
                }
            }
            .fixedSize()

            // Sort direction toggle
            Button(action: { sortNewestFirst.toggle() }) {
                Image(systemName: sortNewestFirst ? "arrow.down.to.line" : "arrow.up.to.line")
                    .font(.caption)
                    .padding(.horizontal, 6)
                    .padding(.vertical, 4)
                    .background(.quaternary)
                    .cornerRadius(4)
            }
            .buttonStyle(.plain)
            .fixedSize()
            .help(sortNewestFirst ? "Newest first — click for oldest first" : "Oldest first — click for newest first")

            // Export button
            if let onExport = onExport {
                ExportButton {
                    Button("Export as JSON") {
                        onExport()
                    }
                }
            }
        }
    }
}

#Preview("LogsFilterControls") {
    struct PreviewWrapper: View {
        @State private var searchText = ""
        @State private var sortNewestFirst = true

        var body: some View {
            LogsFilterControls(searchText: $searchText, sortNewestFirst: $sortNewestFirst)
                .environment(AppStateViewModel())
                .padding()
        }
    }
    return PreviewWrapper()
}
