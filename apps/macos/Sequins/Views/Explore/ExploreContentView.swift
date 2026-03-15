import SwiftUI
import SequinsData

// MARK: - Content Only Wrapper

/// Wrapper following the *ContentOnly pattern — triggers validation on data source changes
struct ExploreContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: ExploreViewModel

    var body: some View {
        ExploreContentView(viewModel: viewModel)
            .task(id: appState.dataSourceId) {
                viewModel.validateQuery(dataSource: appState.dataSource)
            }
    }
}

// MARK: - Main Explore View

/// The main Explore tab — query editor on top, result visualization below
struct ExploreContentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: ExploreViewModel

    var body: some View {
        VSplitView {
            editorPanel
                .frame(minHeight: 100, idealHeight: 140, maxHeight: 300)

            resultPanel
                .frame(minHeight: 100)
        }
        .onChange(of: viewModel.queryText) { _, _ in
            viewModel.validateQuery(dataSource: appState.dataSource)
        }
    }

    // MARK: - Editor Panel

    private var editorPanel: some View {
        VStack(spacing: 0) {
            TextEditor(text: $viewModel.queryText)
                .font(.system(size: 13, design: .monospaced))
                .scrollContentBackground(.hidden)
                .background(Color(nsColor: .textBackgroundColor))
                .padding(8)
                .disableAutocorrection(true)

            Divider()

            toolbarRow
        }
    }

    private var toolbarRow: some View {
        HStack(spacing: 12) {
            // Run / Cancel button
            if viewModel.isExecuting {
                Button(action: { viewModel.cancelQuery() }) {
                    HStack(spacing: 4) {
                        ProgressView()
                            .scaleEffect(0.7)
                            .frame(width: 12, height: 12)
                        Text("Cancel")
                    }
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
            } else {
                Button(action: { viewModel.executeQuery(dataSource: appState.dataSource) }) {
                    Label("Run", systemImage: "play.fill")
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.small)
                .disabled(!viewModel.isQueryValid)
                .keyboardShortcut(.return, modifiers: .command)
            }

            // Parse error or execution error
            if let parseErr = viewModel.parseError {
                HStack(spacing: 4) {
                    Image(systemName: "exclamationmark.triangle.fill")
                        .foregroundStyle(.red)
                        .font(.caption)
                    Text(parseErr.message)
                        .font(.caption)
                        .foregroundStyle(.red)
                        .lineLimit(1)
                }
            } else if let execErr = viewModel.executionError {
                HStack(spacing: 4) {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundStyle(.red)
                        .font(.caption)
                    Text(execErr)
                        .font(.caption)
                        .foregroundStyle(.red)
                        .lineLimit(1)
                }
            } else if let stats = viewModel.stats {
                Text(statsText(stats))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            // Warnings badge
            if !viewModel.warnings.isEmpty {
                Label("\(viewModel.warnings.count) warning\(viewModel.warnings.count == 1 ? "" : "s")", systemImage: "exclamationmark.triangle")
                    .font(.caption)
                    .foregroundStyle(.orange)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(Color(nsColor: .windowBackgroundColor))
    }

    // MARK: - Result Panel

    private var resultPanel: some View {
        Group {
            if viewModel.isExecuting && viewModel.schema == nil {
                VStack(spacing: 12) {
                    ProgressView()
                    Text("Executing query…")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if let schema = viewModel.schema {
                ExploreResultView(
                    schema: schema,
                    rows: viewModel.rows,
                    recordTrees: viewModel.recordTrees,
                    visualizationOverride: viewModel.visualizationOverride,
                    pageSize: viewModel.pageSize,
                    currentPage: $viewModel.currentPage
                )
            } else if viewModel.executionError == nil && !viewModel.isExecuting {
                Text("Run a query to see results")
                    .foregroundStyle(.tertiary)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                EmptyView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .background(Color(nsColor: .textBackgroundColor))
    }

    // MARK: - Helpers

    private func statsText(_ stats: SeQLStats) -> String {
        let timeMs = stats.executionTimeUs / 1000
        let rows = stats.rowsReturned
        return "\(rows) row\(rows == 1 ? "" : "s") in \(timeMs)ms"
    }
}

#Preview {
    struct PreviewWrapper: View {
        @State private var viewModel = ExploreViewModel()
        var body: some View {
            ExploreContentOnly(viewModel: viewModel)
                .environment(AppStateViewModel())
                .frame(width: 900, height: 600)
        }
    }
    return PreviewWrapper()
}
