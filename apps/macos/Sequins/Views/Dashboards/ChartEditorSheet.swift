import SwiftUI
import SequinsData

/// Create or configure a dashboard chart: edit its SeQL query, title, and visualization
/// type with a live preview, then add it (to a row or a new row) or save an existing one.
struct ChartEditorSheet: View {
    let target: ChartEditorTarget
    @Bindable var viewModel: DashboardsViewModel
    let dataSource: DataSource?
    let timeRange: TimeRange

    @Environment(\.dismiss) private var dismiss

    @State private var seql = ""
    @State private var title = ""
    @State private var vizType: VizType?
    @State private var previewSeql = ""
    @State private var loaded = false
    @State private var previewDebounce: Task<Void, Never>?

    private var isEditing: Bool {
        if case .edit = target { return true }
        return false
    }

    private var canSave: Bool {
        !seql.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private var previewVisualization: SavedVisualization {
        SavedVisualization(
            seql: previewSeql,
            title: title.isEmpty ? "Preview" : title,
            shape: vizType?.rawValue
        )
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(isEditing ? "Edit Chart" : "New Chart")
                .font(.headline)

            TextField("Title", text: $title)
                .textFieldStyle(.roundedBorder)

            VStack(alignment: .leading, spacing: 4) {
                Text("SeQL query")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                TextEditor(text: $seql)
                    .font(.system(.body, design: .monospaced))
                    .scrollContentBackground(.hidden)
                    .padding(6)
                    .frame(height: 72)
                    .background(Color(nsColor: .textBackgroundColor))
                    .clipShape(RoundedRectangle(cornerRadius: 6))
                    .overlay(
                        RoundedRectangle(cornerRadius: 6)
                            .stroke(Color.secondary.opacity(0.2), lineWidth: 1)
                    )
                    .onChange(of: seql) { _, newValue in schedulePreview(newValue) }
            }

            HStack(spacing: 8) {
                Text("Type")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                VizTypePicker(selection: $vizType)
                Spacer()
            }

            VStack(alignment: .leading, spacing: 4) {
                Text("Preview")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                VisualizationView(
                    visualization: previewVisualization,
                    dataSource: dataSource,
                    timeRange: timeRange,
                    isLive: false,
                    applyTimeRange: true,
                    vizTypeOverride: vizType
                )
                .frame(height: 220)
                .background(Color(nsColor: .textBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
            }

            HStack {
                Spacer()
                Button("Cancel") { dismiss() }
                Button(isEditing ? "Save" : "Add") { save() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(!canSave)
            }
        }
        .padding()
        .frame(width: 540)
        .task { loadIfNeeded() }
    }

    private func loadIfNeeded() {
        guard !loaded else { return }
        loaded = true
        if case .edit(let r, let p) = target,
           let dashboard = viewModel.selected,
           dashboard.rows.indices.contains(r),
           dashboard.rows[r].panels.indices.contains(p) {
            let viz = dashboard.rows[r].panels[p].visualization
            seql = viz.seql
            title = viz.title
            vizType = viz.vizType
        } else {
            seql = "spans last 15m | group by { ts() bin 1m as bucket } { count() as n }"
            title = "New Chart"
        }
        previewSeql = seql
    }

    /// Debounce preview re-runs so we don't execute a query on every keystroke.
    private func schedulePreview(_ value: String) {
        previewDebounce?.cancel()
        previewDebounce = Task {
            try? await Task.sleep(nanoseconds: 600_000_000)
            if !Task.isCancelled {
                previewSeql = value
            }
        }
    }

    private func save() {
        guard let ds = dataSource else { return }
        let cleanTitle = title.trimmingCharacters(in: .whitespaces)
        let viz = SavedVisualization(
            seql: seql.trimmingCharacters(in: .whitespacesAndNewlines),
            title: cleanTitle.isEmpty ? "Chart" : cleanTitle,
            shape: vizType?.rawValue
        )
        switch target {
        case .addToRow(let r):
            viewModel.addPanel(toRow: r, visualization: viz, dataSource: ds)
        case .newRow:
            viewModel.addRow(with: viz, dataSource: ds)
        case .edit(let r, let p):
            viewModel.updatePanel(row: r, panel: p, visualization: viz, dataSource: ds)
        }
        dismiss()
    }
}
