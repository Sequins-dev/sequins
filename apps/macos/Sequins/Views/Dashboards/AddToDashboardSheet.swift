import SwiftUI
import SequinsData

/// Sheet to add a visualization to a new or existing dashboard. Shared by the
/// Assistant tab (inline viz cards) and the Dashboards tab.
struct AddToDashboardSheet: View {
    let visualization: SavedVisualization
    let dataSource: DataSource?

    @Environment(\.dismiss) private var dismiss
    @State private var dashboards: [Dashboard] = []
    @State private var mode: Mode = .existing
    @State private var selectedId: String?
    @State private var newTitle: String = ""
    @State private var error: String?

    enum Mode: Hashable { case existing, new }

    private var canAdd: Bool {
        switch mode {
        case .existing: return selectedId != nil
        case .new: return !newTitle.trimmingCharacters(in: .whitespaces).isEmpty
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Add to Dashboard")
                .font(.headline)

            Picker("", selection: $mode) {
                Text("Existing").tag(Mode.existing)
                Text("New").tag(Mode.new)
            }
            .pickerStyle(.segmented)
            .labelsHidden()
            .disabled(dashboards.isEmpty && mode == .existing)

            switch mode {
            case .existing:
                if dashboards.isEmpty {
                    Text("No dashboards yet — create one.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else {
                    Picker("Dashboard", selection: $selectedId) {
                        ForEach(dashboards) { d in
                            Text(d.title).tag(Optional(d.id))
                        }
                    }
                }
            case .new:
                TextField("Dashboard name", text: $newTitle)
                    .textFieldStyle(.roundedBorder)
            }

            if let error {
                Text(error)
                    .font(.caption)
                    .foregroundStyle(.red)
            }

            HStack {
                Spacer()
                Button("Cancel") { dismiss() }
                Button("Add") { add() }
                    .keyboardShortcut(.defaultAction)
                    .disabled(!canAdd)
            }
        }
        .padding()
        .frame(width: 360)
        .task { loadDashboards() }
    }

    private func loadDashboards() {
        guard let ds = dataSource else { return }
        do {
            dashboards = try ds.listDashboards()
            if selectedId == nil { selectedId = dashboards.first?.id }
            if dashboards.isEmpty { mode = .new }
        } catch {
            self.error = error.localizedDescription
        }
    }

    private func add() {
        guard let ds = dataSource else { return }
        do {
            var dashboard: Dashboard
            switch mode {
            case .new:
                let title = newTitle.trimmingCharacters(in: .whitespaces)
                dashboard = Dashboard(title: title.isEmpty ? "New Dashboard" : title)
            case .existing:
                guard let id = selectedId,
                      let existing = dashboards.first(where: { $0.id == id }) else { return }
                dashboard = existing
            }
            // Add the visualization as a new full-width row at the bottom.
            dashboard.rows.append(DashboardRow(panels: [RowPanel(visualization: visualization)]))
            _ = try ds.saveDashboard(dashboard)
            dismiss()
        } catch {
            self.error = error.localizedDescription
        }
    }
}
