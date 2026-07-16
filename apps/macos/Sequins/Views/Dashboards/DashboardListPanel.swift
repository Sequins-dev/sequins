import SwiftUI
import SequinsData

/// Sidebar list of dashboards with create/delete.
struct DashboardListPanel: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: DashboardsViewModel

    @State private var showingNew = false
    @State private var newName = ""

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Text("Dashboards")
                    .font(.headline)
                Spacer()
                Button {
                    newName = ""
                    showingNew = true
                } label: {
                    Image(systemName: "plus")
                }
                .buttonStyle(.borderless)
                .help("New dashboard")
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)

            Divider()

            if viewModel.dashboards.isEmpty {
                VStack(spacing: 6) {
                    Image(systemName: "square.grid.2x2")
                        .font(.title2)
                        .foregroundStyle(.tertiary)
                    Text("No dashboards")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List(selection: selectionBinding) {
                    ForEach(viewModel.dashboards) { dashboard in
                        HStack(spacing: 6) {
                            Image(systemName: "square.grid.2x2")
                                .foregroundStyle(.secondary)
                            Text(dashboard.title)
                                .lineLimit(1)
                        }
                        .tag(dashboard.id)
                        .contextMenu {
                            Button("Delete", role: .destructive) {
                                delete(dashboard.id)
                            }
                        }
                    }
                }
                .listStyle(.sidebar)
            }
        }
        .background(Color(nsColor: .controlBackgroundColor).opacity(0.5))
        .alert("New Dashboard", isPresented: $showingNew) {
            TextField("Name", text: $newName)
            Button("Create") {
                if let ds = appState.dataSource {
                    viewModel.createDashboard(title: newName, dataSource: ds)
                }
            }
            Button("Cancel", role: .cancel) {}
        }
    }

    private var selectionBinding: Binding<String?> {
        Binding(
            get: { viewModel.selectedDashboardId },
            set: { newValue in
                if let id = newValue { viewModel.select(id) }
            }
        )
    }

    private func delete(_ id: String) {
        guard let ds = appState.dataSource else { return }
        viewModel.select(id)
        viewModel.deleteSelected(dataSource: ds)
    }
}
