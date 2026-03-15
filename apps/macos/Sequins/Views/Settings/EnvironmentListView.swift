import SwiftUI

struct EnvironmentListView: View {
    @Environment(AppStateViewModel.self) private var appState
    @State private var showingAddEnvironment = false
    @State private var environmentToEdit: ConnectionEnvironment?

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Header
            HStack {
                Text("Environments")
                    .font(.headline)
                Spacer()
                Button {
                    showingAddEnvironment = true
                } label: {
                    Image(systemName: "plus")
                }
                .buttonStyle(.borderless)
                .help("Add remote environment")
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 8)

            Divider()

            // Environment list
            List(selection: Binding(
                get: { appState.environmentManager.selectedEnvironment?.id },
                set: { id in
                    if let id,
                       let env = appState.environmentManager.environments.first(where: { $0.id == id }) {
                        appState.switchToEnvironment(env)
                    }
                }
            )) {
                ForEach(appState.environmentManager.environments, id: \.id) { environment in
                    EnvironmentRowView(
                        environment: environment,
                        isSelected: environment.isSelected,
                        onEdit: { environmentToEdit = environment },
                        onDelete: { appState.environmentManager.deleteEnvironment(environment) }
                    )
                    .tag(environment.id)
                }
            }
            .listStyle(.sidebar)
        }
        .sheet(isPresented: $showingAddEnvironment) {
            AddEnvironmentView()
        }
        .sheet(item: $environmentToEdit) { environment in
            EditEnvironmentView(environment: environment)
        }
    }
}

struct EnvironmentRowView: View {
    let environment: ConnectionEnvironment
    let isSelected: Bool
    let onEdit: () -> Void
    let onDelete: () -> Void

    @State private var isHovering = false

    var body: some View {
        HStack(spacing: 8) {
            // Icon
            Image(systemName: environment.isLocal ? "laptopcomputer" : "network")
                .foregroundStyle(isSelected ? .primary : .secondary)
                .frame(width: 20)

            // Name and subtitle
            VStack(alignment: .leading, spacing: 2) {
                Text(environment.name)
                    .fontWeight(isSelected ? .semibold : .regular)

                if environment.isLocal {
                    Text("localhost:\(String(environment.effectiveGrpcPort))")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                } else if let url = environment.remoteQueryURL {
                    Text(url)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }

            Spacer()

            // Status indicator for selected
            if isSelected {
                Circle()
                    .fill(.green)
                    .frame(width: 8, height: 8)
            }

            // Actions (visible on hover)
            if isHovering {
                HStack(spacing: 4) {
                    Button {
                        onEdit()
                    } label: {
                        Image(systemName: "pencil")
                            .font(.caption)
                    }
                    .buttonStyle(.borderless)
                    .help("Edit environment")

                    if environment.canDelete {
                        Button {
                            onDelete()
                        } label: {
                            Image(systemName: "trash")
                                .font(.caption)
                                .foregroundStyle(.red)
                        }
                        .buttonStyle(.borderless)
                        .help("Delete environment")
                    }
                }
            }
        }
        .padding(.vertical, 4)
        .contentShape(Rectangle())
        .onHover { hovering in
            isHovering = hovering
        }
    }
}

#Preview {
    EnvironmentListView()
        .environment(AppStateViewModel())
        .frame(width: 250, height: 300)
}
