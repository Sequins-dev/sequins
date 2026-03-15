import SwiftUI

struct EditEnvironmentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Environment(\.dismiss) private var dismiss

    @Bindable var environment: ConnectionEnvironment

    @State private var needsReconnect = false

    private var isValid: Bool {
        if environment.isLocal {
            guard let dbPath = environment.dbPath else { return false }
            return !dbPath.trimmingCharacters(in: .whitespaces).isEmpty
        } else {
            guard let queryURL = environment.remoteQueryURL,
                  let managementURL = environment.remoteManagementURL else { return false }
            return URL(string: queryURL) != nil && URL(string: managementURL) != nil
        }
    }

    var body: some View {
        VStack(spacing: 0) {
            // Header
            HStack {
                Text(environment.isLocal ? "Edit Local Environment" : "Edit Remote Environment")
                    .font(.headline)
                Spacer()
                Button {
                    dismiss()
                } label: {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.borderless)
            }
            .padding()

            Divider()

            // Form
            Form {
                if environment.isLocal {
                    localEnvironmentForm
                } else {
                    remoteEnvironmentForm
                }

                if needsReconnect && environment.isSelected {
                    Section {
                        HStack {
                            Image(systemName: "exclamationmark.triangle.fill")
                                .foregroundStyle(.orange)
                            Text("Settings changed. Save and reconnect to apply.")
                                .foregroundStyle(.secondary)
                        }
                    }
                }
            }
            .formStyle(.grouped)
            .scrollContentBackground(.hidden)

            Divider()

            // Actions
            HStack {
                if environment.canDelete {
                    Button("Delete", role: .destructive) {
                        appState.environmentManager.deleteEnvironment(environment)
                        dismiss()
                    }
                }

                Spacer()

                Button("Cancel") {
                    dismiss()
                }
                .keyboardShortcut(.cancelAction)

                Button(needsReconnect && environment.isSelected ? "Save & Reconnect" : "Save") {
                    save()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(!isValid)
            }
            .padding()
        }
        .frame(width: 500, height: environment.isLocal ? 400 : 350)
    }

    @ViewBuilder
    private var localEnvironmentForm: some View {
        Section {
            LabeledContent("Name") {
                Text("Development")
                    .foregroundStyle(.secondary)
            }

            Text("The development environment cannot be renamed or deleted.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }

        Section("Database") {
            LabeledContent("Path") {
                TextField("Path", text: Binding(
                    get: { environment.dbPath ?? "" },
                    set: {
                        environment.dbPath = $0
                        needsReconnect = true
                    }
                ))
                .textFieldStyle(.roundedBorder)
            }
        }

        Section("OTLP Server") {
            LabeledContent("gRPC Port") {
                TextField("Port", text: Binding(
                    get: { String(environment.grpcPort ?? 4317) },
                    set: {
                        if let port = Int($0) {
                            environment.grpcPort = port
                            needsReconnect = true
                        }
                    }
                ))
                .textFieldStyle(.roundedBorder)
                .frame(width: 80)
            }

            LabeledContent("HTTP Port") {
                TextField("Port", text: Binding(
                    get: { String(environment.httpPort ?? 4318) },
                    set: {
                        if let port = Int($0) {
                            environment.httpPort = port
                            needsReconnect = true
                        }
                    }
                ))
                .textFieldStyle(.roundedBorder)
                .frame(width: 80)
            }

            Text("OTLP endpoints will be available at:")
                .font(.caption)
                .foregroundStyle(.secondary)
            Text("gRPC: localhost:\(String(environment.effectiveGrpcPort))")
                .font(.caption.monospaced())
                .foregroundStyle(.secondary)
            Text("HTTP: localhost:\(String(environment.effectiveHttpPort))")
                .font(.caption.monospaced())
                .foregroundStyle(.secondary)
        }
    }

    @ViewBuilder
    private var remoteEnvironmentForm: some View {
        Section {
            LabeledContent("Name") {
                TextField("Name", text: $environment.name)
                    .textFieldStyle(.roundedBorder)
            }
        }

        Section("Connection") {
            LabeledContent("Query URL") {
                TextField("URL", text: Binding(
                    get: { environment.remoteQueryURL ?? "" },
                    set: {
                        environment.remoteQueryURL = $0
                        needsReconnect = true
                    }
                ))
                .textFieldStyle(.roundedBorder)
            }

            LabeledContent("Management URL") {
                TextField("URL", text: Binding(
                    get: { environment.remoteManagementURL ?? "" },
                    set: {
                        environment.remoteManagementURL = $0
                        needsReconnect = true
                    }
                ))
                .textFieldStyle(.roundedBorder)
            }
        }
    }

    private func save() {
        appState.environmentManager.updateEnvironment(environment)

        if needsReconnect && environment.isSelected {
            appState.reconnect()
        }

        dismiss()
    }
}

#Preview {
    EditEnvironmentView(
        environment: ConnectionEnvironment.createLocalEnvironment(dbPath: "/tmp/test.db")
    )
    .environment(AppStateViewModel())
}
