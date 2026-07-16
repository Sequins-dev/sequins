import SwiftUI

struct EditEnvironmentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Environment(\.dismiss) private var dismiss

    @Bindable var environment: ConnectionEnvironment

    @State private var needsReconnect = false
    /// The assistant API key / bearer token — loaded from and saved to the Keychain
    /// (never stored in the SwiftData model).
    @State private var assistantSecret = ""

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
            .task {
                assistantSecret = KeychainStore.shared.assistantSecret(environmentId: environment.id) ?? ""
            }

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
        .frame(width: 500, height: environment.isLocal ? 580 : 520)
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

        assistantSection
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

        assistantSection
    }

    /// Assistant (AI) configuration. Base URL + model persist on the environment; the
    /// secret is stored in the Keychain.
    @ViewBuilder
    private var assistantSection: some View {
        Section("Assistant (AI)") {
            LabeledContent(environment.isLocal ? "Provider Base URL" : "Daemon /v1 URL") {
                TextField(
                    environment.isLocal ? "https://api.openai.com/v1" : "http://host:8082/v1",
                    text: optionalBinding(\.assistantBaseURL)
                )
                .textFieldStyle(.roundedBorder)
            }

            LabeledContent("Model") {
                TextField(
                    environment.isLocal ? "gpt-5.5" : "(optional)",
                    text: optionalBinding(\.assistantModel)
                )
                .textFieldStyle(.roundedBorder)
            }

            LabeledContent(environment.isLocal ? "API Key" : "Bearer Token") {
                SecureField("", text: $assistantSecret)
                    .textFieldStyle(.roundedBorder)
            }

            Text(environment.isLocal
                ? "Stored securely in your Keychain. Required to chat with the assistant."
                : "Bearer token for the daemon's assistant endpoint. Stored in your Keychain.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func optionalBinding(
        _ keyPath: ReferenceWritableKeyPath<ConnectionEnvironment, String?>
    ) -> Binding<String> {
        Binding(
            get: { environment[keyPath: keyPath] ?? "" },
            set: { environment[keyPath: keyPath] = $0.isEmpty ? nil : $0 }
        )
    }

    private func save() {
        appState.environmentManager.updateEnvironment(environment)
        KeychainStore.shared.setAssistantSecret(assistantSecret, environmentId: environment.id)

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
