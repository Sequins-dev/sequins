import SwiftUI

struct SettingsView: View {
    @Environment(AppStateViewModel.self) private var appState

    var body: some View {
        TabView {
            // Environments tab
            HSplitView {
                // Environment list sidebar
                EnvironmentListView()
                    .frame(minWidth: 200, idealWidth: 250, maxWidth: 300)

                // Environment details
                EnvironmentDetailView()
                    .frame(minWidth: 400)
            }
            .tabItem {
                Label("Environments", systemImage: "network")
            }

            // Health rules tab
            HealthRulesSettingsView()
                .tabItem {
                    Label("Health Rules", systemImage: "heart.text.square")
                }
        }
        .frame(width: 700, height: 500)
    }
}

struct EnvironmentDetailView: View {
    @Environment(AppStateViewModel.self) private var appState

    var body: some View {
        if let environment = appState.environmentManager.selectedEnvironment {
            SelectedEnvironmentView(environment: environment)
        } else {
            ContentUnavailableView(
                "No Environment Selected",
                systemImage: "network.slash",
                description: Text("Select an environment from the list to view its settings.")
            )
        }
    }
}

struct SelectedEnvironmentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var environment: ConnectionEnvironment
    @State private var needsReconnect = false
    /// Assistant API key / bearer token — loaded from and saved to the Keychain (never
    /// stored in the SwiftData model).
    @State private var assistantSecret = ""

    var body: some View {
        Form {
            // Status section
            Section("Status") {
                HStack {
                    Circle()
                        .fill(appState.serverStatus.statusColor)
                        .frame(width: 10, height: 10)
                    Text(appState.serverStatus.statusText)
                    Spacer()
                    if environment.isSelected {
                        Button("Reconnect") {
                            appState.reconnect()
                            needsReconnect = false
                        }
                    } else {
                        Button("Connect") {
                            appState.switchToEnvironment(environment)
                        }
                    }
                }

                if let error = appState.dataSourceError {
                    Text(error)
                        .foregroundStyle(.red)
                        .font(.caption)
                }

                if needsReconnect && environment.isSelected {
                    HStack {
                        Image(systemName: "exclamationmark.triangle.fill")
                            .foregroundStyle(.orange)
                        Text("Settings changed. Reconnect to apply.")
                            .foregroundStyle(.orange)
                    }
                }
            }

            // Environment settings
            if environment.isLocal {
                localEnvironmentSettings
            } else {
                remoteEnvironmentSettings
            }

            // Assistant (AI) configuration
            assistantSettings

            // About section
            Section("About") {
                LabeledContent("Version", value: "1.0.0")
                LabeledContent("Build", value: "1")
            }
        }
        .formStyle(.grouped)
        .task(id: environment.id) {
            assistantSecret = KeychainStore.shared.assistantSecret(environmentId: environment.id) ?? ""
        }
    }

    /// Assistant (AI) configuration. Base URL + model persist on the environment; the
    /// API key / bearer token is stored in the Keychain. Changes apply on the next chat
    /// turn (the assistant reads its config fresh each time), so no reconnect is needed.
    @ViewBuilder
    private var assistantSettings: some View {
        Section("Assistant (AI)") {
            LabeledContent(environment.isLocal ? "Provider Base URL" : "Daemon /v1 URL") {
                TextField(
                    environment.isLocal ? "https://api.openai.com/v1" : "http://host:8082/v1",
                    text: assistantBinding(\.assistantBaseURL)
                )
                .textFieldStyle(.roundedBorder)
                .frame(maxWidth: 350)
            }

            LabeledContent(environment.isLocal ? "API Key" : "Bearer Token") {
                SecureField("", text: $assistantSecret)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 350)
                    .onChange(of: assistantSecret) { _, newValue in
                        KeychainStore.shared.setAssistantSecret(newValue, environmentId: environment.id)
                    }
            }

            Text(environment.isLocal
                ? "OpenAI-compatible provider. Leave the base URL blank for api.openai.com. The API key is stored securely in your Keychain. Pick a model from the list in the Assistant tab."
                : "The daemon's assistant /v1 endpoint and bearer token. Stored in your Keychain. Pick a model in the Assistant tab.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    /// A `String` binding over an optional environment field that persists on edit and
    /// maps empty strings to `nil`.
    private func assistantBinding(
        _ keyPath: ReferenceWritableKeyPath<ConnectionEnvironment, String?>
    ) -> Binding<String> {
        Binding(
            get: { environment[keyPath: keyPath] ?? "" },
            set: {
                environment[keyPath: keyPath] = $0.isEmpty ? nil : $0
                appState.environmentManager.updateEnvironment(environment)
            }
        )
    }

    @ViewBuilder
    private var localEnvironmentSettings: some View {
        Section("Development Environment") {
            LabeledContent("Name") {
                Text("Development")
                    .foregroundStyle(.secondary)
            }

            Text("The development environment runs an embedded OTLP server and stores telemetry data locally.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }

        Section("Database") {
            LabeledContent("Path") {
                TextField("Path", text: Binding(
                    get: { environment.dbPath ?? "" },
                    set: {
                        environment.dbPath = $0
                        appState.environmentManager.updateEnvironment(environment)
                        needsReconnect = true
                    }
                ))
                .textFieldStyle(.roundedBorder)
                .frame(maxWidth: 350)
            }
        }

        Section("OTLP Server Ports") {
            LabeledContent("gRPC Port") {
                TextField("Port", text: Binding(
                    get: { String(environment.grpcPort ?? 4317) },
                    set: {
                        if let port = Int($0) {
                            environment.grpcPort = port
                            appState.environmentManager.updateEnvironment(environment)
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
                            appState.environmentManager.updateEnvironment(environment)
                            needsReconnect = true
                        }
                    }
                ))
                .textFieldStyle(.roundedBorder)
                .frame(width: 80)
            }

            Text("Send telemetry to:")
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
    private var remoteEnvironmentSettings: some View {
        Section("Remote Environment") {
            LabeledContent("Name") {
                TextField("Name", text: $environment.name)
                    .textFieldStyle(.roundedBorder)
                    .frame(maxWidth: 200)
                    .onChange(of: environment.name) { _, _ in
                        appState.environmentManager.updateEnvironment(environment)
                    }
            }

            Text("Connect to a remote Sequins daemon instance.")
                .font(.caption)
                .foregroundStyle(.secondary)
        }

        Section("Connection URLs") {
            LabeledContent("Query URL") {
                TextField("URL", text: Binding(
                    get: { environment.remoteQueryURL ?? "" },
                    set: {
                        environment.remoteQueryURL = $0
                        appState.environmentManager.updateEnvironment(environment)
                        needsReconnect = true
                    }
                ))
                .textFieldStyle(.roundedBorder)
                .frame(maxWidth: 350)
            }

            LabeledContent("Management URL") {
                TextField("URL", text: Binding(
                    get: { environment.remoteManagementURL ?? "" },
                    set: {
                        environment.remoteManagementURL = $0
                        appState.environmentManager.updateEnvironment(environment)
                        needsReconnect = true
                    }
                ))
                .textFieldStyle(.roundedBorder)
                .frame(maxWidth: 350)
            }
        }

        Section {
            Button("Delete Environment", role: .destructive) {
                appState.environmentManager.deleteEnvironment(environment)
            }
        }
    }
}

#Preview {
    SettingsView()
        .environment(AppStateViewModel())
}
