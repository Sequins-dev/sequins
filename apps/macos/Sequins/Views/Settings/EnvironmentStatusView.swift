import SwiftUI

struct EnvironmentStatusView: View {
    @Environment(AppStateViewModel.self) private var appState
    @State private var showingAddEnvironment = false

    var body: some View {
        VStack(spacing: 0) {
            // Environment selector
            VStack(alignment: .leading, spacing: 8) {
                HStack {
                    Text("Environment")
                        .font(.headline)
                    Spacer()
                    Button {
                        showingAddEnvironment = true
                    } label: {
                        Image(systemName: "plus")
                            .font(.caption)
                    }
                    .buttonStyle(.borderless)
                    .help("Add remote environment")
                }

                // Environment list
                ForEach(appState.environmentManager.environments, id: \.id) { environment in
                    EnvironmentOptionRow(
                        environment: environment,
                        isSelected: environment.isSelected,
                        onSelect: {
                            if !environment.isSelected {
                                appState.switchToEnvironment(environment)
                            }
                        }
                    )
                }
            }
            .padding()

            Divider()

            // Query connection status for the selected environment
            if let env = appState.environmentManager.selectedEnvironment {
                VStack(alignment: .leading, spacing: 12) {
                    HStack(spacing: 8) {
                        if case .starting = appState.serverStatus {
                            ProgressView()
                                .controlSize(.mini)
                                .frame(width: 10, height: 10)
                        } else {
                            Circle()
                                .fill(appState.serverStatus.statusColor)
                                .frame(width: 10, height: 10)
                        }
                        Text(appState.serverStatus.statusText)
                            .font(.subheadline)
                        Spacer()
                        Button("Reconnect") {
                            appState.reconnect()
                        }
                        .buttonStyle(.borderless)
                        .font(.caption)
                        .disabled(
                            appState.serverStatus == .starting ||
                            appState.localServerStatus == .starting
                        )
                    }

                    // Query connection error
                    if let error = appState.dataSourceError {
                        Text(error)
                            .font(.caption)
                            .foregroundStyle(.red)
                            .padding(8)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .background(Color.red.opacity(0.1))
                            .clipShape(RoundedRectangle(cornerRadius: 4))
                    }

                    // Remote endpoint info
                    if !env.isLocal, let queryURL = env.remoteQueryURL {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Connected to:")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Text(queryURL)
                                .font(.system(.caption, design: .monospaced))
                                .foregroundStyle(.secondary)
                        }
                    }
                }
                .padding()
            }

            Divider()

            // Always-on local OTLP server status (visible regardless of selected env)
            LocalServerStatusSection()

            Divider()

            // Settings link
            HStack {
                SettingsLink {
                    Label("Environment Settings...", systemImage: "gearshape")
                        .font(.caption)
                }
                .buttonStyle(.borderless)
                Spacer()
            }
            .padding(.horizontal)
            .padding(.vertical, 8)
        }
        .frame(width: 320)
        .sheet(isPresented: $showingAddEnvironment) {
            AddEnvironmentView()
        }
    }
}

/// Shows the status of the always-on embedded OTLP server.
struct LocalServerStatusSection: View {
    @Environment(AppStateViewModel.self) private var appState

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                if case .starting = appState.localServerStatus {
                    ProgressView()
                        .controlSize(.mini)
                        .frame(width: 8, height: 8)
                } else {
                    Circle()
                        .fill(appState.localServerStatus.statusColor)
                        .frame(width: 8, height: 8)
                }
                Text("Local OTLP")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Text(appState.localServerStatus.statusText)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            if case .running(let grpcPort, let httpPort) = appState.localServerStatus {
                VStack(alignment: .leading, spacing: 6) {
                    EndpointRow(
                        icon: "network",
                        label: "gRPC",
                        url: "localhost:\(grpcPort)"
                    )
                    EndpointRow(
                        icon: "network",
                        label: "HTTP",
                        url: "localhost:\(httpPort)"
                    )
                }
            }

            if case .error(let msg) = appState.localServerStatus {
                Text(msg)
                    .font(.caption)
                    .foregroundStyle(.red)
                    .padding(6)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color.red.opacity(0.1))
                    .clipShape(RoundedRectangle(cornerRadius: 4))
            }
        }
        .padding()
    }
}

struct EnvironmentOptionRow: View {
    let environment: ConnectionEnvironment
    let isSelected: Bool
    let onSelect: () -> Void

    var body: some View {
        Button(action: onSelect) {
            HStack(spacing: 10) {
                Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                    .foregroundStyle(isSelected ? .blue : .secondary)
                    .font(.system(size: 14))

                Image(systemName: environment.isLocal ? "laptopcomputer" : "network")
                    .foregroundStyle(.secondary)
                    .frame(width: 16)

                Text(environment.name)
                    .foregroundStyle(.primary)

                Spacer()

                if environment.isLocal {
                    Text("localhost:\(String(environment.effectiveGrpcPort))")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                } else if let url = environment.remoteQueryURL,
                          let host = URL(string: url)?.host {
                    Text(host)
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
            .padding(.vertical, 6)
            .padding(.horizontal, 8)
            .background(isSelected ? Color.blue.opacity(0.1) : Color.clear)
            .clipShape(RoundedRectangle(cornerRadius: 6))
        }
        .buttonStyle(.plain)
    }
}

#Preview {
    EnvironmentStatusView()
        .environment(AppStateViewModel())
}
