import SwiftUI

struct ServerStatusView: View {
    @Environment(AppStateViewModel.self) private var appState

    var body: some View {
        @Bindable var appState = appState

        VStack(spacing: 20) {
            // Status indicator
            HStack(spacing: 12) {
                Circle()
                    .fill(appState.serverStatus.statusColor)
                    .frame(width: 12, height: 12)

                VStack(alignment: .leading, spacing: 4) {
                    Text("OTLP Server")
                        .font(.headline)
                    Text(appState.serverStatus.statusText)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                Spacer()
            }

            // Server endpoints (if running)
            if case .running(let grpcPort, let httpPort) = appState.serverStatus {
                VStack(alignment: .leading, spacing: 12) {
                    Text("Send telemetry to:")
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    EndpointRow(
                        icon: "network",
                        label: "gRPC",
                        url: "http://localhost:\(String(grpcPort))"
                    )

                    EndpointRow(
                        icon: "network",
                        label: "HTTP",
                        url: "http://localhost:\(String(httpPort))"
                    )

                    Divider()

                    Text("Example: Configure your app's OTLP exporter")
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    Text("OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:\(String(httpPort))")
                        .font(.system(.caption, design: .monospaced))
                        .padding(8)
                        .background(Color(nsColor: .controlBackgroundColor))
                        .clipShape(RoundedRectangle(cornerRadius: 4))
                        .textSelection(.enabled)
                }
            }

            // Error message (if error)
            if case .error(let message) = appState.serverStatus {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Error:")
                        .font(.caption)
                        .foregroundStyle(.red)

                    Text(message)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .padding()
                .background(Color.red.opacity(0.1))
                .clipShape(RoundedRectangle(cornerRadius: 8))
            }

            Spacer()
        }
        .padding()
        .frame(width: 400, height: 300)
    }
}

struct EndpointRow: View {
    let icon: String
    let label: String
    let url: String

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: icon)
                .foregroundStyle(.secondary)
                .frame(width: 20)

            Text(label)
                .font(.caption)
                .foregroundStyle(.secondary)
                .frame(width: 50, alignment: .leading)

            Text(url)
                .font(.system(.caption, design: .monospaced))
                .textSelection(.enabled)

            Spacer()

            Button {
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(url, forType: .string)
            } label: {
                Image(systemName: "doc.on.doc")
                    .font(.caption)
            }
            .buttonStyle(.plain)
            .help("Copy to clipboard")
        }
    }
}

#Preview {
    ServerStatusView()
        .environment(AppStateViewModel())
}
