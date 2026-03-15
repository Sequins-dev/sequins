import SwiftUI
import SequinsData

/// Individual service row in the sidebar list
struct ServiceRow: View {
    let service: Service
    var healthStatus: HealthStatus?

    var body: some View {
        HStack(spacing: 8) {
            // Health status indicator
            Circle()
                .fill(healthStatus?.color ?? .gray.opacity(0.5))
                .frame(width: 8, height: 8)

            VStack(alignment: .leading, spacing: 4) {
                Text(service.name)
                    .font(.headline)

                HStack(spacing: 8) {
                    Label("\(service.spanCount)", systemImage: "arrow.triangle.branch")
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    if service.logCount > 0 {
                        Label("\(service.logCount)", systemImage: "doc.text")
                            .font(.caption)
                            .foregroundStyle(.tertiary)
                    }
                }
            }
        }
        .padding(.vertical, 4)
    }
}

#Preview("ServiceRow") {
    ServiceRow(service: Service(
        name: "my-web-service",
        spanCount: 1234,
        logCount: 567
    ))
    .padding()
}

#Preview("ServiceRow - No Logs") {
    ServiceRow(service: Service(
        name: "api-gateway",
        spanCount: 56789,
        logCount: 0
    ))
    .padding()
}

#Preview("ServiceRow - Many Logs") {
    ServiceRow(service: Service(
        name: "batch-processor",
        spanCount: 42,
        logCount: 9999
    ))
    .padding()
}
