import SwiftUI
import SequinsData

/// Service name display for the toolbar with optional health status indicator
struct ServiceNameView: View {
    let serviceName: String
    let isExpanded: Bool
    var healthStatus: HealthStatus?
    let action: () -> Void

    var body: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(statusColor)
                .frame(width: 8, height: 8)
            Text(serviceName)
                .font(.headline)
            Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
        .onTapGesture {
            action()
        }
    }

    private var statusColor: Color {
        healthStatus?.color ?? .green
    }
}

#Preview {
    VStack(spacing: 16) {
        ServiceNameView(serviceName: "my-service", isExpanded: false, healthStatus: .healthy) {}
        ServiceNameView(serviceName: "my-service", isExpanded: false, healthStatus: .degraded) {}
        ServiceNameView(serviceName: "my-service", isExpanded: false, healthStatus: .unhealthy) {}
        ServiceNameView(serviceName: "my-service", isExpanded: false, healthStatus: .inactive) {}
        ServiceNameView(serviceName: "my-service", isExpanded: false, healthStatus: nil) {}
    }
    .padding()
}
