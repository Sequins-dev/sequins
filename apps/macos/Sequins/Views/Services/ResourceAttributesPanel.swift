import SwiftUI
import SequinsData

/// Expandable panel showing all resource attributes for a service
struct ResourceAttributesPanel: View {
    let service: Service

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            ForEach(service.resourceAttributes) { attr in
                HStack(alignment: .top, spacing: 8) {
                    Text(attr.key)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .frame(minWidth: 140, alignment: .trailing)

                    AttributeValuesView(values: attr.values, monospaced: true, copyable: true)
                }
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(.quaternary.opacity(0.3))
    }
}

#Preview("ResourceAttributesPanel") {
    ResourceAttributesPanel(
        service: Service(
            name: "my-web-service",
            spanCount: 1234,
            logCount: 567,
            resourceAttributes: [
                ResourceAttribute(key: "service.version", values: ["1.2.3"]),
                ResourceAttribute(key: "deployment.environment", values: ["production"]),
                ResourceAttribute(key: "service.namespace", values: ["backend"]),
                ResourceAttribute(key: "host.name", values: ["server-1", "server-2", "server-3"]),
                ResourceAttribute(key: "process.pid", values: ["12345", "12346"]),
                ResourceAttribute(key: "telemetry.sdk.name", values: ["@opentelemetry/sdk-node"]),
                ResourceAttribute(key: "telemetry.sdk.version", values: ["1.18.0"])
            ]
        )
    )
    .frame(width: 600)
}

#Preview("ResourceAttributesPanel - Empty") {
    ResourceAttributesPanel(
        service: Service(
            name: "new-service",
            spanCount: 0,
            logCount: 0
        )
    )
    .frame(width: 600)
}
