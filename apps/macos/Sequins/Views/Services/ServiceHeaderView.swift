import SwiftUI
import AppKit
import SequinsData

/// Service header view with metadata and expandable resource attributes
struct ServiceHeaderView: View {
    let service: Service
    @State private var isExpanded = false

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Main header row (always visible)
            HStack(spacing: 16) {
                // Service status and name
                HStack(spacing: 8) {
                    Text("🟢")
                        .font(.title3)

                    HStack(spacing: 8) {
                        Text(service.name)
                            .font(.headline)

                        // Prominent attributes inline
                        ForEach(service.prominentAttributes) { attr in
                            ProminentAttributeView(attribute: attr)
                        }
                    }
                }

                Spacer()

                // Expand/collapse button if there are other attributes
                if !service.otherAttributes.isEmpty {
                    Button(action: { withAnimation(.easeInOut(duration: 0.2)) { isExpanded.toggle() } }) {
                        HStack(spacing: 4) {
                            Text("\(service.otherAttributes.count) more")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding()

            // Expandable attributes section
            if isExpanded && !service.otherAttributes.isEmpty {
                Divider()
                    .padding(.horizontal)

                VStack(alignment: .leading, spacing: 6) {
                    ForEach(service.otherAttributes) { attr in
                        HStack(alignment: .top, spacing: 8) {
                            Text(attr.key)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .frame(minWidth: 120, alignment: .trailing)

                            AttributeValuesView(values: attr.values, monospaced: true, copyable: true)
                        }
                    }
                }
                .padding(.horizontal)
                .padding(.vertical, 8)
            }
        }
        .background(.quaternary.opacity(0.3))
    }
}

/// Small inline view for prominent attributes (version, environment, namespace)
/// Renders each value as a colored chip
struct ProminentAttributeView: View {
    let attribute: ResourceAttribute

    var body: some View {
        FlowLayout(spacing: 4, lineSpacing: 4) {
            ForEach(Array(attribute.values.enumerated()), id: \.offset) { _, value in
                ProminentAttributeChip(value: value, backgroundColor: backgroundColor)
            }
        }
    }

    private var backgroundColor: Color {
        switch attribute.key {
        case "service.version":
            return .blue
        case "deployment.environment":
            return .green
        case "service.namespace":
            return .purple
        default:
            return .gray
        }
    }
}

/// Individual chip for prominent attribute values with custom background color
private struct ProminentAttributeChip: View {
    let value: String
    let backgroundColor: Color

    @State private var showCopied = false

    var body: some View {
        HStack(spacing: 4) {
            Text(value)
                .font(.caption)
                .foregroundStyle(.secondary)
                .textSelection(.enabled)

            Button(action: copyToClipboard) {
                Image(systemName: showCopied ? "checkmark" : "doc.on.doc")
                    .font(.caption2)
                    .foregroundStyle(showCopied ? .green : .secondary)
            }
            .buttonStyle(.plain)
            .help("Copy to clipboard")
        }
        .padding(.horizontal, 6)
        .padding(.vertical, 2)
        .background(backgroundColor.opacity(0.15))
        .clipShape(RoundedRectangle(cornerRadius: 4))
    }

    private func copyToClipboard() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)

        withAnimation(.easeInOut(duration: 0.2)) {
            showCopied = true
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) {
            withAnimation(.easeInOut(duration: 0.2)) {
                showCopied = false
            }
        }
    }
}

#Preview("ServiceHeaderView") {
    ServiceHeaderView(
        service: Service(
            name: "my-web-service",
            spanCount: 1234,
            logCount: 567,
            resourceAttributes: [
                ResourceAttribute(key: "service.version", values: ["1.2.3"]),
                ResourceAttribute(key: "deployment.environment", values: ["production"]),
                ResourceAttribute(key: "host.name", values: ["server-1", "server-2"]),
                ResourceAttribute(key: "process.pid", values: ["12345", "12346"])
            ]
        )
    )
}

#Preview("ServiceHeaderView - No Attributes") {
    ServiceHeaderView(
        service: Service(
            name: "new-service",
            spanCount: 0,
            logCount: 0
        )
    )
}

#Preview("ServiceHeaderView - Expanded") {
    ServiceHeaderView(
        service: Service(
            name: "my-web-service",
            spanCount: 1234,
            logCount: 567,
            resourceAttributes: [
                ResourceAttribute(key: "service.version", values: ["1.2.3", "1.2.4"]),
                ResourceAttribute(key: "deployment.environment", values: ["production", "staging"]),
                ResourceAttribute(key: "service.namespace", values: ["backend"]),
                ResourceAttribute(key: "host.name", values: ["server-1", "server-2", "server-3"]),
                ResourceAttribute(key: "process.pid", values: ["12345", "12346"]),
                ResourceAttribute(key: "telemetry.sdk.name", values: ["@opentelemetry/sdk-node"]),
                ResourceAttribute(key: "telemetry.sdk.version", values: ["1.18.0"])
            ]
        )
    )
}
