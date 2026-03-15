import SwiftUI
import SequinsData

/// Right panel showing detailed span attributes
struct SpanDetailsPanel: View {
    let span: Span
    let traceStartTime: Timestamp

    /// Offset from trace start
    private var startOffset: NanoDuration {
        span.startTime - traceStartTime
    }

    private var endOffset: NanoDuration {
        span.endTime - traceStartTime
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            // Header
            HStack {
                Text("Span Details")
                    .font(.headline)
                Spacer()
                SpanStatusBadge(status: span.status)
            }

            Divider()

            // Basic info
            VStack(alignment: .leading, spacing: 12) {
                DetailRow(label: "Operation", value: span.operationName)
                DetailRow(label: "Service", value: span.serviceName)
                DetailRow(label: "Span ID", value: span.spanId, monospaced: true, copyable: true)
                if let parentId = span.parentSpanId, !parentId.isEmpty {
                    DetailRow(label: "Parent ID", value: parentId, monospaced: true)
                }
                DetailRow(label: "Start", value: "@ \(startOffset.formatted)", monospaced: true)
                DetailRow(label: "End", value: "@ \(endOffset.formatted)", monospaced: true)
                DetailRow(label: "Duration", value: span.duration.formatted)
                DetailRow(label: "Kind", value: spanKindDisplayName(span.spanKind))
            }

            if !span.attributes.isEmpty {
                Divider()

                // Attributes
                VStack(alignment: .leading, spacing: 8) {
                    Text("Attributes")
                        .font(.caption.weight(.medium))
                        .foregroundColor(.secondary)

                    ForEach(Array(span.attributes.keys.sorted()), id: \.self) { key in
                        if let value = span.attributes[key] {
                            HStack(alignment: .top, spacing: 8) {
                                Text(key)
                                    .font(.caption.monospaced())
                                    .foregroundColor(.secondary)
                                    .frame(minWidth: 100, alignment: .trailing)

                                AttributeValuesView(
                                    values: formatAttributeValues(value),
                                    monospaced: true,
                                    copyable: true
                                )

                                Spacer()
                            }
                        }
                    }
                }
            }
        }
        .padding()
    }

    private func spanKindDisplayName(_ kind: SpanKind) -> String {
        switch kind {
        case .unspecified: return "Unspecified"
        case .internal: return "Internal"
        case .server: return "Server"
        case .client: return "Client"
        case .producer: return "Producer"
        case .consumer: return "Consumer"
        }
    }

    private func formatAttributeValues(_ value: AttributeValue) -> [String] {
        switch value {
        case .string(let s): return [s]
        case .int(let i): return ["\(i)"]
        case .double(let d): return [String(format: "%.4f", d)]
        case .bool(let b): return [b ? "true" : "false"]
        case .stringArray(let arr): return arr
        case .intArray(let arr): return arr.map { "\($0)" }
        case .doubleArray(let arr): return arr.map { String(format: "%.4f", $0) }
        case .boolArray(let arr): return arr.map { $0 ? "true" : "false" }
        }
    }
}

/// Status badge specific to spans
struct SpanStatusBadge: View {
    let status: SpanStatus

    var body: some View {
        Text(statusText)
            .font(.caption)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(backgroundColor)
            .foregroundColor(.white)
            .clipShape(Capsule())
    }

    private var statusText: String {
        switch status {
        case .unset: return "—"
        case .ok: return "OK"
        case .error: return "Error"
        }
    }

    private var backgroundColor: Color {
        switch status {
        case .unset: return .gray
        case .ok: return .green
        case .error: return .red
        }
    }
}

#Preview("SpanDetailsPanel") {
    let traceStart = Timestamp(Date().addingTimeInterval(-120))
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "1234567890abcdef",
        parentSpanId: nil,
        serviceName: "api-gateway",
        operationName: "GET /api/users",
        startTime: traceStart,
        endTime: traceStart + .milliseconds(200),
        duration: .milliseconds(200),
        attributes: [
            "http.method": .string("GET"),
            "http.status_code": .int(200),
            "http.url": .string("https://api.example.com/v1/users"),
            "server.hosts": .stringArray(["server-1", "server-2", "server-3"]),
            "request.retries": .intArray([1, 2, 3])
        ],
        events: [],
        status: .ok,
        spanKind: .server
    )

    ScrollView {
        SpanDetailsPanel(span: sampleSpan, traceStartTime: traceStart)
    }
    .frame(width: 350, height: 500)
}

#Preview("SpanStatusBadge - All States") {
    HStack(spacing: 12) {
        SpanStatusBadge(status: .ok)
        SpanStatusBadge(status: .error)
        SpanStatusBadge(status: .unset)
    }
    .padding()
}
