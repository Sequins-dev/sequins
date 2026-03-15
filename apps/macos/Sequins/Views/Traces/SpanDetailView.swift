import SwiftUI
import SequinsData

struct SpanDetailView: View {
    let span: Span

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                // Header
                VStack(alignment: .leading, spacing: 8) {
                    HStack {
                        Text(span.operationName)
                            .font(.title2)
                            .fontWeight(.semibold)

                        Spacer()

                        SpanStatusBadge(status: span.status)
                    }

                    Text(span.serviceName)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                .padding()
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))

                // Timing Information
                GroupBox("Timing") {
                    VStack(alignment: .leading, spacing: 8) {
                        SpanDetailRow(label: "Duration", value: span.duration.formatted)
                        SpanDetailRow(label: "Start Time", value: span.startTime.date.formatted())
                        SpanDetailRow(label: "End Time", value: span.endTime.date.formatted())
                    }
                }

                // Trace Information
                GroupBox("Trace Information") {
                    VStack(alignment: .leading, spacing: 8) {
                        SpanDetailRow(label: "Trace ID", value: span.traceId, monospaced: true)
                        SpanDetailRow(label: "Span ID", value: span.spanId, monospaced: true)
                        if let parentSpanId = span.parentSpanId {
                            SpanDetailRow(label: "Parent Span ID", value: parentSpanId, monospaced: true)
                        }
                        SpanDetailRow(label: "Span Kind", value: spanKindText(span.spanKind))
                    }
                }

                // Attributes
                if !span.attributes.isEmpty {
                    GroupBox("Attributes") {
                        VStack(alignment: .leading, spacing: 4) {
                            ForEach(Array(span.attributes.keys.sorted()), id: \.self) { key in
                                if let value = span.attributes[key] {
                                    AttributeRow(key: key, value: value)
                                    if key != span.attributes.keys.sorted().last {
                                        Divider()
                                    }
                                }
                            }
                        }
                    }
                }

                // Events
                if !span.events.isEmpty {
                    GroupBox("Events (\(span.events.count))") {
                        VStack(alignment: .leading, spacing: 12) {
                            ForEach(Array(span.events.enumerated()), id: \.offset) { index, event in
                                VStack(alignment: .leading, spacing: 4) {
                                    HStack {
                                        Text(event.name)
                                            .fontWeight(.medium)
                                        Spacer()
                                        Text(event.timestamp.date.formatted(date: .omitted, time: .standard))
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                    }

                                    if !event.attributes.isEmpty {
                                        VStack(alignment: .leading, spacing: 2) {
                                            ForEach(Array(event.attributes.keys.sorted()), id: \.self) { key in
                                                if let value = event.attributes[key] {
                                                    HStack {
                                                        Text(key + ":")
                                                            .font(.caption)
                                                            .foregroundStyle(.secondary)
                                                        Text(attributeValueText(value))
                                                            .font(.caption)
                                                    }
                                                }
                                            }
                                        }
                                        .padding(.leading, 8)
                                    }
                                }
                                if index < span.events.count - 1 {
                                    Divider()
                                }
                            }
                        }
                    }
                }
            }
            .padding()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func spanKindText(_ kind: SpanKind) -> String {
        switch kind {
        case .unspecified: return "Unspecified"
        case .internal: return "Internal"
        case .server: return "Server"
        case .client: return "Client"
        case .producer: return "Producer"
        case .consumer: return "Consumer"
        }
    }

    private func attributeValueText(_ value: AttributeValue) -> String {
        switch value {
        case .string(let s): return s
        case .bool(let b): return b ? "true" : "false"
        case .int(let i): return String(i)
        case .double(let d): return String(d)
        case .stringArray(let arr): return "[" + arr.joined(separator: ", ") + "]"
        case .boolArray(let arr):
            let strings: [String] = arr.map { $0 ? "true" : "false" }
            return "[" + strings.joined(separator: ", ") + "]"
        case .intArray(let arr):
            let strings: [String] = arr.map(String.init)
            return "[" + strings.joined(separator: ", ") + "]"
        case .doubleArray(let arr):
            let strings: [String] = arr.map { String(describing: $0) }
            return "[" + strings.joined(separator: ", ") + "]"
        }
    }
}

struct SpanDetailRow: View {
    let label: String
    let value: String
    var monospaced: Bool = false

    var body: some View {
        HStack(alignment: .top) {
            Text(label + ":")
                .foregroundStyle(.secondary)
                .frame(width: 120, alignment: .leading)

            Text(value)
                .font(monospaced ? .system(.body, design: .monospaced) : .body)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }
}

struct AttributeRow: View {
    let key: String
    let value: AttributeValue

    var body: some View {
        HStack(alignment: .top) {
            Text(key)
                .foregroundStyle(.secondary)
                .frame(width: 200, alignment: .leading)

            Text(attributeValueText(value))
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(.vertical, 4)
    }

    private func attributeValueText(_ value: AttributeValue) -> String {
        switch value {
        case .string(let s): return s
        case .bool(let b): return b ? "true" : "false"
        case .int(let i): return String(i)
        case .double(let d): return String(d)
        case .stringArray(let arr): return "[" + arr.joined(separator: ", ") + "]"
        case .boolArray(let arr):
            let strings: [String] = arr.map { $0 ? "true" : "false" }
            return "[" + strings.joined(separator: ", ") + "]"
        case .intArray(let arr):
            let strings: [String] = arr.map(String.init)
            return "[" + strings.joined(separator: ", ") + "]"
        case .doubleArray(let arr):
            let strings: [String] = arr.map { String(describing: $0) }
            return "[" + strings.joined(separator: ", ") + "]"
        }
    }
}

#Preview {
    let sampleSpan = Span(
        traceId: "abc123def456",
        spanId: "span789",
        parentSpanId: "parent456",
        serviceName: "api-gateway",
        operationName: "POST /api/users",
        startTime: Date().addingTimeInterval(-5),
        endTime: Date(),
        duration: 5.0,
        attributes: [
            "http.method": .string("POST"),
            "http.status_code": .int(200),
            "http.url": .string("https://api.example.com/users")
        ],
        events: [
            SpanEvent(
                timestamp: Date().addingTimeInterval(-3),
                name: "Database Query",
                attributes: ["query": .string("SELECT * FROM users")]
            )
        ],
        status: .ok,
        spanKind: .server
    )

    SpanDetailView(span: sampleSpan)
}
