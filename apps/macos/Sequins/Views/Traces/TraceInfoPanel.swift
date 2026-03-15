import SwiftUI
import SequinsData

/// Middle panel showing detailed information about the selected trace
struct TraceInfoPanel: View {
    let span: Span
    let traceSpans: [Span]

    private var services: Set<String> {
        Set(traceSpans.map(\.serviceName))
    }

    private var errorSpans: [Span] {
        traceSpans.filter { $0.status == .error }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            // Header
            VStack(alignment: .leading, spacing: 8) {
                Text(span.operationName)
                    .font(.title3.weight(.medium))

                HStack(spacing: 16) {
                    Label("\(traceSpans.count) spans", systemImage: "square.3.layers.3d")
                    Label("\(services.count) services", systemImage: "network")
                    if !errorSpans.isEmpty {
                        Label("\(errorSpans.count) errors", systemImage: "exclamationmark.triangle.fill")
                            .foregroundColor(.red)
                    }
                }
                .font(.caption)
                .foregroundColor(.secondary)
            }

            Divider()

            // Details
            VStack(alignment: .leading, spacing: 12) {
                DetailRow(label: "Trace ID", value: span.traceId, monospaced: true, copyable: true)
                DetailRow(label: "Duration", value: span.duration.formatted)
                DetailRow(label: "Start Time", value: formatDetailTimestamp(span.startTime.date))
                DetailRow(label: "Service", value: span.serviceName)
            }

            Divider()

            // Services legend with colors matching the waterfall
            ServiceLegend(spans: traceSpans)

            if !errorSpans.isEmpty {
                Divider()

                // Errors
                VStack(alignment: .leading, spacing: 8) {
                    Text("Errors")
                        .font(.caption.weight(.medium))
                        .foregroundColor(.red)

                    ForEach(errorSpans, id: \.spanId) { errorSpan in
                        VStack(alignment: .leading, spacing: 4) {
                            Text(errorSpan.operationName)
                                .font(.caption.weight(.medium))
                            Text(errorSpan.serviceName)
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                        .padding(8)
                        .background(Color.red.opacity(0.1))
                        .cornerRadius(4)
                    }
                }
            }
        }
    }

    private func formatDetailTimestamp(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateStyle = .short
        formatter.timeStyle = .medium
        return formatter.string(from: date)
    }
}

/// Empty state when no trace is selected
struct TraceEmptyStateView: View {
    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "square.3.layers.3d")
                .font(.system(size: 48))
                .foregroundColor(.secondary)

            Text("Select a trace to view details")
                .font(.title2)
                .foregroundColor(.secondary)

            Text("Choose a trace from the list to see its spans and details.")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: 300)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

#Preview("TraceInfoPanel") {
    let traceId = "abc123def456789012345678901234ab"
    let rootSpanId = "1234567890abcdef"
    let baseTime = Date().addingTimeInterval(-120)

    let sampleSpans = [
        Span(
            traceId: traceId,
            spanId: rootSpanId,
            parentSpanId: nil,
            serviceName: "api-gateway",
            operationName: "GET /api/users",
            startTime: baseTime,
            endTime: baseTime.addingTimeInterval(0.5),
            duration: 0.5,
            attributes: ["http.method": .string("GET"), "http.status_code": .int(200)],
            events: [],
            status: .ok,
            spanKind: .server
        ),
        Span(
            traceId: traceId,
            spanId: "2234567890abcdef",
            parentSpanId: rootSpanId,
            serviceName: "user-service",
            operationName: "database.query",
            startTime: baseTime.addingTimeInterval(0.05),
            endTime: baseTime.addingTimeInterval(0.35),
            duration: 0.3,
            attributes: ["db.system": .string("postgresql")],
            events: [],
            status: .ok,
            spanKind: .client
        ),
        Span(
            traceId: traceId,
            spanId: "3234567890abcdef",
            parentSpanId: rootSpanId,
            serviceName: "cache-service",
            operationName: "cache.get",
            startTime: baseTime.addingTimeInterval(0.36),
            endTime: baseTime.addingTimeInterval(0.38),
            duration: 0.02,
            attributes: [:],
            events: [],
            status: .ok,
            spanKind: .internal
        )
    ]

    return ScrollView {
        TraceInfoPanel(
            span: sampleSpans[0],
            traceSpans: sampleSpans
        )
        .padding()
    }
    .frame(width: 400, height: 500)
}

#Preview("TraceEmptyStateView") {
    TraceEmptyStateView()
        .frame(width: 400, height: 400)
}
