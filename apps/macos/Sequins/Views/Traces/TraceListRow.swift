import SwiftUI
import SequinsData

/// Individual row in the trace list
struct TraceListRow: View {
    let span: Span
    let spanCount: Int
    let hasError: Bool
    let isSelected: Bool
    let onSelect: () -> Void

    var body: some View {
        Button(action: onSelect) {
            HStack(spacing: 10) {
                // Duration with health indicator bar
                VStack(spacing: 2) {
                    Text(span.duration.formatted)
                        .font(.caption.monospacedDigit())
                        .foregroundColor(span.duration.timeInterval > 1 ? .orange : .primary)
                        .frame(width: 50, alignment: .center)

                    // Duration/health bar
                    Rectangle()
                        .fill(hasError ? Color.red.opacity(0.6) : Color.green.opacity(0.6))
                        .frame(width: 40, height: 3)
                        .cornerRadius(1.5)
                }
                .frame(width: 50)

                VStack(alignment: .leading, spacing: 2) {
                    // Operation name
                    Text(span.operationName)
                        .font(.system(.body, design: .monospaced))
                        .lineLimit(1)

                    // Timestamp and span count
                    HStack(spacing: 4) {
                        Text(formatTimestamp(span.startTime.date))
                            .font(.caption)
                            .foregroundColor(.secondary)

                        Text("•")
                            .font(.caption)
                            .foregroundColor(.secondary)

                        Text("\(spanCount) spans")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }

                Spacer()
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .background(
            Rectangle()
                .fill(isSelected ? Color.accentColor.opacity(0.1) : Color.clear)
        )
        .overlay(
            Rectangle()
                .frame(height: 1)
                .foregroundColor(Color(NSColor.separatorColor)),
            alignment: .bottom
        )
    }

    private func formatTimestamp(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.timeStyle = .medium
        return formatter.string(from: date)
    }
}

/// Empty state for the trace list
struct TraceListEmptyState: View {
    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: "square.3.layers.3d")
                .font(.system(size: 48))
                .foregroundColor(.secondary)

            Text("No traces found")
                .font(.title2)
                .foregroundColor(.secondary)

            Text("Try adjusting your filters or time range.")
                .font(.body)
                .foregroundColor(.secondary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: 200)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

#Preview("TraceListRow - Normal") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "1234567890abcdef",
        parentSpanId: nil,
        serviceName: "api-gateway",
        operationName: "GET /api/users",
        startTime: Date().addingTimeInterval(-120),
        endTime: Date().addingTimeInterval(-119.8),
        duration: 0.2,
        attributes: [:],
        events: [],
        status: .ok,
        spanKind: .server
    )

    return TraceListRow(
        span: sampleSpan,
        spanCount: 5,
        hasError: false,
        isSelected: false,
        onSelect: { }
    )
    .frame(width: 300)
}

#Preview("TraceListRow - Selected") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "1234567890abcdef",
        parentSpanId: nil,
        serviceName: "api-gateway",
        operationName: "GET /api/users",
        startTime: Date().addingTimeInterval(-120),
        endTime: Date().addingTimeInterval(-119.8),
        duration: 0.2,
        attributes: [:],
        events: [],
        status: .ok,
        spanKind: .server
    )

    return TraceListRow(
        span: sampleSpan,
        spanCount: 12,
        hasError: false,
        isSelected: true,
        onSelect: { }
    )
    .frame(width: 300)
}

#Preview("TraceListRow - Error") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "1234567890abcdef",
        parentSpanId: nil,
        serviceName: "api-gateway",
        operationName: "GET /api/users",
        startTime: Date().addingTimeInterval(-120),
        endTime: Date().addingTimeInterval(-119.8),
        duration: 0.2,
        attributes: [:],
        events: [],
        status: .error,
        spanKind: .server
    )

    return TraceListRow(
        span: sampleSpan,
        spanCount: 3,
        hasError: true,
        isSelected: false,
        onSelect: { }
    )
    .frame(width: 300)
}

#Preview("TraceListEmptyState") {
    TraceListEmptyState()
        .frame(width: 300, height: 300)
}
