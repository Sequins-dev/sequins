import SwiftUI
import SequinsData

/// Individual row in the waterfall visualization
struct WaterfallRow: View {
    let span: Span
    let traceStartTime: Timestamp
    let traceDuration: TimeInterval
    let isSelected: Bool
    let serviceColor: Color
    let depth: Int
    let isLastChild: Bool
    let ancestorIsLast: [Bool]
    let onSelect: () -> Void

    private let indentWidth: CGFloat = 16
    private let iconWidth: CGFloat = 20
    private let rowHeight: CGFloat = 24
    private let minBarWidth: CGFloat = 4

    private var offsetPercent: CGFloat {
        guard traceDuration > 0 else { return 0 }
        let offset = (span.startTime - traceStartTime).timeInterval
        return CGFloat(offset / traceDuration)
    }

    private var widthPercent: CGFloat {
        guard traceDuration > 0 else { return 0.01 }
        return max(0.005, CGFloat(span.duration.timeInterval / traceDuration))
    }

    private var spanKindIcon: String {
        switch span.spanKind {
        case .server: return "server.rack"
        case .client: return "arrow.up.right"
        case .internal: return "gearshape"
        case .producer: return "paperplane"
        case .consumer: return "tray.and.arrow.down"
        case .unspecified: return "questionmark.circle"
        }
    }

    var body: some View {
        Button(action: onSelect) {
            HStack(spacing: 0) {
                // Span kind icon
                Image(systemName: spanKindIcon)
                    .font(.caption2)
                    .foregroundColor(.secondary)
                    .frame(width: iconWidth)

                // Tree indentation
                treeIndentation

                // Span bar with label
                spanBarArea
            }
            .frame(height: rowHeight)
            .background(isSelected ? Color.accentColor.opacity(0.15) : Color.clear)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .help("\(span.serviceName): \(span.operationName)")
    }

    @ViewBuilder
    private var treeIndentation: some View {
        HStack(spacing: 0) {
            // Draw vertical lines for ancestors
            ForEach(0..<depth, id: \.self) { level in
                if level < ancestorIsLast.count && !ancestorIsLast[level] {
                    // Continuing vertical line
                    Rectangle()
                        .fill(Color.secondary.opacity(0.3))
                        .frame(width: 1)
                        .padding(.leading, indentWidth / 2 - 0.5)
                        .padding(.trailing, indentWidth / 2 - 0.5)
                } else {
                    // Empty space
                    Color.clear
                        .frame(width: indentWidth)
                }
            }

            // Draw connector for current node
            if depth > 0 {
                HStack(spacing: 0) {
                    VStack(spacing: 0) {
                        Rectangle()
                            .fill(Color.secondary.opacity(0.3))
                            .frame(width: 1)
                            .frame(maxHeight: isLastChild ? rowHeight / 2 : .infinity)
                        if isLastChild {
                            Spacer()
                        }
                    }
                    Rectangle()
                        .fill(Color.secondary.opacity(0.3))
                        .frame(width: indentWidth / 2 - 2, height: 1)
                }
                .frame(width: indentWidth / 2)
            }
        }
        .frame(width: CGFloat(depth) * indentWidth + (depth > 0 ? indentWidth / 2 : 0))
    }

    @ViewBuilder
    private var spanBarArea: some View {
        GeometryReader { geometry in
            let totalWidth = geometry.size.width
            let barOffset = totalWidth * offsetPercent
            let barWidth = max(minBarWidth, totalWidth * widthPercent)

            // Span bar positioned absolutely within the geometry
            RoundedRectangle(cornerRadius: 3)
                .fill(serviceColor)
                .frame(width: barWidth, height: 18)
                .overlay(
                    RoundedRectangle(cornerRadius: 3)
                        .stroke(
                            span.status == .error ? ServiceColorPalette.errorBorderColor : Color.clear,
                            lineWidth: 2
                        )
                )
                .overlay(
                    // Operation name inside bar
                    Text(span.operationName)
                        .font(.caption2)
                        .foregroundColor(.white)
                        .lineLimit(1)
                        .truncationMode(.tail)
                        .padding(.horizontal, 4)
                        .frame(maxWidth: barWidth - 8, alignment: .leading),
                    alignment: .leading
                )
                .position(x: barOffset + barWidth / 2, y: geometry.size.height / 2)
        }
    }
}

#Preview("WaterfallRow - Root Span") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "1234567890abcdef",
        parentSpanId: nil,
        serviceName: "api-gateway",
        operationName: "GET /api/users",
        startTime: Date().addingTimeInterval(-1),
        endTime: Date().addingTimeInterval(-0.5),
        duration: 0.5,
        attributes: [:],
        events: [],
        status: .ok,
        spanKind: .server
    )

    return WaterfallRow(
        span: sampleSpan,
        traceStartTime: Timestamp(Date().addingTimeInterval(-1)),
        traceDuration: 1.0,
        isSelected: false,
        serviceColor: ServiceColorPalette.colors[0],
        depth: 0,
        isLastChild: true,
        ancestorIsLast: [],
        onSelect: { }
    )
    .frame(width: 800)
    .padding()
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("WaterfallRow - Child Span") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "2234567890abcdef",
        parentSpanId: "1234567890abcdef",
        serviceName: "user-service",
        operationName: "database.query",
        startTime: Date().addingTimeInterval(-0.9),
        endTime: Date().addingTimeInterval(-0.6),
        duration: 0.3,
        attributes: [:],
        events: [],
        status: .ok,
        spanKind: .client
    )

    return WaterfallRow(
        span: sampleSpan,
        traceStartTime: Timestamp(Date().addingTimeInterval(-1)),
        traceDuration: 1.0,
        isSelected: false,
        serviceColor: ServiceColorPalette.colors[1],
        depth: 1,
        isLastChild: false,
        ancestorIsLast: [false],
        onSelect: { }
    )
    .frame(width: 800)
    .padding()
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("WaterfallRow - Error Span") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "3234567890abcdef",
        parentSpanId: "1234567890abcdef",
        serviceName: "auth-service",
        operationName: "validate.token",
        startTime: Date().addingTimeInterval(-0.8),
        endTime: Date().addingTimeInterval(-0.7),
        duration: 0.1,
        attributes: [:],
        events: [],
        status: .error,
        spanKind: .server
    )

    return WaterfallRow(
        span: sampleSpan,
        traceStartTime: Timestamp(Date().addingTimeInterval(-1)),
        traceDuration: 1.0,
        isSelected: false,
        serviceColor: ServiceColorPalette.colors[2],
        depth: 1,
        isLastChild: true,
        ancestorIsLast: [false],
        onSelect: { }
    )
    .frame(width: 800)
    .padding()
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("WaterfallRow - Selected") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "1234567890abcdef",
        parentSpanId: nil,
        serviceName: "api-gateway",
        operationName: "GET /api/users/profile/settings",
        startTime: Date().addingTimeInterval(-1),
        endTime: Date().addingTimeInterval(-0.5),
        duration: 0.5,
        attributes: [:],
        events: [],
        status: .ok,
        spanKind: .server
    )

    return WaterfallRow(
        span: sampleSpan,
        traceStartTime: Timestamp(Date().addingTimeInterval(-1)),
        traceDuration: 1.0,
        isSelected: true,
        serviceColor: ServiceColorPalette.colors[0],
        depth: 0,
        isLastChild: true,
        ancestorIsLast: [],
        onSelect: { }
    )
    .frame(width: 800)
    .padding()
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("WaterfallRow - Short Span with Ellipsis") {
    let sampleSpan = Span(
        traceId: "abc123def456789012345678901234ab",
        spanId: "4234567890abcdef",
        parentSpanId: "1234567890abcdef",
        serviceName: "cache-service",
        operationName: "cache.get",
        startTime: Date().addingTimeInterval(-0.5),
        endTime: Date().addingTimeInterval(-0.49),
        duration: 0.01,
        attributes: [:],
        events: [],
        status: .ok,
        spanKind: .internal
    )

    return WaterfallRow(
        span: sampleSpan,
        traceStartTime: Timestamp(Date().addingTimeInterval(-1)),
        traceDuration: 1.0,
        isSelected: false,
        serviceColor: ServiceColorPalette.colors[3],
        depth: 2,
        isLastChild: true,
        ancestorIsLast: [false, true],
        onSelect: { }
    )
    .frame(width: 800)
    .padding()
    .background(Color(NSColor.controlBackgroundColor))
}
