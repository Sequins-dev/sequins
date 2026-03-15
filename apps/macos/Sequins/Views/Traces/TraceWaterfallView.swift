import SwiftUI
import SequinsData

/// Waterfall timeline visualization for trace spans
struct TraceWaterfallView: View {
    let rootSpan: Span
    let allSpans: [Span]
    @Binding var selectedSpanId: String?

    private var traceStartTime: Timestamp {
        allSpans.map(\.startTime).min() ?? rootSpan.startTime
    }

    private var traceDuration: TimeInterval {
        let endTimes = allSpans.map { $0.startTime + $0.duration }
        let maxEnd = endTimes.max() ?? (rootSpan.startTime + rootSpan.duration)
        let duration = (maxEnd - traceStartTime).timeInterval
        // Use actual duration - spans should fill the viewport
        // Only prevent division by zero with a nanosecond minimum
        return duration > 0 ? duration : 0.000000001
    }

    private var treeNodes: [SpanTreeNode] {
        SpanTreeBuilder.buildTree(from: allSpans)
    }

    private var serviceColorMapper: ServiceColorMapper {
        var mapper = ServiceColorMapper()
        // Assign colors in tree order for consistency
        for node in treeNodes {
            _ = mapper.color(for: node.span.serviceName)
        }
        return mapper
    }

    var body: some View {
        VStack(spacing: 0) {
            ScrollView(.vertical) {
                LazyVStack(alignment: .leading, spacing: 0) {
                    var colorMapper = serviceColorMapper

                    ForEach(treeNodes, id: \.span.spanId) { node in
                        WaterfallRow(
                            span: node.span,
                            traceStartTime: traceStartTime,
                            traceDuration: traceDuration,
                            isSelected: selectedSpanId == node.span.spanId,
                            serviceColor: colorMapper.color(for: node.span.serviceName),
                            depth: node.depth,
                            isLastChild: node.isLastChild,
                            ancestorIsLast: node.ancestorIsLast,
                            onSelect: { selectedSpanId = node.span.spanId }
                        )
                    }
                }
                .padding(.horizontal)
                .padding(.top, 8)
                .padding(.bottom, 4)
            }

            // Timeline ruler at bottom
            Divider()
            TimelineRuler(traceDuration: traceDuration)
                .padding(.horizontal)
        }
    }
}

#Preview("TraceWaterfallView") {
    struct PreviewWrapper: View {
        @State private var selectedSpanId: String?

        private var sampleSpans: [Span] {
            let traceId = "abc123def456789012345678901234ab"
            let rootSpanId = "1234567890abcdef"
            let baseTime = Date().addingTimeInterval(-120)

            return [
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
                ),
                Span(
                    traceId: traceId,
                    spanId: "4234567890abcdef",
                    parentSpanId: "2234567890abcdef",
                    serviceName: "user-service",
                    operationName: "connection.acquire",
                    startTime: baseTime.addingTimeInterval(0.06),
                    endTime: baseTime.addingTimeInterval(0.08),
                    duration: 0.02,
                    attributes: [:],
                    events: [],
                    status: .ok,
                    spanKind: .internal
                ),
                Span(
                    traceId: traceId,
                    spanId: "5234567890abcdef",
                    parentSpanId: "2234567890abcdef",
                    serviceName: "user-service",
                    operationName: "query.execute",
                    startTime: baseTime.addingTimeInterval(0.09),
                    endTime: baseTime.addingTimeInterval(0.34),
                    duration: 0.25,
                    attributes: [:],
                    events: [],
                    status: .ok,
                    spanKind: .internal
                ),
            ]
        }

        var body: some View {
            TraceWaterfallView(
                rootSpan: sampleSpans[0],
                allSpans: sampleSpans,
                selectedSpanId: $selectedSpanId
            )
            .frame(width: 800, height: 300)
            .background(Color(NSColor.controlBackgroundColor))
        }
    }
    return PreviewWrapper()
}

#Preview("TraceWaterfallView - With Error") {
    struct PreviewWrapper: View {
        @State private var selectedSpanId: String?

        private var sampleSpans: [Span] {
            let traceId = "abc123def456789012345678901234ab"
            let rootSpanId = "1234567890abcdef"
            let baseTime = Date().addingTimeInterval(-120)

            return [
                Span(
                    traceId: traceId,
                    spanId: rootSpanId,
                    parentSpanId: nil,
                    serviceName: "api-gateway",
                    operationName: "GET /api/users",
                    startTime: baseTime,
                    endTime: baseTime.addingTimeInterval(0.5),
                    duration: 0.5,
                    attributes: [:],
                    events: [],
                    status: .error,
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
                    attributes: [:],
                    events: [],
                    status: .error,
                    spanKind: .client
                ),
                Span(
                    traceId: traceId,
                    spanId: "3234567890abcdef",
                    parentSpanId: rootSpanId,
                    serviceName: "auth-service",
                    operationName: "validate.token",
                    startTime: baseTime.addingTimeInterval(0.36),
                    endTime: baseTime.addingTimeInterval(0.45),
                    duration: 0.09,
                    attributes: [:],
                    events: [],
                    status: .ok,
                    spanKind: .server
                ),
            ]
        }

        var body: some View {
            TraceWaterfallView(
                rootSpan: sampleSpans[0],
                allSpans: sampleSpans,
                selectedSpanId: $selectedSpanId
            )
            .frame(width: 800, height: 250)
            .background(Color(NSColor.controlBackgroundColor))
        }
    }
    return PreviewWrapper()
}
