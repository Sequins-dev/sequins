import SwiftUI
import SequinsData

/// Renders a TraceTimeline response shape as a waterfall view.
///
/// Converts `[[Any?]]` row data (12-column span schema:
/// trace_id, span_id, parent_span_id, name, kind(UInt8), status(UInt8),
/// start_time_unix_nano, end_time_unix_nano, duration_ns, resource_id, scope_id, attributes)
/// into `Span` objects and delegates to `TraceWaterfallView`.
/// Column indices are resolved dynamically from schema names so this view is robust to ordering changes.
struct ExploreTraceTimelineView: View {
    let columns: [String]
    let rows: [[Any?]]

    @State private var selectedSpanId: String?

    // Column index lookup by name
    private func colIndex(_ name: String) -> Int? {
        columns.firstIndex(of: name)
    }

    private var spans: [Span] {
        let iTraceId      = colIndex("trace_id") ?? 0
        let iSpanId       = colIndex("span_id") ?? 1
        let iParentSpanId = colIndex("parent_span_id") ?? 2
        let iOpName       = colIndex("name") ?? 3
        let iSpanKind     = colIndex("kind") ?? 4
        let iStatus       = colIndex("status") ?? 5
        let iStartTime    = colIndex("start_time_unix_nano") ?? 6
        let iEndTime      = colIndex("end_time_unix_nano") ?? 7
        let iDuration     = colIndex("duration_ns") ?? 8

        return rows.compactMap { row in
            guard row.count > iSpanId else { return nil }

            let traceId      = row[safe: iTraceId]    as? String ?? ""
            let spanId       = row[safe: iSpanId]     as? String ?? ""
            let parentRaw    = row[safe: iParentSpanId] as? String
            let parentSpanId = parentRaw.flatMap { $0.isEmpty ? nil : $0 }
            let opName       = row[safe: iOpName]       as? String ?? ""

            // Timestamps come through JSON as NSNumber (large u64 integers)
            let startNs  = (row[safe: iStartTime]  as? NSNumber)?.int64Value ?? 0
            let endNs    = (row[safe: iEndTime]    as? NSNumber)?.int64Value ?? 0
            let durNs    = (row[safe: iDuration]   as? NSNumber)?.int64Value ?? (endNs - startNs)

            // kind and status are UInt8 integers in the new schema
            let kindRaw   = (row[safe: iSpanKind] as? NSNumber)?.uint32Value ?? 0
            let statusRaw = (row[safe: iStatus]   as? NSNumber)?.uint32Value ?? 0

            return Span(
                traceId:       traceId,
                spanId:        spanId,
                parentSpanId:  parentSpanId,
                serviceName:   "",
                operationName: opName,
                startTime:     Timestamp(nanoseconds: startNs),
                endTime:       Timestamp(nanoseconds: endNs),
                duration:      NanoDuration(nanoseconds: durNs),
                attributes:    [:],
                events:        [],
                status:        SpanStatus(rawValue: statusRaw) ?? .unset,
                spanKind:      SpanKind(rawValue: kindRaw) ?? .unspecified
            )
        }
    }

    var body: some View {
        let allSpans = spans
        if allSpans.isEmpty {
            Text("No spans to display")
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            // Find the root span: no parent, or earliest start among those without a parent
            let rootSpan = allSpans.first(where: { $0.parentSpanId == nil })
                ?? allSpans.min(by: { $0.startTime < $1.startTime })!
            TraceWaterfallView(
                rootSpan: rootSpan,
                allSpans: allSpans,
                selectedSpanId: $selectedSpanId
            )
        }
    }

}

// MARK: - Array safe subscript

private extension Array {
    subscript(safe index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}
