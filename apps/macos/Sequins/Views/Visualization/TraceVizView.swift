import SwiftUI
import SequinsData

/// Renders a span/trace query as a real waterfall: decodes `[Span]` from the result
/// columns (timestamps arrive as `Date`), picks the largest trace, and hands it to the
/// existing `TraceWaterfallView`. Shows a clear message when the result isn't
/// trace-shaped (no `trace_id`/`span_id`).
struct TraceVizView: View {
    let columns: [String]
    let rows: [[Any?]]

    @State private var selectedSpanId: String?

    private func index(_ names: String...) -> Int? {
        for name in names {
            if let i = columns.firstIndex(where: { $0.lowercased() == name }) { return i }
        }
        return nil
    }

    private func nanos(_ row: [Any?], _ idx: Int?) -> Int64 {
        guard let i = idx, i < row.count else { return 0 }
        if let d = row[i] as? Date { return Int64(d.timeIntervalSince1970 * 1_000_000_000) }
        if let n = VizFormat.numeric(row[i]) { return Int64(n) }
        return 0
    }

    private var spans: [Span] {
        guard let traceIdx = index("trace_id", "traceid"),
              let spanIdx = index("span_id", "spanid") else { return [] }
        let parentIdx = index("parent_span_id", "parentspanid")
        let nameIdx = index("name", "operation_name", "span_name")
        let startIdx = index("start_time_unix_nano", "start_time", "start")
        let endIdx = index("end_time_unix_nano", "end_time", "end")
        let durIdx = index("duration_ns", "duration")
        let svcIdx = index("service_name", "service")
        let statusIdx = index("status", "status_code")
        let kindIdx = index("kind", "span_kind")

        var out: [Span] = []
        for row in rows {
            guard traceIdx < row.count, spanIdx < row.count else { continue }
            let spanId = VizFormat.string(row[spanIdx])
            guard !spanId.isEmpty else { continue }
            let parent = parentIdx.flatMap { $0 < row.count ? row[$0] as? String : nil }
                .flatMap { $0.isEmpty ? nil : $0 }
            let startNs = nanos(row, startIdx)
            let endNs = nanos(row, endIdx)
            let durNs: Int64 = {
                if let d = durIdx, d < row.count, let n = VizFormat.numeric(row[d]) { return Int64(n) }
                return max(0, endNs - startNs)
            }()
            let statusRaw = statusIdx.flatMap { $0 < row.count ? VizFormat.numeric(row[$0]) : nil } ?? 0
            let kindRaw = kindIdx.flatMap { $0 < row.count ? VizFormat.numeric(row[$0]) : nil } ?? 0
            out.append(Span(
                traceId: VizFormat.string(row[traceIdx]),
                spanId: spanId,
                parentSpanId: parent,
                serviceName: svcIdx.map { VizFormat.string(row[$0]) } ?? "",
                operationName: nameIdx.map { VizFormat.string(row[$0]) } ?? spanId,
                startTime: Timestamp(nanoseconds: startNs),
                endTime: Timestamp(nanoseconds: endNs > 0 ? endNs : startNs + durNs),
                duration: NanoDuration(nanoseconds: durNs),
                attributes: [:],
                events: [],
                status: SpanStatus(rawValue: UInt32(max(0, statusRaw))) ?? .unset,
                spanKind: SpanKind(rawValue: UInt32(max(0, kindRaw))) ?? .unspecified
            ))
        }
        return out
    }

    var body: some View {
        let all = spans
        if all.isEmpty {
            VizMessage(
                icon: "arrow.triangle.branch",
                text: "Not trace-shaped — query spans with trace_id/span_id (e.g. `traces last 15m`)."
            )
        } else {
            let byTrace = Dictionary(grouping: all, by: \.traceId)
            let chosen = byTrace.max { $0.value.count < $1.value.count }?.value ?? all
            let root = chosen.first { span in
                span.parentSpanId == nil || !chosen.contains { $0.spanId == span.parentSpanId }
            } ?? chosen.min { $0.startTime.nanoseconds < $1.startTime.nanoseconds } ?? chosen[0]

            VStack(spacing: 4) {
                if byTrace.count > 1 {
                    Text("Showing the largest of \(byTrace.count) traces (\(chosen.count) spans)")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(.horizontal, 8)
                        .padding(.top, 4)
                }
                TraceWaterfallView(rootSpan: root, allSpans: chosen, selectedSpanId: $selectedSpanId)
            }
        }
    }
}
