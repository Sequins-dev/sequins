import SwiftUI
import SequinsData

/// Left panel displaying the list of traces
struct TraceListPanel: View {
    let spans: [Span]
    let selectedSpanId: String?
    let isLoading: Bool
    let sortBy: TraceSortBy
    let sortOrder: SortOrder
    let onSelect: (Span) -> Void

    /// Group spans by trace
    private var traceGroups: [(traceId: String, rootSpan: Span, spanCount: Int, hasError: Bool)] {
        var groups: [String: (root: Span, count: Int, hasError: Bool)] = [:]

        for span in spans {
            if let existing = groups[span.traceId] {
                // Update if this is a root span (no parent) or count
                let isRoot = span.parentSpanId == nil || span.parentSpanId?.isEmpty == true
                groups[span.traceId] = (
                    root: isRoot ? span : existing.root,
                    count: existing.count + 1,
                    hasError: existing.hasError || span.status == .error
                )
            } else {
                groups[span.traceId] = (
                    root: span,
                    count: 1,
                    hasError: span.status == .error
                )
            }
        }

        let ascending = sortOrder == .ascending
        return groups.map { (traceId: $0.key, rootSpan: $0.value.root, spanCount: $0.value.count, hasError: $0.value.hasError) }
            .sorted { lhs, rhs in
                switch sortBy {
                case .startTime:
                    return ascending
                        ? lhs.rootSpan.startTime.nanoseconds < rhs.rootSpan.startTime.nanoseconds
                        : lhs.rootSpan.startTime.nanoseconds > rhs.rootSpan.startTime.nanoseconds
                case .duration:
                    return ascending
                        ? lhs.rootSpan.duration.nanoseconds < rhs.rootSpan.duration.nanoseconds
                        : lhs.rootSpan.duration.nanoseconds > rhs.rootSpan.duration.nanoseconds
                case .service:
                    return ascending
                        ? lhs.rootSpan.serviceName < rhs.rootSpan.serviceName
                        : lhs.rootSpan.serviceName > rhs.rootSpan.serviceName
                }
            }
    }

    var body: some View {
        VStack(spacing: 0) {
            // Stats header
            if !traceGroups.isEmpty {
                TraceListStatsView(
                    traceCount: traceGroups.count,
                    errorCount: traceGroups.filter(\.hasError).count,
                    avgDuration: traceGroups.map { $0.rootSpan.duration.timeInterval }.reduce(0, +) / Double(max(1, traceGroups.count))
                )
            }

            // List
            if traceGroups.isEmpty && !isLoading {
                TraceListEmptyState()
            } else {
                ScrollView {
                    LazyVStack(spacing: 1) {
                        ForEach(traceGroups, id: \.traceId) { group in
                            TraceListRow(
                                span: group.rootSpan,
                                spanCount: group.spanCount,
                                hasError: group.hasError,
                                isSelected: selectedSpanId == group.rootSpan.spanId,
                                onSelect: { onSelect(group.rootSpan) }
                            )
                        }

                        if isLoading {
                            HStack {
                                ProgressView()
                                    .scaleEffect(0.8)
                                Text("Loading traces...")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                            }
                            .padding()
                        }
                    }
                }
            }
        }
    }
}

#Preview("TraceListPanel - Empty") {
    TraceListPanel(
        spans: [],
        selectedSpanId: nil,
        isLoading: false,
        sortBy: .startTime,
        sortOrder: .descending,
        onSelect: { _ in }
    )
    .frame(width: 300, height: 400)
}

#Preview("TraceListPanel - Loading") {
    TraceListPanel(
        spans: [],
        selectedSpanId: nil,
        isLoading: true,
        sortBy: .startTime,
        sortOrder: .descending,
        onSelect: { _ in }
    )
    .frame(width: 300, height: 400)
}
