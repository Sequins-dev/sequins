import AppKit
import Foundation
import SequinsData

/// HTTP status code range categories for filtering
enum HTTPStatusCodeRange: String, CaseIterable, Hashable {
    case success2xx = "2xx"
    case redirect3xx = "3xx"
    case clientError4xx = "4xx"
    case serverError5xx = "5xx"

    var displayName: String { rawValue }

    func contains(_ statusCode: Int64) -> Bool {
        switch self {
        case .success2xx: return statusCode >= 200 && statusCode < 300
        case .redirect3xx: return statusCode >= 300 && statusCode < 400
        case .clientError4xx: return statusCode >= 400 && statusCode < 500
        case .serverError5xx: return statusCode >= 500 && statusCode < 600
        }
    }
}

// MARK: - Attribute extraction helpers

/// Core span column names that are not promoted attributes.
private let coreSpanColumns: Set<String> = [
    "trace_id", "span_id", "parent_span_id", "name", "kind", "status",
    "start_time_unix_nano", "end_time_unix_nano", "duration_ns",
    "resource_id", "scope_id", "_overflow_attrs"
]

/// Maps Arrow column names (underscore form) to OTLP dot-notation attribute keys.
private let spanPromotedColumnNameToKey: [String: String] = [
    "http_request_method": "http.request.method",
    "http_method": "http.method",
    "http_response_status_code": "http.response.status_code",
    "http_status_code": "http.status.code",
    "http_route": "http.route",
    "url_path": "url.path",
    "url_full": "url.full",
    "http_url": "http.url",
    "server_address": "server.address",
    "server_port": "server.port",
    "db_system": "db.system",
    "db_name": "db.name",
    "db_operation_name": "db.operation.name",
    "db_operation": "db.operation",
    "db_statement": "db.statement",
    "db_collection_name": "db.collection.name",
    "rpc_system": "rpc.system",
    "rpc_method": "rpc.method",
    "rpc_service": "rpc.service",
    "rpc_grpc_status_code": "rpc.grpc.status_code",
    "messaging_system": "messaging.system",
    "messaging_destination_name": "messaging.destination.name",
    "messaging_operation_name": "messaging.operation.name",
    "error_type": "error.type",
    "exception_type": "exception.type",
    "exception_message": "exception.message",
    "network_peer_address": "network.peer.address",
    "net_peer_name": "net.peer.name",
    "net_peer_port": "net.peer.port",
    "service_name_attr": "service.name",
    "service_version": "service.version",
    "deployment_environment_name": "deployment.environment.name",
    "k8s_namespace_name": "k8s.namespace.name",
    "k8s_pod_name": "k8s.pod.name",
    "k8s_deployment_name": "k8s.deployment.name",
]

private func spanAttributeValueFromAny(_ raw: Any) -> AttributeValue? {
    switch raw {
    case let v as Bool:   return .bool(v)
    case let v as String: return .string(v)
    case let v as Int64:  return .int(v)
    case let v as Double: return .double(v)
    case let v as Float:  return .double(Double(v))
    case let v as Int:    return .int(Int64(v))
    case let v as UInt8:  return .int(Int64(v))
    case let v as UInt32: return .int(Int64(v))
    default:              return nil
    }
}

/// Extract per-row attribute dictionaries from a span RecordBatch.
private func extractSpanAttributes(from batch: RecordBatch) -> [[String: AttributeValue]] {
    let numRows = Int(batch.length)
    guard numRows > 0 else { return [] }

    var promotedCols: [(colIdx: Int, attrKey: String)] = []
    for (idx, field) in batch.schema.fields.enumerated() {
        if !coreSpanColumns.contains(field.name) {
            let key = spanPromotedColumnNameToKey[field.name] ?? field.name
            promotedCols.append((idx, key))
        }
    }

    var result = [[String: AttributeValue]](repeating: [:], count: numRows)
    for (colIdx, attrKey) in promotedCols {
        guard colIdx < batch.columns.count else { continue }
        let col = batch.columns[colIdx].array
        for row in 0..<numRows {
            guard let raw = col.asAny(UInt(row)) else { continue }
            if let val = spanAttributeValueFromAny(raw) {
                result[row][attrKey] = val
            }
        }
    }

    let overflow = batch.overflowAttributes()
    for row in 0..<numRows {
        if row < overflow.count {
            for (key, val) in overflow[row] {
                result[row][key] = val
            }
        }
    }
    return result
}

/// Decode a span RecordBatch into `[Span]`, extracting promoted and overflow attributes.
private func decodeSpanBatch(_ batch: RecordBatch, serviceName: String) -> [Span] {
    let decoder = ArrowDecoder(batch)
    guard let records = try? decoder.decode(SpanRecord.self) else { return [] }
    let attrs = extractSpanAttributes(from: batch)
    return records.enumerated().map { idx, record in
        record.toSpan(
            serviceName: serviceName,
            attributes: idx < attrs.count ? attrs[idx] : [:]
        )
    }
}

// MARK: - Codable DTO for Arrow span schema

/// Arrow-schema Codable model matching the span RecordBatch schema.
/// Field names match Arrow column names exactly.
private struct SpanRecord: Codable {
    let trace_id: String
    let span_id: String
    let parent_span_id: String?
    let name: String
    let kind: UInt8
    let status: UInt8
    let start_time_unix_nano: Date   // TimestampArray.asAny() → Date
    let end_time_unix_nano: Date
    let duration_ns: Int64
    let resource_id: UInt32
    let scope_id: UInt32

    func toSpan(serviceName: String, attributes: [String: AttributeValue] = [:]) -> Span {
        let spanKind = SequinsData.SpanKind(rawValue: UInt32(kind)) ?? .internal
        let spanStatus = SequinsData.SpanStatus(rawValue: UInt32(status)) ?? .unset
        let startNs = Int64(start_time_unix_nano.timeIntervalSince1970 * 1_000_000_000)
        let endNs = Int64(end_time_unix_nano.timeIntervalSince1970 * 1_000_000_000)
        return Span(
            traceId: trace_id,
            spanId: span_id,
            parentSpanId: parent_span_id,
            serviceName: serviceName,
            operationName: name,
            startTime: SequinsData.Timestamp(nanoseconds: startNs),
            endTime: SequinsData.Timestamp(nanoseconds: endNs),
            duration: SequinsData.NanoDuration(nanoseconds: duration_ns),
            attributes: attributes,
            events: [],
            status: spanStatus,
            spanKind: spanKind
        )
    }
}

@MainActor
@Observable
final class TracesViewModel {
    // MARK: - Observable State

    /// Loading state
    private(set) var isLoading: Bool = false

    /// Error from query
    private(set) var error: String?

    // MARK: - Private State

    /// Reactive table view backing this view model.
    private var tableView: ReactiveTableView?

    /// Data source reference
    private weak var dataSource: DataSource?

    /// Service name captured at query time
    private var currentServiceName: String?

    // MARK: - Time Range

    var timeRange: TimeRange = .last(hours: 1)

    // MARK: - Selection State

    var selectedSpanId: String? {
        didSet { scheduleFilter() }
    }
    var selectedDetailSpanId: String? {
        didSet { scheduleFilter() }
    }

    // MARK: - Filtering

    var searchText = "" {
        didSet { scheduleFilter() }
    }
    var statusFilter: SpanStatus? = nil {
        didSet { scheduleFilter() }
    }
    var showErrorsOnly = false {
        didSet { scheduleFilter() }
    }
    var sortBy: TraceSortBy = .startTime {
        didSet { scheduleFilter() }
    }
    var sortOrder: SortOrder = .descending {
        didSet { scheduleFilter() }
    }
    var minDurationMs = "" {
        didSet { scheduleFilter() }
    }
    var maxDurationMs = "" {
        didSet { scheduleFilter() }
    }
    var statusCodeRanges: Set<HTTPStatusCodeRange> = [] {
        didSet { scheduleFilter() }
    }

    // MARK: - Derived State (background-computed)

    /// All decoded spans — updated in background when new batches arrive.
    private(set) var allSpans: [Span] = []
    private(set) var filteredSpans: [Span] = []
    private(set) var rootSpans: [Span] = []
    private(set) var selectedSpan: Span?
    private(set) var selectedDetailSpan: Span?
    private(set) var traceSpans: [Span] = []

    private var decodeTask: Task<Void, Never>?
    private var filterTask: Task<Void, Never>?
    private var observeTask: Task<Void, Never>?

    // MARK: - Background Decode

    private nonisolated static func decodeAllSpans(batches: [RecordBatch], serviceName: String) -> [Span] {
        batches.flatMap { decodeSpanBatch($0, serviceName: serviceName) }
    }

    private func scheduleDecodeSpans() {
        let batches = tableView?.batches ?? []
        let serviceName = currentServiceName ?? "unknown"
        decodeTask?.cancel()
        decodeTask = Task.detached { [weak self] in
            guard !Task.isCancelled else { return }
            let spans = Self.decodeAllSpans(batches: batches, serviceName: serviceName)
            guard !Task.isCancelled else { return }
            await MainActor.run { [weak self] in
                self?.allSpans = spans
                self?.scheduleFilter()
            }
        }
    }

    private func startObserving() {
        observeTask?.cancel()
        observeTask = Task { @MainActor [weak self] in
            while !Task.isCancelled {
                await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                    withObservationTracking {
                        _ = self?.tableView?.batchVersion
                    } onChange: {
                        continuation.resume()
                    }
                }
                guard !Task.isCancelled else { return }
                self?.scheduleDecodeSpans()
            }
        }
    }

    // MARK: - Background Filter

    private struct FilterResult {
        let filteredSpans: [Span]
        let rootSpans: [Span]
        let selectedSpan: Span?
        let selectedDetailSpan: Span?
        let traceSpans: [Span]
    }

    private nonisolated static func filterAndDeriveSpans(
        allSpans: [Span],
        searchText: String,
        showErrorsOnly: Bool,
        statusFilter: SpanStatus?,
        minDurationMs: String,
        maxDurationMs: String,
        statusCodeRanges: Set<HTTPStatusCodeRange>,
        sortBy: TraceSortBy,
        sortOrder: SortOrder,
        selectedSpanId: String?,
        selectedDetailSpanId: String?
    ) -> FilterResult {
        var result = allSpans

        if !searchText.isEmpty {
            let search = searchText.lowercased()
            result = result.filter { span in
                span.serviceName.lowercased().contains(search) ||
                span.operationName.lowercased().contains(search) ||
                span.attributes.values.contains { value in
                    if case .string(let str) = value {
                        return str.lowercased().contains(search)
                    }
                    return false
                }
            }
        }

        if showErrorsOnly {
            result = result.filter { $0.status == .error }
        } else if let statusFilter {
            result = result.filter { $0.status == statusFilter }
        }

        if let minMs = Double(minDurationMs), minMs > 0 {
            let minNs = minMs * 1_000_000
            result = result.filter { Double($0.duration.nanoseconds) >= minNs }
        }

        if let maxMs = Double(maxDurationMs), maxMs > 0 {
            let maxNs = maxMs * 1_000_000
            result = result.filter { Double($0.duration.nanoseconds) <= maxNs }
        }

        if !statusCodeRanges.isEmpty {
            result = result.filter { span in
                if let attrValue = span.attributes["http.status_code"] {
                    let code: Int64?
                    switch attrValue {
                    case .int(let value): code = value
                    case .string(let str): code = Int64(str)
                    default: code = nil
                    }
                    if let code { return statusCodeRanges.contains { $0.contains(code) } }
                }
                return false
            }
        }

        result.sort { lhs, rhs in
            let ascending = sortOrder == .ascending
            switch sortBy {
            case .startTime:
                return ascending ? lhs.startTime.nanoseconds < rhs.startTime.nanoseconds :
                                   lhs.startTime.nanoseconds > rhs.startTime.nanoseconds
            case .duration:
                return ascending ? lhs.duration.nanoseconds < rhs.duration.nanoseconds :
                                   lhs.duration.nanoseconds > rhs.duration.nanoseconds
            case .service:
                return ascending ? lhs.serviceName < rhs.serviceName :
                                   lhs.serviceName > rhs.serviceName
            }
        }

        let filtered = result
        let rootSpans = Dictionary(grouping: filtered) { $0.traceId }
            .compactMap { _, spansInTrace in
                spansInTrace.min { $0.startTime.nanoseconds < $1.startTime.nanoseconds }
            }
            .sorted { $0.startTime.nanoseconds > $1.startTime.nanoseconds }

        let selectedSpan = selectedSpanId.flatMap { id in allSpans.first { $0.spanId == id } }
        let selectedDetailSpan = selectedDetailSpanId.flatMap { id in allSpans.first { $0.spanId == id } }
        let traceSpans: [Span]
        if let span = selectedSpan {
            traceSpans = allSpans.filter { $0.traceId == span.traceId }
                .sorted { $0.startTime.nanoseconds < $1.startTime.nanoseconds }
        } else {
            traceSpans = []
        }

        return FilterResult(
            filteredSpans: filtered,
            rootSpans: rootSpans,
            selectedSpan: selectedSpan,
            selectedDetailSpan: selectedDetailSpan,
            traceSpans: traceSpans
        )
    }

    private func scheduleFilter() {
        let allSpans = allSpans
        let searchText = searchText
        let showErrorsOnly = showErrorsOnly
        let statusFilter = statusFilter
        let minDurationMs = minDurationMs
        let maxDurationMs = maxDurationMs
        let statusCodeRanges = statusCodeRanges
        let sortBy = sortBy
        let sortOrder = sortOrder
        let selectedSpanId = selectedSpanId
        let selectedDetailSpanId = selectedDetailSpanId
        filterTask?.cancel()
        filterTask = Task.detached { [weak self] in
            guard !Task.isCancelled else { return }
            let r = Self.filterAndDeriveSpans(
                allSpans: allSpans,
                searchText: searchText,
                showErrorsOnly: showErrorsOnly,
                statusFilter: statusFilter,
                minDurationMs: minDurationMs,
                maxDurationMs: maxDurationMs,
                statusCodeRanges: statusCodeRanges,
                sortBy: sortBy,
                sortOrder: sortOrder,
                selectedSpanId: selectedSpanId,
                selectedDetailSpanId: selectedDetailSpanId
            )
            guard !Task.isCancelled else { return }
            await MainActor.run { [weak self] in
                self?.filteredSpans = r.filteredSpans
                self?.rootSpans = r.rootSpans
                self?.selectedSpan = r.selectedSpan
                self?.selectedDetailSpan = r.selectedDetailSpan
                self?.traceSpans = r.traceSpans
            }
        }
    }

    // MARK: - Public Methods

    /// Configure the view model with a data source
    func configure(dataSource: DataSource?) {
        cancel()
        self.dataSource = dataSource
        isLoading = false
        error = nil
    }

    func loadSpans(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange) async {
        guard let dataSource = dataSource else {
            print("TracesViewModel: ❌ No data source available")
            return
        }

        self.dataSource = dataSource
        self.timeRange = timeRange
        self.currentServiceName = selectedService?.name ?? "unknown"

        let duration = timeRange.bounds.end.timeIntervalSince(timeRange.bounds.start)
        let hours = Int(duration / 3600)
        let query: String
        if hours > 0 && hours <= 24 {
            query = "spans last \(hours)h"
        } else {
            let minutes = Int(duration / 60)
            query = "spans last \(max(minutes, 1))m"
        }

        var fullQuery = query
        if let filter = buildResourceIdFilter(selectedService) {
            fullQuery += filter
        }
        fullQuery += " | take 1000"

        print("TracesViewModel: 🔍 Executing SeQL: \(fullQuery)")

        cancel()
        isLoading = true
        error = nil

        let tv = ReactiveTableView()
        tableView = tv

        startObserving()

        do {
            try tv.startSnapshot(dataSource: dataSource, query: fullQuery)
            isLoading = false

            try? await Task.sleep(nanoseconds: 200_000_000)
            if selectedSpanId == nil, let firstSpan = rootSpans.first {
                selectedSpanId = firstSpan.spanId
            }

            print("TracesViewModel: 📊 Loaded \(allSpans.count) spans")
        } catch {
            print("TracesViewModel: ❌ Query failed: \(error)")
            isLoading = false
            self.error = "Query failed: \(error.localizedDescription)"
        }
    }

    func refresh(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange) {
        Task {
            await loadSpans(dataSource: dataSource, selectedService: selectedService, timeRange: timeRange)
        }
    }

    /// Start a live stream — the view API handles historical + streaming phases.
    func startLiveStream(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange) {
        guard let dataSource = dataSource else { return }

        let serviceName = selectedService?.name ?? "unknown"
        self.dataSource = dataSource
        self.currentServiceName = serviceName

        let duration = timeRange.bounds.end.timeIntervalSince(timeRange.bounds.start)
        let hours = Int(duration / 3600)
        let timeSpec: String
        if hours > 0 && hours <= 24 {
            timeSpec = "spans last \(hours)h"
        } else {
            let minutes = Int(duration / 60)
            timeSpec = "spans last \(max(minutes, 1))m"
        }

        var query = timeSpec
        if let filter = buildResourceIdFilter(selectedService) {
            query += filter
        }
        query += " | take 1000"

        cancel()

        let tv = ReactiveTableView()
        tableView = tv

        startObserving()

        do {
            try tv.start(dataSource: dataSource, query: query)
        } catch {
            self.error = "Live stream failed: \(error.localizedDescription)"
        }
    }

    func stopLiveStream() {
        tableView?.cancel()
        tableView = nil
    }

    private func buildResourceIdFilter(_ selectedService: Service?) -> String? {
        guard let service = selectedService, !service.resourceIds.isEmpty else { return nil }
        if service.resourceIds.count == 1 {
            return " | where resource_id = \(service.resourceIds[0])"
        }
        let idList = service.resourceIds.map { String($0) }.joined(separator: ", ")
        return " | where resource_id in [\(idList)]"
    }

    func cancel() {
        observeTask?.cancel()
        observeTask = nil
        decodeTask?.cancel()
        decodeTask = nil
        filterTask?.cancel()
        filterTask = nil
        tableView?.cancel()
        tableView = nil
        allSpans = []
        filteredSpans = []
        rootSpans = []
        selectedSpan = nil
        selectedDetailSpan = nil
        traceSpans = []
        isLoading = false
    }

    func selectFirstTrace() {
        guard selectedSpanId == nil, let firstSpan = rootSpans.first else { return }
        selectedSpanId = firstSpan.spanId
    }

    func selectSpan(_ span: Span) {
        selectedSpanId = span.spanId
        selectedDetailSpanId = nil
    }

    func exportSpans() {
        let panel = NSSavePanel()
        panel.title = "Export Spans"
        panel.nameFieldStringValue = "spans-export.json"
        panel.allowedContentTypes = [.json]
        panel.canCreateDirectories = true

        guard panel.runModal() == .OK, let url = panel.url else { return }

        let jsonArray: [[String: Any]] = filteredSpans.map { span in
            var dict: [String: Any] = [
                "traceId": span.traceId,
                "spanId": span.spanId,
                "serviceName": span.serviceName,
                "operationName": span.operationName,
                "startTimeNs": span.startTime.nanoseconds,
                "endTimeNs": span.endTime.nanoseconds,
                "durationNs": span.duration.nanoseconds,
                "status": span.status.rawValue,
                "spanKind": span.spanKind.rawValue
            ]
            if let parentId = span.parentSpanId {
                dict["parentSpanId"] = parentId
            }
            return dict
        }

        do {
            let data = try JSONSerialization.data(withJSONObject: jsonArray, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: url)
            NSLog("📋 Exported \(jsonArray.count) spans to \(url.path)")
        } catch {
            NSLog("📋 Failed to export spans: \(error)")
        }
    }
}

// MARK: - Supporting Types

enum TraceSortBy: CaseIterable {
    case startTime
    case duration
    case service

    var displayName: String {
        switch self {
        case .startTime: return "Start Time"
        case .duration: return "Duration"
        case .service: return "Service"
        }
    }
}

enum SortOrder {
    case ascending
    case descending
}
