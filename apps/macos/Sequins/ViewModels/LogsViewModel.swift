//
//  LogsViewModel.swift
//  Sequins
//
//  View model for logs tab using SeQL
//

import Foundation
import SwiftUI
import SequinsData
import Combine

// MARK: - Codable DTO for Arrow log schema

/// Arrow-schema Codable model matching the log RecordBatch schema.
/// Field names match Arrow column names exactly.
private struct LogRecord: Codable {
    // Core log columns (0–10)
    let log_id: String
    let time_unix_nano: Date          // TimestampArray.asAny() → Date
    let service_name: String
    let severity_number: UInt8
    let body: String
    let trace_id: String?
    let span_id: String?

    func toLogEntry(attributes: [String: AttributeValue] = [:]) -> SequinsData.LogEntry {
        let severity = SequinsData.LogSeverity(rawValue: UInt32(severity_number)) ?? .info
        return SequinsData.LogEntry(
            id: log_id,
            timestamp: time_unix_nano,
            observedTimestamp: time_unix_nano,
            serviceName: service_name,
            severity: severity,
            body: body,
            attributes: attributes,
            traceId: trace_id,
            spanId: span_id
        )
    }
}

// MARK: - Promoted attribute extraction

/// Core log column names captured by LogRecord or intentionally excluded.
/// Any column not in this set is a promoted SEMCONV attribute column.
private let coreLogColumns: Set<String> = [
    "log_id", "time_unix_nano", "observed_time_unix_nano",
    "service_name", "severity_text", "severity_number",
    "body", "trace_id", "span_id", "resource_id", "scope_id",
    "_overflow_attrs"
]

/// Maps Arrow column names (underscore form) back to OTLP dot-notation attribute keys.
/// Derived from SEMCONV_ATTRIBUTES in sequins-types/src/schema_catalog.rs.
private let promotedColumnNameToKey: [String: String] = [
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
    "log_iostream": "log.iostream",
    "log_file_name": "log.file.name",
]

/// Build one attribute dictionary per row combining promoted (non-core) columns
/// and overflow attributes from `_overflow_attrs`.
private func extractAllAttributes(from batch: RecordBatch) -> [[String: AttributeValue]] {
    let numRows = Int(batch.length)
    guard numRows > 0 else { return [] }

    var promotedCols: [(colIdx: Int, attrKey: String)] = []
    for (idx, field) in batch.schema.fields.enumerated() {
        if !coreLogColumns.contains(field.name) {
            let key = promotedColumnNameToKey[field.name] ?? field.name
            promotedCols.append((idx, key))
        }
    }

    var result = [[String: AttributeValue]](repeating: [:], count: numRows)
    for (colIdx, attrKey) in promotedCols {
        guard colIdx < batch.columns.count else { continue }
        let col = batch.columns[colIdx].array
        for row in 0..<numRows {
            guard let raw = col.asAny(UInt(row)) else { continue }
            if let val = attributeValueFromAny(raw) {
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

private func attributeValueFromAny(_ raw: Any) -> AttributeValue? {
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

@MainActor
@Observable
final class LogsViewModel {
    // MARK: - Observable State

    /// Loading state
    private(set) var isLoading: Bool = false

    /// Error message if query failed
    private(set) var error: String?

    /// Search text for filtering
    var searchText: String = ""

    /// Sort direction: true = newest first (default), false = oldest first
    var sortNewestFirst: Bool = true {
        didSet { scheduleReprocess() }
    }

    // MARK: - Private State

    /// Reactive table view backing this view model.
    private var tableView: ReactiveTableView?

    /// Data source reference
    private weak var dataSource: DataSource?

    // MARK: - Derived State

    /// The active log entries decoded from all accumulated batches, sorted by `sortNewestFirst`.
    /// Updated in the background whenever `tableView.batchVersion` increments or sort direction changes.
    private(set) var effectiveLogs: [SequinsData.LogEntry] = []

    private var reprocessTask: Task<Void, Never>?
    private var observeTask: Task<Void, Never>?

    /// Decode all batches into sorted log entries on a background thread.
    private nonisolated static func decodeLogs(
        batches: [RecordBatch],
        newestFirst: Bool
    ) -> [SequinsData.LogEntry] {
        let unsorted = batches.flatMap { batch -> [SequinsData.LogEntry] in
            let decoder = ArrowDecoder(batch)
            let records = (try? decoder.decode(LogRecord.self)) ?? []
            let attrs = extractAllAttributes(from: batch)
            return records.enumerated().map { idx, record in
                record.toLogEntry(attributes: idx < attrs.count ? attrs[idx] : [:])
            }
        }
        return newestFirst
            ? unsorted.sorted { $0.timestamp > $1.timestamp }
            : unsorted.sorted { $0.timestamp < $1.timestamp }
    }

    private func scheduleReprocess() {
        let batches = tableView?.batches ?? []
        let newestFirst = sortNewestFirst
        reprocessTask?.cancel()
        reprocessTask = Task.detached { [weak self] in
            guard !Task.isCancelled else { return }
            let result = Self.decodeLogs(batches: batches, newestFirst: newestFirst)
            guard !Task.isCancelled else { return }
            await MainActor.run { [weak self] in
                self?.effectiveLogs = result
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
                self?.scheduleReprocess()
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

    /// Load logs with current filters using the reactive view API.
    func loadLogs(
        dataSource: DataSource?,
        selectedService: Service?,
        timeRange: TimeRange,
        searchText: String,
        severities: [SequinsData.LogSeverity]?
    ) {
        NSLog("📋 LogsViewModel.loadLogs called - service: \(selectedService?.name ?? "nil")")

        guard let dataSource = dataSource else {
            NSLog("📋 Early return - no dataSource")
            return
        }

        self.dataSource = dataSource

        let hours = Int(timeRange.bounds.end.timeIntervalSince(timeRange.bounds.start) / 3600)
        var query: String
        if hours > 0 && hours <= 24 {
            query = "logs last \(hours)h"
        } else {
            let startNs = Int64(timeRange.bounds.start.timeIntervalSince1970 * 1_000_000_000)
            let endNs = Int64(timeRange.bounds.end.timeIntervalSince1970 * 1_000_000_000)
            query = "logs between(\(startNs), \(endNs))"
        }

        if let filter = buildResourceIdFilter(selectedService) {
            query += filter
        }

        if let severities = severities, !severities.isEmpty {
            let severityNumbers = severities.map { "\($0.rawValue)" }.joined(separator: ", ")
            query += " | where severity_number in [\(severityNumbers)]"
        }

        if !searchText.isEmpty {
            let escapedSearch = searchText.replacingOccurrences(of: "'", with: "''")
            query += " | where body contains '\(escapedSearch)'"
        }

        query += " | take 500"

        NSLog("📋 Executing SeQL query: \(query)")

        cancel()
        isLoading = true
        error = nil

        let tv = ReactiveTableView()
        tableView = tv

        do {
            try tv.startSnapshot(dataSource: dataSource, query: query)
            isLoading = false
        } catch {
            NSLog("📋 SeQL execution error: \(error)")
            isLoading = false
            self.error = "Query failed: \(error.localizedDescription)"
        }

        startObserving()
    }

    /// Start live mode — the view API handles historical + streaming phases.
    func startLiveStream(
        dataSource: DataSource?,
        selectedService: Service?,
        searchText: String,
        severities: [SequinsData.LogSeverity]?
    ) {
        guard let dataSource = dataSource else { return }

        cancel()

        var query = "logs last 1h"
        if let filter = buildResourceIdFilter(selectedService) {
            query += filter
        }
        if let severities = severities, !severities.isEmpty {
            let nums = severities.map { "\($0.rawValue)" }.joined(separator: ", ")
            query += " | where severity_number in [\(nums)]"
        }
        if !searchText.isEmpty {
            let escaped = searchText.replacingOccurrences(of: "'", with: "''")
            query += " | where body contains '\(escaped)'"
        }
        query += " | take 500"

        NSLog("📋 [LogsViewModel] Starting live stream: \(query)")

        let tv = ReactiveTableView()
        tableView = tv

        do {
            try tv.start(dataSource: dataSource, query: query)
        } catch {
            NSLog("📋 [LogsViewModel] Live stream failed to start: \(error)")
            self.error = "Query failed: \(error.localizedDescription)"
        }

        startObserving()
    }

    func stopLiveStream() {
        tableView?.cancel()
        tableView = nil
    }

    func cancel() {
        observeTask?.cancel()
        observeTask = nil
        reprocessTask?.cancel()
        reprocessTask = nil
        tableView?.cancel()
        tableView = nil
        effectiveLogs = []
        isLoading = false
    }

    func exportLogs() {
        let logs: [SequinsData.LogEntry] = effectiveLogs
        LogEntry.exportToJSON(logs.map { LogEntry(from: $0) })
    }

    // MARK: - Private Helpers

    private func buildResourceIdFilter(_ selectedService: Service?) -> String? {
        guard let service = selectedService, !service.resourceIds.isEmpty else { return nil }
        if service.resourceIds.count == 1 {
            return " | where resource_id = \(service.resourceIds[0])"
        }
        let idList = service.resourceIds.map { String($0) }.joined(separator: ", ")
        return " | where resource_id in [\(idList)]"
    }
}
