import AppKit
import Foundation
import SequinsData

@MainActor
@Observable
final class ExploreViewModel: SeQLSink {
    // MARK: - Input

    var queryText: String = "spans last 1h"

    // MARK: - Parse State

    private(set) var parseError: SeQLParseError?

    var isQueryValid: Bool {
        parseError == nil && !queryText.trimmingCharacters(in: .whitespaces).isEmpty
    }

    // MARK: - Result State

    private(set) var schema: SeQLSchema?
    private(set) var rows: [[Any?]] = []
    private(set) var batches: [RecordBatch] = []
    private(set) var recordTrees: [RecordNode] = []
    private(set) var isExecuting: Bool = false
    private(set) var executionError: String?
    private(set) var stats: SeQLStats?
    private(set) var warnings: [(code: UInt32, message: String)] = []

    // MARK: - Display Options

    /// When set, overrides the auto-detected visualization shape
    var visualizationOverride: ResponseShape? = nil

    /// Page size for table pagination
    var pageSize: Int = 500

    /// Current page index (0-based)
    var currentPage: Int = 0

    // MARK: - Active Stream

    private var activeStream: SeQLStream?

    // MARK: - Debounce

    private var validateTask: Task<Void, Never>?

    // MARK: - Parse Validation

    /// Normalize typographic quotes to ASCII so the parser handles copy-pasted queries
    private var normalizedQuery: String {
        queryText
            .replacingOccurrences(of: "\u{201C}", with: "\"")  // left double quote "
            .replacingOccurrences(of: "\u{201D}", with: "\"")  // right double quote "
            .replacingOccurrences(of: "\u{2018}", with: "'")   // left single quote '
            .replacingOccurrences(of: "\u{2019}", with: "'")   // right single quote '
    }

    func validateQuery(dataSource: DataSource?) {
        validateTask?.cancel()
        validateTask = Task {
            try? await Task.sleep(nanoseconds: 300_000_000)  // 300ms debounce
            guard !Task.isCancelled else { return }
            parseError = dataSource?.parseSeQL(normalizedQuery)
        }
    }

    // MARK: - Execution

    func executeQuery(dataSource: DataSource?) {
        guard let dataSource else { return }

        cancelQuery()

        schema = nil
        rows = []
        batches = []
        recordTrees = []
        stats = nil
        warnings = []
        executionError = nil
        isExecuting = true

        do {
            activeStream = try dataSource.executeSeQL(normalizedQuery, sink: self)
        } catch let parseErr as SeQLParseError {
            parseError = parseErr
            executionError = parseErr.message
            isExecuting = false
        } catch {
            executionError = error.localizedDescription
            isExecuting = false
        }
    }

    func cancelQuery() {
        activeStream?.cancel()
        activeStream = nil
        isExecuting = false
    }

    // MARK: - SeQLSink

    nonisolated func onSchema(_ schema: SeQLSchema) {
        Task { @MainActor in
            self.schema = schema
            self.currentPage = 0
        }
    }

    nonisolated func onBatch(_ batch: RecordBatch, table: String?) {
        guard table == nil else { return }
        let rows = batch.toRows()
        let trees = batch.toRecordTrees()
        Task { @MainActor in
            if self.rows.isEmpty {
                self.currentPage = 0
            }
            self.rows.append(contentsOf: rows)
            self.batches.append(batch)
            self.recordTrees.append(contentsOf: trees)
        }
    }

    nonisolated func onComplete(_ stats: SeQLStats) {
        Task { @MainActor in
            self.stats = stats
            self.isExecuting = false
        }
    }

    nonisolated func onWarning(code: UInt32, message: String) {
        Task { @MainActor in
            self.warnings.append((code: code, message: message))
        }
    }

    nonisolated func onError(code: UInt32, message: String) {
        Task { @MainActor in
            self.executionError = message
            self.isExecuting = false
        }
    }

    // MARK: - Export / Copy

    func copyQueryToClipboard() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(queryText, forType: .string)
    }

    func exportAsJSON() {
        guard let schema else { return }
        let columns = schema.columnNames
        let jsonArray = rows.map { row -> [String: Any] in
            var dict: [String: Any] = [:]
            for (idx, col) in columns.enumerated() {
                if let val = row[safe: idx] {
                    dict[col] = val
                } else {
                    dict[col] = NSNull()
                }
            }
            return dict
        }

        let panel = NSSavePanel()
        panel.title = "Export Results as JSON"
        panel.nameFieldStringValue = "explore-results.json"
        panel.allowedContentTypes = [.json]
        panel.canCreateDirectories = true
        guard panel.runModal() == .OK, let url = panel.url else { return }

        do {
            let data = try JSONSerialization.data(withJSONObject: jsonArray, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: url)
        } catch {
            NSLog("ExploreViewModel: JSON export failed: \(error)")
        }
    }

    func exportAsCSV() {
        guard let schema else { return }
        let columns = schema.columnNames

        var lines: [String] = []
        lines.append(columns.map { csvEscape($0) }.joined(separator: ","))
        for row in rows {
            let fields = columns.indices.map { idx -> String in
                let val: Any? = idx < row.count ? row[idx] : nil
                guard let val else { return "" }
                return csvEscape(cellString(val))
            }
            lines.append(fields.joined(separator: ","))
        }
        let csv = lines.joined(separator: "\n")

        let panel = NSSavePanel()
        panel.title = "Export Results as CSV"
        panel.nameFieldStringValue = "explore-results.csv"
        panel.allowedContentTypes = [.commaSeparatedText]
        panel.canCreateDirectories = true
        guard panel.runModal() == .OK, let url = panel.url else { return }

        do {
            try csv.write(to: url, atomically: true, encoding: .utf8)
        } catch {
            NSLog("ExploreViewModel: CSV export failed: \(error)")
        }
    }

    private func csvEscape(_ value: String) -> String {
        if value.contains(",") || value.contains("\"") || value.contains("\n") {
            return "\"" + value.replacingOccurrences(of: "\"", with: "\"\"") + "\""
        }
        return value
    }

    private func cellString(_ value: Any) -> String {
        switch value {
        case let s as String: return s
        case let n as NSNumber: return n.stringValue
        case let b as Bool: return b ? "true" : "false"
        default: return String(describing: value)
        }
    }
}

// MARK: - Array safe subscript (file-private)

private extension Array {
    subscript(safe index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}
