import Foundation
import SequinsData

/// Runs the query behind a ``VisualizationView`` and exposes its schema, rows, and
/// per-column semantic types reactively. Supports a one-shot snapshot (`executeSeQL`)
/// and a continuously updating live stream (`executeLiveSeQL`).
@MainActor
@Observable
final class VisualizationRunner: SeQLSink {
    private(set) var schema: SeQLSchema?
    private(set) var rows: [[Any?]] = []
    /// Semantic type per column (temporal/duration/number/id/…), from the Arrow schema.
    private(set) var columnTypes: [NodeTypeLabel] = []
    private(set) var errorMessage: String?
    private(set) var isLoading = false

    private var snapshotStream: SeQLStream?
    private var liveStream: LiveSeQLStream?

    var columns: [String] { schema?.columnNames ?? [] }
    /// Per-column semantic roles (dimensions vs measures), when the schema supplies them.
    var columnRoles: [SeQLColumnRole] { schema?.columnRoles ?? [] }
    var shape: ResponseShape { schema?.shape ?? .table }

    /// (Re)start the query. Cancels any prior stream first.
    func start(dataSource: DataSource, query: String, isLive: Bool) {
        stop()
        errorMessage = nil
        rows = []
        columnTypes = []
        isLoading = true

        guard !query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            isLoading = false
            return
        }

        do {
            if isLive {
                let stream = try dataSource.executeLiveSeQL(query)
                stream.onSchemaCallback = { [weak self] s in
                    Task { @MainActor in self?.schema = s }
                }
                stream.onBatchCallback = { [weak self] _, _ in
                    Task { @MainActor in self?.rebuildFromLive() }
                }
                stream.onDeltaCallback = { [weak self] _ in
                    Task { @MainActor in self?.rebuildFromLive() }
                }
                liveStream = stream
                if let s = stream.schema { schema = s }
                rebuildFromLive()
                isLoading = false
            } else {
                snapshotStream = try dataSource.executeSeQL(query, sink: self)
            }
        } catch {
            errorMessage = error.localizedDescription
            isLoading = false
        }
    }

    func stop() {
        snapshotStream?.cancel()
        snapshotStream = nil
        liveStream?.cancel()
        liveStream = nil
    }

    private func rebuildFromLive() {
        guard let stream = liveStream else { return }
        if let s = stream.schema { schema = s }
        let batches = stream.batches
        rows = batches.flatMap { $0.toRows() }
        if let first = batches.first { columnTypes = first.columnTypeLabels() }
    }

    // MARK: - SeQLSink (snapshot; callbacks arrive off the main thread)

    nonisolated func onSchema(_ schema: SeQLSchema) {
        Task { @MainActor in self.schema = schema }
    }

    nonisolated func onBatch(_ batch: RecordBatch, table: String?) {
        guard table == nil else { return }
        let newRows = batch.toRows()
        let types = batch.columnTypeLabels()
        Task { @MainActor in
            self.rows.append(contentsOf: newRows)
            if self.columnTypes.isEmpty { self.columnTypes = types }
        }
    }

    nonisolated func onComplete(_ stats: SeQLStats) {
        Task { @MainActor in self.isLoading = false }
    }

    nonisolated func onWarning(code: UInt32, message: String) {}

    nonisolated func onError(code: UInt32, message: String) {
        Task { @MainActor in
            self.errorMessage = message
            self.isLoading = false
        }
    }
}
