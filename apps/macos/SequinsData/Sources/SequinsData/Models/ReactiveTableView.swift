//
//  ReactiveTableView.swift
//  SequinsData
//
//  @Observable table-level view driven by ViewDelta callbacks.
//  Handles RowsAppended, RowsExpired, TableReplaced, Ready, Heartbeat, Error.
//  Consumed by LogsViewModel and TracesViewModel.
//

import Foundation
import Logging
import Tracing

@Observable
public final class ReactiveTableView {
    private static let logger = Logger(label: "sequins.reactive-table-view")
    public private(set) var batches: [RecordBatch] = []
    public private(set) var isReady: Bool = false
    public private(set) var lastWatermarkNs: UInt64 = 0
    public private(set) var errorMessage: String?
    /// Incremented whenever `batches` changes. Observers can watch this to schedule reprocessing.
    public private(set) var batchVersion: Int = 0

    private var viewHandle: ViewHandle?
    private var seqlStream: SeQLStream?
    private var snapshotSink: SnapshotSink?

    public init() {}

    /// Start a live view stream. Replaces any existing stream.
    public func start(
        dataSource: DataSource,
        query: String,
        strategy: ViewStrategy = .table,
        retentionNs: UInt64 = 0
    ) throws {
        try withSpan("ReactiveTableView.start") { _ in
            cancel()
            viewHandle = try dataSource.executeView(query, strategy: strategy, retentionNs: retentionNs) {
                [weak self] deltas in self?.applyDeltas(deltas)
            }
        }
    }

    /// Start a one-shot snapshot query. Uses executeSeQL for snapshot mode (no live updates).
    ///
    /// Use this instead of `start` when you only need a historical snapshot and do not want
    /// the stream to continue receiving live updates after the initial data arrives.
    public func startSnapshot(dataSource: DataSource, query: String) throws {
        cancel()
        let sink = SnapshotSink(self)
        snapshotSink = sink
        seqlStream = try dataSource.executeSeQL(query, sink: sink)
    }

    /// Cancel the current stream.
    public func cancel() {
        viewHandle?.cancel()
        viewHandle = nil
        seqlStream?.cancel()
        seqlStream = nil
        snapshotSink = nil
    }

    // MARK: - Snapshot sink

    /// Internal SeQLSink that feeds one-shot snapshot batches into this ReactiveTableView.
    private final class SnapshotSink: SeQLSink {
        weak var owner: ReactiveTableView?

        init(_ owner: ReactiveTableView) { self.owner = owner }

        func onSchema(_ schema: SeQLSchema) {}

        func onBatch(_ batch: RecordBatch, table: String?) {
            guard table == nil else { return }
            DispatchQueue.main.async { [weak self] in
                self?.owner?.batches.append(batch)
                self?.owner?.batchVersion += 1
            }
        }

        func onComplete(_ stats: SeQLStats) {
            DispatchQueue.main.async { [weak self] in
                self?.owner?.isReady = true
            }
        }

        func onWarning(code: UInt32, message: String) {}

        func onError(code: UInt32, message: String) {
            DispatchQueue.main.async { [weak self] in
                self?.owner?.errorMessage = message
            }
        }
    }

    private func applyDeltas(_ deltas: [ViewDelta]) {
        withSpan("ReactiveTableView.applyDeltas") { span in
            span.attributes["delta.count"] = deltas.count
        }
        Self.logger.debug("applyDeltas", metadata: [
            "count": "\(deltas.count)",
            "types": "\(deltas.map { "\($0.type)" })"
        ])
        for d in deltas {
            switch d.type {
            case .rowsAppended:
                batches.append(contentsOf: d.data)
            case .rowsExpired:
                let expireCount = Int(d.count)
                if expireCount > 0 && expireCount <= batches.count {
                    batches.removeFirst(expireCount)
                }
            case .tableReplaced:
                batches = d.data
            case .ready:
                isReady = true
            case .heartbeat:
                lastWatermarkNs = d.watermarkNs
            case .error:
                errorMessage = d.message
            default:
                break
            }
        }
        batchVersion += 1
    }
}
