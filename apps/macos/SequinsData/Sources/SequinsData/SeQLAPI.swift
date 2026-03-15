import Foundation
import Logging
import SequinsFFI
import Tracing
@_exported import Arrow

private let logger = Logger(label: "sequins.seql-api")

// MARK: - Swift Types

/// Result shape from the query schema
public enum ResponseShape: Int {
    case table = 0
    case timeSeries = 1
    case heatmap = 2
    case traceTree = 3
    case traceTimeline = 4
    case patternGroups = 5
    case scalar = 6
}

/// Schema received before data rows
public struct SeQLSchema {
    public let shape: ResponseShape
    public let columnNames: [String]
    public let initialWatermarkNs: UInt64
}

/// Query execution statistics
public struct SeQLStats {
    public let executionTimeUs: UInt64
    public let rowsScanned: UInt64
    public let bytesRead: UInt64
    public let rowsReturned: UInt64
    public let warningCount: UInt32
}

/// Parse error with location info
public struct SeQLParseError: Error {
    public let message: String
    public let offset: Int
}

/// Callback protocol for receiving query results
public protocol SeQLSink: AnyObject {
    func onSchema(_ schema: SeQLSchema)
    /// Called with each batch of rows.
    /// - Parameters:
    ///   - batch: The Arrow RecordBatch.
    ///   - table: `nil` for the primary result table; a non-nil string for auxiliary tables
    ///     from merge stages (e.g. `"datapoints"`, `"stacks"`, `"frames"`).
    func onBatch(_ batch: RecordBatch, table: String?)
    func onComplete(_ stats: SeQLStats)
    func onWarning(code: UInt32, message: String)
    func onError(code: UInt32, message: String)
}

// MARK: - Internal Context Bridge

/// Context object holding a weak sink reference, passed as `void*` to C callbacks
final class SeQLContext {
    weak var sink: (any SeQLSink)?

    init(sink: any SeQLSink) {
        self.sink = sink
    }
}

// MARK: - SeQL Stream Handle

/// Opaque handle to a running query stream
public final class SeQLStream {
    private var handle: OpaquePointer?
    // Keep context alive for the duration of the stream
    private let context: SeQLContext

    init(_ handle: OpaquePointer?, context: SeQLContext) {
        self.handle = handle
        self.context = context
    }

    public func cancel() {
        if let h = handle {
            sequins_seql_cancel(h)
        }
    }

    deinit {
        if let h = handle {
            sequins_seql_stream_free(h)
        }
    }
}

// MARK: - Live SeQL Stream

/// Delta operation type for live query updates
public enum SeQLDeltaOpType: Int {
    case append = 0
    case update = 1
    case expire = 2
    case replace = 3
}

/// A single delta operation from a live query stream
public struct SeQLDeltaOp {
    public let type: SeQLDeltaOpType
    /// For Append: the starting row_id for the first row in the batch.
    /// For Update/Expire: the target row_id.
    /// For Replace: unused (0).
    public let rowId: UInt64
    /// For Append/Replace: Arrow RecordBatch (decoded from IPC bytes).
    /// For Update: array of [col_idx, value] pairs.
    /// For Expire: nil.
    public let data: Any?
}

/// Context bridge for LiveSeQLStream callbacks
final class LiveSeQLContext {
    weak var stream: LiveSeQLStream?
    init(stream: LiveSeQLStream) {
        self.stream = stream
    }
}

/// An `@Observable` live query stream that updates reactively as new data arrives.
///
/// Created by `DataSource.executeLiveSeQL()`. `deinit` cancels the Rust stream.
@Observable
public final class LiveSeQLStream {
    /// Query schema (set once when the stream starts, may be nil for live-only streams)
    public private(set) var schema: SeQLSchema?
    /// Accumulated record batches — appended by Append delta ops, replaced by Replace ops
    public private(set) var batches: [RecordBatch] = []
    /// Query completion stats (nil for live queries that haven't ended)
    public private(set) var stats: SeQLStats?
    /// Last error, if any
    public private(set) var errorMessage: String?
    /// Whether the stream has ended (either completed or errored)
    public private(set) var isComplete: Bool = false
    /// Watermark from the last heartbeat
    public private(set) var lastHeartbeatNs: UInt64 = 0

    /// Optional callback invoked after each historical Data batch is applied.
    /// Set by consumers (e.g. MetricsViewModel) that want to process Arrow data directly
    /// instead of — or in addition to — reading from `batches`.
    /// The `String?` parameter is the table name: `nil` = primary, non-nil = auxiliary alias.
    public var onBatchCallback: ((RecordBatch, String?) -> Void)?

    /// Optional callback invoked after each delta frame is applied.
    /// Receives all ops in the frame; Update ops carry a single-row RecordBatch.
    public var onDeltaCallback: (([SeQLDeltaOp]) -> Void)?

    private var handle: OpaquePointer?
    // Keep context alive for the duration of the stream (strong reference)
    var _contextRetainer: AnyObject?

    init() {}

    func setHandle(_ h: OpaquePointer) {
        handle = h
    }

    // Called from main thread only
    func applySchema(_ schema: SeQLSchema) {
        self.schema = schema
    }

    // Called from main thread only — snapshot-style data batch (for live queries that
    // emit a Schema+Data before delta mode, or for testing).
    func applyBatch(_ batch: RecordBatch, table: String?) {
        if table == nil {
            batches.append(batch)
        }
        onBatchCallback?(batch, table)
    }

    // Called from main thread only
    func applyDelta(_ ops: [SeQLDeltaOp]) {
        for op in ops {
            switch op.type {
            case .append:
                if let batch = op.data as? RecordBatch {
                    batches.append(batch)
                }
            case .replace:
                batches.removeAll()
                if let batch = op.data as? RecordBatch {
                    batches = [batch]
                }
            case .update:
                // Update ops carry a single-row RecordBatch with the changed column(s).
                // Delivered to onDeltaCallback; no change to `batches` (caller owns state).
                break
            case .expire:
                break
            }
        }
        onDeltaCallback?(ops)
    }

    func applyHeartbeat(_ watermarkNs: UInt64) {
        lastHeartbeatNs = watermarkNs
    }

    func applyComplete(_ stats: SeQLStats) {
        self.stats = stats
        self.isComplete = true
    }

    func applyError(code: UInt32, message: String) {
        _ = code
        self.errorMessage = message
        self.isComplete = true
    }

    public func cancel() {
        if let h = handle {
            sequins_seql_cancel(h)
        }
    }

    deinit {
        if let h = handle {
            sequins_seql_stream_free(h)
        }
    }
}

// MARK: - ViewDelta Types

/// Discriminant for view delta callbacks from `sequins_view_create`.
public enum ViewDeltaType: UInt32 {
    case rowsAppended = 0
    case rowsExpired = 1
    case tableReplaced = 2
    case entityCreated = 3
    case entityDataReplaced = 4
    case entityRemoved = 5
    case ready = 6
    case heartbeat = 7
    case warning = 8
    case error = 9
}

/// Strategy selector for `DataSource.executeView`.
public enum ViewStrategy: UInt32 {
    case table = 0
    case aggregate = 1
    case flamegraph = 3
}

/// A single reactive update from a view stream.
public struct ViewDelta {
    public let type: ViewDeltaType
    /// Table identifier — nil = primary table, non-nil = auxiliary alias (e.g. "datapoints").
    public let table: String?
    /// Entity key for entity-level deltas (flamegraph path_key, metric_id, etc.).
    public let key: String?
    /// Decoded Arrow IPC payload (RowsAppended, TableReplaced, EntityCreated data, EntityDataReplaced).
    public let data: [RecordBatch]
    /// Decoded Arrow IPC descriptor (EntityCreated only).
    public let descriptor: [RecordBatch]
    /// Number of rows expired (RowsExpired only).
    public let count: UInt64
    /// Watermark timestamp in nanoseconds (Heartbeat only).
    public let watermarkNs: UInt64
    /// Warning/error code.
    public let code: UInt32
    /// Human-readable message (Warning / Error only).
    public let message: String?
}

// MARK: - ViewHandle

/// Context bridge for view callbacks; kept alive for the duration of the handle.
final class ViewContext {
    let onDeltas: ([ViewDelta]) -> Void
    init(onDeltas: @escaping ([ViewDelta]) -> Void) {
        self.onDeltas = onDeltas
    }
}

/// Opaque handle to a running reactive view. Cancel on deinit.
public final class ViewHandle {
    private var handle: OpaquePointer?
    // Keeps context alive until the handle is freed.
    private var _contextRetainer: AnyObject?

    init(handle: OpaquePointer, contextRetainer: AnyObject) {
        self.handle = handle
        self._contextRetainer = contextRetainer
    }

    public func cancel() {
        guard let h = handle else { return }
        sequins_view_cancel(h)
    }

    deinit {
        if let h = handle {
            sequins_view_free(h)
            handle = nil
        }
    }
}

// MARK: - IPC Decode Helper

func decodeIPC(data dataPtr: UnsafeMutablePointer<UInt8>?, length: Int) -> [RecordBatch] {
    return withSpan("decodeIPC") { _ in
        guard let ptr = dataPtr, length > 0 else { return [] }
        let ipcData = Data(bytes: ptr, count: length)
        let reader = ArrowReader()
        switch reader.readStreaming(ipcData) {
        case .success(let result):
            return result.batches
        case .failure(let error):
            logger.warning("Arrow IPC decode failed", metadata: ["error": "\(error)"])
            return []
        }
    }
}

// MARK: - DataSource Extension

extension DataSource {

    /// Parse a SeQL query string without executing it.
    ///
    /// - Returns: `nil` on success, or a `SeQLParseError` describing the problem.
    public func parseSeQL(_ query: String) -> SeQLParseError? {
        // Convert Swift String to C string explicitly
        let result = query.withCString { queryPtr in
            sequins_seql_parse(queryPtr)
        }
        guard let result else {
            return SeQLParseError(message: "null parse result", offset: 0)
        }
        defer { sequins_seql_parse_result_free(result) }

        if sequins_seql_parse_result_is_ok(result) != 0 {
            return nil  // success
        }

        let msg: String
        if let cStr = sequins_seql_parse_error_message(result) {
            msg = String(cString: cStr)
        } else {
            msg = "parse error"
        }
        let offset = Int(sequins_seql_parse_error_offset(result))
        return SeQLParseError(message: msg, offset: offset)
    }

    /// Execute a SeQL query, streaming results to the sink.
    ///
    /// - Parameters:
    ///   - query: SeQL query text
    ///   - sink: Receiver for schema, data batches, and completion events
    /// - Returns: A `SeQLStream` that can be cancelled
    /// - Throws: `SequinsError` on failures (parse errors are reported via sink.onError)
    @discardableResult
    public func executeSeQL(_ query: String, sink: any SeQLSink) throws -> SeQLStream {
        return try withSpan("DataSource.executeSeQL") { _ in
        let context = SeQLContext(sink: sink)
        let ctxRaw = Unmanaged.passUnretained(context).toOpaque()

        var vtable = CFrameSinkVTable()

        // Schema callback — decode synchronously, free C memory, then call sink
        vtable.on_schema = {
            (framePtr: UnsafePointer<CSchemaFrame>?, ctxPtr: UnsafeMutableRawPointer?) -> Void in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee

            var names: [String] = []
            for i in 0..<Int(frame.column_count) {
                if let cstr = frame.column_names?[i] {
                    names.append(String(cString: cstr))
                }
            }
            let shapeVal = Int(frame.shape.rawValue)
            let shape = ResponseShape(rawValue: shapeVal) ?? .table
            let schema = SeQLSchema(
                shape: shape,
                columnNames: names,
                initialWatermarkNs: frame.initial_watermark_ns
            )
            c_schema_frame_free(UnsafeMutablePointer(mutating: framePtr))

            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onSchema(schema)
        }

        // Data callback — decode Arrow IPC bytes, deliver as RecordBatch to sink
        vtable.on_data = {
            (framePtr: UnsafePointer<CDataFrame>?, ctxPtr: UnsafeMutableRawPointer?) -> Void in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee
            let table: String? = frame.table.map { String(cString: $0) }
            let batches = decodeIPC(data: frame.ipc_data, length: Int(frame.ipc_len))
            c_data_frame_free(UnsafeMutablePointer(mutating: framePtr))

            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            for batch in batches {
                ctx.sink?.onBatch(batch, table: table)
            }
        }

        // Complete callback — stack-allocated frame, no free needed
        vtable.on_complete = {
            (framePtr: UnsafePointer<CCompleteFrame>?, ctxPtr: UnsafeMutableRawPointer?) -> Void in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee

            let stats = SeQLStats(
                executionTimeUs: frame.execution_time_us,
                rowsScanned: frame.rows_scanned,
                bytesRead: frame.bytes_read,
                rowsReturned: frame.rows_returned,
                warningCount: frame.warning_count
            )

            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onComplete(stats)
        }

        // Warning callback — heap-allocated, must free
        vtable.on_warning = {
            (framePtr: UnsafePointer<CWarningFrame>?, ctxPtr: UnsafeMutableRawPointer?) -> Void in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee

            let code = UInt32(frame.code)
            let message: String
            if let cStr = frame.message {
                message = String(cString: cStr)
            } else {
                message = "warning"
            }
            c_warning_frame_free(UnsafeMutablePointer(mutating: framePtr))

            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onWarning(code: code, message: message)
        }

        // Error callback — heap-allocated, must free
        vtable.on_error = {
            (framePtr: UnsafePointer<CQueryError>?, ctxPtr: UnsafeMutableRawPointer?) -> Void in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee

            let code = UInt32(frame.code)
            let message: String
            if let cStr = frame.message {
                message = String(cString: cStr)
            } else {
                message = "query error"
            }
            c_query_error_free(UnsafeMutablePointer(mutating: framePtr))

            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onError(code: code, message: message)
        }

        // Convert Swift String to C string explicitly
        let streamHandle = query.withCString { queryPtr in
            sequins_seql_query(rawPointer, queryPtr, vtable, ctxRaw)
        }

        guard let streamHandle else {
            throw SequinsError.ffiError("failed to start SeQL query stream")
        }

        return SeQLStream(streamHandle, context: context)
        } // end withSpan("DataSource.executeSeQL")
    }

    /// Execute a SeQL query in live streaming mode, returning an observable stream.
    ///
    /// The stream emits Delta frames continuously as new data arrives from the WAL.
    /// Heartbeat frames keep the connection alive. The stream never emits a Complete
    /// frame until cancelled.
    ///
    /// - Parameter query: SeQL query text (time range is used as initial window only)
    /// - Returns: A `LiveSeQLStream` with `@Observable` properties (`batches`, `schema`, etc.)
    /// - Throws: `SequinsError` if the stream handle cannot be created
    public func executeLiveSeQL(_ query: String) throws -> LiveSeQLStream {
        return try withSpan("DataSource.executeLiveSeQL") { _ in
        let stream = LiveSeQLStream()
        let context = LiveSeQLContext(stream: stream)
        let ctxRaw = Unmanaged.passUnretained(context).toOpaque()

        var vtable = CFrameSinkVTable()

        // Schema callback
        vtable.on_schema = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee

            var names: [String] = []
            for i in 0..<Int(frame.column_count) {
                if let cstr = frame.column_names?[i] {
                    names.append(String(cString: cstr))
                }
            }
            let shapeVal = Int(frame.shape.rawValue)
            let shape = ResponseShape(rawValue: shapeVal) ?? .table
            let schema = SeQLSchema(
                shape: shape,
                columnNames: names,
                initialWatermarkNs: frame.initial_watermark_ns
            )
            c_schema_frame_free(UnsafeMutablePointer(mutating: framePtr))

            let ctx = Unmanaged<LiveSeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.applySchema(schema) }
        }

        // Data callback — decode Arrow IPC bytes, deliver batch to stream
        vtable.on_data = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee
            let table: String? = frame.table.map { String(cString: $0) }
            let batches = decodeIPC(data: frame.ipc_data, length: Int(frame.ipc_len))
            c_data_frame_free(UnsafeMutablePointer(mutating: framePtr))

            let ctx = Unmanaged<LiveSeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            for batch in batches {
                let b = batch
                let t = table
                DispatchQueue.main.async { ctx.stream?.applyBatch(b, table: t) }
            }
        }

        // Delta callback — incremental updates
        vtable.on_delta = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee

            var ops: [SeQLDeltaOp] = []
            for i in 0..<Int(frame.ops_count) {
                let op = frame.ops.advanced(by: i).pointee
                let rowId = op.row_id
                let opType = SeQLDeltaOpType(rawValue: Int(op.op_type.rawValue)) ?? .append

                let opData: Any?
                switch opType {
                case .append, .replace:
                    let ipcBatches = decodeIPC(data: op.data, length: Int(op.data_len))
                    opData = ipcBatches.first
                case .update:
                    // Update ops now carry a single-row Arrow IPC RecordBatch containing
                    // only the changed column(s) — same encoding as Append/Replace.
                    let ipcBatches = decodeIPC(data: op.data, length: Int(op.data_len))
                    opData = ipcBatches.first
                case .expire:
                    opData = nil
                }
                ops.append(SeQLDeltaOp(type: opType, rowId: rowId, data: opData))
            }
            c_delta_frame_free(UnsafeMutablePointer(mutating: framePtr))

            let ctx = Unmanaged<LiveSeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.applyDelta(ops) }
        }

        // Heartbeat callback
        vtable.on_heartbeat = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let watermarkNs = framePtr.pointee.watermark_ns
            let ctx = Unmanaged<LiveSeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.applyHeartbeat(watermarkNs) }
        }

        // Complete callback
        vtable.on_complete = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee
            let stats = SeQLStats(
                executionTimeUs: frame.execution_time_us,
                rowsScanned: frame.rows_scanned,
                bytesRead: frame.bytes_read,
                rowsReturned: frame.rows_returned,
                warningCount: frame.warning_count
            )
            let ctx = Unmanaged<LiveSeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.applyComplete(stats) }
        }

        // Warning callback
        vtable.on_warning = { (framePtr, _) in
            guard let framePtr else { return }
            let frame = framePtr.pointee
            let message: String
            if let cStr = frame.message {
                message = String(cString: cStr)
            } else {
                message = "warning"
            }
            c_warning_frame_free(UnsafeMutablePointer(mutating: framePtr))
            logger.warning("LiveSeQL warning", metadata: ["message": "\(message)"])
        }

        // Error callback
        vtable.on_error = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let frame = framePtr.pointee
            let code = UInt32(frame.code)
            let message: String
            if let cStr = frame.message {
                message = String(cString: cStr)
            } else {
                message = "query error"
            }
            c_query_error_free(UnsafeMutablePointer(mutating: framePtr))
            let ctx = Unmanaged<LiveSeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.applyError(code: code, message: message) }
        }

        let handle = query.withCString { queryPtr in
            sequins_seql_query_live(rawPointer, queryPtr, vtable, ctxRaw)
        }

        guard let handle else {
            throw SequinsError.ffiError("failed to start live SeQL query stream")
        }

        stream.setHandle(handle)
        // Keep the context alive for the stream's lifetime
        stream._contextRetainer = context

        return stream
        } // end withSpan("DataSource.executeLiveSeQL")
    }

    /// Create a reactive view that transforms a live SeQL query into `ViewDelta` callbacks.
    ///
    /// - Parameters:
    ///   - query: SeQL query text
    ///   - strategy: View strategy (table, aggregate, or flamegraph)
    ///   - retentionNs: Retention window in nanoseconds (0 = default 1h, flamegraph only)
    ///   - onDeltas: Callback invoked on the main thread with each batch of deltas
    /// - Returns: A `ViewHandle` that cancels the view on deinit
    /// - Throws: `SequinsError` if the handle cannot be created
    @discardableResult
    public func executeView(
        _ query: String,
        strategy: ViewStrategy,
        retentionNs: UInt64 = 0,
        onDeltas: @escaping ([ViewDelta]) -> Void
    ) throws -> ViewHandle {
        return try withSpan("DataSource.executeView") { span in
        span.attributes["strategy"] = "\(strategy)"
        let context = ViewContext(onDeltas: onDeltas)
        let ctxRaw = Unmanaged.passRetained(context).toOpaque()

        // C callback: convert CViewDelta array → [ViewDelta], free C memory, dispatch to main.
        let callback: @convention(c) (
            UnsafeMutablePointer<CViewDelta>?, UInt32, UnsafeMutableRawPointer?
        ) -> Void = { deltasPtr, count, ctxPtr in
            guard let deltasPtr, count > 0, let ctxPtr else { return }

            // Build Swift array from C array before freeing
            var swiftDeltas: [ViewDelta] = []
            swiftDeltas.reserveCapacity(Int(count))

            for i in 0..<Int(count) {
                let cd = deltasPtr.advanced(by: i).pointee
                let deltaType = ViewDeltaType(rawValue: cd.delta_type) ?? .error
                let table: String? = cd.table.map { String(cString: $0) }
                let key: String? = cd.key.map { String(cString: $0) }
                let message: String? = cd.message.map { String(cString: $0) }
                let data = decodeIPC(data: cd.data, length: Int(cd.data_len))
                let descriptor = decodeIPC(data: cd.descriptor, length: Int(cd.descriptor_len))

                swiftDeltas.append(ViewDelta(
                    type: deltaType,
                    table: table,
                    key: key,
                    data: data,
                    descriptor: descriptor,
                    count: cd.count,
                    watermarkNs: cd.watermark_ns,
                    code: cd.code,
                    message: message
                ))
            }

            // Free the C batch now that we've copied all data
            c_view_delta_batch_free(deltasPtr, count)

            // Retain context, dispatch to main, then release
            let ctx = Unmanaged<ViewContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async {
                ctx.onDeltas(swiftDeltas)
            }
        }

        let handle = query.withCString { queryPtr in
            sequins_view_create(rawPointer, queryPtr, strategy.rawValue, retentionNs, callback, ctxRaw)
        }

        guard let handle else {
            // Release the over-retained context since we won't hand it to a ViewHandle
            Unmanaged<ViewContext>.fromOpaque(ctxRaw).release()
            throw SequinsError.ffiError("failed to create view handle")
        }

        // ViewHandle is responsible for releasing ctxRaw via _contextRetainer
        let retainer = Unmanaged<ViewContext>.fromOpaque(ctxRaw).takeRetainedValue()
        return ViewHandle(handle: handle, contextRetainer: retainer)
        } // end withSpan("DataSource.executeView")
    }
}
