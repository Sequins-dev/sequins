import Foundation
import SequinsFFI
@_exported import Arrow

// MARK: - Read-only plain SQL over the normalized query path

extension DataSource {
    /// Execute a **read-only** plain SQL query (SELECT only) and stream the framed
    /// result to `sink`, exactly like ``executeSeQL(_:sink:)`` but over the SQL path.
    ///
    /// SeQL cannot address the app-state tables (`conversations`, `messages`,
    /// `dashboards`); this reads them (and any other registered DataFusion table)
    /// via Flight SQL's standard plain-SQL path — in-process locally, or over the
    /// wire against a Pro daemon. Results arrive as a `.table`-shaped snapshot
    /// (schema → data → complete). DDL/DML are rejected server-side.
    ///
    /// - Parameters:
    ///   - sql: A SQL `SELECT` statement.
    ///   - sink: Receiver for schema, data batches, and completion events.
    /// - Returns: A `SeQLStream` that can be cancelled.
    /// - Throws: `SequinsError` if the stream handle cannot be created.
    @discardableResult
    public func executeSQL(_ sql: String, sink: any SeQLSink) throws -> SeQLStream {
        try executeFramedSQL(sql, sink: sink, invoke: sequins_sql_query)
    }

    /// Like ``executeSQL(_:sink:)`` but reads **only** the app-state tables
    /// (`conversations`, `messages`, `dashboards`) via a telemetry-free context, so a
    /// signal/cold-tier issue can never block or hang the read. Use this for chat
    /// history and dashboard reads.
    @discardableResult
    public func executeAppStateSQL(_ sql: String, sink: any SeQLSink) throws -> SeQLStream {
        try executeFramedSQL(sql, sink: sink, invoke: sequins_app_state_query)
    }

    /// Shared framed driver for the snapshot (schema → data → complete) callback vtable,
    /// starting the stream via `invoke`. Used by the plain-SQL path (`sequins_sql_query` /
    /// `sequins_app_state_query`) and by ``executeSeQL(_:sink:)`` (`sequins_seql_query`),
    /// which all share this exact vtable. `label` names the stream in the start error.
    func executeFramedSQL(
        _ sql: String,
        sink: any SeQLSink,
        invoke: @escaping (OpaquePointer?, UnsafePointer<CChar>?, CFrameSinkVTable, UnsafeMutableRawPointer?) -> OpaquePointer?,
        label: String = "SQL"
    ) throws -> SeQLStream {
        let context = SeQLContext(sink: sink)
        let ctxRaw = Unmanaged.passUnretained(context).toOpaque()

        var vtable = CFrameSinkVTable()

        vtable.on_schema = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let schema = decodeSchemaFrame(framePtr)
            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onSchema(schema)
        }

        vtable.on_data = { (framePtr, ctxPtr) in
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

        vtable.on_complete = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let stats = decodeStatsFrame(framePtr)
            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onComplete(stats)
        }

        vtable.on_warning = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let (code, message) = decodeWarningFrame(framePtr)
            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onWarning(code: code, message: message)
        }

        vtable.on_error = { (framePtr, ctxPtr) in
            guard let framePtr, let ctxPtr else { return }
            let (code, message) = decodeErrorFrame(framePtr)
            let ctx = Unmanaged<SeQLContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            ctx.sink?.onError(code: code, message: message)
        }

        let streamHandle = sql.withCString { sqlPtr in
            invoke(rawPointer, sqlPtr, vtable, ctxRaw)
        }
        guard let streamHandle else {
            throw SequinsError.ffiError("failed to start \(label) query stream")
        }
        return SeQLStream(streamHandle, context: context)
    }

    /// Run a read-only SQL query and collect all result rows once it completes.
    ///
    /// A convenience over ``executeSQL(_:sink:)`` for one-shot metadata reads (e.g.
    /// listing conversations). Delivers `(columns, rows)` on the main actor.
    public func fetchSQL(
        _ sql: String,
        completion: @escaping (Result<(columns: [String], rows: [[Any?]]), Error>) -> Void
    ) {
        let collector = SQLRowCollector(completion: completion)
        do {
            collector.stream = try executeSQL(sql, sink: collector)
        } catch {
            collector.startFailed(error)
        }
    }

    /// Like ``fetchSQL(_:completion:)`` but over the telemetry-free app-state path
    /// (``executeAppStateSQL(_:sink:)``). Use for one-shot conversation/message reads
    /// so they never hang on a signal/cold-tier issue.
    public func fetchAppStateSQL(
        _ sql: String,
        completion: @escaping (Result<(columns: [String], rows: [[Any?]]), Error>) -> Void
    ) {
        let collector = SQLRowCollector(completion: completion)
        do {
            collector.stream = try executeAppStateSQL(sql, sink: collector)
        } catch {
            collector.startFailed(error)
        }
    }
}

/// One-shot SQL result collector: accumulates rows across batches and reports them
/// (with the schema's column names) on completion or error.
///
/// The stream's ``SeQLContext`` holds its sink **weakly** (to avoid leaking streaming
/// sinks the caller already owns). A one-shot collector, though, is created as a local
/// with no other owner, so it would be deallocated the instant the `fetch…` call
/// returns — before the async query delivers any frames, leaving the callbacks to
/// no-op against a nil sink and the completion to never fire. So it retains **itself**
/// for the query's lifetime (`selfRetain`), releasing exactly once in ``finish(_:)``.
final class SQLRowCollector: SeQLSink {
    private let completion: (Result<(columns: [String], rows: [[Any?]]), Error>) -> Void
    private var columns: [String] = []
    private var rows: [[Any?]] = []
    private var finished = false
    var stream: SeQLStream?
    /// Strong self-reference keeping the collector alive across the async query.
    private var selfRetain: SQLRowCollector?

    init(completion: @escaping (Result<(columns: [String], rows: [[Any?]]), Error>) -> Void) {
        self.completion = completion
        self.selfRetain = self
    }

    /// Report a failure to even start the stream (no frames will arrive).
    func startFailed(_ error: Error) {
        finish(.failure(error))
    }

    func onSchema(_ schema: SeQLSchema) {
        columns = schema.columnNames
    }

    func onBatch(_ batch: RecordBatch, table: String?) {
        // Only the primary result table (auxiliary tables are unused for SQL reads).
        guard table == nil else { return }
        rows.append(contentsOf: batch.toRows())
    }

    func onComplete(_ stats: SeQLStats) {
        finish(.success((columns, rows)))
    }

    func onWarning(code: UInt32, message: String) {}

    func onError(code: UInt32, message: String) {
        finish(.failure(SequinsError.ffiError(message)))
    }

    private func finish(_ result: Result<(columns: [String], rows: [[Any?]]), Error>) {
        guard !finished else { return }
        finished = true
        let cb = completion
        DispatchQueue.main.async {
            cb(result)
            // Release the retained stream after delivery.
        }
        stream = nil
        // Drop the self-reference last, so `self` stays alive until delivery is queued.
        selfRetain = nil
    }
}
