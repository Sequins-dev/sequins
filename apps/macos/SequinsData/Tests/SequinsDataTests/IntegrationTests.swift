import XCTest
import Arrow
@testable import SequinsData

// MARK: - Query collector sink

/// Collects all RecordBatches from a SeQL snapshot query and fulfills an expectation
/// when the query completes or errors. Keep an instance alive for the query's lifetime.
private final class QueryCollector: SeQLSink {
    private let onBatchReceived: (RecordBatch) -> Void
    private let onFinished: () -> Void
    private let onFailed: (String) -> Void

    init(
        onBatch: @escaping (RecordBatch) -> Void,
        onDone: @escaping () -> Void,
        onError: @escaping (String) -> Void
    ) {
        self.onBatchReceived = onBatch
        self.onFinished = onDone
        self.onFailed = onError
    }

    nonisolated func onSchema(_ schema: SeQLSchema) {}

    nonisolated func onBatch(_ batch: RecordBatch, table: String?) {
        onBatchReceived(batch)
    }

    nonisolated func onComplete(_ stats: SeQLStats) {
        onFinished()
    }

    nonisolated func onWarning(code: UInt32, message: String) {}

    nonisolated func onError(code: UInt32, message: String) {
        onFailed("[\(code)] \(message)")
    }
}

// MARK: - Integration test suite

final class SeQLIntegrationTests: XCTestCase {
    var dataSource: DataSource!
    var tempDir: URL!

    override func setUp() async throws {
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("SequinsDataTests-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)

        // Port 0 = OS-assigned ephemeral port, avoids test conflicts and we don't need OTLP
        let config = OTLPServerConfig(grpcPort: 0, httpPort: 0)
        dataSource = try DataSource.local(
            dbPath: tempDir.appendingPathComponent("test.db").path,
            config: config
        )
        let spanCount = try dataSource.generateTestData()
        XCTAssertGreaterThan(spanCount, 0, "generateTestData should create at least one span")
    }

    override func tearDown() async throws {
        dataSource = nil
        if let dir = tempDir {
            try? FileManager.default.removeItem(at: dir)
        }
    }

    // MARK: - Helper

    /// Run a snapshot SeQL query and collect all RecordBatches.
    ///
    /// Blocks until the query completes (or `timeout` elapses). Fails the test if
    /// the query returns an error.
    @discardableResult
    private func collect(query: String, timeout: TimeInterval = 10) throws -> [RecordBatch] {
        let done = expectation(description: "Query '\(query)' complete")
        var batches: [RecordBatch] = []
        var queryError: String?

        let sink = QueryCollector(
            onBatch: { batches.append($0) },
            onDone: { done.fulfill() },
            onError: { msg in
                queryError = msg
                done.fulfill()
            }
        )

        let stream = try dataSource.executeSeQL(query, sink: sink)
        wait(for: [done], timeout: timeout)

        // Explicit keep-alive: Swift's ARC optimizer must not release these before
        // the wait returns, since SeQLContext holds only a weak sink reference.
        withExtendedLifetime(stream) {}
        withExtendedLifetime(sink) {}

        if let err = queryError {
            XCTFail("Query '\(query)' failed: \(err)")
        }
        return batches
    }

    // MARK: - Resources table

    func testResourcesQueryReturnsBatches() throws {
        let batches = try collect(query: "resources last 24h")
        XCTAssertFalse(batches.isEmpty, "Expected at least one RecordBatch from resources query")
    }

    func testResourcesQueryReturnsServiceNames() throws {
        let batches = try collect(query: "resources last 24h")
        let rows = batches.flatMap { $0.toRows() }

        XCTAssertFalse(rows.isEmpty, "Expected at least one resource row")

        // resources schema: resource_id (col 0), service_name (col 1), attributes (col 2)
        let serviceNames = rows.compactMap { row -> String? in
            guard row.count >= 2 else { return nil }
            return row[1] as? String
        }

        XCTAssertFalse(
            serviceNames.isEmpty,
            "service_name column yielded no strings. Rows: \(rows.prefix(3))"
        )

        // generate_test_data creates these 3 services
        for expected in ["web-service", "api-gateway", "worker-service"] {
            XCTAssertTrue(
                serviceNames.contains(expected),
                "Expected service '\(expected)' in \(serviceNames)"
            )
        }
    }

    func testResourcesSchemaHasExpectedColumns() throws {
        var schema: SeQLSchema?
        let done = expectation(description: "schema")

        final class SchemaSink: SeQLSink {
            let onSchema: (SeQLSchema) -> Void
            let onDone: () -> Void
            init(onSchema: @escaping (SeQLSchema) -> Void, onDone: @escaping () -> Void) {
                self.onSchema = onSchema
                self.onDone = onDone
            }
            nonisolated func onSchema(_ s: SeQLSchema) { onSchema(s) }
            nonisolated func onBatch(_ batch: RecordBatch, table: String?) {}
            nonisolated func onComplete(_ stats: SeQLStats) { onDone() }
            nonisolated func onWarning(code: UInt32, message: String) {}
            nonisolated func onError(code: UInt32, message: String) { onDone() }
        }

        let sink = SchemaSink(onSchema: { schema = $0 }, onDone: { done.fulfill() })
        let stream = try dataSource.executeSeQL("resources last 24h", sink: sink)
        wait(for: [done], timeout: 10)
        withExtendedLifetime(stream) {}
        withExtendedLifetime(sink) {}

        let cols = schema?.columnNames ?? []
        XCTAssertTrue(cols.contains("resource_id"), "Missing resource_id in \(cols)")
        XCTAssertTrue(cols.contains("service_name"), "Missing service_name in \(cols)")
        XCTAssertTrue(cols.contains("attributes"), "Missing attributes in \(cols)")
    }

    // MARK: - Spans table

    func testSpansQueryReturnsBatches() throws {
        let batches = try collect(query: "spans last 1h")
        XCTAssertFalse(batches.isEmpty, "Expected at least one RecordBatch from spans query")
    }

    func testSpansQueryReturnsRows() throws {
        let batches = try collect(query: "spans last 1h")
        let rows = batches.flatMap { $0.toRows() }
        XCTAssertFalse(rows.isEmpty, "Expected at least one span row")

        // spans schema starts with: trace_id (col 0), span_id (col 1), ...
        let traceIds = rows.compactMap { $0.first as? String }
        XCTAssertFalse(traceIds.isEmpty, "trace_id column should yield strings, got: \(rows.prefix(1))")
    }

    func testSpansToRowsDoesNotCrash() throws {
        // Regression test for BinaryArray crashing when reading LargeBinary offsets as Int32.
        // The spans schema includes _overflow_attrs: Map<Utf8View, LargeBinary>. Before the fix,
        // BinaryArray.subscript read Int64 offsets as Int32, producing negative array lengths
        // and crashing in UnsafeBufferPointer.init(start:count:) when count < 0.
        let batches = try collect(query: "spans last 1h")
        // toRows() exercises every column including the nested Map<Utf8View, LargeBinary>
        for batch in batches {
            let rows = batch.toRows()
            // Simply completing without crashing is the pass condition
            XCTAssertEqual(rows.count, Int(batch.length))
        }
    }

    func testSpansSchemaHasExpectedColumns() throws {
        var schema: SeQLSchema?
        let done = expectation(description: "spans-schema")

        final class SchemaSink: SeQLSink {
            let onSchema: (SeQLSchema) -> Void
            let onDone: () -> Void
            init(onSchema: @escaping (SeQLSchema) -> Void, onDone: @escaping () -> Void) {
                self.onSchema = onSchema
                self.onDone = onDone
            }
            nonisolated func onSchema(_ s: SeQLSchema) { onSchema(s) }
            nonisolated func onBatch(_ batch: RecordBatch, table: String?) {}
            nonisolated func onComplete(_ stats: SeQLStats) { onDone() }
            nonisolated func onWarning(code: UInt32, message: String) {}
            nonisolated func onError(code: UInt32, message: String) { onDone() }
        }

        let sink = SchemaSink(onSchema: { schema = $0 }, onDone: { done.fulfill() })
        let stream = try dataSource.executeSeQL("spans last 1h", sink: sink)
        wait(for: [done], timeout: 10)
        withExtendedLifetime(stream) {}
        withExtendedLifetime(sink) {}

        let cols = schema?.columnNames ?? []
        XCTAssertTrue(cols.contains("trace_id"), "Missing trace_id in \(cols)")
        XCTAssertTrue(cols.contains("span_id"), "Missing span_id in \(cols)")
        XCTAssertTrue(cols.contains("name"), "Missing name in \(cols)")
        XCTAssertTrue(cols.contains("start_time_unix_nano"), "Missing start_time_unix_nano in \(cols)")
        XCTAssertTrue(cols.contains("end_time_unix_nano"), "Missing end_time_unix_nano in \(cols)")
    }

    func testSpansRowsHaveNonNullCoreColumns() throws {
        // Verifies that the Arrow IPC decode path correctly reads Utf8View string columns
        // and timestamp columns from the real Rust FFI output.
        // A bug in ProtoUtil.fromProto not handling .utf8view caused all rows to appear
        // as nil even though stats.rowsReturned reported the correct count.
        let batches = try collect(query: "spans last 1h")
        let rows = batches.flatMap { $0.toRows() }
        XCTAssertFalse(rows.isEmpty, "Expected rows from spans query")

        // Find column indices from schema
        guard let batch = batches.first else {
            XCTFail("No batches returned")
            return
        }
        let colNames = batch.schema.fields.map { $0.name }
        guard let traceIdIdx = colNames.firstIndex(of: "trace_id"),
              let spanIdIdx = colNames.firstIndex(of: "span_id"),
              let nameIdx = colNames.firstIndex(of: "name") else {
            XCTFail("Expected columns not found. Schema: \(colNames)")
            return
        }

        var nullTraceIds = 0
        var nullSpanIds = 0
        var nullNames = 0

        for row in rows {
            if row[traceIdIdx] == nil { nullTraceIds += 1 }
            if row[spanIdIdx] == nil { nullSpanIds += 1 }
            if row[nameIdx] == nil { nullNames += 1 }
        }

        XCTAssertEqual(nullTraceIds, 0, "\(nullTraceIds)/\(rows.count) rows have null trace_id")
        XCTAssertEqual(nullSpanIds, 0, "\(nullSpanIds)/\(rows.count) rows have null span_id")
        XCTAssertEqual(nullNames, 0, "\(nullNames)/\(rows.count) rows have null span name")
    }

    func testRowCountMatchesBatchContent() throws {
        // Regression test: stats.rowsReturned should equal the actual decoded row count.
        // Before the ProtoUtil utf8view fix, stats showed rows but decoded batches were empty.
        let done = expectation(description: "resources-with-stats")
        var batches: [RecordBatch] = []
        var stats: SeQLStats?

        final class StatSink: SeQLSink {
            let onBatch: (RecordBatch) -> Void
            let onDone: (SeQLStats) -> Void
            let onFail: (String) -> Void
            init(onBatch: @escaping (RecordBatch) -> Void,
                 onDone: @escaping (SeQLStats) -> Void,
                 onFail: @escaping (String) -> Void) {
                self.onBatch = onBatch
                self.onDone = onDone
                self.onFail = onFail
            }
            nonisolated func onSchema(_ schema: SeQLSchema) {}
            nonisolated func onBatch(_ batch: RecordBatch, table: String?) { onBatch(batch) }
            nonisolated func onComplete(_ s: SeQLStats) { onDone(s) }
            nonisolated func onWarning(code: UInt32, message: String) {}
            nonisolated func onError(code: UInt32, message: String) { onFail("[\(code)] \(message)") }
        }

        let sink = StatSink(
            onBatch: { batches.append($0) },
            onDone: { stats = $0; done.fulfill() },
            onFail: { msg in XCTFail(msg); done.fulfill() }
        )

        let stream = try dataSource.executeSeQL("resources last 24h", sink: sink)
        wait(for: [done], timeout: 10)
        withExtendedLifetime(stream) {}
        withExtendedLifetime(sink) {}

        guard let finalStats = stats else {
            XCTFail("Did not receive completion stats")
            return
        }

        let decodedRowCount = batches.reduce(0) { $0 + Int($1.length) }
        XCTAssertEqual(
            decodedRowCount,
            Int(finalStats.rowsReturned),
            "Decoded row count (\(decodedRowCount)) must equal stats.rowsReturned (\(finalStats.rowsReturned)). " +
            "A mismatch means IPC decode is failing silently."
        )
    }

    // MARK: - Arrow IPC decode round-trip

    func testDecodeIPCRoundTrip() {
        // Build a tiny IPC stream using Arrow builders, then decode it and verify values.
        // This exercises the Arrow-swift IPC reader independently of the Rust FFI.
        do {
            let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
            stringBuilder.append("hello")
            stringBuilder.append("world")
            stringBuilder.append(nil)

            let intBuilder: NumberArrayBuilder<Int32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
            intBuilder.append(42)
            intBuilder.append(7)
            intBuilder.append(nil)

            let labelHolder = ArrowArrayHolderImpl(try stringBuilder.finish())
            let valueHolder = ArrowArrayHolderImpl(try intBuilder.finish())

            let rbResult = RecordBatch.Builder()
                .addColumn("label", arrowArray: labelHolder)
                .addColumn("value", arrowArray: valueHolder)
                .finish()
            guard case .success(let rb) = rbResult else {
                XCTFail("Failed to build RecordBatch")
                return
            }

            let writer = ArrowWriter()
            let writerInfo = ArrowWriter.Info(.recordbatch, schema: rb.schema, batches: [rb])
            guard case .success(let data) = writer.writeStreaming(writerInfo) else {
                XCTFail("Failed to write IPC stream")
                return
            }

            // Decode via the same code path used in SeQLAPI.swift
            var rawBytes = [UInt8](data)
            let decoded = rawBytes.withUnsafeMutableBufferPointer { buf -> [RecordBatch] in
                decodeIPC(data: buf.baseAddress, length: buf.count)
            }

            XCTAssertEqual(decoded.count, 1, "Should decode 1 RecordBatch")
            guard let batch = decoded.first else { return }
            XCTAssertEqual(Int(batch.length), 3)

            let rows = batch.toRows()
            XCTAssertEqual(rows.count, 3)
            XCTAssertEqual(rows[0][0] as? String, "hello")
            XCTAssertEqual(rows[1][0] as? String, "world")
            XCTAssertNil(rows[2][0])
            XCTAssertEqual(rows[0][1] as? Int32, 42)
            XCTAssertEqual(rows[1][1] as? Int32, 7)
            XCTAssertNil(rows[2][1])
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }
}
