import XCTest
import Arrow
@testable import SequinsData

final class RecordBatchTreeTests: XCTestCase {

    // MARK: - Helpers

    /// Build a RecordBatch with string + int columns for simple tests.
    private func makeSimpleBatch() throws -> RecordBatch {
        let strBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        strBuilder.append("hello")
        strBuilder.append(nil)

        let intBuilder: NumberArrayBuilder<Int32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        intBuilder.append(42)
        intBuilder.append(7)

        let strHolder = ArrowArrayHolderImpl(try strBuilder.finish())
        let intHolder = ArrowArrayHolderImpl(try intBuilder.finish())

        let result = RecordBatch.Builder()
            .addColumn("name", arrowArray: strHolder)
            .addColumn("value", arrowArray: intHolder)
            .finish()
        guard case .success(let rb) = result else {
            throw NSError(domain: "test", code: 1, userInfo: [NSLocalizedDescriptionKey: "Failed to build RecordBatch"])
        }
        return rb
    }

    // MARK: - Basic structure

    func testToRecordTreesRowCount() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        XCTAssertEqual(trees.count, 2, "One top-level node per row")
    }

    func testToRecordTreesColumnCount() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        // Each row node has one child per column
        XCTAssertEqual(trees[0].children.count, 2)
        XCTAssertEqual(trees[1].children.count, 2)
    }

    func testToRecordTreesFieldNames() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        XCTAssertEqual(trees[0].children[0].name, "name")
        XCTAssertEqual(trees[0].children[1].name, "value")
    }

    func testToRecordTreesStringValue() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        let nameNode = trees[0].children[0]
        XCTAssertEqual(nameNode.displayValue, "hello")
        XCTAssertEqual(nameNode.typeLabel, .string)
    }

    func testToRecordTreesNullValue() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        let nameNode = trees[1].children[0]  // second row, name col is nil
        XCTAssertEqual(nameNode.typeLabel, .null)
        XCTAssertEqual(nameNode.displayValue, "null")
    }

    func testToRecordTreesIntValue() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        let valueNode = trees[0].children[1]
        XCTAssertEqual(valueNode.displayValue, "42")
        XCTAssertEqual(valueNode.typeLabel, .number)
    }

    // MARK: - Top-level node

    func testTopLevelNodeIsStructType() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        XCTAssertEqual(trees[0].typeLabel, .structType)
        XCTAssertFalse(trees[0].isLeaf)
    }

    func testTopLevelNodeIds() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        XCTAssertEqual(trees[0].id, "0")
        XCTAssertEqual(trees[1].id, "1")
    }

    // MARK: - Path IDs

    func testChildPathIds() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        XCTAssertEqual(trees[0].children[0].id, "0/name")
        XCTAssertEqual(trees[0].children[1].id, "0/value")
        XCTAssertEqual(trees[1].children[0].id, "1/name")
    }

    // MARK: - Boolean column

    func testBooleanColumn() throws {
        let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
        boolBuilder.append(true)
        boolBuilder.append(false)
        let holder = ArrowArrayHolderImpl(try boolBuilder.finish())
        let result = RecordBatch.Builder()
            .addColumn("active", arrowArray: holder)
            .finish()
        guard case .success(let rb) = result else {
            XCTFail("Failed to build batch"); return
        }

        let trees = rb.toRecordTrees()
        XCTAssertEqual(trees[0].children[0].typeLabel, .boolean)
        XCTAssertEqual(trees[0].children[0].displayValue, "true")
        XCTAssertEqual(trees[1].children[0].displayValue, "false")
    }

    // MARK: - Empty batch

    func testEmptyBatchProducesNoTrees() throws {
        let strBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        let holder = ArrowArrayHolderImpl(try strBuilder.finish())
        let result = RecordBatch.Builder()
            .addColumn("x", arrowArray: holder)
            .finish()
        guard case .success(let rb) = result else {
            XCTFail("Failed to build batch"); return
        }
        XCTAssertEqual(rb.toRecordTrees().count, 0)
    }

    // MARK: - Summary

    func testSummaryIsPresentOnNonEmptyBatch() throws {
        let batch = try makeSimpleBatch()
        let trees = batch.toRecordTrees()
        // Summary may be empty string or non-nil but should not crash
        XCTAssertNotNil(trees[0].summary)
    }

    // MARK: - Duration formatting

    func testDurationFormattingNanoseconds() throws {
        let builder: NumberArrayBuilder<Int64> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        builder.append(500)
        let holder = ArrowArrayHolderImpl(try builder.finish())
        let result = RecordBatch.Builder()
            .addColumn("duration_ns", arrowArray: holder)
            .finish()
        guard case .success(let rb) = result else {
            XCTFail("Failed to build batch"); return
        }
        let tree = rb.toRecordTrees()[0]
        XCTAssertEqual(tree.children[0].typeLabel, .duration)
        XCTAssertEqual(tree.children[0].displayValue, "500ns")
    }

    func testDurationFormattingMilliseconds() throws {
        let builder: NumberArrayBuilder<Int64> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        builder.append(5_000_000)  // 5ms
        let holder = ArrowArrayHolderImpl(try builder.finish())
        let result = RecordBatch.Builder()
            .addColumn("duration_ns", arrowArray: holder)
            .finish()
        guard case .success(let rb) = result else {
            XCTFail("Failed to build batch"); return
        }
        let tree = rb.toRecordTrees()[0]
        XCTAssertEqual(tree.children[0].displayValue, "5.00ms")
    }

    // MARK: - ID column label

    func testIdColumnGetsIdTypeLabel() throws {
        let strBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        strBuilder.append("abc123")
        let holder = ArrowArrayHolderImpl(try strBuilder.finish())
        let result = RecordBatch.Builder()
            .addColumn("trace_id", arrowArray: holder)
            .finish()
        guard case .success(let rb) = result else {
            XCTFail("Failed to build batch"); return
        }
        let tree = rb.toRecordTrees()[0]
        XCTAssertEqual(tree.children[0].typeLabel, .id)
    }
}
