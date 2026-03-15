// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import XCTest
@testable import Arrow

// MARK: - Helpers

/// Builds a Map<String, String> NestedArray.
/// Each row is a list of (key, value) pairs; nil means a null row.
private func makeStringMapArray(_ rows: [[(key: String, value: String)]?]) throws -> NestedArray {
    let keyField = ArrowField("key", type: ArrowType(ArrowType.ArrowString), isNullable: false)
    let valueField = ArrowField("value", type: ArrowType(ArrowType.ArrowString), isNullable: false)

    // Concatenate all key/value pairs into the entries struct
    let entriesBuilder = try StructArrayBuilder([keyField, valueField])
    for row in rows {
        if let pairs = row {
            for (k, v) in pairs {
                entriesBuilder.append([k, v])
            }
        }
    }
    let entriesData = try entriesBuilder.finish().arrowData

    // Build offsets/nulls for the list layer
    let listBufBuilder = try ListBufferBuilder()
    for row in rows {
        if let pairs = row {
            listBufBuilder.append(pairs.count)
        } else {
            listBufBuilder.append(nil as [Any?]?)
        }
    }
    let listBuffers = listBufBuilder.finish()
    let nullCount = UInt(rows.filter { $0 == nil }.count)

    let mapType = ArrowTypeMap(keyField: keyField, valueField: valueField, keysSorted: false)
    let arrowData = try ArrowData(mapType, buffers: listBuffers,
                                  children: [entriesData],
                                  nullCount: nullCount,
                                  length: UInt(rows.count))
    return try NestedArray(arrowData)
}

/// Builds a List<String> NestedArray.
private func makeStringListArray(_ rows: [[String]?]) throws -> NestedArray {
    let listType = ArrowTypeList(ArrowField("item", type: ArrowType(ArrowType.ArrowString), isNullable: true))
    let builder = try ListArrayBuilder(listType)
    for row in rows {
        if let items = row {
            builder.append(items.map { $0 as Any? })
        } else {
            builder.append(nil)
        }
    }
    return try builder.finish()
}

// MARK: - Map IPC Roundtrip

final class MapIPCTests: XCTestCase {
    func testMapIPCRoundtrip() throws {
        // Row 0: {env: prod, service: api}  (2 entries)
        // Row 1: {env: dev}                  (1 entry)
        // Row 2: null
        let mapArray = try makeStringMapArray([
            [(key: "env", value: "prod"), (key: "service", value: "api")],
            [(key: "env", value: "dev")],
            nil
        ])
        let mapHolder = ArrowArrayHolderImpl(mapArray)

        let rb = try RecordBatch.Builder()
            .addColumn("attrs", arrowArray: mapHolder)
            .finish()
            .get()

        // Write to Arrow IPC streaming format
        let writer = ArrowWriter()
        let writeInfo = ArrowWriter.Info(.recordbatch, schema: rb.schema, batches: [rb])
        let ipcData = try writer.writeStreaming(writeInfo).get()

        // Read back
        let reader = ArrowReader()
        let readResult = try reader.readStreaming(ipcData).get()

        XCTAssertEqual(readResult.batches.count, 1)
        let readBatch = readResult.batches[0]
        XCTAssertEqual(readBatch.length, 3)
        XCTAssertEqual(readBatch.schema.fields[0].name, "attrs")
        XCTAssertEqual(readBatch.schema.fields[0].type.id, .map)

        guard let nested = readBatch.columns[0].array as? NestedArray else {
            XCTFail("Expected NestedArray for map column")
            return
        }
        XCTAssertTrue(nested.isMapArray)

        // Row 0: 2 entries
        XCTAssertNotNil(nested[0])
        XCTAssertEqual(nested[0]?.count, 2)

        // Row 1: 1 entry
        XCTAssertNotNil(nested[1])
        XCTAssertEqual(nested[1]?.count, 1)

        // Row 2: null
        XCTAssertTrue(nested.arrowData.isNull(2))
        XCTAssertNil(nested[2])
    }
}

// MARK: - Nested Codable Tests

final class NestedCodableTests: XCTestCase {

    // MARK: Map column → [String: String]

    struct RecordWithMap: Codable {
        let name: String
        let attrs: [String: String]
    }

    func testCodableDecodeWithMap() throws {
        let nameBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        nameBuilder.append("alpha", "beta")

        let mapArray = try makeStringMapArray([
            [(key: "env", value: "prod"), (key: "region", value: "us-east")],
            [(key: "env", value: "dev")]
        ])

        let rb = try RecordBatch.Builder()
            .addColumn("name", arrowArray: try nameBuilder.toHolder())
            .addColumn("attrs", arrowArray: ArrowArrayHolderImpl(mapArray))
            .finish()
            .get()

        let records = try ArrowDecoder(rb).decode(RecordWithMap.self)

        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0].name, "alpha")
        XCTAssertEqual(records[0].attrs["env"], "prod")
        XCTAssertEqual(records[0].attrs["region"], "us-east")
        XCTAssertEqual(records[1].name, "beta")
        XCTAssertEqual(records[1].attrs["env"], "dev")
        XCTAssertEqual(records[1].attrs.count, 1)
    }

    // MARK: List column → [String]

    struct RecordWithList: Codable {
        let id: Int32
        let tags: [String]
    }

    func testCodableDecodeWithList() throws {
        let idBuilder: NumberArrayBuilder<Int32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        idBuilder.append(1, 2)

        let listArray = try makeStringListArray([
            ["swift", "arrow", "test"],
            ["codable"]
        ])

        let rb = try RecordBatch.Builder()
            .addColumn("id", arrowArray: try idBuilder.toHolder())
            .addColumn("tags", arrowArray: ArrowArrayHolderImpl(listArray))
            .finish()
            .get()

        let records = try ArrowDecoder(rb).decode(RecordWithList.self)

        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0].id, 1)
        XCTAssertEqual(records[0].tags, ["swift", "arrow", "test"])
        XCTAssertEqual(records[1].id, 2)
        XCTAssertEqual(records[1].tags, ["codable"])
    }

    // MARK: Struct column → nested Codable struct

    struct Address: Codable {
        let street: String
        let city: String
    }

    struct RecordWithStruct: Codable {
        let name: String
        let address: Address
    }

    func testCodableDecodeWithStruct() throws {
        let nameBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        nameBuilder.append("Alice", "Bob")

        let streetField = ArrowField("street", type: ArrowType(ArrowType.ArrowString), isNullable: true)
        let cityField = ArrowField("city", type: ArrowType(ArrowType.ArrowString), isNullable: true)
        let structBuilder = try StructArrayBuilder([streetField, cityField])
        structBuilder.append(["123 Main St", "Springfield"])
        structBuilder.append(["456 Oak Ave", "Shelbyville"])

        let rb = try RecordBatch.Builder()
            .addColumn("name", arrowArray: try nameBuilder.toHolder())
            .addColumn("address", arrowArray: ArrowArrayHolderImpl(try structBuilder.finish()))
            .finish()
            .get()

        let records = try ArrowDecoder(rb).decode(RecordWithStruct.self)

        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0].name, "Alice")
        XCTAssertEqual(records[0].address.street, "123 Main St")
        XCTAssertEqual(records[0].address.city, "Springfield")
        XCTAssertEqual(records[1].name, "Bob")
        XCTAssertEqual(records[1].address.street, "456 Oak Ave")
        XCTAssertEqual(records[1].address.city, "Shelbyville")
    }

    // MARK: Timestamp column → Date

    struct RecordWithTimestamp: Codable {
        let id: Int32
        let ts: Date
    }

    func testTimestampDecodesAsDate() throws {
        // Nanoseconds since Unix epoch
        let ns1: Int64 = 1_700_000_000_000_000_000
        let ns2: Int64 = 1_710_000_000_000_000_000

        let idBuilder: NumberArrayBuilder<Int32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        idBuilder.append(1, 2)

        let tsBuilder = try ArrowArrayBuilders.loadTimestampArrayBuilder(.nanoseconds)
        tsBuilder.append(ns1, ns2)

        let rb = try RecordBatch.Builder()
            .addColumn("id", arrowArray: try idBuilder.toHolder())
            .addColumn("ts", arrowArray: try tsBuilder.toHolder())
            .finish()
            .get()

        // Verify asAny() returns Date, not Int64
        let tsCol = rb.columns[1].array
        let val0 = tsCol.asAny(0)
        XCTAssertTrue(val0 is Date, "Expected Date but got \(type(of: val0))")

        let date0 = val0 as! Date // swiftlint:disable:this force_cast
        let expectedSeconds0 = Double(ns1) / 1e9
        XCTAssertEqual(date0.timeIntervalSince1970, expectedSeconds0, accuracy: 1e-3)

        // Verify ArrowDecoder decodes Timestamp as Date
        let records = try ArrowDecoder(rb).decode(RecordWithTimestamp.self)
        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0].ts.timeIntervalSince1970, expectedSeconds0, accuracy: 1e-3)
    }

    // MARK: Mixed flat and nested columns

    struct MixedRecord: Codable {
        let id: Int32
        let label: String
        let attrs: [String: String]
        let tags: [String]
    }

    func testCodableDecodeMixedFlatAndNested() throws {
        let idBuilder: NumberArrayBuilder<Int32> = try ArrowArrayBuilders.loadNumberArrayBuilder()
        idBuilder.append(10, 20)

        let labelBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
        labelBuilder.append("foo", "bar")

        let mapArray = try makeStringMapArray([
            [(key: "k1", value: "v1")],
            [(key: "k2", value: "v2"), (key: "k3", value: "v3")]
        ])

        let listArray = try makeStringListArray([["a", "b"], ["c"]])

        let rb = try RecordBatch.Builder()
            .addColumn("id", arrowArray: try idBuilder.toHolder())
            .addColumn("label", arrowArray: try labelBuilder.toHolder())
            .addColumn("attrs", arrowArray: ArrowArrayHolderImpl(mapArray))
            .addColumn("tags", arrowArray: ArrowArrayHolderImpl(listArray))
            .finish()
            .get()

        let records = try ArrowDecoder(rb).decode(MixedRecord.self)

        XCTAssertEqual(records.count, 2)
        XCTAssertEqual(records[0].id, 10)
        XCTAssertEqual(records[0].label, "foo")
        XCTAssertEqual(records[0].attrs["k1"], "v1")
        XCTAssertEqual(records[0].tags, ["a", "b"])
        XCTAssertEqual(records[1].id, 20)
        XCTAssertEqual(records[1].label, "bar")
        XCTAssertEqual(records[1].attrs["k2"], "v2")
        XCTAssertEqual(records[1].attrs["k3"], "v3")
        XCTAssertEqual(records[1].tags, ["c"])
    }
}
