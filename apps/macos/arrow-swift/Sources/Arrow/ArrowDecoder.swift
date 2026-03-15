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

import Foundation

public class ArrowDecoder: Decoder {
    var rbIndex: UInt = 0
    var singleRBCol: Int = 0
    public var codingPath: [CodingKey] = []
    public var userInfo: [CodingUserInfoKey: Any] = [:]
    public let rb: RecordBatch
    public let nameToCol: [String: ArrowArrayHolder]
    public let columns: [ArrowArrayHolder]
    public init(_ decoder: ArrowDecoder) {
        self.userInfo = decoder.userInfo
        self.codingPath = decoder.codingPath
        self.rb = decoder.rb
        self.columns = decoder.columns
        self.nameToCol = decoder.nameToCol
        self.rbIndex = decoder.rbIndex
    }

    public init(_ rb: RecordBatch) {
        self.rb = rb
        var colMapping = [String: ArrowArrayHolder]()
        var columns = [ArrowArrayHolder]()
        for index in 0..<self.rb.schema.fields.count {
            let field = self.rb.schema.fields[index]
            columns.append(self.rb.column(index))
            colMapping[field.name] = self.rb.column(index)
        }

        self.columns = columns
        self.nameToCol = colMapping
    }

    public func decode<T: Decodable, U: Decodable>(_ type: [T: U].Type) throws -> [T: U] {
        var output = [T: U]()
        if rb.columnCount != 2 {
            throw ArrowError.invalid("RecordBatch column count of 2 is required to decode to map")
        }

        for index in 0..<rb.length {
            self.rbIndex = index
            self.singleRBCol = 0
            let key = try T.init(from: self)
            self.singleRBCol = 1
            let value = try U.init(from: self)
            output[key] = value
        }

        self.singleRBCol = 0
        return output
    }

    public func decode<T: Decodable>(_ type: T.Type) throws -> [T] {
        var output = [T]()
        for index in 0..<rb.length {
            self.rbIndex = index
            output.append(try type.init(from: self))
        }

        return output
    }

    public func container<Key>(keyedBy type: Key.Type
    ) -> KeyedDecodingContainer<Key> where Key: CodingKey {
        let container = ArrowKeyedDecoding<Key>(self, codingPath: codingPath)
        return KeyedDecodingContainer(container)
    }

    public func unkeyedContainer() -> UnkeyedDecodingContainer {
        return ArrowUnkeyedDecoding(self, codingPath: codingPath)
    }

    public func singleValueContainer() -> SingleValueDecodingContainer {
        return ArrowSingleValueDecoding(self, codingPath: codingPath)
    }

    func getCol(_ name: String) throws -> AnyArray {
        guard let col = self.nameToCol[name] else {
            throw ArrowError.invalid("Column for key \"\(name)\" not found")
        }

        return col.array
    }

    func getCol(_ index: Int) throws -> AnyArray {
        if index >= self.columns.count {
            throw ArrowError.outOfBounds(index: Int64(index))
        }

        return self.columns[index].array
    }

    func getHolder(_ name: String) throws -> ArrowArrayHolder {
        guard let holder = self.nameToCol[name] else {
            throw ArrowError.invalid("Column for key \"\(name)\" not found")
        }
        return holder
    }

    func doDecode<T>(_ key: CodingKey) throws -> T? {
        let array: AnyArray = try self.getCol(key.stringValue)
        return array.asAny(self.rbIndex) as? T
    }

    func doDecode<T>(_ col: Int) throws -> T? {
        let array: AnyArray = try self.getCol(col)
        return array.asAny(self.rbIndex) as? T
    }

    func isNull(_ key: CodingKey) throws -> Bool {
        let array: AnyArray = try self.getCol(key.stringValue)
        return array.asAny(self.rbIndex) == nil
    }

    func isNull(_ col: Int) throws -> Bool {
        let array: AnyArray = try self.getCol(col)
        return array.asAny(self.rbIndex) == nil
    }
}

private struct ArrowUnkeyedDecoding: UnkeyedDecodingContainer {
    var codingPath: [CodingKey]
    var count: Int? = 0
    var isAtEnd: Bool = false
    var currentIndex: Int = 0
    let decoder: ArrowDecoder

    init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
        self.decoder = decoder
        self.codingPath = codingPath
        self.count = self.decoder.columns.count
    }

    mutating func increment() {
        self.currentIndex += 1
        self.isAtEnd = self.currentIndex >= self.count!
    }

    mutating func decodeNil() throws -> Bool {
        defer {increment()}
        return try self.decoder.isNull(self.currentIndex)
    }

    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if type == Int8?.self || type == Int16?.self ||
            type == Int32?.self || type == Int64?.self ||
            type == UInt8?.self || type == UInt16?.self ||
            type == UInt32?.self || type == UInt64?.self ||
            type == String?.self || type == Double?.self ||
            type == Float?.self || type == Date?.self ||
            type == Bool?.self || type == Bool.self ||
            type == Int8.self || type == Int16.self ||
            type == Int32.self || type == Int64.self ||
            type == UInt8.self || type == UInt16.self ||
            type == UInt32.self || type == UInt64.self ||
            type == String.self || type == Double.self ||
            type == Float.self || type == Date.self {
            defer {increment()}
            return try self.decoder.doDecode(self.currentIndex)!
        } else {
            throw ArrowError.invalid("Type \(type) is currently not supported")
        }
    }

    func nestedContainer<NestedKey>(
        keyedBy type: NestedKey.Type
    ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        throw ArrowError.invalid("Nested decoding is currently not supported.")
    }

    func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw ArrowError.invalid("Nested decoding is currently not supported.")
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding is currently not supported.")
    }
}

private struct ArrowKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    var codingPath = [CodingKey]()
    var allKeys = [Key]()
    let decoder: ArrowDecoder

    init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
        self.decoder = decoder
        self.codingPath = codingPath
    }

    func contains(_ key: Key) -> Bool {
        return self.decoder.nameToCol.keys.contains(key.stringValue)
    }

    func decodeNil(forKey key: Key) throws -> Bool {
        try self.decoder.isNull(key)
    }

    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: String.Type, forKey key: Key) throws -> String {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
        throw ArrowError.invalid(
            "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
    }

    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
        throw ArrowError.invalid(
            "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
    }

    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
        return try self.decoder.doDecode(key)!
    }

    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
        return try self.decoder.doDecode(key)!
    }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        if ArrowArrayBuilders.isValidBuilderType(type) || type == Date.self {
            return try self.decoder.doDecode(key)!
        }
        // Fall through to nested decoder for complex column types (Struct, List, Map)
        let holder = try self.decoder.getHolder(key.stringValue)
        if let nestedArray = holder.array as? NestedArray {
            let nestedDecoder = NestedArrowDecoder(nestedArray, rowIndex: self.decoder.rbIndex)
            return try T.init(from: nestedDecoder)
        }
        throw ArrowError.invalid("Type \(type) is not supported for column \(key.stringValue)")
    }

    func nestedContainer<NestedKey>(
        keyedBy type: NestedKey.Type,
        forKey key: Key
    ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        let holder = try self.decoder.getHolder(key.stringValue)
        if let nestedArray = holder.array as? NestedArray {
            let nestedDecoder = NestedArrowDecoder(nestedArray, rowIndex: self.decoder.rbIndex)
            return try nestedDecoder.container(keyedBy: type)
        }
        throw ArrowError.invalid("Nested keyed container not supported for column \(key.stringValue)")
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        let holder = try self.decoder.getHolder(key.stringValue)
        if let nestedArray = holder.array as? NestedArray {
            let nestedDecoder = NestedArrowDecoder(nestedArray, rowIndex: self.decoder.rbIndex)
            return try nestedDecoder.unkeyedContainer()
        }
        throw ArrowError.invalid("Nested unkeyed container not supported for column \(key.stringValue)")
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding is currently not supported.")
    }

    func superDecoder(forKey key: Key) throws -> Decoder {
        throw ArrowError.invalid("super decoding is currently not supported.")
    }
}

private struct ArrowSingleValueDecoding: SingleValueDecodingContainer {
    var codingPath = [CodingKey]()
    let decoder: ArrowDecoder

    init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
        self.decoder = decoder
        self.codingPath = codingPath
    }

    func decodeNil() -> Bool {
        do {
            return try self.decoder.isNull(self.decoder.singleRBCol)
        } catch {
            return false
        }
    }

    func decode(_ type: Bool.Type) throws -> Bool {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: String.Type) throws -> String {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: Double.Type) throws -> Double {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: Float.Type) throws -> Float {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: Int.Type) throws -> Int {
        throw ArrowError.invalid(
            "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
    }

    func decode(_ type: Int8.Type) throws -> Int8 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: Int16.Type) throws -> Int16 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: Int32.Type) throws -> Int32 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: Int64.Type) throws -> Int64 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: UInt.Type) throws -> UInt {
        throw ArrowError.invalid(
            "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
    }

    func decode(_ type: UInt8.Type) throws -> UInt8 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: UInt16.Type) throws -> UInt16 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: UInt32.Type) throws -> UInt32 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode(_ type: UInt64.Type) throws -> UInt64 {
        return try self.decoder.doDecode(self.decoder.singleRBCol)!
    }

    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if ArrowArrayBuilders.isValidBuilderType(type) || type == Date.self {
            return try self.decoder.doDecode(self.decoder.singleRBCol)!
        } else {
            throw ArrowError.invalid("Type \(type) is currently not supported")
        }
    }
}

// ── NestedArrowDecoder ─────────────────────────────────────────────────────────
//
// Decodes a single cell of a nested Arrow column (Struct, List, or Map)
// into a Swift Codable model.

class NestedArrowDecoder: Decoder {
    let array: NestedArray
    let rowIndex: UInt
    public var codingPath: [CodingKey] = []
    public var userInfo: [CodingUserInfoKey: Any] = [:]

    init(_ array: NestedArray, rowIndex: UInt) {
        self.array = array
        self.rowIndex = rowIndex
    }

    public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key: CodingKey {
        switch array.arrowData.type.id {
        case .strct:
            return KeyedDecodingContainer(NestedStructDecoding<Key>(array, rowIndex: rowIndex))
        case .map:
            return KeyedDecodingContainer(try NestedMapDecoding<Key>(array, rowIndex: rowIndex))
        default:
            throw ArrowError.invalid("Cannot create keyed container for nested type \(array.arrowData.type.id)")
        }
    }

    public func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        switch array.arrowData.type.id {
        case .list:
            return NestedListDecoding(array, rowIndex: rowIndex)
        case .map:
            // Unkeyed map: alternating key, value pairs (for non-String-keyed Dictionaries)
            return NestedMapUnkeyedDecoding(array, rowIndex: rowIndex)
        default:
            throw ArrowError.invalid("Cannot create unkeyed container for nested type \(array.arrowData.type.id)")
        }
    }

    public func singleValueContainer() throws -> SingleValueDecodingContainer {
        throw ArrowError.invalid("Single value container not supported for nested Arrow types")
    }
}

// ── Struct keyed decoding ──────────────────────────────────────────────────────

private struct NestedStructDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    var codingPath = [CodingKey]()
    var allKeys: [Key] {
        guard let structType = array.arrowData.type as? ArrowTypeStruct else { return [] }
        return structType.fields.compactMap { Key(stringValue: $0.name) }
    }
    let array: NestedArray
    let rowIndex: UInt

    init(_ array: NestedArray, rowIndex: UInt) {
        self.array = array
        self.rowIndex = rowIndex
    }

    private func fieldHolder(for key: Key) -> ArrowArrayHolder? {
        guard let structType = array.arrowData.type as? ArrowTypeStruct,
              let fields = array.fields else { return nil }
        for (i, field) in structType.fields.enumerated() where field.name == key.stringValue {
            return i < fields.count ? fields[i] : nil
        }
        return nil
    }

    func contains(_ key: Key) -> Bool { fieldHolder(for: key) != nil }

    func decodeNil(forKey key: Key) throws -> Bool {
        return fieldHolder(for: key)?.array.asAny(rowIndex) == nil
    }

    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? Bool else {
            throw ArrowError.invalid("Cannot decode Bool for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: String.Type, forKey key: Key) throws -> String {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? String else {
            throw ArrowError.invalid("Cannot decode String for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? Double else {
            throw ArrowError.invalid("Cannot decode Double for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? Float else {
            throw ArrowError.invalid("Cannot decode Float for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
        throw ArrowError.invalid("Int not supported; use Int8/Int16/Int32/Int64")
    }

    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? Int8 else {
            throw ArrowError.invalid("Cannot decode Int8 for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? Int16 else {
            throw ArrowError.invalid("Cannot decode Int16 for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? Int32 else {
            throw ArrowError.invalid("Cannot decode Int32 for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? Int64 else {
            throw ArrowError.invalid("Cannot decode Int64 for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
        throw ArrowError.invalid("UInt not supported; use UInt8/UInt16/UInt32/UInt64")
    }

    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? UInt8 else {
            throw ArrowError.invalid("Cannot decode UInt8 for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? UInt16 else {
            throw ArrowError.invalid("Cannot decode UInt16 for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? UInt32 else {
            throw ArrowError.invalid("Cannot decode UInt32 for key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
        guard let v = fieldHolder(for: key)?.array.asAny(rowIndex) as? UInt64 else {
            throw ArrowError.invalid("Cannot decode UInt64 for key \(key.stringValue)")
        }
        return v
    }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        guard let holder = fieldHolder(for: key) else {
            throw ArrowError.invalid("Struct field \(key.stringValue) not found")
        }
        if let value = holder.array.asAny(rowIndex) as? T {
            return value
        }
        if let nestedArray = holder.array as? NestedArray {
            return try T.init(from: NestedArrowDecoder(nestedArray, rowIndex: rowIndex))
        }
        throw ArrowError.invalid("Cannot decode \(type) for struct field \(key.stringValue)")
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type,
                                    forKey key: Key) throws -> KeyedDecodingContainer<NestedKey>
    where NestedKey: CodingKey {
        guard let holder = fieldHolder(for: key),
              let nestedArray = holder.array as? NestedArray else {
            throw ArrowError.invalid("No nested array for key \(key.stringValue)")
        }
        return try NestedArrowDecoder(nestedArray, rowIndex: rowIndex).container(keyedBy: type)
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        guard let holder = fieldHolder(for: key),
              let nestedArray = holder.array as? NestedArray else {
            throw ArrowError.invalid("No nested array for key \(key.stringValue)")
        }
        return try NestedArrowDecoder(nestedArray, rowIndex: rowIndex).unkeyedContainer()
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding not supported in NestedStructDecoding")
    }

    func superDecoder(forKey key: Key) throws -> Decoder {
        throw ArrowError.invalid("super decoding not supported in NestedStructDecoding")
    }
}

// ── Map keyed decoding (String-keyed Dictionary) ───────────────────────────────

private struct NestedMapDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    var codingPath = [CodingKey]()
    let array: NestedArray
    let rowIndex: UInt
    let startOffset: Int
    let endOffset: Int
    let keyHolder: ArrowArrayHolder
    let valueHolder: ArrowArrayHolder

    init(_ array: NestedArray, rowIndex: UInt) throws {
        self.array = array
        self.rowIndex = rowIndex
        guard let (start, end) = array.entryRange(at: rowIndex) else {
            throw ArrowError.invalid("Map array has no offset buffer")
        }
        self.startOffset = Int(start)
        self.endOffset = Int(end)
        // entries child is the struct; its fields are key (0) and value (1)
        guard let entriesHolder = array.entries,
              let entriesNested = entriesHolder.array as? NestedArray,
              let fields = entriesNested.fields,
              fields.count >= 2 else {
            throw ArrowError.invalid("Map entries struct missing key/value fields")
        }
        self.keyHolder = fields[0]
        self.valueHolder = fields[1]
    }

    var allKeys: [Key] {
        var keys = [Key]()
        for i in startOffset..<endOffset {
            if let keyStr = keyHolder.array.asAny(UInt(i)) as? String,
               let key = Key(stringValue: keyStr) {
                keys.append(key)
            }
        }
        return keys
    }

    func contains(_ key: Key) -> Bool {
        for i in startOffset..<endOffset {
            if let keyStr = keyHolder.array.asAny(UInt(i)) as? String,
               keyStr == key.stringValue {
                return true
            }
        }
        return false
    }

    func decodeNil(forKey key: Key) throws -> Bool {
        for i in startOffset..<endOffset {
            if let keyStr = keyHolder.array.asAny(UInt(i)) as? String,
               keyStr == key.stringValue {
                return valueHolder.array.asAny(UInt(i)) == nil
            }
        }
        return true
    }

    private func rawValue(forKey key: Key) -> Any? {
        for i in startOffset..<endOffset {
            if let keyStr = keyHolder.array.asAny(UInt(i)) as? String,
               keyStr == key.stringValue {
                return valueHolder.array.asAny(UInt(i))
            }
        }
        return nil
    }

    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
        guard let v = rawValue(forKey: key) as? Bool else {
            throw ArrowError.invalid("Cannot decode Bool for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: String.Type, forKey key: Key) throws -> String {
        guard let v = rawValue(forKey: key) as? String else {
            throw ArrowError.invalid("Cannot decode String for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
        guard let v = rawValue(forKey: key) as? Double else {
            throw ArrowError.invalid("Cannot decode Double for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
        guard let v = rawValue(forKey: key) as? Float else {
            throw ArrowError.invalid("Cannot decode Float for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
        throw ArrowError.invalid("Int not supported; use Int8/Int16/Int32/Int64")
    }

    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
        guard let v = rawValue(forKey: key) as? Int8 else {
            throw ArrowError.invalid("Cannot decode Int8 for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
        guard let v = rawValue(forKey: key) as? Int16 else {
            throw ArrowError.invalid("Cannot decode Int16 for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
        guard let v = rawValue(forKey: key) as? Int32 else {
            throw ArrowError.invalid("Cannot decode Int32 for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
        guard let v = rawValue(forKey: key) as? Int64 else {
            throw ArrowError.invalid("Cannot decode Int64 for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
        throw ArrowError.invalid("UInt not supported; use UInt8/UInt16/UInt32/UInt64")
    }

    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
        guard let v = rawValue(forKey: key) as? UInt8 else {
            throw ArrowError.invalid("Cannot decode UInt8 for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
        guard let v = rawValue(forKey: key) as? UInt16 else {
            throw ArrowError.invalid("Cannot decode UInt16 for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
        guard let v = rawValue(forKey: key) as? UInt32 else {
            throw ArrowError.invalid("Cannot decode UInt32 for map key \(key.stringValue)")
        }
        return v
    }

    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
        guard let v = rawValue(forKey: key) as? UInt64 else {
            throw ArrowError.invalid("Cannot decode UInt64 for map key \(key.stringValue)")
        }
        return v
    }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        if let v = rawValue(forKey: key) as? T { return v }
        throw ArrowError.invalid("Cannot decode \(type) for map key \(key.stringValue)")
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type,
                                    forKey key: Key) throws -> KeyedDecodingContainer<NestedKey>
    where NestedKey: CodingKey {
        throw ArrowError.invalid("Nested container not supported in NestedMapDecoding")
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw ArrowError.invalid("Nested unkeyed container not supported in NestedMapDecoding")
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding not supported in NestedMapDecoding")
    }

    func superDecoder(forKey key: Key) throws -> Decoder {
        throw ArrowError.invalid("super decoding not supported in NestedMapDecoding")
    }
}

// ── List unkeyed decoding (for Array / List columns) ───────────────────────────

private struct NestedListDecoding: UnkeyedDecodingContainer {
    var codingPath = [CodingKey]()
    var count: Int?
    var isAtEnd: Bool { currentIndex >= (count ?? 0) }
    var currentIndex: Int = 0
    let array: NestedArray
    let rowIndex: UInt
    let startOffset: Int
    let endOffset: Int
    let valueHolder: ArrowArrayHolder

    init(_ array: NestedArray, rowIndex: UInt) {
        self.array = array
        self.rowIndex = rowIndex
        if let (start, end) = array.entryRange(at: rowIndex),
           let values = array.values {
            self.startOffset = Int(start)
            self.endOffset = Int(end)
            self.valueHolder = values
            self.count = Int(end - start)
        } else {
            self.startOffset = 0
            self.endOffset = 0
            self.valueHolder = array.values!
            self.count = 0
        }
    }

    private var absoluteIndex: Int { startOffset + currentIndex }

    mutating func decodeNil() throws -> Bool {
        defer { currentIndex += 1 }
        return valueHolder.array.asAny(UInt(absoluteIndex)) == nil
    }

    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        defer { currentIndex += 1 }
        let idx = UInt(absoluteIndex)
        if let v = valueHolder.array.asAny(idx) as? T { return v }
        if let nestedArray = valueHolder.array as? NestedArray {
            return try T.init(from: NestedArrowDecoder(nestedArray, rowIndex: idx))
        }
        throw ArrowError.invalid("Cannot decode \(type) at list index \(currentIndex)")
    }

    func nestedContainer<NestedKey>(
        keyedBy type: NestedKey.Type
    ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        throw ArrowError.invalid("Nested container not supported in NestedListDecoding")
    }

    func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw ArrowError.invalid("Nested unkeyed container not supported in NestedListDecoding")
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding not supported in NestedListDecoding")
    }
}

// ── Map unkeyed decoding (alternating key, value — for non-String-keyed Dict) ──

private struct NestedMapUnkeyedDecoding: UnkeyedDecodingContainer {
    var codingPath = [CodingKey]()
    var count: Int?
    var isAtEnd: Bool { currentIndex >= (count ?? 0) }
    var currentIndex: Int = 0
    let array: NestedArray
    let rowIndex: UInt
    let startOffset: Int
    let endOffset: Int
    let keyHolder: ArrowArrayHolder
    let valueHolder: ArrowArrayHolder

    init(_ array: NestedArray, rowIndex: UInt) {
        self.array = array
        self.rowIndex = rowIndex
        let range = array.entryRange(at: rowIndex)
        let start = range.map { Int($0.0) } ?? 0
        let end = range.map { Int($0.1) } ?? 0
        self.startOffset = start
        self.endOffset = end
        // 2 elements per entry (key then value), so total count = 2 * entries
        self.count = (end - start) * 2

        if let entriesHolder = array.entries,
           let entriesNested = entriesHolder.array as? NestedArray,
           let fields = entriesNested.fields, fields.count >= 2 {
            self.keyHolder = fields[0]
            self.valueHolder = fields[1]
        } else {
            // Fallback (shouldn't happen for valid map arrays)
            let placeholder = array.entries!
            self.keyHolder = placeholder
            self.valueHolder = placeholder
        }
    }

    /// Entry index (0-based) and whether we're reading the key (true) or value (false)
    private var entryIndex: Int { currentIndex / 2 }
    private var isKey: Bool { currentIndex % 2 == 0 }
    private var absoluteEntryIndex: Int { startOffset + entryIndex }

    mutating func decodeNil() throws -> Bool {
        defer { currentIndex += 1 }
        let idx = UInt(absoluteEntryIndex)
        let holder = isKey ? keyHolder : valueHolder
        return holder.array.asAny(idx) == nil
    }

    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        defer { currentIndex += 1 }
        let idx = UInt(absoluteEntryIndex)
        let holder = isKey ? keyHolder : valueHolder
        if let v = holder.array.asAny(idx) as? T { return v }
        throw ArrowError.invalid("Cannot decode \(type) at map entry \(entryIndex) (\(isKey ? "key" : "value"))")
    }

    func nestedContainer<NestedKey>(
        keyedBy type: NestedKey.Type
    ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        throw ArrowError.invalid("Nested container not supported in NestedMapUnkeyedDecoding")
    }

    func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw ArrowError.invalid("Nested unkeyed container not supported in NestedMapUnkeyedDecoding")
    }

    func superDecoder() throws -> Decoder {
        throw ArrowError.invalid("super decoding not supported in NestedMapUnkeyedDecoding")
    }
}
