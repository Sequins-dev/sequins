import Foundation
import Arrow

// MARK: - RecordBatch → RecordNode tree

extension RecordBatch {
    /// Convert every row in this batch into a top-level `RecordNode` tree.
    ///
    /// Each returned node represents one record row and has children for each column field.
    /// Nested Arrow types (List, Struct, Map) produce sub-trees preserving field names from
    /// the schema.
    public func toRecordTrees() -> [RecordNode] {
        let numRows = Int(length)
        guard numRows > 0, !columns.isEmpty else { return [] }
        let fields = schema.fields
        var records: [RecordNode] = []
        records.reserveCapacity(numRows)

        for rowIdx in 0..<numRows {
            let pathPrefix = "\(rowIdx)"
            var children: [RecordNode] = []
            children.reserveCapacity(fields.count)

            for (colIdx, field) in fields.enumerated() {
                guard colIdx < columns.count else { continue }
                let rawValue = columns[colIdx].array.asAny(UInt(rowIdx))
                let node = buildNode(
                    name: field.name,
                    type: field.type,
                    value: rawValue,
                    pathPrefix: "\(pathPrefix)/\(field.name)"
                )
                children.append(node)
            }

            let summary = buildRecordSummary(fields: fields, children: children)
            let record = RecordNode(
                id: pathPrefix,
                name: "Row \(rowIdx + 1)",
                typeLabel: .structType,
                children: children,
                summary: summary
            )
            records.append(record)
        }
        return records
    }
}

// MARK: - Recursive node builder

private func buildNode(
    name: String,
    type: ArrowType,
    value: Any?,
    pathPrefix: String
) -> RecordNode {
    // Nil / null
    guard let value else {
        return RecordNode(id: pathPrefix, name: name, typeLabel: .null, displayValue: "null")
    }

    // Struct
    if type.id == .strct, let structType = type as? ArrowTypeStruct {
        // asAny() for a struct returns [Any?] where index matches field position
        if let elements = value as? [Any?] {
            var children: [RecordNode] = []
            children.reserveCapacity(structType.fields.count)
            for (i, childField) in structType.fields.enumerated() {
                let childValue: Any? = i < elements.count ? elements[i] : nil
                let child = buildNode(
                    name: childField.name,
                    type: childField.type,
                    value: childValue,
                    pathPrefix: "\(pathPrefix)/\(childField.name)"
                )
                children.append(child)
            }
            return RecordNode(
                id: pathPrefix,
                name: name,
                typeLabel: .structType,
                children: children
            )
        }
    }

    // List
    if type.id == .list, let listType = type as? ArrowTypeList {
        // asAny() for a list returns [Any?] where each element is a list item
        if let elements = value as? [Any?] {
            var children: [RecordNode] = []
            children.reserveCapacity(elements.count)
            for (i, elem) in elements.enumerated() {
                let child = buildNode(
                    name: "[\(i)]",
                    type: listType.elementField.type,
                    value: elem,
                    pathPrefix: "\(pathPrefix)/\(i)"
                )
                children.append(child)
            }
            return RecordNode(
                id: pathPrefix,
                name: name,
                typeLabel: .list,
                children: children
            )
        }
    }

    // Map — asAny() returns [Any?] where each entry is [Any?] = [key, value]
    if type.id == .map, let mapType = type as? ArrowTypeMap {
        // Special case: _overflow_attrs Map<Utf8, LargeBinary> → CBOR-decode values
        if name == "_overflow_attrs" {
            return buildOverflowAttrsNode(name: name, value: value, pathPrefix: pathPrefix)
        }

        if let entries = value as? [Any?] {
            var children: [RecordNode] = []
            children.reserveCapacity(entries.count)
            for (i, rawEntry) in entries.enumerated() {
                guard let entry = rawEntry as? [Any?], entry.count >= 2 else {
                    let child = buildNode(
                        name: "[\(i)]",
                        type: mapType.valueField.type,
                        value: rawEntry,
                        pathPrefix: "\(pathPrefix)/\(i)"
                    )
                    children.append(child)
                    continue
                }
                let keyStr = entry[0].map { "\($0)" } ?? "nil"
                let child = buildNode(
                    name: keyStr,
                    type: mapType.valueField.type,
                    value: entry[1],
                    pathPrefix: "\(pathPrefix)/\(i)"
                )
                children.append(child)
            }
            return RecordNode(
                id: pathPrefix,
                name: name,
                typeLabel: .map,
                children: children
            )
        }
    }

    // Scalar leaf
    return buildScalarNode(name: name, type: type, value: value, pathPrefix: pathPrefix)
}

// MARK: - Scalar leaf nodes

private func buildScalarNode(
    name: String,
    type: ArrowType,
    value: Any,
    pathPrefix: String
) -> RecordNode {
    let typeLabel = nodeTypeLabel(for: name, arrowType: type)
    let display = formatScalar(name: name, type: type, value: value)
    return RecordNode(id: pathPrefix, name: name, typeLabel: typeLabel, displayValue: display)
}

private func nodeTypeLabel(for name: String, arrowType: ArrowType) -> NodeTypeLabel {
    if isIdColumn(name) { return .id }
    switch arrowType.id {
    case .timestamp, .date32, .date64, .time32, .time64:
        return .timestamp
    case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64, .float, .double:
        return isDurationColumn(name) ? .duration : .number
    case .boolean:
        return .boolean
    case .string, .stringView:
        return .string
    case .binary:
        return .binary
    case .null:
        return .null
    default:
        return .unknown
    }
}

private func formatScalar(name: String, type: ArrowType, value: Any) -> String {
    // Duration columns: value is nanoseconds as Int64
    if isDurationColumn(name) {
        if let ns = value as? Int64 {
            return formatDurationNs(ns)
        }
        if let ns = value as? UInt64 {
            return formatDurationNs(Int64(bitPattern: ns))
        }
    }

    // Timestamp columns from Arrow: arrive as Date
    if let date = value as? Date {
        return formatTimestamp(date)
    }

    // Numbers
    if let n = value as? Int64 { return "\(n)" }
    if let n = value as? Int32 { return "\(n)" }
    if let n = value as? Int16 { return "\(n)" }
    if let n = value as? Int8  { return "\(n)" }
    if let n = value as? UInt64 { return "\(n)" }
    if let n = value as? UInt32 { return "\(n)" }
    if let n = value as? UInt16 { return "\(n)" }
    if let n = value as? UInt8  { return "\(n)" }
    if let n = value as? Double { return formatDouble(n) }
    if let n = value as? Float  { return formatDouble(Double(n)) }

    // Bool
    if let b = value as? Bool { return b ? "true" : "false" }

    // String / StringView
    if let s = value as? String { return s }

    // Binary data
    if let d = value as? Data { return "<\(d.count) bytes>" }

    return String(describing: value)
}

// MARK: - Overflow attrs (CBOR)

private func buildOverflowAttrsNode(name: String, value: Any, pathPrefix: String) -> RecordNode {
    guard let entries = value as? [Any?] else {
        return RecordNode(id: pathPrefix, name: name, typeLabel: .map, displayValue: String(describing: value))
    }
    var children: [RecordNode] = []
    for (i, rawEntry) in entries.enumerated() {
        guard let entry = rawEntry as? [Any?],
              entry.count >= 2,
              let key = entry[0] as? String,
              let valueData = entry[1] as? Data,
              let attrValue = cborDecode(valueData) else {
            continue
        }
        let childNode = buildAttributeValueNode(key: key, value: attrValue, pathPrefix: "\(pathPrefix)/\(i)")
        children.append(childNode)
    }
    return RecordNode(id: pathPrefix, name: name, typeLabel: .map, children: children)
}

private func buildAttributeValueNode(key: String, value: AttributeValue, pathPrefix: String) -> RecordNode {
    switch value {
    case .string(let s):
        return RecordNode(id: pathPrefix, name: key, typeLabel: .string, displayValue: s)
    case .bool(let b):
        return RecordNode(id: pathPrefix, name: key, typeLabel: .boolean, displayValue: b ? "true" : "false")
    case .int(let i):
        return RecordNode(id: pathPrefix, name: key, typeLabel: .number, displayValue: "\(i)")
    case .double(let d):
        return RecordNode(id: pathPrefix, name: key, typeLabel: .number, displayValue: formatDouble(d))
    case .stringArray(let arr):
        let children = arr.enumerated().map { (i, s) in
            RecordNode(id: "\(pathPrefix)/\(i)", name: "[\(i)]", typeLabel: .string, displayValue: s)
        }
        return RecordNode(id: pathPrefix, name: key, typeLabel: .list, children: children)
    case .intArray(let arr):
        let children = arr.enumerated().map { (i, n) in
            RecordNode(id: "\(pathPrefix)/\(i)", name: "[\(i)]", typeLabel: .number, displayValue: "\(n)")
        }
        return RecordNode(id: pathPrefix, name: key, typeLabel: .list, children: children)
    case .doubleArray(let arr):
        let children = arr.enumerated().map { (i, d) in
            RecordNode(id: "\(pathPrefix)/\(i)", name: "[\(i)]", typeLabel: .number, displayValue: formatDouble(d))
        }
        return RecordNode(id: pathPrefix, name: key, typeLabel: .list, children: children)
    case .boolArray(let arr):
        let children = arr.enumerated().map { (i, b) in
            RecordNode(id: "\(pathPrefix)/\(i)", name: "[\(i)]", typeLabel: .boolean, displayValue: b ? "true" : "false")
        }
        return RecordNode(id: pathPrefix, name: key, typeLabel: .list, children: children)
    }
}

// MARK: - Summary line

private func buildRecordSummary(fields: [ArrowField], children: [RecordNode]) -> String {
    // Prefer first ID field, then first string field, then first timestamp
    var idPart: String?
    var namePart: String?
    var timePart: String?

    for child in children {
        switch child.typeLabel {
        case .id where idPart == nil:
            if let v = child.displayValue { idPart = "\(child.name): \(v.prefix(16))" }
        case .string where namePart == nil:
            if let v = child.displayValue, !v.isEmpty { namePart = v }
        case .timestamp where timePart == nil:
            if let v = child.displayValue { timePart = v }
        default:
            break
        }
    }

    let parts = [idPart, namePart, timePart].compactMap { $0 }
    return parts.prefix(2).joined(separator: "  ·  ")
}

// MARK: - Column classification helpers

private func isIdColumn(_ name: String) -> Bool {
    let lower = name.lowercased()
    return lower.hasSuffix("_id") || lower == "trace_id" || lower == "span_id" || lower == "profile_id"
}

private func isDurationColumn(_ name: String) -> Bool {
    let lower = name.lowercased()
    return lower.contains("duration") || lower == "elapsed_ns"
}

// MARK: - Formatting helpers

private let timestampFormatter: DateFormatter = {
    let f = DateFormatter()
    f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
    f.timeZone = TimeZone.current
    return f
}()

private func formatTimestamp(_ date: Date) -> String {
    timestampFormatter.string(from: date)
}

private func formatDurationNs(_ ns: Int64) -> String {
    let absNs = abs(ns)
    let sign = ns < 0 ? "-" : ""
    if absNs < 1_000 {
        return "\(sign)\(absNs)ns"
    } else if absNs < 1_000_000 {
        return "\(sign)\(String(format: "%.2f", Double(absNs) / 1_000))µs"
    } else if absNs < 1_000_000_000 {
        return "\(sign)\(String(format: "%.2f", Double(absNs) / 1_000_000))ms"
    } else {
        return "\(sign)\(String(format: "%.3f", Double(absNs) / 1_000_000_000))s"
    }
}

private func formatDouble(_ d: Double) -> String {
    if d == d.rounded() && !d.isInfinite && abs(d) < 1e15 {
        return String(format: "%.0f", d)
    }
    return String(format: "%g", d)
}
