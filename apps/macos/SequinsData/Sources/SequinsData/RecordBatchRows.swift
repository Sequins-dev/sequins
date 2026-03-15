import Foundation
import Arrow

extension RecordBatch {
    /// Convert to row-major `[[Any?]]` format using each column's `asAny()` accessor.
    ///
    /// This is useful for ViewModels that parse column values dynamically rather
    /// than via a Codable model. Values are returned as-is from Arrow's type system:
    /// - Timestamps are returned as `Date` (via `TimestampArray.asAny()` override)
    /// - Strings, numbers, booleans follow their native Swift types
    /// - Null values are represented as `nil`
    public func toRows() -> [[Any?]] {
        let numRows = Int(length)
        guard numRows > 0, !columns.isEmpty else { return [] }
        var rows: [[Any?]] = Array(repeating: Array(repeating: nil, count: columns.count), count: numRows)
        for (colIdx, holder) in columns.enumerated() {
            let array = holder.array
            for rowIdx in 0..<numRows {
                rows[rowIdx][colIdx] = array.asAny(UInt(rowIdx))
            }
        }
        return rows
    }

    /// Decode the `_overflow_attrs` Map<Utf8, LargeBinary> column into
    /// per-row attribute dictionaries. Values are CBOR-encoded via ciborium.
    ///
    /// Returns an array of length `numRows`. Rows without overflow attrs get `[:]`.
    public func overflowAttributes() -> [[String: AttributeValue]] {
        let numRows = Int(length)
        guard numRows > 0 else { return [] }
        guard let colIdx = schema.fields.firstIndex(where: { $0.name == "_overflow_attrs" }),
              colIdx < columns.count else {
            return Array(repeating: [:], count: numRows)
        }
        let col = columns[colIdx].array
        var result = [[String: AttributeValue]](repeating: [:], count: numRows)
        for row in 0..<numRows {
            // Map asAny returns [Any?] — one element per map entry.
            // Each entry is itself [Any?] = [String? key, Data? cborValue].
            guard let rawEntries = col.asAny(UInt(row)) as? [Any?] else { continue }
            var attrs: [String: AttributeValue] = [:]
            for rawEntry in rawEntries {
                guard let entry = rawEntry as? [Any?],
                      entry.count >= 2,
                      let key = entry[0] as? String,
                      let valueData = entry[1] as? Data else { continue }
                if let val = cborDecode(valueData) {
                    attrs[key] = val
                }
            }
            result[row] = attrs
        }
        return result
    }
}
