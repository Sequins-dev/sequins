import Foundation

/// Small helpers for mapping loosely-typed SeQL/SQL result rows (`[[Any?]]`) into the
/// numeric/category values the shared visualization charts consume.
enum VizData {
    /// Best-effort numeric coercion for a single cell.
    static func numeric(_ value: Any?) -> Double? {
        guard let value else { return nil }
        switch value {
        case let n as NSNumber: return n.doubleValue
        case let d as Double: return d
        case let i as Int: return Double(i)
        case let u as UInt64: return Double(u)
        case let i as Int64: return Double(i)
        case let date as Date: return date.timeIntervalSince1970
        case let s as String: return Double(s)
        default: return nil
        }
    }

    /// Display string for a single cell.
    static func string(_ value: Any?) -> String {
        guard let value else { return "" }
        switch value {
        case let s as String: return s
        case let n as NSNumber: return n.stringValue
        case let b as Bool: return b ? "true" : "false"
        case let d as Date: return d.formatted(date: .abbreviated, time: .standard)
        default: return String(describing: value)
        }
    }

    /// The indices/names of numeric "value" columns — every column after the first for
    /// which at least one row holds a numeric value.
    static func valueColumns(columns: [String], rows: [[Any?]]) -> [(index: Int, name: String)] {
        guard columns.count > 1 else { return [] }
        return (1..<columns.count).compactMap { idx in
            let hasNumeric = rows.contains { row in idx < row.count && numeric(row[idx]) != nil }
            return hasNumeric ? (idx, columns[idx]) : nil
        }
    }

    /// The first numeric value found in a row (used for single-value charts).
    static func firstNumeric(in row: [Any?]) -> Double? {
        for cell in row {
            if let v = numeric(cell) { return v }
        }
        return nil
    }

    /// (category, value) pairs from the first (category) column and the first numeric
    /// value column — used by bar and pie charts.
    static func categoryValues(columns: [String], rows: [[Any?]]) -> [(category: String, value: Double)] {
        guard !columns.isEmpty else { return [] }
        let valueIdx = valueColumns(columns: columns, rows: rows).first?.index ?? (columns.count > 1 ? 1 : 0)
        return rows.compactMap { row in
            guard !row.isEmpty else { return nil }
            let category = string(row.first ?? nil)
            guard valueIdx < row.count, let value = numeric(row[valueIdx]) else { return nil }
            return (category, value)
        }
    }
}
