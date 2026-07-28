import Foundation

/// One home for value/label formatting across every chart and table renderer:
/// grouped/compact numbers, bytes, percents, durations (ns→µs/ms/s), timestamps, and a
/// type-aware cell formatter. Consolidates logic previously duplicated in the Metrics
/// charts, `RecordBatchTree`, and `ExploreTableView`.
public enum VizFormat {

    // MARK: - Numbers

    private static let grouped: NumberFormatter = {
        let f = NumberFormatter()
        f.numberStyle = .decimal
        f.maximumFractionDigits = 0
        return f
    }()

    /// Full-precision number for stats/tables/tooltips: whole numbers get thousands
    /// separators ("6,784"), fractions get two decimals.
    public static func number(_ value: Double) -> String {
        if value.rounded() == value, abs(value) < 1e15 {
            return grouped.string(from: NSNumber(value: Int64(value))) ?? "\(Int64(value))"
        }
        return String(format: "%.2f", value)
    }

    /// Compact number for dense axis ticks: k / M / B.
    public static func compact(_ value: Double) -> String {
        let a = abs(value)
        if a >= 1_000_000_000 { return String(format: "%.1fB", value / 1_000_000_000) }
        if a >= 1_000_000 { return String(format: "%.1fM", value / 1_000_000) }
        if a >= 1_000 { return String(format: "%.1fk", value / 1_000) }
        if a >= 1 || a == 0 { return String(format: "%.0f", value) }
        return String(format: "%.2f", value)
    }

    /// Bytes (base 1024) → B/KB/MB/GB/TB.
    public static func bytes(_ value: Double) -> String {
        let a = abs(value)
        if a >= 1_099_511_627_776 { return String(format: "%.1f TB", value / 1_099_511_627_776) }
        if a >= 1_073_741_824 { return String(format: "%.1f GB", value / 1_073_741_824) }
        if a >= 1_048_576 { return String(format: "%.1f MB", value / 1_048_576) }
        if a >= 1_024 { return String(format: "%.0f KB", value / 1_024) }
        return String(format: "%.0f B", value)
    }

    public static func percent(_ value: Double) -> String {
        String(format: "%.1f%%", value)
    }

    /// Duration given in nanoseconds → ns/µs/ms/s (reuses `NanoDuration.formatted`).
    public static func durationNs(_ ns: Double) -> String {
        NanoDuration(nanoseconds: Int64(ns.rounded())).formatted
    }

    // MARK: - Time

    private static let timestampFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        return f
    }()

    private static let clockFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm:ss"
        return f
    }()

    private static let clockShortFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm"
        return f
    }()

    public static func timestamp(_ date: Date) -> String {
        timestampFormatter.string(from: date)
    }

    /// Axis/tick time label. `includeSeconds` for windows under a few minutes.
    public static func axisTime(_ date: Date, includeSeconds: Bool = false) -> String {
        (includeSeconds ? clockFormatter : clockShortFormatter).string(from: date)
    }

    // MARK: - Coercion

    /// Best-effort numeric value of a cell (does not preserve timestamp identity — use
    /// `date(_:column:)` for the time axis).
    public static func numeric(_ value: Any?) -> Double? {
        switch value {
        case let n as NSNumber: return n.doubleValue
        case let d as Double: return d
        case let f as Float: return Double(f)
        case let i as Int: return Double(i)
        case let i as Int64: return Double(i)
        case let u as UInt64: return Double(u)
        case let i as Int32: return Double(i)
        case let u as UInt32: return Double(u)
        case let date as Date: return date.timeIntervalSince1970
        case let s as String: return Double(s)
        default: return nil
        }
    }

    /// A `Date` for a cell that represents a time — either an Arrow `Date`, or an
    /// integer that looks like epoch nanoseconds (>~1e15) when the column name is timey.
    public static func date(_ value: Any?, column: String) -> Date? {
        if let date = value as? Date { return date }
        guard isTimeColumn(column), let n = numeric(value) else { return nil }
        // Heuristic: epoch nanoseconds are ~1.7e18 today; treat large integers as ns.
        if n > 1_000_000_000_000_000 {
            return Date(timeIntervalSince1970: n / 1_000_000_000)
        }
        return nil
    }

    public static func string(_ value: Any?) -> String {
        switch value {
        case .none: return ""
        case let s as String: return s
        case let d as Date: return timestamp(d)
        case let b as Bool: return b ? "true" : "false"
        case let n as NSNumber: return number(n.doubleValue)
        default: return String(describing: value ?? "")
        }
    }

    // MARK: - Type-aware cell

    /// Format a cell for display given its semantic type + column name.
    public static func cell(_ value: Any?, type: NodeTypeLabel, column: String) -> String {
        if value == nil { return "—" }
        switch type {
        case .id, .string, .binary:
            return string(value)
        case .boolean:
            if let b = value as? Bool { return b ? "true" : "false" }
            return string(value)
        case .timestamp:
            if let d = date(value, column: column) ?? value as? Date { return timestamp(d) }
            return string(value)
        case .duration:
            if let n = numeric(value) { return durationNs(n) }
            return string(value)
        case .number:
            if let n = numeric(value) { return number(n) }
            return string(value)
        case .null:
            return "—"
        default:
            return string(value)
        }
    }

    // MARK: - Column-name heuristics (unified)

    public static func isIdColumn(_ name: String) -> Bool {
        let n = name.lowercased()
        return n == "id" || n.hasSuffix("_id") || n == "trace_id" || n == "span_id"
    }

    public static func isDurationColumn(_ name: String) -> Bool {
        let n = name.lowercased()
        return n.contains("duration") || n.hasSuffix("_ns") || n == "elapsed_ns" || n == "latency"
    }

    public static func isTimeColumn(_ name: String) -> Bool {
        let n = name.lowercased()
        return n == "bucket" || n == "time" || n == "timestamp" || n == "ts"
            || n.hasSuffix("_time") || n.hasSuffix("_unix_nano") || n.contains("time_")
    }

    /// A human-readable label for a raw column name (snake/camel → Title Case).
    public static func label(_ raw: String) -> String {
        let spaced = raw
            .replacingOccurrences(of: "_", with: " ")
            .replacingOccurrences(of: "-", with: " ")
        let words = spaced.split(separator: " ").map { word -> String in
            let w = String(word)
            return w.prefix(1).uppercased() + w.dropFirst()
        }
        return words.joined(separator: " ")
    }

    // MARK: - Data-shape helpers (shared across chart renderers)

    /// The numeric "value" columns of a result — the series to plot.
    ///
    /// When column `roles` are known, only **measures** (aggregations/computed) count as
    /// values, so a numeric *dimension* (e.g. `http_status_code`) is never mistaken for a
    /// data series. Without roles it falls back to "every column after the first that
    /// carries numbers." A column is only included if it actually contains numeric data.
    public static func valueColumns(
        columns: [String], rows: [[Any?]], roles: [SeQLColumnRole] = []
    ) -> [(index: Int, name: String)] {
        func carriesNumbers(_ idx: Int) -> Bool {
            rows.contains { $0.count > idx && numeric($0[idx]) != nil }
        }
        if !roles.isEmpty {
            let measures: [(index: Int, name: String)] = (0..<columns.count).compactMap { idx in
                guard idx < roles.count, roles[idx].isMeasure, carriesNumbers(idx) else { return nil }
                return (idx, columns[idx])
            }
            if !measures.isEmpty { return measures }
            // Roles present but no populated measure — fall through to the heuristic.
        }
        guard columns.count > 1 else { return [] }
        return (1..<columns.count).compactMap { idx in
            carriesNumbers(idx) ? (idx, columns[idx]) : nil
        }
    }

    /// Whether the first column is temporal — either typed as a timestamp, or the first
    /// cell is already a `Date` (the backend emits time buckets as Arrow timestamps).
    public static func isTemporalFirstColumn(
        columnTypes: [NodeTypeLabel], rows: [[Any?]]
    ) -> Bool {
        columnTypes.first == .timestamp || rows.first?.first is Date
    }

    /// The first numeric value in the first row (the scalar behind stats/gauges).
    public static func firstNumeric(inFirstRowOf rows: [[Any?]]) -> Double? {
        rows.first?.compactMap { numeric($0) }.first
    }
}
