import Foundation

/// App-side visualization type — a superset of the Rust-inferred ``ResponseShape``.
///
/// A visualization descriptor may pin a `VizType` (via a model hint or the user's
/// choice); when unset, ``autoSelect(shape:columns:rows:)`` derives a sensible default
/// from the query's `ResponseShape` and result columns. Rows are mapped to the chosen
/// chart regardless of the Rust shape, so the same result can be shown several ways.
///
/// The raw value is what persists in `SavedVisualization.shape`.
public enum VizType: String, CaseIterable, Codable, Sendable {
    case line
    case area
    case bar
    case stackedBar
    case pie
    case gauge
    case stat
    case table
    case heatmap
    case trace

    /// Human-readable label for pickers.
    public var displayName: String {
        switch self {
        case .line: return "Line"
        case .area: return "Area"
        case .bar: return "Bar"
        case .stackedBar: return "Stacked Bar"
        case .pie: return "Pie"
        case .gauge: return "Gauge"
        case .stat: return "Stat"
        case .table: return "Table"
        case .heatmap: return "Heatmap"
        case .trace: return "Trace"
        }
    }

    /// SF Symbol name for pickers/menus.
    public var systemImage: String {
        switch self {
        case .line: return "chart.xyaxis.line"
        case .area: return "chart.line.uptrend.xyaxis"
        case .bar: return "chart.bar"
        case .stackedBar: return "chart.bar.doc.horizontal"
        case .pie: return "chart.pie"
        case .gauge: return "gauge.medium"
        case .stat: return "number"
        case .table: return "tablecells"
        case .heatmap: return "square.grid.3x3.fill"
        case .trace: return "arrow.triangle.branch"
        }
    }

    /// Parse a stored `shape` string into a `VizType`, accepting both `VizType` raw
    /// values and legacy `ResponseShape` strings (e.g. "timeseries", "trace_tree").
    public static func from(shapeString: String?) -> VizType? {
        guard let raw = shapeString, !raw.isEmpty else { return nil }
        // Exact raw value (preserves camelCase like "stackedBar")…
        if let direct = VizType(rawValue: raw) { return direct }
        let lower = raw.lowercased()
        // …then a case-insensitive match against the raw values…
        if let ci = VizType.allCases.first(where: { $0.rawValue.lowercased() == lower }) {
            return ci
        }
        // …then legacy `ResponseShape` string aliases.
        switch lower {
        case "timeseries", "time_series": return .line
        case "scalar": return .stat
        case "trace_tree", "trace_timeline", "tracetimeline", "tracetree": return .trace
        case "pattern_groups", "patterngroups": return .table
        default: return nil
        }
    }

    /// Whether this type plots numeric measures (so an absence of any measure/numeric
    /// column means it can't render and should fall back to a table).
    public var plotsMeasures: Bool {
        switch self {
        case .line, .area, .bar, .stackedBar, .pie, .gauge, .heatmap: return true
        case .stat, .table, .trace: return false
        }
    }

    /// Choose a default `VizType` from the Rust-inferred shape, the result columns, and
    /// (when available) their semantic roles.
    ///
    /// Roles make table-shaped results reliable: a grouped aggregation like
    /// `group by { http_route, http_status_code } { count() }` has two *dimensions* and
    /// one *measure* — that's a heatmap (dim × dim → value), not a broken line/bar that
    /// mistakes `http_status_code` for a data series. When roles are absent we fall back
    /// to a conservative column-count heuristic; either way an unplottable result becomes
    /// a table rather than an empty chart.
    public static func autoSelect(
        shape: ResponseShape, columns: [String], rows: [[Any?]], roles: [SeQLColumnRole] = []
    ) -> VizType {
        switch shape {
        case .timeSeries: return .line
        case .scalar: return .stat
        case .traceTimeline, .traceTree: return .trace
        case .heatmap: return .heatmap
        case .patternGroups: return .table
        case .table:
            if !roles.isEmpty {
                let dimensions = roles.filter { $0.isDimension }.count
                let measures = roles.filter { $0.isMeasure }.count
                if measures == 0 { return .table }
                if dimensions >= 2 && measures == 1 { return .heatmap }
                if dimensions == 1 && rows.count > 1 && rows.count <= 24 { return .bar }
                return .table
            }
            // No roles: a compact two-column (category + one value) result reads well as
            // a bar chart; anything wider or larger stays a table.
            if columns.count == 2, rows.count <= 24, rows.count > 1 {
                return .bar
            }
            return .table
        }
    }
}
