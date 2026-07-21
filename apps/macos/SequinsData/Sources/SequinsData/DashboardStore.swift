import Foundation
import SequinsFFI

// MARK: - Dashboard model (mirrors `sequins_metadata::types`)

/// A saved visualization: a SeQL query plus presentation hints. `shape` is a free-form
/// string holding the app's `VizType` raw value (or a `ResponseShape` string); `nil`
/// lets the client auto-select.
public struct SavedVisualization: Codable, Hashable, Sendable {
    public var seql: String
    public var title: String
    public var shape: String?
    /// Optional presentation overrides (units, axis scale, stacking, …). Empty on
    /// dashboards saved before this blob existed.
    public var options: VisualizationOptions

    public init(
        seql: String,
        title: String,
        shape: String? = nil,
        options: VisualizationOptions = VisualizationOptions()
    ) {
        self.seql = seql
        self.title = title
        self.shape = shape
        self.options = options
    }

    private enum CodingKeys: String, CodingKey { case seql, title, shape, options }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.seql = try c.decode(String.self, forKey: .seql)
        self.title = try c.decode(String.self, forKey: .title)
        self.shape = try c.decodeIfPresent(String.self, forKey: .shape)
        self.options =
            try c.decodeIfPresent(VisualizationOptions.self, forKey: .options)
            ?? VisualizationOptions()
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(seql, forKey: .seql)
        try c.encode(title, forKey: .title)
        try c.encodeIfPresent(shape, forKey: .shape)
        if !options.isEmpty {
            try c.encode(options, forKey: .options)
        }
    }

    /// The app-side visualization type, backed by the free-form `shape` string.
    /// `nil` means "auto-select from the query result".
    public var vizType: VizType? {
        get { VizType.from(shapeString: shape) }
        set { shape = newValue?.rawValue }
    }
}

/// Presentation overrides for a `SavedVisualization` (mirrors
/// `sequins_metadata::VisualizationOptions`). Every field is optional; an all-unset
/// value is considered empty and omitted from the wire format.
public struct VisualizationOptions: Codable, Hashable, Sendable {
    /// Value unit shown on axes/labels (e.g. "ms", "bytes", "req/s").
    public var unit: String?
    /// Y-axis scale: "linear" (default) or "log".
    public var yScale: String?
    /// Forced y-axis lower bound.
    public var yMin: Double?
    /// Forced y-axis upper bound.
    public var yMax: Double?
    /// Stack series instead of overlaying them.
    public var stacked: Bool?
    /// Show a series legend.
    public var legend: Bool?
    /// Cap the number of series rendered (top-N by magnitude).
    public var seriesLimit: Int?
    /// Horizontal reference lines drawn across the plot.
    public var thresholds: [VizThreshold]

    public init(
        unit: String? = nil,
        yScale: String? = nil,
        yMin: Double? = nil,
        yMax: Double? = nil,
        stacked: Bool? = nil,
        legend: Bool? = nil,
        seriesLimit: Int? = nil,
        thresholds: [VizThreshold] = []
    ) {
        self.unit = unit
        self.yScale = yScale
        self.yMin = yMin
        self.yMax = yMax
        self.stacked = stacked
        self.legend = legend
        self.seriesLimit = seriesLimit
        self.thresholds = thresholds
    }

    private enum CodingKeys: String, CodingKey {
        case unit, yScale, yMin, yMax, stacked, legend, seriesLimit, thresholds
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.unit = try c.decodeIfPresent(String.self, forKey: .unit)
        self.yScale = try c.decodeIfPresent(String.self, forKey: .yScale)
        self.yMin = try c.decodeIfPresent(Double.self, forKey: .yMin)
        self.yMax = try c.decodeIfPresent(Double.self, forKey: .yMax)
        self.stacked = try c.decodeIfPresent(Bool.self, forKey: .stacked)
        self.legend = try c.decodeIfPresent(Bool.self, forKey: .legend)
        self.seriesLimit = try c.decodeIfPresent(Int.self, forKey: .seriesLimit)
        self.thresholds = try c.decodeIfPresent([VizThreshold].self, forKey: .thresholds) ?? []
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encodeIfPresent(unit, forKey: .unit)
        try c.encodeIfPresent(yScale, forKey: .yScale)
        try c.encodeIfPresent(yMin, forKey: .yMin)
        try c.encodeIfPresent(yMax, forKey: .yMax)
        try c.encodeIfPresent(stacked, forKey: .stacked)
        try c.encodeIfPresent(legend, forKey: .legend)
        try c.encodeIfPresent(seriesLimit, forKey: .seriesLimit)
        if !thresholds.isEmpty {
            try c.encode(thresholds, forKey: .thresholds)
        }
    }

    /// True when no override is set.
    public var isEmpty: Bool {
        unit == nil && yScale == nil && yMin == nil && yMax == nil
            && stacked == nil && legend == nil && seriesLimit == nil && thresholds.isEmpty
    }

    /// Whether the y-axis should use a logarithmic scale.
    public var useLogScale: Bool { yScale?.lowercased() == "log" }

    /// A leading-space unit suffix for axis/tooltip labels (e.g. `" ms"`), or empty.
    public var unitSuffix: String {
        if let u = unit, !u.isEmpty { return " \(u)" }
        return ""
    }
}

/// A horizontal reference line on a chart (mirrors `sequins_metadata::Threshold`).
public struct VizThreshold: Codable, Hashable, Sendable {
    public var value: Double
    public var color: String?
    public var label: String?

    public init(value: Double, color: String? = nil, label: String? = nil) {
        self.value = value
        self.color = color
        self.label = label
    }
}

/// Default dashboard row height, in points (mirrors `DEFAULT_ROW_HEIGHT` in Rust).
public let defaultRowHeight: Double = 280

/// A panel within a row: a visualization and its relative width weight. The row
/// normalizes weights across its panels to fill the full width.
public struct RowPanel: Codable, Hashable, Sendable, Identifiable {
    /// Client-only stable identity for SwiftUI (not part of the wire format).
    public let id: UUID
    public var visualization: SavedVisualization
    public var weight: Double

    public init(visualization: SavedVisualization, weight: Double = 1.0) {
        self.id = UUID()
        self.visualization = visualization
        self.weight = weight
    }

    private enum CodingKeys: String, CodingKey { case visualization, weight }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.id = UUID()
        self.visualization = try c.decode(SavedVisualization.self, forKey: .visualization)
        self.weight = try c.decodeIfPresent(Double.self, forKey: .weight) ?? 1.0
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(visualization, forKey: .visualization)
        try c.encode(weight, forKey: .weight)
    }
}

/// A full-width dashboard row: a height and an ordered set of panels split by weight.
public struct DashboardRow: Codable, Hashable, Sendable, Identifiable {
    public let id: UUID
    public var height: Double
    public var panels: [RowPanel]

    public init(height: Double = defaultRowHeight, panels: [RowPanel] = []) {
        self.id = UUID()
        self.height = height
        self.panels = panels
    }

    private enum CodingKeys: String, CodingKey { case height, panels }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.id = UUID()
        self.height = try c.decodeIfPresent(Double.self, forKey: .height) ?? defaultRowHeight
        self.panels = try c.decodeIfPresent([RowPanel].self, forKey: .panels) ?? []
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(height, forKey: .height)
        try c.encode(panels, forKey: .panels)
    }

    /// Sum of panel weights (>= a small epsilon so division is safe).
    public var totalWeight: Double {
        max(0.0001, panels.reduce(0) { $0 + $1.weight })
    }
}

/// A dashboard: an ordered stack of full-width rows.
public struct Dashboard: Codable, Hashable, Sendable, Identifiable {
    public var id: String
    public var title: String
    public var createdAtNs: UInt64
    public var updatedAtNs: UInt64
    public var rows: [DashboardRow]

    public init(
        id: String = "",
        title: String,
        createdAtNs: UInt64 = 0,
        updatedAtNs: UInt64 = 0,
        rows: [DashboardRow] = []
    ) {
        self.id = id
        self.title = title
        self.createdAtNs = createdAtNs
        self.updatedAtNs = updatedAtNs
        self.rows = rows
    }

    private enum CodingKeys: String, CodingKey {
        case id, title, createdAtNs, updatedAtNs, rows
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try c.decode(String.self, forKey: .id)
        self.title = try c.decode(String.self, forKey: .title)
        self.createdAtNs = try c.decodeIfPresent(UInt64.self, forKey: .createdAtNs) ?? 0
        self.updatedAtNs = try c.decodeIfPresent(UInt64.self, forKey: .updatedAtNs) ?? 0
        self.rows = try c.decodeIfPresent([DashboardRow].self, forKey: .rows) ?? []
    }

    /// Total number of panels across all rows.
    public var panelCount: Int { rows.reduce(0) { $0 + $1.panels.count } }
}

// MARK: - JSON coding (snake_case wire format)

enum DashboardJSON {
    static let encoder: JSONEncoder = {
        let e = JSONEncoder()
        e.keyEncodingStrategy = .convertToSnakeCase
        return e
    }()
    static let decoder: JSONDecoder = {
        let d = JSONDecoder()
        d.keyDecodingStrategy = .convertFromSnakeCase
        return d
    }()
}

// MARK: - DataSource dashboard CRUD (over the normalized dashboard FFI)

extension DataSource {
    /// List all dashboards (local `Storage` or remote Pro daemon, transparently).
    public func listDashboards() throws -> [Dashboard] {
        let json = try callJSON { out, err in
            sequins_dashboard_list(rawPointer, out, err)
        }
        return try DashboardJSON.decoder.decode([Dashboard].self, from: Data(json.utf8))
    }

    /// Fetch a dashboard by id, or `nil` if it does not exist.
    public func getDashboard(id: String) throws -> Dashboard? {
        let json = try id.withCString { idPtr in
            try callJSON { out, err in
                sequins_dashboard_get(rawPointer, idPtr, out, err)
            }
        }
        if json == "null" { return nil }
        return try DashboardJSON.decoder.decode(Dashboard?.self, from: Data(json.utf8))
    }

    /// Create or update a dashboard. Returns the stored dashboard (with id/timestamps).
    @discardableResult
    public func saveDashboard(_ dashboard: Dashboard) throws -> Dashboard {
        let data = try DashboardJSON.encoder.encode(dashboard)
        let jsonIn = String(decoding: data, as: UTF8.self)
        let json = try jsonIn.withCString { inPtr in
            try callJSON { out, err in
                sequins_dashboard_save(rawPointer, inPtr, out, err)
            }
        }
        return try DashboardJSON.decoder.decode(Dashboard.self, from: Data(json.utf8))
    }

    /// Delete a dashboard by id.
    public func deleteDashboard(id: String) throws {
        var errorPtr: UnsafeMutablePointer<CChar>?
        let ok = id.withCString { idPtr in
            sequins_dashboard_delete(rawPointer, idPtr, &errorPtr)
        }
        if !ok {
            throw ffiError(errorPtr, fallback: "failed to delete dashboard")
        }
    }

    // MARK: - Shared JSON out-param helper

    /// Invoke an FFI function shaped `bool f(…, char **out_json, char **error_out)`,
    /// returning the owned JSON string (freed here) or throwing the error string.
    fileprivate func callJSON(
        _ body: (_ out: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>,
                 _ err: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>) -> Bool
    ) throws -> String {
        var outPtr: UnsafeMutablePointer<CChar>?
        var errorPtr: UnsafeMutablePointer<CChar>?
        let ok = body(&outPtr, &errorPtr)
        if !ok {
            throw ffiError(errorPtr, fallback: "dashboard operation failed")
        }
        guard let outPtr else {
            throw SequinsError.ffiError("dashboard operation returned no data")
        }
        let json = String(cString: outPtr)
        sequins_string_free(outPtr)
        return json
    }

    fileprivate func ffiError(
        _ errorPtr: UnsafeMutablePointer<CChar>?, fallback: String
    ) -> SequinsError {
        consumeFFIError(errorPtr, fallback: .ffiError(fallback))
    }
}
