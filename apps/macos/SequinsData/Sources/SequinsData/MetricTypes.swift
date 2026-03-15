import Foundation
import SequinsFFI

/// Metric type
public enum MetricType: UInt32 {
    case gauge = 0
    case counter = 1
    case histogram = 2
    case summary = 3
}

/// Grouping pattern for related metrics
public enum GroupingPattern: UInt32 {
    case statisticalVariants = 0  // min, max, mean, p50, p90, p99
    case histogramFamily = 1      // _bucket, _count, _sum
    case attributeStreams = 2     // Same metric, different attribute values
    case custom = 3               // User-defined groupings
    case namespace = 4            // Common prefix (e.g., v8js.memory.heap.*)
}

/// Recommended visualization type for a metric group
public enum VisualizationType: UInt32 {
    case multiLineChart = 0   // Multiple lines on same Y-axis
    case boxPlot = 1          // For statistical distributions
    case histogram = 2        // For histogram families
    case rangeChart = 3       // For min/max ranges
    case stackedArea = 4      // For additive metrics
    case separateCharts = 5   // Fallback: render separately
}

/// A group of related metrics that should be visualized together
public struct MetricGroup: Identifiable {
    /// Unique ID for this group (base_name + service_name)
    public var id: String { "\(baseName).\(serviceName)" }

    /// Base name of the metric group (e.g., "nodejs.eventloop.delay")
    public let baseName: String

    /// Names of all metrics in this group
    public let metricNames: [String]

    /// The detected grouping pattern
    public let pattern: GroupingPattern

    /// Service name this group belongs to
    public let serviceName: String

    /// Metric type (if all metrics share the same type)
    public let metricType: MetricType?

    /// Shared unit for all metrics in the group
    public let unit: String

    /// Recommended visualization type for this group
    public let visualization: VisualizationType

    public init(
        baseName: String,
        metricNames: [String],
        pattern: GroupingPattern,
        serviceName: String,
        metricType: MetricType?,
        unit: String,
        visualization: VisualizationType
    ) {
        self.baseName = baseName
        self.metricNames = metricNames
        self.pattern = pattern
        self.serviceName = serviceName
        self.metricType = metricType
        self.unit = unit
        self.visualization = visualization
    }
}

/// OpenTelemetry metric definition
public struct Metric: Identifiable {
    /// Metric ID (UUID hex string)
    public let id: String

    /// Metric name (e.g., "http.server.duration")
    public let name: String

    /// Human-readable description
    public let description: String

    /// Unit of measurement (e.g., "ms", "bytes", "1")
    public let unit: String

    /// Type of metric
    public let metricType: MetricType

    /// Service that produces this metric
    public let serviceName: String

    /// Whether this metric was generated internally (e.g., health metrics)
    /// vs reported via OTLP. Defaults to false for OTLP-reported metrics.
    public let isGenerated: Bool

    public init(
        id: String,
        name: String,
        description: String,
        unit: String,
        metricType: MetricType,
        serviceName: String,
        isGenerated: Bool = false
    ) {
        self.id = id
        self.name = name
        self.description = description
        self.unit = unit
        self.metricType = metricType
        self.serviceName = serviceName
        self.isGenerated = isGenerated
    }

    /// Whether this is a health metric (name starts with "sequins.health.")
    public var isHealthMetric: Bool {
        name.hasPrefix("sequins.health.")
    }
}

/// Metric data point (time-series value)
public struct MetricDataPoint: Identifiable {
    /// Unique ID for this data point
    public var id: String { "\(metricId)-\(timestamp.timeIntervalSince1970)" }

    /// Metric ID this point belongs to
    public let metricId: String

    /// Timestamp of this data point
    public let timestamp: Date

    /// Numeric value
    public let value: Double

    /// Additional attributes for this data point
    public let attributes: [String: String]

    public init(
        metricId: String,
        timestamp: Date,
        value: Double,
        attributes: [String: String]
    ) {
        self.metricId = metricId
        self.timestamp = timestamp
        self.value = value
        self.attributes = attributes
    }
}

/// Metric data point query parameters
public struct MetricDataPointQuery {
    /// Metric ID to fetch data points for (UUID hex string)
    public let metricId: String

    /// Time range start
    public let startTime: Date

    /// Time range end
    public let endTime: Date

    /// Bucket duration for downsampling (nil = no downsampling)
    public let bucketDuration: TimeInterval?

    public init(
        metricId: String,
        startTime: Date,
        endTime: Date,
        bucketDuration: TimeInterval? = nil
    ) {
        self.metricId = metricId
        self.startTime = startTime
        self.endTime = endTime
        self.bucketDuration = bucketDuration
    }

    /// Convert to C struct
    func toCStruct() -> CMetricDataPointQuery {
        let startNanos = Int64(startTime.timeIntervalSince1970 * 1_000_000_000)
        let endNanos = Int64(endTime.timeIntervalSince1970 * 1_000_000_000)
        let bucketNanos = bucketDuration.map { Int64($0 * 1_000_000_000) } ?? 0

        return CMetricDataPointQuery(
            metric_id: metricId.withCString { strdup($0) },
            start_time: startNanos,
            end_time: endNanos,
            bucket_duration_nanos: bucketNanos
        )
    }
}

/// Metric query parameters
public struct MetricQuery {
    /// Metric name filter, nil if not filtering
    public let name: String?

    /// Service name filter, nil if not filtering
    public let service: String?

    /// Time range start
    public let startTime: Date

    /// Time range end
    public let endTime: Date

    /// Limit number of results, nil = no limit
    public let limit: UInt?

    public init(
        name: String? = nil,
        service: String? = nil,
        startTime: Date,
        endTime: Date,
        limit: UInt? = nil
    ) {
        self.name = name
        self.service = service
        self.startTime = startTime
        self.endTime = endTime
        self.limit = limit
    }

    /// Convert to C struct
    func toCStruct() -> CMetricQuery {
        let startNanos = Int64(startTime.timeIntervalSince1970 * 1_000_000_000)
        let endNanos = Int64(endTime.timeIntervalSince1970 * 1_000_000_000)

        return CMetricQuery(
            name: name?.withCString { strdup($0) },
            service: service?.withCString { strdup($0) },
            start_time: startNanos,
            end_time: endNanos,
            limit: limit ?? 0
        )
    }
}

// MARK: - C FFI Conversion

extension Metric {
    /// Convert from C metric struct
    ///
    /// Note: This does NOT take ownership of the C metric. Caller is responsible for freeing it.
    init(cMetric: CMetric) {
        self.id = String(cString: cMetric.id)
        self.name = String(cString: cMetric.name)
        self.description = String(cString: cMetric.description)
        self.unit = String(cString: cMetric.unit)
        self.serviceName = String(cString: cMetric.service_name)
        self.metricType = MetricType(rawValue: cMetric.metric_type.rawValue) ?? .gauge
        self.isGenerated = cMetric.is_generated
    }
}

extension MetricDataPoint {
    /// Convert from C metric data point struct
    ///
    /// Note: This does NOT take ownership of the C struct. Caller is responsible for freeing it.
    init(cDataPoint: CMetricDataPoint) {
        self.metricId = String(cString: cDataPoint.metric_id)
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(cDataPoint.timestamp) / 1_000_000_000)
        self.value = cDataPoint.value

        // Convert parallel arrays to dictionary
        var attrs: [String: String] = [:]
        let keyCount = cDataPoint.attribute_keys.len
        let valueCount = cDataPoint.attribute_values.len

        if keyCount > 0, keyCount == valueCount,
           let keys = cDataPoint.attribute_keys.data,
           let values = cDataPoint.attribute_values.data {
            for i in 0..<keyCount {
                if let keyPtr = keys.advanced(by: Int(i)).pointee,
                   let valuePtr = values.advanced(by: Int(i)).pointee {
                    let key = String(cString: keyPtr)
                    let value = String(cString: valuePtr)
                    attrs[key] = value
                }
            }
        }
        self.attributes = attrs
    }
}

extension MetricGroup {
    /// Convert from C metric group struct
    ///
    /// Note: This does NOT take ownership of the C struct. Caller is responsible for freeing it.
    init(cMetricGroup: CMetricGroup) {
        self.baseName = String(cString: cMetricGroup.base_name)
        self.serviceName = String(cString: cMetricGroup.service_name)
        self.unit = String(cString: cMetricGroup.unit)
        self.pattern = GroupingPattern(rawValue: cMetricGroup.pattern.rawValue) ?? .statisticalVariants
        self.visualization = VisualizationType(rawValue: cMetricGroup.visualization.rawValue) ?? .multiLineChart

        // Convert metric_type with optional handling
        if cMetricGroup.has_metric_type {
            self.metricType = MetricType(rawValue: cMetricGroup.metric_type.rawValue)
        } else {
            self.metricType = nil
        }

        // Convert metric_names CStringArray to [String]
        var names: [String] = []
        let count = cMetricGroup.metric_names.len
        if count > 0, let data = cMetricGroup.metric_names.data {
            for i in 0..<count {
                if let namePtr = data.advanced(by: Int(i)).pointee {
                    names.append(String(cString: namePtr))
                }
            }
        }
        self.metricNames = names
    }
}
