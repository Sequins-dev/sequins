import Foundation
import SequinsFFI

/// Log severity level
public enum LogSeverity: UInt32 {
    case trace = 1
    case debug = 5
    case info = 9
    case warn = 13
    case error = 17
    case fatal = 21

    /// Display name for the severity level
    public var displayName: String {
        switch self {
        case .trace: return "TRACE"
        case .debug: return "DEBUG"
        case .info: return "INFO"
        case .warn: return "WARN"
        case .error: return "ERROR"
        case .fatal: return "FATAL"
        }
    }
}

/// OpenTelemetry log entry
public struct LogEntry: Identifiable {
    /// Log ID (UUID string)
    public let id: String

    /// Log timestamp
    public let timestamp: Date

    /// Observed timestamp (when the log was collected)
    public let observedTimestamp: Date

    /// Service name
    public let serviceName: String

    /// Severity level
    public let severity: LogSeverity

    /// Log message body
    public let body: String

    /// Log attributes
    public let attributes: [String: AttributeValue]

    /// Trace ID (32-char hex string), nil if not linked to trace
    public let traceId: String?

    /// Span ID (16-char hex string), nil if not linked to span
    public let spanId: String?

    public init(
        id: String,
        timestamp: Date,
        observedTimestamp: Date,
        serviceName: String,
        severity: LogSeverity,
        body: String,
        attributes: [String: AttributeValue],
        traceId: String?,
        spanId: String?
    ) {
        self.id = id
        self.timestamp = timestamp
        self.observedTimestamp = observedTimestamp
        self.serviceName = serviceName
        self.severity = severity
        self.body = body
        self.attributes = attributes
        self.traceId = traceId
        self.spanId = spanId
    }
}

// MARK: - C FFI Conversion

extension LogEntry {
    /// Convert from C log entry struct
    ///
    /// Note: This does NOT take ownership of the C log entry. Caller is responsible for freeing it.
    init(cLogEntry: CLogEntry) {
        self.id = String(cString: cLogEntry.id)
        self.serviceName = String(cString: cLogEntry.service_name)
        self.body = String(cString: cLogEntry.body)

        // Convert timestamps from nanoseconds to Date
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(cLogEntry.timestamp) / 1_000_000_000)
        self.observedTimestamp = Date(timeIntervalSince1970: TimeInterval(cLogEntry.observed_timestamp) / 1_000_000_000)

        // Convert severity - map C enum values to Swift enum
        switch cLogEntry.severity {
        case LogTrace:
            self.severity = .trace
        case LogDebug:
            self.severity = .debug
        case LogInfo:
            self.severity = .info
        case LogWarn:
            self.severity = .warn
        case LogError:
            self.severity = .error
        case LogFatal:
            self.severity = .fatal
        default:
            self.severity = .info
        }

        // Convert attributes
        self.attributes = AttributeValue.convertKeyValueArray(cLogEntry.attributes)

        // Convert optional trace/span IDs
        self.traceId = cLogEntry.trace_id != nil ? String(cString: cLogEntry.trace_id) : nil
        self.spanId = cLogEntry.span_id != nil ? String(cString: cLogEntry.span_id) : nil
    }
}

/// Log query parameters
public struct LogQuery {
    /// Service name filter, nil if not filtering
    public let service: String?

    /// Time range start
    public let startTime: Date

    /// Time range end
    public let endTime: Date

    /// Severity levels to include, nil = all severities
    public let severities: [LogSeverity]?

    /// Full-text search in log body, nil if not searching
    public let search: String?

    /// Limit number of results, nil = no limit
    public let limit: UInt?

    public init(
        service: String? = nil,
        startTime: Date,
        endTime: Date,
        severities: [LogSeverity]? = nil,
        search: String? = nil,
        limit: UInt? = nil
    ) {
        self.service = service
        self.startTime = startTime
        self.endTime = endTime
        self.severities = severities
        self.search = search
        self.limit = limit
    }

    /// Convert to C struct
    /// Note: Caller is responsible for freeing the severities array
    func toCStruct() -> (query: CLogQuery, severitiesArray: [CLogSeverity]) {
        let startNanos = Int64(startTime.timeIntervalSince1970 * 1_000_000_000)
        let endNanos = Int64(endTime.timeIntervalSince1970 * 1_000_000_000)

        // Convert Swift severities to C enum array
        var cSeverities: [CLogSeverity] = []
        if let severities = severities {
            cSeverities = severities.map { severity in
                switch severity {
                case .trace: return LogTrace
                case .debug: return LogDebug
                case .info: return LogInfo
                case .warn: return LogWarn
                case .error: return LogError
                case .fatal: return LogFatal
                }
            }
        }

        let query = CLogQuery(
            service: service?.withCString { strdup($0) },
            start_time: startNanos,
            end_time: endNanos,
            severities: cSeverities.isEmpty ? nil : UnsafePointer(cSeverities),
            severities_len: UInt(cSeverities.count),
            search: search?.withCString { strdup($0) },
            limit: UInt(limit ?? 0)
        )

        return (query, cSeverities)
    }
}
