import Foundation
import SequinsFFI

/// Span status
public enum SpanStatus: UInt32 {
    case unset = 0
    case ok = 1
    case error = 2
}

/// Span kind
public enum SpanKind: UInt32 {
    case unspecified = 0
    case `internal` = 1
    case server = 2
    case client = 3
    case producer = 4
    case consumer = 5
}

/// OpenTelemetry attribute value
public enum AttributeValue {
    case string(String)
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case stringArray([String])
    case boolArray([Bool])
    case intArray([Int64])
    case doubleArray([Double])
}

/// Span event
public struct SpanEvent {
    /// Event timestamp (high precision)
    public let timestamp: Timestamp

    /// Event name
    public let name: String

    /// Event attributes
    public let attributes: [String: AttributeValue]

    public init(timestamp: Timestamp, name: String, attributes: [String: AttributeValue]) {
        self.timestamp = timestamp
        self.name = name
        self.attributes = attributes
    }

    /// Convenience initializer using Date (loses sub-microsecond precision)
    public init(timestamp: Date, name: String, attributes: [String: AttributeValue]) {
        self.init(timestamp: Timestamp(timestamp), name: name, attributes: attributes)
    }
}

/// OpenTelemetry span
public struct Span: Identifiable {
    /// Trace ID (32-char hex string)
    public let traceId: String

    /// Span ID (16-char hex string)
    public let spanId: String

    /// Identifiable conformance - uses spanId as unique identifier
    public var id: String { spanId }

    /// Parent span ID (16-char hex string), nil if root span
    public let parentSpanId: String?

    /// Service name
    public let serviceName: String

    /// Operation name
    public let operationName: String

    /// Start time (high precision)
    public let startTime: Timestamp

    /// End time (high precision)
    public let endTime: Timestamp

    /// Duration (high precision)
    public let duration: NanoDuration

    /// Span attributes
    public let attributes: [String: AttributeValue]

    /// Span events
    public let events: [SpanEvent]

    /// Span status
    public let status: SpanStatus

    /// Span kind
    public let spanKind: SpanKind

    public init(
        traceId: String,
        spanId: String,
        parentSpanId: String?,
        serviceName: String,
        operationName: String,
        startTime: Timestamp,
        endTime: Timestamp,
        duration: NanoDuration,
        attributes: [String: AttributeValue],
        events: [SpanEvent],
        status: SpanStatus,
        spanKind: SpanKind
    ) {
        self.traceId = traceId
        self.spanId = spanId
        self.parentSpanId = parentSpanId
        self.serviceName = serviceName
        self.operationName = operationName
        self.startTime = startTime
        self.endTime = endTime
        self.duration = duration
        self.attributes = attributes
        self.events = events
        self.status = status
        self.spanKind = spanKind
    }

    /// Convenience initializer using Date and TimeInterval (loses precision)
    public init(
        traceId: String,
        spanId: String,
        parentSpanId: String?,
        serviceName: String,
        operationName: String,
        startTime: Date,
        endTime: Date,
        duration: TimeInterval,
        attributes: [String: AttributeValue],
        events: [SpanEvent],
        status: SpanStatus,
        spanKind: SpanKind
    ) {
        self.init(
            traceId: traceId,
            spanId: spanId,
            parentSpanId: parentSpanId,
            serviceName: serviceName,
            operationName: operationName,
            startTime: Timestamp(startTime),
            endTime: Timestamp(endTime),
            duration: NanoDuration(duration),
            attributes: attributes,
            events: events,
            status: status,
            spanKind: spanKind
        )
    }
}

// MARK: - C FFI Conversion

extension Span {
    /// Convert from C span struct
    ///
    /// Note: This does NOT take ownership of the C span. Caller is responsible for freeing it.
    init(cSpan: CSpan) {
        self.traceId = String(cString: cSpan.trace_id)
        self.spanId = String(cString: cSpan.span_id)
        self.parentSpanId = cSpan.parent_span_id != nil ? String(cString: cSpan.parent_span_id) : nil
        self.serviceName = String(cString: cSpan.service_name)
        self.operationName = String(cString: cSpan.operation_name)

        // Store high-precision timestamps
        self.startTime = Timestamp(nanoseconds: cSpan.start_time)
        self.endTime = Timestamp(nanoseconds: cSpan.end_time)
        self.duration = NanoDuration(nanoseconds: cSpan.duration)

        // Convert attributes
        self.attributes = AttributeValue.convertKeyValueArray(cSpan.attributes)

        // Convert events
        var events: [SpanEvent] = []
        if cSpan.events.len > 0, let eventsData = cSpan.events.data {
            for i in 0..<cSpan.events.len {
                let cEvent = eventsData.advanced(by: Int(i)).pointee
                let timestamp = Timestamp(nanoseconds: cEvent.timestamp)
                let name = String(cString: cEvent.name)
                let attrs = AttributeValue.convertKeyValueArray(cEvent.attributes)
                events.append(SpanEvent(timestamp: timestamp, name: name, attributes: attrs))
            }
        }
        self.events = events

        // Convert status and kind
        self.status = SpanStatus(rawValue: cSpan.status.rawValue) ?? .unset
        self.spanKind = SpanKind(rawValue: cSpan.span_kind.rawValue) ?? .unspecified
    }
}

extension AttributeValue {
    /// Convert C key-value array to Swift dictionary
    static func convertKeyValueArray(_ array: CKeyValueArray) -> [String: AttributeValue] {
        var result: [String: AttributeValue] = [:]

        guard array.len > 0, let data = array.data else {
            return result
        }

        for i in 0..<array.len {
            let kv = data.advanced(by: Int(i)).pointee
            let key = String(cString: kv.key)
            let value = convertAttributeValue(kv.value)
            result[key] = value
        }

        return result
    }

    /// Convert C attribute value to Swift enum
    static func convertAttributeValue(_ value: CAttributeValue) -> AttributeValue {
        switch value.tag {
        case String:
            return .string(Swift.String(cString: value.value.string_val))
        case Bool:
            return .bool(value.value.bool_val)
        case Int:
            return .int(value.value.int_val)
        case Double:
            return .double(value.value.double_val)
        case StringArray:
            let array = value.value.string_array
            var strings: [Swift.String] = []
            if array.len > 0, let data = array.data {
                for i in 0..<array.len {
                    let ptr = data.advanced(by: Int(i)).pointee
                    strings.append(Swift.String(cString: ptr!))
                }
            }
            return .stringArray(strings)
        case BoolArray:
            let array = value.value.bool_array
            var bools: [Swift.Bool] = []
            if array.len > 0, let data = array.data {
                for i in 0..<array.len {
                    bools.append(data.advanced(by: Int(i)).pointee)
                }
            }
            return .boolArray(bools)
        case IntArray:
            let array = value.value.int_array
            var ints: [Int64] = []
            if array.len > 0, let data = array.data {
                for i in 0..<array.len {
                    ints.append(data.advanced(by: Int(i)).pointee)
                }
            }
            return .intArray(ints)
        case DoubleArray:
            let array = value.value.double_array
            var doubles: [Swift.Double] = []
            if array.len > 0, let data = array.data {
                for i in 0..<array.len {
                    doubles.append(data.advanced(by: Int(i)).pointee)
                }
            }
            return .doubleArray(doubles)
        default:
            // Fallback to string for unknown types
            return .string("")
        }
    }
}
