import Foundation
import SequinsFFI

/// A single frame in a call stack
public struct StackFrame {
    /// Function name
    public let functionName: String

    /// Source file path, nil if unknown
    public let file: String?

    /// Source line number, 0 if unknown
    public let line: UInt32

    /// Module or package name, nil if unknown
    public let module: String?

    public init(functionName: String, file: String?, line: UInt32, module: String?) {
        self.functionName = functionName
        self.file = file
        self.line = line
        self.module = module
    }
}

/// A single profile sample with resolved stack frames
public struct ProfileSample: Identifiable {
    /// Unique ID for SwiftUI identity
    public let id: UUID = UUID()

    /// Profile ID this sample originated from
    public let profileId: String

    /// Timestamp when the sample was captured
    public let timestamp: Date

    /// Service that produced this sample
    public let serviceName: String

    /// Type of profile
    public let profileType: ProfileType

    /// Value type label (e.g., "cpu", "alloc_objects")
    public let valueType: String

    /// Sample value in units specified by valueType
    public let value: Int64

    /// Call stack — leaf (innermost) to root (outermost)
    public let stack: [StackFrame]

    /// Associated trace ID (32-char hex string), nil if not linked
    public let traceId: String?

    public init(
        profileId: String,
        timestamp: Date,
        serviceName: String,
        profileType: ProfileType,
        valueType: String,
        value: Int64,
        stack: [StackFrame],
        traceId: String?
    ) {
        self.profileId = profileId
        self.timestamp = timestamp
        self.serviceName = serviceName
        self.profileType = profileType
        self.valueType = valueType
        self.value = value
        self.stack = stack
        self.traceId = traceId
    }
}

/// Profile sample query parameters
public struct ProfileSampleQuery {
    /// Service name filter, nil if not filtering
    public let service: String?

    /// Profile type filter (e.g., "cpu", "memory"), nil if not filtering
    public let profileTypeFilter: String?

    /// Value type filter (e.g., "cpu"), nil if not filtering
    public let valueTypeFilter: String?

    /// Time range start
    public let startTime: Date

    /// Time range end
    public let endTime: Date

    /// Trace ID filter (32-char hex string), nil if not filtering
    public let traceId: String?

    /// Limit number of results, nil = no limit
    public let limit: UInt?

    public init(
        service: String? = nil,
        profileTypeFilter: String? = nil,
        valueTypeFilter: String? = nil,
        startTime: Date,
        endTime: Date,
        traceId: String? = nil,
        limit: UInt? = nil
    ) {
        self.service = service
        self.profileTypeFilter = profileTypeFilter
        self.valueTypeFilter = valueTypeFilter
        self.startTime = startTime
        self.endTime = endTime
        self.traceId = traceId
        self.limit = limit
    }

    /// Convert to C struct
    func toCStruct() -> CProfileSampleQuery {
        let startNanos = Int64(startTime.timeIntervalSince1970 * 1_000_000_000)
        let endNanos = Int64(endTime.timeIntervalSince1970 * 1_000_000_000)

        return CProfileSampleQuery(
            service: service?.withCString { strdup($0) },
            profile_type: profileTypeFilter?.withCString { strdup($0) },
            value_type: valueTypeFilter?.withCString { strdup($0) },
            start_time: startNanos,
            end_time: endNanos,
            trace_id: traceId?.withCString { strdup($0) },
            limit: limit ?? 0
        )
    }
}

// MARK: - C FFI Conversion

extension StackFrame {
    /// Convert from C stack frame struct (does not take ownership)
    init(cFrame: CStackFrame) {
        self.functionName = cFrame.function_name != nil ? String(cString: cFrame.function_name) : ""
        self.file = cFrame.file != nil ? String(cString: cFrame.file) : nil
        self.line = cFrame.line
        self.module = cFrame.module != nil ? String(cString: cFrame.module) : nil
    }
}

extension ProfileSample {
    /// Convert from C profile sample struct (does not take ownership)
    init(cSample: CProfileSample) {
        self.profileId = cSample.profile_id != nil ? String(cString: cSample.profile_id) : ""
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(cSample.timestamp) / 1_000_000_000)
        self.serviceName = cSample.service_name != nil ? String(cString: cSample.service_name) : ""
        self.profileType = ProfileType(rawValue: cSample.profile_type.rawValue) ?? .other
        self.valueType = cSample.value_type != nil ? String(cString: cSample.value_type) : ""
        self.value = cSample.value
        self.traceId = cSample.trace_id != nil ? String(cString: cSample.trace_id) : nil

        // Convert stack frames
        var frames: [StackFrame] = []
        if let data = cSample.stack.data {
            for i in 0..<Int(cSample.stack.len) {
                frames.append(StackFrame(cFrame: data[i]))
            }
        }
        self.stack = frames
    }
}
