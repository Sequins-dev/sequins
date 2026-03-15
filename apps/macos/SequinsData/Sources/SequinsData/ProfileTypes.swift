import Foundation
import SequinsFFI

/// Profile type
public enum ProfileType: UInt32 {
    case cpu = 0
    case memory = 1
    case goroutine = 2
    case other = 3
}

/// OpenTelemetry profile definition
public struct Profile: Identifiable {
    /// Profile ID (UUID hex string)
    public let id: String

    /// Timestamp of the profile
    public let timestamp: Date

    /// Service that produced this profile
    public let serviceName: String

    /// Type of profile
    public let profileType: ProfileType

    /// Sample type (e.g., "cpu", "alloc_objects")
    public let sampleType: String

    /// Sample unit (e.g., "nanoseconds", "bytes")
    public let sampleUnit: String

    /// Encoded pprof data
    public let data: Data

    /// Associated trace ID (32-char hex string), nil if not linked
    public let traceId: String?

    public init(
        id: String,
        timestamp: Date,
        serviceName: String,
        profileType: ProfileType,
        sampleType: String,
        sampleUnit: String,
        data: Data,
        traceId: String?
    ) {
        self.id = id
        self.timestamp = timestamp
        self.serviceName = serviceName
        self.profileType = profileType
        self.sampleType = sampleType
        self.sampleUnit = sampleUnit
        self.data = data
        self.traceId = traceId
    }
}

/// Profile query parameters
public struct ProfileQuery {
    /// Service name filter, nil if not filtering
    public let service: String?

    /// Profile type filter, nil if not filtering
    public let profileTypeFilter: String?

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
        startTime: Date,
        endTime: Date,
        traceId: String? = nil,
        limit: UInt? = nil
    ) {
        self.service = service
        self.profileTypeFilter = profileTypeFilter
        self.startTime = startTime
        self.endTime = endTime
        self.traceId = traceId
        self.limit = limit
    }

    /// Convert to C struct
    func toCStruct() -> CProfileQuery {
        let startNanos = Int64(startTime.timeIntervalSince1970 * 1_000_000_000)
        let endNanos = Int64(endTime.timeIntervalSince1970 * 1_000_000_000)

        return CProfileQuery(
            service: service?.withCString { strdup($0) },
            profile_type: profileTypeFilter?.withCString { strdup($0) },
            start_time: startNanos,
            end_time: endNanos,
            trace_id: traceId?.withCString { strdup($0) },
            limit: limit ?? 0
        )
    }
}

// MARK: - C FFI Conversion

extension Profile {
    /// Convert from C profile struct
    ///
    /// Note: This does NOT take ownership of the C profile. Caller is responsible for freeing it.
    init(cProfile: CProfile) {
        self.id = String(cString: cProfile.id)
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(cProfile.timestamp) / 1_000_000_000)
        self.serviceName = String(cString: cProfile.service_name)
        self.profileType = ProfileType(rawValue: cProfile.profile_type.rawValue) ?? .other
        self.sampleType = String(cString: cProfile.sample_type)
        self.sampleUnit = String(cString: cProfile.sample_unit)

        // Convert CByteArray to Data
        if cProfile.data.len > 0, cProfile.data.data != nil {
            self.data = Data(bytes: cProfile.data.data, count: Int(cProfile.data.len))
        } else {
            self.data = Data()
        }

        // Convert trace_id (may be null)
        if cProfile.trace_id != nil {
            self.traceId = String(cString: cProfile.trace_id)
        } else {
            self.traceId = nil
        }
    }
}
