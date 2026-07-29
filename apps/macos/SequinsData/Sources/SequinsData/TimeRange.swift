import Foundation

/// Represents a time range for queries - either static or sliding
///
/// Use `.absolute(start:end:)` for historical queries that should not update.
/// Use `.relative(duration:)` for live/sliding window queries that continuously advance.
public enum TimeRange: Equatable, Sendable {
    /// Fixed time window - won't change
    case absolute(start: Date, end: Date)

    /// Sliding window relative to now - continuously advances
    case relative(duration: TimeInterval)

    /// Computed current time bounds
    public var bounds: (start: Date, end: Date) {
        switch self {
        case .absolute(let start, let end):
            return (start, end)
        case .relative(let duration):
            let now = Date()
            return (now.addingTimeInterval(-duration), now)
        }
    }

    /// Whether this is a live/streaming query
    public var isLive: Bool {
        if case .relative = self { return true }
        return false
    }

    /// The `(range_kind, range_a_ns, range_b_ns)` triple passed to
    /// `sequins_seql_query[_live]`. Supplying this to a query overrides its inline
    /// time scope, so a saved visualization runs against the selected range.
    /// kind 1 = relative sliding window (`a` = duration ns); 2 = absolute
    /// (`a` = start ns, `b` = end ns).
    public var ffiScalars: (kind: UInt32, a: UInt64, b: UInt64) {
        switch self {
        case .relative(let duration):
            return (1, UInt64(max(0, duration) * 1_000_000_000), 0)
        case .absolute(let start, let end):
            let s = UInt64(max(0, start.timeIntervalSince1970) * 1_000_000_000)
            let e = UInt64(max(0, end.timeIntervalSince1970) * 1_000_000_000)
            return (2, s, e)
        }
    }

    /// The duration of this time range
    public var duration: TimeInterval {
        switch self {
        case .absolute(let start, let end):
            return end.timeIntervalSince(start)
        case .relative(let duration):
            return duration
        }
    }
}

// MARK: - Convenience Constructors

extension TimeRange {
    /// Create a sliding window for the last N minutes
    public static func last(minutes: Int) -> TimeRange {
        .relative(duration: TimeInterval(minutes * 60))
    }

    /// Create a sliding window for the last N hours
    public static func last(hours: Int) -> TimeRange {
        .relative(duration: TimeInterval(hours * 3600))
    }

    /// Create a sliding window for the last N seconds
    public static func last(seconds: Int) -> TimeRange {
        .relative(duration: TimeInterval(seconds))
    }

    /// Create a fixed time window between two dates
    public static func between(_ start: Date, and end: Date) -> TimeRange {
        .absolute(start: start, end: end)
    }

    /// Create a fixed time window from start date until now
    public static func since(_ start: Date) -> TimeRange {
        .absolute(start: start, end: Date())
    }

    /// All time - from epoch to now
    public static var allTime: TimeRange {
        .absolute(start: Date(timeIntervalSince1970: 0), end: Date())
    }
}

