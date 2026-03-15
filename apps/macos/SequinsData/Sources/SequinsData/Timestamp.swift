import Foundation

/// A high-precision timestamp representing nanoseconds since Unix epoch.
///
/// Unlike `Date` which uses `Double` (limited to ~microsecond precision at current epoch times),
/// `Timestamp` stores the full nanosecond value as `Int64`, preserving OpenTelemetry's native precision.
public struct Timestamp: Hashable, Comparable, Sendable {
    /// Nanoseconds since Unix epoch (January 1, 1970 00:00:00 UTC)
    public let nanoseconds: Int64

    /// Create a timestamp from nanoseconds since Unix epoch
    public init(nanoseconds: Int64) {
        self.nanoseconds = nanoseconds
    }

    /// Create a timestamp from a Date (loses sub-microsecond precision)
    public init(_ date: Date) {
        self.nanoseconds = Int64(date.timeIntervalSince1970 * 1_000_000_000)
    }

    /// The timestamp as a Date (may lose sub-microsecond precision)
    public var date: Date {
        Date(timeIntervalSince1970: TimeInterval(nanoseconds) / 1_000_000_000)
    }

    /// Seconds since Unix epoch
    public var seconds: Int64 {
        nanoseconds / 1_000_000_000
    }

    /// The sub-second nanoseconds component (0-999,999,999)
    public var subSecondNanoseconds: Int64 {
        nanoseconds % 1_000_000_000
    }

    // MARK: - Comparable

    public static func < (lhs: Timestamp, rhs: Timestamp) -> Bool {
        lhs.nanoseconds < rhs.nanoseconds
    }

    // MARK: - Arithmetic

    /// Compute the duration between two timestamps
    public static func - (lhs: Timestamp, rhs: Timestamp) -> NanoDuration {
        NanoDuration(nanoseconds: lhs.nanoseconds - rhs.nanoseconds)
    }

    /// Add a duration to a timestamp
    public static func + (lhs: Timestamp, rhs: NanoDuration) -> Timestamp {
        Timestamp(nanoseconds: lhs.nanoseconds + rhs.nanoseconds)
    }

    // MARK: - Factory Methods

    /// Current time (uses Date, so limited precision)
    public static var now: Timestamp {
        Timestamp(Date())
    }

    /// Unix epoch (January 1, 1970 00:00:00 UTC)
    public static let epoch = Timestamp(nanoseconds: 0)
}

/// A high-precision duration in nanoseconds.
///
/// Unlike `TimeInterval` which is a `Double`, `NanoDuration` preserves full nanosecond precision
/// using `Int64` storage.
public struct NanoDuration: Hashable, Comparable, Sendable {
    /// The duration in nanoseconds (can be negative for time differences)
    public let nanoseconds: Int64

    /// Create a duration from nanoseconds
    public init(nanoseconds: Int64) {
        self.nanoseconds = nanoseconds
    }

    /// Create a duration from a TimeInterval (loses sub-microsecond precision)
    public init(_ timeInterval: TimeInterval) {
        self.nanoseconds = Int64(timeInterval * 1_000_000_000)
    }

    /// The duration as a TimeInterval (may lose precision for very small durations)
    public var timeInterval: TimeInterval {
        TimeInterval(nanoseconds) / 1_000_000_000
    }

    // MARK: - Comparable

    public static func < (lhs: NanoDuration, rhs: NanoDuration) -> Bool {
        lhs.nanoseconds < rhs.nanoseconds
    }

    // MARK: - Arithmetic

    public static func + (lhs: NanoDuration, rhs: NanoDuration) -> NanoDuration {
        NanoDuration(nanoseconds: lhs.nanoseconds + rhs.nanoseconds)
    }

    public static func - (lhs: NanoDuration, rhs: NanoDuration) -> NanoDuration {
        NanoDuration(nanoseconds: lhs.nanoseconds - rhs.nanoseconds)
    }

    // MARK: - Factory Methods

    public static func nanoseconds(_ ns: Int64) -> NanoDuration {
        NanoDuration(nanoseconds: ns)
    }

    public static func microseconds(_ us: Int64) -> NanoDuration {
        NanoDuration(nanoseconds: us * 1_000)
    }

    public static func milliseconds(_ ms: Int64) -> NanoDuration {
        NanoDuration(nanoseconds: ms * 1_000_000)
    }

    public static func seconds(_ s: Int64) -> NanoDuration {
        NanoDuration(nanoseconds: s * 1_000_000_000)
    }

    /// Zero duration
    public static let zero = NanoDuration(nanoseconds: 0)

    // MARK: - Formatting

    /// Format the duration with appropriate unit (ns, µs, ms, s)
    public var formatted: String {
        let absNanos = abs(nanoseconds)
        let sign = nanoseconds < 0 ? "-" : ""

        if absNanos < 1_000 {
            return "\(sign)\(absNanos) ns"
        } else if absNanos < 1_000_000 {
            return String(format: "%@%.2f µs", sign, Double(absNanos) / 1_000)
        } else if absNanos < 1_000_000_000 {
            return String(format: "%@%.2f ms", sign, Double(absNanos) / 1_000_000)
        } else {
            return String(format: "%@%.2f s", sign, Double(absNanos) / 1_000_000_000)
        }
    }
}
