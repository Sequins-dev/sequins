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

// MARK: - SeQL time-scope rewriting

extension TimeRange {
    /// SeQL scan-position signal keywords (the mandatory leading token of a query).
    private static let signalKeywords: Set<String> = [
        "spans", "span_links", "logs", "datapoints", "histograms", "metrics",
        "samples", "traces", "profiles", "stacks", "frames", "mappings",
        "resources", "scopes",
    ]

    /// The SeQL scan-level time-scope clause for this range:
    /// `last <dur>` for relative, `between(<startNs>, <endNs>)` for absolute.
    public var seqlTimeScope: String {
        switch self {
        case .relative(let duration):
            return "last \(TimeRange.durationClause(duration))"
        case .absolute(let start, let end):
            let startNs = Int64(start.timeIntervalSince1970 * 1_000_000_000)
            let endNs = Int64(end.timeIntervalSince1970 * 1_000_000_000)
            return "between(\(startNs), \(endNs))"
        }
    }

    /// Format a duration as a single SeQL duration literal (`<n><unit>`), picking the
    /// largest exact unit. SeQL has no compound durations, so a non-exact duration
    /// falls back to whole seconds.
    private static func durationClause(_ seconds: TimeInterval) -> String {
        let total = max(1, Int(seconds.rounded()))
        if total % 86_400 == 0 { return "\(total / 86_400)d" }
        if total % 3_600 == 0 { return "\(total / 3_600)h" }
        if total % 60 == 0 { return "\(total / 60)m" }
        return "\(total)s"
    }

    /// Rewrite `seql` so its leading `<signal> <time-scope>` uses this range.
    ///
    /// SeQL requires a time-scope immediately after the signal keyword, so we replace
    /// that token. Returns the query unchanged when it doesn't begin with a recognized
    /// `<signal> <time-scope>` (e.g. the `signal(id)` shorthand), so a query we can't
    /// safely rewrite is never corrupted.
    public func applied(to seql: String) -> String {
        let full = Substring(seql)
        let leadingWS = full.prefix { $0.isWhitespace }
        var rest = full.dropFirst(leadingWS.count)

        let signal = rest.prefix { $0.isLetter || $0 == "_" }
        guard !signal.isEmpty, TimeRange.signalKeywords.contains(String(signal)) else {
            return seql
        }
        rest = rest.dropFirst(signal.count)

        // `signal(id)` shorthand carries no time-scope — leave untouched.
        if rest.first == "(" { return seql }

        let midWS = rest.prefix { $0.isWhitespace }
        guard !midWS.isEmpty else { return seql }
        rest = rest.dropFirst(midWS.count)

        guard let tail = TimeRange.dropTimeScope(rest) else { return seql }
        return "\(leadingWS)\(signal)\(midWS)\(seqlTimeScope)\(tail)"
    }

    /// If `s` begins with a SeQL time-scope token, return the remainder after it;
    /// otherwise `nil`.
    private static func dropTimeScope(_ s: Substring) -> Substring? {
        for keyword in ["today", "yesterday"] where s.hasPrefix(keyword) {
            let tail = s.dropFirst(keyword.count)
            if tail.isEmpty || tail.first!.isWhitespace || tail.first! == "|" {
                return tail
            }
        }

        if s.hasPrefix("between") {
            let t = s.dropFirst("between".count).drop { $0.isWhitespace }
            guard t.first == "(", let close = t.firstIndex(of: ")") else { return nil }
            return t[t.index(after: close)...]
        }

        if s.hasPrefix("last") {
            var t = s.dropFirst("last".count)
            guard let boundary = t.first, boundary.isWhitespace else { return nil }
            t = t.drop { $0.isWhitespace }
            let digits = t.prefix { $0.isNumber }
            guard !digits.isEmpty else { return nil }
            t = t.dropFirst(digits.count)
            if t.hasPrefix("ms") { return t.dropFirst(2) }
            guard let unit = t.first, "smhd".contains(unit) else { return nil }
            return t.dropFirst(1)
        }

        return nil
    }
}
