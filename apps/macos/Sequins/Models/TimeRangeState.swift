import Foundation
import SwiftUI
import SequinsData

// Re-export TimeRange from SequinsData for convenience
typealias TimeRange = SequinsData.TimeRange

/// Duration presets for live mode (relative to now)
enum LiveDuration: String, CaseIterable, Identifiable {
    case last1Min = "1m"
    case last5Min = "5m"
    case last15Min = "15m"
    case last30Min = "30m"
    case lastHour = "1h"
    case last6Hours = "6h"

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .last1Min: return "1 min"
        case .last5Min: return "5 min"
        case .last15Min: return "15 min"
        case .last30Min: return "30 min"
        case .lastHour: return "1 hour"
        case .last6Hours: return "6 hours"
        }
    }

    var seconds: TimeInterval {
        switch self {
        case .last1Min: return 60
        case .last5Min: return 5 * 60
        case .last15Min: return 15 * 60
        case .last30Min: return 30 * 60
        case .lastHour: return 3600
        case .last6Hours: return 6 * 3600
        }
    }

    /// Convert to SequinsData.TimeRange
    var timeRange: TimeRange {
        .relative(duration: seconds)
    }
}

/// Duration presets for paused mode (can also use custom range)
enum PausedDuration: String, CaseIterable, Identifiable {
    case last15Min = "15m"
    case lastHour = "1h"
    case last6Hours = "6h"
    case last24Hours = "24h"
    case last7Days = "7d"
    case custom = "custom"

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .last15Min: return "Last 15 min"
        case .lastHour: return "Last hour"
        case .last6Hours: return "Last 6 hours"
        case .last24Hours: return "Last 24 hours"
        case .last7Days: return "Last 7 days"
        case .custom: return "Custom range"
        }
    }

    var seconds: TimeInterval? {
        switch self {
        case .last15Min: return 15 * 60
        case .lastHour: return 3600
        case .last6Hours: return 6 * 3600
        case .last24Hours: return 24 * 3600
        case .last7Days: return 7 * 24 * 3600
        case .custom: return nil
        }
    }

    /// Convert to SequinsData.TimeRange (uses current time as end for preset durations)
    func timeRange(customStart: Date? = nil, customEnd: Date? = nil) -> TimeRange {
        if self == .custom, let start = customStart, let end = customEnd {
            return .absolute(start: start, end: end)
        } else if let seconds = seconds {
            // For paused mode presets, create absolute range from "now" when selected
            let end = customEnd ?? Date()
            let start = end.addingTimeInterval(-seconds)
            return .absolute(start: start, end: end)
        } else {
            // Fallback for custom without dates
            return .absolute(start: Date().addingTimeInterval(-3600), end: Date())
        }
    }
}

/// Manages time range state with separate settings for live and paused modes
///
/// This class wraps `SequinsData.TimeRange` and provides UI-friendly presets
/// and state management for live vs paused modes.
@Observable
final class TimeRangeState {
    // MARK: - Core TimeRange

    /// The underlying TimeRange value - this is the source of truth
    private(set) var timeRange: TimeRange = .relative(duration: 5 * 60) // Default: last 5 min, live

    // MARK: - UI State

    /// Whether we're in live mode (continuously updating) or paused (historical)
    var isLive: Bool {
        get { timeRange.isLive }
        set {
            if newValue {
                // Switch to live mode with current live duration
                timeRange = liveDuration.timeRange
            } else {
                // Switch to paused mode - snapshot current time range as absolute
                let bounds = timeRange.bounds
                timeRange = .absolute(start: bounds.start, end: bounds.end)
                pausedDuration = .custom
            }
        }
    }

    // MARK: - Live Mode Settings

    /// Duration preset for live mode
    var liveDuration: LiveDuration = .last5Min {
        didSet {
            if isLive {
                timeRange = liveDuration.timeRange
            }
        }
    }

    // MARK: - Paused Mode Settings

    /// Duration preset for paused mode (reflects what preset was selected, or .custom)
    var pausedDuration: PausedDuration = .lastHour

    /// Custom start time for paused mode (used when pausedDuration is .custom)
    var pausedCustomStart: Date {
        get {
            if case .absolute(let start, _) = timeRange {
                return start
            }
            return timeRange.bounds.start
        }
        set {
            if case .absolute(_, let end) = timeRange {
                timeRange = .absolute(start: newValue, end: end)
                pausedDuration = .custom
            }
        }
    }

    /// Custom end time for paused mode (used when pausedDuration is .custom)
    var pausedCustomEnd: Date {
        get {
            if case .absolute(_, let end) = timeRange {
                return end
            }
            return timeRange.bounds.end
        }
        set {
            if case .absolute(let start, _) = timeRange {
                timeRange = .absolute(start: start, end: newValue)
                pausedDuration = .custom
            }
        }
    }

    // MARK: - Computed Properties (delegating to TimeRange)

    /// The effective start time based on current mode
    var startTime: Date {
        timeRange.bounds.start
    }

    /// The effective end time based on current mode
    var endTime: Date {
        timeRange.bounds.end
    }

    /// Human-readable description of the current time range
    var displayDescription: String {
        if isLive {
            return "Live - \(liveDuration.displayName)"
        } else {
            if pausedDuration == .custom {
                return formatTimeRange(start: startTime, end: endTime)
            } else {
                return pausedDuration.displayName
            }
        }
    }

    /// Short label for the time range (for compact UI)
    var shortLabel: String {
        if isLive {
            return liveDuration.displayName
        } else {
            if pausedDuration == .custom {
                return "Custom"
            } else {
                return pausedDuration.displayName.replacingOccurrences(of: "Last ", with: "")
            }
        }
    }

    // MARK: - Actions

    /// Switch to live mode
    func goLive() {
        timeRange = liveDuration.timeRange
    }

    /// Switch to paused mode, optionally setting a specific time range
    func pause(start: Date? = nil, end: Date? = nil) {
        if let start = start, let end = end {
            setCustomRange(start: start, end: end)
        } else {
            // Snapshot current bounds as absolute
            let bounds = timeRange.bounds
            timeRange = .absolute(start: bounds.start, end: bounds.end)
            pausedDuration = .custom
        }
    }

    /// Set a custom time range (automatically switches to paused mode)
    func setCustomRange(start: Date, end: Date) {
        timeRange = .absolute(start: start, end: end)
        pausedDuration = .custom
    }

    /// Set live duration
    func setLiveDuration(_ duration: LiveDuration) {
        liveDuration = duration
        timeRange = duration.timeRange
    }

    /// Set paused duration (relative to now)
    func setPausedDuration(_ duration: PausedDuration) {
        pausedDuration = duration
        if duration != .custom {
            // Create absolute range from now
            let now = Date()
            if let seconds = duration.seconds {
                timeRange = .absolute(start: now.addingTimeInterval(-seconds), end: now)
            }
        }
    }

    /// Refresh the time range for queries (call before querying in live mode)
    /// Returns the effective start and end times
    func refreshedTimeRange() -> (start: Date, end: Date) {
        return timeRange.bounds
    }

    // MARK: - Formatting Helpers

    private func formatTimeRange(start: Date, end: Date) -> String {
        let formatter = DateFormatter()

        // Check if same day
        let calendar = Calendar.current
        let sameDay = calendar.isDate(start, inSameDayAs: end)

        if sameDay {
            formatter.dateFormat = "MMM d, HH:mm"
            let dateStr = formatter.string(from: start)
            formatter.dateFormat = "HH:mm"
            let startTime = formatter.string(from: start)
            let endTime = formatter.string(from: end)
            return "\(dateStr.prefix(dateStr.count - 6)) \(startTime) - \(endTime)"
        } else {
            formatter.dateFormat = "MMM d HH:mm"
            return "\(formatter.string(from: start)) - \(formatter.string(from: end))"
        }
    }
}
