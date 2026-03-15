import SwiftUI

/// Timeline ruler showing time markers at the bottom of the waterfall view
struct TimelineRuler: View {
    let traceDuration: TimeInterval

    /// Calculate nice tick interval that gives ~5-10 ticks
    private var tickInterval: TimeInterval {
        guard traceDuration > 0 else { return 1 }

        // Target ~5 ticks across the timeline
        let targetTicks = 5.0
        let rawInterval = traceDuration / targetTicks

        // Round to a "nice" number (1, 2, 5 × 10^n)
        let magnitude = pow(10, floor(log10(rawInterval)))
        let normalized = rawInterval / magnitude

        let niceMultiplier: Double
        if normalized <= 1 {
            niceMultiplier = 1
        } else if normalized <= 2 {
            niceMultiplier = 2
        } else if normalized <= 5 {
            niceMultiplier = 5
        } else {
            niceMultiplier = 10
        }

        return niceMultiplier * magnitude
    }

    private var tickCount: Int {
        guard traceDuration > 0 else { return 0 }
        return Int(floor(traceDuration / tickInterval)) + 1
    }

    var body: some View {
        GeometryReader { geometry in
            let totalWidth = geometry.size.width

            ZStack(alignment: .top) {
                // Tick marks
                ForEach(0 ..< tickCount, id: \.self) { index in
                    let time = TimeInterval(index) * tickInterval
                    let position = traceDuration > 0 ? (time / traceDuration) * totalWidth : 0

                    VStack(spacing: 2) {
                        Rectangle()
                            .fill(Color.secondary.opacity(0.5))
                            .frame(width: 1, height: 8)

                        Text(formatTime(time))
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    }
                    .position(x: position, y: 14)
                }
            }
        }
        .frame(height: 28)
    }

    private func formatTime(_ time: TimeInterval) -> String {
        if time == 0 {
            return "0"
        } else if time < 0.000001 {
            // Nanoseconds (< 1 microsecond)
            let ns = time * 1_000_000_000
            if ns < 10 {
                return String(format: "%.1fns", ns)
            } else {
                return String(format: "%.0fns", ns)
            }
        } else if time < 0.001 {
            // Microseconds (< 1 millisecond)
            let us = time * 1_000_000
            if us < 10 {
                return String(format: "%.1fus", us)
            } else {
                return String(format: "%.0fus", us)
            }
        } else if time < 1 {
            // Milliseconds (< 1 second)
            let ms = time * 1000
            if ms < 10 {
                return String(format: "%.1fms", ms)
            } else {
                return String(format: "%.0fms", ms)
            }
        } else if time < 60 {
            return String(format: "%.1fs", time)
        } else {
            let minutes = Int(time / 60)
            let seconds = Int(time.truncatingRemainder(dividingBy: 60))
            return seconds > 0 ? "\(minutes)m\(seconds)s" : "\(minutes)m"
        }
    }
}

#Preview("TimelineRuler - 500ns") {
    VStack {
        Spacer()
        TimelineRuler(traceDuration: 0.0000005) // 500 nanoseconds
            .padding(.horizontal)
    }
    .frame(width: 800, height: 100)
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("TimelineRuler - 100us") {
    VStack {
        Spacer()
        TimelineRuler(traceDuration: 0.0001) // 100 microseconds
            .padding(.horizontal)
    }
    .frame(width: 800, height: 100)
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("TimelineRuler - 500us") {
    VStack {
        Spacer()
        TimelineRuler(traceDuration: 0.0005) // 500 microseconds
            .padding(.horizontal)
    }
    .frame(width: 800, height: 100)
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("TimelineRuler - 5ms") {
    VStack {
        Spacer()
        TimelineRuler(traceDuration: 0.005) // 5 milliseconds
            .padding(.horizontal)
    }
    .frame(width: 800, height: 100)
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("TimelineRuler - 500ms") {
    VStack {
        Spacer()
        TimelineRuler(traceDuration: 0.5)
            .padding(.horizontal)
    }
    .frame(width: 800, height: 100)
    .background(Color(NSColor.controlBackgroundColor))
}

#Preview("TimelineRuler - 2s") {
    VStack {
        Spacer()
        TimelineRuler(traceDuration: 2.0)
            .padding(.horizontal)
    }
    .frame(width: 800, height: 100)
    .background(Color(NSColor.controlBackgroundColor))
}
