import SwiftUI
import AppKit

/// Terminal-style log display view with drag-to-select time filtering
struct LogTerminalView: View {
    let logs: [LogEntry]
    let isLoading: Bool
    var sortNewestFirst: Bool = true
    var onTimeSelection: ((Date, Date) -> Void)?

    @State private var hoveredLogId: String?
    @State private var knownLogIds: Set<String> = []

    // Scroll offset (content offset Y), updated via onScrollGeometryChange
    @State private var scrollOffset: CGFloat = 0

    // Drag selection state
    @State private var isDragging = false
    @State private var dragStartIndex: Int?
    @State private var dragEndIndex: Int?
    @State private var escapeMonitor: Any?

    // Estimated collapsed row height: padding(4+4) + monospaced body line height ~17px
    private let rowHeight: CGFloat = 25
    // LazyVStack padding inset
    private let listPadding: CGFloat = 16

    private var selectedIndexRange: ClosedRange<Int>? {
        guard isDragging,
              let s = dragStartIndex,
              let e = dragEndIndex else { return nil }
        let lo = max(0, min(s, e))
        let hi = min(logs.count - 1, max(s, e))
        guard lo <= hi else { return nil }
        return lo...hi
    }

    // Set built once per render from the index range — O(1) per-row lookup in ForEach
    private var selectedLogIds: Set<String> {
        guard let range = selectedIndexRange else { return [] }
        return Set(logs[range].map { $0.id })
    }

    var body: some View {
        GeometryReader { _ in
            ScrollViewReader { scrollProxy in
                ScrollView {
                    if logs.isEmpty && !isLoading {
                        VStack(spacing: 16) {
                            Image(systemName: "doc.text")
                                .font(.system(size: 48))
                                .foregroundColor(.gray)

                            Text("No Logs")
                                .font(.title2)
                                .foregroundColor(.gray)

                            Text("Logs will appear here when your application sends log data.")
                                .font(.body)
                                .foregroundColor(.gray.opacity(0.7))
                                .multilineTextAlignment(.center)
                                .frame(maxWidth: 300)
                        }
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .padding(.top, 100)
                    } else {
                        LazyVStack(alignment: .leading, spacing: 0) {
                            ForEach(logs) { log in
                                LogEntryRow(
                                    log: log,
                                    isHovered: hoveredLogId == log.id && !isDragging,
                                    isDragSelected: selectedLogIds.contains(log.id),
                                    isNew: !knownLogIds.contains(log.id)
                                )
                                .id(log.id)
                                .onHover { isHovered in
                                    if !isDragging {
                                        hoveredLogId = isHovered ? log.id : nil
                                    }
                                }
                                .onAppear {
                                    knownLogIds.insert(log.id)
                                }
                            }
                        }
                        .padding(listPadding)
                    }
                }
                .onScrollGeometryChange(for: CGFloat.self) { geo in
                    geo.contentOffset.y
                } action: { _, newOffset in
                    scrollOffset = newOffset
                }
                .defaultScrollAnchor(sortNewestFirst ? .top : .bottom)
                .coordinateSpace(name: "logScrollView")
                .background(Color(NSColor.textBackgroundColor))
                .font(.system(.body, design: .monospaced))
                .overlay(alignment: .center) {
                    if isLoading && logs.isEmpty {
                        ProgressView("Loading logs...")
                            .padding()
                            .background(Color(NSColor.windowBackgroundColor))
                            .cornerRadius(8)
                    }
                }
                .overlay(alignment: .topLeading) {
                    if isDragging, let range = selectedIndexRange {
                        let count = range.upperBound - range.lowerBound + 1
                        VStack(spacing: 4) {
                            HStack(spacing: 4) {
                                Image(systemName: "clock")
                                Text("Selecting \(count) log\(count == 1 ? "" : "s")")
                            }
                            Text("Release to filter • Esc to cancel")
                                .font(.caption2)
                                .foregroundColor(.white.opacity(0.8))
                        }
                        .font(.caption)
                        .padding(8)
                        .background(Color.blue.opacity(0.9))
                        .foregroundColor(.white)
                        .cornerRadius(6)
                        .padding(8)
                    }
                }
                .gesture(
                    DragGesture(minimumDistance: 10)
                        .onChanged { value in
                            if !isDragging {
                                isDragging = true
                                dragStartIndex = indexAt(y: value.startLocation.y)
                                startEscapeMonitor()
                            }
                            dragEndIndex = indexAt(y: value.location.y)
                        }
                        .onEnded { _ in
                            stopEscapeMonitor()

                            if let range = selectedIndexRange {
                                let selectedLogs = Array(logs[range])
                                if let firstLog = selectedLogs.min(by: { $0.timestamp < $1.timestamp }),
                                   let lastLog = selectedLogs.max(by: { $0.timestamp < $1.timestamp }) {
                                    let startTime = firstLog.timestamp.addingTimeInterval(-0.001)
                                    let endTime = lastLog.timestamp.addingTimeInterval(0.001)
                                    onTimeSelection?(startTime, endTime)
                                }
                            }

                            resetDragState()
                        }
                )
                .onChange(of: logs.last?.id) { _, newLastId in
                    if !sortNewestFirst, let id = newLastId {
                        withAnimation {
                            scrollProxy.scrollTo(id, anchor: .bottom)
                        }
                    }
                }
                .onChange(of: sortNewestFirst) { _, newest in
                    if newest, let id = logs.first?.id {
                        scrollProxy.scrollTo(id, anchor: .top)
                    } else if !newest, let id = logs.last?.id {
                        scrollProxy.scrollTo(id, anchor: .bottom)
                    }
                }
            }
        }
    }

    /// Estimate which log index falls at the given Y coordinate in the scroll view's frame.
    private func indexAt(y: CGFloat) -> Int? {
        let contentY = scrollOffset + y - listPadding
        guard contentY >= 0 else { return 0 }
        let idx = Int(contentY / rowHeight)
        guard idx < logs.count else { return logs.count - 1 }
        return idx
    }

    private func resetDragState() {
        isDragging = false
        dragStartIndex = nil
        dragEndIndex = nil
    }

    private func startEscapeMonitor() {
        escapeMonitor = NSEvent.addLocalMonitorForEvents(matching: .keyDown) { event in
            if event.keyCode == 53 {
                DispatchQueue.main.async {
                    self.stopEscapeMonitor()
                    self.resetDragState()
                }
                return nil
            }
            return event
        }
    }

    private func stopEscapeMonitor() {
        if let monitor = escapeMonitor {
            NSEvent.removeMonitor(monitor)
            escapeMonitor = nil
        }
    }
}

#Preview("LogTerminalView - With Logs") {
    let sampleLogs: [LogEntry] = (0..<20).map { index in
        let severities: [LogSeverity] = [.error, .warn, .info, .debug, .trace]
        let messages = [
            "Request received from client",
            "Processing payment for order #12345",
            "Database connection established",
            "Cache miss for key: user_session_abc123",
            "Rate limit exceeded for IP: 192.168.1.100"
        ]
        let components = ["http", "db", "cache", "auth", "api"]
        return LogEntry(
            id: index,
            timestamp: Date().addingTimeInterval(-Double(index * 5)),
            severity: severities[index % severities.count],
            message: messages[index % messages.count],
            component: components[index % components.count],
            metadata: [:]
        )
    }

    return LogTerminalView(
        logs: sampleLogs,
        isLoading: false,
        onTimeSelection: { start, end in
            print("Selected time range: \(start) - \(end)")
        }
    )
    .frame(width: 1000, height: 400)
}

#Preview("LogTerminalView - Empty") {
    LogTerminalView(
        logs: [],
        isLoading: false
    )
    .frame(width: 1000, height: 400)
}

#Preview("LogTerminalView - Loading") {
    LogTerminalView(
        logs: [],
        isLoading: true
    )
    .frame(width: 1000, height: 400)
}

#Preview("LogTerminalView - Interactive Drag Selection") {
    struct PreviewWrapper: View {
        @State private var selectedRange: String = "Drag to select logs"

        private var sampleLogs: [LogEntry] {
            (0..<50).map { index in
                let severities: [LogSeverity] = [.error, .warn, .info, .debug, .trace]
                let messages = [
                    "Request received from client",
                    "Processing payment for order #12345",
                    "Database connection established",
                    "Cache miss for key: user_session_abc123",
                    "Rate limit exceeded for IP: 192.168.1.100"
                ]
                let components = ["http", "db", "cache", "auth", "api"]
                return LogEntry(
                    id: index,
                    timestamp: Date().addingTimeInterval(-Double(index * 5)),
                    severity: severities[index % severities.count],
                    message: messages[index % messages.count],
                    component: components[index % components.count],
                    metadata: [:]
                )
            }
        }

        var body: some View {
            VStack(spacing: 0) {
                Text(selectedRange)
                    .font(.caption)
                    .padding(8)
                    .frame(maxWidth: .infinity)
                    .background(Color(NSColor.controlBackgroundColor))

                LogTerminalView(
                    logs: sampleLogs,
                    isLoading: false,
                    onTimeSelection: { start, end in
                        let formatter = DateFormatter()
                        formatter.dateFormat = "HH:mm:ss.SSS"
                        selectedRange = "Selected: \(formatter.string(from: start)) - \(formatter.string(from: end))"
                    }
                )
            }
            .frame(width: 1000, height: 500)
        }
    }
    return PreviewWrapper()
}
