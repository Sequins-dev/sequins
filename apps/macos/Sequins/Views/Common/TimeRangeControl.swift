import SwiftUI

/// Time range control with live/paused modes and custom date picker
struct TimeRangeControl: View {
    @Environment(AppStateViewModel.self) private var appState
    @State private var showingCustomPicker = false

    var body: some View {
        let timeState = appState.timeRangeState

        HStack(spacing: 8) {
            // Duration/Range selector
            if timeState.isLive {
                // Live mode: show duration menu
                liveDurationMenu(timeState: timeState)
            } else {
                // Paused mode: show range menu with custom option
                pausedRangeMenu(timeState: timeState)
            }
        }
        .sheet(isPresented: $showingCustomPicker) {
            CustomTimeRangePicker(timeState: timeState, isPresented: $showingCustomPicker)
        }
    }

    @ViewBuilder
    private func liveDurationMenu(timeState: TimeRangeState) -> some View {
        Menu {
            ForEach(LiveDuration.allCases) { duration in
                Button(action: {
                    timeState.setLiveDuration(duration)
                }) {
                    HStack {
                        Text(duration.displayName)
                        if timeState.liveDuration == duration {
                            Image(systemName: "checkmark")
                        }
                    }
                }
            }
        } label: {
            HStack(spacing: 4) {
                Image(systemName: "clock")
                    .font(.caption)
                Text(timeState.liveDuration.displayName)
                    .font(.caption)
                Image(systemName: "chevron.down")
                    .font(.caption2)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.quaternary)
            .cornerRadius(4)
        }
        .menuStyle(.borderlessButton)
        .fixedSize()
    }

    @ViewBuilder
    private func pausedRangeMenu(timeState: TimeRangeState) -> some View {
        Menu {
            // Preset durations
            ForEach(PausedDuration.allCases.filter { $0 != .custom }) { duration in
                Button(action: {
                    timeState.setPausedDuration(duration)
                }) {
                    HStack {
                        Text(duration.displayName)
                        if timeState.pausedDuration == duration {
                            Image(systemName: "checkmark")
                        }
                    }
                }
            }

            Divider()

            // Custom range option
            Button(action: {
                showingCustomPicker = true
            }) {
                HStack {
                    Text("Custom range...")
                    if timeState.pausedDuration == .custom {
                        Image(systemName: "checkmark")
                    }
                }
            }
        } label: {
            HStack(spacing: 4) {
                Image(systemName: "clock")
                    .font(.caption)
                Text(pausedRangeLabel(timeState: timeState))
                    .font(.caption)
                    .lineLimit(1)
                Image(systemName: "chevron.down")
                    .font(.caption2)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.quaternary)
            .cornerRadius(4)
        }
        .menuStyle(.borderlessButton)
        .fixedSize()
    }

    private func pausedRangeLabel(timeState: TimeRangeState) -> String {
        if timeState.pausedDuration == .custom {
            return formatShortRange(start: timeState.pausedCustomStart, end: timeState.pausedCustomEnd)
        } else {
            return timeState.pausedDuration.displayName
        }
    }

    private func formatShortRange(start: Date, end: Date) -> String {
        let formatter = DateFormatter()
        let calendar = Calendar.current

        if calendar.isDate(start, inSameDayAs: end) {
            // Same day: show "Dec 3, 10:00-14:00"
            formatter.dateFormat = "MMM d"
            let dateStr = formatter.string(from: start)
            formatter.dateFormat = "HH:mm"
            return "\(dateStr), \(formatter.string(from: start))-\(formatter.string(from: end))"
        } else {
            // Different days: show "Dec 1-3"
            formatter.dateFormat = "MMM d"
            return "\(formatter.string(from: start)) - \(formatter.string(from: end))"
        }
    }
}

/// Custom time range picker sheet
struct CustomTimeRangePicker: View {
    let timeState: TimeRangeState
    @Binding var isPresented: Bool

    @State private var startDate: Date
    @State private var endDate: Date

    init(timeState: TimeRangeState, isPresented: Binding<Bool>) {
        self.timeState = timeState
        self._isPresented = isPresented
        // Initialize with current custom range or reasonable defaults
        self._startDate = State(initialValue: timeState.pausedCustomStart)
        self._endDate = State(initialValue: timeState.pausedCustomEnd)
    }

    var body: some View {
        VStack(spacing: 16) {
            Text("Custom Time Range")
                .font(.headline)

            HStack(spacing: 20) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Start")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    DatePicker("", selection: $startDate)
                        .labelsHidden()
                        .datePickerStyle(.field)
                }

                VStack(alignment: .leading, spacing: 8) {
                    Text("End")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    DatePicker("", selection: $endDate)
                        .labelsHidden()
                        .datePickerStyle(.field)
                }
            }

            // Quick presets
            HStack(spacing: 8) {
                Text("Quick:")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                ForEach(["1h", "6h", "24h", "7d"], id: \.self) { preset in
                    Button(preset) {
                        applyQuickPreset(preset)
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                }
            }

            Divider()

            HStack {
                Button("Cancel") {
                    isPresented = false
                }
                .keyboardShortcut(.cancelAction)

                Spacer()

                Button("Apply") {
                    timeState.setCustomRange(start: startDate, end: endDate)
                    isPresented = false
                }
                .keyboardShortcut(.defaultAction)
                .disabled(startDate >= endDate)
            }
        }
        .padding(20)
        .frame(width: 400)
    }

    private func applyQuickPreset(_ preset: String) {
        let now = Date()
        endDate = now

        switch preset {
        case "1h":
            startDate = now.addingTimeInterval(-3600)
        case "6h":
            startDate = now.addingTimeInterval(-6 * 3600)
        case "24h":
            startDate = now.addingTimeInterval(-24 * 3600)
        case "7d":
            startDate = now.addingTimeInterval(-7 * 24 * 3600)
        default:
            break
        }
    }
}

#Preview("TimeRangeControl - Live") {
    let appState = AppStateViewModel()
    appState.timeRangeState.isLive = true
    return TimeRangeControl()
        .environment(appState)
        .padding()
}

#Preview("TimeRangeControl - Paused") {
    let appState = AppStateViewModel()
    appState.timeRangeState.isLive = false
    return TimeRangeControl()
        .environment(appState)
        .padding()
}

#Preview("Custom Picker") {
    @Previewable @State var isPresented = true
    let timeState = TimeRangeState()
    return CustomTimeRangePicker(timeState: timeState, isPresented: $isPresented)
}
