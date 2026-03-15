import SwiftUI
import SequinsData

/// Content-only traces view (filter bar is in MainWindow's UnifiedFilterBar)
struct TracesContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: TracesViewModel

    var body: some View {
        TracesContentView(viewModel: viewModel)
            .task(id: appState.dataSourceId) {
                if appState.isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    await viewModel.loadSpans(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onChange(of: appState.selectedService) { _, _ in
                if appState.isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onChange(of: appState.timeRangeState.timeRange) { _, newTimeRange in
                if !appState.isLive {
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: newTimeRange
                    )
                }
            }
            .onChange(of: appState.isLive) { _, isLive in
                if isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    viewModel.cancel()
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onAppear {
                if appState.isLive {
                    viewModel.startLiveStream(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                } else {
                    viewModel.refresh(
                        dataSource: appState.dataSource,
                        selectedService: appState.selectedService,
                        timeRange: appState.timeRangeState.timeRange
                    )
                }
            }
            .onDisappear {
                viewModel.cancel()
            }
    }
}

/// Main traces content view with split layout
struct TracesContentView: View {
    @Bindable var viewModel: TracesViewModel

    var body: some View {
        VSplitView {
            // Top: Waterfall timeline
            // Use Color base so VSplitView sees stable size when content switches
            Color(NSColor.controlBackgroundColor)
                .overlay {
                    if let selectedSpan = viewModel.selectedSpan {
                        TraceWaterfallView(
                            rootSpan: selectedSpan,
                            allSpans: viewModel.traceSpans,
                            selectedSpanId: $viewModel.selectedDetailSpanId
                        )
                    } else {
                        Text("Select a trace to view timeline")
                            .font(.title3)
                            .foregroundColor(.secondary)
                    }
                }
                .frame(minHeight: 200, idealHeight: 350, maxHeight: 500)

            // Bottom: Three-panel layout — 30/40/30 split
            // All panels use Color.clear base so NSSplitView sees a stable, constraint-free
            // preferred size regardless of content changes. idealWidth sets the starting split
            // position only — no minWidth to avoid propagating constraints that force the window wider.
            HSplitView {
                // Left: Trace list
                Color.clear
                    .overlay {
                        TraceListPanel(
                            spans: viewModel.filteredSpans,
                            selectedSpanId: viewModel.selectedSpanId,
                            isLoading: viewModel.isLoading,
                            sortBy: viewModel.sortBy,
                            sortOrder: viewModel.sortOrder,
                            onSelect: { viewModel.selectSpan($0) }
                        )
                    }
                    .frame(idealWidth: 220, maxWidth: 400)

                // Middle: selected trace info
                Color.clear
                    .overlay {
                        if let selectedSpan = viewModel.selectedSpan {
                            ScrollView {
                                TraceInfoPanel(span: selectedSpan, traceSpans: viewModel.traceSpans)
                                    .padding()
                            }
                        } else {
                            TraceEmptyStateView()
                        }
                    }
                    .frame(minWidth: 350, idealWidth: 380, maxWidth: .infinity)
                    .background(Color(NSColor.windowBackgroundColor))

                // Right: span details
                Color.clear
                    .overlay {
                        if let detailSpan = viewModel.selectedDetailSpan {
                            let traceStartTime = viewModel.traceSpans.map(\.startTime).min() ?? detailSpan.startTime
                            ScrollView {
                                SpanDetailsPanel(span: detailSpan, traceStartTime: traceStartTime)
                            }
                        } else {
                            VStack {
                                Spacer()
                                Text("Select a span to view details")
                                    .font(.caption)
                                    .foregroundColor(.secondary)
                                Spacer()
                            }
                        }
                    }
                    .frame(idealWidth: 220, maxWidth: 400)
                    .background(Color(NSColor.windowBackgroundColor))
            }
            .frame(minHeight: 200)
        }
    }
}

// MARK: - Supporting Types

// TraceSortBy and SortOrder moved to TracesViewModel

#Preview {
    struct PreviewWrapper: View {
        @State private var viewModel = TracesViewModel()
        var body: some View {
            TracesContentOnly(viewModel: viewModel)
                .environment(AppStateViewModel())
                .frame(width: 1200, height: 800)
        }
    }
    return PreviewWrapper()
}
