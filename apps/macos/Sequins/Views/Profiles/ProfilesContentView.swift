import SwiftUI
import AppKit
import SequinsData

/// Content-only profiles view (filter bar is in MainWindow's UnifiedFilterBar)
struct ProfilesContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: ProfilesViewModel

    @State private var hoveredNodeId: String?
    @State private var selectedNodeId: String?
    @State private var selectedStackTrace: [FlamegraphNode] = []

    var body: some View {
        Group {
            if let feed = viewModel.feed {
                ProfileGraphContent(
                    feed: feed,
                    viewModel: viewModel,
                    hoveredNodeId: $hoveredNodeId,
                    selectedNodeId: $selectedNodeId,
                    selectedStackTrace: $selectedStackTrace
                )
            } else {
                ProfileEmptyState(isLoading: true)
            }
        }
        .task(id: appState.dataSourceId) {
            if appState.isLive {
                viewModel.startLiveStream(
                    dataSource: appState.dataSource,
                    selectedService: appState.selectedService,
                    timeRange: appState.timeRangeState.timeRange
                )
            } else {
                await viewModel.loadProfiles(
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
            if appState.isLive {
                viewModel.startLiveStream(
                    dataSource: appState.dataSource,
                    selectedService: appState.selectedService,
                    timeRange: newTimeRange
                )
            } else {
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
        .onChange(of: viewModel.feed?.availableValueTypes) { _, types in
            // Auto-select first value type when types become available
            guard let types, !types.isEmpty, viewModel.selectedValueType == nil else { return }
            viewModel.selectedValueType = types.first
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
        .onChange(of: viewModel.selectedValueType) { _, _ in
            // Re-query when user changes the type
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
        .onChange(of: selectedNodeId) { _, newValue in
            if let id = newValue {
                selectedStackTrace = viewModel.getStackTrace(for: id)
            } else {
                selectedStackTrace = []
            }
        }
        .onDisappear {
            viewModel.cancel()
        }
    }
}

/// Content view showing the profile graph and detail panel
struct ProfileGraphContent: View {
    let feed: FlamegraphFeed
    @Bindable var viewModel: ProfilesViewModel
    @Binding var hoveredNodeId: String?
    @Binding var selectedNodeId: String?
    @Binding var selectedStackTrace: [FlamegraphNode]

    var body: some View {
        VStack(spacing: 0) {
            icicleGraphSection
            detailPanelSection
        }
    }

    private var icicleGraphSection: some View {
        GeometryReader { geometry in
            ZStack(alignment: .topTrailing) {
                if feed.isLoading {
                    ProfileEmptyState(isLoading: true)
                } else if feed.nodes.isEmpty {
                    ProfileEmptyState(isLoading: false)
                } else {
                    CanvasIcicleGraphView(
                        feed: feed,
                        width: geometry.size.width,
                        hoveredNodeId: $hoveredNodeId,
                        selectedNodeId: $selectedNodeId,
                        searchText: viewModel.searchText,
                        zoomedNodeId: $viewModel.zoomedNodeId
                    )
                }

                zoomIndicator
            }
        }
        .background(Color(NSColor.textBackgroundColor))
    }

    @ViewBuilder
    private var zoomIndicator: some View {
        if let zoomedId = viewModel.zoomedNodeId,
           let zoomedNode = viewModel.getNode(nodeId: zoomedId) {
            ZoomIndicatorView(frameName: zoomedNode.functionName) {
                viewModel.zoomedNodeId = nil
            }
            .padding(12)
        }
    }

    @ViewBuilder
    private var detailPanelSection: some View {
        if let selectedId = selectedNodeId,
           let selectedNode = viewModel.getNode(nodeId: selectedId) {
            ProfileDetailPanel(
                node: selectedNode,
                stackTrace: selectedStackTrace,
                onClose: { selectedNodeId = nil }
            )
        }
    }
}

#Preview {
    struct PreviewWrapper: View {
        @State private var viewModel = ProfilesViewModel()
        var body: some View {
            ProfilesContentOnly(viewModel: viewModel)
                .environment(AppStateViewModel())
                .frame(width: 800, height: 600)
        }
    }
    return PreviewWrapper()
}
