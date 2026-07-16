import SwiftUI
import SequinsData

// MARK: - Content Only Wrapper

struct DashboardsContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: DashboardsViewModel

    var body: some View {
        DashboardsContentView(viewModel: viewModel)
            .task(id: appState.dataSourceId) {
                if let ds = appState.dataSource {
                    viewModel.refresh(dataSource: ds)
                }
            }
            .onAppear {
                // Pick up dashboards created elsewhere (e.g. by the assistant) when
                // switching to this tab.
                if let ds = appState.dataSource {
                    viewModel.refresh(dataSource: ds)
                }
            }
    }
}

// MARK: - Dashboards tab

/// The Dashboards tab: a dashboard list on the left, the selected dashboard's grid on
/// the right. The grid honors the universal live toggle + time-range controls.
struct DashboardsContentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: DashboardsViewModel

    var body: some View {
        HSplitView {
            DashboardListPanel(viewModel: viewModel)
                .frame(minWidth: 180, idealWidth: 220, maxWidth: 320)

            detail
                .frame(minWidth: 420, maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    @ViewBuilder
    private var detail: some View {
        if appState.dataSource == nil {
            VizMessage(icon: "bolt.slash", text: "Not connected to a data source")
        } else if viewModel.selected == nil {
            VStack(spacing: 12) {
                Image(systemName: "square.grid.2x2")
                    .font(.system(size: 44))
                    .foregroundStyle(.secondary)
                Text("No dashboard selected")
                    .font(.title3)
                Text("Select a dashboard, or add a visualization from the Assistant.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .frame(maxWidth: 320)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            DashboardGridView(
                viewModel: viewModel,
                dataSource: appState.dataSource,
                timeRange: appState.timeRangeState.timeRange,
                isLive: appState.isLive
            )
        }
    }
}
