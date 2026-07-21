import Foundation
import SwiftUI
import SequinsData

/// Drives the Dashboards tab: the dashboard list, the selected dashboard's working
/// copy, and row/panel edits (resize rows, adjust panel ratios, add/remove/configure
/// panels, add rows) persisted via the dashboard FFI.
@MainActor
@Observable
final class DashboardsViewModel {
    private(set) var dashboards: [Dashboard] = []
    var selectedDashboardId: String?
    /// A mutable working copy of the selected dashboard (edited in place).
    var selected: Dashboard?
    var errorMessage: String?

    // MARK: - List / selection

    func refresh(dataSource: DataSource) {
        do {
            dashboards = try dataSource.listDashboards()
            if let id = selectedDashboardId {
                selected = dashboards.first { $0.id == id }
            } else {
                selected = nil
            }
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func select(_ id: String) {
        selectedDashboardId = id
        selected = dashboards.first { $0.id == id }
    }

    func createDashboard(title: String, dataSource: DataSource) {
        do {
            let name = title.trimmingCharacters(in: .whitespaces)
            let saved = try dataSource.saveDashboard(Dashboard(title: name.isEmpty ? "New Dashboard" : name))
            refresh(dataSource: dataSource)
            select(saved.id)
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    func deleteSelected(dataSource: DataSource) {
        guard let id = selectedDashboardId else { return }
        do {
            try dataSource.deleteDashboard(id: id)
            selectedDashboardId = nil
            selected = nil
            refresh(dataSource: dataSource)
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    // MARK: - Row / panel edits
    //
    // During an active drag, pass `persist: false` (updates the working copy only, so
    // the gesture stays smooth); on drag/edit end pass `persist: true` to save.

    /// Set a row's height (row-resize drag).
    func setRowHeight(_ rowIndex: Int, height: Double, persist: Bool, dataSource: DataSource) {
        guard var d = selected, d.rows.indices.contains(rowIndex) else { return }
        d.rows[rowIndex].height = max(120, height)
        commit(d, persist: persist, dataSource: dataSource)
    }

    /// Replace the weights of a row's panels (ratio drag between two panels).
    func setPanelWeights(_ rowIndex: Int, weights: [Double], persist: Bool, dataSource: DataSource) {
        guard var d = selected, d.rows.indices.contains(rowIndex),
              d.rows[rowIndex].panels.count == weights.count else { return }
        for i in d.rows[rowIndex].panels.indices {
            d.rows[rowIndex].panels[i].weight = max(0.05, weights[i])
        }
        commit(d, persist: persist, dataSource: dataSource)
    }

    /// Append a panel to an existing row (splitting its width).
    func addPanel(toRow rowIndex: Int, visualization: SavedVisualization, dataSource: DataSource) {
        guard var d = selected, d.rows.indices.contains(rowIndex) else { return }
        d.rows[rowIndex].panels.append(RowPanel(visualization: visualization, weight: 1.0))
        commit(d, persist: true, dataSource: dataSource)
    }

    /// Add a new full-width row at the bottom, holding `visualization`.
    func addRow(with visualization: SavedVisualization, dataSource: DataSource) {
        guard var d = selected else { return }
        d.rows.append(DashboardRow(panels: [RowPanel(visualization: visualization)]))
        commit(d, persist: true, dataSource: dataSource)
    }

    /// Replace a panel's visualization (chart editor "save").
    func updatePanel(row rowIndex: Int, panel panelIndex: Int, visualization: SavedVisualization, dataSource: DataSource) {
        guard var d = selected, d.rows.indices.contains(rowIndex),
              d.rows[rowIndex].panels.indices.contains(panelIndex) else { return }
        d.rows[rowIndex].panels[panelIndex].visualization = visualization
        commit(d, persist: true, dataSource: dataSource)
    }

    /// Move a panel (identified by its stable id) to `destRow`, inserted at `destIndex`
    /// within that row's *current* layout (nil = append to the end). Handles reordering
    /// within a row and moving across rows; if the source row empties it is removed.
    /// Drag-and-drop reorder entry point.
    func movePanel(_ panelId: UUID, toRow destRow: Int, at destIndex: Int?, dataSource: DataSource) {
        guard var d = selected, let src = locate(panelId, in: d), d.rows.indices.contains(destRow) else { return }

        var dest = destRow
        var insert = destIndex ?? d.rows[dest].panels.count

        // Dropping a panel back onto its own slot (either side) is a no-op.
        if src.row == dest && (insert == src.panel || insert == src.panel + 1) { return }

        let panel = d.rows[src.row].panels.remove(at: src.panel)

        // Same-row move: removing the source shifts later indices left by one.
        if src.row == dest && src.panel < insert { insert -= 1 }

        // If the source row emptied, drop it and fix up the destination index.
        if d.rows[src.row].panels.isEmpty {
            d.rows.remove(at: src.row)
            if src.row < dest { dest -= 1 }
        }

        guard d.rows.indices.contains(dest) else { return }
        insert = min(max(0, insert), d.rows[dest].panels.count)
        d.rows[dest].panels.insert(panel, at: insert)
        commit(d, persist: true, dataSource: dataSource)
    }

    /// Find a panel's (row, panel) position by its stable id.
    private func locate(_ panelId: UUID, in dashboard: Dashboard) -> (row: Int, panel: Int)? {
        for (ri, row) in dashboard.rows.enumerated() {
            if let pi = row.panels.firstIndex(where: { $0.id == panelId }) { return (ri, pi) }
        }
        return nil
    }

    /// Remove a panel; drops the row if it becomes empty.
    func removePanel(row rowIndex: Int, panel panelIndex: Int, dataSource: DataSource) {
        guard var d = selected, d.rows.indices.contains(rowIndex),
              d.rows[rowIndex].panels.indices.contains(panelIndex) else { return }
        d.rows[rowIndex].panels.remove(at: panelIndex)
        if d.rows[rowIndex].panels.isEmpty {
            d.rows.remove(at: rowIndex)
        }
        commit(d, persist: true, dataSource: dataSource)
    }

    // MARK: - Internals

    private func commit(_ dashboard: Dashboard, persist: Bool, dataSource: DataSource) {
        selected = dashboard
        guard persist else { return }
        save(dashboard, dataSource: dataSource)
    }

    private func save(_ dashboard: Dashboard, dataSource: DataSource) {
        do {
            let saved = try dataSource.saveDashboard(dashboard)
            if let idx = dashboards.firstIndex(where: { $0.id == saved.id }) {
                dashboards[idx] = saved
            } else {
                dashboards.append(saved)
            }
            // Preserve the working copy's live edits but adopt the saved id/timestamps.
            selected?.id = saved.id
            selectedDashboardId = saved.id
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}
