//
//  ProfilesViewModel.swift
//  Sequins
//
//  Thin query orchestrator for the profiles tab.
//  Uses executeView(.flamegraph) which handles both historical snapshot and
//  live streaming phases, delivering entity-level deltas to FlamegraphFeed.
//

import AppKit
import Foundation
import SwiftUI
import SequinsData

@MainActor
@Observable
final class ProfilesViewModel {
    // MARK: - Observable state

    /// The live flamegraph feed. Replaced on each loadProfiles call.
    private(set) var feed: FlamegraphFeed?

    /// Query-level error.
    private(set) var error: String?

    // MARK: - UI state

    var searchText: String = ""
    var selectedValueType: String? = nil
    var zoomedNodeId: String? = nil

    // MARK: - Private

    private var viewHandle: ViewHandle?
    private weak var dataSource: DataSource?

    // MARK: - Configuration

    func configure(dataSource: DataSource?) {
        cancel()
        self.dataSource = dataSource
    }

    // MARK: - Query lifecycle

    func loadProfiles(
        dataSource: DataSource?,
        selectedService: Service?,
        timeRange: TimeRange
    ) async {
        guard let dataSource else { return }
        self.dataSource = dataSource

        let duration = timeRange.bounds.end.timeIntervalSince(timeRange.bounds.start)
        let durationStr = formatDuration(duration)
        let retentionNs = UInt64(duration * 1_000_000_000)
        let resourceFilter = buildResourceIdFilter(selectedService)
        let valueTypeFilter = selectedValueType.map { " | where value_type = '\($0)'" } ?? ""

        let query = "samples last \(durationStr)\(resourceFilter)\(valueTypeFilter) <- stacks <- frames"

        cancel()
        error = nil

        let newFeed = FlamegraphFeed()
        feed = newFeed

        do {
            viewHandle = try dataSource.executeView(
                query,
                strategy: .flamegraph,
                retentionNs: retentionNs
            ) { [weak newFeed] deltas in
                newFeed?.applyViewDeltas(deltas)
            }
        } catch {
            self.error = error.localizedDescription
            newFeed.markFailed()
        }
    }

    func refresh(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange) {
        Task {
            await loadProfiles(
                dataSource: dataSource,
                selectedService: selectedService,
                timeRange: timeRange
            )
        }
    }

    /// Start live mode — identical to loadProfiles since executeView handles both phases.
    func startLiveStream(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange = .last(hours: 1)) {
        Task {
            await loadProfiles(
                dataSource: dataSource,
                selectedService: selectedService,
                timeRange: timeRange
            )
        }
    }

    func stopLiveStream() {
        viewHandle?.cancel()
        viewHandle = nil
    }

    func cancel() {
        viewHandle?.cancel()
        viewHandle = nil
        feed = nil
    }

    // MARK: - Export

    func exportAsJSON() {
        guard let feed else { return }

        let panel = NSSavePanel()
        panel.title = "Export Profile"
        panel.nameFieldStringValue = "profile-export.json"
        panel.allowedContentTypes = [.json]
        panel.canCreateDirectories = true

        guard panel.runModal() == .OK, let url = panel.url else { return }

        let jsonArray: [[String: Any]] = feed.nodes.map { node in
            var dict: [String: Any] = [
                "id": node.id,
                "functionName": node.functionName,
                "depth": node.depth,
                "selfValue": node.selfValue,
                "totalValue": node.totalValue,
                "selfPercentage": node.selfPercentage,
                "totalPercentage": node.totalPercentage,
                "childIds": node.childIds
            ]
            if let filename = node.filename { dict["filename"] = filename }
            if let line = node.line { dict["line"] = line }
            if let parentId = node.parentId { dict["parentId"] = parentId }
            if let systemName = node.systemName { dict["systemName"] = systemName }
            return dict
        }

        do {
            let data = try JSONSerialization.data(withJSONObject: jsonArray, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: url)
            NSLog("🔥 Exported \(jsonArray.count) profile nodes to \(url.path)")
        } catch {
            NSLog("🔥 Failed to export profile: \(error)")
        }
    }

    // MARK: - Node lookup helpers

    func getNode(nodeId: String) -> FlamegraphNode? {
        guard let feed, let idx = feed.nodeIndex[nodeId], idx < feed.nodes.count else { return nil }
        return feed.nodes[idx]
    }

    /// Walk parentId chain from the given node back to root.
    func getStackTrace(for nodeId: String) -> [FlamegraphNode] {
        guard let feed else { return [] }
        var result: [FlamegraphNode] = []
        var currentId: String? = nodeId
        while let id = currentId, let node = getNode(nodeId: id) {
            result.insert(node, at: 0)
            currentId = node.parentId
        }
        return result
    }

    // MARK: - Private helpers

    private func buildResourceIdFilter(_ selectedService: Service?) -> String {
        guard let service = selectedService, !service.resourceIds.isEmpty else { return "" }
        if service.resourceIds.count == 1 {
            return " | where resource_id = \(service.resourceIds[0])"
        }
        let ids = service.resourceIds.map { String($0) }.joined(separator: ", ")
        return " | where resource_id in [\(ids)]"
    }

    private func formatDuration(_ duration: TimeInterval) -> String {
        let hours = Int(duration / 3600)
        if hours > 0 && hours <= 24 {
            return "\(hours)h"
        }
        let minutes = Int(duration / 60)
        return "\(max(minutes, 1))m"
    }
}
