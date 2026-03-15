//
//  FlamegraphFeed.swift
//  SequinsData
//
//  @Observable flamegraph model driven by entity deltas from a `FlamegraphStrategy` view.
//
//  The Rust FlamegraphStrategy emits:
//    EntityCreated:      descriptor (path_key, frame_id, function_name, system_name,
//                                   filename, line, depth, parent_path_key)
//                        + data (total_value, self_value)
//    EntityDataReplaced: key + data (total_value, self_value)
//    EntityRemoved:      key
//    Ready:              initial snapshot complete, live updates now streaming
//    Heartbeat:          periodic keepalive
//
//  applyViewDeltas(_:) accumulates entity state.
//  rebuildFromEntities() produces the flat [FlamegraphNode] array consumed by the view.
//

import Foundation
import Logging
import Tracing

// MARK: - FlamegraphNode

/// A single frozen node in the flamegraph tree, optimised for display.
public struct FlamegraphNode: Identifiable, Equatable {
    /// Path key from root: "frameId0/frameId1/.../frameIdK" (or "root" for synthetic root).
    public let id: String
    public let frameId: UInt64
    public let functionName: String
    public let systemName: String?
    public let filename: String?
    public let line: Int64?
    public let depth: Int
    public let selfValue: Int64
    public let totalValue: Int64
    public let parentId: String?
    public let childIds: [String]
    public let selfPercentage: Double
    public let totalPercentage: Double

    public init(
        id: String, frameId: UInt64, functionName: String,
        systemName: String?, filename: String?, line: Int64?,
        depth: Int, selfValue: Int64, totalValue: Int64,
        parentId: String?, childIds: [String],
        selfPercentage: Double, totalPercentage: Double
    ) {
        self.id = id
        self.frameId = frameId
        self.functionName = functionName
        self.systemName = systemName
        self.filename = filename
        self.line = line
        self.depth = depth
        self.selfValue = selfValue
        self.totalValue = totalValue
        self.parentId = parentId
        self.childIds = childIds
        self.selfPercentage = selfPercentage
        self.totalPercentage = totalPercentage
    }

    public static func == (lhs: FlamegraphNode, rhs: FlamegraphNode) -> Bool {
        lhs.id == rhs.id
    }
}

// MARK: - FlamegraphFeed

/// An `@Observable` flamegraph populated by entity deltas from a `FlamegraphStrategy` view.
///
/// Created fresh by `ProfilesViewModel` on each load. The view observes `nodes` directly;
/// it is replaced atomically by `rebuildFromEntities()` after each delta batch.
@Observable
public final class FlamegraphFeed {
    private static let logger = Logger(label: "sequins.flamegraph-feed")

    // MARK: - Public state

    /// Flat node array, ordered by a BFS traversal of the tree.
    public private(set) var nodes: [FlamegraphNode] = []

    /// Fast lookup: nodeId → index in `nodes`.
    public private(set) var nodeIndex: [String: Int] = [:]

    /// The id of the synthetic root node ("root"), or nil before rebuild.
    public private(set) var rootNodeId: String? = nil

    /// Total value summed across all root-level contributions.
    public private(set) var totalValue: Int64 = 0

    /// True until markReady() or markFailed() is called.
    public private(set) var isLoading: Bool = true

    /// Distinct value types — populated from entity descriptors.
    public private(set) var availableValueTypes: [String] = []

    // MARK: - Private entity state

    private struct EntityData {
        let id: String         // path_key
        let frameId: UInt64
        let functionName: String
        let systemName: String?
        let filename: String?
        let line: Int64?
        let depth: Int
        let parentId: String?  // parent path_key, or nil for root-level nodes
        var totalValue: Int64
        var selfValue: Int64
    }

    private enum EntityMutation {
        case upsert(EntityData)
        case update(key: String, totalValue: Int64, selfValue: Int64)
        case remove(key: String)
        case ready
        case failed
    }

    private var entities: [String: EntityData] = [:]
    private var rebuildTask: Task<Void, Never>?

    public init() {}

    // MARK: - Lifecycle

    public func markReady() { isLoading = false }
    public func markFailed() { isLoading = false }

    // MARK: - Entity delta application

    /// Apply a batch of view deltas from the FlamegraphStrategy.
    /// Parsing (toRows + field extraction) runs in background; mutations and rebuild are staged.
    public func applyViewDeltas(_ deltas: [ViewDelta]) {
        withSpan("FlamegraphFeed.applyViewDeltas") { _ in }

        Task.detached { [weak self] in
            let mutations = Self.parseDeltaMutations(deltas)
            await MainActor.run { [weak self] in
                self?.applyEntityMutations(mutations)
            }
        }
    }

    // MARK: - Background parse

    private static func parseDeltaMutations(_ deltas: [ViewDelta]) -> [EntityMutation] {
        var mutations: [EntityMutation] = []

        for delta in deltas {
            switch delta.type {
            case .entityCreated:
                guard let descBatch = delta.descriptor.first,
                      let dataBatch = delta.data.first,
                      let key = delta.key
                else { continue }

                // Descriptor schema: path_key(0), frame_id(1), function_name(2),
                //                    system_name(3), filename(4), line(5),
                //                    depth(6), parent_path_key(7)
                let descRows = descBatch.toRows()
                guard let row = descRows.first, row.count >= 8 else { continue }

                let frameId: UInt64
                if let n = row[1] as? UInt64 { frameId = n }
                else if let n = row[1] as? NSNumber { frameId = n.uint64Value }
                else { continue }

                let functionName = row[2] as? String ?? "???"
                let systemName = row[3] as? String
                let filename = row[4] as? String
                let line: Int64? = (row[5] as? NSNumber)?.int64Value

                let depth: Int
                if let n = row[6] as? UInt32 { depth = Int(n) }
                else if let n = row[6] as? NSNumber { depth = n.intValue }
                else { depth = 0 }

                let parentId = row[7] as? String

                let (totalValue, selfValue) = extractDataValues(dataBatch)

                let entity = EntityData(
                    id: key,
                    frameId: frameId,
                    functionName: functionName,
                    systemName: systemName,
                    filename: filename,
                    line: line,
                    depth: depth,
                    parentId: parentId,
                    totalValue: totalValue,
                    selfValue: selfValue
                )
                mutations.append(.upsert(entity))

            case .entityDataReplaced:
                guard let key = delta.key,
                      let dataBatch = delta.data.first
                else { continue }

                let (totalValue, selfValue) = extractDataValues(dataBatch)
                mutations.append(.update(key: key, totalValue: totalValue, selfValue: selfValue))

            case .entityRemoved:
                guard let key = delta.key else { continue }
                mutations.append(.remove(key: key))

            case .ready:
                mutations.append(.ready)

            case .error:
                mutations.append(.failed)

            default:
                break
            }
        }

        return mutations
    }

    // MARK: - Main-thread mutation application

    @MainActor
    private func applyEntityMutations(_ mutations: [EntityMutation]) {
        var didChange = false

        for mutation in mutations {
            switch mutation {
            case .upsert(let entity):
                entities[entity.id] = entity
                didChange = true

            case .update(let key, let totalValue, let selfValue):
                if entities[key] != nil {
                    entities[key]!.totalValue = totalValue
                    entities[key]!.selfValue = selfValue
                    didChange = true
                }

            case .remove(let key):
                entities.removeValue(forKey: key)
                didChange = true

            case .ready:
                isLoading = false

            case .failed:
                isLoading = false
            }
        }

        if didChange {
            scheduleRebuild()
        }
    }

    // MARK: - Background tree reconstruction

    private func scheduleRebuild() {
        let entitySnapshot = entities
        rebuildTask?.cancel()
        rebuildTask = Task.detached { [weak self] in
            guard !Task.isCancelled else { return }
            let (resultNodes, resultIndex, resultRootId, resultTotal) =
                Self.buildNodeTree(entities: entitySnapshot)
            guard !Task.isCancelled else { return }
            await MainActor.run { [weak self] in
                self?.nodes = resultNodes
                self?.nodeIndex = resultIndex
                self?.rootNodeId = resultRootId
                self?.totalValue = resultTotal
            }
        }
    }

    /// Build `nodes` from the current `entities` dictionary.
    private static func buildNodeTree(
        entities: [String: EntityData]
    ) -> (nodes: [FlamegraphNode], index: [String: Int], rootNodeId: String?, totalValue: Int64) {
        guard !entities.isEmpty else {
            return ([], [:], nil, 0)
        }

        // Compute grand total: sum of total_value for root-level nodes (depth == 0)
        let grandTotal: Int64 = entities.values.reduce(0) { acc, e in
            e.depth == 0 ? acc + e.totalValue : acc
        }
        guard grandTotal > 0 else { return ([], [:], nil, 0) }

        // Build child lists
        var childIds: [String: [String]] = [:]
        var rootIds: [String] = []

        for entity in entities.values {
            if let pid = entity.parentId {
                childIds[pid, default: []].append(entity.id)
            } else {
                rootIds.append(entity.id)
            }
        }

        // Sort children by descending total_value
        for key in childIds.keys {
            childIds[key]!.sort { a, b in
                (entities[a]?.totalValue ?? 0) > (entities[b]?.totalValue ?? 0)
            }
        }
        rootIds.sort { a, b in
            (entities[a]?.totalValue ?? 0) > (entities[b]?.totalValue ?? 0)
        }

        var result: [FlamegraphNode] = []
        var index: [String: Int] = [:]

        func visit(_ entityId: String) {
            guard let entity = entities[entityId] else { return }
            let kids = childIds[entityId] ?? []
            let selfPct = Double(entity.selfValue) / Double(grandTotal) * 100
            let totalPct = Double(entity.totalValue) / Double(grandTotal) * 100
            let frozen = FlamegraphNode(
                id: entity.id,
                frameId: entity.frameId,
                functionName: entity.functionName,
                systemName: entity.systemName,
                filename: entity.filename,
                line: entity.line,
                depth: entity.depth,
                selfValue: entity.selfValue,
                totalValue: entity.totalValue,
                parentId: entity.parentId,
                childIds: kids,
                selfPercentage: selfPct,
                totalPercentage: totalPct
            )
            index[entity.id] = result.count
            result.append(frozen)
            for childId in kids {
                visit(childId)
            }
        }

        for rootId in rootIds {
            visit(rootId)
        }

        return (result, index, rootIds.first, grandTotal)
    }

    // MARK: - Helpers

    private static func extractDataValues(_ batch: RecordBatch) -> (totalValue: Int64, selfValue: Int64) {
        let rows = batch.toRows()
        guard let row = rows.first, row.count >= 2 else { return (0, 0) }
        let total = (row[0] as? NSNumber)?.int64Value ?? 0
        let self_ = (row[1] as? NSNumber)?.int64Value ?? 0
        return (total, self_)
    }
}
