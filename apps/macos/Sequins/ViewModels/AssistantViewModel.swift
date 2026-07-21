import Foundation
import SwiftUI
import SequinsData

/// A lightweight summary of a persisted conversation (read from the `conversations`
/// DataFusion table via SQL).
struct ConversationSummary: Identifiable, Hashable {
    let id: String
    let title: String?
    let updatedAtNs: UInt64
    let itemCount: Int

    init?(row: [Any?]) {
        guard let id = row.first.flatMap({ $0 as? String }) else { return nil }
        self.id = id
        self.title = row.count > 1 ? row[1] as? String : nil
        self.updatedAtNs = row.count > 2 ? ((row[2] as? NSNumber)?.uint64Value ?? 0) : 0
        self.itemCount = row.count > 3 ? ((row[3] as? NSNumber)?.intValue ?? 0) : 0
    }

    var displayTitle: String {
        if let title, !title.isEmpty { return title }
        return "Untitled chat"
    }
}

/// One rendered item in a chat transcript.
struct ChatItem: Identifiable {
    enum Kind {
        case userText(String)
        case assistantText(String)
        case toolActivity(AssistantToolActivity)
        case visualization(SavedVisualization)
    }
    let id: UUID
    var kind: Kind
    init(id: UUID = UUID(), kind: Kind) {
        self.id = id
        self.kind = kind
    }
}

/// Reasoning effort for the assistant's next turn. `auto` sends nothing — the model's
/// default — while the middleware reconciles models that require a specific value.
enum ReasoningEffort: String, CaseIterable, Identifiable {
    case auto, none, low, medium, high

    var id: String { rawValue }

    var label: String {
        switch self {
        case .auto: return "Auto"
        case .none: return "None"
        case .low: return "Low"
        case .medium: return "Medium"
        case .high: return "High"
        }
    }

    /// Value sent as `reasoning.effort`, or `nil` for `auto` (omit it entirely).
    var wireValue: String? { self == .auto ? nil : rawValue }
}

/// Drives the Assistant chat tab: the conversation list, the active transcript, and a
/// streaming turn. Conversation history is read from the `messages`/`conversations`
/// tables via SQL; turns are sent through the normalized assistant FFI.
@MainActor
@Observable
final class AssistantViewModel {
    private(set) var conversations: [ConversationSummary] = []
    private(set) var transcript: [ChatItem] = []
    private(set) var isStreaming = false
    var selectedConversationId: String?
    var inputText: String = ""
    var errorMessage: String?

    /// Models advertised by the provider/daemon's `/v1/models`, for the input-bar picker.
    private(set) var availableModels: [String] = []
    /// The model used for the next turn — chosen in the input bar, seeded from the
    /// environment's saved model, defaulting to the first advertised model.
    var selectedModel: String?
    /// Reasoning effort for the next turn (chosen in the input bar).
    var selectedReasoning: ReasoningEffort = .auto
    /// Populated when the model list couldn't be fetched (bad key, offline, no `/models`).
    private(set) var modelsError: String?

    private var activeStream: AssistantChatStream?
    private var assistant: Assistant?
    private var assistantConfigKey: String?
    private var lastResponseId: String?

    /// Called when a turn completes, so the host can reload dashboards the assistant may
    /// have created/edited server-side (set by `MainWindow`).
    var onDashboardsChanged: (() -> Void)?

    /// A destructive action the assistant proposed (delete a dashboard / remove a chart)
    /// that awaits the user's approval. Rendered as a card in the transcript; the input
    /// stays live so the user can instead type revised instructions.
    private(set) var pendingApproval: PendingApproval?

    struct PendingApproval: Identifiable {
        let id = UUID()
        let toolName: String
        let arguments: String
        let title: String
        let detail: String
        let confirmLabel: String
    }

    // MARK: - Conversation list

    func refreshConversations(dataSource: DataSource) {
        dataSource.fetchAppStateSQL(
            "SELECT id, title, updated_at_ns, item_count FROM conversations ORDER BY updated_at_ns DESC"
        ) { [weak self] result in
            guard let self else { return }
            switch result {
            case .success(let payload):
                self.conversations = payload.rows.compactMap { ConversationSummary(row: $0) }
            case .failure(let error):
                self.errorMessage = error.localizedDescription
            }
        }
    }

    // MARK: - Open / new

    func openConversation(_ id: String, dataSource: DataSource) {
        selectedConversationId = id
        transcript = []
        lastResponseId = nil
        errorMessage = nil
        let escaped = id.replacingOccurrences(of: "'", with: "''")
        let sql = """
        SELECT role, item_type, text, tool_name, tool_arguments, tool_output, response_id, position \
        FROM messages WHERE conversation_id = '\(escaped)' ORDER BY position
        """
        dataSource.fetchAppStateSQL(sql) { [weak self] result in
            guard let self else { return }
            guard case .success(let payload) = result else {
                if case .failure(let error) = result { self.errorMessage = error.localizedDescription }
                return
            }
            self.transcript = payload.rows.compactMap { self.chatItem(fromMessageRow: $0) }
            self.lastResponseId = payload.rows
                .compactMap { $0.count > 6 ? $0[6] as? String : nil }
                .last
        }
    }

    func newChat() {
        activeStream?.cancel()
        activeStream = nil
        selectedConversationId = nil
        transcript = []
        lastResponseId = nil
        isStreaming = false
        errorMessage = nil
    }

    /// Delete a persisted conversation, removing it from the list and clearing the
    /// transcript if it was the open chat.
    func deleteConversation(_ id: String, dataSource: DataSource) {
        do {
            try dataSource.deleteConversation(id: id)
            conversations.removeAll { $0.id == id }
            if selectedConversationId == id { newChat() }
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    // MARK: - Destructive-action approval

    /// Turn a destructive tool call into a pending approval card rather than executing it.
    private func proposeDestructive(name: String, arguments: String) {
        let obj = (arguments.data(using: .utf8)
            .flatMap { try? JSONSerialization.jsonObject(with: $0) }) as? [String: Any] ?? [:]
        let dashboard = (obj["dashboard"] as? String) ?? "this dashboard"
        let pending: PendingApproval
        switch name {
        case "delete_dashboard":
            pending = PendingApproval(
                toolName: name, arguments: arguments,
                title: "Delete dashboard “\(dashboard)”?",
                detail: "This permanently removes the dashboard and all of its charts.",
                confirmLabel: "Delete")
        default: // remove_chart
            let row = (obj["row"] as? Int).map(String.init) ?? "?"
            let col = (obj["column"] as? Int).map(String.init) ?? "?"
            pending = PendingApproval(
                toolName: name, arguments: arguments,
                title: "Remove chart [\(row),\(col)] from “\(dashboard)”?",
                detail: "This removes the chart from the dashboard.",
                confirmLabel: "Remove")
        }
        pendingApproval = pending
    }

    /// User approved the pending destructive action — execute it via the data source.
    func approvePending(dataSource: DataSource) {
        guard let p = pendingApproval else { return }
        pendingApproval = nil
        let obj = (p.arguments.data(using: .utf8)
            .flatMap { try? JSONSerialization.jsonObject(with: $0) }) as? [String: Any] ?? [:]
        let key = (obj["dashboard"] as? String) ?? ""
        do {
            let output: String
            switch p.toolName {
            case "delete_dashboard":
                guard let d = try resolveDashboard(key, dataSource: dataSource) else {
                    throw SequinsError.ffiError("no dashboard matching “\(key)”")
                }
                try dataSource.deleteDashboard(id: d.id)
                output = "Deleted dashboard “\(d.title)”."
            default: // remove_chart
                guard var d = try resolveDashboard(key, dataSource: dataSource) else {
                    throw SequinsError.ffiError("no dashboard matching “\(key)”")
                }
                let row = obj["row"] as? Int ?? -1
                let col = obj["column"] as? Int ?? -1
                guard d.rows.indices.contains(row), d.rows[row].panels.indices.contains(col) else {
                    throw SequinsError.ffiError("no chart at [\(row),\(col)]")
                }
                d.rows[row].panels.remove(at: col)
                d.rows.removeAll { $0.panels.isEmpty }
                _ = try dataSource.saveDashboard(d)
                output = "Removed chart [\(row),\(col)] from “\(d.title)”."
            }
            transcript.append(ChatItem(kind: .toolActivity(AssistantToolActivity(
                name: p.toolName, arguments: p.arguments, output: output))))
            onDashboardsChanged?()
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    /// User declined the pending destructive action.
    func rejectPending() {
        guard let p = pendingApproval else { return }
        pendingApproval = nil
        transcript.append(ChatItem(kind: .toolActivity(AssistantToolActivity(
            name: p.toolName, arguments: p.arguments, output: "Declined."))))
    }

    /// Resolve a dashboard by id, then case-insensitive title.
    private func resolveDashboard(_ key: String, dataSource: DataSource) throws -> Dashboard? {
        if let byId = try dataSource.getDashboard(id: key) { return byId }
        return try dataSource.listDashboards()
            .first { $0.title.caseInsensitiveCompare(key) == .orderedSame }
    }

    // MARK: - Model list

    /// Fetch the provider/daemon's model list for the input-bar picker and settle on a
    /// selection: keep the current one, else the environment's saved model, else the
    /// first advertised model.
    func loadModels(config: AssistantConfig) async {
        if selectedModel == nil { selectedModel = config.model }
        do {
            let models = try await fetchAssistantModels(config)
            availableModels = models
            modelsError = nil
            if selectedModel == nil || selectedModel?.isEmpty == true {
                selectedModel = models.first
            }
        } catch {
            availableModels = []
            modelsError = error.localizedDescription
        }
    }

    // MARK: - Sending

    func send(dataSource: DataSource, config baseConfig: AssistantConfig) {
        let text = inputText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !text.isEmpty, !isStreaming else { return }
        // A new instruction supersedes any pending destructive proposal — treat it as a
        // decline and let the model act on the revised guidance.
        if let pending = pendingApproval {
            pendingApproval = nil
            transcript.append(ChatItem(kind: .toolActivity(AssistantToolActivity(
                name: pending.toolName, arguments: pending.arguments,
                output: "Skipped — see new instructions below."))))
        }
        // The input-bar picker's model wins over the environment default.
        var config = baseConfig
        if let selectedModel, !selectedModel.isEmpty { config.model = selectedModel }
        inputText = ""
        errorMessage = nil
        transcript.append(ChatItem(kind: .userText(text)))
        isStreaming = true

        do {
            let assistant = try makeOrReuseAssistant(dataSource: dataSource, config: config)
            let request = buildRequestJSON(userText: text, model: config.model)
            let stream = try assistant.chat(requestJSON: request)
            stream.onText = { [weak self] delta in self?.appendTextDelta(delta) }
            stream.onToolActivity = { [weak self] activity in
                self?.transcript.append(ChatItem(kind: .toolActivity(activity)))
            }
            stream.onToolCall = { [weak self] call in
                guard let self else { return }
                switch call.name {
                case "render_visualization":
                    if let viz = self.parseVisualization(from: call.arguments) {
                        self.transcript.append(ChatItem(kind: .visualization(viz)))
                    }
                case "delete_dashboard", "remove_chart":
                    self.proposeDestructive(name: call.name, arguments: call.arguments)
                default:
                    break
                }
            }
            stream.onDone = { [weak self] responseId, conversationId in
                self?.handleDone(responseId: responseId, conversationId: conversationId, dataSource: dataSource)
            }
            activeStream = stream
        } catch {
            errorMessage = error.localizedDescription
            isStreaming = false
        }
    }

    func cancelStreaming() {
        activeStream?.cancel()
        activeStream = nil
        isStreaming = false
    }

    // MARK: - Internals

    private func makeOrReuseAssistant(dataSource: DataSource, config: AssistantConfig) throws -> Assistant {
        let key = "\(config.baseURL ?? "")|\(config.model ?? "")|\(config.apiKey?.isEmpty == false)"
        if let assistant, assistantConfigKey == key {
            return assistant
        }
        let created = try dataSource.makeAssistant(config)
        assistant = created
        assistantConfigKey = key
        return created
    }

    private func handleDone(responseId: String?, conversationId: String?, dataSource: DataSource) {
        lastResponseId = responseId
        if let conversationId { selectedConversationId = conversationId }
        isStreaming = false
        activeStream = nil
        refreshConversations(dataSource: dataSource)
        // The turn may have created/edited dashboards via server-side tools; reload them.
        onDashboardsChanged?()
    }

    private func appendTextDelta(_ delta: String) {
        if let last = transcript.last, case .assistantText(let existing) = last.kind {
            transcript[transcript.count - 1] = ChatItem(id: last.id, kind: .assistantText(existing + delta))
        } else {
            transcript.append(ChatItem(kind: .assistantText(delta)))
        }
    }

    private func chatItem(fromMessageRow row: [Any?]) -> ChatItem? {
        func str(_ i: Int) -> String? { i < row.count ? row[i] as? String : nil }
        let role = str(0) ?? ""
        let itemType = str(1) ?? "message"
        switch itemType {
        case "message":
            guard let text = str(2), !text.isEmpty else { return nil }
            return role == "user"
                ? ChatItem(kind: .userText(text))
                : ChatItem(kind: .assistantText(text))
        case "sequins.tool_result":
            return ChatItem(kind: .toolActivity(AssistantToolActivity(
                name: str(3) ?? "", arguments: str(4) ?? "", output: str(5) ?? "")))
        case "function_call":
            if str(3) == "render_visualization", let viz = parseVisualization(from: str(4) ?? "") {
                return ChatItem(kind: .visualization(viz))
            }
            return ChatItem(kind: .toolActivity(AssistantToolActivity(
                name: str(3) ?? "", arguments: str(4) ?? "", output: "")))
        default:
            return nil
        }
    }

    /// Parse `render_visualization` arguments (`{ query|seql, title, chart_type? }`)
    /// into a `SavedVisualization`.
    func parseVisualization(from argsJSON: String) -> SavedVisualization? {
        guard let data = argsJSON.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else { return nil }
        let seql = (obj["query"] as? String) ?? (obj["seql"] as? String) ?? ""
        guard !seql.isEmpty else { return nil }
        let title = (obj["title"] as? String) ?? "Visualization"
        let shape = (obj["chart_type"] as? String) ?? (obj["shape"] as? String)
        let options = Self.parseOptions(from: obj["options"])
        return SavedVisualization(seql: seql, title: title, shape: shape, options: options)
    }

    /// Parse a presentation-options object (snake_case, as the model emits it) into a
    /// `VisualizationOptions`. Missing/invalid fields fall back to unset.
    static func parseOptions(from raw: Any?) -> VisualizationOptions {
        guard let obj = raw as? [String: Any] else { return VisualizationOptions() }
        func dbl(_ k: String) -> Double? {
            if let d = obj[k] as? Double { return d }
            if let n = obj[k] as? NSNumber { return n.doubleValue }
            return nil
        }
        let thresholds: [VizThreshold] = (obj["thresholds"] as? [[String: Any]] ?? []).compactMap { t in
            let v = (t["value"] as? Double) ?? (t["value"] as? NSNumber)?.doubleValue
            guard let value = v else { return nil }
            return VizThreshold(value: value, color: t["color"] as? String, label: t["label"] as? String)
        }
        return VisualizationOptions(
            unit: obj["unit"] as? String,
            yScale: obj["y_scale"] as? String,
            yMin: dbl("y_min"),
            yMax: dbl("y_max"),
            stacked: obj["stacked"] as? Bool,
            legend: obj["legend"] as? Bool,
            seriesLimit: (obj["series_limit"] as? Int) ?? (obj["series_limit"] as? NSNumber)?.intValue,
            thresholds: thresholds
        )
    }

    private func buildRequestJSON(userText: String, model: String?) -> String {
        var req: [String: Any] = [
            "model": model ?? "default",
            "input": [["type": "message", "role": "user", "content": userText]],
            "stream": true,
            "store": true,
            "tools": [renderVisualizationTool, deleteDashboardTool, removeChartTool],
        ]
        if let effort = selectedReasoning.wireValue {
            req["reasoning"] = ["effort": effort]
        }
        if let lastResponseId {
            req["previous_response_id"] = lastResponseId
        }
        let data = (try? JSONSerialization.data(withJSONObject: req)) ?? Data("{}".utf8)
        return String(decoding: data, as: UTF8.self)
    }

    private var renderVisualizationTool: [String: Any] {
        [
            "name": "render_visualization",
            "description": "Render a chart, table, or other visualization in the client UI from a "
                + "SeQL query. Call this whenever the user would benefit from seeing data plotted.",
            "parameters": [
                "type": "object",
                "properties": [
                    "query": [
                        "type": "string",
                        "description": "The SeQL query whose results should be visualized.",
                    ],
                    "title": [
                        "type": "string",
                        "description": "A short, descriptive title for the visualization.",
                    ],
                    "chart_type": [
                        "type": "string",
                        "enum": VizType.allCases.map { $0.rawValue },
                        "description": "Optional chart type. Omit to let the client choose.",
                    ],
                    "options": [
                        "type": "object",
                        "description": "Optional presentation overrides.",
                        "properties": [
                            "unit": ["type": "string", "description": "Value unit for axes/labels (e.g. ms, bytes, req/s)."],
                            "y_scale": ["type": "string", "enum": ["linear", "log"], "description": "Y-axis scale."],
                            "y_min": ["type": "number", "description": "Force y-axis lower bound."],
                            "y_max": ["type": "number", "description": "Force y-axis upper bound."],
                            "stacked": ["type": "boolean", "description": "Stack series instead of overlaying."],
                            "legend": ["type": "boolean", "description": "Show a series legend."],
                            "series_limit": ["type": "integer", "description": "Cap the number of series rendered (top-N)."],
                            "thresholds": [
                                "type": "array",
                                "description": "Horizontal reference lines (e.g. SLO boundaries).",
                                "items": [
                                    "type": "object",
                                    "properties": [
                                        "value": ["type": "number"],
                                        "color": ["type": "string"],
                                        "label": ["type": "string"],
                                    ],
                                    "required": ["value"],
                                ],
                            ],
                        ],
                    ],
                ],
                "required": ["query", "title"],
            ],
        ]
    }

    private var deleteDashboardTool: [String: Any] {
        [
            "name": "delete_dashboard",
            "description": "Delete a dashboard (by id or title). DESTRUCTIVE — the user must "
                + "approve; propose it and let them confirm.",
            "parameters": [
                "type": "object",
                "properties": [
                    "dashboard": ["type": "string", "description": "Dashboard id or title to delete."],
                ],
                "required": ["dashboard"],
            ],
        ]
    }

    private var removeChartTool: [String: Any] {
        [
            "name": "remove_chart",
            "description": "Remove a chart from a dashboard by its [row,column] position (from "
                + "get_dashboard). DESTRUCTIVE — the user must approve.",
            "parameters": [
                "type": "object",
                "properties": [
                    "dashboard": ["type": "string", "description": "Dashboard id or title."],
                    "row": ["type": "integer", "description": "Row index of the chart."],
                    "column": ["type": "integer", "description": "Column index within the row."],
                ],
                "required": ["dashboard", "row", "column"],
            ],
        ]
    }
}
