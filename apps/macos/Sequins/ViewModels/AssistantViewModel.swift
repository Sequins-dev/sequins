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

    private var activeStream: AssistantChatStream?
    private var assistant: Assistant?
    private var assistantConfigKey: String?
    private var lastResponseId: String?

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

    // MARK: - Sending

    func send(dataSource: DataSource, config: AssistantConfig) {
        let text = inputText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !text.isEmpty, !isStreaming else { return }
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
                case "add_to_dashboard":
                    self.handleAddToDashboard(arguments: call.arguments, dataSource: dataSource)
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

    /// Handle an `add_to_dashboard` tool call: find the named dashboard (or create it)
    /// and append the visualization as a new full-width row, then persist.
    private func handleAddToDashboard(arguments: String, dataSource: DataSource) {
        guard let viz = parseVisualization(from: arguments),
              let obj = jsonObject(arguments) else { return }
        let dashboardName = (obj["dashboard"] as? String)
            ?? (obj["dashboard_name"] as? String)
            ?? "Assistant"
        do {
            let existing = try dataSource.listDashboards()
            var dashboard = existing.first { $0.title.caseInsensitiveCompare(dashboardName) == .orderedSame }
                ?? Dashboard(title: dashboardName)
            dashboard.rows.append(DashboardRow(panels: [RowPanel(visualization: viz)]))
            _ = try dataSource.saveDashboard(dashboard)
            // Show the chart inline plus a confirmation of where it went.
            transcript.append(ChatItem(kind: .visualization(viz)))
            transcript.append(ChatItem(kind: .toolActivity(AssistantToolActivity(
                name: "add_to_dashboard",
                arguments: arguments,
                output: "Added “\(viz.title)” to dashboard “\(dashboardName)”."))))
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func jsonObject(_ json: String) -> [String: Any]? {
        guard let data = json.data(using: .utf8) else { return nil }
        return (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
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
        return SavedVisualization(seql: seql, title: title, shape: shape)
    }

    private func buildRequestJSON(userText: String, model: String?) -> String {
        var req: [String: Any] = [
            "model": model ?? "default",
            "input": [["type": "message", "role": "user", "content": userText]],
            "stream": true,
            "store": true,
            "tools": [renderVisualizationTool, addToDashboardTool],
        ]
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
                ],
                "required": ["query", "title"],
            ],
        ]
    }

    private var addToDashboardTool: [String: Any] {
        [
            "name": "add_to_dashboard",
            "description": "Save a visualization to a dashboard (by name). If a dashboard with "
                + "that name exists the chart is appended as a new row; otherwise a new dashboard "
                + "with that name is created. Use this when the user asks to add a chart to, or "
                + "build, a dashboard — as opposed to render_visualization which only shows a chart "
                + "inline in the chat.",
            "parameters": [
                "type": "object",
                "properties": [
                    "query": [
                        "type": "string",
                        "description": "The SeQL query whose results the chart shows.",
                    ],
                    "title": [
                        "type": "string",
                        "description": "A short, descriptive title for the chart.",
                    ],
                    "dashboard": [
                        "type": "string",
                        "description": "Name of the dashboard to add the chart to (created if missing).",
                    ],
                    "chart_type": [
                        "type": "string",
                        "enum": VizType.allCases.map { $0.rawValue },
                        "description": "Optional chart type. Omit to let the client choose.",
                    ],
                ],
                "required": ["query", "title", "dashboard"],
            ],
        ]
    }
}
