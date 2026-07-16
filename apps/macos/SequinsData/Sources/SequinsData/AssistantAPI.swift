import Foundation
import Observation
import SequinsFFI

// MARK: - Assistant configuration

/// LLM/provider configuration for constructing an ``Assistant``.
///
/// - **Local** data source: `baseURL`/`model`/`apiKey` describe the backing
///   OpenAI-compatible provider (`baseURL` nil ⇒ api.openai.com).
/// - **Remote** data source: `baseURL` is the daemon's `/v1` base (e.g.
///   `http://host:8082/v1`), `apiKey` is the bearer token, `model` is ignored.
public struct AssistantConfig: Sendable {
    public var baseURL: String?
    public var model: String?
    public var apiKey: String?

    public init(baseURL: String? = nil, model: String? = nil, apiKey: String? = nil) {
        self.baseURL = baseURL
        self.model = model
        self.apiKey = apiKey
    }

    /// Build from `OPENAI_API_KEY` / `OPENAI_BASE_URL` / `OPENAI_MODEL` process
    /// environment variables — a dev/CI convenience that mirrors the daemon's
    /// auto-enable. Returns `nil` when no key is present.
    public static func fromProcessEnvironment() -> AssistantConfig? {
        let env = ProcessInfo.processInfo.environment
        guard let key = env["OPENAI_API_KEY"], !key.isEmpty else { return nil }
        return AssistantConfig(
            baseURL: env["OPENAI_BASE_URL"],
            model: env["OPENAI_MODEL"] ?? "gpt-5.5",
            apiKey: key
        )
    }
}

/// A server-executed tool call and its rendered result (assistant "activity").
public struct AssistantToolActivity: Identifiable, Hashable, Sendable {
    public let id = UUID()
    public let name: String
    public let arguments: String
    public let output: String

    public init(name: String, arguments: String, output: String) {
        self.name = name
        self.arguments = arguments
        self.output = output
    }
}

/// A tool the client is expected to handle — e.g. `render_visualization`.
public struct AssistantToolCall: Identifiable, Hashable, Sendable {
    public let id = UUID()
    public let name: String
    /// Raw JSON string of the tool arguments.
    public let arguments: String

    public init(name: String, arguments: String) {
        self.name = name
        self.arguments = arguments
    }
}

// MARK: - Assistant handle

/// A constructed assistant (Local in-process middleware model, or Remote daemon).
///
/// Retains the ``DataSource`` it was built over (the FFI requires the data source to
/// outlive the assistant). Freed on `deinit`.
public final class Assistant {
    private let pointer: OpaquePointer
    private let dataSource: DataSource

    fileprivate init(pointer: OpaquePointer, dataSource: DataSource) {
        self.pointer = pointer
        self.dataSource = dataSource
    }

    deinit {
        sequins_assistant_free(pointer)
    }

    /// Start a chat turn. `requestJSON` is an OpenAI Responses-shaped request
    /// (`model`, `input`, `tools`, `instructions`, `previous_response_id`, …).
    /// Returns an observable stream that emits text, tool activity, client tool
    /// calls, and a terminal done/error.
    @discardableResult
    public func chat(requestJSON: String) throws -> AssistantChatStream {
        let stream = AssistantChatStream()
        let context = AssistantContext(stream: stream)
        let ctxRaw = Unmanaged.passUnretained(context).toOpaque()

        var vtable = CAssistantEventVTable()

        vtable.on_text = { (textPtr, ctxPtr) in
            guard let textPtr, let ctxPtr else { return }
            let text = String(cString: textPtr)
            let ctx = Unmanaged<AssistantContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.appendText(text) }
        }

        vtable.on_tool_activity = { (actPtr, ctxPtr) in
            guard let actPtr, let ctxPtr else { return }
            let a = actPtr.pointee
            let activity = AssistantToolActivity(
                name: a.name.map { String(cString: $0) } ?? "",
                arguments: a.arguments.map { String(cString: $0) } ?? "",
                output: a.output.map { String(cString: $0) } ?? ""
            )
            let ctx = Unmanaged<AssistantContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.appendActivity(activity) }
        }

        vtable.on_tool_call = { (callPtr, ctxPtr) in
            guard let callPtr, let ctxPtr else { return }
            let c = callPtr.pointee
            let call = AssistantToolCall(
                name: c.name.map { String(cString: $0) } ?? "",
                arguments: c.arguments.map { String(cString: $0) } ?? ""
            )
            let ctx = Unmanaged<AssistantContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.appendToolCall(call) }
        }

        vtable.on_done = { (donePtr, ctxPtr) in
            guard let donePtr, let ctxPtr else { return }
            let d = donePtr.pointee
            let responseId = d.response_id.map { String(cString: $0) }
            let conversationId = d.conversation_id.map { String(cString: $0) }
            let ctx = Unmanaged<AssistantContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.finish(responseId: responseId, conversationId: conversationId) }
        }

        vtable.on_error = { (msgPtr, ctxPtr) in
            guard let ctxPtr else { return }
            let message = msgPtr.map { String(cString: $0) } ?? "assistant error"
            let ctx = Unmanaged<AssistantContext>.fromOpaque(ctxPtr).takeUnretainedValue()
            DispatchQueue.main.async { ctx.stream?.fail(message: message) }
        }

        let handle = requestJSON.withCString { reqPtr in
            sequins_assistant_chat(pointer, reqPtr, vtable, ctxRaw)
        }
        guard let handle else {
            throw SequinsError.ffiError("failed to start assistant chat stream")
        }
        stream.setHandle(handle)
        stream.assistant = self
        stream._contextRetainer = context
        return stream
    }
}

extension DataSource {
    /// Construct an ``Assistant`` over this data source and provider/daemon config.
    public func makeAssistant(_ config: AssistantConfig) throws -> Assistant {
        // strdup the config strings; `sequins_assistant_new` copies them synchronously,
        // so we free right after the call.
        let baseURL = config.baseURL.map { strdup($0) } ?? nil
        let model = config.model.map { strdup($0) } ?? nil
        let apiKey = config.apiKey.map { strdup($0) } ?? nil
        defer {
            if let baseURL { free(baseURL) }
            if let model { free(model) }
            if let apiKey { free(apiKey) }
        }

        let cConfig = CAssistantConfig(base_url: baseURL, model: model, api_key: apiKey)
        var errorPtr: UnsafeMutablePointer<CChar>?
        guard let pointer = sequins_assistant_new(rawPointer, cConfig, &errorPtr) else {
            if let errorPtr {
                let msg = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(msg)
            }
            throw SequinsError.ffiError("failed to construct assistant")
        }
        return Assistant(pointer: pointer, dataSource: self)
    }
}

// MARK: - Chat stream

/// Context bridge for `AssistantChatStream` callbacks (passed as `void*`).
final class AssistantContext {
    weak var stream: AssistantChatStream?
    init(stream: AssistantChatStream) { self.stream = stream }
}

/// An `@Observable` streaming chat turn. Text accumulates in `assistantText`; server
/// tool activity and client tool calls are surfaced as they arrive; `isComplete` flips
/// on the terminal `done`/`error`. `deinit` frees the Rust stream (which blocks until
/// the task stops, so no callback fires afterward).
@Observable
public final class AssistantChatStream {
    /// Accumulated assistant text for this turn.
    public private(set) var assistantText: String = ""
    /// Server-executed tool activity (e.g. `run_seql`) surfaced for rendering.
    public private(set) var toolActivities: [AssistantToolActivity] = []
    /// Client tool calls the app must handle (e.g. `render_visualization`).
    public private(set) var toolCalls: [AssistantToolCall] = []
    /// Continuation ids (set on `done`): pass `responseId` as `previous_response_id`.
    public private(set) var responseId: String?
    public private(set) var conversationId: String?
    /// Last error message, if any.
    public private(set) var errorMessage: String?
    /// Whether the turn has ended (done or error).
    public private(set) var isComplete: Bool = false

    /// Fired for each streamed text delta (in addition to `assistantText`).
    public var onText: ((String) -> Void)?
    /// Fired when a client tool call (e.g. `render_visualization`) arrives.
    public var onToolCall: ((AssistantToolCall) -> Void)?
    /// Fired when a server tool activity arrives.
    public var onToolActivity: ((AssistantToolActivity) -> Void)?
    /// Fired once on completion (`responseId`, `conversationId`).
    public var onDone: ((_ responseId: String?, _ conversationId: String?) -> Void)?

    private var handle: OpaquePointer?
    /// Retains the assistant (FFI requires it to outlive the stream) and the context.
    fileprivate var assistant: Assistant?
    var _contextRetainer: AnyObject?

    init() {}

    func setHandle(_ h: OpaquePointer) { handle = h }

    // All `apply*`/`append*` methods below are called on the main thread.
    func appendText(_ text: String) {
        assistantText += text
        onText?(text)
    }

    func appendActivity(_ activity: AssistantToolActivity) {
        toolActivities.append(activity)
        onToolActivity?(activity)
    }

    func appendToolCall(_ call: AssistantToolCall) {
        toolCalls.append(call)
        onToolCall?(call)
    }

    func finish(responseId: String?, conversationId: String?) {
        self.responseId = responseId
        self.conversationId = conversationId
        self.isComplete = true
        onDone?(responseId, conversationId)
    }

    func fail(message: String) {
        self.errorMessage = message
        self.isComplete = true
    }

    public func cancel() {
        if let h = handle {
            sequins_assistant_cancel(h)
        }
    }

    deinit {
        guard let h = handle else { return }
        handle = nil
        let retainer = _contextRetainer
        _contextRetainer = nil
        let keepAssistant = assistant
        assistant = nil
        DispatchQueue.global(qos: .utility).async {
            sequins_assistant_stream_free(h)
            _ = retainer
            _ = keepAssistant
        }
    }
}
