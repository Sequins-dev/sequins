//! FFI: streaming AI-assistant chat over the normalized [`Assistant`] façade.
//!
//! One interface regardless of backend: Local runs the in-process middleware model
//! (persisting conversations); Remote posts the daemon's `/v1/responses`. Events are
//! delivered through a C vtable — text, server-tool activity, client tool calls (the
//! app's `render_visualization`), a terminal `done` (with ids), and errors. Mirrors
//! the `seql.rs` streaming pattern (opaque handle + spawned task + **blocking** free).

use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use sequins_assistant::{Assistant, BackingModel, ChatEvent};
use tokio::task::JoinHandle;

use crate::data_source::{CDataSource, DataSourceImpl};
use crate::runtime::RUNTIME;
use crate::seql::{AssertSend, SendPtr};

/// Opaque handle to a constructed assistant (Local or Remote).
pub struct CAssistant {
    inner: Arc<Assistant<BackingModel>>,
}

/// Connection config for the assistant.
///
/// - **Local**: `base_url`/`model`/`api_key` describe the backing LLM provider
///   (OpenAI-compatible). `base_url` may be null for api.openai.com.
/// - **Remote**: `base_url` is the daemon's `/v1` base (e.g. `http://host:8082/v1`),
///   `api_key` is the bearer token, `model` is ignored.
#[repr(C)]
pub struct CAssistantConfig {
    pub base_url: *const c_char,
    pub model: *const c_char,
    pub api_key: *const c_char,
}

/// A server-executed tool call and its result (for rendering activity).
#[repr(C)]
pub struct CToolActivity {
    pub name: *const c_char,
    /// Tool arguments as a JSON string.
    pub arguments: *const c_char,
    /// Rendered tool output.
    pub output: *const c_char,
}

/// A tool call the client should handle (e.g. `render_visualization`).
#[repr(C)]
pub struct CToolCall {
    pub name: *const c_char,
    /// Tool arguments as a JSON string.
    pub arguments: *const c_char,
}

/// Terminal event with continuation ids.
#[repr(C)]
pub struct CAssistantDone {
    pub response_id: *const c_char,
    /// Conversation id to continue via `previous_response_id`; null if unpersisted.
    pub conversation_id: *const c_char,
}

/// C vtable for assistant events. Callbacks may fire from a Tokio worker thread and
/// must copy out what they need and return promptly (the pointers are freed after).
#[repr(C)]
pub struct CAssistantEventVTable {
    pub on_text: Option<unsafe extern "C" fn(*const c_char, ctx: *mut c_void)>,
    pub on_tool_activity: Option<unsafe extern "C" fn(*const CToolActivity, ctx: *mut c_void)>,
    pub on_tool_call: Option<unsafe extern "C" fn(*const CToolCall, ctx: *mut c_void)>,
    pub on_done: Option<unsafe extern "C" fn(*const CAssistantDone, ctx: *mut c_void)>,
    pub on_error: Option<unsafe extern "C" fn(*const c_char, ctx: *mut c_void)>,
}
// SAFETY: only fn pointers + a caller-owned void* context.
unsafe impl Send for CAssistantEventVTable {}
unsafe impl Sync for CAssistantEventVTable {}

struct AssistantSink {
    vtable: CAssistantEventVTable,
    ctx: SendPtr,
}
// SAFETY: vtable is Send; ctx is a SendPtr (caller-guaranteed thread-safe).
unsafe impl Send for AssistantSink {}

/// Opaque handle to a running assistant chat stream.
pub struct CAssistantStream {
    cancel: Arc<AtomicBool>,
    _task: JoinHandle<()>,
}

fn opt_cstr(p: *const c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .ok()
        .map(str::to_string)
        .filter(|s| !s.is_empty())
}

unsafe fn set_error(error_out: *mut *mut c_char, msg: &str) {
    if !error_out.is_null() {
        *error_out = CString::new(msg).unwrap_or_default().into_raw();
    }
}

fn to_cstring(s: String) -> CString {
    CString::new(s).unwrap_or_default()
}

/// Construct an assistant over a data source and provider/daemon config.
///
/// # Safety
/// - `data_source` must be a valid `CDataSource*`.
/// - `config` string pointers must be valid null-terminated UTF-8 or null.
/// - On error returns null and, if `error_out` is non-null, writes an owned error
///   string the caller must free with `sequins_string_free`.
#[no_mangle]
pub unsafe extern "C" fn sequins_assistant_new(
    data_source: *mut CDataSource,
    config: CAssistantConfig,
    error_out: *mut *mut c_char,
) -> *mut CAssistant {
    if data_source.is_null() {
        set_error(error_out, "data source is null");
        return std::ptr::null_mut();
    }
    let impl_ref = &*(data_source as *const DataSourceImpl);
    let base_url = opt_cstr(config.base_url);
    let api_key = opt_cstr(config.api_key);

    let assistant: Assistant<BackingModel> = match impl_ref {
        #[cfg(feature = "local")]
        DataSourceImpl::Local {
            storage, backend, ..
        } => {
            let Some(model_name) = opt_cstr(config.model) else {
                set_error(error_out, "local assistant requires a model name");
                return std::ptr::null_mut();
            };
            let Some(key) = api_key.as_deref() else {
                set_error(error_out, "local assistant requires an api_key");
                return std::ptr::null_mut();
            };
            let backing =
                match sequins_assistant::build_backing_model(base_url.as_deref(), &model_name, key)
                {
                    Ok(b) => b,
                    Err(e) => {
                        set_error(error_out, &format!("failed to build backing model: {e}"));
                        return std::ptr::null_mut();
                    }
                };
            let tools = sequins_assistant::Tools::new(Arc::clone(backend));
            let model = sequins_assistant::SequinsAssistantModel::new(backing, tools);
            Assistant::local(model, Some(Arc::clone(storage.app_state())))
        }
        DataSourceImpl::Remote { .. } => {
            let Some(base) = base_url else {
                set_error(error_out, "remote assistant requires a base_url");
                return std::ptr::null_mut();
            };
            Assistant::remote(base, api_key)
        }
    };

    Box::into_raw(Box::new(CAssistant {
        inner: Arc::new(assistant),
    }))
}

/// Free an assistant handle.
///
/// # Safety
/// `assistant` must be a valid pointer from `sequins_assistant_new` (or null).
#[no_mangle]
pub unsafe extern "C" fn sequins_assistant_free(assistant: *mut CAssistant) {
    if !assistant.is_null() {
        drop(Box::from_raw(assistant));
    }
}

/// Start a chat turn. `request_json` is an OpenAI Responses-shaped request
/// (`input`, `tools`, `instructions`, `conversation`/`previous_response_id`, …).
/// Returns a stream handle; events arrive via `vtable` until `on_done`/`on_error`.
///
/// # Safety
/// - `assistant` must be valid for the stream's lifetime.
/// - `request_json` must be valid null-terminated UTF-8.
/// - `vtable` fn pointers and `ctx` must remain valid until the stream is freed.
#[no_mangle]
pub unsafe extern "C" fn sequins_assistant_chat(
    assistant: *mut CAssistant,
    request_json: *const c_char,
    vtable: CAssistantEventVTable,
    ctx: *mut c_void,
) -> *mut CAssistantStream {
    if assistant.is_null() || request_json.is_null() {
        return std::ptr::null_mut();
    }
    let Some(request) = CStr::from_ptr(request_json)
        .to_str()
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
    else {
        return std::ptr::null_mut();
    };

    let inner = Arc::clone(&(*assistant).inner);
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_task = Arc::clone(&cancel);
    let sink = AssistantSink {
        vtable,
        ctx: SendPtr(ctx),
    };

    let task = RUNTIME.spawn(AssertSend(async move {
        let mut stream = inner.chat(request);
        while let Some(event) = stream.next().await {
            if cancel_task.load(Ordering::Relaxed) {
                break;
            }
            dispatch_chat_event(event, &sink);
        }
    }));

    Box::into_raw(Box::new(CAssistantStream {
        cancel,
        _task: task,
    }))
}

/// Best-effort cancel a running chat stream. Still free it with
/// `sequins_assistant_stream_free`.
///
/// # Safety
/// `handle` must be a valid `CAssistantStream*` from `sequins_assistant_chat`.
#[no_mangle]
pub unsafe extern "C" fn sequins_assistant_cancel(handle: *mut CAssistantStream) {
    if handle.is_null() {
        return;
    }
    (*handle).cancel.store(true, Ordering::Relaxed);
    (*handle)._task.abort();
}

/// Free a chat stream handle. Aborts the task and **blocks** until it stops, so no
/// callback fires after the caller's context is freed.
///
/// # Safety
/// `handle` must be a valid `CAssistantStream*` (or null).
#[no_mangle]
pub unsafe extern "C" fn sequins_assistant_stream_free(handle: *mut CAssistantStream) {
    if !handle.is_null() {
        let stream = Box::from_raw(handle);
        stream.cancel.store(true, Ordering::Relaxed);
        stream._task.abort();
        let _ = futures::executor::block_on(stream._task);
    }
}

fn dispatch_chat_event(event: ChatEvent, sink: &AssistantSink) {
    let ctx = sink.ctx.0;
    unsafe {
        match event {
            ChatEvent::Text(t) => {
                if let Some(cb) = sink.vtable.on_text {
                    let c = to_cstring(t);
                    cb(c.as_ptr(), ctx);
                }
            }
            ChatEvent::ServerTool {
                name,
                arguments,
                output,
            } => {
                if let Some(cb) = sink.vtable.on_tool_activity {
                    let (n, a, o) = (to_cstring(name), to_cstring(arguments), to_cstring(output));
                    let activity = CToolActivity {
                        name: n.as_ptr(),
                        arguments: a.as_ptr(),
                        output: o.as_ptr(),
                    };
                    cb(&activity, ctx);
                }
            }
            ChatEvent::ClientTool { name, arguments } => {
                if let Some(cb) = sink.vtable.on_tool_call {
                    let (n, a) = (to_cstring(name), to_cstring(arguments));
                    let call = CToolCall {
                        name: n.as_ptr(),
                        arguments: a.as_ptr(),
                    };
                    cb(&call, ctx);
                }
            }
            ChatEvent::Done {
                response_id,
                conversation_id,
            } => {
                if let Some(cb) = sink.vtable.on_done {
                    let r = to_cstring(response_id);
                    let c = conversation_id.map(to_cstring);
                    let done = CAssistantDone {
                        response_id: r.as_ptr(),
                        conversation_id: c.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
                    };
                    cb(&done, ctx);
                }
            }
            ChatEvent::Error(m) => {
                if let Some(cb) = sink.vtable.on_error {
                    let c = to_cstring(m);
                    cb(c.as_ptr(), ctx);
                }
            }
        }
    }
}
