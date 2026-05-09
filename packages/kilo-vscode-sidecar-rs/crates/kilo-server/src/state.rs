//! Shared state types for the kilo-server crate.
//!
//! `AppState` is the central handle threaded through every route handler and
//! agent helper. The other types in this module are owned by `AppState` (or
//! its peer `RunnerGuard`) and split out for readability only — they have no
//! independent purpose.
//!
//! Field visibility is `pub(crate)` so handlers, the SSE bus, the agent
//! turn loop, and the OAuth flow can read/mutate state directly. The crate
//! is the encapsulation boundary; this module does not present a façade.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    process::{Child, ChildStdin},
    sync::{atomic::AtomicBool, mpsc, Arc, Mutex},
    time::Instant,
};

use kilo_store::Store;
use serde_json::Value;
use tokio::sync::{broadcast, watch, RwLock, Semaphore};

use crate::http::sse::BusEvent;
use crate::limits::MAX_SSE_CLIENTS;

pub(crate) struct AppState {
    pub(crate) username: String,
    pub(crate) password: Option<String>,
    pub(crate) store: Store,
    /// Pre-serialized SSE bus. Each broadcast `BusEvent` carries the
    /// already-JSON-serialized bytes for both the global and instance
    /// stream shapes; subscribers no longer re-walk the JSON tree per
    /// event. See `http::sse::BusEvent` for the design notes.
    pub(crate) bus: broadcast::Sender<BusEvent>,
    pub(crate) viewed: RwLock<ViewedState>,
    pub(crate) runners: Mutex<BTreeMap<String, Runner>>,
    pub(crate) permissions: Mutex<BTreeMap<String, PendingPermission>>,
    pub(crate) approvals: Mutex<Vec<PermissionRule>>,
    pub(crate) questions: Mutex<BTreeMap<String, PendingQuestion>>,
    pub(crate) suggestions: Mutex<BTreeMap<String, PendingSuggestion>>,
    pub(crate) mcp: Mutex<kilo_mcp::StatusMap>,
    pub(crate) mcp_configs: Mutex<BTreeMap<String, kilo_mcp::Config>>,
    pub(crate) mcp_children: Mutex<BTreeMap<String, McpChild>>,
    /// Active PTY sessions keyed by id (`crate::routes::pty::PtyHandle`).
    /// Spawned by `POST /pty`, killed on `DELETE /pty/:id`. Output streams
    /// onto the SSE bus as `pty.output` events.
    pub(crate) pty: Mutex<crate::routes::pty::PtyMap>,
    /// Registered plugin tools (Bun parity: `plugin.tool()` decoration).
    /// Populated by callers via [`AppState::register_plugin_tool`] before
    /// `serve()` opens the listener; merged into `real_tools` and
    /// dispatched by `real_tool_part`.
    pub(crate) plugin_tools: Mutex<Vec<crate::agent::plugin::PluginTool>>,
    /// Per-session selected agent name. Set by `prompt_turn` from
    /// `PromptInput.agent` before the loop opens; consulted by
    /// `agent::permission::ask_permission` to derive the agent's
    /// `hardRuleset` (Bun parity:
    /// `packages/opencode/src/kilocode/session/prompt.ts:60-72`).
    pub(crate) session_agents: Mutex<BTreeMap<String, String>>,
    /// Per-turn hard permission rules keyed by session id. Resolved once
    /// at turn start so every gated tool does not re-read agent config.
    pub(crate) session_hard_rules: Mutex<BTreeMap<String, Vec<PermissionRule>>>,
    /// Audit Fix 3: pending OAuth flows keyed by provider id. The Bun
    /// SDK's `oauth_callback` only echoes back `{ method, code, state }`
    /// — the verifier is server-side. We populate this map on
    /// `oauth_authorize` and consume it on `oauth_callback`.
    pub(crate) oauth_pending: Mutex<BTreeMap<String, PendingAuth>>,
    pub(crate) oauth_listener: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub(crate) oauth_listener_addr: SocketAddr,
    pub(crate) oauth_token_endpoint: String,
    /// Concurrent SSE-stream cap (Operational invariant 5). Each
    /// `/global/event` and `/event` connection acquires a permit for the
    /// life of the stream; clients beyond the cap receive 503
    /// `sse_capacity_exceeded`. `Arc` so individual handlers can hold
    /// `OwnedSemaphorePermit`s past the request future without holding the
    /// state lock.
    pub(crate) sse_capacity: Arc<Semaphore>,
}

impl AppState {
    pub(crate) fn mutating_tools_enabled(&self) -> bool {
        self.permission_gate_active()
    }

    pub(crate) fn permission_gate_active(&self) -> bool {
        true
    }

    pub(crate) fn new_sse_capacity() -> Arc<Semaphore> {
        Arc::new(Semaphore::new(MAX_SSE_CLIENTS))
    }

    /// Record (or clear) the agent name currently driving the given
    /// session. Called once per turn; the `RunnerGuard` clears it on drop.
    pub(crate) fn set_session_agent(&self, sid: &str, agent: Option<&str>) {
        let hard = agent
            .filter(|name| !name.is_empty())
            .map(|name| self.agent_hard_rules(name));
        let mut guard = self.session_agents.lock().unwrap();
        match agent {
            Some(name) if !name.is_empty() => {
                guard.insert(sid.to_string(), name.to_string());
                self.session_hard_rules
                    .lock()
                    .unwrap()
                    .insert(sid.to_string(), hard.unwrap_or_default());
            }
            _ => {
                guard.remove(sid);
                self.session_hard_rules.lock().unwrap().remove(sid);
            }
        }
    }

    /// Resolve the current agent name for a session, if any was set.
    pub(crate) fn session_agent(&self, sid: &str) -> Option<String> {
        self.session_agents.lock().unwrap().get(sid).cloned()
    }

    pub(crate) fn session_hard_rules(&self, sid: &str) -> Vec<PermissionRule> {
        self.session_hard_rules
            .lock()
            .unwrap()
            .get(sid)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn agent_info(&self, agent: &str) -> Option<Value> {
        crate::agent::catalog::get(self, agent)
    }

    pub(crate) fn agent_permission_rules(&self, agent: &str) -> Vec<crate::PermissionRule> {
        self.agent_info(agent)
            .and_then(|value| value.get("permission").cloned())
            .map(|value| permission_rules_from_value(&value))
            .unwrap_or_default()
    }

    /// Hard permission rules for an agent: rules the user's "always"
    /// approvals cannot unlock. Matches Bun's `hardPermissions` policy
    /// (`kilocode/session/prompt.ts:60-72`) — the agent's declared
    /// `permission` block, used as a veto layer for `ask`/`plan` agents.
    /// Returns an empty Vec when the agent doesn't exist or has no
    /// rules. Source: the resolved config under `agent.<name>.permission`.
    pub(crate) fn agent_hard_rules(&self, agent: &str) -> Vec<crate::PermissionRule> {
        if !matches!(agent, "ask" | "plan") {
            // Only the constraint modes get a hard veto layer in Bun.
            return Vec::new();
        }
        self.agent_permission_rules(agent)
    }
}

fn permission_rules_from_value(value: &Value) -> Vec<crate::PermissionRule> {
    let mut out = Vec::new();
    if let Some(items) = value.as_array() {
        for item in items {
            let Some(permission) = item.get("permission").and_then(Value::as_str) else {
                continue;
            };
            let Some(action) = item.get("action").and_then(Value::as_str) else {
                continue;
            };
            out.push(crate::PermissionRule {
                permission: permission.to_string(),
                pattern: item
                    .get("pattern")
                    .and_then(Value::as_str)
                    .unwrap_or("*")
                    .to_string(),
                action: action.to_string(),
            });
        }
        return out;
    }
    let Some(map) = value.as_object() else {
        return out;
    };
    for (perm, rule_value) in map {
        if let Some(action) = rule_value.as_str() {
            out.push(crate::PermissionRule {
                permission: perm.clone(),
                pattern: "*".to_string(),
                action: action.to_string(),
            });
            continue;
        }
        if let Some(items) = rule_value.as_object() {
            for (pattern, action) in items {
                if let Some(action) = action.as_str() {
                    out.push(crate::PermissionRule {
                        permission: perm.clone(),
                        pattern: pattern.clone(),
                        action: action.to_string(),
                    });
                }
            }
        }
    }
    out
}

/// Audit Fix 3: per-flow pending OAuth state. Verifier + state are stored
/// server-side, keyed by provider id.
#[derive(Clone)]
pub(crate) struct PendingAuth {
    pub(crate) verifier: String,
    pub(crate) state: String,
    pub(crate) expires_at: Instant,
    pub(crate) complete: watch::Sender<Option<Result<(), String>>>,
}

#[allow(dead_code)]
pub(crate) struct McpChild {
    pub(crate) child: Child,
    pub(crate) stdin: ChildStdin,
    pub(crate) rx: mpsc::Receiver<Value>,
    pub(crate) tools_changed: bool,
}

pub(crate) struct Runner {
    pub(crate) cancel: Arc<AtomicBool>,
    /// Parent runner for live delegated work. Store-backed children cover
    /// normal task sessions, but this active edge lets abort propagate even
    /// while a task is inside the non-Send child runtime.
    pub(crate) parent: Option<String>,
    /// Abort handle for the spawned turn task (async path only). Set
    /// after `tokio::spawn` returns so `abort_session` can preempt a
    /// task suspended on a non-cooperative `.await` (OAuth refresh, slow
    /// plugin handler, etc.). The sync `prompt` path leaves this `None`
    /// — its future is owned by the calling task.
    pub(crate) abort: Mutex<Option<tokio::task::AbortHandle>>,
}

pub(crate) struct RunnerGuard {
    pub(crate) state: Arc<AppState>,
    pub(crate) id: String,
    pub(crate) cancel: Arc<AtomicBool>,
}

impl Drop for RunnerGuard {
    fn drop(&mut self) {
        self.state.runners.lock().unwrap().remove(&self.id);
    }
}

pub(crate) struct PendingPermission {
    pub(crate) info: Value,
    pub(crate) reply: tokio::sync::oneshot::Sender<PermissionDecision>,
}

/// Outstanding question awaiting a UI reply. Mirrors
/// [`PendingPermission`]. The agent loop (or a tool that wants to
/// surface a clarifying prompt) creates this via
/// [`crate::agent::permission::ask_question`]; the HTTP
/// `reply_question` / `reject_question` routes consume the sender to
/// resolve the awaiting future.
pub(crate) struct PendingQuestion {
    pub(crate) info: Value,
    pub(crate) reply: tokio::sync::oneshot::Sender<QuestionReply>,
}

/// What the user replied with. `Answers` carries the typed selection(s);
/// `Rejected` means the user dismissed the prompt without picking.
#[derive(Clone, Debug)]
pub(crate) enum QuestionReply {
    Answers(Value),
    Rejected,
}

pub(crate) struct PendingSuggestion {
    pub(crate) info: Value,
    pub(crate) reply: tokio::sync::oneshot::Sender<SuggestionDecision>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SuggestionDecision {
    Accept(usize),
    Dismiss,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PermissionDecision {
    Allow,
    Always,
    Reject,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PermissionRule {
    pub(crate) permission: String,
    pub(crate) pattern: String,
    pub(crate) action: String,
}

impl PermissionRule {
    /// Decode rules persisted on disk into typed `PermissionRule`s,
    /// silently dropping malformed entries (so an externally-edited
    /// `permissions.json` can't poison the in-memory ruleset).
    pub(crate) fn from_persisted(values: Vec<Value>) -> Vec<Self> {
        values
            .into_iter()
            .filter_map(|value| serde_json::from_value(value).ok())
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FakeCall {
    pub(crate) tool: String,
    pub(crate) input: Value,
    pub(crate) delay: u64,
    pub(crate) invalid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Repair {
    Valid(String),
    Invalid(String),
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ViewedState {
    pub(crate) focused: BTreeSet<String>,
    pub(crate) open: BTreeSet<String>,
}
