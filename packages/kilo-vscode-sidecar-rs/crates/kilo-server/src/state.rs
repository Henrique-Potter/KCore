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
    sync::{
        atomic::{AtomicBool, AtomicU8},
        mpsc, Arc, Mutex,
    },
    time::Instant,
};

use kilo_store::Store;
use serde_json::Value;
use tokio::sync::{broadcast, watch, Notify, RwLock, Semaphore};

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
    pub(crate) runner_notify: Notify,
    pub(crate) prompt_queues: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
    pub(crate) prompt_queue_versions: Mutex<BTreeMap<String, u64>>,
    pub(crate) permissions: Mutex<BTreeMap<String, PendingPermission>>,
    pub(crate) approvals: Mutex<Vec<PermissionRule>>,
    pub(crate) questions: Mutex<BTreeMap<String, PendingQuestion>>,
    pub(crate) suggestions: Mutex<BTreeMap<String, PendingSuggestion>>,
    pub(crate) network: Mutex<BTreeMap<String, PendingNetwork>>,
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
    /// Pending re-anchor for the next queued follow-up prompt, keyed by
    /// session id. Set when an active turn breaks via `follow_up_break`
    /// (Bun parity: `KiloSessionPromptQueue.scope()` retargeting). The
    /// next `prompt_turn` for the same session consumes this value once
    /// and overrides the new user message's `parentID` so the queued
    /// follow-up becomes a sibling of the broken turn's user message
    /// instead of a child of the partial assistant. Empty string means
    /// "session root" (no parent). Routes that bypass the runner cannot
    /// observe a stale anchor — `take_broken_turn_anchor` consumes it.
    pub(crate) broken_turn_anchors: Mutex<BTreeMap<String, String>>,
    /// Audit Fix 3: pending OAuth flows keyed by provider id. The Bun
    /// SDK's `oauth_callback` only echoes back `{ method, code, state }`
    /// — the verifier is server-side. We populate this map on
    /// `oauth_authorize` and consume it on `oauth_callback`.
    pub(crate) oauth_pending: Mutex<BTreeMap<String, PendingAuth>>,
    /// Single-flight OpenAI OAuth refresh. Refresh tokens can rotate; parallel
    /// turns/subagents must not all spend the same old refresh token.
    pub(crate) oauth_refresh: tokio::sync::Mutex<()>,
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

    pub(crate) fn prompt_queue(&self, sid: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut queues = self.prompt_queues.lock().unwrap();
        queues
            .entry(sid.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub(crate) fn prompt_queue_version(&self, sid: &str) -> u64 {
        self.prompt_queue_versions
            .lock()
            .unwrap()
            .get(sid)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn prompt_queue_current(&self, sid: &str, version: u64) -> bool {
        self.prompt_queue_version(sid) == version
    }

    pub(crate) fn cancel_prompt_queue(&self, sid: &str) {
        let mut versions = self.prompt_queue_versions.lock().unwrap();
        let next = versions.get(sid).copied().unwrap_or(0).saturating_add(1);
        versions.insert(sid.to_string(), next);
        self.runner_notify.notify_waiters();
    }

    pub(crate) fn remove_prompt_queue(&self, sid: &str) {
        self.prompt_queues.lock().unwrap().remove(sid);
        self.prompt_queue_versions.lock().unwrap().remove(sid);
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

    /// Record the parent message id the next queued follow-up should
    /// re-anchor under. Called from the agent turn loop when a runner
    /// breaks via `follow_up_break` and the partial assistant has been
    /// finalized. The empty string is a valid anchor — it means "session
    /// root" (the broken user message had no parent).
    pub(crate) fn set_broken_turn_anchor(&self, sid: &str, parent_id: &str) {
        self.broken_turn_anchors
            .lock()
            .unwrap()
            .insert(sid.to_string(), parent_id.to_string());
    }

    /// Consume the pending re-anchor for the given session (returns
    /// `Some(parent_id)` once and clears it, or `None` when no anchor
    /// is pending). The take semantics ensure only the first follow-up
    /// after a break inherits the broken turn's parentage; subsequent
    /// turns use natural parentage.
    pub(crate) fn take_broken_turn_anchor(&self, sid: &str) -> Option<String> {
        self.broken_turn_anchors.lock().unwrap().remove(sid)
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

    /// True when at least one pending network wait is registered for the
    /// given session. The OpenAI streaming path consults this before
    /// asking, so rapid successive transport failures within a single
    /// turn don't fan out into a queue of duplicate `session.network.asked`
    /// events for the webview. Mirrors Bun's `state.pending` lookup
    /// inside `SessionNetwork.ask` (single in-flight per session in
    /// practice; see `session/network.ts:179-226`).
    pub(crate) fn has_network_wait_for_session(&self, sid: &str) -> bool {
        let Ok(guard) = self.network.lock() else {
            return false;
        };
        guard.values().any(|wait| {
            wait.info
                .get("sessionID")
                .and_then(Value::as_str)
                .map(|s| s == sid)
                .unwrap_or(false)
        })
    }

    /// Direct add of a network-wait entry, bypassing the
    /// `routes::network::ask_network_wait` flow. Returns the generated
    /// id. The wait carries no resolver — callers using this entrypoint
    /// own their own retry/resume logic and reclaim the entry via
    /// [`AppState::take_network_wait`]. Existing `ask_network_wait`
    /// callers retain the channel-based flow; this helper exists for
    /// state-side machinery (and the unit test below).
    #[cfg(test)]
    pub(crate) fn add_network_wait(&self, sid: &str, reason: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        use std::time::{SystemTime, UNIX_EPOCH};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let created = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let seq = SEQ.fetch_add(1, AtomicOrdering::SeqCst);
        let id = format!("network_{created}_{seq}");
        let info = serde_json::json!({
            "id": id,
            "sessionID": sid,
            "message": reason,
            "restored": false,
            "time": { "created": created },
        });
        self.network
            .lock()
            .unwrap()
            .insert(id.clone(), PendingNetwork { info, reply: None });
        id
    }

    /// Remove and return a network-wait entry by id. Used to drain the
    /// state when the agent loop confirms the wait has resolved (the
    /// resolver path on `ask_network_wait` already removes from the map
    /// — this is for [`AppState::add_network_wait`] users).
    #[cfg(test)]
    pub(crate) fn take_network_wait(&self, id: &str) -> Option<PendingNetwork> {
        self.network.lock().unwrap().remove(id)
    }

    /// Drop every wait belonging to the given session. Called from
    /// turn cleanup so an aborted/failed turn doesn't leave dangling
    /// waits visible to `/network`. Returns the count cleared so the
    /// caller can decide whether to publish a `restored`/`rejected`
    /// event chain (the reply machinery handles its own events).
    pub(crate) fn clear_network_waits_for_session(&self, sid: &str) -> usize {
        let mut guard = self.network.lock().unwrap();
        let drained: Vec<String> = guard
            .iter()
            .filter(|(_, wait)| {
                wait.info
                    .get("sessionID")
                    .and_then(Value::as_str)
                    .map(|s| s == sid)
                    .unwrap_or(false)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &drained {
            guard.remove(id);
        }
        drained.len()
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

pub(crate) struct McpChild {
    pub(crate) child: Child,
    pub(crate) stdin: ChildStdin,
    pub(crate) rx: mpsc::Receiver<Value>,
    pub(crate) tools_changed: bool,
}

pub(crate) struct Runner {
    pub(crate) cancel: Arc<AtomicBool>,
    /// Mid-loop follow-up break signal. When a same-session
    /// `prompt_async` arrives while this runner is active, the route
    /// trips both this flag and `cancel`. The agent loop observes
    /// `cancel` and finalizes the partial assistant message; the
    /// `dismiss_question_suggestion_waits` / `reject_pending` fan-out
    /// is gated on this flag so the queued follow-up turn keeps the
    /// session's pending UI waits intact (Bun parity:
    /// `kilocode/session/prompt-queue.ts:hasFollowup`). Routes that
    /// want a hard user abort (`abort_session`) leave this `false`
    /// so the existing reject/cascade path runs.
    pub(crate) follow_up_break: Arc<AtomicBool>,
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
    /// Maximum consecutive mid-stream retries within a single iteration
    /// before terminal failure. Incremented when the OpenAI Responses
    /// stream errors AFTER content (text deltas / reasoning / tool
    /// calls) has streamed within a single iteration. Resets to 0 on
    /// every clean iteration completion (so each iteration's burst gets
    /// its own budget), on a user-driven abort (without
    /// `follow_up_break`), and after exhausting the cap so the next
    /// iteration starts fresh. Capped at
    /// [`crate::agent::openai_stream::MID_STREAM_RETRY_CAP`]; beyond
    /// that the partial assistant message is finalized with a terminal
    /// error envelope.
    ///
    /// Note: The field name implies a per-turn cap, but the
    /// implementation in `openai_stream.rs` resets per iteration.
    /// Aligning to a per-turn cap would require moving the reset out
    /// of the per-iteration success path.
    pub(crate) mid_stream_retries: Arc<AtomicU8>,
}

pub(crate) struct RunnerGuard {
    pub(crate) state: Arc<AppState>,
    pub(crate) id: String,
    pub(crate) cancel: Arc<AtomicBool>,
}

impl Drop for RunnerGuard {
    fn drop(&mut self) {
        self.state.runners.lock().unwrap().remove(&self.id);
        // Clear the per-session agent selection and resolved hard-rule
        // veto layer so plan-mode hard rules don't leak into routes that
        // run after the turn ends (e.g. a follow-up read tool on an
        // already-finished session).
        self.state.set_session_agent(&self.id, None);
        self.state.runner_notify.notify_waiters();
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

pub(crate) struct PendingNetwork {
    pub(crate) info: Value,
    pub(crate) reply: Option<tokio::sync::oneshot::Sender<Result<(), String>>>,
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

/// Set by `routes::sessions::set_viewed` from VS Code focus/open events.
/// Production callers only write this state; the read side is reserved
/// for the M11+ session-resume / mirror filtering pass and tests round
/// it back through `viewed_snapshot`. Fields stay non-`#[cfg(test)]`
/// because production constructs them every PATCH, but rustc reports
/// them as never-read absent the test build.
#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ViewedState {
    pub(crate) focused: BTreeSet<String>,
    pub(crate) open: BTreeSet<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::OPENAI_ISSUER;
    use std::net::SocketAddr;
    use tokio::sync::broadcast;

    fn fixture() -> Arc<AppState> {
        let (bus, _) = broadcast::channel(16);
        Arc::new(AppState {
            username: "kilo".to_string(),
            password: None,
            store: kilo_store::Store::new(),
            bus,
            viewed: RwLock::default(),
            runners: Mutex::default(),
            runner_notify: Notify::new(),
            prompt_queues: Mutex::default(),
            prompt_queue_versions: Mutex::default(),
            permissions: Mutex::default(),
            approvals: Mutex::default(),
            questions: Mutex::default(),
            suggestions: Mutex::default(),
            network: Mutex::default(),
            mcp: Mutex::default(),
            mcp_configs: Mutex::default(),
            mcp_children: Mutex::default(),
            pty: Mutex::default(),
            plugin_tools: Mutex::default(),
            session_agents: Mutex::default(),
            session_hard_rules: Mutex::default(),
            broken_turn_anchors: Mutex::default(),
            oauth_pending: Mutex::default(),
            oauth_refresh: tokio::sync::Mutex::new(()),
            oauth_listener: Mutex::default(),
            oauth_listener_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            oauth_token_endpoint: format!("{OPENAI_ISSUER}/oauth/token"),
            sse_capacity: AppState::new_sse_capacity(),
        })
    }

    #[test]
    fn network_wait_lifecycle_add_take_clear() {
        let state = fixture();
        assert!(!state.has_network_wait_for_session("sid_a"));

        let id_a = state.add_network_wait("sid_a", "Connection refused");
        assert!(state.has_network_wait_for_session("sid_a"));
        assert!(!state.has_network_wait_for_session("sid_b"));

        let id_b = state.add_network_wait("sid_b", "DNS lookup failed");
        assert!(state.has_network_wait_for_session("sid_b"));

        // `take` returns the entry once and clears it.
        let taken = state.take_network_wait(&id_a).expect("entry present");
        assert_eq!(
            taken.info.get("sessionID").and_then(Value::as_str),
            Some("sid_a"),
        );
        assert!(state.take_network_wait(&id_a).is_none());
        assert!(!state.has_network_wait_for_session("sid_a"));

        // `clear_*` drops every wait for the named session and reports
        // how many it removed.
        let _ = id_b;
        let cleared = state.clear_network_waits_for_session("sid_b");
        assert_eq!(cleared, 1);
        assert!(!state.has_network_wait_for_session("sid_b"));
        // Idempotent on an empty session.
        assert_eq!(state.clear_network_waits_for_session("sid_b"), 0);
    }

    /// `RunnerGuard::drop` must clear both `session_agents` and the
    /// resolved `session_hard_rules` entry so plan-mode hard rules
    /// don't leak past turn end (e.g. a follow-up read tool on the
    /// finished session). Regression for the lifecycle gap where only
    /// the runner-map entry was removed on drop.
    #[test]
    fn runner_guard_drop_clears_session_agent_and_hard_rules() {
        let state = fixture();
        let sid = "ses_a";
        // Pre-load the agent state the way `prompt_turn` would.
        state.set_session_agent(sid, Some("plan"));
        assert!(state.session_agents.lock().unwrap().contains_key(sid));
        // The `plan` agent intentionally yields no rules in the bare
        // fixture (no config loaded), but the map entry exists with a
        // (possibly empty) Vec — the leak check is about the entry's
        // presence, not its contents.
        assert!(state.session_hard_rules.lock().unwrap().contains_key(sid));

        {
            let _guard = RunnerGuard {
                state: state.clone(),
                id: sid.to_string(),
                cancel: Arc::new(AtomicBool::new(false)),
            };
            // Guard still alive — entries remain.
            assert!(state.session_agents.lock().unwrap().contains_key(sid));
        }
        // Drop fired — both maps cleared for this session.
        assert!(!state.session_agents.lock().unwrap().contains_key(sid));
        assert!(!state.session_hard_rules.lock().unwrap().contains_key(sid));
    }
}
