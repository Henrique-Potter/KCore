//! Route handlers for /network*.
//!
//! Bun exposes these endpoints so clients can drain offline reconnect waits
//! before destructive operations such as config save. Rust keeps the state
//! in-memory like the other per-turn wait surfaces; the agent loop only
//! asks here before any provider side effects have streamed, so a reply can
//! safely retry without replaying text or tools.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::GlobalEvent;
use serde_json::{json, Value};

use crate::{http::sse, AppState, PendingNetwork};

static IDS: AtomicU64 = AtomicU64::new(0);

/// Default deadline before an unanswered wait gives up. Mirrors the
/// JJ wave-9 follow-up: a wait must not block the agent loop forever
/// when the user never replies and connectivity does not return.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Default probe poll interval. Bun uses ~5s (`POLL_MS` in
/// `session/network.ts`). DNS lookups are cheap so this stays equal.
const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// Default DNS target for the auto-restore probe. Bun probes
/// `dns.google` for the same purpose; we mirror that here so a
/// reachable DNS resolver is interpreted as connectivity returning.
const DEFAULT_PROBE_TARGET: &str = "dns.google:443";

/// Tunables for [`ask_network_wait_with_options`]. Tests override the
/// timeout and probe target to keep cases under a second; production
/// uses the defaults above.
#[derive(Clone, Debug)]
pub(crate) struct NetworkWaitOptions {
    pub(crate) timeout: Duration,
    pub(crate) probe_interval: Duration,
    pub(crate) probe_target: Option<String>,
}

impl Default for NetworkWaitOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            probe_interval: DEFAULT_PROBE_INTERVAL,
            probe_target: Some(DEFAULT_PROBE_TARGET.to_string()),
        }
    }
}

pub(crate) async fn network_waits(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let waits = state
        .network
        .lock()
        .unwrap()
        .values()
        .map(|wait| wait.info.clone())
        .collect::<Vec<_>>();
    Json(waits)
}

pub(crate) async fn reply_network_wait(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    complete_network_wait(&state, &id, true)
}

pub(crate) async fn reject_network_wait(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    complete_network_wait(&state, &id, false)
}

pub(crate) async fn ask_network_wait(
    state: &Arc<AppState>,
    session: &str,
    message: String,
    cancel: &AtomicBool,
) -> Result<(), String> {
    ask_network_wait_with_options(
        state,
        session,
        message,
        cancel,
        NetworkWaitOptions::default(),
    )
    .await
}

/// Same as [`ask_network_wait`] but with a configurable timeout and
/// probe target. The wait completes when any of the following occurs:
///
/// - The user replies via `POST /network/{id}/reply` (Ok).
/// - The user rejects via `POST /network/{id}/reject` (Err).
/// - The DNS probe succeeds — auto-restored (Ok).
/// - The deadline elapses — treated as a hard network failure
///   (`Err("network wait timed out")`).
/// - `cancel` flips — `Err("aborted")`.
///
/// On either auto-resolution path the pending entry is drained and
/// the matching `session.network.{restored,rejected}` envelope is
/// published so the webview clears its offline UI.
pub(crate) async fn ask_network_wait_with_options(
    state: &Arc<AppState>,
    session: &str,
    message: String,
    cancel: &AtomicBool,
    opts: NetworkWaitOptions,
) -> Result<(), String> {
    let id = network_id();
    let info = json!({
        "id": id,
        "sessionID": session,
        "message": message,
        "restored": false,
        "time": { "created": now_ms() },
    });
    let (tx, rx) = tokio::sync::oneshot::channel();
    state.network.lock().unwrap().insert(
        id.clone(),
        PendingNetwork {
            info: info.clone(),
            reply: Some(tx),
        },
    );
    sse::publish(state, GlobalEvent::bus("session.network.asked", info));

    // Restore probe: a sibling task that polls DNS and, on the first
    // successful lookup, drains the wait via the same code path as a
    // user reply (so the SSE envelope and channel resolution stay
    // observable in one place). The probe is aborted when this
    // function returns, so it doesn't need its own cancel handle —
    // the outer `cancel` check below tears it down on the next
    // poll-cycle wakeup.
    let probe_handle = opts.probe_target.clone().map(|target| {
        let state = state.clone();
        let id = id.clone();
        let interval = opts.probe_interval;
        tokio::spawn(probe_loop(state, id, target, interval))
    });

    let wait = async move {
        rx.await
            .unwrap_or_else(|_| Err("Network wait closed".to_string()))
    };
    tokio::pin!(wait);
    let deadline = tokio::time::sleep(opts.timeout);
    tokio::pin!(deadline);
    let outcome = loop {
        if cancel.load(Ordering::SeqCst) {
            reject_network_wait_id(state, &id, "Network wait aborted");
            break Err("aborted".to_string());
        }
        tokio::select! {
            out = &mut wait => break out,
            _ = &mut deadline => {
                reject_network_wait_id(state, &id, "Network wait timed out");
                break Err("network wait timed out".to_string());
            }
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
    };
    if let Some(handle) = probe_handle {
        handle.abort();
    }
    outcome
}

/// DNS probe loop. Polls `target` (a `host:port` string) every
/// `interval` and, on the first resolved address, marks the wait as
/// restored via [`restore_network_wait_id`]. Stops as soon as the
/// pending entry is gone (the reply route, the timeout, or the
/// cancel path has already drained it).
async fn probe_loop(state: Arc<AppState>, id: String, target: String, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        if !state.network.lock().unwrap().contains_key(&id) {
            return;
        }
        let ok = tokio::net::lookup_host(target.as_str())
            .await
            .map(|mut iter| iter.next().is_some())
            .unwrap_or(false);
        if !ok {
            continue;
        }
        if !state.network.lock().unwrap().contains_key(&id) {
            return;
        }
        restore_network_wait_id(&state, &id);
        return;
    }
}

pub(crate) fn disconnected(err: &kilo_provider::ProviderError) -> bool {
    match err {
        kilo_provider::ProviderError::Http(err) | kilo_provider::ProviderError::Response(err) => {
            let msg = err.to_lowercase();
            msg.contains("connection reset")
                || msg.contains("connection refused")
                || msg.contains("dns")
                || msg.contains("timed out")
                || msg.contains("timeout")
                || msg.contains("network is unreachable")
                || msg.contains("network request failed")
                || msg.contains("fetch failed")
                || msg.contains("unable to connect")
        }
        _ => false,
    }
}

pub(crate) fn message(err: &kilo_provider::ProviderError) -> String {
    let raw = err.to_string();
    let lower = raw.to_lowercase();
    if lower.contains("connection reset") {
        return "Connection reset by server".to_string();
    }
    if lower.contains("connection refused") {
        return "Connection refused".to_string();
    }
    if lower.contains("dns") {
        return "DNS lookup failed".to_string();
    }
    if lower.contains("timed out") || lower.contains("timeout") {
        return "Connection timed out".to_string();
    }
    if lower.contains("network is unreachable") {
        return "Network is unreachable".to_string();
    }
    "Network request failed".to_string()
}

fn complete_network_wait(state: &AppState, id: &str, allow: bool) -> Response {
    let mut wait = match state.network.lock().unwrap().remove(id) {
        Some(wait) => wait,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let sid = wait
        .info
        .get("sessionID")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let kind = if allow {
        "session.network.replied"
    } else {
        "session.network.rejected"
    };
    if let Some(tx) = wait.reply.take() {
        let _ = tx.send(if allow {
            Ok(())
        } else {
            Err("Network reconnect was rejected".to_string())
        });
    }
    sse::publish(
        state,
        GlobalEvent::bus(kind, json!({ "sessionID": sid, "requestID": id })),
    );
    Json(true).into_response()
}

fn restore_network_wait_id(state: &AppState, id: &str) {
    let Some(mut wait) = state.network.lock().unwrap().remove(id) else {
        return;
    };
    let sid = wait
        .info
        .get("sessionID")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if let Some(tx) = wait.reply.take() {
        let _ = tx.send(Ok(()));
    }
    sse::publish(
        state,
        GlobalEvent::bus(
            "session.network.restored",
            json!({ "sessionID": sid, "requestID": id }),
        ),
    );
}

fn reject_network_wait_id(state: &AppState, id: &str, message: &str) {
    let Some(mut wait) = state.network.lock().unwrap().remove(id) else {
        return;
    };
    let sid = wait
        .info
        .get("sessionID")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if let Some(tx) = wait.reply.take() {
        let _ = tx.send(Err(message.to_string()));
    }
    sse::publish(
        state,
        GlobalEvent::bus(
            "session.network.rejected",
            json!({ "sessionID": sid, "requestID": id }),
        ),
    );
}

fn network_id() -> String {
    let seq = IDS.fetch_add(1, Ordering::SeqCst);
    format!("network_{}_{}", now_ms(), seq)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use tokio::sync::broadcast;

    #[test]
    fn disconnected_matches_transport_errors_only() {
        assert!(disconnected(&kilo_provider::ProviderError::Http(
            "connection refused".to_string()
        )));
        assert!(!disconnected(&kilo_provider::ProviderError::Api(
            "bad request".to_string()
        )));
    }

    /// Minimal AppState for the wait-loop tests below. Mirrors the
    /// shape of `tests/common.rs::state_at_with` but lives inside
    /// this module so the unit tests stay self-contained.
    fn test_state() -> Arc<AppState> {
        let (bus, _) = broadcast::channel(64);
        Arc::new(AppState {
            username: "kilo".to_string(),
            password: None,
            store: kilo_store::Store::new(),
            bus,
            viewed: tokio::sync::RwLock::default(),
            runners: Mutex::new(Default::default()),
            runner_notify: tokio::sync::Notify::new(),
            prompt_queues: Mutex::new(Default::default()),
            prompt_queue_versions: Mutex::new(Default::default()),
            permissions: Mutex::new(Default::default()),
            approvals: Mutex::new(Default::default()),
            questions: Mutex::new(Default::default()),
            suggestions: Mutex::new(Default::default()),
            network: Mutex::new(Default::default()),
            mcp: Mutex::new(Default::default()),
            mcp_configs: Mutex::new(Default::default()),
            mcp_children: Mutex::new(Default::default()),
            pty: Mutex::new(Default::default()),
            plugin_tools: Mutex::new(Default::default()),
            session_agents: Mutex::new(Default::default()),
            session_hard_rules: Mutex::new(Default::default()),
            broken_turn_anchors: Mutex::new(Default::default()),
            oauth_pending: Mutex::new(Default::default()),
            oauth_listener: Mutex::new(Default::default()),
            oauth_listener_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            oauth_token_endpoint: "https://example.invalid/oauth/token".to_string(),
            sse_capacity: AppState::new_sse_capacity(),
        })
    }

    #[tokio::test]
    async fn network_wait_timeout_after_60_seconds_returns_rejected() {
        // Use a 100ms timeout override so the test stays fast. Probe
        // points at a domain that should never resolve so the
        // restoration path can't accidentally win the race.
        let state = test_state();
        let cancel = AtomicBool::new(false);
        let opts = NetworkWaitOptions {
            timeout: Duration::from_millis(100),
            probe_interval: Duration::from_secs(60),
            probe_target: Some("nonexistent.invalid.kilo.test:443".to_string()),
        };
        let started = std::time::Instant::now();
        let outcome = ask_network_wait_with_options(
            &state,
            "ses_t",
            "Connection refused".into(),
            &cancel,
            opts,
        )
        .await;
        let elapsed = started.elapsed();
        assert!(outcome.is_err(), "expected timeout to surface as Err");
        let err = outcome.unwrap_err();
        assert!(
            err.contains("timed out"),
            "expected timeout reason in error, got `{err}`"
        );
        assert!(
            elapsed >= Duration::from_millis(80),
            "wait returned before deadline: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "wait honored the 60s default instead of override: {elapsed:?}"
        );
        // Pending entry must be drained on the timeout path so the
        // route's GET /network list does not show stale waits.
        assert!(state.network.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn network_wait_probe_restores_when_dns_succeeds() {
        // Point the probe at localhost — DNS resolution always
        // succeeds for `127.0.0.1`, so the probe wins the race
        // before either the user reply or the timeout deadline.
        let state = test_state();
        let cancel = AtomicBool::new(false);
        let opts = NetworkWaitOptions {
            timeout: Duration::from_secs(5),
            probe_interval: Duration::from_millis(50),
            probe_target: Some("127.0.0.1:443".to_string()),
        };
        let mut rx = state.bus.subscribe();
        let outcome = ask_network_wait_with_options(
            &state,
            "ses_p",
            "DNS lookup failed".into(),
            &cancel,
            opts,
        )
        .await;
        assert!(
            outcome.is_ok(),
            "expected probe to resolve wait as Ok, got {outcome:?}"
        );
        assert!(state.network.lock().unwrap().is_empty());

        // Confirm the SSE envelope flipped to `restored` so the
        // webview can clear its offline UI. We accept any of the
        // bus frames carrying that kind — the bus also publishes
        // the `asked` event before this one.
        let mut saw_restored = false;
        while let Ok(frame) = rx.try_recv() {
            let ev = frame.as_global();
            if ev.payload.kind == "session.network.restored" {
                saw_restored = true;
                break;
            }
        }
        assert!(
            saw_restored,
            "expected `session.network.restored` SSE envelope after probe success"
        );
    }

    #[tokio::test]
    async fn network_wait_cancel_short_circuits() {
        // Cancel flips immediately; the wait must return promptly
        // with `aborted` and drain the pending entry. Guards the
        // existing cancel-safety contract under the new timeout
        // wrapper so the wave-9 change doesn't regress it.
        let state = test_state();
        let cancel = AtomicBool::new(true);
        let opts = NetworkWaitOptions {
            timeout: Duration::from_secs(60),
            probe_interval: Duration::from_secs(60),
            probe_target: None,
        };
        let outcome = ask_network_wait_with_options(
            &state,
            "ses_c",
            "Connection reset".into(),
            &cancel,
            opts,
        )
        .await;
        assert_eq!(outcome.unwrap_err(), "aborted");
        assert!(state.network.lock().unwrap().is_empty());
    }
}
