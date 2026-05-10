use std::{
    error::Error,
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use kilo_store::Store;
use tokio::{net::TcpListener, sync::broadcast, sync::RwLock};

#[derive(Clone)]
pub struct ServeOptions {
    pub hostname: String,
    pub port: u16,
}

mod agent;
mod error;
mod http;
mod limits;
mod lock;
mod oauth;
mod registry;
mod routes;
mod snapshot;
mod state;
mod telemetry;
mod util;

#[cfg(test)]
mod tests;

// Internal re-exports kept narrow on purpose. After Step 8 every cross-
// module symbol crosses through a typed seam (`util::*`, `agent::*`,
// `http::*`, `oauth::*`, `routes::*`); the only re-exports that remain
// at crate root are the ones the inline `tests` module reaches via
// `super::*` and a small set of state types whose canonical home is
// `state.rs`.
pub(crate) use error::{
    busy_error, internal_error, internal_error_named, unsupported_provider_error, RouteError,
    TurnError,
};
pub(crate) use http::sse::{
    publish_error, publish_events, publish_for_session, publish_idle, publish_part_delta,
    publish_status, publish_status_value, publish_turn_close, publish_turn_open,
};
#[allow(unused_imports)]
pub(crate) use state::{
    AppState, FakeCall, McpChild, PendingAuth, PendingNetwork, PendingPermission, PendingQuestion,
    PendingSuggestion, PermissionDecision, PermissionRule, QuestionReply, Repair, Runner,
    RunnerGuard, SuggestionDecision, ViewedState,
};
// `mcp_stop_child` is consumed by `impl Drop for AppState` below.
use routes::mcp::mcp_stop_child;

impl Drop for AppState {
    fn drop(&mut self) {
        if let Ok(mut children) = self.mcp_children.lock() {
            for (_, mut child) in std::mem::take(&mut *children) {
                let _ = mcp_stop_child(&mut child);
            }
        }
    }
}

pub(crate) const KNOWN_TOOLS: &[&str] = &[
    "read",
    "glob",
    "grep",
    "webfetch",
    "todowrite",
    "skill",
    "suggest",
    "lsp",
    "write",
    "edit",
    "apply_patch",
    "bash",
    "task",
    "question",
    "plan_exit",
];

pub async fn serve(
    opts: ServeOptions,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Build the Store once and share it for every startup-time path
    // resolution. Pre-A1 there were three `Store::new()` calls: the lock
    // probe, the optional log-dir resolver in main.rs, and the AppState
    // constructor. Each call re-read env vars, joined paths, and built a
    // fresh `Arc<Mutex<Option<Connection>>>` for the writer cache.
    // Sharing one instance saves the redundant work and keeps every
    // path-resolver in lockstep if env vars mutate mid-startup (which
    // tests do under the ENV_LOCK guard).
    let store = Store::new();

    // Single-sidecar-per-user lock (Operational invariants → 3). Resolve
    // the same state path the store uses, then check for a live owner.
    // If found, emit the readiness line for the existing port and exit
    // — the spawning extension's `parseServerPort` reads it off stdout
    // and connects to the established sidecar instead.
    let state_dir: std::path::PathBuf = std::path::PathBuf::from(store.paths().state);
    let lock = lock::SidecarLock::at(&state_dir);
    if let Ok(lock::LockOutcome::Join { port }) = lock.acquire() {
        println!(
            "kilo server listening on http://127.0.0.1:{} (contract={})",
            port,
            kilo_protocol::CONTRACT_VERSION,
        );
        return Ok(());
    }

    let addr = SocketAddr::from((util::encoding::loopback(&opts.hostname), opts.port));
    let listener = TcpListener::bind(addr).await.map_err(|err| {
        // Telemetry → Operational invariants 2: emit a structured event
        // for the most common startup failure (port in use, permission
        // denied) so the no-op default consumer can be swapped for a
        // live one without code churn. The error still propagates to
        // the caller as before — telemetry is observation, not control
        // flow.
        telemetry::telemetry().emit(telemetry::TelemetryEvent::SidecarStartupError {
            reason: format!("bind {addr}: {err}"),
        });
        err
    })?;
    let local = listener.local_addr()?;
    let (bus, _) = broadcast::channel(128);
    // Seed in-memory approvals from disk so user "always" decisions
    // survive a sidecar restart (Bun parity: `PermissionTable`).
    let persisted_approvals = PermissionRule::from_persisted(store.permission_rules());
    let state = Arc::new(AppState {
        username: std::env::var("KILO_SERVER_USERNAME").unwrap_or_else(|_| "kilo".to_string()),
        password: std::env::var("KILO_SERVER_PASSWORD").ok(),
        store,
        bus,
        viewed: RwLock::default(),
        runners: Mutex::default(),
        runner_notify: tokio::sync::Notify::new(),
        prompt_queues: Mutex::default(),
        prompt_queue_versions: Mutex::default(),
        permissions: Mutex::default(),
        approvals: Mutex::new(persisted_approvals),
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
        oauth_listener: Mutex::default(),
        oauth_listener_addr: SocketAddr::from(([127, 0, 0, 1], 1455)),
        oauth_token_endpoint: format!("{}/oauth/token", oauth::OPENAI_ISSUER),
        sse_capacity: AppState::new_sse_capacity(),
    });
    let app = http::build_router(state.clone());

    // Periodic GC of snapshot worktrees and orphaned plan markdowns. Both
    // walks are sync filesystem I/O, so we hop onto `spawn_blocking` rather
    // than holding a tokio worker. Gated behind `cfg!(not(test))` so unit
    // test runs (which never call `serve`) stay deterministic and don't
    // leave a stray task chewing on the shared store between cases.
    if cfg!(not(test)) {
        let cleanup_state = state.clone();
        tokio::spawn(async move {
            // Run once at startup to mop up anything left over from a prior
            // run that crashed before the next tick fired.
            let store = cleanup_state.store.clone();
            let _ = tokio::task::spawn_blocking(move || {
                let snap = crate::snapshot::cleanup_old_snapshots(&store, 30);
                let plans = crate::snapshot::cleanup_orphaned_plans(&store, 30);
                if snap > 0 || plans > 0 {
                    eprintln!(
                        "[kilo-server] cleanup: removed {snap} snapshots, {plans} orphan plans"
                    );
                }
            })
            .await;
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            // First tick fires immediately; we just ran the startup pass, so
            // skip it.
            tick.tick().await;
            loop {
                tick.tick().await;
                let store = cleanup_state.store.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let snap = crate::snapshot::cleanup_old_snapshots(&store, 30);
                    let plans = crate::snapshot::cleanup_orphaned_plans(&store, 30);
                    if snap > 0 || plans > 0 {
                        eprintln!(
                            "[kilo-server] cleanup: removed {snap} snapshots, {plans} orphan plans"
                        );
                    }
                })
                .await;
            }
        });
    }

    // Backward-compatible readiness line. The extension's `parseServerPort`
    // matches `listening on http://<host>:<port>`; the trailing
    // `(contract=N)` suffix is appended for newer extensions to read the
    // contract version off the same line. Older extensions ignore
    // anything after the port. Per migration plan **Wire-protocol
    // versioning beyond v1** section.
    println!(
        "kilo server listening on http://{}:{} (contract={})",
        local.ip(),
        local.port(),
        kilo_protocol::CONTRACT_VERSION,
    );

    // Record the live PID + port AFTER the readiness line so a racing
    // second process never observes a half-baked lock.
    let _ = lock.record(local.port());

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await;
    // Best-effort cleanup. If we crashed instead of shutting down
    // gracefully, the next launcher will detect the dead PID via
    // `process_alive` and take over.
    lock.release();
    serve_result?;

    Ok(())
}
