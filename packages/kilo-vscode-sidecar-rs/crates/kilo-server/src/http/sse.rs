//! Server-Sent Events plumbing: the global and per-instance SSE endpoints
//! plus the `publish_*` helpers the agent loop and route handlers use to
//! broadcast events to subscribers via `state.bus`.
//!
//! ## Pre-serialized fanout (item 10 perf win)
//!
//! Before this slice, every SSE subscriber re-serialized each broadcast
//! event to JSON independently in `frame()` / `payload_frame()`. With N
//! subscribers (sidebar + agent manager + history + each tab panel) and M
//! events per turn, that produced `N × M` calls to `serde_json::to_string`,
//! each walking the entire JSON payload tree.
//!
//! Now: every `publish_*` helper serializes the event ONCE on the producer
//! side into two `Arc<str>` blobs (one for `/global/event`, one for
//! `/event` which only ships `event.payload`). The bus broadcasts the
//! `BusEvent` wrapper; cloning a `BusEvent` is two `Arc::clone` refcount
//! bumps. Subscribers hand the appropriate `Arc<str>` to axum's
//! `Event::data(&str)` directly — axum copies the bytes into its frame
//! buffer, but the JSON walk happens exactly once per event regardless of
//! N.
//!
//! Heartbeat / connected / lagged frames are constructed locally per
//! subscriber and never go through the bus, so they keep their old
//! per-frame serialization. They only fire 1× per subscriber per tick;
//! the fanout cost was always O(1) for those.

use std::{
    convert::Infallible,
    sync::{Arc, OnceLock},
    time::Duration,
};

use async_stream::stream;
use axum::{
    extract::State,
    response::{
        sse::{Event, Sse},
        Response,
    },
};
use futures_core::Stream;
use kilo_protocol::GlobalEvent;
use kilo_store::StoredEvent;
use serde_json::{json, Value};
use tokio::{
    sync::{broadcast, OwnedSemaphorePermit},
    time::MissedTickBehavior,
};

use crate::limits::sse_capacity_exceeded_error;
use crate::telemetry::{telemetry, TelemetryEvent};
use crate::AppState;

/// Lazily-memoized event broadcast over `state.bus`. The source
/// `GlobalEvent` is shared between subscribers via `Arc<GlobalEvent>` —
/// clones are refcount bumps, not heap allocations. Each wire-format
/// shape (full `GlobalEvent` JSON for `/global/event`, payload-only JSON
/// for `/event`) is serialized at most once across the entire fanout,
/// the FIRST time a subscriber on that endpoint reads it. A workspace
/// with only `/global/event` subscribers never pays the instance-form
/// serialization cost, and vice-versa.
///
/// `OnceLock` gives us "compute once across many readers" without an
/// explicit Mutex: the first reader wins the race to populate the cell,
/// later readers see the already-computed value. `Arc<OnceLock<…>>` so
/// every cloned `BusEvent` shares the same cell.
///
/// Tradeoff vs eager-serialize-both: the source `Arc<GlobalEvent>` is
/// kept alive while subscribers process the event, which is slightly
/// more memory than discarding the source after serialization. Net win
/// for typical workloads where most events are small and subscriber
/// count is modest; for events with large tool-output payloads the
/// shared source is actually smaller than the sum of two serialized
/// copies.
#[derive(Clone, Debug)]
pub(crate) struct BusEvent {
    source: Arc<GlobalEvent>,
    global: Arc<OnceLock<Arc<str>>>,
    instance: Arc<OnceLock<Arc<str>>>,
}

impl BusEvent {
    /// Wrap a `GlobalEvent` for broadcast. Serialization is deferred to
    /// the first subscriber that asks for it (per shape).
    pub(crate) fn from_event(event: GlobalEvent) -> Self {
        Self {
            source: Arc::new(event),
            global: Arc::new(OnceLock::new()),
            instance: Arc::new(OnceLock::new()),
        }
    }

    /// JSON for `/global/event`. Computes once and caches; subsequent
    /// callers see the cached `Arc<str>` (refcount bump only).
    pub(crate) fn global_json(&self) -> &str {
        self.global.get_or_init(|| {
            let s = serde_json::to_string(&*self.source).unwrap_or_else(|_| "{}".to_string());
            Arc::from(s.into_boxed_str())
        })
    }

    /// JSON for `/event`. Same lazy semantics as `global_json` but
    /// caches the payload-only form.
    pub(crate) fn instance_json(&self) -> &str {
        self.instance.get_or_init(|| {
            let s =
                serde_json::to_string(&self.source.payload).unwrap_or_else(|_| "{}".to_string());
            Arc::from(s.into_boxed_str())
        })
    }

    /// Inspect the event's `payload.type` discriminator without paying
    /// for serialization. Used by the `/event` SSE loop to detect the
    /// `server.instance.disposed` close-stream signal Bun emits at
    /// `server/routes/instance/event.ts:69-74`.
    pub(crate) fn kind(&self) -> &str {
        &self.source.payload.kind
    }

    /// Test-only round-trip back to a typed `GlobalEvent`. Production
    /// subscribers consume the lazily-serialized bytes directly via
    /// [`global_json`] / [`instance_json`] and never need this; the
    /// unit-test harness uses it to keep the existing
    /// `drain(&mut rx) -> Vec<GlobalEvent>` shape working without
    /// rewriting hundreds of assertions. Returns a clone of the source
    /// so test mutations don't poison the broadcast.
    #[cfg(test)]
    pub(crate) fn as_global(&self) -> GlobalEvent {
        (*self.source).clone()
    }
}

/// Producer-side helper: wrap and broadcast. Replaces the old
/// `state.bus.send(GlobalEvent::...)` pattern in every call site.
/// Serialization is deferred until a subscriber reads the wire-form.
pub(crate) fn publish(state: &AppState, event: GlobalEvent) {
    let _ = state.bus.send(BusEvent::from_event(event));
}

pub(crate) async fn events(
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Response> {
    let permit = acquire_sse_permit(&state)?;
    let mut rx = state.bus.subscribe();
    let stream = stream! {
        // Permit lives for the life of the stream — dropping closes the slot.
        let _permit: OwnedSemaphorePermit = permit;
        yield frame_local(GlobalEvent::connected());

        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => yield frame_local(GlobalEvent::heartbeat()),
                event = rx.recv() => match event {
                    Ok(event) => yield frame_bus(&event, BusFrame::Global),
                    // Bus channel is bounded (`broadcast::channel(...)`).
                    // A slow SSE consumer that falls behind by more than the
                    // capacity loses `n` events. Surface the count so the
                    // client can refetch state and an operator sees the
                    // pressure, instead of silently desyncing the UI.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[kilo-server] /global/event subscriber lagged, dropped {n} events");
                        // Operational invariant 2: emit a structured
                        // telemetry event for SSE pressure so an opt-in
                        // collector sees the rate. The default no-op
                        // implementation drops it.
                        telemetry().emit(TelemetryEvent::SseReconnect { stream: "global" });
                        yield frame_local(GlobalEvent::bus(
                            "server.lagged",
                            json!({ "dropped": n, "channel": "global" }),
                        ));
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    };

    Ok(Sse::new(stream))
}

pub(crate) async fn instance_events(
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Response> {
    let permit = acquire_sse_permit(&state)?;
    let mut rx = state.bus.subscribe();
    let stream = stream! {
        let _permit: OwnedSemaphorePermit = permit;
        yield payload_frame_local(GlobalEvent::connected());

        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => yield payload_frame_local(GlobalEvent::heartbeat()),
                event = rx.recv() => match event {
                    Ok(event) => {
                        // Bun (`server/routes/instance/event.ts:69-74`)
                        // emits the `server.instance.disposed` frame
                        // and immediately closes the stream so the
                        // SDK reconnect callback chain
                        // (`SdkSSEAdapter` → `recoverPendingPrompts`
                        // / `flushPendingSessionRefresh` /
                        // `checkConfigWarnings`) refires.
                        let stop = event.kind() == "server.instance.disposed";
                        yield frame_bus(&event, BusFrame::Instance);
                        if stop {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[kilo-server] /event subscriber lagged, dropped {n} events");
                        telemetry().emit(TelemetryEvent::SseReconnect { stream: "instance" });
                        yield payload_frame_local(GlobalEvent::bus(
                            "server.lagged",
                            json!({ "dropped": n, "channel": "instance" }),
                        ));
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    };

    Ok(Sse::new(stream))
}

fn acquire_sse_permit(state: &AppState) -> Result<OwnedSemaphorePermit, Response> {
    state
        .sse_capacity
        .clone()
        .try_acquire_owned()
        .map_err(|_| sse_capacity_exceeded_error())
}

#[derive(Clone, Copy)]
enum BusFrame {
    /// Full GlobalEvent JSON (used by `/global/event`).
    Global,
    /// Payload-only JSON (used by `/event`).
    Instance,
}

/// Hand a lazily-serialized bus event to axum's SSE Event. The first
/// subscriber on each endpoint shape pays the serialization cost; all
/// subsequent subscribers (and subsequent calls from this same one)
/// hit the memoized `Arc<str>` and just memcpy bytes into axum's frame
/// buffer.
fn frame_bus(event: &BusEvent, kind: BusFrame) -> Result<Event, Infallible> {
    let body: &str = match kind {
        BusFrame::Global => event.global_json(),
        BusFrame::Instance => event.instance_json(),
    };
    Ok(Event::default().data(body))
}

/// Build an axum SSE event from a locally-constructed `GlobalEvent`. Used
/// only for connected/heartbeat/lagged frames that don't fan out — those
/// fire once per subscriber per tick, so per-frame serialization there is
/// O(1) and not worth pre-baking.
pub(crate) fn frame_local(event: GlobalEvent) -> Result<Event, Infallible> {
    Ok(Event::default().data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string())))
}

/// Same as [`frame_local`] but only emits the `payload` portion. Mirrors
/// the instance-stream wire shape for connected/heartbeat/lagged frames.
pub(crate) fn payload_frame_local(event: GlobalEvent) -> Result<Event, Infallible> {
    Ok(Event::default()
        .data(serde_json::to_string(&event.payload).unwrap_or_else(|_| "{}".to_string())))
}

pub(crate) fn publish_turn_open(state: &AppState, id: &str) {
    publish(
        state,
        GlobalEvent::bus("session.turn.open", json!({ "sessionID": id })),
    );
}

pub(crate) fn publish_turn_close(state: &AppState, id: &str, reason: &str) {
    publish(
        state,
        GlobalEvent::bus(
            "session.turn.close",
            json!({ "sessionID": id, "reason": reason }),
        ),
    );
}

pub(crate) fn publish_status(state: &AppState, id: &str, status: &str) {
    publish(
        state,
        GlobalEvent::bus(
            "session.status",
            json!({ "sessionID": id, "status": { "type": status } }),
        ),
    );
}

pub(crate) fn publish_idle(state: &AppState, id: &str) {
    publish_status(state, id, "idle");
    publish(
        state,
        GlobalEvent::bus("session.idle", json!({ "sessionID": id })),
    );
}

pub(crate) fn publish_error(state: &AppState, id: &str, error: Value) {
    publish(
        state,
        GlobalEvent::bus("session.error", json!({ "sessionID": id, "error": error })),
    );
}

pub(crate) fn publish_part_delta(state: &AppState, sid: &str, mid: &str, pid: &str, delta: &str) {
    publish(
        state,
        GlobalEvent::bus(
            "message.part.delta",
            json!({
                "sessionID": sid,
                "messageID": mid,
                "partID": pid,
                "field": "text",
                "delta": delta,
            }),
        ),
    );
}

pub(crate) fn publish_events(
    state: &AppState,
    dir: String,
    project: String,
    events: impl IntoIterator<Item = StoredEvent>,
) {
    // Hot path. Allocate the `Arc<str>` for `dir` and `project` exactly
    // once per batch — every subsequent `GlobalEvent::message` /
    // `GlobalEvent::sync` clone bumps the refcount instead of allocating
    // a fresh `String`. With N SSE subscribers and M events per turn,
    // pre-A3 fanout did `N × M × 2` String allocations; post-A3 it's
    // `M × 2` Arc bumps + 2 string allocs total per call.
    let dir: Arc<str> = Arc::from(dir);
    let project: Arc<str> = Arc::from(project);
    for event in events {
        if event.seq < 0 {
            continue;
        }
        // Bun emits TWO frames per store mutation:
        //
        // 1. **Bus shape** via `ProjectBus.publish` →
        //    `opencode/src/bus/index.ts:80-100`. The webview consumer matches
        //    on this — see `packages/kilo-vscode/src/services/cli-backend/connection-utils.ts:88-100`
        //    for `event.type === "message.updated"` etc. Without this frame
        //    streaming text deltas render but the assistant message never
        //    "settles" because the SDK side mapper at `connection-utils.ts:89`
        //    never registers the messageID→sessionID binding.
        //
        // 2. **Sync envelope** — `payload.type === "sync"` with versioned
        //    `syncEvent.type` per `opencode/src/sync/index.ts:170-181`. Used
        //    by replay / state-rehydration paths.
        //
        // Wire-shape parity for the sync envelope's `syncEvent.type`: Bun
        // produces `"<type>.1"` via `versionedType(def.type, 1)`
        // (`opencode/src/sync/index.ts:79-83`); the SDK type union encodes
        // that exact suffix at `packages/sdk/js/src/v2/gen/types.gen.ts:1175`.
        // The store column persists the versioned form for replay; we
        // translate `.v1` → `.1` on egress so consumers see the Bun shape.
        let bus_kind = unversioned(&event.event_type);
        publish(
            state,
            GlobalEvent::message(
                bus_kind.to_string(),
                Arc::clone(&dir),
                Arc::clone(&project),
                event.data.clone(),
            ),
        );
        let data = json!({
            "type": dot_one_versioned(&event.event_type),
            "id": event.id,
            "seq": event.seq,
            "aggregateID": event.aggregate_id,
            "data": event.data,
        });
        publish(
            state,
            GlobalEvent::sync(Arc::clone(&dir), Arc::clone(&project), data),
        );
    }
}

/// Strip the trailing version suffix from a stored event type. Mirrors
/// `versionedType(type)` (no version) in `opencode/src/sync/index.ts:81-82`,
/// which returns the un-versioned key the bus shape consumers (`event.type`)
/// expect. Recognizes both the legacy `.v1` storage form and Bun's `.1`
/// canonical form so this stays stable if the column gets backfilled.
fn unversioned(event_type: &str) -> &str {
    event_type
        .strip_suffix(".v1")
        .or_else(|| event_type.strip_suffix(".1"))
        .unwrap_or(event_type)
}

/// Translate the stored event type to Bun's canonical `<type>.1` form for
/// the `syncEvent.type` field. Idempotent — `<type>.1` round-trips.
fn dot_one_versioned(event_type: &str) -> String {
    let base = unversioned(event_type);
    format!("{base}.1")
}

pub(crate) fn publish_for_session(state: &AppState, id: &str, events: Vec<StoredEvent>) {
    let Some(session) = state.store.session(id) else {
        return;
    };
    publish_events(
        state,
        state.store.paths().directory,
        session.project_id,
        events,
    );
}
