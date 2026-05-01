# VS Code sidecar contract preview

This document freezes the first safe sidecar seam used by the VS Code extension while the Rust sidecar is still a preview skeleton.

## Contract version

- Contract: `kilo-vscode-sidecar.preview.0`
- Rust sidecar: `0.0.0-preview.0`
- Bun oracle: the bundled `kilo serve` binary used by `packages/kilo-vscode`.

## Process contract

- The extension starts a child process with `serve --port 0`.
- Compatible flags accepted by the Rust skeleton:
  - `--port <number>`
  - `--hostname <host>`
  - `--host <host>`
  - `--mdns`, `--mdns-domain`, and `--cors` are accepted for CLI compatibility and ignored by the skeleton.
- The preview Rust server binds loopback only. Non-loopback host flags are normalized to loopback for this scaffold.
- Readiness stdout remains:

```text
kilo server listening on http://127.0.0.1:<port>
```

- The extension parser must continue to find the port with the existing readiness regex.
- `KILO_SERVER_PASSWORD` is the Basic Auth password passed by the extension.
- The Basic Auth username is `KILO_SERVER_USERNAME` or `kilo` when unset.
- Shutdown is best-effort: Ctrl-C and Unix terminate signals are handled gracefully; Windows process termination from the extension may still be immediate during this skeleton stage.

## HTTP contract in this slice

Implemented routes:

| Method | Path | Behavior |
|---|---|---|
| `GET` | `/global/health` | Returns `{ "healthy": true, "version": "0.0.0-preview.0" }`. |
| `GET` | `/global/event` | Opens a global SSE stream. |
| `GET` | `/path` | Returns the Rust-resolved Kilo path snapshot. |
| `GET` | `/config` | Returns a shallow read-only config snapshot. |
| `GET` | `/global/config` | Preview alias for the same shallow config snapshot. |
| `GET` | `/config/providers` | Returns the static preview provider catalog. Envelope `{ providers, default }` matches `Provider.ConfigProvidersResult.zod`. |
| `GET` | `/config/warnings` | Returns an empty warning list. |
| `GET` | `/provider` | Returns the static preview provider catalog. Envelope `{ all, default, connected }` matches `Provider.ListResult.zod`; `Info`/`Model` field set matches the Bun schema (optional fields are omitted, never null). |
| `GET` | `/provider/auth` | Returns an empty provider auth map. |
| `GET` | `/agent` | Returns static preview agent entries. |
| `GET` | `/skill` | Returns an empty skill list. |
| `GET` | `/command` | Returns an empty command list. |
| `GET` | `/project/current` | Returns a shallow current-project stub. |
| `GET` | `/session` | Reads existing Bun-created sessions from the SQLite store when available. |
| `GET` | `/session/status` | Returns an empty session status map. |
| `GET` | `/session/:sessionID` | Reads one existing Bun-created session from the SQLite store when available. |
| `GET` | `/session/:sessionID/message` | Reads existing Bun-created messages and parts from the SQLite store when available. |

All mutation, prompt, provider execution, tool, MCP transport, permission, question, worktree, and session-write routes are intentionally absent in this slice. They must remain on the Bun sidecar until later milestones add route parity.

## Auth contract in this slice

- If `KILO_SERVER_PASSWORD` is set, every non-`OPTIONS` route requires `Authorization: Basic <base64(username:password)>`.
- `auth_token` query compatibility is accepted by treating the query value as the Basic credential payload, matching the Bun middleware seam.
- Invalid or missing credentials return `401 Unauthorized` with `WWW-Authenticate: Basic realm="kilo"`.
- If `KILO_SERVER_PASSWORD` is missing, the skeleton allows requests to match Bun's unsecured development behavior. The VS Code extension always sets a random password.

## SSE contract in this slice

The stream emits Bun-compatible JSON in `data:` frames:

```json
{ "payload": { "type": "server.connected", "properties": {} } }
```

Then every 10 seconds:

```json
{ "payload": { "type": "server.heartbeat", "properties": {} } }
```

The stream intentionally does not emit session, provider, permission, tool, MCP, or Agent Manager events yet.

## Runtime switch contract

- VS Code setting: `kilo-code.new.sidecarRuntime`
  - `bun`: existing bundled sidecar.
  - `rust`: Rust sidecar only; startup failure is surfaced.
  - `auto`: try Rust, then fall back to Bun only if Rust fails before any SDK request can mutate state.
- Environment override: `KILO_VSCODE_SIDECAR_RUNTIME=bun|rust|auto`.
- Rust binary override: `KILO_VSCODE_RUST_SIDECAR_PATH=<absolute-or-relative-path>`.
- Bun remains default until later rollout milestones explicitly change it.

## Determinism contract

The Bun sidecar is the executable oracle. These rules describe what Rust must
reproduce — and where Bun is non-deterministic, the bounds within which Rust is
allowed to differ. They are the test schema for the M6 oracle.

### Per-stream ordering (strict)

For a single SSE consumer, events delivered for a given session must arrive in
the order Bun emits them. Concretely:

- `message.updated` for a new message arrives before any `message.part.updated`
  for that message.
- `message.part.updated` events for the same `partID` are delivered in append
  order. The final `message.part.updated` of a part has the part's terminal
  state (text fully assembled, tool result populated, etc.).
- `message.removed` for a message arrives after the last `message.part.updated`
  for any of its parts.
- `permission.updated` and `question.updated` arrive in the order their underlying
  bus events fired.
- `session.idle` is the last event of a turn for a session: no
  `message.part.updated`, `message.updated`, or `permission.updated` for that
  session arrives after the `session.idle` for the same turn until the next
  turn opens.
- `session.error` (if emitted) is followed by `session.idle` for the same turn.

### Cross-session interleaving (allowed)

Between sessions, events may interleave. Consumers must route by `sessionID` and
must not assume any total ordering between events from different sessions. Rust
may schedule cross-session events in any order Bun could plausibly schedule them.

### Heartbeat cadence (jitter-tolerant)

Bun emits `server.heartbeat` every 10 seconds with `setInterval(..., 10_000)`
([packages/opencode/src/server/routes/global.ts:38-47](../../opencode/src/server/routes/global.ts)).
Real recordings carry sub-100 ms drift. The oracle:

- Accepts a heartbeat at any point within `[10s − 1s, 10s + 1s]` of its expected
  tick (1-second jitter window).
- Never asserts on the absolute wall-time of a heartbeat; only on the inter-tick
  delta and the count over a fixed observation window.
- Treats `server.connected` as the t=0 anchor; the first heartbeat is expected
  near t=10s.

### Reconnect window

A consumer that drops and reconnects to `/global/event` must receive a fresh
`server.connected` followed by heartbeats on the same 10s cadence from the
reconnect anchor. Rust does not replay missed bus events on reconnect (Bun does
not either). Per-session streams may be re-derived by the consumer via
`/session` + `/session/:id/message`.

### Sort tie-breaks (read paths)

- `GET /session` (list): sorted by `time.updated DESC`. Ties broken by
  `id ASC` (ULIDs are lexicographically and chronologically ordered).
- `GET /session/:id/message` (list): sorted by `info.time.created ASC`. Ties
  broken by `id ASC`. The `before` cursor encodes `{ id, time }` and is
  exclusive: returned items have `(time, id) < cursor` lexicographic order.
- `Provider.ListResult.default[providerID]`: the lexicographically smallest
  model ID for that provider (matches Bun's
  `sort(Object.values(item.models))[0].id`).

### Abort terminal state

When a turn is aborted:

- The last in-flight `message.part.updated` for the cancelled tool/text part
  carries a terminal state ("aborted" or equivalent) before the next event.
- `session.idle` for that turn arrives after the abort settles.
- Storage is durable BEFORE the abort SSE event is published: a reconnecting
  consumer that re-reads `/session/:id/message` sees the same terminal state.

### Storage ordering invariant

For any event that reflects durable state (session create/delete, message
append, message removal, abort settle), Rust must commit the state to the
session store BEFORE publishing the SSE event. This is the rule that makes
"reconnect and re-read" a reliable recovery path.

### What Bun does not promise (and Rust must not promise either)

- Strict total ordering across SSE consumers.
- Immediate event delivery — events may be queued in the per-consumer
  `AsyncQueue` for tens of milliseconds under load.
- Stable iteration order of `Record<string, T>` payload fields beyond what
  JSON serialization happens to produce. Consumers must key by ID, not index.

## Preview limitations

- Route parity is limited to read-only startup/render paths plus shallow static stubs.
- Session reads are best-effort from the Bun SQLite store; no Rust writes or migrations occur.
- Config and provider routes are shallow preview approximations, not full Bun parity.
- No provider execution, tool, MCP transport, prompt, permission, question, worktree, Marketplace, or KiloClaw migration.
- No Rust-created durable state.
- Automatic fallback is startup-only; after Rust can accept mutable requests in later milestones, fallback must be gated by explicit mutation tracking.
