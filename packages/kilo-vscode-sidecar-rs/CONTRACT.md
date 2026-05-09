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
| `GET` | `/config/providers` | Returns the OpenAI-only preview provider catalog. Envelope `{ providers, default }` matches `Provider.ConfigProvidersResult.zod`. |
| `GET` | `/config/warnings` | Returns an empty warning list. |
| `GET` | `/provider` | Returns the OpenAI-only preview provider catalog. Envelope `{ all, default, connected }` matches `Provider.ListResult.zod`; `Info`/`Model` field set matches the Bun schema (optional fields are omitted, never null). |
| `GET` | `/provider/auth` | Returns only the OpenAI ChatGPT Pro/Plus browser OAuth method. |
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

## Runtime contract

- The VS Code extension launches the Rust sidecar only.
- Startup failure is surfaced directly; there is no Bun fallback candidate.
- Rust binary override: `KILO_VSCODE_RUST_SIDECAR_PATH=<absolute-or-relative-path>`.

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

## Stable error-name set

Every non-2xx response body is a `NamedError`-shaped envelope:

```json
{ "name": "<stable-name>", "data": { "message": "<safe-for-toast>" } }
```

The allowed `name` values are enforced by `ALLOWED_INTERNAL_ERROR_NAMES` in
`crates/kilo-server/src/error.rs` and a `debug_assert!` in `internal_error_named`/`named_response`. Adding a new error name requires
appending it to that constant *and* this table in the same commit.

| Name | Convention | Status | Trigger |
|---|---|---|---|
| `InternalError` | PascalCase | 500 | Default fallback for unwrapped server errors |
| `BusyError` | PascalCase | 409 | A second prompt arriving on a busy session |
| `UnsupportedProviderError` | PascalCase | 400 | A provider path that's still on Bun |
| `OauthCallbackError` | PascalCase | 500 | OAuth callback exchange failure |
| `OauthCallbackListenerError` | PascalCase | 500 | OAuth callback listener bind failure |
| `request_too_large` | snake_case | 413 | Request body over `MAX_REQUEST_BODY_BYTES` |
| `sse_capacity_exceeded` | snake_case | 503 | More than `MAX_SSE_CLIENTS` concurrent streams |
| `session_quota_exceeded` | snake_case | 507 | More than `MAX_SESSIONS_PER_WORKSPACE` sessions |
| `message_part_too_large` | snake_case | 413 | Message part over `MAX_MESSAGE_PART_BYTES` |
| `shell_unavailable` | snake_case | 500 | bash tool with no resolvable shell on Windows |
| `schema_migration_failed` | snake_case | 500 | `PRAGMA user_version` walk failed mid-migration |
| `store_unavailable` | snake_case | 500 | Writer connection cannot be opened |

OAuth flow errors (emitted by `routes/config.rs` OAuth handlers):

| Name | Convention | Status | Trigger |
|---|---|---|---|
| `OauthUnsupportedProvider` | PascalCase | 400 | `/auth/{providerID}` for a provider other than `openai` |
| `OauthUnsupportedMethod` | PascalCase | 400 | OAuth method other than `auto` |
| `OauthCodeMissing` | PascalCase | 400 | OAuth callback body without `code` |
| `OauthPendingMissing` | PascalCase | 400 | Callback for a flow with no pending entry (or expired) |
| `OauthStateMismatch` | PascalCase | 400 | Callback `state` does not match the pending PKCE state |
| `OauthCallbackTimeout` | PascalCase | 400 | Browser callback wait exceeded `OAUTH_PENDING_TTL` |

PTY route errors (Rust-only surface; `routes/pty.rs`):

| Name | Convention | Status | Trigger |
|---|---|---|---|
| `RustPtyOpenError` | PascalCase | 500 | `portable_pty::PtySystem::openpty` failure |
| `RustPtySpawnError` | PascalCase | 500 | Child process spawn failure inside the PTY |
| `RustPtyWriterError` | PascalCase | 500 | PTY writer handle could not be acquired |
| `RustPtyReaderError` | PascalCase | 500 | PTY reader handle could not be acquired |
| `RustPtyNotFoundError` | PascalCase | 404 | PUT/DELETE for an unknown PTY id |
| `RustPtyWriteError` | PascalCase | 500 | Write to PTY writer failed |
| `RustPtyResizeError` | PascalCase | 500 | PTY resize ioctl failed |

Worktree route errors (`routes/worktree.rs`):

| Name | Convention | Status | Trigger |
|---|---|---|---|
| `WorktreeInvalidInputError` | PascalCase | 400 | Body is missing `branch`/`path` or has an empty value |
| `WorktreeCreateFailedError` | PascalCase | 500 | `git worktree add` failed |
| `WorktreeRemoveFailedError` | PascalCase | 500 | `git worktree remove` (or filesystem retry) failed |
| `WorktreeResetFailedError` | PascalCase | 500 | `git reset --hard` against a worktree failed |
| `WorktreeResetUnsafeError` | PascalCase | 400 | Reset target is the primary worktree |
| `WorktreeNotGitError` | PascalCase | 400 | Project root is not a git repository |
| `WorktreeListFailedError` | PascalCase | 500 | `git worktree list --porcelain` failed |
| `WorktreePathSafetyError` | PascalCase | 400 | Path canonicalization escaped the project root |

MCP route errors (Rust-only surface; `routes/mcp.rs` and `kilo-mcp`):

| Name | Convention | Status | Trigger |
|---|---|---|---|
| `RustMcpAuthInvalidError` | PascalCase | 400 | Auth body for `/mcp/{name}/auth` is malformed |
| `RustMcpNotFoundError` | PascalCase | 404 | Unknown MCP server name |
| `RustMcpAuthPersistError` | PascalCase | 500 | Persisting MCP auth blob to disk failed |
| `RustMcpOAuthConfigError` | PascalCase | 400 | Remote MCP server config missing `oauth.*` fields |
| `RustMcpOAuthPersistError` | PascalCase | 500 | Persisting MCP OAuth tokens failed |
| `RustMcpOAuthCallbackError` | PascalCase | 400 | OAuth callback for an MCP server failed exchange |
| `RustMcpOAuthStateError` | PascalCase | 400 | Pending MCP OAuth state mismatch / missing |
| `RustMcpDisabledError` | PascalCase | 400 | Tool call against a server with `enabled = false` |
| `RustMcpDisconnectedError` | PascalCase | 503 | Tool call against a not-currently-connected server |
| `RustMcpOAuthDiscoveryError` | PascalCase | 400 | OAuth metadata discovery (issuer/registration endpoint) failed |
| `RustMcpOAuthRegistrationError` | PascalCase | 400 | Dynamic client registration with the MCP OAuth server failed |
| `RustMcpAuthRefreshError` | PascalCase | 500 | Refresh-token grant against the MCP server failed |
| `RustMcpRemoteConfigError` | PascalCase | 400 | Remote MCP transport config invalid |
| `RustMcpHttpError` | PascalCase | 502 | Upstream MCP HTTP transport error |
| `RustMcpTimeoutError` | PascalCase | 504 | MCP RPC exceeded its budget |
| `RustMcpMalformedResponseError` | PascalCase | 502 | MCP server returned an unparseable JSON-RPC frame |
| `RustMcpToolError` | PascalCase | 502 | MCP `tools/call` returned a JSON-RPC error |
| `RustMcpWriteError` | PascalCase | 500 | Writing to a local stdio MCP child failed |
| `RustMcpClosedError` | PascalCase | 503 | Local stdio MCP child closed mid-flight |
| `RustMcpNotImplementedError` | PascalCase | 501 | MCP lifecycle route reached the `not_implemented` placeholder |

Assistant-side error envelopes (emitted via direct `json!` into
`assistant.info.error`; documented also in "Assistant message error
envelopes" below — listed here so this registry stays the single source
of truth for every named error the sidecar emits):

| Name | Convention | Surface | Trigger |
|---|---|---|---|
| `CompactionError` | PascalCase | assistant.info.error | Compaction provider/store failure (`agent/compaction.rs`) |
| `APIError` | PascalCase | assistant.info.error | OpenAI Responses API failure surfaced through `kilo-provider` |

The `Rust*` prefixed names are Rust-only HTTP route surfaces (PTY, MCP)
that have no Bun analogue under the M10 OpenAI Pro narrowing — the
prefix flags them as Rust-side identifiers.

The `bad_request_named` helper in `routes/config.rs` is **deprecated**:
it bypasses the registry's `debug_assert!`. The canonical replacement
lives in `error.rs::bad_request_named` and enforces the same check used
by `internal_error_named` / `named_response`. Existing call sites are
migrated in a separate pass; new code should call the `error.rs`
helper.

The two case conventions are deliberate. `PascalCase` predates the
operational-invariant work and matches Bun's `NamedError` class names. New
boundary/policy errors specified by the migration plan's "Operational
invariants" section use `snake_case`. The SDK type union accepts both.

Truncation sentinel for tool-output capture is the literal string
`"\n…[output truncated by sidecar]\n"` (`limits::TRUNCATION_SENTINEL`).

## Assistant message error envelopes

A separate stable name set lives on `assistant.info.error` (NOT the HTTP
response body — those go through `ALLOWED_INTERNAL_ERROR_NAMES`). These
are surfaced to the client when a turn ends in a recoverable failure and
must round-trip across reconnect.

| Name | Trigger |
|---|---|
| `MessageAbortedError` | The runner cancel flag tripped mid-turn (user abort, sidecar shutdown, or upstream `ProviderError::Aborted`). |
| `MaxIterationsError` | The OpenAI OAuth tool loop hit `OPENAI_OAUTH_MAX_ITERATIONS` (16) without a terminal stop. |
| `MalformedToolArgumentsError` | The model emitted the same JSON-parse-failed tool call twice in a turn. |
| `StructuredOutputError` | `format.type == "json_schema"` but the model never invoked the synthesized `StructuredOutput` tool. |
| `PermissionRejectedError` | A tool's `ask_permission` call returned `Reject`. Carried inside the tool part's `state.metadata.error`, not on the assistant `info.error`. |

## Agent loop contract

The OpenAI OAuth Codex loop (`crates/kilo-server/src/agent/openai_stream.rs`)
must reproduce Bun's `streamText` step semantics
([`packages/opencode/src/session/processor.ts:402-473`](../../opencode/src/session/processor.ts)).

### Per-iteration step parts (strict)

Every iteration of the agent loop emits a `step-start` part at the top
and a `step-finish` part at the bottom, persisted to the assistant
message in append order. A turn with N iterations therefore has N
`step-start` parts, N `step-finish` parts, and any tool parts produced
that iteration sandwiched between them. Bun parity ratio: one
`step-start` per `start-step` event, one `step-finish` per `finish-step`
event.

`step-finish` shape:

```json
{
  "id": "<pid>_step_finish_<iter>",
  "type": "step-finish",
  "messageID": "<mid>",
  "sessionID": "<sid>",
  "reason": "stop",
  "cost": 0,
  "tokens": {
    "input": 0, "output": 0, "reasoning": 0, "total": 0,
    "cache": { "read": 0, "write": 0 }
  }
}
```

`tokens` carries that iteration's usage, NOT the cumulative turn total.
The cumulative total appears on `assistant.info.tokens` and is the sum
of every iteration's usage (Bun: `processor.ts:443-447`).

### Token usage fields (Bun parity)

`assistant.info.tokens` and per-step `step-finish.tokens` are populated
from `kilo_provider::ChatUsage` with these fields read from the OpenAI
Responses API:

| JSON path | Maps to |
|---|---|
| `usage.input_tokens` (or legacy `usage.prompt_tokens`) | `input` |
| `usage.output_tokens` (or `usage.completion_tokens`) | `output` |
| `usage.total_tokens` (defaults to `input + output`) | `total` |
| `usage.input_tokens_details.cached_tokens` (or flat `cached_tokens`) | `cache.read` |
| `usage.input_tokens_details.cache_creation_input_tokens` (or flat) | `cache.write` |
| `usage.output_tokens_details.reasoning_tokens` (or flat) | `reasoning` |

Provider-specific cache_write keys (`anthropic.cacheCreationInputTokens`,
`vertex.*`, `bedrock.*`, `venice.*`) that Bun's `Session.getUsage` reads
are NOT parsed in this slice — M10 narrows to the OpenAI Responses path.

### Structured output tool

When `PromptInput.format.type == "json_schema"` the loop:

1. Synthesizes a `StructuredOutput` tool with `parameters = format.schema`
   and includes it in the per-turn tool catalog.
2. Prepends `STRUCTURED_OUTPUT_SYSTEM_PROMPT` to the instructions block.
3. Intercepts any tool call named `StructuredOutput` — does NOT spawn a
   tool runner; captures the input.
4. On capture, sets `last_finish = "stop"` and breaks the loop.
5. On terminal write, sets `assistant.info.structured = <captured input>`.
6. If the loop ends without capture, the assistant message is failed
   with `StructuredOutputError`.

### Doom-loop guard

After every iteration's tool drain, the loop inspects the trailing
`DOOM_LOOP_THRESHOLD = 3` *completed* tool parts (`tool_parts` tail,
ignoring text/step parts and pending tools). If all three share the
same tool name AND the same `state.input` JSON, `ask_doom_loop` is
called with:

- `permission = "doom_loop"`
- `pattern = <tool name>`
- `metadata = { "tool": <name>, "input": <last input> }`

A user reply of `Allow` / `Always` lets the loop continue; `Reject` (or
a session/global rule `"doom_loop": "deny"` or
`"doom_loop": { "<tool>": "deny" }`) breaks the loop gracefully. A rule
`"doom_loop": "allow"` (or scoped to a tool) skips the prompt.

### MCP tool dispatch

Connected MCP servers (any client whose `Status::Connected { tools: [..] }`
is non-empty) contribute namespaced entries to the per-turn tool catalog:
each tool is exposed as `<client>_<tool>` (Bun parity: `mcp/index.ts:685`).
A tool call against a name not in the built-in catalog is reverse-resolved
against the live connected MCP catalog before being declared unknown;
matches dispatch through `mcp_invoke` (local stdio or remote HTTP).

Permission gate (`ask_mcp_permission`):

- `permission = "mcp"`
- `pattern = <namespaced tool name>`

A blanket `"mcp": "deny"` blocks every MCP server; per-tool rules like
`"mcp": { "context7_resolve_library_id": "allow" }` work as expected.

### Permission rule evaluation

`evaluate_permission_layered(permission, pattern, soft, hard)` is the
single entry point. Bun's two-layer model
(`permission/index.ts:217-271`):

- **Hard layer** (agent-derived; only `ask` and `plan` agents have one):
  scanned first. A matching `deny` is unbeatable; a matching `allow`
  short-circuits the prompt; a matching `ask` is treated as no-match
  and falls through.
- **Soft layer** (`state.approvals` ∪ `session.permission`): scanned
  with `findLast` semantics — later rules win.

`wildcard_match` supports full glob (`*` anywhere, `?` for one char),
not just prefix/suffix.

## Preview limitations

- Route parity is limited to read-only startup/render paths plus shallow static stubs.
- Session reads are best-effort from the Bun SQLite store; no Rust writes or migrations occur.
- Config and provider routes are shallow preview approximations, not full Bun parity.
- No provider execution, tool, MCP transport, prompt, permission, question, worktree, Marketplace, or KiloClaw migration.
- No Rust-created durable state.
- Automatic fallback is startup-only; after Rust can accept mutable requests in later milestones, fallback must be gated by explicit mutation tracking.
