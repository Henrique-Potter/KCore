# Rust VS Code Sidecar Migration Plan

## Implementation status

Last updated: 2026-05-03

| Area | Status | Notes |
|---|---|---|
| Milestone 0: Contract freeze and Bun oracle harness | Partial | [`CONTRACT.md`](../packages/kilo-vscode-sidecar-rs/CONTRACT.md) and [`docs/route-inventory.md`](../packages/kilo-vscode-sidecar-rs/docs/route-inventory.md) exist. The `kilo-oracle` crate has a real harness scaffold but **no real recordings**: the SSE fixtures under [`fixtures/`](../packages/kilo-vscode-sidecar-rs/fixtures) use synthetic 10000/20000 ms heartbeat offsets, and `fixtures/store/empty.json` carries `"captured": false`. Recording against a live Bun process is still required. |
| Milestone 1: Add Rust sidecar package structure | Scaffolded + validated | Workspace and skeleton crates exist under [`packages/kilo-vscode-sidecar-rs`](../packages/kilo-vscode-sidecar-rs). Rust toolchain is installed and `cargo fmt --all`, `cargo check`, `cargo test -p kilo-store`, and `cargo test -p kilo-server` pass. |
| Milestone 2: Rust server skeleton | Beyond M2 scope | Process-compatible binary, readiness stdout, Basic Auth, health route, global SSE connected/heartbeat events, graceful shutdown, and an `x-kilo-directory`/`x-kilo-workspace` header→query rewrite middleware are in place in [`kilo-server/src/lib.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/lib.rs). The same crate already registers read-only route skeletons for `/path`, `/config`, `/global/config`, `/config/providers`, `/config/warnings`, `/provider`, `/provider/auth`, `/agent`, `/skill`, `/command`, `/project/current`, `/session`, `/session/status`, `/session/{id}`, `/session/{id}/message`. [`kilo-store/src/lib.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-store/src/lib.rs) opens Bun's `kilo.db` via `rusqlite` (read-only). All of this work is M5/M6 territory and was rolled into the M2 commit. |
| Milestone 3: Runtime switch and preview fallback | Mutation gate fixed; packaging deferred | Bun/Rust/Auto resolver, Bun default, environment overrides, startup diagnostics, and startup-only Auto fallback in [`ServerManager`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts) and [`sidecar-runtime.ts`](../packages/kilo-vscode/src/services/cli-backend/sidecar-runtime.ts). The mutation gate now flips **only after** Rust accepts a mutation with a 2xx response — see [`createMutationTrackingFetch`](../packages/kilo-vscode/src/services/cli-backend/connection-utils.ts) and the gate behavior tests in [`tests/unit/connection-utils.test.ts`](../packages/kilo-vscode/tests/unit/connection-utils.test.ts). Earlier request-time flipping was a footgun: any 4xx/5xx (missing route, server crash, validation error) used to permanently lock fallback even though Rust never touched state. Rust packaging integration ([`local-bin.ts`](../packages/kilo-vscode/script/local-bin.ts)) is still untouched and is deferred to M12. |
| Milestone 4: Remove Marketplace and KiloClaw from lean target | Complete on the kilo-vscode side, VSIX smoke-passed | Removed VS Code Marketplace and KiloClaw integration points from activation, package contributions, webview bundles, webview routes/messages, services, tests/stories, telemetry names, and KiloClaw-only Stream Chat dependency in `packages/kilo-vscode`. User smoke-tested the packaged VSIX successfully. The opencode-side claw code (`packages/opencode/src/kilocode/claw/*`) and its `stream-chat` dependency in `packages/opencode/package.json` and `package.json` patches remain in place; whether they are also out of the lean target is an open scope question — flagged for a later plan revision, **not** done in M4. |
| Milestone 5: Read-only route parity for sidebar and Agent Manager | M5 contract-complete on the Rust side | Rust now exposes the full M5 inventory: read-only startup/session routes, Bun-compatible AppData/XDG path resolution, SQL-side session filtering, opaque message cursor pagination with `X-Next-Cursor`/`Link` headers, plus `PATCH /global/config` (persists to `<config_dir>/config.json`), `POST /global/dispose`, `POST /instance/dispose`, `PATCH /session/{id}` (title rename with `session.updated` SSE projection), `GET /provider/{providerID}`, and `PUT`/`DELETE /auth/{providerID}` stubs. `Project` shape now carries `icon` and `commands` so the sidebar doesn't drop them. Remaining gap: live Rust runtime UI smoke test, mutation-gate timing fix in [`KiloConnectionService`](../packages/kilo-vscode/src/services/cli-backend/connection-service.ts) (currently flips on request, not 2xx response), and full MCP parity (M11). |
| Milestone 6: Session store and SSE event parity | M6 complete on the Rust side | Rust now implements durable session create/list/get/update/delete, viewed/open session state, child lookup, fork cloning with message/part ID remapping, message get/delete, part delete/update, revert/unrevert storage projection, diff readback, share/unshare storage, event table persistence, and write-before-publish SSE projection for `session.created`/`session.updated`/`session.deleted`, `message.updated`/`message.removed`, and `message.part.updated`/`message.part.removed`. `prompt_async`, abort, and fake prompt scaffolding exist, but the real provider-backed prompt runner remains M7. `summarize` is intentionally M6-safe/no-provider: it verifies the session and returns `true` without compaction mutation. Validated with `cargo fmt --all`, `cargo test -p kilo-store`, and `cargo test -p kilo-server`. |
| Milestone 7: First vertical agent turn | Live OpenAI OAuth chat smoke passed | Rust now has per-session runner state, `BusyError` rejection, cross-session concurrent fake turns, `prompt_async`, abort signaling, deterministic fake turns, parallel fake tool calls, OpenAI OAuth Responses streaming/parser coverage, M7 oracle traces, and a deterministic sidebar-first-chat smoke (`m7_sidebar_first_chat_smoke_streams_persists_and_reads_back`) that launches Rust, opens `/global/event`, creates a session, sends a fake-provider first prompt, observes streamed text delta/session events, verifies persisted messages, and reads history back. On 2026-05-03, the latest packaged VSIX was installed and a live VS Code smoke confirmed the Rust sidecar OpenAI Pro / ChatGPT OAuth account-provider path can complete chat (`finally chat works`). Validated with `cargo fmt --all`, `cargo test -p kilo-server`, `cargo test -p kilo-oracle`, targeted `m7_rust_fixtures`, `cargo test -p kilo-server prompt_turn_openai_oauth`, `cargo test -p kilo-provider`, `bun run package`, VSIX packaging, and live smoke. Remaining M7 automation risk: no local UI e2e harness/fake-provider toggle exists. |
| Storage and process self-healing invariants | Defined; partial implementation | First-run schema bootstrap fixed in `kilo-store` (`init_schema` now runs unconditionally with `create table if not exists`) after a HEAD bug shipped `Invalid parameter name: missing table project` toasts on first chat. Schema versioning, internal-error normalization, Bun↔Rust round-trip oracle, and OS-keychain auth storage remain unscheduled — see new "Storage and process self-healing invariants" section. |
| Operational invariants | Defined; not implemented | Structured logging via `tracing`, opt-in telemetry seam, single-sidecar-per-user lock, lazy activation event, and request/SSE/session/message resource limits remain unscheduled — see new "Operational invariants" section. |

Current continuation target: treat the Rust OpenAI Pro / ChatGPT OAuth account-provider path as live-smoke validated, then move remaining UI automation work into a dedicated e2e harness task. After that, work the new self-healing/operational invariant items into M5/M6/M8/M12/M14 per the cross-references below.

### 2026-05-03 live OpenAI OAuth chat smoke

User confirmation: after installing the latest VSIX and retrying chat, `finally chat works`. This validates the Rust VS Code sidecar's **OpenAI Pro / ChatGPT OAuth account-provider path** for live chat. It does **not** claim generic provider parity.

Fresh artifact that worked: [`packages/kilo-vscode/kilo-code-latest.vsix`](../packages/kilo-vscode/kilo-code-latest.vsix), approximately 83,461,402 bytes, modified 2026-05-03 05:07 PM local time.

Root-cause/fix sequence that made the smoke pass:

- JSON body leniency for SDK requests missing `Content-Type` in [`middleware.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/http/middleware.rs).
- SQLite store schema initialization on first write in [`lib.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-store/src/lib.rs).
- OpenAI OAuth permissions, tool loop behavior, and provider streaming improvements in [`openai_stream.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/agent/openai_stream.rs).
- Persisted Responses transcript replay and empty assistant placeholder filtering in [`parts.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/agent/parts.rs).
- Slash command expansion and inline subtask handling in [`turn.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/agent/turn.rs).
- Skill/command registry and richer local registry parsing in [`registry.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/registry.rs) and [`routes/registry.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/routes/registry.rs).
- Provider error diagnostics in [`lib.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-provider/src/lib.rs).

Validation highlights:

- `cargo test -p kilo-server prompt_turn_openai_oauth` passed 10 tests from [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/Cargo.toml).
- `cargo test -p kilo-provider` passed 54 tests, 1 ignored from [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/Cargo.toml).
- `bun run package` passed from [`package.json`](../packages/kilo-vscode/package.json).
- VSIX packaging passed, and the packaged VSIX above passed the live chat smoke.

Known remaining gaps:

- This is OpenAI OAuth/Codex account-provider parity, not full provider parity.
- Inline subtasks exist, but full Bun child-agent/subagent orchestration is not implemented.
- MCP prompt discovery, remote skill URL fetching, permission-filtered skill availability, and long-session compaction/trimming remain future work.
- Previously known unrelated full-suite issue: [`worktree_list_reads_git_porcelain_paths()`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/src/tests.rs) may fail independently.

## North star

Replace the current Bun-built VS Code sidecar with a full Rust sidecar while preserving the existing extension-facing process and protocol boundary.

The migration seam is already strong:

- VS Code extension activation starts in [`activate()`](../packages/kilo-vscode/src/extension.ts:27).
- Shared backend access flows through [`KiloConnectionService`](../packages/kilo-vscode/src/services/cli-backend/connection-service.ts:61).
- Process ownership is isolated in [`ServerManager`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:18).
- Current sidecar spawn is [`serve --port 0`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:72).
- Current Bun serve behavior is rooted in [`ServeCommand`](../packages/opencode/src/cli/cmd/serve.ts:7), [`create()`](../packages/opencode/src/server/server.ts:40), [`Server.listen()`](../packages/opencode/src/server/server.ts:106), and [`adapter.bun.ts`](../packages/opencode/src/server/adapter.bun.ts:1).

Do not move the full backend into the VS Code extension host. Keep the sidecar process because the backend owns long-running streams, tool execution, MCP, git/worktree work, provider calls, session state, storage, permissions, and process supervision.

## Fixed scope decisions

- Product scope: VS Code plugin only.
- Keep: sidebar chat and Agent Manager.
- Preserve: HTTP/SSE in phase 1.
- Remove: Marketplace and KiloClaw from the lean target.
- Keep Bun backend as oracle and rollback path during preview.
- Target runtime: full Rust sidecar, not Rust wrapper around Bun.
- Avoid broad redesign of extension transport, SDK, or webview architecture during phase 1.

## Behavioral fidelity invariants

These are the parts of the current product where matching the Bun behavior is **the greatest value of Kilo Code**. Anyone porting M7+ to Rust must reproduce the existing harness shape, agentic-loop control flow, and parallel-execution semantics — not redesign them. A "cleaner" Rust loop that loses these properties is a regression even if every contract test passes, because the product's UX and throughput live here.

These invariants override the temptation to translate idiom-by-idiom. Translate behavior, not code structure.

### 1. Agentic loop harness must match Bun's `streamText`-driven loop

The canonical loop is in [`packages/opencode/src/session/llm.ts`](../packages/opencode/src/session/llm.ts) (the `stream` function around line 357). Rust must preserve every load-bearing call-site behavior:

- **Same step contract:** one turn = one `streamText` call. The model decides when the turn ends. Step boundaries, message-part deltas, and tool-call resolution are observed *inside* the stream, not orchestrated externally.
- **Same tool-call repair:** [`experimental_repairToolCall`](../packages/opencode/src/session/llm.ts:363) at lines 363–383 recovers from case-mismatched tool names (`Bash` → `bash`) and from invalid arguments by routing to a synthetic `invalid` tool. Without this, the user sees raw provider errors on every malformed tool call. Rust must reproduce both branches.
- **Same `_noop` tool injection:** [`llm.ts:237-252`](../packages/opencode/src/session/llm.ts:237) adds a `_noop` stub tool **only** when the triple `(isLiteLLMProxy || providerID.includes("github-copilot")) && tools.length === 0 && hasToolCalls(messages)` holds. The condition is **specifically** about LiteLLM/Copilot rejecting requests whose history contains tool calls but whose current `tools` param is empty (e.g. during compaction) — not a general "no tools available" rule. Rust must match the full triple. Injecting `_noop` broadly will pollute every provider's tool list; missing the case will reproduce the LiteLLM rejection bug.
- **Same active-tools filtering:** `activeTools: Object.keys(tools).filter((x) => x !== "invalid")` — `invalid` is registered but hidden from the model. Behavior change here causes silent tool-call leak.
- **Same abort signal plumbing:** the abort signal must reach the provider stream, the in-flight tool calls, **and** the message-part writer atomically. The Bun code threads `input.abort` into `streamText` and into every tool runner. A Rust loop that aborts the stream but lets a tool finish running corrupts session state.
- **Same `isOpenaiOauth` system-prompt routing.** The single `isOpenaiOauth = item.id === "openai" && info?.type === "oauth"` test at [`llm.ts:106`](../packages/opencode/src/session/llm.ts:106) drives three different behavioral branches that the Rust turn assembler must reproduce exactly: (1) **soul prompt placement** — for OAuth, the Kilo soul prompt is omitted from the `system` array and instead prepended into `options.instructions` ([`llm.ts:112,157`](../packages/opencode/src/session/llm.ts:112)); (2) **message wrap** — for OAuth, `input.messages` is passed raw to the Responses API, not wrapped with synthetic `system`-role messages ([`llm.ts:162-174`](../packages/opencode/src/session/llm.ts:162)); (3) **API surface** — OAuth uses the Responses API at `chatgpt.com/backend-api/codex/responses`, not the Chat Completions API. Diverging on any of the three changes how the model interprets the conversation in ways that contract tests cannot catch — the persisted message shape is the same, but the model behavior shifts. M10 implementers must mirror all three branches; not just the API endpoint.

### 2. Parallel execution capability is not optional

Bun runs work in parallel in three places that matter for product feel. None can be silently serialized:

- **Multiple tool calls per assistant step.** Parallelism here is a contract of the AI SDK's `streamText`, **not** an explicit `Effect.forEach` in Bun's code. The model emits multiple `tool-call` stream parts per step; the AI SDK invokes the registered tool functions concurrently as those parts arrive. The Bun loop is a passive observer: tool-call dispatch happens at the `tool-input-start` / `tool-call` event handlers in [`processor.ts:301-302, 334-335`](../packages/opencode/src/session/processor.ts:301), where `Deferred` slots are populated; the `Effect.forEach(... { concurrency: "unbounded" })` at [`processor.ts:564-568`](../packages/opencode/src/session/processor.ts:564) is the **drain/await** point, not the dispatch. A Rust port must reproduce parallelism at the dispatch layer — i.e. the model-stream consumer runs each tool-call as a separate task as it parses out of the stream. A naive port that builds a JoinSet only at the drain layer will execute tools sequentially in the consumer and lose the property entirely.
- **Per-turn provider/config/auth resolution:** [`llm.ts:95-103`](../packages/opencode/src/session/llm.ts:95) does `Effect.all([getLanguage, getConfig, getProvider, auth.get], { concurrency: "unbounded" })` at turn start. These are independent reads; Bun overlaps them. Rust must too.
- **Multi-session concurrency under one sidecar:** [`run-state.ts`](../packages/opencode/src/session/run-state.ts) holds one `Runner` per session, scoped by `SessionID`. Agent Manager opens *N* concurrent prompts and the Bun sidecar runs them in parallel without cross-session interference. Rust must hold the same per-session isolation: one busy session never blocks another, and one session's abort never cancels another's stream.

The Rust implementation should pick runtime constructs that give the same shape: a per-session `tokio::task::JoinHandle` + `CancellationToken` (replacing Effect's `Runner`), plus a tool-call `JoinSet` spawned **inside the model-stream consumer** as tool-call parts are parsed (replacing the AI SDK's implicit per-tool-call invocation). Do not invent a single-threaded executor and call it "simpler" — Agent Manager users will notice immediately.

### 3. Runner / cancellation semantics must match

[`run-state.ts:48-68`](../packages/opencode/src/session/run-state.ts:48) defines the per-session runner with:

- `assertNotBusy` — a second prompt arriving on a busy session throws `BusyError`, not "queue."
- `cancel` — surfaces an `onInterrupt` effect that produces a final assistant `MessageV2.WithParts` so the transcript closes cleanly even on abort.
- Idempotent `onIdle` cleanup when the runner exits.

Each of these is a UX-visible behavior. A Rust runner that queues prompts, that returns a generic error on abort, or that leaves the session marked "busy" after a panic will all degrade the product in ways no contract test catches.

### 4. Provider stream parity beats provider-call parity

When porting providers (M10), match what the *stream* surface produces — token deltas, tool-call deltas, finish reasons, error shapes — not the SDK call shape. The Bun loop reads from the AI SDK's stream parts; if Rust mints its own tokens but gets the part order or finish-reason taxonomy wrong, the UI will silently render broken transcripts. The oracle harness must compare normalized stream traces, not request/response pairs.

### 5. Storage durability ordering must match

[`processor.ts`](../packages/opencode/src/session/processor.ts) writes message parts to storage **before** publishing the corresponding SSE event. Reverse the order and a fast-reconnecting client receives an SSE pointing at a row that doesn't exist yet — a race that produces `NotFoundError` toasts in the sidebar. M5/M6 already follow write-before-publish; M7+ must keep that discipline through the entire turn, including tool-call result rows, reasoning parts, and abort terminals.

### Verification

The oracle harness ([`packages/kilo-vscode-sidecar-rs/crates/kilo-oracle`](../packages/kilo-vscode-sidecar-rs/crates/kilo-oracle)) is the enforcement mechanism for invariants 1, 4, and 5. Invariants 2 and 3 require **dedicated parallel-execution traces**: at minimum, one fixture exercising two-tool-calls-per-step (verifying overlapped execution) and one fixture exercising N concurrent sessions in Agent Manager (verifying zero cross-session leakage). Without those fixtures, drift here will not be caught until production users notice it.

## Storage and process self-healing invariants

These are the parts of the persistence and process surface that must keep working when state is unfamiliar, partially initialized, or recovered from a crash. Anyone porting M5+ to Rust must reproduce these — no toast saying "Invalid parameter name: missing table project" should ever reach a user. They are parallel to the agent-loop invariants above: contract tests can pass while these regress.

### 1. Persistent files self-heal on missing or partial state

Every file the sidecar opens for read-write — `kilo.db`, `auth.json`, `model.json`, `mcp-auth.json`, future caches — must produce a working state regardless of prior content. "Empty 4 KB SQLite file with no tables plus orphaned WAL/SHM siblings" is a real on-disk state: it is what one crashed init leaves behind. The same is true for half-written JSON and stale lock files.

For SQLite: bootstrap with `create table if not exists` for every table on every writer-connection initialization. Do not gate schema creation on file existence — `path.exists()` is true after the first failed `Connection::open`, so a one-time crash leaves every subsequent launch broken. For JSON: parse failures fall back to the empty-default value and overwrite on next successful write, never crash the process.

Reference incident: [`kilo-store/src/lib.rs:964`](../packages/kilo-vscode-sidecar-rs/crates/kilo-store/src/lib.rs:964) prior to the M6.1 fix opened `kilo.db` with `SQLITE_OPEN_CREATE`, applied pragmas, then never created any tables in production (the only `create table` lived in `seed_for_test`, gated behind `cfg(any(test, feature = "test-utils"))`). First chat surfaced `Invalid parameter name: missing table project` to the user as a toast.

### 2. Schema evolves through versioned, idempotent migrations

The SQLite schema must carry a `schema_version` cell (PRAGMA `user_version` is sufficient) and a migration runner that walks from the on-disk version to the binary version on every writer-connection init. Migrations are idempotent (`add column if not exists` patterns where SQLite supports them, version-gated `alter table` elsewhere). New columns default to NULL or to a backfill written at migration time and never break Bun-compat reads for the rollback window.

Adding a column without bumping `schema_version` is a process bug, not a schema bug. CI must fail any change to `init_schema` that does not also add a migration entry.

### 3. Internal errors normalize before reaching the SDK boundary

Library error text — rusqlite's `Display`, `std::io::Error` chains, OAuth refresh failures, panics from spawned tasks — never reaches the SDK envelope unmangled. The sidecar wraps these in the same `NamedError` shape M10 specifies for OpenAI errors: `{name, data: {message}}` with a stable `name` the extension can localize and a `message` safe for the toast surface. `Invalid parameter name: missing table project` is the failure mode for skipping this rule.

### 4. Bun ↔ Rust storage round-trip is verified before Bun fallback removal

Storage compatibility is asymmetric in M0/M5/M6 today (they verify Rust reads Bun-written rows). Before M14 step 7 removes Bun, the oracle must also verify the inverse: Bun reads Rust-written sessions, messages, parts, and events without column-order drift, JSON-default drift, or unknown-field loss. The check runs on every Rust schema change, not once.

### 5. Auth secrets at rest follow OS-native protection

OAuth tokens (ChatGPT Pro refresh tokens, future provider credentials, MCP OAuth) live behind the OS keychain — DPAPI on Windows, Keychain Services on macOS, libsecret/Secret Service on Linux — not in plaintext under `data/kilo/auth.json`. The current plaintext path is M5-era scaffolding; before stable rollout the storage layer routes through a `KeyringStore` shim with a documented plaintext fallback for environments where the keyring is unavailable (CI, headless containers, locked Linux sessions), gated behind an explicit user opt-in or env flag.

### Verification

The oracle harness covers invariants 1, 2, and 4: (a) a fresh-install fixture with no `kilo.db` on disk exercises bootstrap; (b) a half-init fixture (empty `kilo.db` + WAL/SHM siblings) exercises self-heal; (c) a Bun-writes-then-Rust-reads fixture and a Rust-writes-then-Bun-reads fixture exercise round-trip on every schema change. Invariant 3 needs an integration test that asserts every error reaching `internal_error` carries a stable `name` from a documented set. Invariant 5 needs a cross-platform smoke that round-trips an OAuth token through the keyring without writing it to disk.

## Operational invariants

These are runtime behaviors that no contract test catches but that determine whether the sidecar is supportable in production.

### 1. Structured logging with `tracing`

The sidecar emits structured logs through `tracing` and an env-controlled subscriber: spans for HTTP requests, SSE connections, and per-session turns; events at INFO for state transitions, WARN for self-healed conditions, ERROR for surfaced failures. Default destination is `<state_dir>/kilo/log/sidecar.log` with size-based rotation. `RUST_LOG`/`KILO_LOG` overrides the level. `eprintln!` is acceptable only for pre-tracing-init startup messages.

### 2. Telemetry and crash reporting are opt-in but designed in

Even without shipping telemetry day-one, the seam exists: a `Telemetry` trait with a no-op default and one structured emit-point per failure class (sidecar startup error, SSE disconnect-then-reconnect, schema bootstrap, OAuth refresh failure, panic). A future opt-in implementation slots in without rewiring call sites.

### 3. Single sidecar per user across VS Code windows

Two VS Code windows on the same workspace must not spawn two writer processes against the same `kilo.db`. SQLite WAL handles concurrent writes safely at the file level, but the per-Store writer mutex inside [`kilo-store/src/lib.rs:964`](../packages/kilo-vscode-sidecar-rs/crates/kilo-store/src/lib.rs:964) does not coordinate across processes — split-brain session state is the failure mode. Resolution: a per-user lock file (`<state_dir>/kilo/sidecar.lock`) records the live sidecar's PID and listening port; new launches detect the lock, attempt a health probe, and either join the existing instance or take over if the prior PID is dead. Multi-root workspaces and multiple-window-same-workspace both resolve to one sidecar.

### 4. Activation event picks lazy-on-chat, not eager-on-startup

The Rust sidecar spawns on the first kilo-code activation event the user actually triggers — opening the sidebar, opening Agent Manager, invoking a kilo command — not on VS Code startup. Cold start measurement in M13 is from activation event to readiness, not from VS Code launch. Keeps RSS at zero for users who don't use Kilo in a given window. `KILO_VSCODE_SIDECAR_PRESPAWN=1` overrides for benchmarking.

### 5. Resource limits at the boundary

HTTP request body limit: 16 MiB. Max concurrent SSE clients: 32 per workspace. Max sessions per workspace: 1000 (older are archived, not deleted). Max single-message-part size: 1 MiB. Max captured tool-output: 1 MiB (truncated with sentinel). Each is a constant in `kilo-server/src/limits.rs` so changes show up in code review and the oracle can assert them.

### Verification

Logging and telemetry seams are verified by integration tests that assert one structured emit per failure class and that the log file exists after a controlled startup error. The single-sidecar lock is verified by a multi-process test that races two `serve --port 0` invocations and asserts the second one joins or replaces, never both serve. Activation timing is verified manually in the live UI smoke (sidebar open → cold-start measurement starts here, not at VS Code launch). Resource limits are asserted at the route layer: oversize body returns 413, oversize SSE client count returns 503, etc. — all with stable error names.

## Milestone 0: Contract freeze and Bun oracle harness

### Goal

Before writing real Rust behavior, freeze what the VS Code extension expects from the current sidecar. Treat Bun as the executable oracle.

### Freeze these contracts

1. Process contract
   - Binary path resolution currently lives in [`getCliPath()`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:160).
   - Spawn args are currently [`serve --port 0`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:72).
   - Readiness stdout is parsed by [`parseServerPort`](../packages/kilo-vscode/src/services/cli-backend/server-utils.ts:1).
   - Password is passed through [`KILO_SERVER_PASSWORD`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:83).
   - Shutdown is managed by [`killProcess()`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:174).
2. HTTP contract
   - Keep the current generated SDK behavior used through [`createKiloClient()`](../packages/sdk/js/src/v2/client.ts:46).
   - Freeze route paths, methods, status codes, error envelopes, auth behavior, and directory scoping.
3. SSE contract
   - Keep the global event stream consumed by [`SdkSSEAdapter`](../packages/kilo-vscode/src/services/cli-backend/sdk-sse-adapter.ts:27).
   - Freeze event names, payload shapes, heartbeat behavior, reconnect behavior, and per-session ordering.
4. Storage contract
   - Rust must read Bun-created sessions/config/state during preview.
   - No Rust-only destructive migration until Bun rollback is removed.
5. Feature scope contract
   - Marketplace and KiloClaw are intentionally out.
   - Sidebar chat and Agent Manager are intentionally in.

### Deliverables

- Route inventory of every VS Code-used SDK operation.
- Golden startup fixture for readiness stdout.
- Golden SSE traces for session create, prompt stream, permission allow/deny, abort, and Agent Manager concurrent sessions.
- Store fixtures created by Bun and read by Rust.
- Contract version document in a new sidecar docs file such as [`CONTRACT.md`](../packages/kilo-vscode-sidecar-rs/CONTRACT.md).

### Route inventory governance

[`docs/route-inventory.md`](../packages/kilo-vscode-sidecar-rs/docs/route-inventory.md) is canonical and must be updated in the same commit as any route addition, removal, or method change. The `route_inventory_methods_match_sdk_emission` oracle test in [`kilo-oracle/tests/inventory_parity.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-oracle/tests/inventory_parity.rs) enforces parity against [`packages/sdk/js/src/v2/gen/sdk.gen.ts`](../packages/sdk/js/src/v2/gen/sdk.gen.ts); the test failing on a PR means the inventory is stale, not that the SDK is wrong. The owner is whoever lands the route change.

### Exit gate

Bun oracle tests pass consistently before Rust is judged.

## Milestone 1: Add Rust sidecar package structure

### Recommended monorepo layout

Keep this Kilo-specific to avoid upstream OpenCode merge churn.

| Path | Purpose |
|---|---|
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/Cargo.toml) | Rust workspace root. |
| [`README.md`](../packages/kilo-vscode-sidecar-rs/README.md) | Architecture, build, rollback, and contract notes. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-vscode-sidecar/Cargo.toml) | Binary crate: command parsing, process entrypoint, startup/shutdown. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-server/Cargo.toml) | HTTP/SSE server, middleware, route composition. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-protocol/Cargo.toml) | Request, response, error, and event types. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-store/Cargo.toml) | Bun-compatible config/session/message storage. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-session/Cargo.toml) | Session lifecycle, message parts, prompt runner, cancellation. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-provider/Cargo.toml) | Provider registry, auth, streaming adapters, transforms. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-tools/Cargo.toml) | Built-in tools, permissions, filesystem, shell, diff/edit behavior. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-mcp/Cargo.toml) | MCP config, transports, OAuth, tool bridge. |
| [`Cargo.toml`](../packages/kilo-vscode-sidecar-rs/crates/kilo-oracle/Cargo.toml) | Bun/Rust oracle test harness and fixture diffing. |

### Why this layout

- The binary crate stays thin.
- Server, protocol, store, session, providers, tools, and MCP have explicit seams.
- Rust code remains VS Code-sidecar-specific until it proves reusable.
- The migration avoids refactoring shared upstream files first.

### Toolchain and dependency policy

- `rust-toolchain.toml` at the workspace root pins the MSRV to the version CI uses; bumps require an explicit PR with a changelog entry.
- `cargo-deny` config in `deny.toml` enforces a license allow-list and refuses dependencies with known advisories. Runs on every PR.
- `Cargo.lock` is committed (already true). `cargo update` runs only via dedicated dependency-bump PRs, never as a side effect of feature work.
- New transitive dependency over a size threshold (e.g. > 500 KiB compiled or > 50 transitive crates) requires a one-line justification in the PR description.

### Exit gate

Rust workspace builds, formats, lints, and can produce an empty sidecar binary for all local dev targets. `cargo deny check` is green and `rust-toolchain.toml` is checked in.

## Milestone 2: Rust server skeleton

### Goal

Create a Rust binary that can be launched by the existing VS Code extension process model.

### Implement

1. Command compatibility
   - Accept [`serve --port 0`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:72).
   - Accept host/port flags compatible with [`ServeCommand`](../packages/opencode/src/cli/cmd/serve.ts:7).
2. Startup behavior
   - Bind loopback.
   - If port is zero, preserve current Bun behavior from [`adapter.bun.ts`](../packages/opencode/src/server/adapter.bun.ts:24): try the preferred port behavior first if required, then fall back safely.
   - Print the exact readiness line expected by [`parseServerPort`](../packages/kilo-vscode/src/services/cli-backend/server-utils.ts:1).
3. Auth
   - Read [`KILO_SERVER_PASSWORD`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:83).
   - Match Basic Auth semantics expected by the SDK client.
4. Health and SSE
   - Implement health route.
   - Implement global SSE stream compatible with [`SdkSSEAdapter`](../packages/kilo-vscode/src/services/cli-backend/sdk-sse-adapter.ts:27).
   - Emit heartbeats and tolerate reconnects.
5. Shutdown
   - Handle interrupt/terminate signals.
   - Stop accepting requests.
   - Close SSE connections.
   - Drain/cancel tasks.
   - Exit without orphaning children.

### Process model and operational invariants

These are required at M2 because they shape every subsequent milestone's runtime behavior.

- **Single-sidecar-per-user lock.** See **Operational invariants → 3**. Implementation lives in `kilo-server/src/lock.rs` and gates the bind step. A second `serve --port 0` invocation discovers the existing lock, health-probes the recorded port/PID, and either joins (returns the same readiness line) or replaces (if the prior PID is dead).
- **Activation timing.** Spawn on the first kilo activation event the user actually triggers, not on VS Code startup. See **Operational invariants → 4**. Cold-start measurement in M13 starts at the activation event.
- **Resource limits.** Constants in `kilo-server/src/limits.rs` per **Operational invariants → 5**. Applied at axum's `Body::Limited`, the SSE connection counter, and the session-create gate. Oversize/over-limit requests return stable error names (`request_too_large`, `sse_capacity_exceeded`, `session_quota_exceeded`).
- **Structured logging.** `tracing` subscriber initialized before route registration. See **Operational invariants → 1**. Pre-tracing-init startup messages may use `eprintln!`; everything past route registration is structured.

### Exit gate

The extension can spawn Rust, parse the port, authenticate, call health, connect SSE, and shut it down without changing [`KiloConnectionService`](../packages/kilo-vscode/src/services/cli-backend/connection-service.ts:61). A second concurrent `serve --port 0` is rejected or joined per the lock policy. Logs land at the configured destination.

## Milestone 3: Runtime switch and preview fallback

### Goal

Allow Bun and Rust sidecars to coexist during preview.

### Extension changes

1. Extract sidecar resolution from [`ServerManager`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:18).
2. Add runtime choices: Bun, Rust, and Auto.
3. Add environment override for CI and dev builds.
4. Keep Bun as default initially.
5. During preview, allow automatic Bun fallback only if Rust fails before any mutable request.
6. Add diagnostics for selected runtime, binary path, version, startup time, exit code, stderr summary, and fallback reason.

### Important rule

Do not silently fall back to Bun after Rust has accepted a prompt, created a session, edited state, or touched storage. After state mutation, fallback requires explicit user/runtime switch.

### Exit gate

A dev build can select Bun or Rust without changing webviews, SDK calls, or sidebar/Agent Manager code.

## Milestone 4: Remove Marketplace and KiloClaw from lean target

### KiloClaw removal tasks

- Remove import and construction of [`KiloClawProvider`](../packages/kilo-vscode/src/extension.ts:5).
- Remove command contribution [`kilo-code.new.kiloClawOpen`](../packages/kilo-vscode/package.json:104).
- Remove serializer and command registration from [`extension.ts`](../packages/kilo-vscode/src/extension.ts:155).
- Remove KiloClaw webview bundle from [`esbuild.js`](../packages/kilo-vscode/esbuild.js:192).
- Remove code rooted at [`KiloClawProvider.ts`](../packages/kilo-vscode/src/kiloclaw/KiloClawProvider.ts:26) if no other references remain.
- Remove Stream Chat dependency if KiloClaw is the only consumer.

### Marketplace removal tasks

- Remove command contribution [`kilo-code.new.marketplaceButtonClicked`](../packages/kilo-vscode/package.json:109).
- Remove command handler from [`extension.ts`](../packages/kilo-vscode/src/extension.ts:260).
- Remove Marketplace service rooted at [`api.ts`](../packages/kilo-vscode/src/services/marketplace/api.ts:4).
- Remove Marketplace handlers from [`KiloProvider`](../packages/kilo-vscode/src/KiloProvider.ts:729).
- Remove Marketplace settings panel mode from [`SettingsEditorProvider`](../packages/kilo-vscode/src/SettingsEditorProvider.ts:7).
- Keep skills, modes, agents, and config support if used outside Marketplace.

### Exit gate

[`typecheck`](../packages/kilo-vscode/package.json:862), [`lint`](../packages/kilo-vscode/package.json:868), [`test:unit`](../packages/kilo-vscode/package.json:870), and unused-export checks pass. Command palette no longer shows Marketplace or KiloClaw entries.

## Milestone 5: Read-only route parity for sidebar and Agent Manager

### Goal

Make the UI load against Rust without running an agent turn.

### Route groups to port first

- Global routes from [`GlobalRoutes`](../packages/opencode/src/server/routes/global.ts:86).
- Instance route composition from [`InstanceRoutes`](../packages/opencode/src/server/routes/instance/index.ts:55).
- Config routes from [`ConfigRoutes`](../packages/opencode/src/server/routes/instance/config.ts:17).
- Provider routes from [`ProviderRoutes`](../packages/opencode/src/server/routes/instance/provider.ts:16).
- Project routes from [`ProjectRoutes`](../packages/opencode/src/server/routes/instance/project.ts:15).
- Session routes from [`SessionRoutes`](../packages/opencode/src/server/routes/instance/session.ts:34) for list/get/create basics.
- MCP status routes from [`McpRoutes`](../packages/opencode/src/server/routes/instance/mcp.ts:12) if settings or chat UI needs them.

### Sidebar requirements

- Load config.
- Load provider/model/agent state.
- Load sessions and messages.
- Open an empty/new session.
- Show existing history.

### Agent Manager requirements

- Open Agent Manager panel.
- Reuse the same sidecar.
- List sessions.
- Attach viewed/open sessions.
- Respect directory/workspace scoping.

### Exit gate

Sidebar and Agent Manager render against Rust and can load existing Bun-created sessions without prompting the model.

## Milestone 6: Session store and SSE event parity

### Goal

Make Rust own durable sessions and live event projection.

### Design rules

- Write durable state before publishing SSE.
- Preserve Bun-compatible session/message/part shape during preview.
- Preserve unknown fields in serialized JSON where possible.
- Keep per-session event ordering strict.
- Allow cross-session interleaving only where Bun does.

### Implement

- Session create/list/get/delete.
- Message and part storage.
- Event bus.
- SSE projection for session/message updates.
- Viewed/open-session behavior used by Agent Manager.
- Directory-scoped session records for worktree sessions.
- Abort terminal state representation.

### Oracle tests

Compare Bun and Rust for:

- Session create.
- Message append.
- Session reload.
- Session delete.
- Prompt placeholder events.
- Agent Manager multi-session event routing.

### Schema bootstrap and migrations

- First writer-connection initialization runs `init_schema` unconditionally; tables use `create table if not exists`. See **Storage and process self-healing invariants → 1**. The historical pattern of gating bootstrap on `path.exists()` is forbidden — that pattern is the bug that produced `Invalid parameter name: missing table project` toasts in the M6.1 incident.
- Schema version cell uses SQLite's `PRAGMA user_version`. A `migrations` array in `kilo-store/src/migrations.rs` is walked from the on-disk version to the binary version on every writer init. See **Storage and process self-healing invariants → 2**. Each migration is `(version: u32, sql: &str)` and runs in a transaction.
- Internal-error normalization at the SDK boundary wraps every `rusqlite::Error`, `std::io::Error`, and panic as a `NamedError`. See **Storage and process self-healing invariants → 3**. The set of allowed `name` values is documented in [`CONTRACT.md`](../packages/kilo-vscode-sidecar-rs/CONTRACT.md) and asserted by an integration test.
- Bun ↔ Rust storage round-trip oracle fixture lives in `kilo-oracle/tests/storage_roundtrip.rs` and runs on every schema change. See **Storage and process self-healing invariants → 4**. Failures block merging changes that affect persisted shape.

### Exit gate

Rust-created sessions survive restart, Bun-created sessions can be read by Rust, **Bun can read Rust-created sessions**, and Agent Manager never receives events for the wrong session. A fresh-install fixture (no `kilo.db` on disk) and a half-init fixture (empty `kilo.db` with WAL/SHM siblings) both produce a working store on first chat. Every error path observed in CI carries a stable `name` from the documented set.

## Milestone 7: First vertical agent turn

### Goal

Implement one complete chat turn through Rust before porting all providers/tools. Read the **Behavioral fidelity invariants** section above before starting — it lists the loop-level behaviors M7 must reproduce and is non-negotiable. M7 is the milestone where harness drift becomes irreversible if it ships.

### Implement

- Prompt route.
- Prompt async route.
- Deterministic fake/local provider for tests.
- Streaming assistant message deltas.
- Message completion event.
- Abort.
- Error event.
- Persisted transcript.
- Per-session runner with `BusyError` semantics matching [`run-state.ts`](../packages/opencode/src/session/run-state.ts) (one prompt per session at a time; no implicit queuing).
- Tool-call repair hook (case fix + `invalid` fallback) matching [`llm.ts:363-383`](../packages/opencode/src/session/llm.ts:363) — even before real tools are ported, the repair seam must exist so M8 plugs in cleanly.
- `_noop` tool injection condition matching [`llm.ts:242`](../packages/opencode/src/session/llm.ts:242).
- Parallel turn-startup reads (provider/config/auth) overlapped, mirroring [`llm.ts:95-103`](../packages/opencode/src/session/llm.ts:95).
- Concurrent tool-call resolution per assistant step (single-step join set with structured cancellation), matching [`processor.ts:564-568`](../packages/opencode/src/session/processor.ts:564). Even with a fake provider that emits at most one tool call, the join-set seam must be in place so M8 doesn't have to refactor it.
- Write-before-publish ordering for every part type produced during the turn (text, reasoning, tool-call, tool-result, finish, abort terminal).

### Then add one real provider path

The first (and for the lean target, only) real provider is **OpenAI Pro / ChatGPT Plus** via the OAuth-authenticated Responses API. See **Milestone 10** for the full scope. M7 only needs the minimum surface: OAuth token persistence, Responses API streaming consumer for the fake provider trace, and the `isOpenaiOauth` turn-assembly branch wired so M10's full implementation slots in without refactoring. All other providers (Kilo Gateway, API-key OpenAI, Anthropic, Gemini, OpenRouter) stay on Bun via the runtime resolver and are explicitly out of scope for the lean target.

### Oracle additions for M7

- One golden trace of a single-tool-call turn through the fake provider.
- One golden trace of a multi-tool-call step (two parallel tool calls completing in non-deterministic order, normalized by ID before diff).
- One golden trace of an abort mid-stream (the trace must end with the persisted abort terminal, no orphan parts).
- One golden trace of two concurrent sessions on one sidecar producing interleaved events without cross-session leakage.

### Exit gate

Sidebar first chat works against Rust, streams tokens to the existing UI, persists history, reloads correctly, and matches Bun golden trace after normalizing volatile IDs/timestamps. Two concurrent sessions complete without a single cross-session SSE event in stress test. Parallel tool calls in a single step finish in ~max(tool durations), not sum. Abort produces an identical persisted shape (modulo IDs) on Rust and Bun.

## Milestone 8: Permission, question, file, diff, and tool basics

### Route groups

- Permission routes from [`PermissionRoutes`](../packages/opencode/src/server/routes/instance/permission.ts:13).
- Question routes from [`QuestionRoutes`](../packages/opencode/src/server/routes/instance/question.ts:18).
- File routes from [`FileRoutes`](../packages/opencode/src/server/routes/instance/file.ts:12).
- Event routes from [`EventRoutes`](../packages/opencode/src/server/routes/instance/event.ts:12).

### Tool ladder

1. Tool registry/listing.
2. Read-only file/search tools.
3. Edit/write/apply-patch tools.
4. Permission allow/deny handling.
5. Shell/bash tool with process supervision.
6. Diff/revert integration.
7. Tool output truncation and error normalization.
8. Tool repair behavior.

### Cross-shell tool execution

The `bash` tool surface must work on Windows, where bash is not the default shell. Resolution order on Windows: WSL `bash` if available → Git Bash if installed (detect via `where bash` and `git --exec-path`) → cmd.exe-wrapped `bash.exe` if Git for Windows is on PATH → fail with a stable error name (`shell_unavailable`) if none resolve. PowerShell parity is **not** a goal — Bun's tool semantics assume bash quoting, redirects, and environment expansion; pretending PowerShell is interchangeable is a worse failure mode than refusing to run. The resolution order and fallback policy is documented in [`CONTRACT.md`](../packages/kilo-vscode-sidecar-rs/CONTRACT.md).

### Exit gate

A realistic task involving file read, edit, diff, permission prompt, denial path, and abort works through Rust with Bun-compatible session history. The bash tool runs on Windows hosts that have either WSL or Git for Windows installed, and returns `shell_unavailable` with a user-actionable message on hosts that have neither.

## Milestone 9: Agent Manager worktree and concurrency parity

### Route groups

- Experimental/worktree routes from [`ExperimentalRoutes`](../packages/opencode/src/server/routes/instance/experimental.ts:48).
- PTY routes from [`PtyRoutes`](../packages/opencode/src/server/routes/instance/pty.ts:14) if retained for terminal integration.

### Implement and verify

- Create/list/delete/reset worktree.
- Worktree diff summary/file views.
- Worktree session directory scoping.
- Session fork/continue flows.
- Concurrent multi-session prompts.
- Agent Manager state reload.
- Sidecar death and recovery without corrupting `.kilo` state.

### Exit gate

Agent Manager can run multiple sessions concurrently over one Rust sidecar with zero cross-session event leaks in stress tests.

## Milestone 10: OpenAI Pro / ChatGPT (Codex) provider — scoped first slice

### Scope decision

For the first stable Rust release, the only real provider implemented in the Rust sidecar is **OpenAI Pro / ChatGPT Plus / Pro** via the OAuth-authenticated **Responses API at `chatgpt.com/backend-api/codex/responses`**. Every other provider (API-key OpenAI, Anthropic, Gemini, OpenRouter, Kilo Gateway) stays on Bun via the runtime resolver until a follow-up plan revises this milestone.

This narrowing is intentional. The OpenAI Pro path is:

1. **The most common kilocode user surface** for the lean target. Users who are already paying for ChatGPT Pro/Plus get the agent loop without an additional API-key spend.
2. **Self-contained.** The OAuth flow, the Responses API stream shape, and the `isOpenaiOauth` branching in [`session/llm.ts:106-159`](../packages/opencode/src/session/llm.ts:106) are all implementable without a generic provider abstraction.
3. **Reusable later.** The Responses API is OpenAI's strategic streaming surface. Implementing it well first informs the eventual API-key-OpenAI and Anthropic ports without throwaway work.

Other providers are not blocked — they continue working on Bun via the `auto`/`bun` runtime modes and the runtime resolver in [`sidecar-runtime.ts`](../packages/kilo-vscode/src/services/cli-backend/sidecar-runtime.ts). A user signed in with both an Anthropic API key and an OpenAI Pro account will route OpenAI requests through Rust and Anthropic requests through Bun until M10b adds Anthropic.

### Reference implementation in Bun

| Concern | Bun source | Rust port lands in |
|---|---|---|
| OAuth PKCE + token exchange | [`plugin/codex.ts:23-129`](../packages/opencode/src/plugin/codex.ts:23) | `kilo-provider/src/openai_oauth.rs` (new) |
| Refresh-on-expiry, account-id extraction | [`plugin/codex.ts:404-465`](../packages/opencode/src/plugin/codex.ts:404) | same |
| Allowed Codex model list + cost zeroing | [`plugin/codex.ts:373-400`](../packages/opencode/src/plugin/codex.ts:373) | `kilo-provider/src/openai_models.rs` (new) |
| Responses API request shape | [`session/llm.ts:155-174`](../packages/opencode/src/session/llm.ts:155) (instructions, raw messages) | `kilo-provider/src/openai_responses.rs` (new) |
| `isOpenaiOauth` system-prompt branching | [`session/llm.ts:106-112,155-159`](../packages/opencode/src/session/llm.ts:106) | `kilo-session` turn assembler |
| Streaming consumer + tool-call dispatch | AI SDK's Responses transport (Bun side) | `kilo-session` model-stream consumer (own implementation against the Responses API event stream) |

### Implement (in order)

1. **Auth storage:** persist `{ type: "oauth", access, refresh, expires, accountId }` in `Auth` for `provider_id = "openai"`. Reuse the existing `PUT /auth/{providerID}` route registered in M5. Routes the persistence through the `KeyringStore` shim required by **Storage and process self-healing invariants → 5**; plaintext `auth.json` is preview-only and must not survive into stable.
2. **OAuth flow:** loopback HTTP server on port 1455, PKCE challenge, redirect handling, token exchange against `https://auth.openai.com/oauth/token`. Match the exact scopes and custom params Bun uses (`id_token_add_organizations=true`, `codex_cli_simplified_flow=true`). Headless device-flow variant ([`plugin/codex.ts:516-538`](../packages/opencode/src/plugin/codex.ts:516)) is optional for VS Code (browser flow is the default UX) but worth implementing for SSH/remote-extension cases.
3. **Refresh:** on expiry-on-call, refresh against `https://auth.openai.com/oauth/token` and persist back. Mirror the structure at [`plugin/codex.ts:425-440`](../packages/opencode/src/plugin/codex.ts:425).
4. **Model registry:** filter the static provider list down to the Codex-allowed models plus the gpt-5.2/5.3/5.4/5.5 base. Zero out costs (subscription-included). The Codex regex [`/^gpt-(\d+\.\d+)/`](../packages/opencode/src/plugin/codex.ts:388) gating is load-bearing — copy it.
5. **Responses API streaming client:** POST to `https://chatgpt.com/backend-api/codex/responses` with `Authorization: Bearer <access>` and `ChatGPT-Account-Id: <accountId>`. Parse the SSE event stream into the Rust equivalent of the AI SDK's stream parts (`text-delta`, `tool-call-delta`, `tool-call`, `tool-result`, `finish`). The wire shape is OpenAI's documented Responses event schema.
6. **Turn assembler with `isOpenaiOauth` branch:**
   - Skip the system-message-as-first-input wrap.
   - Compose `options.instructions` as `<soul>\n<system_array_joined>` per [`llm.ts:157`](../packages/opencode/src/session/llm.ts:157).
   - Pass `input.messages` raw (no synthetic system role).
7. **Tool-call dispatch:** spawn each parsed `tool-call` part into a `tokio::task::JoinSet` immediately as it arrives in the stream consumer, **not** at a drain point. See **Behavioral fidelity invariants → 2 (Parallel execution)**.
8. **Token usage + cost metadata:** zero costs but populate token counts from the Responses API `usage` field so the existing UI's usage tile renders.
9. **Error normalization:** map OpenAI HTTP errors and `error` stream events to the Bun `NamedError` shape (`{name, data: {message}}`) so the SDK error envelope round-trips. Reuse the M3-fix `internal_error` helper pattern.
10. **Abort:** thread `tokio::sync::CancellationToken` into the Responses HTTP request *and* every tool-call task. Aborting the session must close the stream and cancel in-flight tools atomically.

### Out of scope for this milestone

The following stay on Bun (and are explicitly NOT implemented in Rust):

- API-key OpenAI (Chat Completions API).
- Kilo Gateway (`@kilocode/kilo-gateway`), including profile, cloud-session, remote, and FIM surfaces. These are not migration targets for the Rust sidecar.
- Anthropic, Gemini, OpenRouter.
- Provider-specific tool-schema transforms beyond what Responses API requires.
- LiteLLM/Copilot `_noop` injection (M7 must stub the seam, but LiteLLM/Copilot still go to Bun).
- Workflow / DWS provider paths.

Provider expansion is not part of this migration plan.

### Testing strategy

- **Fake provider** in `kilo-provider/src/fake.rs` for deterministic CI: feeds canned Responses API SSE events. Used by M7 oracle traces.
- **Recorded Responses streams** captured from the real Codex endpoint (with redaction) for stream-parser regression tests.
- **OAuth flow contract tests** stub the `auth.openai.com` token endpoint and assert PKCE/refresh round-trips. Treat the live OAuth IdP as opt-in only.
- **Live smoke test** behind an env-gated flag (`KILO_OPENAI_PRO_LIVE_TEST=1`) that runs one full turn end-to-end against a real account. Run weekly in a non-CI cron, not on every PR.
- The oracle harness compares Bun and Rust on **observable** behavior: persisted message/part shape, SSE event order, token/cost reporting. It does not compare Responses API request bodies (those are owned by Rust now).

### Exit gate

A user signed into ChatGPT Pro/Plus through the Rust sidecar can complete a full agent turn — multi-tool steps, abort, refresh-on-expiry — with persisted history that round-trips to Bun if the user toggles the runtime back. Token usage renders in the UI. Other providers continue to function via the runtime resolver. The fake provider's deterministic transcripts diff identically against Bun's golden traces.

## Milestone 11: MCP migration ladder

### Order

1. MCP config parsing and status listing.
2. Stdio server connect/disconnect.
3. Tool discovery.
4. Tool invocation with timeout/cancel.
5. Tool-list-changed notifications.
6. HTTP/SSE MCP transports.
7. OAuth and callback handling.
8. Auth persistence and refresh.
9. Crash/reconnect behavior.
10. Multiple concurrent MCP servers.

### Exit gate

A real small MCP fixture server can be configured, connected, invoked, disconnected, crashed, and reconnected through Rust while producing Bun-compatible UI-visible events and errors.

## Milestone 12: Build, package, signing, and release artifacts

### Build targets

- Windows x64.
- Windows arm64 if supported by extension target.
- macOS x64.
- macOS arm64.
- Linux x64.
- Linux arm64.

### Packaging tasks

- Replace or extend [`local-bin.ts`](../packages/kilo-vscode/script/local-bin.ts:1) so it can stage Bun, Rust, or both.
- During preview, package both sidecars but only the target platform binary for each runtime.
- Ensure executable bit on Unix.
- Ensure Windows spawn works without console flashing via the extension process wrapper described in [`AGENTS.md`](../packages/kilo-vscode/AGENTS.md:210).
- Track sidecar binary size and VSIX size.
- Add checksums and version metadata.

### Signing tasks

- Sign Windows sidecar using the existing signing flow represented by [`sign-windows.ps1`](../script/sign-windows.ps1:1).
- Decide and document macOS sidecar signing/notarization requirements.
- Publish checksums for all target triples.

### Cross-platform path handling

- All path comparisons inside the sidecar normalize separators before comparing — Windows comparisons must not depend on `\\` vs `/`. The `worktree_list_reads_git_porcelain_paths` test failure observed during M6.1 was a symptom of missing normalization, not git output drift; the helper must live in `kilo-store/src/paths.rs` (or a peer crate) and every comparator uses it.
- UNC paths, drive letters, paths with non-ASCII characters, and paths over 260 characters round-trip through workspace and session storage without truncation. CI runs the path test suite on Windows.
- Console-flashing prevention on Windows extends to all child processes the sidecar spawns (bash tool, MCP stdio servers, git invocations), not just the sidecar binary itself. Use `CREATE_NO_WINDOW` on every `std::process::Command` Windows spawn, not only on top-level startup.

### Exit gate

Preview VSIX packages install and launch the Rust sidecar on all supported OS/architecture targets. The Windows path test suite is green. No spawned child process flashes a console window.

### Artifact metadata contract

[`packages/kilo-vscode/bin/sidecars.json`](../packages/kilo-vscode/bin/sidecars.json) is the release artifact manifest included in every VSIX alongside the staged sidecar binaries. Schema version `1` contains an `artifacts` array sorted by `kind:file`. Each artifact records `kind` (`bun-cli` or `rust-sidecar`), packaged binary `file`, target platform (`win32-x64`, `darwin-arm64`, etc., or the host target for local staging), SHA-256 checksum, byte `size`, and source `version` git hash when available. The manifest is deterministic for a fixed pair of sidecar binaries and source revisions; it intentionally omits timestamps.

Local extension builds emit the manifest from [`local-bin.ts`](../packages/kilo-vscode/script/local-bin.ts:1) after staging or reusing the host Bun/Rust sidecars. Cross-target VSIX builds emit an equivalent target-specific manifest from [`build.ts`](../packages/kilo-vscode/script/build.ts:1) immediately before `vsce package`, so [`packages/kilo-vscode/.vscodeignore`](../packages/kilo-vscode/.vscodeignore) includes it through `!bin/**`.

Signing remains a release-policy follow-up for M12: Windows has the existing helper [`script/sign-windows.ps1`](../script/sign-windows.ps1:1), but this slice does not wire it into packaging. macOS signing/notarization requirements are still undecided and must be settled before the all-target preview release gate.

## Milestone 13: Benchmark gates

Compare Bun oracle and Rust candidate with identical workspaces, settings, and provider fixtures.

| Metric | Preview gate | Stable target |
|---|---|---|
| Cold start to readiness | p95 under 5s; preferably under 2s | Faster than Bun p95 or no worse than Bun plus 15% |
| Time to first visible token | No worse than Bun plus 25% p95 | No worse than Bun plus 15% median |
| Idle RSS | No worse than Bun plus 15% | Lower than Bun or explicitly justified |
| Active RSS | No unbounded growth across repeated prompts | Returns near idle baseline after work completes |
| Idle CPU with SSE connected | Under 5% | Near zero steady-state |
| Agent Manager concurrency | Zero wrong-session events | Zero wrong-session events under stress |
| Shutdown | p95 under 1s, hard fail above 5s | Same |
| Process leaks | Zero leaked children | Zero leaked children |
| Package size | No worse than Bun plus 10-25% without approval | Smaller than Bun target preferred |

### M13 benchmark gate contract

The CI-safe gate lives in [`m13_benchmark_gate.rs`](../packages/kilo-vscode-sidecar-rs/crates/kilo-oracle/tests/m13_benchmark_gate.rs) and can be run from [`packages/kilo-vscode-sidecar-rs`](../packages/kilo-vscode-sidecar-rs) with:

```bash
cargo test -p kilo-oracle --test m13_benchmark_gate m13_rust_sidecar_benchmark_gate -- --nocapture
```

It is intentionally deterministic scaffolding rather than a noisy performance lab. The gate launches the Rust sidecar through the existing in-process oracle harness, measures cold start to readiness, drives the fake-provider first visible token path without live credentials, measures shutdown, emits structured JSON, and fails only concrete duration/package-size measurements that exceed configured thresholds. Defaults are conservative for local/CI stability: cold start under 5000 ms, first visible token under 5000 ms, shutdown under 1000 ms, and hard shutdown cap under 5000 ms. Override them with `KILO_M13_COLD_START_MS`, `KILO_M13_FIRST_TOKEN_MS`, `KILO_M13_SHUTDOWN_MS`, and `KILO_M13_SHUTDOWN_HARD_MS`.

Package-size gating is opt-in until the release artifact matrix is finalized: set `KILO_M13_PACKAGE_PATH` and `KILO_M13_PACKAGE_SIZE_BYTES` to make the gate compare bytes. RSS, CPU, and leaked-child measurements are reported as `unsupported`/`not_measured` fields in the JSON because portable deterministic sampling is not yet implemented in the oracle harness; those remain M13 gaps for a future platform-specific runner.

## Milestone 14: Rollout and rollback

### Rollout phases

1. Internal opt-in: Bun default, Rust hidden setting.
2. Internal default: Rust default, Bun fallback.
3. Preview opt-in: users can select Rust.
4. Preview default: Rust default, Bun fallback.
5. Stable opt-in: guarded by kill switch.
6. Stable default: Rust default, Bun fallback still packaged.
7. Bun removal: only after at least one clean release cycle with no storage rollback incidents.

### Rollback rules

- Bun fallback is packaged during preview and early stable.
- Runtime setting can force Bun with `kilo-code.new.sidecarRuntime = bun`; `KILO_VSCODE_SIDECAR_RUNTIME` overrides the setting for CI/dev emergency use.
- Local rollout phase is declared by `kilo-code.new.sidecarRollout` (`disabled`, `internal`, `preview`, `stable`) and can be overridden by `KILO_VSCODE_SIDECAR_ROLLOUT`.
- Emergency rollback kill switch is local/static for this slice: `KILO_VSCODE_SIDECAR_KILL_SWITCH=1` forces Bun by suppressing Rust candidates in both `auto` and forced `rust` plans. No live remote-config dependency is introduced for M14; a future release can wire this same policy to a remote source if one exists.
- Automatic fallback only before mutable Rust requests.
- Store remains Bun-compatible until Bun fallback is removed.
- Every sidecar artifact declares `contractVersion`, source `version`, target triple, and `compatibleOracleVersion` in `bin/sidecars.json`. `contractVersion = 1` means the current VS Code HTTP/SSE/process/storage contract. `compatibleOracleVersion = bun-cli-v1` means the artifact is expected to compare against the current packaged Bun CLI oracle contract.

### Stable rollout fallback policy

Through preview and early stable rollout windows, Bun remains packaged and is the only automatic fallback target. `auto` starts Rust first unless rollout is `disabled` or the kill switch is active; if Rust fails before any accepted mutation, startup tries Bun. After Rust accepts a mutating request, automatic fallback is suppressed to avoid storage/state rollback corruption. Forced `rust` normally has no automatic startup fallback, but rollout-disabled or kill-switch policy blocks Rust before spawn and selects Bun with an explicit diagnostic in the sidecar plan.

### Storage round-trip gate before Bun removal

Before M14 step 7 (Bun fallback removal), an automated test runs the full Bun ↔ Rust storage round-trip against representative session shapes: single message, multi-tool turn with parts, aborted turn with reasoning + abort terminal, archived session, forked session, session with summary diffs, session with revert state. Failures of any case block Bun removal. See **Storage and process self-healing invariants → 4**. The asymmetric M0/M5/M6 path "Rust reads Bun-written rows" is necessary but not sufficient — the inverse "Bun reads Rust-written rows" must also pass on every Rust schema change.

### Wire-protocol versioning beyond v1

- `contractVersion = 1` covers the M0-frozen surface. Any breaking change to route paths, request/response shapes, error envelope, or SSE event names increments the version. Additive changes (new optional field, new event name with documented client tolerance) do not.
- The artifact manifest's `compatibleOracleVersion` advances in lockstep with `contractVersion`.
- Sidecars announce their `contractVersion` in the readiness stdout line (extended format, backward-compatible) and on `GET /health`. The extension refuses to use a sidecar whose `contractVersion` is newer than the extension's compiled-in maximum.
- A deprecation cycle of one full release window precedes any contract version bump in stable; preview can bump freely.

### Remote kill-switch follow-up (M14b)

Once a remote-config surface exists in the extension (none today), wire the local kill-switch to consume it without changing M14's policy: the kill switch still forces Bun by suppressing Rust candidates. Until then, `KILO_VSCODE_SIDECAR_KILL_SWITCH=1` is the only mechanism. M14b is a separate plan revision when remote config lands.

## Issue-ready work breakdown

1. Create sidecar contract freeze doc and Bun oracle fixtures.
2. Add Rust workspace and skeleton crates.
3. Implement Rust process contract, auth, health, SSE heartbeat, and shutdown.
4. Add extension sidecar runtime resolver and Bun/Rust/Auto setting.
5. Add preview fallback and diagnostics.
6. Remove Marketplace command, service, panel, and webview paths.
7. Remove KiloClaw provider, command, serializer, bundle, and dependencies.
8. Port read-only global/config/provider/project/session routes.
9. Implement Bun-compatible session store read path.
10. Implement Rust event bus and SSE projection.
11. Implement session create/list/get/message history/write path.
12. Implement prompt route with deterministic fake provider.
13. Implement first real streaming provider path.
14. Implement abort and error event parity.
15. Implement permission and question flows.
16. Implement file/search/edit/diff tool basics.
17. Implement shell tool process supervision.
18. Implement Agent Manager viewed/session concurrency behavior.
19. Implement worktree create/list/delete/reset/diff routes.
20. Implement PTY/terminal routes if retained.
21. Implement OpenAI Pro / ChatGPT Plus provider via OAuth + Responses API (Codex endpoint). Other providers stay on Bun for the lean target — see Milestone 10.
22. Implement MCP ladder: config, stdio, tool discovery, invocation, HTTP/SSE, OAuth.
23. Add cross-platform Rust build matrix.
24. Add VSIX packaging for Bun/Rust preview binaries.
25. Add signing/checksum/version metadata.
26. Add benchmark suite comparing Bun and Rust.
27. Run internal opt-in rollout.
28. Run preview opt-in rollout.
29. Run preview default rollout.
30. Ship stable Rust default with Bun fallback.
31. Remove Bun fallback after rollback window.
32. Implement first-run schema bootstrap (`init_schema` unconditional + `create table if not exists`) and the matching fresh-install/half-init oracle fixtures.
33. Implement schema-version migration runner in `kilo-store/src/migrations.rs` with `PRAGMA user_version` and CI gate against unversioned schema changes.
34. Implement internal-error normalization wrapping `rusqlite::Error`, `std::io::Error`, and panics at the SDK boundary; document the allowed `name` set in `CONTRACT.md`.
35. Implement Bun ↔ Rust storage round-trip oracle fixture (`kilo-oracle/tests/storage_roundtrip.rs`).
36. Implement OS-keychain `KeyringStore` shim (DPAPI / Keychain / libsecret) with documented plaintext fallback gated by env flag.
37. Implement structured `tracing` subscriber with file rotation, `KILO_LOG`/`RUST_LOG` control, and `<state_dir>/kilo/log/sidecar.log` default destination.
38. Implement opt-in `Telemetry` trait seam (no-op default; one structured emit-point per failure class).
39. Implement single-sidecar-per-user lock at `<state_dir>/kilo/sidecar.lock` with health-probe + take-over policy.
40. Pick lazy-on-chat activation event in `package.json`; add `KILO_VSCODE_SIDECAR_PRESPAWN=1` for benchmarking.
41. Add request-body, SSE-client-count, session-quota, message-part, and tool-output limits in `kilo-server/src/limits.rs` with stable error names at the route layer.
42. Add `rust-toolchain.toml`, `deny.toml` license/advisory policy, and the dependency-bump PR convention.
43. Add cross-shell `bash` resolution on Windows (WSL → Git Bash → cmd-wrapped) with `shell_unavailable` named error.
44. Add path-normalization helper in `kilo-store/src/paths.rs` and Windows path-handling test suite (UNC, drive letters, non-ASCII, long paths).
45. Add `route_inventory` PR check that fails if `docs/route-inventory.md` is unchanged when route handlers move.
46. Add `contractVersion` advancement policy, readiness-line extension, and `GET /health` reporting; teach the extension to refuse a too-new sidecar.

## Highest-risk areas

- **Agentic-loop harness drift (M7+):** the single greatest product risk. The Bun loop's [`streamText`](../packages/opencode/src/session/llm.ts:357) shape, tool-repair seam, `_noop` injection, and active-tools filter are load-bearing UX. A Rust loop that "tidies" any of these silently degrades the product. See **Behavioral fidelity invariants** above.
- **Parallel-execution regression (M7/M9):** Bun resolves concurrent tool calls per step, overlaps turn-startup reads, and runs Agent Manager sessions in parallel. A Rust port that serializes tool calls or single-threads the executor cuts effective throughput on every multi-tool turn — invisible to contract tests, immediately visible to users. Tokio `JoinSet` + per-session `CancellationToken` is the minimum mapping.
- **Cancellation/abort fidelity:** `BusyError` semantics, mid-stream abort that closes the transcript with a final assistant part, and idempotent `onIdle` cleanup are all UX-visible. Generic Rust cancellation that leaves sessions stuck in "busy" or that swallows in-flight tool output corrupts state.
- **OpenAI Pro / Responses API stability (M10):** the `chatgpt.com/backend-api/codex/responses` endpoint is OpenAI-internal and not formally documented as a stable surface. Schema drifts (event-stream part shapes, finish-reason taxonomy, refresh-token semantics) will surface as silent transcript corruption. Mitigations: recorded-stream regression tests, an opt-in live smoke test that runs at least weekly, and an explicit contract version field in the auth blob so we can detect drift on token refresh.
- **OAuth refresh-on-mid-stream-expiry:** Bun refreshes only when the next request fires; the Rust port must do the same and must not drop the in-flight stream. A naive "refresh-then-retry" implementation that resends the prompt re-runs the agent step with the same input, doubling tool execution. Refresh must wrap only new requests, not active streams.
- **`isOpenaiOauth` branching drift:** the system-prompt-as-instructions vs system-message-as-first-input split in [`llm.ts:106-159`](../packages/opencode/src/session/llm.ts:106) is invisible to contract tests but changes how the model interprets the conversation. Rust must reproduce the branch exactly, including the soul prepend at [`llm.ts:157`](../packages/opencode/src/session/llm.ts:157).
- Provider parity (intentionally non-migrated providers): for any provider or product surface that stays on Bun/outside Rust (Anthropic, Gemini, OpenRouter, Kilo Gateway, profile/cloud/remote/FIM, API-key OpenAI), the runtime resolver must not route real provider work to Rust. Rust may expose fallback response shapes for extension background calls, but those handlers are not parity implementations.
- Storage compatibility: any Rust-only migration can block Bun rollback.
- SSE shape drift: small event differences can break UI state inside [`KiloConnectionService`](../packages/kilo-vscode/src/services/cli-backend/connection-service.ts:61).
- Agent Manager concurrency: worktree sessions require strict directory and session isolation.
- MCP OAuth and transports: likely cross-platform edge cases.
- Packaging/signing: can block release even after functionality works.
- Marketplace/KiloClaw removal: can leave hidden command, serializer, localization, or bundle references.
- **Persistent-state self-healing (M5/M6+):** the sidecar opens five files on startup and writes to a sixth on first turn. Any one of them in a half-initialized state (empty file, partial JSON, missing schema) blocks the user. The HEAD schema-init bug fixed in M6.1 is the canonical example. Mitigations live in **Storage and process self-healing invariants**; the cost of skipping any of them is one failed user session per occurrence with no log to diagnose from.
- **Bun ↔ Rust storage asymmetry (M14):** the rollback window's only safety net is that Bun can read Rust-written data. A column-order, JSON-default, or unknown-field-loss regression is silent until the day Bun is removed and irreversible after that day. The round-trip oracle (Storage invariant 4) is the only line of defense.
- **Auth secret exposure:** plaintext OAuth tokens at `data/kilo/auth.json` are an exfiltration target on shared machines, CI runners, and backup scrapers. Migration to the OS keychain is preview-blocking, not stable-blocking, but cannot slip past M14 without a documented exception.
- **Multi-window contention:** without the per-user sidecar lock, two VS Code windows on the same workspace race on `kilo.db`. SQLite WAL handles concurrent writes safely at the file level, but the in-process writer mutex doesn't coordinate across processes — split-brain session state is the failure mode.
- **Production observability gap:** without structured logging, every user-visible failure becomes a re-run-and-hope debugging session. The schema-init bug surfaced as one toast and zero log entries; an opt-in `tracing` subscriber + per-failure-class telemetry seam is the minimum supportable baseline.
- **Cross-shell tool execution on Windows (M8):** `bash` is not installed by default on Windows. A naive port that assumes bash, or that silently substitutes PowerShell, will produce subtly wrong tool output (different quoting, different env expansion) that contract tests cannot catch.
- **Wire-protocol versioning beyond v1:** `contractVersion = 1` is declared in M14 but no policy governs how it advances. Without a documented advancement and deprecation policy, the day a breaking change ships is the day every older extension version breaks silently.

## Final summary

Build a Kilo-specific Rust sidecar behind the existing [`ServerManager`](../packages/kilo-vscode/src/services/cli-backend/server-manager.ts:18) process seam and [`KiloConnectionService`](../packages/kilo-vscode/src/services/cli-backend/connection-service.ts:61) HTTP/SSE seam. Freeze the current Bun sidecar as the oracle, add a Rust skeleton, add a runtime switch, prune Marketplace and KiloClaw, then migrate vertical slices: read-only UI load, session/store/event parity, first chat turn, tools/permissions, Agent Manager worktrees, **OpenAI Pro / ChatGPT Plus via OAuth + Responses API as the only Rust-side provider for the lean target**, MCP, packaging, benchmarks, and rollout. All other providers and Kilo Gateway/profile/cloud/remote/FIM surfaces are explicitly outside the Rust migration target. Keep Bun fallback until Rust has passed contract, storage, sidebar, Agent Manager, OpenAI-Pro provider, tool/MCP, benchmark, and packaging gates.

The migration succeeds only if the Rust sidecar reproduces three categories of invariants. The **Behavioral fidelity invariants** cover Bun's agentic-loop harness, parallel tool-call execution, per-session cancellation semantics, and the `isOpenaiOauth` system-prompt branching — without these, the product loses its UX and throughput. The **Storage and process self-healing invariants** require that no library error text ever reaches a user toast, that persistent files survive partial init, that schema evolves through versioned migrations, and that the Bun↔Rust round-trip stays verified until Bun fallback is removed — without these, every fresh install or partial crash blocks a user. The **Operational invariants** require structured logging, single-sidecar-per-user coordination, lazy activation, and resource limits at the boundary — without these, the sidecar is unsupportable in production. The HTTP/SSE contract is the visible surface; the agent loop, the storage layer, and the operational surface underneath it are where Kilo Code's product value and supportability live. Translate behavior, not code structure — and self-heal everything that can be observed in a half-broken state.
