# `kilo-oracle` design — Milestone 0 Bun oracle harness

Version: `kilo-vscode-sidecar.preview.0` aligned with [`CONTRACT.md`](../CONTRACT.md).
Owner: Tier A, Milestone 0.
Status: design + initial implementation, build unverified locally (no `cargo` on PATH).

## Purpose

`kilo-oracle` records the **observable behavior of the Bun-built sidecar**
across the surfaces the VS Code extension relies on, and makes those recordings
replayable. In Milestone 0 the recordings are the contract; in later milestones
the Rust sidecar is judged by diffing its own behavior against these golden
fixtures.

The crate is intentionally **decoupled from the Rust sidecar runtime**. It
talks to the Bun process the same way the VS Code extension does — through
stdout, HTTP, and SSE — so it can be re-pointed at the Rust binary later by
swapping a single `binary_path` argument.

## Crate shape

### Layout

```
crates/kilo-oracle/
├── Cargo.toml
├── src/
│   ├── lib.rs              // Re-exports + crate-level docs
│   ├── error.rs            // OracleError, OracleResult
│   ├── spawn.rs            // SidecarHandle: spawn Bun, parse readiness, stop
│   ├── readiness.rs        // parse_ready_line(), normalization
│   ├── http.rs             // OracleClient: minimal Basic-auth HTTP client
│   ├── sse.rs              // SseRecorder: line-by-line SSE capture
│   ├── normalize.rs        // Volatile-field redaction (UUIDs, ports, paths…)
│   ├── fixture.rs          // FixtureFile: read/write JSONL goldens
│   └── scenarios/          // Higher-level capture flows
│       ├── mod.rs
│       ├── startup.rs      // readiness + /global/health
│       ├── global_event.rs // first N seconds of /global/event
│       ├── session.rs      // create/list/delete (Bun-driven)
│       ├── prompt.rs       // promptAsync + matching SSE trace
│       ├── permission.rs   // allow / deny round trips
│       ├── abort.rs        // session.abort during stream
│       └── concurrent.rs   // Agent Manager: 2+ sessions in parallel
└── tests/
    ├── readiness_parse.rs  // pure-function determinism tests
    ├── normalize.rs        // golden normalization tests
    └── replay.rs           // load fixtures, assert deterministic shape
```

`lib.rs` exposes the public surface:

```rust
pub use error::{OracleError, OracleResult};
pub use fixture::{Fixture, FixtureFile, FixtureFrame};
pub use http::OracleClient;
pub use normalize::{Normalizer, Redactions};
pub use readiness::{ReadyLine, parse_ready_line};
pub use sse::{SseFrame, SseRecorder};
pub use spawn::{SidecarHandle, SpawnConfig};
pub use scenarios::{
    ScenarioRunner, StartupScenario, GlobalEventScenario,
    SessionScenario, PromptScenario, PermissionScenario,
    AbortScenario, ConcurrentScenario,
};
```

### Library + thin binary?

The harness is a **library only** for M0. Tests in `crates/kilo-oracle/tests/`
drive everything that needs to run under `cargo test`. We do **not** ship a
`bin/oracle` CLI yet because:

1. Capturing flows that need a real provider key (prompt / permission / abort /
   concurrent) cannot be made hermetic in CI today, so a CLI would be a foot-gun.
2. The binary would just be a wrapper around `ScenarioRunner::run()` — easy to
   add later as `crates/kilo-oracle/bin/record.rs` once the provider story is
   resolved.

The crate will be **`[lib]` only with integration tests**; no `[[bin]]` target
in M0.

## Process spawn

### Choosing the binary path

The VS Code extension resolves the Bun sidecar at
[`server-manager.ts:160`](../../kilo-vscode/src/services/cli-backend/server-manager.ts#L160)
via:

```ts
const binName = process.platform === "win32" ? "kilo.exe" : "kilo"
const cliPath = path.join(this.context.extensionPath, "bin", binName)
```

The oracle reproduces this convention. `SpawnConfig` defaults the binary to
`<workspace-root>/packages/kilo-vscode/bin/kilo[.exe]`, but every field is
overridable so a CI driver can point at a freshly-built binary in
`packages/kilo-cli-bun/dist/` or wherever the build pipeline drops it.

`SpawnConfig` also accepts `KILO_ORACLE_BINARY` as an env-var override so tests
can be run against a non-default build without touching code.

### Spawn arguments

We mirror the extension exactly: `serve --port 0`. Working directory is taken
from `SpawnConfig::cwd` (default: a fresh per-test `tempdir`, **not** the repo
root, so we don't pollute the developer's `.kilo`). Env defaults:

| Variable                          | Value                                    |
|-----------------------------------|------------------------------------------|
| `KILO_SERVER_PASSWORD`            | random 64-character ASCII alphanumeric string (L4: not hex; see [`spawn.rs::random_password`](../crates/kilo-oracle/src/spawn.rs)) |
| `KILO_CLIENT`                     | `vscode`                                 |
| `KILO_ENABLE_QUESTION_TOOL`       | `true`                                   |
| `KILOCODE_FEATURE`                | `vscode-extension`                       |
| `KILO_TELEMETRY_LEVEL`            | `off`                                    |
| `KILO_APP_NAME`                   | `kilo-code`                              |
| `KILO_PLATFORM`                   | `vscode`                                 |
| `KILO_DISABLE_CLAUDE_CODE`        | `true`                                   |
| `MIMALLOC_PURGE_DELAY`            | `0`                                      |
| `KILO_HOME`                       | `<spawn-cwd>/.kilo` (forces local store) |

`KILO_HOME` redirection is the M0 mechanism that makes oracle runs self-contained.
This is verified to be honored by the Bun backend; if a future Bun release
ignores it, the oracle falls back to running the entire test inside `tempdir`
as cwd.

### Readiness parsing

The Bun process prints exactly:

```
kilo server listening on http://127.0.0.1:<port>
```

(verified at [`packages/opencode/src/cli/cmd/serve.ts:17`](../../opencode/src/cli/cmd/serve.ts#L17))

`parse_ready_line()` accepts the same regex the extension uses
(`listening on http://[\w.]+:(\d+)`) so the oracle and the extension never
disagree on what "ready" means. It returns a structured `ReadyLine { host,
port, raw }` so the readiness fixture can carry the host as well as the port.

### Shutdown

`SidecarHandle::Drop` follows the extension pattern: SIGTERM → 5 s grace →
SIGKILL. On Windows we just `kill` the child handle (matches
`server-manager.ts:179`).

## HTTP client

Choice: **`reqwest`** with `rustls` + `json` features.

Rationale:
- It's the de-facto Rust HTTP client and already lives in many adjacent crates,
  so it minimizes audit surface.
- Supports streaming response bodies natively (needed for SSE).
- `rustls` keeps us off OpenSSL (Windows build hygiene).
- Has built-in basic auth helper.

Alternatives considered:
- `hyper` directly — too much boilerplate for fixture capture.
- `ureq` — synchronous; loses ergonomics for SSE.

The HTTP client is wrapped by `OracleClient` which preconfigures:
- `http://127.0.0.1:<port>`
- Basic auth `kilo:<KILO_SERVER_PASSWORD>` (matching
  [`CONTRACT.md`](../CONTRACT.md)).
- `x-kilo-directory` header (matching the SDK's `client.ts` rewrite logic).

`OracleClient` exposes typed helpers for the routes the M0 scenarios need
(`global_health`, `global_event_stream`, `session_create`, `session_list`,
`session_delete`, `permission_list`, `permission_reply`, `session_abort`,
`session_prompt_async`). Anything else uses a generic `request_json()` escape
hatch.

## SSE capture

`SseRecorder` is a **line-by-line decoder** that reads the streaming response
body of `GET /global/event` and produces `SseFrame { event, data, id, retry,
timestamp }` records.

We deliberately do **not** parse the JSON payload at recording time:

- Recording stays loss-less. If Bun changes a payload field between recording
  and replay, we can still see the diff in raw form.
- The contract is "what bytes go over the wire", not "what serde sees".

A second pass during fixture *writing* parses each frame's `data:` field as
JSON and stores both the raw text and a normalized object. This is the only
JSON parsing the oracle does inline. If parsing fails the frame is recorded as
`{ "raw": "...", "parse_error": "..." }` rather than failing the run.

### SSE termination

For scenarios where we want a finite trace, `SseRecorder` accepts a
`StopCondition`:

- `Frames(n)` — capture exactly N data frames.
- `Duration(t)` — capture for T wall-clock seconds.
- `EventType(s)` — stop when a payload `type` matches `s` (e.g.
  `session.idle`). Used by [`PromptScenario`](../crates/kilo-oracle/src/scenarios/prompt.rs)
  to terminate on the first idle event.
- `Predicate(F)` — caller-supplied closure for complex flows. Used by
  [`AbortScenario`](../crates/kilo-oracle/src/scenarios/abort.rs) (to wait
  for the first `message.updated` and then abort) and
  [`ConcurrentScenario`](../crates/kilo-oracle/src/scenarios/concurrent.rs)
  (to wait until both sessions have reached `session.idle`).

Internally everything funnels into a `tokio::select!` between
`abort_signal` and the next-frame future.

## Fixture file format

### Why JSONL

Each scenario writes a `.jsonl` file (one JSON object per line). Reasoning:

- **Diff-friendly**. A flake in frame 17 surfaces as a 1-line diff, not a
  thousand-line context blob.
- **Streaming-friendly**. Recording can flush as it goes; replay can be
  line-buffered.
- **Human-readable**. A reviewer can `head -n 20` a fixture and understand it.

A single JSON document is used only for the small startup fixture, where there
are no frames and the readiness payload is a fixed shape.

### Frame schema

```jsonc
// fixtures/sse/global-event-bootstrap.jsonl
{"frame_index": 0, "wall_offset_ms": 12,   "raw": "data: {\"directory\":null,\"payload\":{\"type\":\"server.connected\",\"properties\":{}}}\n", "payload": {"type":"server.connected","properties":{}}, "directory": null}
{"frame_index": 1, "wall_offset_ms": 10024,"raw": "data: {\"directory\":null,\"payload\":{\"type\":"server.heartbeat","properties\":{}}}\n", "payload": {"type":"server.heartbeat","properties":{}}, "directory": null}
```

`wall_offset_ms` is normalized to multiples of the heartbeat tick and **bucketed**
to 1 s granularity (10 000 / 20 000 / 30 000) to keep heartbeat fixtures stable
across machines. See *Normalization*.

### Startup fixture

```jsonc
// fixtures/startup/ready-line.json (keys serialize alphabetically because
// serde_json defaults to BTreeMap; the inline example below preserves the
// recorder's output order for readability).
{
  "contract_version": "kilo-vscode-sidecar.preview.0",
  "scenario": "startup",
  "stdout_line": "kilo server listening on http://127.0.0.1:<PORT>",
  "parsed": { "host": "127.0.0.1", "port": "<PORT>" },
  "health_response_status": 200,
  "health_response_body": { "healthy": true, "version": "<VERSION>" },
  "notes": [/* reviewer-facing strings; see StartupScenario::STARTUP_NOTES */]
}
```

`<PORT>` and `<VERSION>` are deliberate placeholders written by the
normalizer:

- `<PORT>` is produced both by the string-form regex (`:NNNNN/` → `:<PORT>/`)
  and by the recursive walker for numeric `port` values keyed under `port`
  (which serialize as a JSON number, not a string).
- `<VERSION>` is keyed entirely off the field name. Any string under a
  `version` or `bun_version` key is replaced wholesale, regardless of
  content.

The shipped golden is byte-for-byte equal to what
[`StartupScenario::record_to`](../crates/kilo-oracle/src/scenarios/startup.rs)
produces. See `tests/startup_recorder.rs` for the regression that locks
that contract.

### Store fixture

`fixtures/store/empty.json` records the initial `.kilo/` directory layout that
Bun creates on first launch, captured as a list of relative paths and SHA-256
content hashes (only for files smaller than 16 KiB; larger ones get a size
record only). This is what later milestones diff Rust's own store layout
against.

## Normalization

Every fixture passes through `Normalizer::normalize_value()` on write, and
**the same normalization on read** when comparing against a recorded run.
Volatile fields handled in M0:

| Field                          | Replacement              | Detection                              |
|--------------------------------|--------------------------|----------------------------------------|
| Server password                | `<KILO_SERVER_PASSWORD>` | env-var match against `password`       |
| Port number                    | `<PORT>`                 | URL parsing in stdout / config dumps   |
| Session ID (`ses_*`, ULID)     | `<SESSION_ID:N>`         | regex `\b(ses|sess)_[A-Za-z0-9]+`      |
| Message ID                     | `<MESSAGE_ID:N>`         | regex `\bmsg_[A-Za-z0-9]+`             |
| Part ID                        | `<PART_ID:N>`            | regex `\bprt_[A-Za-z0-9]+`             |
| Permission/question request ID | `<REQUEST_ID:N>`         | regex `\b(perm|qst)_[A-Za-z0-9]+`      |
| Generic UUID                   | `<UUID:N>`               | RFC 4122 regex                         |
| Unix epoch ms / ns             | `<TIMESTAMP:N>`          | numeric field names: `created`, `time`, `updated`, `ts`, `timestamp`, `completedAt`, `startedAt` |
| Wall-clock duration            | bucketed integer (1 s granularity) | `wall_offset_ms` is rounded to the nearest 1000 by [`Normalizer::normalize_duration_ms`](../crates/kilo-oracle/src/normalize.rs) and stored as the rounded integer (no `<DURATION_BUCKETED>` token — see [`fixture.rs::FixtureFrame::wall_offset_ms`](../crates/kilo-oracle/src/fixture.rs)) |
| Absolute path with workspace   | `<WORKSPACE>/...`        | prefix match against spawn cwd         |
| Absolute path with home        | `<HOME>/...`             | prefix match against `HOME`/`USERPROFILE` |
| Path separator                 | `/`                      | `\\` → `/` after prefix substitution   |
| Process PID                    | `<PID>`                  | env-var match                          |

Order matters: the longest replacement (workspace path) is tried first to avoid
double-substitutions. Identifier replacement is **stable per scenario** —
the first session ID seen becomes `<SESSION_ID:1>`, the second `<SESSION_ID:2>`,
etc. — so a multi-session scenario produces a deterministic mapping.

The mapping is recorded alongside the fixture inside the `identity_map`
field of the paired `<fixture>.meta.json` ([`fixture.rs`](../crates/kilo-oracle/src/fixture.rs)
`FixtureMeta::identity_map`) so a reviewer can tell which concrete IDs were
collapsed without an extra companion file.

## Deterministic replay

Replay does not re-spawn Bun. It just:

1. Loads the fixture file.
2. Re-normalizes each frame (idempotent — running normalize over an already
   normalized frame must be a no-op).
3. Asserts that the loaded frames serialize back to the same canonical bytes
   (deterministic round-trip).
4. Asserts that the parsed payload satisfies the contract version (e.g.
   `payload.type` is in the known set).

Test seed: scenarios that need randomness (e.g. picking a session name) draw
from `rand::rngs::StdRng::seed_from_u64(0xCAFE)` so two recordings produce the
same prompt text. The seed is fixed in code, not env-controlled.

## Verification & exit gate

The plan's exit gate is "Bun oracle tests pass consistently". Concretely:

- `cargo test -p kilo-oracle` succeeds with **no network**.
  All recording-driven tests are gated behind `#[ignore]` and run only when
  `KILO_ORACLE_RECORD=1` is set, since they need Bun + a real provider.
- `cargo test -p kilo-oracle -- --ignored` is the developer command that
  re-records fixtures. It is **not** part of the pass gate; it is run on
  demand and inspected by hand.
- `cargo test -p kilo-oracle` (the default, non-ignored set) loads every
  fixture in `fixtures/` and asserts deterministic replay + canonical shape.
  This is what "passes consistently" means in M0: the recordings round-trip
  cleanly, and any drift in the fixture format breaks the build.

This separation matches the plan's intent: M0 freezes the oracle. Later
milestones diff Rust output against the same fixtures.

## How later milestones consume fixtures

1. **M1/M2 (Rust skeleton).** A new helper `kilo-oracle::compare::diff_against`
   takes a live `OracleClient` pointed at the Rust sidecar and a fixture file,
   runs the same scenario, and emits a diff. Rust passes the contract slice
   when the diff is empty modulo declared `<...>` placeholders.
2. **M3 (Runtime switch).** The runtime resolver runs the compare helper at
   startup in dev builds and surfaces drift to the extension log.
3. **M4+ (Route parity).** Each new Rust route lifts itself into the oracle by
   adding a scenario that records the Bun side and asserts the Rust side
   matches.

Because the oracle never imports `kilo-server` or `kilo-protocol`, this lateral
dependency stays clean — no risk of accidentally testing Rust against Rust.

## Crate dependencies

```toml
[dependencies]
anyhow = "1"
base64 = { workspace = true }
futures-util = "0.3"
once_cell = "1"
rand = { version = "0.8", features = ["std_rng"] }
regex = "1"
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json", "stream"] }
serde = { workspace = true }
serde_json = { workspace = true }
sha2 = "0.10"
tempfile = "3"
thiserror = "1"
tokio = { workspace = true, features = ["macros", "process", "rt-multi-thread", "time", "io-util", "sync"] }
tokio-stream = "0.1"
tracing = "0.1"
url = "2"

[dev-dependencies]
pretty_assertions = "1"
```

`reqwest` and `regex` are heavyweight, but they unlock the rest of the crate and
will appear in production crates eventually anyway. `tempfile` is dev-quality
across Windows and Unix — same as what the existing extension test infrastructure
relies on.

## Out of scope for M0

- No actual prompt fixture is recorded today (no provider key in CI). The
  `PromptScenario` driver is implemented but its captured fixture is a
  documented stub.
- No SSE comparator that diffs a Rust trace against the Bun trace. That lives
  in M2.
- No fixture for OAuth flows; those need an interactive browser hand-off.
- No worktree fixture; Agent Manager scenarios assume an already-initialized
  worktree.

## Risks

- **Bun output drift.** If the Bun readiness line ever changes, the oracle will
  catch it on the next recording, but the regex sourced from the extension is
  the same as the one in `server-utils.ts`, so the failure mode is "regenerate
  fixtures" not "silent breakage".
- **Heartbeat jitter on slow Windows boxes.** Bucketing `wall_offset_ms` to 1 s
  is the mitigation; if that turns out to be too tight, expand to 2 s and
  re-record.
- **Env leakage on Windows.** If a developer has `KILO_HOME` set globally, the
  oracle could write into their real `.kilo`. The mitigation is implemented as
  an **allowlist**, not a blocklist: `SpawnConfig::env_allowlist`
  ([`spawn.rs`](../crates/kilo-oracle/src/spawn.rs)) starts empty plus a
  hard-coded set of OS-essential vars (`PATH`, `SystemRoot`, `USERPROFILE`,
  `HOME`, `TEMP`, etc.). Anything else from the parent environment is dropped
  before the oracle layers in its own `KILO_*` defaults via
  `build_env`. This is strictly stricter than a blocklist — even an
  unanticipated `KILO_*_OVERRIDE` cannot leak in.
