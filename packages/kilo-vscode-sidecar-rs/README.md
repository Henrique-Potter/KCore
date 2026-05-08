# Kilo VS Code Rust sidecar preview

This package is a Kilo-specific Rust workspace for the VS Code sidecar migration. It intentionally lives outside `packages/opencode` so early preview work does not increase upstream OpenCode merge churn.

## Current scope

This currently includes milestones 0-3 scaffolding plus the first Milestone 5 read-only route slice:

- Add the Rust workspace and crate seams.
- Provide a preview binary that accepts `serve --port 0` plus compatible host/port flags.
- Bind loopback, print the existing readiness line, enforce Basic Auth when `KILO_SERVER_PASSWORD` is set, expose `/global/health`, and keep `/global/event` open with server heartbeat events.
- Expose read-only startup/render routes for path/config/provider/agent/project/session data.
- Read Bun-created sessions, messages, and parts from the existing `kilo.db` SQLite database when present; otherwise return safe empty fallbacks.
- Keep Bun as the VS Code extension default and allow Rust only through the preview runtime switch or environment override.

It does not implement write route parity, real provider auth, tool execution, MCP, session creation/mutation, Marketplace removal, or KiloClaw removal.

## Workspace layout

| Path | Purpose |
|---|---|
| `crates/kilo-vscode-sidecar` | Thin binary: command parsing, process entrypoint, signal handling. |
| `crates/kilo-server` | HTTP/SSE router, middleware, route handlers, OAuth, the agent turn loop, fake-provider tools, permissions, file search. Internal module layout in [`docs/architecture.md`](docs/architecture.md). |
| `crates/kilo-protocol` | Frozen preview wire types and version constants. |
| `crates/kilo-store` | Bun-compatible session/config/auth SQLite + JSON store. |
| `crates/kilo-provider` | Provider registry, OpenAI Responses API client, OAuth helpers. |
| `crates/kilo-session` | Reserved (empty) — future M8/M9 extraction target for session lifecycle. |
| `crates/kilo-tools` | Reserved (empty) — future M8/M9 extraction target for built-in tools + permission. |
| `crates/kilo-mcp` | Reserved (empty) — future MCP transport seam. |
| `crates/kilo-oracle` | Bun/Rust oracle fixture harness for behavioral parity. |

For the internal module tree of `kilo-server` (state, http, routes, oauth, agent,
util) and the in-flight migration sequence, see
[`docs/architecture.md`](docs/architecture.md).

## Build and run

```sh
cargo fmt --all
cargo check --workspace
cargo run -p kilo-vscode-sidecar -- serve --port 0
```

## M7 first-chat smoke

The deterministic sidebar first-chat smoke is an oracle integration test instead of a VS Code UI e2e. It launches the Rust sidecar, opens the same `/global/event` SSE stream consumed by the extension, creates a session, sends a first prompt through `prompt_async` with the Rust fake provider, verifies streamed text deltas, then reads the persisted transcript back.

```sh
cargo test -p kilo-oracle --test m7_rust_fixtures m7_sidebar_first_chat_smoke_streams_persists_and_reads_back -- --nocapture
```

For a manual live UI check after building the Rust binary, launch the extension with the local sidecar path:

```bat
set KILO_VSCODE_RUST_SIDECAR_PATH=packages\kilo-vscode-sidecar-rs\target\debug\kilo-vscode-sidecar.exe
bun run extension
```

The scripted smoke does not require provider credentials. The manual UI path uses the normal sidebar model selection, so it still requires a configured provider unless the UI grows a fake-provider developer toggle.

For VS Code extension development, point the extension at a locally built sidecar:

```sh
set KILO_VSCODE_RUST_SIDECAR_PATH=packages\kilo-vscode-sidecar-rs\target\debug\kilo-vscode-sidecar.exe
```

## Rollback

There is no extension runtime switch now. Roll back by reverting the Rust-only sidecar wiring and rebuilding the extension package.

See `CONTRACT.md` for the frozen preview seam.
