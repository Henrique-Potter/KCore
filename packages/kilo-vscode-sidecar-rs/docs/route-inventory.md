# VS Code sidecar route inventory

Source-of-truth scan of every SDK operation actually invoked by
`packages/kilo-vscode/`. Method/path columns reflect the generated SDK at
[`packages/sdk/js/src/v2/gen/sdk.gen.ts`](../../sdk/js/src/v2/gen/sdk.gen.ts);
the URL strings in this table were verified against that file.

The inventory is the **whitelist** for the Rust port. Anything not listed here
must remain on Bun until it is added intentionally.

> Note on directory scoping: nearly every non-global route requires a
> `directory` query parameter. The SDK rewrites it from the
> `x-kilo-directory` header in [`packages/sdk/js/src/v2/client.ts`](../../sdk/js/src/v2/client.ts).
> Rust must implement the same header-to-query rewrite to remain
> drop-in.

## Family overview

| Family       | Routes used | Streaming | Required by                          |
|--------------|-------------|-----------|--------------------------------------|
| Global       | 5           | 1 (SSE)   | activation, SSE adapter              |
| Auth         | 2           | -         | provider sign-in                     |
| Config       | 1           | -         | settings sync                        |
| Path         | 1           | -         | model-state worktree resolution      |
| Project      | 0           | -         | (unused by VS Code today)            |
| Provider     | 1           | -         | provider list                        |
| Session      | 13          | 1 (prompt)| chat, agent manager                  |
| Permission   | 2           | -         | tool permission UX                   |
| Question     | 3           | -         | inline question UX                   |
| Suggestion   | 3           | -         | suggestion UX                        |
| Find         | 1           | -         | file search                          |
| Mcp          | 4           | -         | MCP toggling, browser tool           |
| Pty          | 3           | -         | Agent Manager terminal               |
| Worktree     | 1           | -         | DiffViewer / WorktreeManager         |
| Instance     | 1           | -         | org switch, dispose                  |
| Remote       | 3           | -         | RemoteStatusService                  |
| CommitMessage| 1           | -         | commit-message gen                   |
| Kilo         | 6           | 1 (FIM)   | profile, FIM, cloud sessions         |
| Kilocode     | 6           | -         | skill/agent/sessionImport            |

That is the full surface. **No** route under `/tui/*`, `/experimental/*` (other
than the ones listed below), `/event` (CLI command stream), `/file/*`,
`/lsp/*`, `/formatter/*`, `/network/*`, `/sync/*`, `/vcs/*`, `/log`,
`/agent`, `/skill`, `/telemetry/*` is touched by the extension. They remain
Bun-only.

`/path` is a **single GET** consumed by `kilo-provider/model-state.ts:21`
(`client.path.get()`) to resolve the worktree root displayed in the sidebar.
It is therefore in scope for the Rust port. It used to be listed as
out-of-inventory; that was a mistake and has been corrected.

## Detailed inventory

Legend:
- `M` = HTTP method
- `→ Body` = POST/PATCH body shape (the SDK ships this as JSON)
- `→ Resp` = the SDK response type (sufficient to bound the contract; see
  `types.gen.ts` for full shape)
- `[stream]` marks SSE / chunked responses

### Global

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET | `/global/health` | - | `{ healthy, version }` | (used implicitly by ServerManager readiness; not yet via SDK) |
| GET | `/global/event` | - | `[stream]` GlobalEvent JSON | `services/cli-backend/sdk-sse-adapter.ts:142` |
| GET | `/global/config` | - | `Config` | `provider-actions.ts:238`, `:324`; `KiloProvider.ts:2028` |
| PATCH | `/global/config` | `{ config }` | `Config` | `provider-actions.ts:266,282,329`; `KiloProvider.ts:1974,2271`; `legacy-migration/migration-service.ts:222,275,523,546,574,620,722` |
| POST | `/global/dispose` | - | `void` | `provider-actions.ts:69`; `KiloProvider.ts:2723` |

### Auth

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| PUT | `/auth/{providerID}` | `{ auth }` | `Auth` | `legacy-migration/migration-service.ts:465,494,517`; `provider-actions.ts:162,347` |
| DELETE | `/auth/{providerID}` | - | `void` | `provider-actions.ts:248,350`; `kilo-provider/handlers/auth.ts:83` |

### Config

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET | `/config/warnings` | - | `ConfigWarning[]` | `KiloProvider.ts:2116` |

(Note: `client.config.warnings` is mapped to `/config/warnings` even though it
sits under the same SDK namespace as global config. The SDK's `Config2` class
wraps directory-scoped config; `Config` wraps `/global/config`. Both are used.)

### Path

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET | `/path` | - | `KiloPath` (home/state/config/worktree/directory) | `kilo-provider/model-state.ts:21` |

### Provider

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET | `/provider` | - | `Provider[]` | `provider-actions.ts:36` |

### Session

The `sessionID` placeholder always lands in the path. `directory` lands in the
query (or `x-kilo-directory` header).

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| POST | `/session` | `{ ... }` | `Session` | `KiloProvider.ts:1250,2321`; `agent-manager/AgentManagerProvider.ts:747,940` |
| GET  | `/session` | - | `Session[]` | `KiloProvider.ts:1437` |
| GET  | `/session/status` | - | `Record<id, SessionStatus>` | `session-status.ts:33`; `KiloProvider.ts:1284` |
| GET  | `/session/{sessionID}` | - | `Session` | `KiloProvider.ts:1273`; `SubAgentViewerProvider.ts:57`; `legacy-migration/sessions/migrate.ts:71` |
| PATCH | `/session/{sessionID}` | `{ title? }` | `Session` | `KiloProvider.ts:1541` |
| DELETE | `/session/{sessionID}` | - | `void` | `KiloProvider.ts:1509` |
| GET  | `/session/{sessionID}/message` | - | `Message[]` | `kilo-provider/message-page.ts:34` |
| POST | `/session/{sessionID}/fork` | `{ messageID? }` | `Session` | `agent-manager/fork-session.ts:47`; `agent-manager/continue-in-worktree.ts:84` |
| POST | `/session/{sessionID}/abort` | - | `void` | `kilo-provider/abort.ts:4`; `agent-manager/continue-in-worktree.ts:31` |
| POST | `/session/{sessionID}/summarize` | `{ providerID, modelID }` | `void` | `KiloProvider.ts:2640` |
| POST | `/session/{sessionID}/revert` | `{ messageID }` | `Session` | `KiloProvider.ts:2590` |
| POST | `/session/{sessionID}/unrevert` | - | `Session` | `KiloProvider.ts:2602` |
| POST | `/session/{sessionID}/prompt_async` | `{ messageID?, parts, model?, agent?, variant?, editorContext? }` | `void` (status streamed via global SSE) | `KiloProvider.ts:2462`; `kilo-provider/handlers/cloud-session.ts:214` |
| POST | `/session/{sessionID}/command` | `{ messageID?, parts, command, ... }` | `void` | `kilo-provider/handlers/cloud-session.ts:188` |

### Permission

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET  | `/permission` | - | `PermissionRequest[]` | `commands/toggle-auto-approve.ts:64`; `services/cli-backend/connection-service.ts:394`; `kilo-provider/handlers/permission-handler.ts:130` |
| POST | `/permission/{requestID}/reply` | `{ reply }` | `void` | `commands/toggle-auto-approve.ts:67,85`; `connection-service.ts:398` |

### Question

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET  | `/question` | - | `QuestionRequest[]` | `kilo-provider/handlers/question.ts:32`; `connection-service.ts:402` |
| POST | `/question/{requestID}/reply` | `{ value }` | `void` | `kilo-provider/handlers/question.ts:70` |
| POST | `/question/{requestID}/reject` | - | `void` | `kilo-provider/handlers/question.ts:96`; `connection-service.ts:406` |

### Suggestion

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET  | `/suggestion` | - | `Suggestion[]` | `kilo-provider/handlers/suggestion.ts:98`; `connection-service.ts:641` |
| POST | `/suggestion/{requestID}/accept` | - | `void` | `kilo-provider/handlers/suggestion.ts:60` |
| POST | `/suggestion/{requestID}/dismiss` | - | `void` | `kilo-provider/handlers/suggestion.ts:81`; `connection-service.ts:645` |

### Find

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET  | `/find/file` | - | `FoundFile[]` | `kilo-provider/file-search.ts:36,37` |

(The SDK exposes `client.find.files`, which routes to `/find/file`. Other
`/find/*` endpoints exist but are unused by the extension.)

### Mcp

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| POST | `/mcp` | `{ name, config }` | `McpStatus` | `services/browser-automation/browser-automation-service.ts:97` |
| POST | `/mcp/{name}/connect` | - | `void` | `KiloProvider.ts:1899` |
| POST | `/mcp/{name}/disconnect` | - | `void` | `KiloProvider.ts:1911`; `services/browser-automation/browser-automation-service.ts:141` |
| GET  | `/mcp` | - | `McpStatus[]` (per `client.mcp.status`) | `services/browser-automation/browser-automation-service.ts:164` |

### Pty

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| POST | `/pty` | `{ ... }` | `Pty` | `agent-manager/terminal-manager.ts:77` |
| PUT | `/pty/{ptyID}` | `{ title?, size? }` | `Pty` | `agent-manager/terminal-manager.ts:105` (SDK `Pty.update`, see `packages/sdk/js/src/v2/gen/sdk.gen.ts:1322`) |
| DELETE | `/pty/{ptyID}` | - | `void` | `agent-manager/terminal-manager.ts:127,191` |

### Worktree

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET  | `/experimental/worktree/diff` | - | `WorktreeDiff` | `DiffViewerProvider.ts:171,196` |
| GET  | `/experimental/worktree/diff/file` | - | `WorktreeFileDiff` | `worktree-diff-client.ts:35` |

### Instance

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| POST | `/instance/dispose` | - | `void` | `KiloProvider.ts:1980` |

### Remote

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET  | `/remote/status` | - | `RemoteStatus` | `services/RemoteStatusService.ts:52,63` |
| POST | `/remote/enable` | - | `void` | `services/RemoteStatusService.ts:72` |
| POST | `/remote/disable` | - | `void` | `services/RemoteStatusService.ts:74` |

### CommitMessage

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| POST | `/commit-message` | `{ diff, ... }` | `{ message }` | `services/commit-message/index.ts:88` |

### Kilo (Kilo Cloud / FIM)

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| GET  | `/kilo/profile` | - | `Profile` | `services/autocomplete/AutocompleteModel.ts:124`; `kilo-provider/handlers/auth.ts:65,113,125,147` |
| POST | `/kilo/organization` | `{ organizationId }` | `void` | `kilo-provider/handlers/auth.ts:108` |
| POST | `/kilo/fim` | `{ ... }` | `[stream]` | `services/autocomplete/AutocompleteModel.ts:57` |
| GET  | `/kilo/cloud-sessions` | - | `CloudSession[]` | `kilo-provider/handlers/cloud-session.ts:41` |
| GET  | `/kilo/cloud/session/{id}` | - | `CloudSession` | `kilo-provider/handlers/cloud-session.ts:76` |
| POST | `/kilo/cloud/session/import` | `{ id, ... }` | `Session` | `kilo-provider/handlers/cloud-session.ts:138` |

### Kilocode (skill / agent / sessionImport)

| M | Path | Body | Resp | Call sites |
|---|------|------|------|-------------|
| POST | `/kilocode/skill/remove` | `{ location }` | `void` | `KiloProvider.ts:1767` |
| POST | `/kilocode/agent/remove` | `{ name }` | `void` | `KiloProvider.ts:1799` |
| POST | `/kilocode/session-import/project` | `{ ... }` | `Project` | `legacy-migration/sessions/migrate.ts:78` |
| POST | `/kilocode/session-import/session` | `{ ... }` | `Session` | `legacy-migration/sessions/migrate.ts:80` |
| POST | `/kilocode/session-import/message` | `{ ... }` | `void` | `legacy-migration/sessions/migrate.ts:101` |
| POST | `/kilocode/session-import/part` | `{ ... }` | `void` | `legacy-migration/sessions/migrate.ts:105` |

The legacy-migration paths run only during one-off Bun-side migration; they are
not part of the steady-state extension flow but must remain functional in M0
because users with old sessions still hit them once.

## Streaming routes

Two streaming surfaces matter for M0:

1. **`GET /global/event`** — single global SSE channel. Carries every event
   the extension cares about: `server.connected`, `server.heartbeat`, every
   `session.*`, `message.*`, `part.*`, `permission.*`, `question.*`,
   `suggestion.*`, `mcp.*`, and `provider.*`. Disconnect/reconnect logic in
   [`SdkSSEAdapter`](../../kilo-vscode/src/services/cli-backend/sdk-sse-adapter.ts).
   The contract is: heartbeat every ~10 s, reconnect on 15 s of silence.

2. **`POST /kilo/fim`** — autocomplete FIM stream. Used only by
   `AutocompleteModel`. Out of scope for the M0 oracle (autocomplete is
   not part of the M0 fixture set).

## Required headers / auth

Every authenticated route requires:
- `Authorization: Basic base64("kilo:<KILO_SERVER_PASSWORD>")` — when the
  password env-var is set.
- `x-kilo-directory: <encoded-workspace-path>` — for any directory-scoped
  route (effectively all of session/, permission/, question/, etc.).
  `client.ts` rewrites this into a `directory` query parameter on GET/HEAD
  requests; on POST/PATCH/DELETE the SDK sends the body's `directory` field
  instead.

## Out of inventory

The extension never invokes:
- Anything under `/tui/*`.
- Most of `/experimental/*` (only `/experimental/worktree/diff*` is used).
- `/event` (CLI command bus — different stream from `/global/event`).
- `/file`, `/file/content`, `/file/status`.
- `/lsp`, `/formatter`, `/network/*`.
- `/sync/*`, `/vcs`, `/vcs/diff`.
- `/log`, `/agent`, `/skill` (top-level — used by the CLI, not the extension).
- `/telemetry/capture`.
- `/kilo/notifications`, `/kilo/modes`, `/kilo/claw/*`.
- `/kilocode/heap/snapshot` (dev-only command).

> Note: as of M5 (post-fix), the Rust server in `crates/kilo-server/src/lib.rs`
> registers the full M5 inventory plus stubs for `/agent`, `/skill`, and
> `/command` (static stub responses — empty array / hard-coded agent list)
> so the SDK doesn't 404 on a future webview iteration. The auth write
> routes (`PUT`/`DELETE /auth/{providerID}`) are also registered as
> echo/no-op stubs — Bun-compatible persistence lands in M10. None of the
> stubs are part of the contract; they exist solely to keep the M3
> mutation gate from locking fallback on a 4xx error.

These can stay Bun-only forever from the VS Code extension's perspective. They
are listed here so future contributors don't accidentally promote them into
the contract.
