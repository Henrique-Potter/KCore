# Migration gaps — Bun → Rust sidecar (working punch list)

Living document. Snapshot date: 2026-05-09. Generated from two parallel deep-inspection sweeps and then reconciled after the Wave 1/2 Rust fixes. Round 1: 8 domains (routes, tools, providers, storage/auth, MCP/permissions/bus, kilocode features, CLI/infra/PTY, agent turn loop). Round 2: 5 domains (VS Code extension client, oracle/test coverage, SDK wire-shape, webview SSE consumers, subagent task tool deep dive). Round-2 additions are appended at the end.

## Final status (post-wave-11)

The lean OpenAI-Pro VS Code Rust sidecar is functionally feature-complete with parity to Bun across every documented user-visible flow. 11 parallel coder waves closed all P0 visible UX bugs and ~45 fix groups. Test count: 517 (was 229 at the start of this push).

Remaining items are either testing infrastructure (PTY browser smoke) or explicitly out of scope (LSP, indexing, sync, experimental workspaces). See "## 2026-05-10 reconciliation" below for the wave-by-wave closure log.

## 2026-05-09 reconciliation

The older tables below intentionally preserve the original investigation notes, but several rows are now closed or narrowed. Treat this section as the current punch list until the historical rows are fully split into "done" and "open" sections.

Closed since the original sweep:

- Edit replacer chain, BOM-aware file I/O, CRLF preservation, and prompt text loading for migrated built-in tools.
- Bash permission prefix patterns, `saveAlwaysRules` drain, reject cascade, config-path protection, `/permission/allow-everything`, named-error registry expansion.
- POSIX `auth.json` mode hardening, SQLite PRAGMA hardening, wellknown auth blob preservation.
- PTY Windows shell priority, environment credential scrub, tree-kill, SDK `Pty` response shape, native `Agent` response shape.
- `global.disposed`, `server.instance.disposed`, `/event` dispose termination, `mcp.tools.changed`, `session.compaction.compacted`.
- `kilocode.removeSkill` / `removeAgent`, `enhance-prompt`, `kilo.cloud.session.get`, `glob`, `webfetch`, `todowrite`, `skill`, `suggest`, `lsp`, `plan_exit`, and first-turn title generation.

Still open after reconciliation:

| Area | Current status |
|---|---|
| PTY terminal transport | `/pty/{id}/connect` WebSocket, bidirectional input/output, 2 MiB replay buffer, cursor control frame, REST fallback, and `pty.created`/`updated`/`exited`/`deleted` events are implemented. Remaining risk: no full Agent Manager browser smoke in this pass. |
| Snapshot/revert | Implemented for new Rust turns: pre-turn shadow-git snapshots are captured, OpenAI turns emit `patch` parts, `/session/{id}/revert` now stages once under one snapshot lock, stores a capped redo diff/summary, and `/unrevert` restores redo state. Still missing full Bun `diffFull` event wiring and complete historical cleanup parity. |
| Plan mode | `plan_exit` exists, hard-stops the OpenAI loop, and now aborts sibling tools for the same streamed iteration instead of waiting behind a long-running parallel call. Plan-agent prompts inject the `.kilo/plans/<created>-<slug>.md` write contract. Plan follow-up/approval handoff and cleanup lifecycle remain partial. |
| Retry layer | Partial: OpenAI stream pre-response failures now carry retry metadata, honor `Retry-After` / `retry-after-ms`, publish `session.status` retry frames, retry only when no text/reasoning/tool side effects have streamed, and offline-style transport failures register `/network` waits that can be replied/rejected. Still missing automatic `session.network.restored` probing and mid-stream safe replay. |
| Reasoning summaries | Stream parsing and persisted `reasoning` parts exist; encrypted reasoning/details are not round-tripped into later Responses requests. |
| `apply_patch` parity | Basic patching exists; move/rename, EOF anchors, and full Bun parser parity remain missing. |
| Cost propagation | Closed for task subagents: usage tokens, assistant-level model cost, OpenAI stream `step-finish.cost`, and parent/subagent cost propagation now persist when model pricing is available. Resumed task sessions reconcile by durable per-task delta so child cost is not double-counted. |
| Subagents | Child sessions now inherit MCP-specific deny rules, resolve agent model defaults, thread variants, disable recursive task tools, and propagate child assistant costs into the parent wrapper; richer tool-disable inheritance remains open. |
| Missing tools | No high-impact built-in tool remains fully absent. `lsp` is partial/heuristic until a real LSP client exists; indexing/search-dependent tools remain out of narrow scope. |
| Multimodal input | Partial: persisted data-URL image/PDF/file attachments now survive into OpenAI Responses content items. Rust still lacks Bun's earlier file/resource resolver for `file:` URLs, directories, and MCP resources. |
| Indexing | `/indexing/status` compatibility stub only. |
| Extra routes | Extension steady-state routes are covered; broad Bun routes such as `/session/{id}/init`, `/vcs`, `/lsp`, `/sync/*`, and most `/experimental/*` remain absent or explicitly out of scope. |
| Misc lifecycle | Title generation is implemented for first root turns with default titles. `prompt_async` now queues same-session follow-ups, installs abort handles without a spin loop, cleans queue state on session delete, and abort drops queued work. Mid-loop follow-up break is wired: a same-session `prompt_async` arriving while a runner is active sets the runner's `follow_up_break` flag and trips `cancel`, finalizing the in-flight turn at the next cancel-observation point (existing `is_canceled` checks in `prompt_turn`, `wait_fake`, and the OpenAI stream loop) without rejecting pending permissions or bumping the queue version, so the queued follow-up turn picks up against the persisted partial assistant message. Bun's `prompt-queue.scope()` retargeting (re-anchoring a queued user message onto the active turn's parent message id) is not yet implemented — see follow-up notes. File watcher and real LSP client remain missing. OpenAI OAuth auth blobs are refreshed before every provider stream request. Editor-context injection now reaches the model context for OpenAI turns. |

## 2026-05-10 reconciliation

Additional rows closed since the 2026-05-09 snapshot:

- Snapshot wiring into routes/sessions.rs — `revert_session` populates summary via `diff_full` + `summary_from_diff_full`; `delete_session` triggers `on_session_deleted` via `spawn_blocking`.
- Subagent richer tool-disable inheritance — `task_child_tools` threads disable map (task / todowrite / primary_tools).
- Reasoning encrypted metadata round-trip — within-turn (`StreamEvent::ReasoningItem` + `responses_input` emission) and cross-turn (`real_messages` reads persisted `itemID` / `encryptedContent`).
- `prompt_async` follow-up break — `Runner.follow_up_break` flag; `prompt_async` trips it without dropping queue or rejecting permissions.
- Plan-mode follow-up question handoff — `plan_followup_decision` detects `plan_exit`, raises question, on yes synthesizes implementation prompt with code agent.
- Distinct `finish:"follow_up"` terminal state — `finalize_openai_aborted` skips `publish_error` when `follow_up_break` is set.
- Queue scope retargeting — `broken_turn_anchors` session map, take/set semantics so queued follow-up's `parentID` inherits the broken turn's parent.
- Plan markdown cleanup on session delete — fire-and-forget removal of `<worktree>/.kilo/plans/<created>-<slug>.md`.
- `/lsp` + `/formatter` SDK stub routes — return `[]` for SDK shape parity.
- `Truncate.Service` for tool outputs — 50KB / 2000-line caps, write to `<state_dir>/kilo/truncate/`, preview + `outputPath` metadata.
- `assertExternalDirectoryEffect` permission gate — `resolve_with_external` + `ask_external_directory` + gated wrappers (`fake_*_gated`) + wired into `parts.rs::real_tool_part`.
- `summarize_session` boolean body shape — accepts `{providerID, modelID, auto}`, returns boolean; `auto=true` fires follow-up turn.
- `event` table flag-gating — `KILO_EXPERIMENTAL_WORKSPACES` env flag gates `write_event` SQLite inserts (SSE publish unaffected).
- Plan orphan cleanup helper — `cleanup_orphaned_plans(store, max_age_days)` with TTL sweep.
- Network-offline detection — `has_network_wait_for_session` + `add` / `take` / `clear_network_wait` + `session.network.restored` emission on resume.
- Network-wait timeout + DNS probe — 60s deadline + periodic `lookup_host("dns.google:443")` that auto-resolves on success.
- Periodic GC scheduler — hourly tokio interval running `cleanup_old_snapshots(30)` + `cleanup_orphaned_plans(30)`.
- Mid-stream retry safe replay — `Runner.mid_stream_retries` cap 3; partial assistant content replayed via synthetic `ChatMessage` in next request `input[]`; resets on success.

Still open after reconciliation (final state):

| Area | Current status |
|---|---|
| PTY Agent Manager browser smoke | Testing infrastructure only — Rust transport implemented; no end-to-end browser smoke harness yet. |
| `/sync/*`, most `/experimental/*` routes | Explicitly out of scope for the OpenAI-Pro narrow target. |
| Real LSP client / file watcher / `@parcel/watcher` | Explicitly out of scope. |
| Indexing pipeline / Lance DB | Explicitly out of scope. |

Test count growth: from 229 baseline → 517 across 11 waves.

Conventions:
- **Severity**: H = visible UX or data-loss bug; M = degraded behavior or compat-only stub; L = cosmetic, deferred, or out-of-scope shim.
- **Status**: `missing` (no Rust code), `partial` (some Rust code, drift), `stub` (route exists returning placeholder), `oos` (intentionally out of scope per CONTRACT.md / scope decision).
- File refs are absolute.

---

## P0 — User-visible bugs blocking even narrow OpenAI-Pro VS Code use

> **Status reconciled through wave 12.** Rows showing `closed` are landed; see the reconciliation logs above for landing details.

| Area | Gap | Bun ref | Rust ref | Status | Sev |
|---|---|---|---|---|---|
| edit tool | closed: Wave 2 F+H — 9-replacer fallback chain (`agent/tools/replacers.rs`). | `packages/opencode/src/tool/edit.ts:267-579, 701-738` | `crates/kilo-server/src/agent/tools/replacers.rs` | closed | H |
| edit/write/apply_patch diff format | closed: Wave 4 O — unified-diff via `agent/tools/diff.rs::unified_diff`. | `packages/opencode/src/tool/edit.ts:126,168-176` | `crates/kilo-server/src/agent/tools/diff.rs::unified_diff` | closed | H |
| reasoning summary parts | closed: Wave 5 U + Wave 6 X — encrypted reasoning round-trip within turn and across turns. | `packages/opencode/src/provider/sdk/copilot/responses/openai-responses-language-model.ts:1543-1658` | `crates/kilo-provider/src/lib.rs`, `crates/kilo-server/src/agent/openai_stream.rs`, `crates/kilo-server/src/agent/parts.rs:real_messages` | closed | H |
| bash permission patterns | closed: Wave 2 K — first-word heuristic for prefix matching. | `packages/opencode/src/permission/arity.ts:1-9` (BashArity.prefix) | `crates/kilo-server/src/agent/tools/common.rs` | closed | H |
| `saveAlwaysRules` doesn't unblock pending request | closed: Wave 1 B — drain pass on save. | `packages/opencode/src/server/routes/instance/permission.ts:60-110` | `crates/kilo-server/src/routes/permissions.rs` | closed | H |
| Reject cascade missing | closed: Wave 1 B — sibling pending requests rejected together. | `packages/opencode/src/permission/index.ts:291-301` | `crates/kilo-server/src/routes/permissions.rs` | closed | H |
| Config-path protection absent | closed: Wave 1 B — `.kilo/`, `.kilocode/`, `kilo.json` edits flagged and `always` downgraded to `once`. | `packages/opencode/src/kilocode/permission/config-paths.ts:120-167` | `crates/kilo-server/src/agent/permissions` | closed | H |
| Hard ruleset producer | implemented | `packages/opencode/src/kilocode/session/prompt.ts:60-72` | `state.set_session_agent` now caches ask/plan hard rules at turn start and `evaluate_permission_layered` enforces them | closed |
| `auth.json` chmod | closed: Wave 1 C — POSIX 0600 mode set on auth file. | `packages/opencode/src/auth/index.ts:81,90` | `crates/kilo-store/src/lib.rs:1453-1482` | closed | H |
| MCP tools-changed event | closed: Wave 1 A — `mcp.tools.changed` SSE publish on `tools/list_changed`. | `packages/opencode/src/mcp/index.ts:72-77,510` | `crates/kilo-server/src/routes/mcp.rs` | closed | M |
| `/permission/allow-everything` | closed: Wave 1 B — route registered. | `packages/opencode/src/kilocode/permission/routes.ts:14-86` | `crates/kilo-server/src/routes/permissions.rs` | closed | H |
| API-key OpenAI path uses `/chat/completions` | Bun routes ALL OpenAI through `/responses`. Rust falls back to `/chat/completions` for ChatAuth::Api with the wrong tools envelope (`function:{name,...}` vs flat). Reasoning models would fail. | `packages/opencode/src/provider/provider.ts:190-205` | `crates/kilo-provider/src/lib.rs:557-569,849-863` | partial | M (only matters if api-key flow is exercised) |
| Image / multimodal input dropped | mostly closed: Wave 4 P — data-URL attachments + `file://` + directories + `@`-mentions wired through to Responses content. Note: MCP resource resolver still open. | `packages/opencode/src/provider/transform.ts:297-333` | `crates/kilo-provider/src/lib.rs:responses_input`, `crates/kilo-server/src/agent/parts.rs:real_messages` | partial | H |
| Retry layer partial | mostly closed: pre-stream + mid-stream safe replay + offline detection (Wave 5/wave 11). Note: automatic `session.network.restored` probe still partial. | `packages/opencode/src/session/retry.ts:23-160` | `crates/kilo-server/src/agent/retry.rs`, `crates/kilo-server/src/agent/openai_stream.rs`, `crates/kilo-provider/src/lib.rs` | partial | H |
| Compaction is reactive-only | partial: proactive trigger landing in wave 12 (Wave 12 TT in flight). | `packages/opencode/src/session/prompt.ts:1493-1516,1654-1675` | `crates/kilo-server/src/agent/openai_stream.rs:436-502` | partial | M |
| Compaction quality | partial: structured Goal/Constraints/Progress template landing in wave 12 (Wave 12 UU in flight). | `packages/opencode/src/session/compaction.ts:40-75,121-131` | `crates/kilo-server/src/agent/compaction.rs` | partial | M |
| `task` subagent cost propagation | Closed for task subagents: Rust sums child assistant costs and serializes parent assistant cost updates so parallel task completions do not lose child spend. | `packages/opencode/src/kilocode/session/cost-propagation.ts:7-69` | `crates/kilo-store/src/lib.rs:add_message_cost_record`, `crates/kilo-server/src/agent/parts.rs:execute_task_tool` | closed | M |
| `task` subagent permission inheritance | closed: Wave 5 T — agent.permission + guardPermissions + hard ruleset + MCP-server-specific denies merged. | `packages/opencode/src/tool/task.ts:71-72,108` | `crates/kilo-server/src/agent/parts.rs` | closed | M |
| PTY shell selection on Windows | closed: Wave 2 J — pwsh > powershell > git-bash > COMSPEC priority + `KILO_GIT_BASH_PATH` honored. | `packages/opencode/src/shell/shell.ts:55-91` | `crates/kilo-server/src/routes/pty.rs` | closed | H |
| PTY env credential leak | closed: Wave 2 J — strips `KILO_SERVER_PASSWORD`/`USERNAME`, sets `TERM`, `KILO_TERMINAL=1`, UTF-8 on Windows. | `packages/opencode/src/pty/index.ts:185-208` | `crates/kilo-server/src/routes/pty.rs` | closed | H |
| PTY tree-kill | closed: Wave 2 J — `taskkill /T /F` on Windows. | `packages/opencode/src/shell/shell.ts:15-44` | `crates/kilo-server/src/routes/pty.rs` | closed | M |
| PTY replay buffer / cursor | Closed: Rust keeps a 2 MiB replay buffer, honors `cursor=-1`, replays from numeric cursors, and sends Bun-shaped binary cursor metadata frames. | `packages/opencode/src/pty/index.ts:38-44,222-263,308-367` | `crates/kilo-server/src/routes/pty.rs` | closed | H |
| PTY WebSocket bidirectional | Closed: Rust registers `/pty/{id}/connect` as a WebSocket endpoint; client text/binary frames write to the PTY and live PTY output is broadcast to connected sockets. REST/SSE remains as a compatibility fallback. | `packages/opencode/src/server/routes/instance/pty.ts:13,168` | `crates/kilo-server/src/routes/pty.rs`, `crates/kilo-server/src/http/mod.rs` | closed | H |
| `bash` tool prompt loading | closed: Wave 2 M — multi-paragraph `.txt` descriptions loaded for migrated built-in tools. | `packages/opencode/src/tool/bash.ts:6,611-616` (and every other tool .txt) | `crates/kilo-server/src/agent/tools/defs.rs` | closed | H |
| Encoding-aware file I/O | closed: Wave 2 F+H — `agent/tools/encoding.rs` (UTF-16 BOM, Shift-JIS, Windows-1252 round-trip). | `packages/opencode/src/kilocode/encoding.ts:27-142`, `kilocode/tool/encoded-io.ts:11-17` | `crates/kilo-server/src/agent/tools/encoding.rs` | closed | H |
| `apply_patch` parser | closed: Wave 4 N — move/rename, multi-chunk update, EOF anchors, BOM preservation. | `packages/opencode/src/patch/index.ts` (full file) | `crates/kilo-server/src/agent/tools/patch.rs` | closed | M |
| `bash` parsing for permission scope | partial: Wave 2 K heuristic in place; tree-sitter version not landed. | `packages/opencode/src/tool/bash.ts:101-231,384-424` | `crates/kilo-server/src/agent/tools/bash.rs` | partial | M |
| `assertExternalDirectoryEffect` | closed: Wave 8 DD + Wave 9 GG — gated wrappers (`fake_*_gated`) + `parts.rs::real_tool_part` wiring. | `packages/opencode/src/tool/external-directory.ts:25-56` | `crates/kilo-server/src/agent/permissions`, `crates/kilo-server/src/agent/parts.rs` | closed | H |
| `Truncate.Service` for tool outputs | closed: Wave 8 CC — 50KB / 2000-line caps, `<state_dir>/kilo/truncate/`, preview + `outputPath` metadata. | `packages/opencode/src/tool/tool.ts:91-130` | `crates/kilo-server/src/agent/tools` (Truncate service) | closed | H |
| `summarize_session` body shape | closed: Wave 8 EE (+ Wave 10 PP for auto) — accepts `{providerID, modelID, auto}` and returns boolean. | `packages/opencode/src/server/routes/instance/session.ts:532` | `crates/kilo-server/src/routes/sessions.rs` | closed | M |
| `bad_request_named` bypasses error registry | partial: canonical helper exists (Wave 12 VV), migration in flight (Wave 12 WW). | n/a | `crates/kilo-server/src/error.rs`, `routes/config.rs`, `agent/compaction.rs` | partial | M |
| `event` table writes are unconditional | closed: Wave 8 FF — `KILO_EXPERIMENTAL_WORKSPACES` env flag gates SQLite inserts. | `packages/opencode/src/sync/index.ts:138-158` | `crates/kilo-store/src/lib.rs` | closed | M |

## P1 — Major missing routes / features (lean target may still need)

### Missing routes (extension-callable)

| method | path | purpose | bun_ref |
|---|---|---|---|
| POST | `/session/{id}/init` | Run `INIT` command (creates `AGENTS.md`) | `packages/opencode/src/server/routes/instance/session.ts:320` |
| POST | `/session/{id}/command`, `/shell` | Send command/shell to session (cloud-session uses these) | `…/session.ts:930,968` |
| GET | `/vcs`, `/vcs/diff` | Branch + working-tree diff | `…/instance/index.ts:130,156` |
| GET | `/lsp`, `/formatter` | LSP / formatter status arrays | `…/instance/index.ts:254,277` |
| GET | `/project`, POST `/project/git/init`, PATCH `/project/{id}` | Project management | `…/project.ts:16,59,94` |
| GET | `/experimental/console`, `/console/orgs`, POST `/console/switch` | Console org metadata + switch | `…/experimental.ts:49,79,116` |
| GET | `/experimental/tool`, `/experimental/tool/ids` | Live tool list with schemas | `…/experimental.ts:142,167` |
| GET | `/experimental/session` | Cross-project global session listing | `…/experimental.ts:448` |
| GET | `/experimental/resource` | List MCP resources from connected servers | `…/experimental.ts:530` |
| `*` | `/experimental/workspace/*` | Workspace adaptors / create / list / status / delete / session-restore | `…/control/workspace.ts:18,39,72,93,115,144` |
| POST | `/global/upgrade` | Self-upgrade (probably out of scope — VSIX bundles binary) | `…/global.ts:227` |
| GET | `/doc`, POST `/log` | OpenAPI 3.1 spec / log forwarding | `…/control/index.ts:89,111` |
| POST | `/permission/allow-everything` | Webview "Allow everything" toggle | `…/kilocode/permission/routes.ts:18` |
| POST | `/enhance-prompt` | Webview "Improve my prompt" button | `…/instance/enhance-prompt.ts:10` |
| POST | `/telemetry/capture` | Forward telemetry to PostHog | `…/instance/telemetry.ts:10` |
| GET | `/indexing/status` and `/indexing/*` | Code indexing status panel | `packages/kilo-indexing/src/server/routes.ts:6` |
| POST | `/sync/start`, `/sync/replay`, `/sync/history` | Workspace sync (Rust writes events but no replay) | `…/instance/sync.ts:25,47,102` |
| MCP | POST `/mcp/{name}/auth`, `/auth/callback`, `/auth/authenticate`, DELETE `/mcp/{name}/auth` | Bun's MCP OAuth shape (Rust uses different verbs/paths) | `…/instance/mcp.ts:69,112,145,184` |

### Missing tool implementations (out-of-scope search/indexing tools)

| tool | what it does | bun_ref | priority |
|---|---|---|---|
| `kilo_local_recall`, `codebase_search` (warpgrep), `semantic_search`, `websearch`, `codesearch` | Niche / Exa / indexing-dependent | various | low (most oos) |

### Tool plumbing missing

- Tool prompt-text (`.txt` siblings) is loaded for the migrated built-ins (`read`, `glob`, `grep`, `write`, `edit`, `apply_patch`, `bash`, `question`, `task`, `webfetch`, `todowrite`, `skill`, `suggest`, `lsp`). New tools still need their Bun prompt copied when implemented.
- `Tool.Def` registry trait — current Rust splits across `KNOWN_TOOLS` + `defs.rs` + dispatch arms in `parts.rs` + `common.rs::tool_permission`+`tool_patterns`. Adding a tool needs multi-site edits. Centralize.
- `formatValidationError` per tool — Bun lets each tool override schema-validation error wording.
- Repair: `repair_tool_name` only consults `KNOWN_TOOLS`, not MCP catalog.
- `edit` vs `apply_patch` selection should be model-id-gated (gpt-* → patch, else → edit). Rust always exposes both.

## P2 — Persistence and identity

| Artifact | Status | Bun ref | Rust ref | Sev |
|---|---|---|---|---|
| `workspace` table | missing | `packages/opencode/src/control-plane/workspace.sql.ts:6-18` | sessions reference `workspace_id` but no producer | M |
| `account`, `account_state`, `control_account` | missing | `packages/opencode/src/account/account.sql.ts:6-39` | absent | H (cloud-session UI) |
| `session_share` (`{id, secret, url}`) | partial | `packages/opencode/src/share/share.sql.ts:5-13` | only `share_url` text on `session` (`kilo-store/src/lib.rs:1099-1126`) — share-upload protocol cannot run | H |
| `todo` table | implemented | `packages/opencode/src/session/session.sql.ts:79-96` | `crates/kilo-store/src/migrations.rs`, `crates/kilo-store/src/lib.rs` | M |
| `permission` (project-scoped ruleset) | partial | `packages/opencode/src/session/session.sql.ts:117-123` | global JSON array `permissions.json` (cross-project pollution) | M |
| Snapshot git dirs `data/kilo/snapshot/<project>/<hash>.git` | partial | `packages/opencode/src/snapshot/index.ts:97-104,316-326` | `crates/kilo-server/src/snapshot.rs` now has shadow-git `track`, `patch`, `restore`, and unified `diff`; `diffFull` remains open | M |
| `summary.diffs` produce path | implemented | column exists; Rust now writes summary diffs for snapshot-backed revert | `/session/{id}/diff` returns stored summary diffs | closed |
| Plans markdown sidecar `<worktree>/.kilo/plans/...md` | partial | `packages/opencode/src/session/session.ts:301-306` | plan-agent prompts now tell the model to write only `.kilo/plans/<created>-<slug>.md`; full follow-up/approval flow remains open | M |
| Project-id cache `<git_common_dir>/kilo` | missing | `packages/opencode/src/project/project.ts:170-176,236-239` | absent — every cold start re-runs `git rev-list --max-parents=0` | L |
| Storage migration `data/kilo/storage/migration` | missing | `packages/opencode/src/storage/storage.ts:88-246` | Rust never reads existing JSON layouts left by Bun → silent data loss on Bun→Rust handoff | H |
| `session.summary` populated by Rust | missing | Bun fills `additions/deletions/files/diffs` | Rust always writes nulls | M |
| `session.revert.snapshot/diff` | partial | Bun embeds commit hash + patch | Rust stores redo `snapshot` and unified `diff` when the target message has a captured snapshot | M |
| `auth.json` mode 0600 | missing | `packages/opencode/src/auth/index.ts:81,90` | `crates/kilo-store/src/lib.rs:1453-1482` | H (security) |
| MCP tokens file split | drift | Bun → `auth.json` (key `mcp/<name>`) | Rust → separate `mcp-auth.json` | M |
| sqlite PRAGMAs | implemented | `synchronous=NORMAL`, `cache_size=-64000`, `wal_checkpoint(PASSIVE)` set in Bun | Rust now stamps the Bun-parity PRAGMAs on writer connection setup | closed |
| Auto-share on session create | missing | `packages/opencode/src/share/session.ts:40-47` honors `KILO_AUTO_SHARE` / `share=auto` | absent | M |
| Session delete cascade | partial | Bun recursively deletes children + cancels active runner + cloud unregister | Rust does FK cascade only — leaves children with dangling `parent_id`, doesn't cancel runner | M |

## P3 — Agent loop part-type and event coverage

Part types missing on Rust (`MessageV2.Part` union at `packages/opencode/src/session/message-v2.ts:413-456`):

- `reasoning` — entirely missing.
- `tool` pending state — Rust starts at `running`.
- `snapshot`, `patch` — missing entirely.
- `retry` — missing entirely (no SessionRetry).
- `agent` — `@<agent>` user-mention parts not synthesized.
- `subtask` — round-trips as opaque JSON; Bun's typed shape lost.
- `compaction` — anchored part on user message missing; only `assistant.info.summary=true` is set.
- `file` — user-side `@<path>` mentions not converted to file parts; tool-side `attachments` (image/PDF) not produced.

Bus events missing on Rust (extension subscribers will see stale UI):

- `server.instance.disposed`, `global.disposed` — never fires.
- `global.config.updated` — published on auth/permission changes by Bun; absent in Rust.
- `session.diff` — revert/summary diff updates not emitted.
- `session.network.asked`/`replied`/`rejected`/`restored` — `routes/network.rs:16-26` returns `[]` and 404, never publishes.
- `mcp.tools.changed` — see P0.
- `pty.created`/`updated`/`deleted` — Rust now publishes `pty.exited`; create/update/delete event coverage and replay semantics remain partial.
- `file.edited`, `file.watcher.updated` — no file watcher in Rust.
- `lsp.client.diagnostics`, `lsp.updated` — no LSP client.
- `project.updated`, `project.vcs.branch.updated` — absent.
- `worktree.ready`/`failed`, `workspace.ready`/`failed`/`restore`/`status` — control-plane events not bridged.
- `installation.updated`/`updateAvailable` — sidecar version updates not surfaced.
- `command.executed` — absent.
- `session.compaction.compacted` — `compact_session` runs but no event publish.
- `session.todo.updated` — absent (no todo store).
- `kilo.indexing.event` — absent (no indexing).

## P4 — Kilocode-specific surfaces

Highest-impact untouched in Rust:

| Feature | Bun ref | Status | Sev |
|---|---|---|---|
| Indexing / semantic search (`@kilocode/kilo-indexing`, lancedb) | `kilocode/indexing.ts:138-361`, `lancedb.ts:25-39` | missing | H |
| Plan-mode follow-up + plan permission split | `kilocode/plan-followup.ts:30-`, `kilocode/session/prompt.ts:guardPermissions/hardPermissions` | missing | H |
| Commit-message generation | `kilocode/commit-message/generate.ts:118-204` | stub returns `"Update N selected file(s)"` literal (`routes/compat.rs:36-48`) | H |
| Worktree cleanup retry loop (Win EBUSY 60×500ms) | `kilocode/worktree-cleanup.ts:33-69` | partial — one shot in `routes/worktree.rs:247-288` | H (Win) |
| Suggest tool / suggestion-driven action chips | `kilocode/suggestion/tool.ts`, `tool/registry.ts:14-100` | implemented: tool side now publishes/waits through existing routes | M |
| Built-in `kilo-config` skill + walk-up skill discovery | `kilocode/skills/builtin.ts:14-21`, `kilocode/paths.ts:skillDirectories` | missing | M |
| Title generation on first turn | `packages/opencode/src/session/prompt.ts:172-232` | implemented for root sessions with default titles and exactly one real user message; runs as a cancellable background OpenAI Responses call and publishes `session.updated` on success. | closed |
| Editor-context env_details | `kilocode/editor-context.ts:13-57`, `kilocode/session/prompt.ts:injectEditorContext` | closed for OpenAI turns: Rust injects the dynamic `<environment_details>` block into the latest user message and adds the static shell line to the system env block. | closed |
| Snapshot diff-full (`git diff --unified=INT_MAX`) | `kilocode/snapshot/diff-full.ts:43-` | missing | M |
| `/local-review` & `/local-review-uncommitted` slash commands | `kilocode/review/{review,command}.ts` | missing | M |
| `KiloSessionPromptQueue` (per-session prompt queue with version cancellation) | `kilocode/session/prompt-queue.ts` | partial — Rust `prompt_async` now serializes same-session follow-ups, dismisses active question/suggestion waits before enqueue, and abort cancels queued slots instead of returning `BusyError`; still missing immediate user-message persistence, `scope()` retargeting, and mid-loop `hasFollowup` break. | M |
| Insert system reminders on older user messages on multi-step turns | `packages/opencode/src/session/prompt.ts:234-339,1578-1594` | missing | M |
| Doom-loop check timing | Bun checks INSIDE stream (catches 4th identical call before tools run); Rust checks AFTER iteration drain (all N parallel duplicates run first) | `packages/opencode/src/session/processor.ts:357-381` | partial | L |

Confirmed out-of-scope (stays on Bun, per CONTRACT.md / OpenAI Pro narrowing):
ACP, Kilo cloud-session websocket relay (`kilo-sessions/`), KiloClaw, multi-provider (Anthropic/Bedrock/Gemini/Vertex/Azure/Copilot/OpenRouter/xAI/Mistral/etc.), Kilo Gateway routes (`/kilo/profile`, `/kilo/organization`, `/kilo/fim`, `/kilo/cloud-sessions`), TUI surfaces, `web` / `pr` / `agent create` / `models` / `providers` / `session` / `export`/`import`/`stats`/`db`/`debug`/`generate` CLI subcommands, plugin loader (dynamic npm-resolution), mDNS publish, OpenAPI runtime spec, OAuth headless device-code flow, `wellknown` auth blob form, control-plane workspace routes, file watcher (`@parcel/watcher`), heap-snapshot watcher.

## P5 — Infrastructure / utility drift

- CORS middleware absent. `--cors` flag dropped at `main.rs:50`. `http::build_router` has no CORS layer. Browser-origin clients blocked. (May be OK if extension is the only consumer.)
- `--print-logs` / `--log-level` global flags rejected by clap. Verify VS Code launcher doesn't pass them.
- ~40 `Flag.*` env vars unread by Rust crates (only `KILO_SERVER_PASSWORD/USERNAME` and `KILO_LOG/RUST_LOG`). Notable: `KILO_PURE`, `KILO_PERMISSION`, `KILO_CONFIG`, `KILO_CONFIG_CONTENT`, `KILO_FAKE_VCS`, `KILO_DB`, `KILO_SKIP_MIGRATIONS`, `KILO_DISABLE_AUTOCOMPACT`, `KILO_EXPERIMENTAL_BASH_DEFAULT_TIMEOUT_MS`, `KILO_AUTO_SHARE`, `KILO_GIT_BASH_PATH`.
- `AGENT=1` / `OPENCODE=1` / `KILO_PID` env not exported to spawned children.
- `Telemetry` trait is no-op default (`telemetry.rs:46-72`); OTEL_EXPORTER env vars not honored. Telemetry data is lost.
- LSP client entirely deferred. The `lsp` tool exists with heuristic document/workspace symbols, but server-backed hover/definition/reference/call-hierarchy operations still return "No LSP server available".
- `Git.Service` typed wrapper missing — Rust shells out per route; risk of duplication and complex git command divergence.
- `apply_patch` and `bash` use Rust-reimplemented parsers; behavioral parity not verified beyond fake-tool tests.
- `kilo-tools`, `kilo-session` crates are placeholder stubs (`pub const CRATE = "..."`); reserved for later milestones.
- `kilo-mcp` crate is stale-named — `CLAUDE.md` says "empty for later extraction" but file is 209 lines and live-imported.

## Behavioral correctness flags by area

- Streaming: `response.output_item.added` for non-`function_call` items silently ignored at `lib.rs:1145-1180` (refusals, file-citations, web_search_call). `response.output_text.annotation.added` not parsed. `response.refusal.*` not parsed.
- Error mapping: `parseStreamError` mapping for `usage_not_included` (Plus upsell hint), `insufficient_quota` (billing hint), `invalid_prompt`, `server_error` not implemented (`provider/error.ts:131-167`). OpenAI HTTP status errors now preserve status/body/retry-after metadata and set `isRetryable` for 5xx/429-style failures; in-stream provider error events still map less richly.
- OAuth refresh: OpenAI turns now refresh before every stream request, so multi-iteration tool loops do not reuse a stale turn-start auth blob. LLM-backed enhance-prompt and commit-message routes also use the same fresh-auth path. Long compaction subcalls still need the same wrapper treatment.
- `prompt_cache_key` not sent (loses subscription-side cache savings).
- Cost: `assistant.info.cost` and OpenAI stream `step-finish.cost` now use model pricing when present; task subagent child assistant costs are propagated into the parent assistant message via a serialized store update.
- `path:{cwd,root}` is now populated on user/assistant info via `assistant_path`; older snapshots of this document that mention `{}` are stale.

---

## Round-1 source-of-truth pointers

Each agent's full report (with file:line citations and confidence calls) is preserved in this conversation's transcript; their punch lists feed each section above. Domains covered:

1. HTTP routes parity
2. Agent tools parity
3. LLM-provider parity (OpenAI/Codex narrow target)
4. Storage / sessions / sync / snapshot / auth
5. MCP, permissions/approvals, event bus / SSE
6. Kilocode-specific feature surface
7. CLI, infrastructure, PTY, LSP, IDE, plugin
8. Agent turn loop, streaming, compaction, abort, sub-agent

---

# Round 2 — additions and refinements

## P0 additions (user-visible bugs)

| Area | Gap | Bun ref | Rust ref | Sev |
|---|---|---|---|---|
| `pty.exit` vs `pty.exited` event name | Closed: Rust now emits `"pty.exited"`. Remaining PTY work is WebSocket transport plus replay buffer/cursor. | n/a | `routes/pty.rs` | closed |
| `global.disposed` not published | `routes/health.rs:25-30` returns `true` and contains a self-incriminating comment about the missing publish. Webview subscribers at `event-reducer.ts:27`, `KiloProvider.ts:2712`, `AutocompleteServiceManager.ts:106` never re-bootstrap after login/logout/org-switch. **One-line fix.** | n/a | `routes/health.rs:28-30` | H |
| `server.instance.disposed` not published | `routes/health.rs:36-38` is no-op. Subscribers at `event-reducer.ts:105`, `KiloProvider.ts:2717-2723` never refresh after config save. **One-line fix.** | n/a | `routes/health.rs:36-38` | H |
| `/event` SSE doesn't terminate on instance dispose | Bun closes the stream on `Bus.InstanceDisposed` (`server/routes/instance/event.ts:69-74`); Rust's `instance_events` loop only ends on `RecvError::Closed`. The `SdkSSEAdapter` reconnect callback (which re-runs `recoverPendingPrompts`, `flushPendingSessionRefresh`, `checkConfigWarnings`) never refires. | `server/routes/instance/event.ts:69-74` | `http/sse.rs:177-210` | H |
| `kilocode.removeSkill` / `removeAgent` no-op stubs | Closed: Rust now deletes local registry files/directories with path-safety checks and returns 404 for missing entries. | `packages/opencode/src/server/routes/instance/kilocode.ts:39,69` | `routes/compat.rs` | closed |
| `enhance-prompt` is echo stub (correction to round-1 P1) | Round-1 said "missing route". Actually the route is registered (`routes/enhance.rs`) but echoes the input verbatim. `KiloProvider.ts:891-905` reads `data.text` — user clicks "Improve my prompt" and gets back the same text. | `packages/opencode/src/kilocode/enhance-prompt.ts:22-46` | `routes/enhance.rs:14-20` | H |
| `kilo.cloud.session.get` returns shape that crashes consumer | Stub returns `{id, messages: []}` with no `info` field; extension reads `data.info.title` at `kilo-provider/handlers/cloud-session.ts:86` → TypeError → posts `cloudSessionImportFailed`. Cloud-sessions sidebar errors on click. | `packages/kilo-gateway/src/server/routes.ts:420` | `routes/compat.rs:86-88` | H |
| `kilo.profile` blank stub consumed eagerly | Extension fetches at every `disposeGlobal`/login (`handlers/auth.ts:147`); Rust returns `{profile:{email:"", name:"Kilo", organizations:[]}, balance:null, currentOrgId:null}`. Account UI shows "Kilo / no email", org switcher empty. (OOS per scope, but extension is not feature-gated.) | `packages/kilo-gateway/src/server/routes.ts:129` | `routes/compat.rs:50` | M |
| `Pty` response shape missing fields | Rust returns `{id, cols, rows}`; SDK `Pty` requires `{id, title, command, args, cwd, status, pid}` (`types.gen.ts:653-666`). Consumers reading `result.status`/`pid`/`cwd` get `undefined`. | n/a | `routes/pty.rs:190-195` | M |
| `Agent` response shape missing fields | Rust emits `[{name, mode, native, prompt?, hidden?, deprecated?, permission: [], options}]`; SDK requires `{name, mode, builtIn, permission: object, tools: object, options}`. `agent.builtIn` returns `undefined`; `agent.permission.edit` throws (array vs object). | `packages/opencode/src/server/routes/instance/index.ts:208` | `agent/catalog.rs:86-200` | M |
| `assistant.info.error` literal types not in SDK union | Rust emits `MaxIterationsError`, `MalformedToolArgumentsError`, `StructuredOutputError`, `CompactionError`. SDK `AssistantMessage.error` union (`types.gen.ts:120`) only includes `MessageAbortedError`, `ApiError`, `ProviderAuthError`, `UnknownError`, `MessageOutputLengthError`. TS narrowing on these literals dies; `tsgo` flags as unreachable. | `packages/opencode/src/provider/error.ts:50-167` | `agent/parts.rs:74-83`, `agent/openai_stream.rs:952-977`, `agent/compaction.rs:207-216` | M |
| Bare-status 404/400 responses lose error envelope | ~30 sites in `routes/sessions.rs`, `routes/messages.rs`, `routes/permissions.rs`, `routes/prompt.rs`, `routes/files.rs`, `routes/network.rs` return `StatusCode::*::into_response()` with empty body. SDK decoder (`packages/sdk/js/src/gen/client/client.gen.ts:151-160`) parses the body as JSON; on failure (empty), `error = ""` (string, not object). Consumer `error.name === "NotFoundError"` is `undefined === ...`. | n/a | many `routes/*` | M |
| Subagent: MCP-prefix deny rules dropped | Closed: child task sessions now preserve MCP-specific deny permissions, including sanitized server/tool prefixes, when deriving inherited rules. | `packages/opencode/src/kilocode/tool/task.ts:35-46` | `agent/parts.rs` | closed |
| Subagent: tools-disable map not threaded | Closed for recursive task suppression: Rust now passes a child tools map and filters individual tools before advertising. Remaining gap: richer inheritance of arbitrary parent-disabled tools. | `packages/opencode/src/tool/task.ts:154-169` | `agent/parts.rs` | partial |
| Subagent: agent default model ignored | Closed: task subagents now resolve agent model defaults before falling back to the parent model. | `packages/opencode/src/tool/task.ts:117-124` (`KiloTask.resolveModel`) | `agent/parts.rs` | closed |
| Subagent: variant not threaded | Closed: task subagents now thread the parent/user variant into the child prompt input. | `packages/opencode/src/tool/task.ts:158-161` | `agent/parts.rs` | closed |
| Subagent: cost propagation snapshot/release missing | Closed for task subagents: child assistant spend is reconciled into the parent wrapper message through `kilo-store` serialized cost updates. | `packages/opencode/src/kilocode/session/cost-propagation.ts:7-69` | `kilo-store/src/lib.rs`, `agent/parts.rs` | closed |

## Named-error registry drift (full inventory, replaces P0 row 24 in round-1 table)

40+ named errors are emitted via `bad_request_named` (`routes/config.rs:32-41`), `pty_error`, `worktree_error`, `mcp_error`, or direct `json!` envelopes — none registered in `ALLOWED_INTERNAL_ERROR_NAMES` (`error.rs:34-49`):

OAuth (4): `OauthUnsupportedProvider`, `OauthUnsupportedMethod`, `OauthCodeMissing`, `OauthPendingMissing`, `OauthStateMismatch`, `OauthCallbackTimeout`.

PTY (7): `RustPtyOpenError`, `RustPtySpawnError`, `RustPtyWriterError`, `RustPtyReaderError`, `RustPtyNotFoundError`, `RustPtyWriteError`, `RustPtyResizeError`.

Worktree (8): `WorktreeInvalidInputError`, `WorktreeCreateFailedError`, `WorktreeRemoveFailedError`, `WorktreeResetFailedError`, `WorktreeResetUnsafeError`, `WorktreeNotGitError`, `WorktreeListFailedError`, `WorktreePathSafetyError`.

MCP (16): `RustMcpAuthInvalidError`, `RustMcpNotFoundError`, `RustMcpAuthPersistError`, `RustMcpOAuthConfigError`, `RustMcpOAuthPersistError`, `RustMcpOAuthCallbackError`, `RustMcpOAuthStateError`, `RustMcpDisabledError`, `RustMcpDisconnectedError`, `RustMcpOAuthDiscoveryError`, `RustMcpOAuthRegistrationError`, `RustMcpAuthRefreshError`, `RustMcpRemoteConfigError`, `RustMcpHttpError`, `RustMcpTimeoutError`, `RustMcpMalformedResponseError`, `RustMcpToolError`, `RustMcpWriteError`, `RustMcpClosedError`.

Other: `CompactionError` (assistant-side, also not in CONTRACT.md "Assistant message error envelopes"), `APIError` (caps mismatch — CONTRACT shows `ApiError`).

**Decision needed**: extend `ALLOWED_INTERNAL_ERROR_NAMES` + CONTRACT.md, OR collapse Rust-only error names into a single `RustInternalError` discriminator with a `code` sub-field.

## Quick wins (publish-only Rust changes; no new compute)

These items have a working webview consumer AND existing Rust state — they need just an event-publish call. Total estimated effort: 1 day.

1. **PTY WebSocket + replay buffer/cursor**. The event spelling is fixed; terminal reconnect/restoration still needs the architectural transport work.
2. **`global.disposed` publish on `POST /global/dispose`** (`routes/health.rs:28-30`). Restores post-login refresh.
3. **`server.instance.disposed` publish on `POST /instance/dispose`** (`routes/health.rs:36-38`). Restores post-config-save refresh.
4. **Terminate `/event` SSE on instance dispose** (`http/sse.rs:177-210`, match Bun's `InstanceDisposed` close). Re-enables SSE-reconnect callback chain.
5. **`session.diff` publish after edit/write/patch** — diff data already in `agent/tools/diff.rs`; emit on tool completion. Updates review-tab badge mid-turn.
6. **`vcs.branch.updated` poll-and-publish on each `idle` transition.** Cheap (`git rev-parse --abbrev-ref HEAD`).
7. **`suggestion.shown` publish** when a `suggestion` is queued, paired with the existing accept/dismiss publishes.
8. **`mcp.tools.changed` publish** on the existing `tools/list_changed` notification handler (`routes/mcp.rs:469-510`).
9. **`session.compaction.compacted` publish** after `agent/compaction.rs::compact_session` returns.
10. **`server.lagged` consumer-side handler** — webview side, refetch affected directory's session list on receipt.

## Architectural fixes (non-trivial)

**PTY bidirectional WebSocket** — combined gap blocks the entire Agent Manager terminal feature on Rust:
- No `/pty/{id}/connect` route at all (`http/mod.rs:149-150` registers only `/pty` + `/pty/{id}` PUT/DELETE).
- Webview opens `ws://` and sends keystrokes via `ws.send()` (`packages/app/src/components/terminal.tsx:425-427, 512-523`).
- Webview expects `cursor=-1` resume protocol against a 2 MiB ring buffer.
- Auth via `?auth_token=base64(user:pass)` query param (`agent-manager/terminal-routing.ts:155`) — Rust middleware does accept this for non-loopback (`http/mod.rs`).

Three things needed: WS upgrade handler, ring buffer with cursor, bidirectional framing matching Bun's wire shape.

**Routes outside SDK contract** — these Rust-defined routes have no SDK type:
- `GET /skill`, `GET /global/dispose`, `POST /enhance-prompt`, `GET /config/warnings`, `GET /provider/auth`, `POST /session/{id}/revert`/`unrevert`, `POST /internal/session/{id}/message`, `POST /mcp/{name}/oauth/authorize`, `GET /mcp/{name}/oauth/callback`, `POST /mcp/{name}/tool`, all `/permission/*`, all `/question/*`, all `/network/*`, all `/suggestion/*`, all `/experimental/worktree/*`, `GET /remote/status`/`enable`/`disable`, `GET /indexing/status`, `POST /commit-message`, all `/kilo/*`, all `/kilocode/*`.

Either generate the SDK from Rust's spec instead of Bun's, or have Bun add stubs so the SDK regenerates with the right types.

## Test coverage shortlist (top 10 next tests)

Bun has ~1100 tests; Rust kilo-server has 194; Rust oracle has ~30. Production-tool internals have **near-zero coverage** — fake-provider tests bypass the real `read`/`write`/`edit`/`bash`/`apply_patch` codepaths. Recommended additions, in priority order:

1. Edit replacer property test (LF/CRLF, leading whitespace, indentation skew, escape skew, trim) — currently fails.
2. apply_patch parity: move + multi-chunk + EOF anchor.
3. Bash arity / pattern matching (`git push *`, `cat /etc/passwd` → `external_directory`).
4. Permission "always" rule drain across pending sibling requests.
5. Permission reject cascade across siblings.
6. Compaction repeated-anchor + retained-tail budget.
7. Encoding round-trip via real (not fake) `read`/`write` tools — UTF-16 LE BOM, Shift_JIS, Windows-1252.
8. apply_patch property: any add+update+delete combination round-trips.
9. MCP `tools.changed` SSE publish assertion.
10. Synchronous-tool abort propagation (200ms budget).

Recorded oracle fixtures: 10 SSE/startup fixtures all tied to assertions; only `fixtures/store/empty.json` is a near-stub (schema-only check, no Bun-vs-Rust contents parity) — promote into a real cross-impl test.

## Subagent task tool: implementation order (from F-section deep dive)

Each step independently shippable:

1. **Cost propagation** — closed for task subagents: Rust now sums child assistant-message cost and adds it to the parent assistant message through a serialized store update, then final OpenAI completion adds the parent turn's own model cost on top.
2. **Model resolution** — wire agent default model from `state.agent_info(agent)["model"]`; thread `variant` through `task_tool_part` → `execute_task_tool` → `PromptInput`. Optionally read `model.json` (gate on `KILO_CLIENT`).
3. **Permission inheritance** — MCP-prefix denies, child tool filtering, and child `todowrite` suppression are now implemented; remaining work is `experimental.primary_tools` and fuller parent-disabled tool propagation.
4. **Tokio runtime mechanics** — replace 20ms polling bridge with `tokio::sync::watch`; consider making `prompt_turn` `Send` to drop `LocalSet`; drop the per-thread runtime cache; surface panics via `RouteError`.

## Worktree diff polling deliberately bypasses SDK

`packages/kilo-vscode/src/agent-manager/worktree-diff-controller.ts:21-23` documents that hot polling uses an in-process TS reimplementation (`agent-manager/local-diff.ts`) to keep git spawns out of the sidecar. This means **steady-state worktree diff has zero Rust dependency**; only revert and the sidebar Changes panel hit Rust. Diff-format drift (P0 row 2) only surfaces during revert and explicit DiffViewer fetches — narrower blast radius than initially scoped.

## Round-2 source-of-truth pointers

Each agent's full report is preserved in this conversation's transcript. Domains covered:

- VS Code extension client expectations — every `client.*.method(` and direct-fetch site enumerated; stubs that bite vs harmless silence classified.
- Oracle/test coverage — Bun ~1100 tests vs Rust ~224; production-tool divergence quantified; top 10 next tests prioritized.
- SDK wire-shape and CONTRACT.md drift — full named-error registry drift; per-route shape parse outcomes; SDK error-decoder behavior on drift.
- Webview SSE consumer expectations — every event subscriber traced; UX failures from missing producers; quick wins identified.
- Subagent task tool deep dive — phase-by-phase Bun↔Rust diff, Tokio runtime mechanics audit, cost propagation gap, permission inheritance gap, 10 concrete failure scenarios, implementation order.

## Round-3 inspection queue (if desired)

Most of the territory is now mapped. Remaining unexplored corners:

- VS Code extension webview UI bundle (`packages/kilo-vscode/webview-ui/` if it exists separately from the shared `packages/app/src/`) — confirm whether the extension uses the shared Solid app or its own bundle, which changes which webview subscribers are actually live.
- Bash tree-sitter parsing model — implementation-design pass (largest single tool lift; currently `agent/tools/common.rs:83` returns `vec!["*"]` shortcut).
- Edit replacer 9-strategy chain — implementation-design pass with property-test design.
- Plan mode lifecycle — plan-file contract injection and `plan_exit` hard stop are implemented; full plan-followup approval loop remains open.
- Snapshot/revert subsystem — shadow-git snapshots and restore/unrestore are implemented for new Rust turns; structuredPatch/diffFull and MAX_DIFF_SIZE remain open.
- Indexing pipeline (`@kilocode/kilo-indexing`, lancedb) — if in-scope.
- SSE termination invariants — full audit beyond the instance-dispose case.
