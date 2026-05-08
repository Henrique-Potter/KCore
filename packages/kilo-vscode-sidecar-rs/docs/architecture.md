# kilo-server module architecture

Source-of-truth for the internal layout of `crates/kilo-server`. Read this before
moving code. The crate exposes one public function (`serve`) plus the `ServeOptions`
struct; everything else is `pub(crate)` and lives behind a module seam.

## Why a redesign

`crates/kilo-server/src/lib.rs` is 8287 lines and mixes route handlers, session
state, OAuth, the SSE bus, the agent turn loop, the fake-provider tool runtime,
permission evaluation, file-search helpers, and ~3270 lines of inline tests. AI
agents and humans both lose context because every concern lives in one symbol
namespace. The redesign compartmentalizes by concern, not by HTTP verb, with one
near-stable seam per concern.

## Sizing rule

- Soft target per file: **300-800 lines**.
- Hard ceiling for genuinely cohesive units: **~1.2k**.
- Anything larger means the module is hiding two concerns and must split.
- Tests live in `tests.rs` siblings (or `mod tests` at the bottom) of the
  module they exercise, not in a single mega-`mod tests`.

## Module tree (target)

```
crates/kilo-server/src/
  lib.rs                      // ~120 lines: pub serve() + ServeOptions + re-exports
  state.rs                    // AppState, PendingAuth, Runner, RunnerGuard, ViewedState
  error.rs                    // TurnError, RouteError, internal_error*, busy_error,
                              //   turn_error, unsupported_provider_error, frame helpers
  http/
    mod.rs                    // build_router(state) -> axum::Router
    middleware.rs             // auth, directory_header_rewrite, authorized, credential
    sse.rs                    // events, instance_events, frame, payload_frame,
                              //   publish_* helpers (turn_open/close/status/idle/error/
                              //   part_delta/events/for_session)
  routes/
    mod.rs                    // route_module re-exports, shared util (rewrite, cursor)
    health.rs                 // /global/health, /global/event(s), dispose stubs, paths,
                              //   warnings, status, mcp_status, remote_status, agents,
                              //   skills/commands/suggestions
    config.rs                 // /config*, providers, provider_detail, provider_auth,
                              //   set_auth/clear_auth
    sessions.rs               // /session* CRUD, viewed, fork, diff, share, summarize,
                              //   revert, update, delete, append_message
    messages.rs               // /session/{id}/message{,/...}/part/* CRUD + cursors
    prompt.rs                 // /session/{id}/message POST + prompt_async + abort
                              //   (thin: dispatch into agent::)
    permissions.rs            // /permission*, /question* + permission_list/question_list
    files.rs                  // /find*, /file*, search_files, search_text, walk, etc.
  oauth/
    mod.rs                    // oauth_authorize, oauth_callback, oauth_browser_callback
    listener.rs               // ensure_oauth_listener, run_oauth_listener, oauth_html
    crypto.rs                 // pkce_challenge, oauth_secret, jwt_claims, claim_account
    tokens.rs                 // exchange_code, refresh_access, fresh_auths,
                              //   token_auth, merge_token_auth, extract_account
    url.rs                    // oauth_url, query_params, url_decode, url_encode
  agent/
    mod.rs                    // pub(crate) run_turn() entrypoint
    turn.rs                   // prompt_turn, prompt_guarded, start_runner, ensure_*
    fake.rs                   // prompt_fake, fake_tool_parts, fake_call_id, fake_flag,
                              //   fake_delay, fake_provider, fake_abort, fake_error,
                              //   fake_tool_calls, wait_fake, is_canceled
    openai_stream.rs          // prompt_openai_stream, finalize_openai_aborted,
                              //   assistant_*_info, step_finish_part_*, repair_tool_name,
                              //   should_inject_noop, has_tool_call(s), tokens_value
    parts.rs                  // append_assistant, assistant_parts*, max_iterations_error,
                              //   tool_part_response, tool_completed/error,
                              //   tool_input, prompt_text, real_messages, real_tools,
                              //   real_tool_parts, real_safe_tool_part,
                              //   real_mutating_tool_part, real_tool_part
    tools/
      mod.rs                  // pub(crate) tool defs + dispatch
      defs.rs                 // read/grep/write/edit/apply_patch/bash ChatTool defs
      bash.rs                 // fake_bash, shell_command, read_pipe, combined_output,
                              //   truncate_output, tool_timeout
      fs.rs                   // fake_read, fake_read_dir, fake_read_file, fake_write,
                              //   fake_edit, fake_grep
      patch.rs                // fake_apply_patch + parse_apply_patch + PatchKind/Section/
                              //   Result + patch_added/updated, join_patch_lines, count_lines
      diff.rs                 // text_diff, title, slash, resolve_under
      common.rs               // tool_input, tool_usize, tool_enabled, model_toolcall,
                              //   tool_permission, tool_patterns, patch_patterns
    permission.rs             // ask_permission, ask_permission_once, permission_decision,
                              //   permission_ruleset, parse_permission_rules,
                              //   evaluate_permission, wildcard_match
  util/
    cursor.rs                 // MessageCursor encode/decode (bun-compat base64)
    paths.rs                  // resolve_under, slash, list_nodes, read_content,
                              //   is_binary, walk
    encoding.rs               // url_encode/decode, decode_query, hex, html_escape,
                              //   unix_millis, loopback
tests/                        // (later) integration tests pulled out of inline mod tests
```

`kilo-session`, `kilo-tools`, and `kilo-mcp` are existing empty workspace crates
reserved for future extraction. Nothing moves there during M7. If the
`agent::` or `routes::sessions` subtree later grows past the size budget, those
crates absorb it without further refactor of `kilo-server`.

## Interface discipline

- `serve` and `ServeOptions` are the only `pub` items.
- Everything else is `pub(crate)`.
- Cross-module shared types live at the smallest scope that covers all callers:
  - `AppState` in `state.rs` (every module depends on it; carry by `&Arc<AppState>`).
  - `TurnError`, `RouteError` in `error.rs`.
  - `ChatTool`, `ChatMessage`, `ChatResponseItem` stay in `kilo-provider` (already there).
  - `MessageCursor` in `util/cursor.rs`.
- Handlers in `routes::` are thin: extract input, validate, call into
  `agent::` / `oauth::` / `state.rs` helpers, map the result to a `Response`.
- The agent turn loop is invoked through one entrypoint:
  `agent::run_turn(state, input, runner) -> Result<TurnOutcome, TurnError>`.
  `routes::prompt` does not import `prompt_openai_stream`, `prompt_fake`, or
  `prompt_turn` directly.
- The fake-provider tools are reachable only via `agent::tools::dispatch`.
  `routes::` never calls `fake_*` directly.

### Traits — defer

No new traits during the M7 migration. The seam between `agent::fake` and
`agent::openai_stream` is clear enough as a free function dispatched on the
provider id; introducing a `Provider` trait now adds indirection without payoff
because `kilo-provider` already owns the wire-shape boundary. Revisit when M10
adds a second provider.

## Migration sequence

Each step is one commit. Each step compiles and passes the existing oracle and
unit tests before merging. Stop the line if any of these break:

- `cargo check --workspace`
- `cargo test -p kilo-server`
- `cargo test -p kilo-oracle --test m7_rust_fixtures`
- `cargo test -p kilo-oracle --test rust_harness`
- M7 sidebar smoke fixture replay (`fixtures/sse/m7-*.jsonl`).

1. **Lift the test module out.** Move `mod tests { ... }` (lines 5016-8287)
   into `crates/kilo-server/src/tests.rs` with `#[cfg(test)] mod tests;` in
   `lib.rs`. Pure cut/paste; nothing else changes. ~3270 lines removed from
   `lib.rs`. Stop-the-line: full unit suite passes.

2. **Carve `state.rs` and `error.rs`.** Move `AppState` (+ `impl AppState`),
   `PendingAuth`, `Runner`, `RunnerGuard`, `PendingPermission`, `PermissionRule`,
   `PermissionDecision`, `ViewedState`, `FakeCall`, `Repair` to `state.rs`.
   Move `TurnError`, `RouteError`, `From<rusqlite::Error>`, `internal_error*`,
   `busy_error`, `turn_error`, `unsupported_provider_error` to `error.rs`.
   `lib.rs` only re-exports. Stop-the-line: same as step 1.

3. **Extract `http/` (router + middleware + SSE plumbing).** Move the `Router::new()`
   block from `serve` into `http::build_router(state)`. Move `auth`,
   `directory_header_rewrite`, `authorized`, `credential`, `auth_token`,
   `events`, `instance_events`, `frame`, `payload_frame`, and all
   `publish_*` helpers. `serve` shrinks to bind+listen+graceful-shutdown.
   Stop-the-line: oracle harness reaches readiness; SSE smoke replays.

4. **Split `routes/` by concern.** Create `routes::{health,config,sessions,
   messages,prompt,permissions,files}` and move the handlers verbatim.
   `routes::prompt` keeps the `prompt`/`prompt_async`/`abort_session` handlers
   but delegates to a still-private `agent::run_turn` shim that initially just
   calls the existing `prompt_turn`. Stop-the-line: full HTTP route parity
   tests in `kilo-oracle` pass.

5. **Extract `oauth/`.** Move OAuth listener, callback, token exchange,
   PKCE, JWT claim parsing, URL helpers, and the static `oauth_html` into
   `oauth::{listener,crypto,tokens,url}`. `routes::config` keeps the thin
   `oauth_authorize` / `oauth_callback` handlers and calls into `oauth::`.
   Stop-the-line: `cargo test -p kilo-server` (existing OAuth fixture tests).

6. **Carve `agent/turn.rs` and `agent/parts.rs`.** Move `prompt_turn`,
   `prompt_guarded`, `start_runner`, `ensure_prompt_supported`, and the
   `assistant_*`/`append_assistant`/`tool_*`/`real_*` helpers. `agent::run_turn`
   becomes the single seam called from `routes::prompt`. Stop-the-line:
   M7 fake-provider fixtures (`m7-single-fake-tool-call`, `m7-parallel-fake-tool-calls`,
   `m7-concurrent-sessions`) replay byte-for-byte.

7. **Split fake provider into `agent/fake.rs` + `agent/tools/`.** Move
   `prompt_fake` and the FakeCall plumbing into `agent::fake`. Move the six
   fake tools and apply-patch parser into `agent::tools::{defs,bash,fs,patch,
   diff,common}`. `agent::tools::dispatch(name, input, ctx)` is the only
   surface called by `agent::fake`. Stop-the-line: same M7 fixtures plus the
   apply-patch tests in the lifted `tests.rs`.

8. **Extract `agent/openai_stream.rs` and `agent/permission.rs`.** Move
   `prompt_openai_stream`, `finalize_openai_aborted`, the
   `assistant_openai_info` / `step_finish_part_*` helpers, repair logic,
   noop-injection rules, and tool-call detection. Move `ask_permission*`,
   `evaluate_permission`, ruleset parsing. Move file-search and path helpers
   into `routes/files.rs` + `util/paths.rs`. Stop-the-line: full M7 oracle
   suite, including the OpenAI Pro Codex Responses smoke.

After step 8, `lib.rs` is ~120 lines, every other module fits the size budget,
and the tests sit next to the code they cover.

## Risks and tradeoffs

- **Re-export drift.** Keeping `pub use` lines in `lib.rs` for items that the
  oracle harness imports (`kilo_server::serve`, `kilo_server::ServeOptions`)
  is mandatory. Any other internal symbol the oracle reaches into must stay
  reachable via the same path or be reworked in the same commit.
- **Route registration order.** Axum is order-sensitive for overlapping path
  patterns. `build_router` must register routes in the same order they appear
  today; do not reorder while moving handlers between files. Add a comment in
  `http::mod` pinning the order.
- **Fixture coupling.** The SSE fixtures in `fixtures/sse/m7-*.jsonl` are
  recorded against current `publish_*` ordering. Step 3 must preserve that
  ordering; if a publish helper changes signature, run the fixture replay
  before committing and re-record only with explicit user sign-off.
- **Test module split.** Step 1 moves ~3270 lines as a pure cut. Some inline
  tests reference `super::` private items; those keep working because
  `mod tests` lives inside the same crate. Avoid renaming any private item
  in the same step.
- **`kilo-session`/`kilo-tools` crates remain empty.** That is intentional:
  they are M8/M9 extraction targets. Do not migrate code there during M7.

## Validation

After each step:

```sh
cargo check --workspace
cargo test -p kilo-server
cargo test -p kilo-oracle --test m7_rust_fixtures
cargo test -p kilo-oracle --test rust_harness
```

Plus, for steps 3-8, replay the M7 SSE fixtures:

```sh
cargo test -p kilo-oracle --test fixture_pipeline
```

If a step touches publish ordering or the prompt path, also run the sidebar
smoke listed in the README.
