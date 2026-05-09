//! HTTP plumbing: router construction, middleware (auth + header rewrite),
//! and SSE bus glue. Route handlers themselves live in `routes/` (Step 4
//! of the migration); this module owns the wiring around them.

pub(crate) mod middleware;
pub(crate) mod sse;

use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    middleware::{from_fn, from_fn_with_state},
    routing::{get, patch, post, put},
    Router,
};

use crate::limits::MAX_REQUEST_BODY_BYTES;

use crate::routes::{
    compat::{
        commit_message, kilo_cloud_import, kilo_cloud_session, kilo_cloud_sessions, kilo_fim,
        kilo_organization, kilo_profile, kilocode_import_message, kilocode_import_part,
        kilocode_import_project, kilocode_import_session, kilocode_remove_agent,
        kilocode_remove_skill, remote_disable, remote_enable,
    },
    config::{
        clear_auth, config, config_providers, oauth_authorize, oauth_callback, provider_auth,
        provider_detail, providers, set_auth, update_config,
    },
    enhance::enhance_prompt,
    files::{file_content, file_status, find_file, find_symbol, find_text, list_file},
    health::{
        agents, global_dispose, health, instance_dispose, paths, project, remote_status, status,
        warnings,
    },
    indexing::indexing_status,
    mcp::{
        mcp_add, mcp_auth, mcp_call_tool, mcp_connect, mcp_disconnect, mcp_oauth_authorize,
        mcp_oauth_callback, mcp_status,
    },
    messages::{delete_message, delete_part, message, messages, update_part},
    network::{network_waits, reject_network_wait, reply_network_wait},
    permissions::{
        accept_suggestion, allow_everything, dismiss_suggestion, permission_rules, permissions,
        questions, reject_question, reply_permission, reply_question, suggestions,
    },
    prompt::{abort_session, command, prompt, prompt_async},
    pty::{pty_create, pty_delete, pty_update},
    registry::{commands, skills},
    sessions::{
        append_message, children, create_session, delete_session, diff_session, fork_session,
        revert_session, session, sessions, share_session, summarize_session, todos,
        unrevert_session, unshare_session, update_session, viewed,
    },
    worktree::{
        create_worktree, delete_worktree, reset_worktree, worktree_diff, worktree_diff_file,
        worktree_diff_summary, worktrees,
    },
};
use crate::AppState;

use middleware::{auth, directory_header_rewrite, json_body_lenient};

/// Build the Axum router. Route registration order is byte-for-byte
/// identical to the previous `lib.rs::app()` body. Axum's path matching
/// is order-sensitive for overlapping patterns; do NOT reorder routes
/// without also re-recording the SSE / fixture replays.
pub(crate) fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/global/health", get(health))
        .route("/global/event", get(sse::events))
        .route("/event", get(sse::instance_events))
        .route("/global/dispose", post(global_dispose))
        .route("/instance/dispose", post(instance_dispose))
        .route("/path", get(paths))
        .route("/config", get(config))
        .route("/global/config", get(config).patch(update_config))
        .route("/enhance-prompt", post(enhance_prompt))
        .route("/config/providers", get(config_providers))
        .route("/config/warnings", get(warnings))
        .route("/provider", get(providers))
        .route("/provider/auth", get(provider_auth))
        .route(
            "/provider/{provider_id}/oauth/authorize",
            post(oauth_authorize),
        )
        .route(
            "/provider/{provider_id}/oauth/callback",
            post(oauth_callback),
        )
        .route("/provider/{provider_id}", get(provider_detail))
        .route("/auth/{provider_id}", put(set_auth).delete(clear_auth))
        .route("/agent", get(agents))
        .route("/skill", get(skills))
        .route("/command", get(commands))
        .route("/project/current", get(project))
        .route("/session", get(sessions).post(create_session))
        .route("/session/viewed", post(viewed))
        .route("/session/status", get(status))
        .route(
            "/session/{id}",
            get(session).patch(update_session).delete(delete_session),
        )
        .route("/session/{id}/children", get(children))
        .route("/session/{id}/todo", get(todos))
        .route("/session/{id}/fork", post(fork_session))
        .route("/session/{id}/diff", get(diff_session))
        .route(
            "/session/{id}/share",
            post(share_session).delete(unshare_session),
        )
        .route("/session/{id}/summarize", post(summarize_session))
        .route("/session/{id}/revert", post(revert_session))
        .route("/session/{id}/unrevert", post(unrevert_session))
        .route("/session/{id}/message", get(messages).post(prompt))
        .route("/session/{id}/command", post(command))
        .route(
            "/session/{id}/message/{message_id}",
            get(message).delete(delete_message),
        )
        .route(
            "/session/{id}/message/{message_id}/part/{part_id}",
            patch(update_part).delete(delete_part),
        )
        .route("/session/{id}/prompt_async", post(prompt_async))
        .route("/session/{id}/abort", post(abort_session))
        .route("/internal/session/{id}/message", post(append_message))
        .route("/mcp", get(mcp_status).post(mcp_add))
        .route("/mcp/{name}/auth", put(mcp_auth))
        .route("/mcp/{name}/oauth/authorize", post(mcp_oauth_authorize))
        .route("/mcp/{name}/oauth/callback", get(mcp_oauth_callback))
        .route("/mcp/{name}/connect", post(mcp_connect))
        .route("/mcp/{name}/disconnect", post(mcp_disconnect))
        .route("/mcp/{name}/tool", post(mcp_call_tool))
        .route("/permission", get(permissions))
        .route("/permission/allow-everything", post(allow_everything))
        .route("/permission/{id}/reply", post(reply_permission))
        .route("/permission/{id}/always-rules", post(permission_rules))
        .route("/question", get(questions))
        .route("/question/{id}/reply", post(reply_question))
        .route("/question/{id}/reject", post(reject_question))
        .route("/network", get(network_waits))
        .route("/network/{id}/reply", post(reply_network_wait))
        .route("/network/{id}/reject", post(reject_network_wait))
        .route("/find", get(find_text))
        .route("/find/file", get(find_file))
        .route("/find/symbol", get(find_symbol))
        .route("/file", get(list_file))
        .route("/file/content", get(file_content))
        .route("/file/status", get(file_status))
        .route("/pty", post(pty_create))
        .route("/pty/{id}", put(pty_update).delete(pty_delete))
        .route(
            "/experimental/worktree",
            get(worktrees).post(create_worktree).delete(delete_worktree),
        )
        .route("/experimental/worktree/reset", post(reset_worktree))
        .route("/experimental/worktree/diff", get(worktree_diff))
        .route(
            "/experimental/worktree/diff/summary",
            get(worktree_diff_summary),
        )
        .route("/experimental/worktree/diff/file", get(worktree_diff_file))
        .route("/suggestion", get(suggestions))
        .route("/suggestion/{id}/accept", post(accept_suggestion))
        .route("/suggestion/{id}/dismiss", post(dismiss_suggestion))
        .route("/remote/status", get(remote_status))
        .route("/indexing/status", get(indexing_status))
        .route("/remote/enable", post(remote_enable))
        .route("/remote/disable", post(remote_disable))
        .route("/commit-message", post(commit_message))
        .route("/kilo/profile", get(kilo_profile))
        .route("/kilo/organization", post(kilo_organization))
        .route("/kilo/fim", post(kilo_fim))
        .route("/kilo/cloud-sessions", get(kilo_cloud_sessions))
        .route("/kilo/cloud/session/import", post(kilo_cloud_import))
        .route("/kilo/cloud/session/{id}", get(kilo_cloud_session))
        .route(
            "/kilocode/session-import/project",
            post(kilocode_import_project),
        )
        .route(
            "/kilocode/session-import/session",
            post(kilocode_import_session),
        )
        .route(
            "/kilocode/session-import/message",
            post(kilocode_import_message),
        )
        .route("/kilocode/session-import/part", post(kilocode_import_part))
        .route("/kilocode/skill/remove", post(kilocode_remove_skill))
        .route("/kilocode/agent/remove", post(kilocode_remove_agent))
        .with_state(state.clone())
        .layer(from_fn_with_state(state, auth))
        // Header→query rewrite must run BEFORE the inner extractors see Query<>
        // (and before auth, which is fine — auth doesn't read these headers).
        // SDK clients (`packages/sdk/js/src/v2/client.ts`) rewrite the headers
        // client-side for GET/HEAD, so this middleware is a safety net for
        // non-SDK callers (oracle harness, curl). It mirrors the SDK's behavior:
        // only GET/HEAD are rewritten, and existing query values win.
        .layer(from_fn(directory_header_rewrite))
        // Outermost: normalize empty-body POST/PUT/PATCH/DELETE so axum's
        // strict `Json<T>` extractor mirrors Hono's `c.req.valid("json") ?? {}`
        // tolerance. The hey-api generated SDK strips `Content-Type` on
        // empty-body calls (e.g. `client.session.create({ directory })`),
        // which would otherwise hit the inner extractor and bounce with a
        // 415. Must run before `directory_header_rewrite` because GET/HEAD
        // are passed through unchanged here, and before `auth` because we
        // only mutate headers/body, not credentials.
        .layer(from_fn(json_body_lenient))
        // Apply the body cap from `limits::MAX_REQUEST_BODY_BYTES`. axum's
        // default limit is 2 MiB which would reject the 16 MiB ceiling the
        // operational invariants spec; raising the body limit explicitly is
        // safer than relying on transitive defaults. Any request larger than
        // the cap is rejected by axum before extractors run with a 413
        // (axum surfaces it as `LengthLimitError`). The middleware layer is
        // outermost so the cap applies before json_body_lenient buffers.
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
}
