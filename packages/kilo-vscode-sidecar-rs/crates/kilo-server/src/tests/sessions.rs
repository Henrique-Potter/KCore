//! Session route tests: update, fork, message routes, part-mutation
//! routes, viewed-state, suggestion accept/dismiss, and the
//! `publish_events` ordering invariant.

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use kilo_protocol::{
    MessageAppendInput, SessionCreateInput, SessionForkInput, SessionRevertInput,
    SessionShareInput, SessionUpdateInput, SessionViewedInput,
};
use kilo_store::StoredEvent;
use serde_json::json;

use crate::http::sse::publish_events;
use crate::routes::messages::{delete_message, delete_part, message, update_part};
use crate::routes::permissions::{accept_suggestion, dismiss_suggestion, suggestion_list};
use crate::routes::sessions::{
    children, delete_session, diff_session, fork_session, init_session, revert_session, set_viewed,
    share_session, summarize_session, todos, unrevert_session, unshare_session, update_session,
    viewed_snapshot, SessionInitInput, SummarizeBody,
};
use crate::{PendingSuggestion, SuggestionDecision};

use super::common::{
    assert_sync, drain_no_store_mirror, recv_sync, response_to_value, seed, state, state_at,
    unique_root,
};

#[tokio::test]
async fn update_session_route_persists_permission_and_archived_time() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let mut rx = state.bus.subscribe();

    let res = update_session(
        State(state.clone()),
        Path(session.id.clone()),
        Json(SessionUpdateInput {
            title: Some("Updated".to_string()),
            permission: Some(json!({ "edit": "allow" })),
            time: Some(json!({ "archived": 123 })),
        }),
    )
    .await;
    let updated = state.store.session(&session.id).unwrap();
    let event = rx.try_recv().expect("session updated event").as_global();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(updated.title, "Updated");
    assert_eq!(updated.permission, Some(json!({ "edit": "allow" })));
    assert_eq!(updated.time.archived, Some(123));
    assert_eq!(event.payload.kind, "session.updated");
    assert_eq!(
        event.payload.properties["info"]["permission"],
        json!({ "edit": "allow" })
    );
    assert_eq!(event.payload.properties["info"]["time"]["archived"], 123);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn todo_route_returns_session_todos() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    state
        .store
        .update_todos(
            &session.id,
            &[json!({
                "content": "ship",
                "status": "pending",
                "priority": "high"
            })],
        )
        .unwrap();

    let body = response_to_value(todos(State(state.clone()), Path(session.id.clone())).await).await;
    assert_eq!(body[0]["content"], "ship");
    assert_eq!(
        todos(State(state), Path("missing".to_string()))
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn child_fork_and_message_routes_publish_sync_events() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput {
            title: Some("Root".to_string()),
            ..Default::default()
        })
        .expect("create session");
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({ "id": "msg_route", "role": "user" }),
                parts: vec![json!({ "id": "prt_route", "type": "text", "text": "hello" })],
            },
        )
        .unwrap();
    let mut rx = state.bus.subscribe();

    let res = fork_session(
        State(state.clone()),
        Path(session.id.clone()),
        Json(SessionForkInput::default()),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let events = drain_no_store_mirror(&mut rx);
    assert_sync(&events[0], "session.created.v1", "", None);
    assert_sync(&events[1], "message.updated.v1", "user", None);
    assert_sync(&events[2], "message.part.updated.v1", "", Some("hello"));

    let kids = state.store.children(&session.id).unwrap();
    let res = children(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(kids.len(), 1);
    assert_eq!(kids[0].title, "Root (fork #1)");

    let res = message(
        State(state.clone()),
        Path((session.id.clone(), "msg_route".to_string())),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn part_delete_revert_share_and_summarize_routes_are_safe() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({ "id": "msg_mut", "role": "user" }),
                parts: vec![json!({ "id": "prt_mut", "type": "text", "text": "old" })],
            },
        )
        .unwrap();
    let mut rx = state.bus.subscribe();

    let res = update_part(
            State(state.clone()),
            Path((
                session.id.clone(),
                "msg_mut".to_string(),
                "prt_mut".to_string(),
            )),
            Json(json!({ "id": "prt_mut", "messageID": "msg_mut", "sessionID": session.id, "type": "text", "text": "new" })),
        )
        .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_sync(
        &recv_sync(&mut rx),
        "message.part.updated.v1",
        "",
        Some("new"),
    );

    let res = revert_session(
        State(state.clone()),
        Path(session.id.clone()),
        Json(SessionRevertInput {
            message_id: Some("msg_mut".to_string()),
            summary: Some(json!({
                "additions": 0,
                "deletions": 0,
                "files": 0,
                "diffs": []
            })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        state.store.session(&session.id).unwrap().revert.unwrap()["messageID"],
        "msg_mut"
    );
    assert_sync(&recv_sync(&mut rx), "session.updated.v1", "", None);

    let res = diff_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = share_session(
        State(state.clone()),
        Path(session.id.clone()),
        Some(Json(SessionShareInput {
            url: Some("https://share.test/s".to_string()),
        })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_sync(&recv_sync(&mut rx), "session.updated.v1", "", None);
    let res = summarize_session(State(state.clone()), Path(session.id.clone()), None).await;
    assert_eq!(res.status(), StatusCode::OK);
    let res = unshare_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_sync(&recv_sync(&mut rx), "session.updated.v1", "", None);
    let res = unrevert_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_sync(&recv_sync(&mut rx), "session.updated.v1", "", None);
    let res = delete_part(
        State(state.clone()),
        Path((
            session.id.clone(),
            "msg_mut".to_string(),
            "prt_mut".to_string(),
        )),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_sync(&recv_sync(&mut rx), "message.part.removed.v1", "", None);
    let res = delete_message(
        State(state.clone()),
        Path((session.id.clone(), "msg_mut".to_string())),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_sync(&recv_sync(&mut rx), "message.removed.v1", "", None);

    let _ = std::fs::remove_dir_all(root);
}

/// Bun parity: `POST /session/{id}/summarize` accepts
/// `{providerID, modelID, auto?}` and returns a boolean
/// (`packages/opencode/src/server/routes/instance/session.ts:556-562,596`).
/// With no provider auth configured the route short-circuits to `false`
/// rather than 500-ing — body parsing must still succeed so the SDK
/// client doesn't see a 4xx for a well-formed payload.
#[tokio::test]
async fn summarize_session_accepts_provider_model_body_and_returns_boolean() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let res = summarize_session(
        State(state.clone()),
        Path(session.id.clone()),
        Some(Json(SummarizeBody {
            provider_id: Some("openai".to_string()),
            model_id: Some("gpt-5".to_string()),
            auto: false,
        })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = response_to_value(res).await;
    assert!(body.is_boolean(), "expected boolean response, got {body}");

    let _ = std::fs::remove_dir_all(root);
}

/// Bun parity: when `summarize` is invoked with `auto: true`, after
/// `compaction.create` lands the summary anchor Bun chains
/// `prompt.loop({sessionID})` to keep the agent driving
/// (`packages/opencode/src/server/routes/instance/session.ts:586-595`).
/// The Rust port mirrors that by spawning a background turn through
/// `agent::run_turn_async`. We seed the session with a single user
/// message tagged `summary: true` so `compact_session` short-circuits
/// to `EmptyHistory` (no provider call needed) while the follow-up
/// path can still recover the user text + fake-provider hint.
#[tokio::test]
async fn summarize_session_with_auto_true_fires_followup_turn() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    state
        .store
        .set_provider_auth("openai", json!({ "type": "api", "key": "sk-test" }))
        .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({
                    "id": "msg_seed",
                    "role": "user",
                    "summary": true,
                    "provider": { "fake": true }
                }),
                parts: vec![json!({ "id": "prt_seed", "type": "text", "text": "task" })],
            },
        )
        .unwrap();

    let res = summarize_session(
        State(state.clone()),
        Path(session.id.clone()),
        Some(Json(SummarizeBody {
            provider_id: None,
            model_id: None,
            auto: true,
        })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = response_to_value(res).await;
    assert_eq!(body, json!(true));

    // Follow-up runs on the prompt-queue task. Poll for the new
    // user + assistant pair (3 messages total) up to ~500ms.
    let mut final_len = 0;
    for _ in 0..50 {
        let page = state.store.messages(&session.id, None, None).unwrap();
        final_len = page.items.len();
        if final_len >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        final_len >= 3,
        "expected follow-up turn to persist a new user + assistant pair, got {final_len} messages",
    );
    let page = state.store.messages(&session.id, None, None).unwrap();
    // Items[0] is the seeded summary user message; the follow-up
    // appends a new user + assistant carrying the replayed text.
    assert_eq!(page.items[0].info["id"], "msg_seed");
    assert_eq!(page.items[1].info["role"], "user");
    assert_eq!(page.items[1].parts[0]["text"], "task");
    assert_eq!(page.items[2].info["role"], "assistant");
    assert_eq!(page.items[2].parts[0]["text"], "Echo: task");

    let _ = std::fs::remove_dir_all(root);
}

/// Counterpart to the auto-true test: with `auto: false` the route
/// must NOT spawn a follow-up turn. Same seed, same 500ms wait — the
/// session should still hold exactly the seeded message.
#[tokio::test]
async fn summarize_session_with_auto_false_does_not_fire_followup() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    state
        .store
        .set_provider_auth("openai", json!({ "type": "api", "key": "sk-test" }))
        .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({
                    "id": "msg_seed",
                    "role": "user",
                    "summary": true,
                    "provider": { "fake": true }
                }),
                parts: vec![json!({ "id": "prt_seed", "type": "text", "text": "task" })],
            },
        )
        .unwrap();

    let res = summarize_session(
        State(state.clone()),
        Path(session.id.clone()),
        Some(Json(SummarizeBody {
            provider_id: None,
            model_id: None,
            auto: false,
        })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = response_to_value(res).await;
    assert_eq!(body, json!(true));

    // Wait the same window the auto-true test uses; assert nothing
    // new lands. Seeing >1 message here means a stray follow-up
    // fired despite `auto: false`.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(
        page.items.len(),
        1,
        "auto=false must not spawn a follow-up turn; got {} messages",
        page.items.len()
    );
    assert_eq!(page.items[0].info["id"], "msg_seed");

    let _ = std::fs::remove_dir_all(root);
}

/// Empty / missing body must not 4xx — Rust's `summarize_session` accepts
/// `Option<Json<SummarizeBody>>` so callers can omit the override and let
/// the provider crate fall back to the configured default model. The
/// route returns `false` here because the test fixture has no provider
/// auths, but the fact that we got a 200 boolean (and not a 400 from the
/// extractor) is the contract under test.
#[tokio::test]
async fn summarize_session_with_no_body_succeeds() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let res = summarize_session(State(state.clone()), Path(session.id.clone()), None).await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = response_to_value(res).await;
    assert!(body.is_boolean(), "expected boolean response, got {body}");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn revert_restores_pre_turn_snapshot_and_unrevert_restores_redo_snapshot() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("a.txt"), "before\n").unwrap();
    let snapshot = match crate::snapshot::track(&state.store, &session.project_id) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            let _ = std::fs::remove_dir_all(root);
            return;
        }
    };
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({ "id": "msg_snap", "role": "user", "snapshot": snapshot }),
                parts: vec![json!({ "type": "text", "text": "change a" })],
            },
        )
        .unwrap();
    std::fs::write(repo.join("a.txt"), "after\n").unwrap();
    std::fs::write(repo.join("new.txt"), "new\n").unwrap();

    let res = revert_session(
        State(state.clone()),
        Path(session.id.clone()),
        Json(SessionRevertInput {
            message_id: Some("msg_snap".to_string()),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        std::fs::read_to_string(repo.join("a.txt"))
            .unwrap()
            .replace("\r\n", "\n"),
        "before\n"
    );
    assert!(!repo.join("new.txt").exists());
    let session = state.store.session(&session.id).unwrap();
    assert!(session.revert.unwrap()["snapshot"].as_str().is_some());
    let res = diff_session(State(state.clone()), Path(session.id.clone())).await;
    let diff = response_to_value(res).await;
    assert!(diff.as_array().is_some_and(|items| !items.is_empty()));

    let res = unrevert_session(State(state.clone()), Path(session.id.clone())).await;

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        std::fs::read_to_string(repo.join("a.txt"))
            .unwrap()
            .replace("\r\n", "\n"),
        "after\n"
    );
    assert_eq!(
        std::fs::read_to_string(repo.join("new.txt"))
            .unwrap()
            .replace("\r\n", "\n"),
        "new\n"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// `revert_session` should populate `session.summary` using the structured
/// `diff_full(<base=snapshot>..<head=redo>)` projection — `additions`,
/// `deletions`, `files`, `diffs[]` — matching Bun's `summaryFromDiffFull`
/// shape (`packages/opencode/kilocode/snapshot/diff-full.ts`).
#[tokio::test]
async fn revert_populates_summary_from_diff_full() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("a.txt"), "alpha\nbravo\ncharlie\n").unwrap();
    let snapshot = match crate::snapshot::track(&state.store, &session.project_id) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            // Skip on hosts without git (matches existing snapshot test pattern).
            let _ = std::fs::remove_dir_all(root);
            return;
        }
    };
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({ "id": "msg_summary", "role": "user", "snapshot": snapshot }),
                parts: vec![json!({ "type": "text", "text": "edit a" })],
            },
        )
        .unwrap();
    // Two adds, one delete on a.txt, plus a brand-new file.
    std::fs::write(repo.join("a.txt"), "alpha\nBRAVO\ncharlie\nfour\n").unwrap();
    std::fs::write(repo.join("new.txt"), "new\n").unwrap();

    let res = revert_session(
        State(state.clone()),
        Path(session.id.clone()),
        Json(SessionRevertInput {
            message_id: Some("msg_summary".to_string()),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let summary = state
        .store
        .session(&session.id)
        .unwrap()
        .summary
        .expect("summary populated by diff_full");
    let additions = summary["additions"].as_u64().unwrap();
    let deletions = summary["deletions"].as_u64().unwrap();
    let files = summary["files"].as_u64().unwrap();
    let diffs = summary["diffs"].as_array().unwrap();
    assert!(additions > 0, "expected positive additions, got {summary}");
    assert!(deletions > 0, "expected positive deletions, got {summary}");
    assert_eq!(files, diffs.len() as u64);
    assert!(files >= 2, "expected >=2 changed files, got {summary}");
    // Bun-parity `summary.diffs[*]` shape — each row carries
    // `{file, additions, deletions, status}` and NO `patch` field.
    for entry in diffs {
        assert!(entry["file"].is_string(), "missing file: {entry}");
        assert!(entry["status"].is_string(), "missing status: {entry}");
        assert!(
            entry.get("patch").is_none(),
            "patch must be stripped: {entry}"
        );
    }

    let _ = std::fs::remove_dir_all(root);
}

/// `delete_session` should fire the `on_session_deleted` snapshot cleanup
/// (best-effort `git gc --prune=now`) on the blocking pool. We can't easily
/// observe the GC side effect without racing the spawned task, so we assert
/// the route still returns 200 once a snapshot dir is on disk and that the
/// dir survives (the cleanup runs `git gc`, it does NOT remove dirs — they're
/// shared per worktree across sessions).
#[tokio::test]
async fn delete_session_triggers_snapshot_cleanup() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("a.txt"), "x\n").unwrap();
    if crate::snapshot::track(&state.store, &session.project_id).is_err() {
        // Host without git — wiring still verified by the cargo build.
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    let snapshot_root = crate::snapshot::snapshot_root(&state.store);
    assert!(
        snapshot_root.is_dir(),
        "snapshot root must exist after track()"
    );

    let res = delete_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    // Give the spawn_blocking cleanup a moment to land. The assertion below
    // is structural (dir survives the GC), so a missed scheduling window
    // doesn't cause a false negative.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(
        snapshot_root.is_dir(),
        "snapshot dir must survive GC (it's shared per worktree)"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// `delete_session` should also remove any `<worktree>/.kilo/plans/*.md`
/// authored by `plan_exit` for the deleted session. Cleanup runs on the
/// blocking pool (best-effort, fire-and-forget) so we sleep briefly before
/// asserting. The path is derived from `<created>-<slug>.md` per
/// `agent::parts::plan_path`. Sibling plans for OTHER sessions must
/// survive — only the deleted session's file is touched.
#[tokio::test]
async fn delete_session_removes_plan_markdown_file() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let worktree = std::path::PathBuf::from(state.store.paths().directory);
    let plans_dir = worktree.join(".kilo").join("plans");
    std::fs::create_dir_all(&plans_dir).unwrap();
    let plan = plans_dir.join(format!("{}-{}.md", session.time.created, session.slug));
    std::fs::write(&plan, b"# plan\n").unwrap();
    // Sibling plan for another session must survive — guards against an
    // accidental directory-wide wipe.
    let bystander = plans_dir.join("999999-other.md");
    std::fs::write(&bystander, b"# other\n").unwrap();

    let res = delete_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    assert!(!plan.exists(), "plan markdown should be removed");
    assert!(bystander.exists(), "unrelated plan must survive");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn revert_target_after_snapshot_uses_transcript_order_not_message_id_order() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("a.txt"), "before\n").unwrap();
    let snapshot = match crate::snapshot::track(&state.store, &session.project_id) {
        Ok(snapshot) => snapshot,
        Err(_) => {
            let _ = std::fs::remove_dir_all(root);
            return;
        }
    };
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({ "id": "msg_z_user", "role": "user", "snapshot": snapshot }),
                parts: vec![json!({ "type": "text", "text": "change a" })],
            },
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({ "id": "msg_a_assistant", "role": "assistant" }),
                parts: vec![json!({ "type": "text", "text": "done" })],
            },
        )
        .unwrap();
    std::fs::write(repo.join("a.txt"), "after\n").unwrap();

    let res = revert_session(
        State(state.clone()),
        Path(session.id.clone()),
        Json(SessionRevertInput {
            message_id: Some("msg_a_assistant".to_string()),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        std::fs::read_to_string(repo.join("a.txt"))
            .unwrap()
            .replace("\r\n", "\n"),
        "before\n"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn delete_session_cleans_prompt_queue_state() {
    let state = state();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let _queue = state.prompt_queue(&session.id);
    state.cancel_prompt_queue(&session.id);
    assert!(state
        .prompt_queues
        .lock()
        .unwrap()
        .contains_key(&session.id));
    assert!(state
        .prompt_queue_versions
        .lock()
        .unwrap()
        .contains_key(&session.id));

    let res = delete_session(State(state.clone()), Path(session.id.clone())).await;

    assert_eq!(res.status(), StatusCode::OK);
    assert!(!state
        .prompt_queues
        .lock()
        .unwrap()
        .contains_key(&session.id));
    assert!(!state
        .prompt_queue_versions
        .lock()
        .unwrap()
        .contains_key(&session.id));
}

#[tokio::test]
async fn suggestion_accept_dismiss_routes_match_bun_shape() {
    let state = state();
    let (tx, rx) = tokio::sync::oneshot::channel();
    state.suggestions.lock().unwrap().insert(
        "sgt_accept".to_string(),
        PendingSuggestion {
            info: json!({
                "id": "sgt_accept",
                "sessionID": "ses_suggestion",
                "text": "Try this?",
                "actions": [{
                    "label": "Do it",
                    "prompt": "Do it now"
                }],
                "blocking": false,
            }),
            reply: tx,
        },
    );
    let mut bus = state.bus.subscribe();

    let list = suggestion_list(&state);
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["id"], "sgt_accept");
    let res = accept_suggestion(
        State(state.clone()),
        Path("sgt_accept".to_string()),
        Json(json!({ "index": 0 })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(rx.await.unwrap(), SuggestionDecision::Accept(0));
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "suggestion.accepted");
    assert_eq!(event.payload.properties["requestID"], "sgt_accept");
    assert!(suggestion_list(&state).is_empty());

    let res = accept_suggestion(
        State(state.clone()),
        Path("missing".to_string()),
        Json(json!({ "index": 0 })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let (tx, rx) = tokio::sync::oneshot::channel();
    state.suggestions.lock().unwrap().insert(
        "sgt_dismiss".to_string(),
        PendingSuggestion {
            info: json!({
                "id": "sgt_dismiss",
                "sessionID": "ses_suggestion",
                "text": "Skip?",
                "actions": [{ "label": "Skip", "prompt": "skip" }],
            }),
            reply: tx,
        },
    );

    let res = dismiss_suggestion(State(state.clone()), Path("sgt_dismiss".to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(rx.await.unwrap(), SuggestionDecision::Dismiss);
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "suggestion.dismissed");
    assert_eq!(event.payload.properties["requestID"], "sgt_dismiss");
}

#[tokio::test]
async fn viewed_replaces_and_clears_sets() {
    let state = state();

    set_viewed(
        &state,
        SessionViewedInput {
            focused: vec!["ses_a".to_string(), "ses_b".to_string()],
            open: vec!["ses_b".to_string()],
        },
    )
    .await;
    let view = viewed_snapshot(&state).await;
    assert_eq!(view.focused.len(), 2);
    assert!(view.focused.contains("ses_a"));
    assert!(view.open.contains("ses_b"));

    set_viewed(
        &state,
        SessionViewedInput {
            focused: vec!["ses_c".to_string()],
            open: vec![],
        },
    )
    .await;
    let view = viewed_snapshot(&state).await;
    assert_eq!(view.focused.iter().collect::<Vec<_>>(), vec!["ses_c"]);
    assert!(view.open.is_empty());

    set_viewed(&state, SessionViewedInput::default()).await;
    let view = viewed_snapshot(&state).await;
    assert!(view.focused.is_empty());
    assert!(view.open.is_empty());
}

#[test]
fn publish_events_preserves_message_then_part_order() {
    let state = state();
    let mut rx = state.bus.subscribe();

    publish_events(
        &state,
        "/repo".to_string(),
        "global".to_string(),
        vec![
            StoredEvent {
                id: "evt_1".to_string(),
                seq: 1,
                aggregate_id: "ses_test".to_string(),
                event_type: "message.updated.v1".to_string(),
                data: json!({ "sessionID": "ses_test", "info": { "id": "msg_test" } }),
            },
            StoredEvent {
                id: "evt_2".to_string(),
                seq: 2,
                aggregate_id: "ses_test".to_string(),
                event_type: "message.part.updated.v1".to_string(),
                data: json!({ "sessionID": "ses_test", "part": { "id": "prt_test" }, "time": 1 }),
            },
        ],
    );

    // Bun-parity: each store event emits a bus-shape mirror first, then the
    // sync envelope. See `http/sse.rs::publish_events`.
    let first_mirror = rx.try_recv().unwrap().as_global();
    let first_sync = rx.try_recv().unwrap().as_global();
    let second_mirror = rx.try_recv().unwrap().as_global();
    let second_sync = rx.try_recv().unwrap().as_global();

    assert_eq!(first_mirror.payload.kind, "message.updated");
    assert_eq!(first_mirror.payload.properties["info"]["id"], "msg_test");
    assert_eq!(first_sync.payload.kind, "sync");
    assert_eq!(
        first_sync.payload.sync_event.as_ref().unwrap()["type"],
        "message.updated.1"
    );

    assert_eq!(second_mirror.payload.kind, "message.part.updated");
    assert_eq!(second_mirror.payload.properties["part"]["id"], "prt_test");
    assert_eq!(
        second_sync.payload.sync_event.as_ref().unwrap()["type"],
        "message.part.updated.1"
    );
}

/// `POST /session/{id}/init` for an unknown session must return 404 —
/// matching the Bun route at
/// `packages/opencode/src/server/routes/instance/session.ts:320`. The
/// 404 comes from `agent::run_turn` -> `prompt_guarded`'s pre-check, so
/// this test also exercises the `TurnError::NotFound` -> 404 mapping in
/// the route.
#[tokio::test]
async fn init_session_route_returns_404_for_missing_session() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);

    let res = init_session(
        State(state),
        Path("ses_does_not_exist".to_string()),
        Some(Json(SessionInitInput::default())),
    )
    .await;

    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let _ = std::fs::remove_dir_all(root);
}

/// For an existing session, `POST /session/{id}/init` must dispatch
/// through the same agent turn entrypoint as `/message`. With no
/// provider auth configured the route bounces with the standard
/// `UnsupportedProviderError` envelope (Bun parity:
/// `packages/opencode/src/server/middleware/error.ts`). Hitting that
/// error confirms the dispatch path was taken — without it the route
/// would return 200 / the early-NOT_FOUND or skip the provider gate
/// entirely.
#[tokio::test]
async fn init_session_route_dispatches_init_prompt_for_existing_session() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let res = init_session(
        State(state.clone()),
        Path(session.id.clone()),
        Some(Json(SessionInitInput {
            provider_id: Some("anthropic".to_string()),
            model_id: Some("claude-test".to_string()),
            message_id: None,
        })),
    )
    .await;

    // Provider gate fires (Rust sidecar only supports OpenAI OAuth + fake)
    // and the route forwards the named-error envelope. If the route had
    // skipped `agent::run_turn` we'd see 200 or 404 instead.
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(res).await;
    assert_eq!(body["name"], "UnsupportedProviderError");
    assert_eq!(body["data"]["providerID"], "anthropic");
    assert_eq!(body["data"]["modelID"], "claude-test");
    // No message persisted: the provider gate runs before we reach the
    // user-message append, mirroring `routes::prompt::prompt`.
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert!(page.items.is_empty());

    let _ = std::fs::remove_dir_all(root);
}
