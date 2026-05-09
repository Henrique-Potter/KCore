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
    children, diff_session, fork_session, revert_session, set_viewed, share_session,
    summarize_session, todos, unrevert_session, unshare_session, update_session, viewed_snapshot,
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
    let res = summarize_session(State(state.clone()), Path(session.id.clone())).await;
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
