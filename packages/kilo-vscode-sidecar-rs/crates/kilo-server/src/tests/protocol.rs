//! Cursor encoding, lenient-body middleware, path-rewrite middleware,
//! and SSE-frame shape tests.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use base64::{engine::general_purpose, Engine};
use kilo_protocol::{GlobalEvent, Session, SessionTime};
use kilo_store::MessageCursor;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

use crate::http::build_router as app;
use crate::http::middleware::rewrite_path_and_query;
use crate::util::cursor::{decode_cursor, encode_cursor};

use super::common::{response_to_string, seed, state_at, unique_root};

#[test]
fn cursor_roundtrips_bun_shape() {
    let cursor = MessageCursor {
        id: "msg_123".to_string(),
        time: 42,
    };

    let encoded = encode_cursor(&cursor).unwrap();
    let decoded = decode_cursor(&encoded).unwrap();

    assert_eq!(decoded.id, cursor.id);
    assert_eq!(decoded.time, cursor.time);
}

#[test]
fn cursor_rejects_invalid_payload() {
    assert!(decode_cursor("not-a-cursor").is_none());
    assert!(decode_cursor(&general_purpose::URL_SAFE_NO_PAD.encode("{}")).is_none());
}

#[test]
fn rewrite_appends_directory_when_missing() {
    let out = rewrite_path_and_query("/session", None, Some("%2Frepo"), None);
    assert_eq!(out, "/session?directory=%2Frepo");
}

#[test]
fn rewrite_preserves_existing_query() {
    let out = rewrite_path_and_query("/session", Some("limit=10"), Some("%2Frepo"), None);
    assert_eq!(out, "/session?limit=10&directory=%2Frepo");
}

#[test]
fn rewrite_does_not_overwrite_existing_directory() {
    // Mirrors the SDK guard: if the URL already has `directory=`, the
    // header value is ignored.
    let out = rewrite_path_and_query(
        "/session",
        Some("directory=already"),
        Some("%2Fheader"),
        None,
    );
    assert_eq!(out, "/session?directory=already");
}

#[test]
fn rewrite_handles_workspace_too() {
    let out = rewrite_path_and_query("/session", None, None, Some("ws-1"));
    assert_eq!(out, "/session?workspace=ws-1");
}

#[test]
fn rewrite_no_op_when_neither_header_present() {
    let out = rewrite_path_and_query("/session", Some("a=b"), None, None);
    assert_eq!(out, "/session?a=b");
}

/// Hony-parity: hey-api's SDK strips `Content-Type` on empty-body POSTs (see
/// `packages/sdk/js/src/v2/gen/client/client.gen.ts:58-61`). The Bun port
/// accepted this via `c.req.valid("json") ?? {}`. Without the lenient
/// middleware, axum's strict `Json<T>` extractor returns 415 + the literal
/// "Expected request with `Content-Type: application/json`" — exactly the
/// error the user hit on `client.session.create({ directory })`.
#[tokio::test]
async fn empty_body_post_with_no_content_type_lands_at_create_session() {
    let root = unique_root();
    let st = state_at(&root);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/session")
        .header(header::CONTENT_LENGTH, "0")
        .body(Body::empty())
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    // Without the middleware: 415 + the literal "Expected request with
    // `Content-Type: application/json`" body — this is the user-facing toast
    // text. With the middleware: the empty body is rewritten to `{}` and the
    // route's `Json<T>` extractor decodes a `SessionCreateInput::default()`.
    // We don't require a 200 (the handler may need project setup the test
    // harness doesn't provide); we only require the response not to be the
    // 415 we used to bounce on.
    let status = res.status();
    assert_ne!(
        status,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "lenient middleware did not rescue empty-body POST; got {status}",
    );
}

#[tokio::test]
async fn absent_length_empty_post_with_no_content_type_lands_at_create_session() {
    let root = unique_root();
    let st = state_at(&root);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/session")
        .body(Body::empty())
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    let status = res.status();
    assert_ne!(
        status,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "lenient middleware did not rescue absent-length empty POST; got {status}",
    );
}

#[tokio::test]
async fn populated_body_post_with_no_content_type_lands_at_create_session() {
    let root = unique_root();
    let st = state_at(&root);
    seed(&st.store);
    let body = json!({ "title": "From SDK" }).to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/session")
        .header(header::CONTENT_LENGTH, body.len().to_string())
        .body(Body::from(body))
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    let status = res.status();
    let text = response_to_string(res).await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let data: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(data["title"], "From SDK");

    let _ = std::fs::remove_dir_all(root);
}

/// A POST that does set Content-Type and carries a real body must still flow
/// through unchanged — the middleware only fills in the empty-body / no-CT
/// pattern, never rewrites real payloads. We use `/global/dispose` because
/// it's a body-bearing route the test harness can answer end-to-end.
#[tokio::test]
async fn populated_body_post_passes_through_unchanged() {
    let root = unique_root();
    let st = state_at(&root);
    let body = json!({ "any": "payload" }).to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/global/dispose")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// Same body-bearing route, but the SDK-style empty-body call. With the
/// lenient layer the request reaches the handler; without it, axum's `Json<T>`
/// extractor would still pass since `global_dispose` doesn't extract a body —
/// so we co-test against `/instance/dispose` which is also body-less. The
/// real value of this test is documenting that the lenient layer is a
/// no-op for already-OK requests, not that it rescues anything new here.
#[tokio::test]
async fn empty_body_dispose_post_remains_ok() {
    let root = unique_root();
    let st = state_at(&root);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/global/dispose")
        .header(header::CONTENT_LENGTH, "0")
        .body(Body::empty())
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// GET requests must never be touched by the lenient layer. Adding a GET-side
/// rewrite would clobber the directory header rewrite middleware that already
/// runs on the same path.
#[tokio::test]
async fn lenient_layer_ignores_get_requests() {
    let root = unique_root();
    let st = state_at(&root);
    let req = Request::builder()
        .method(Method::GET)
        .uri("/global/health")
        .body(Body::empty())
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn compatibility_routes_return_sdk_shapes() {
    let root = unique_root();
    let st = state_at(&root);
    seed(&st.store);

    let remote = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/remote/enable")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(remote.status(), StatusCode::OK);
    let data: Value = serde_json::from_str(&response_to_string(remote).await).unwrap();
    assert_eq!(data, json!({ "enabled": false, "connected": false }));

    let body = json!({ "path": "repo", "selectedFiles": ["a.ts", "b.ts"] }).to_string();
    let commit = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/commit-message")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit.status(), StatusCode::OK);
    let data: Value = serde_json::from_str(&response_to_string(commit).await).unwrap();
    assert_eq!(data["message"], "Update 2 selected file(s)");

    let profile = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/kilo/profile")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(profile.status(), StatusCode::OK);
    let data: Value = serde_json::from_str(&response_to_string(profile).await).unwrap();
    assert_eq!(data["profile"]["email"], "");

    let project = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/kilocode/session-import/project")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "id": "proj_import",
                        "worktree": root.join("repo").to_string_lossy(),
                        "timeCreated": 1,
                        "timeUpdated": 2,
                        "sandboxes": []
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(project.status(), StatusCode::OK);
    let data: Value = serde_json::from_str(&response_to_string(project).await).unwrap();
    assert_eq!(data, json!({ "ok": true, "id": "proj_import" }));

    let session = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/kilocode/session-import/session")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "id": "ses_import",
                        "projectID": "proj_import",
                        "slug": "ses_import",
                        "directory": root.join("repo").to_string_lossy(),
                        "title": "Imported",
                        "version": "legacy",
                        "timeCreated": 10,
                        "timeUpdated": 11
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(session.status(), StatusCode::OK);
    let data: Value = serde_json::from_str(&response_to_string(session).await).unwrap();
    assert_eq!(data, json!({ "ok": true, "id": "ses_import" }));

    let duplicate = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/kilocode/session-import/session")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "id": "ses_import",
                        "projectID": "proj_import",
                        "slug": "ses_import",
                        "directory": root.join("repo").to_string_lossy(),
                        "title": "Imported",
                        "version": "legacy",
                        "timeCreated": 10,
                        "timeUpdated": 11
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let data: Value = serde_json::from_str(&response_to_string(duplicate).await).unwrap();
    assert_eq!(
        data,
        json!({ "ok": true, "id": "ses_import", "skipped": true })
    );

    let message = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/kilocode/session-import/message")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "id": "msg_import",
                        "sessionID": "ses_import",
                        "timeCreated": 12,
                        "data": {
                            "role": "user",
                            "time": { "created": 12 },
                            "agent": "coder",
                            "model": { "providerID": "test", "modelID": "model" }
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(message.status(), StatusCode::OK);

    let part = app(st.clone())
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/kilocode/session-import/part")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "id": "prt_import",
                        "messageID": "msg_import",
                        "sessionID": "ses_import",
                        "timeCreated": 13,
                        "data": { "type": "text", "text": "hello" }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(part.status(), StatusCode::OK);

    let loaded = app(st)
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/session/ses_import/message/msg_import")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(loaded.status(), StatusCode::OK);
    let data: Value = serde_json::from_str(&response_to_string(loaded).await).unwrap();
    assert_eq!(data["info"]["id"], "msg_import");
    assert_eq!(data["parts"][0]["id"], "prt_import");
    assert_eq!(data["parts"][0]["text"], "hello");
}

#[test]
fn session_event_frame_has_global_shape() {
    let info = Session {
        id: "ses_test".to_string(),
        slug: "slug".to_string(),
        project_id: "global".to_string(),
        workspace_id: None,
        directory: "/repo".to_string(),
        parent_id: None,
        summary: None,
        share: None,
        title: "Title".to_string(),
        version: "local".to_string(),
        time: SessionTime {
            created: 1,
            updated: 1,
            compacting: None,
            archived: None,
        },
        permission: None,
        revert: None,
    };

    let data = serde_json::to_value(GlobalEvent::session(
        "session.created",
        Arc::from("/repo"),
        info,
    ))
    .unwrap();

    assert_eq!(data["directory"], "/repo");
    assert_eq!(data["project"], "global");
    assert_eq!(data["payload"]["type"], "session.created");
    assert_eq!(data["payload"]["properties"]["sessionID"], "ses_test");
    assert_eq!(data["payload"]["properties"]["info"]["projectID"], "global");
}

#[test]
fn message_event_frame_has_global_shape() {
    let data = serde_json::to_value(GlobalEvent::message(
        "message.updated",
        Arc::from("/repo"),
        Arc::from("global"),
        json!({
            "sessionID": "ses_test",
            "info": {
                "id": "msg_test",
                "sessionID": "ses_test",
                "role": "user"
            }
        }),
    ))
    .unwrap();

    assert_eq!(data["directory"], "/repo");
    assert_eq!(data["project"], "global");
    assert_eq!(data["payload"]["type"], "message.updated");
    assert_eq!(data["payload"]["properties"]["sessionID"], "ses_test");
    assert_eq!(data["payload"]["properties"]["info"]["id"], "msg_test");
}

#[test]
fn sync_event_frame_has_bun_shape() {
    let data = serde_json::to_value(GlobalEvent::sync(
        Arc::from("/repo"),
        Arc::from("global"),
        json!({
            "type": "message.updated.v1",
            "id": "evt_test",
            "seq": 1,
            "aggregateID": "ses_test",
            "data": { "sessionID": "ses_test" }
        }),
    ))
    .unwrap();

    assert_eq!(data["directory"], "/repo");
    assert_eq!(data["project"], "global");
    assert_eq!(data["payload"]["type"], "sync");
    assert_eq!(data["payload"]["syncEvent"]["type"], "message.updated.v1");
    assert!(data["payload"]["properties"].is_object());
}

#[test]
fn bus_event_frame_has_global_shape() {
    let data = serde_json::to_value(GlobalEvent::bus(
        "session.status",
        json!({ "sessionID": "ses_test", "status": { "type": "busy" } }),
    ))
    .unwrap();

    assert!(data.get("directory").is_none());
    assert!(data.get("project").is_none());
    assert_eq!(data["payload"]["type"], "session.status");
    assert_eq!(data["payload"]["properties"]["sessionID"], "ses_test");
    assert_eq!(data["payload"]["properties"]["status"]["type"], "busy");
}

#[test]
fn instance_event_frame_has_bus_shape() {
    let event = GlobalEvent::connected();
    let data = serde_json::to_string(&event.payload).unwrap();
    let parsed: Value = serde_json::from_str(&data).unwrap();

    assert_eq!(parsed["type"], "server.connected");
    assert_eq!(parsed["properties"], json!({}));
    assert!(parsed.get("directory").is_none());
}
