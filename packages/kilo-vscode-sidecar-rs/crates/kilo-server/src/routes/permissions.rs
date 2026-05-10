//! Route handlers for /permission*, /question*, and /suggestion*.
//! The list helpers (permission_list, question_list, suggestion_list) live
//! alongside their handlers since no other crate module needs them.

use std::{collections::BTreeSet, sync::Arc};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::GlobalEvent;
use serde_json::{json, Value};

use crate::agent::permission::{
    evaluate_permission, is_protected_info, permission_decision, permission_rules_for_session,
};
use crate::{AppState, PermissionDecision, PermissionRule, QuestionReply, SuggestionDecision};

pub(crate) async fn permissions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(permission_list(&state))
}

pub(crate) async fn reply_permission(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    let decision = match permission_decision(&input) {
        Some(decision) => decision,
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let reply_kind = input.get("reply").cloned().unwrap_or_else(|| json!("once"));

    // On reject: cascade across siblings in the same session. Bun parity
    // (`packages/opencode/src/permission/index.ts:291-301`) — a single
    // user reject must clear every pending entry tied to that session so
    // parallel tool calls don't require N reject clicks.
    if decision == PermissionDecision::Reject {
        let mut map = state.permissions.lock().unwrap();
        let Some(entry) = map.remove(&id) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let session = entry.info["sessionID"].clone();
        let mut siblings: Vec<(String, crate::PendingPermission)> = Vec::new();
        let session_str = session.as_str().unwrap_or_default().to_string();
        if !session_str.is_empty() {
            let keys: Vec<String> = map
                .iter()
                .filter(|(_, sib)| {
                    sib.info
                        .get("sessionID")
                        .and_then(Value::as_str)
                        .is_some_and(|sid| sid == session_str)
                })
                .map(|(id, _)| id.clone())
                .collect();
            for key in keys {
                if let Some(sib) = map.remove(&key) {
                    siblings.push((key, sib));
                }
            }
        }
        drop(map);

        crate::http::sse::publish(
            &state,
            GlobalEvent::bus(
                "permission.replied",
                json!({
                    "sessionID": session.clone(),
                    "requestID": id,
                    "reply": reply_kind,
                }),
            ),
        );
        let _ = entry.reply.send(PermissionDecision::Reject);
        for (sib_id, sib) in siblings {
            crate::http::sse::publish(
                &state,
                GlobalEvent::bus(
                    "permission.replied",
                    json!({
                        "sessionID": sib.info["sessionID"].clone(),
                        "requestID": sib_id,
                        "reply": "reject",
                    }),
                ),
            );
            let _ = sib.reply.send(PermissionDecision::Reject);
        }
        return Json(true).into_response();
    }

    if let Some(entry) = state.permissions.lock().unwrap().remove(&id) {
        crate::http::sse::publish(
            &state,
            GlobalEvent::bus(
                "permission.replied",
                json!({
                    "sessionID": entry.info["sessionID"].clone(),
                    "requestID": id,
                    "reply": reply_kind,
                }),
            ),
        );
        let _ = entry.reply.send(decision);
        return Json(true).into_response();
    }

    StatusCode::NOT_FOUND.into_response()
}

pub(crate) async fn permission_rules(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    let entry_info = {
        let map = state.permissions.lock().unwrap();
        let Some(item) = map.get(&id) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        item.info.clone()
    };
    let permission = match entry_info.get("permission").and_then(Value::as_str) {
        Some(value) => value.to_string(),
        None => return StatusCode::NOT_FOUND.into_response(),
    };

    // Bun parity (`permission/index.ts:364`): protected requests cannot
    // persist always-rules. Treat the call as a no-op and let the caller
    // fall back to a plain `once` reply for the originating entry.
    if is_protected_info(&state, &entry_info) {
        return Json(true).into_response();
    }

    let mut new_rules: Vec<PermissionRule> = Vec::new();
    for pattern in input
        .get("approvedAlways")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        new_rules.push(PermissionRule {
            permission: permission.clone(),
            pattern: pattern.to_string(),
            action: "allow".to_string(),
        });
    }
    for pattern in input
        .get("deniedAlways")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        new_rules.push(PermissionRule {
            permission: permission.clone(),
            pattern: pattern.to_string(),
            action: "deny".to_string(),
        });
    }

    if !new_rules.is_empty() {
        let serialized: Vec<Value> = new_rules
            .iter()
            .filter_map(|r| serde_json::to_value(r).ok())
            .collect();
        if let Err(err) = state.store.append_permission_rules(&serialized) {
            eprintln!("[kilo-server] failed to persist permission rules: {err}; in-memory only");
        }
        state.approvals.lock().unwrap().extend(new_rules.clone());
    }

    // Bun parity: `kilocode/permission/drain.ts::drainCovered` —
    // re-evaluate every pending entry against the new rules. If all of
    // its patterns are covered with `allow`, resolve as `once`; if any is
    // covered with `deny`, reject. The originating entry is excluded
    // because Bun's flow leaves it for the follow-up `once` reply that
    // arrives next from the UI.
    drain_covered(&state, &id);

    Json(true).into_response()
}

/// Bun parity: `drainCovered` (`kilocode/permission/drain.ts:18-56`).
/// Walk the pending map and resolve any entry whose patterns are now
/// fully covered by the merged ruleset. `exclude` keeps the origin entry
/// untouched (saveAlwaysRules / allow-everything need this so the UI's
/// follow-up "once" reply still has its target).
fn drain_covered(state: &Arc<AppState>, exclude: &str) {
    let candidates: Vec<(String, crate::PendingPermission)> = {
        let mut map = state.permissions.lock().unwrap();
        let keys: Vec<String> = map
            .iter()
            .filter(|(id, _)| id.as_str() != exclude)
            .filter(|(_, item)| !is_protected_info(state, &item.info))
            .filter_map(|(id, item)| {
                let sid = item.info.get("sessionID").and_then(Value::as_str)?;
                let permission = item.info.get("permission").and_then(Value::as_str)?;
                let patterns: Vec<String> = item
                    .info
                    .get("patterns")
                    .and_then(Value::as_array)?
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect();
                let merged = permission_rules_for_session(state, sid);
                let mut denied = false;
                let mut all_allow = !patterns.is_empty();
                for pattern in &patterns {
                    let action = evaluate_permission(permission, pattern, &merged).action;
                    if action == "deny" {
                        denied = true;
                        all_allow = false;
                        break;
                    }
                    if action != "allow" {
                        all_allow = false;
                    }
                }
                if denied || all_allow {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        keys.into_iter()
            .filter_map(|id| map.remove(&id).map(|entry| (id, entry)))
            .collect()
    };

    for (id, entry) in candidates {
        let permission = entry
            .info
            .get("permission")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let sid = entry
            .info
            .get("sessionID")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let patterns: Vec<String> = entry
            .info
            .get("patterns")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let merged = permission_rules_for_session(state, sid);
        let denied = patterns
            .iter()
            .any(|pat| evaluate_permission(permission, pat, &merged).action == "deny");
        let reply = if denied { "reject" } else { "once" };
        crate::http::sse::publish(
            state,
            GlobalEvent::bus(
                "permission.replied",
                json!({
                    "sessionID": entry.info["sessionID"].clone(),
                    "requestID": id,
                    "reply": reply,
                }),
            ),
        );
        let _ = entry.reply.send(if denied {
            PermissionDecision::Reject
        } else {
            PermissionDecision::Allow
        });
    }
}

/// `POST /permission/allow-everything`. Bun shape (`kilocode/permission/
/// routes.ts:14-86`): `{ enable, sessionID?, requestID? }`. When
/// `sessionID` is present, the wildcard `{permission:"*",pattern:"*",
/// action:"allow"}` rule is scoped to that session via
/// `state.store.update_session({permission: ...})`. Otherwise it lands in
/// the global `state.approvals` Vec. Either path triggers a drain pass
/// that clears any pending requests now covered by the new rule.
pub(crate) async fn allow_everything(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    let enable = input
        .get("enable")
        .and_then(Value::as_bool)
        .or_else(|| input.get("allow").and_then(Value::as_bool))
        .unwrap_or(true);
    let session_id = input
        .get("sessionID")
        .or_else(|| input.get("session_id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let request_id = input
        .get("requestID")
        .or_else(|| input.get("request_id"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let wildcard = PermissionRule {
        permission: "*".to_string(),
        pattern: "*".to_string(),
        action: "allow".to_string(),
    };

    if !enable {
        if let Some(sid) = session_id.as_deref() {
            if let Some(session) = state.store.session(sid) {
                let filtered = filter_session_permission(session.permission.as_ref());
                let _ = state.store.update_session(
                    sid,
                    kilo_protocol::SessionUpdateInput {
                        permission: Some(filtered),
                        ..Default::default()
                    },
                );
            }
            return Json(true).into_response();
        }
        // Drop any wildcard from the global approvals (Bun: `findLastIndex`
        // matching the same shape).
        let mut approvals = state.approvals.lock().unwrap();
        if let Some(idx) = approvals
            .iter()
            .rposition(|r| r.permission == "*" && r.pattern == "*" && r.action == "allow")
        {
            approvals.remove(idx);
        }
        return Json(true).into_response();
    }

    if let Some(sid) = session_id.as_deref() {
        let existing = state
            .store
            .session(sid)
            .and_then(|s| s.permission)
            .unwrap_or_else(|| json!([]));
        let merged = append_session_rule(existing, &wildcard);
        if state
            .store
            .update_session(
                sid,
                kilo_protocol::SessionUpdateInput {
                    permission: Some(merged),
                    ..Default::default()
                },
            )
            .ok()
            .flatten()
            .is_none()
        {
            return StatusCode::NOT_FOUND.into_response();
        }
    } else {
        let serialized = serde_json::to_value(&wildcard)
            .ok()
            .into_iter()
            .collect::<Vec<_>>();
        if let Err(err) = state.store.append_permission_rules(&serialized) {
            eprintln!("[kilo-server] failed to persist allow-everything rule: {err}");
        }
        state.approvals.lock().unwrap().push(wildcard);
    }

    // Drain pending entries covered by the new rule. Excluding the
    // origin requestID (if any) matches Bun's `allowEverything` flow,
    // which resolves the origin separately as `once`.
    let exclude = request_id.as_deref().unwrap_or("");
    drain_for_session(&state, exclude, session_id.as_deref());

    Json(true).into_response()
}

/// Drain pending entries covered by current rules. Mirrors `drain_covered`
/// but scoped to a single session when `session_id` is `Some`. The
/// `exclude` parameter skips the originating request (if any) so the UI
/// can resolve it as `once` separately.
fn drain_for_session(state: &Arc<AppState>, exclude: &str, session_id: Option<&str>) {
    let candidates: Vec<(String, crate::PendingPermission)> = {
        let mut map = state.permissions.lock().unwrap();
        let keys: Vec<String> = map
            .iter()
            .filter(|(id, _)| id.as_str() != exclude)
            .filter(|(_, item)| !is_protected_info(state, &item.info))
            .filter(|(_, item)| match session_id {
                Some(sid) => item
                    .info
                    .get("sessionID")
                    .and_then(Value::as_str)
                    .is_some_and(|item_sid| item_sid == sid),
                None => true,
            })
            .filter_map(|(id, item)| {
                let sid = item.info.get("sessionID").and_then(Value::as_str)?;
                let permission = item.info.get("permission").and_then(Value::as_str)?;
                let patterns: Vec<String> = item
                    .info
                    .get("patterns")
                    .and_then(Value::as_array)?
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect();
                let merged = permission_rules_for_session(state, sid);
                let mut denied = false;
                let mut all_allow = !patterns.is_empty();
                for pattern in &patterns {
                    let action = evaluate_permission(permission, pattern, &merged).action;
                    if action == "deny" {
                        denied = true;
                        all_allow = false;
                        break;
                    }
                    if action != "allow" {
                        all_allow = false;
                    }
                }
                if denied || all_allow {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        keys.into_iter()
            .filter_map(|id| map.remove(&id).map(|entry| (id, entry)))
            .collect()
    };

    for (id, entry) in candidates {
        let permission = entry
            .info
            .get("permission")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let sid = entry
            .info
            .get("sessionID")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let patterns: Vec<String> = entry
            .info
            .get("patterns")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let merged = permission_rules_for_session(state, sid);
        let denied = patterns
            .iter()
            .any(|pat| evaluate_permission(permission, pat, &merged).action == "deny");
        let reply = if denied { "reject" } else { "once" };
        crate::http::sse::publish(
            state,
            GlobalEvent::bus(
                "permission.replied",
                json!({
                    "sessionID": entry.info["sessionID"].clone(),
                    "requestID": id,
                    "reply": reply,
                }),
            ),
        );
        let _ = entry.reply.send(if denied {
            PermissionDecision::Reject
        } else {
            PermissionDecision::Allow
        });
    }
}

/// Append a wildcard rule to a session's `permission` value. Sessions can
/// store rules either as an object map (`{edit: "allow"}`) or an array of
/// `{permission, pattern, action}` records — Bun normalizes to the array
/// form for new rules; we do the same here, keeping any pre-existing
/// shape intact.
fn append_session_rule(existing: Value, rule: &PermissionRule) -> Value {
    let rule_value = serde_json::to_value(rule).unwrap_or(Value::Null);
    match existing {
        Value::Array(mut items) => {
            items.push(rule_value);
            Value::Array(items)
        }
        Value::Object(_) => {
            // Convert legacy object form into a single-rule array. The
            // wildcard supersedes anything in the object form anyway.
            Value::Array(vec![existing, rule_value])
        }
        _ => Value::Array(vec![rule_value]),
    }
}

/// Inverse of `append_session_rule`: remove the all-wildcard rule from
/// the session's permission value (used by `enable=false`).
fn filter_session_permission(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Array(items)) => Value::Array(
            items
                .iter()
                .filter(|item| {
                    !(item.get("permission").and_then(Value::as_str) == Some("*")
                        && item.get("pattern").and_then(Value::as_str) == Some("*")
                        && item.get("action").and_then(Value::as_str) == Some("allow"))
                })
                .cloned()
                .collect(),
        ),
        Some(other) => other.clone(),
        None => Value::Array(Vec::new()),
    }
}

pub(crate) async fn questions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(question_list(&state))
}

pub(crate) async fn reply_question(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    let Some(entry) = state.questions.lock().unwrap().remove(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let answers = input.get("answers").cloned().unwrap_or_else(|| json!([]));
    let session = entry.info.get("sessionID").cloned().unwrap_or(Value::Null);
    let _ = entry
        .reply
        .send(crate::QuestionReply::Answers(answers.clone()));
    crate::http::sse::publish(
        &state,
        GlobalEvent::bus(
            "question.replied",
            json!({
                "sessionID": session,
                "requestID": id,
                "answers": answers,
            }),
        ),
    );
    Json(true).into_response()
}

pub(crate) async fn reject_question(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(entry) = state.questions.lock().unwrap().remove(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let session = entry.info.get("sessionID").cloned().unwrap_or(Value::Null);
    let _ = entry.reply.send(crate::QuestionReply::Rejected);
    crate::http::sse::publish(
        &state,
        GlobalEvent::bus(
            "question.rejected",
            json!({
                "sessionID": session,
                "requestID": id,
            }),
        ),
    );
    Json(true).into_response()
}

pub(crate) async fn suggestions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(suggestion_list(&state))
}

pub(crate) fn reject_pending_for_sessions(state: &AppState, sessions: &[String]) {
    let ids: BTreeSet<&str> = sessions.iter().map(String::as_str).collect();
    let perms = {
        let mut items = state.permissions.lock().unwrap();
        let keys = items
            .iter()
            .filter_map(|(id, entry)| session_matches(&entry.info, &ids).then(|| id.clone()))
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|id| items.remove(&id).map(|entry| (id, entry)))
            .collect::<Vec<_>>()
    };
    for (id, entry) in perms {
        crate::http::sse::publish(
            state,
            GlobalEvent::bus(
                "permission.replied",
                json!({
                    "sessionID": entry.info["sessionID"].clone(),
                    "requestID": id,
                    "reply": "reject",
                }),
            ),
        );
        let _ = entry.reply.send(PermissionDecision::Reject);
    }

    dismiss_question_suggestion_waits(state, sessions);
}

pub(crate) fn dismiss_question_suggestion_waits(state: &AppState, ids: &[String]) {
    let ids: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
    let questions = {
        let mut items = state.questions.lock().unwrap();
        let keys = items
            .iter()
            .filter_map(|(id, entry)| session_matches(&entry.info, &ids).then(|| id.clone()))
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|id| items.remove(&id).map(|entry| (id, entry)))
            .collect::<Vec<_>>()
    };
    for (id, entry) in questions {
        let session = entry.info.get("sessionID").cloned().unwrap_or(Value::Null);
        crate::http::sse::publish(
            state,
            GlobalEvent::bus(
                "question.rejected",
                json!({
                    "sessionID": session,
                    "requestID": id,
                }),
            ),
        );
        let _ = entry.reply.send(QuestionReply::Rejected);
    }

    let suggestions = {
        let mut items = state.suggestions.lock().unwrap();
        let keys = items
            .iter()
            .filter_map(|(id, entry)| session_matches(&entry.info, &ids).then(|| id.clone()))
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|id| items.remove(&id).map(|entry| (id, entry)))
            .collect::<Vec<_>>()
    };
    for (id, entry) in suggestions {
        crate::http::sse::publish(
            state,
            GlobalEvent::bus(
                "suggestion.dismissed",
                json!({
                    "sessionID": entry.info["sessionID"].clone(),
                    "requestID": id,
                }),
            ),
        );
        let _ = entry.reply.send(SuggestionDecision::Dismiss);
    }
}

pub(crate) async fn accept_suggestion(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    let Some(index) = input
        .get("index")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(entry) = state.suggestions.lock().unwrap().remove(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(action) = entry
        .info
        .get("actions")
        .and_then(Value::as_array)
        .and_then(|items| items.get(index))
        .cloned()
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let session = entry.info["sessionID"].clone();
    crate::http::sse::publish(
        &state,
        GlobalEvent::bus(
            "suggestion.accepted",
            json!({
                "sessionID": session,
                "requestID": id,
                "index": index,
                "action": action,
            }),
        ),
    );
    let _ = entry.reply.send(SuggestionDecision::Accept(index));

    Json(true).into_response()
}

pub(crate) async fn dismiss_suggestion(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(entry) = state.suggestions.lock().unwrap().remove(&id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    crate::http::sse::publish(
        &state,
        GlobalEvent::bus(
            "suggestion.dismissed",
            json!({
                "sessionID": entry.info["sessionID"].clone(),
                "requestID": id,
            }),
        ),
    );
    let _ = entry.reply.send(SuggestionDecision::Dismiss);

    Json(true).into_response()
}

pub(crate) fn permission_list(state: &AppState) -> Vec<Value> {
    state
        .permissions
        .lock()
        .unwrap()
        .values()
        .map(|item| item.info.clone())
        .collect()
}

pub(crate) fn question_list(state: &AppState) -> Vec<Value> {
    state
        .questions
        .lock()
        .unwrap()
        .values()
        .map(|entry| entry.info.clone())
        .collect()
}

pub(crate) fn suggestion_list(state: &AppState) -> Vec<Value> {
    state
        .suggestions
        .lock()
        .unwrap()
        .values()
        .map(|item| item.info.clone())
        .collect()
}

fn session_matches(info: &Value, ids: &BTreeSet<&str>) -> bool {
    info.get("sessionID")
        .and_then(Value::as_str)
        .is_some_and(|id| ids.contains(id))
}
