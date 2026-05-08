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

use crate::agent::permission::permission_decision;
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
    if let Some(entry) = state.permissions.lock().unwrap().remove(&id) {
        crate::http::sse::publish(
            &state,
            GlobalEvent::bus(
                "permission.replied",
                json!({
                    "sessionID": entry.info["sessionID"].clone(),
                    "requestID": id,
                    "reply": input.get("reply").cloned().unwrap_or_else(|| json!("once")),
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
    let entry = state.permissions.lock().unwrap();
    let Some(permission) = entry.get(&id).and_then(|item| {
        item.info
            .get("permission")
            .and_then(Value::as_str)
            .map(str::to_string)
    }) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    drop(entry);

    let mut rules = state.approvals.lock().unwrap();
    for pattern in input
        .get("approvedAlways")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        rules.push(PermissionRule {
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
        rules.push(PermissionRule {
            permission: permission.clone(),
            pattern: pattern.to_string(),
            action: "deny".to_string(),
        });
    }

    if state.permissions.lock().unwrap().contains_key(&id) {
        return Json(true).into_response();
    }

    StatusCode::NOT_FOUND.into_response()
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

pub(crate) fn reject_pending_for_sessions(state: &AppState, ids: &[String]) {
    let ids: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
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
