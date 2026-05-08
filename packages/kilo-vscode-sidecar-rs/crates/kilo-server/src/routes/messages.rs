//! Route handlers for the per-session message and part REST surface.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value;

use crate::util::cursor::{decode_cursor, encode_cursor};
use crate::{internal_error, publish_for_session, AppState};

pub(crate) async fn messages(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let limit = query.get("limit").and_then(|value| value.parse().ok());
    let before = match query.get("before") {
        Some(_) if limit.is_none() => {
            return (StatusCode::BAD_REQUEST, "before requires limit").into_response()
        }
        Some(value) => match decode_cursor(value) {
            Some(cursor) => Some(cursor),
            None => return (StatusCode::BAD_REQUEST, "Invalid cursor").into_response(),
        },
        None => None,
    };
    match state.store.messages(&id, limit, before.as_ref()) {
        Some(page) => {
            let mut res = Json(page.items).into_response();
            if let Some(cursor) = page
                .more
                .then(|| page.cursor)
                .flatten()
                .and_then(|cursor| encode_cursor(&cursor))
            {
                let link = format!(
                    "</session/{id}/message?limit={}&before={cursor}>; rel=\"next\"",
                    limit.unwrap_or(0)
                );
                let headers = res.headers_mut();
                headers.insert(
                    header::ACCESS_CONTROL_EXPOSE_HEADERS,
                    HeaderValue::from_static("Link, X-Next-Cursor"),
                );
                if let Ok(value) = HeaderValue::from_str(&link) {
                    headers.insert(header::LINK, value);
                }
                if let Ok(value) = HeaderValue::from_str(&cursor) {
                    headers.insert("x-next-cursor", value);
                }
            }

            res
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(crate) async fn message(
    State(state): State<Arc<AppState>>,
    Path((id, mid)): Path<(String, String)>,
) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.store.message(&id, &mid) {
        Some(message) => Json(message).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(crate) async fn delete_message(
    State(state): State<Arc<AppState>>,
    Path((id, mid)): Path<(String, String)>,
) -> Response {
    match state.store.remove_message_record(&id, &mid) {
        Ok(record) if record.message.is_some() => {
            publish_for_session(&state, &id, record.events);
            Json(true).into_response()
        }
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn delete_part(
    State(state): State<Arc<AppState>>,
    Path((id, mid, pid)): Path<(String, String, String)>,
) -> Response {
    match state.store.remove_part_record(&id, &mid, &pid) {
        Ok(record) if record.part.is_some() => {
            publish_for_session(&state, &id, record.events);
            Json(true).into_response()
        }
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn update_part(
    State(state): State<Arc<AppState>>,
    Path((id, mid, pid)): Path<(String, String, String)>,
    Json(input): Json<Value>,
) -> Response {
    match state.store.update_part_record(&id, &mid, &pid, input) {
        Ok(record) => {
            publish_for_session(&state, &id, record.events);
            Json(record.part.unwrap_or(Value::Null)).into_response()
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(rusqlite::Error::InvalidParameterName(_)) => StatusCode::BAD_REQUEST.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}
