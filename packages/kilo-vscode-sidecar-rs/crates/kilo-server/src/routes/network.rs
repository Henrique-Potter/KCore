//! Route handlers for /network*.
//!
//! Bun exposes these endpoints so clients can drain offline reconnect waits
//! before destructive operations such as config save. The Rust sidecar does
//! not currently pause turns on network waits, so the compatible state is an
//! empty list plus 404 for unknown wait ids.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value;

pub(crate) async fn network_waits() -> impl IntoResponse {
    Json(Vec::<Value>::new())
}

pub(crate) async fn reply_network_wait(Path(_id): Path<String>) -> Response {
    StatusCode::NOT_FOUND.into_response()
}

pub(crate) async fn reject_network_wait(Path(_id): Path<String>) -> Response {
    StatusCode::NOT_FOUND.into_response()
}
