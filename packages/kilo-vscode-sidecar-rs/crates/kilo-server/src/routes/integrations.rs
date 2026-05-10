//! SDK-shape-parity stubs for the LSP and formatter status endpoints.
//! The real LSP client and formatter integration are out of scope for
//! the lean OpenAI-Pro target, but the routes need to exist so the SDK
//! doesn't 404 when polling them. Bun reference:
//! `packages/opencode/src/server/routes/instance/index.ts:254` (`GET /lsp`)
//! and `packages/opencode/src/server/routes/instance/index.ts:277`
//! (`GET /formatter`). Each returns an empty array until implemented.

use axum::Json;
use serde_json::Value;

pub(crate) async fn lsp_status() -> Json<Vec<Value>> {
    Json(Vec::new())
}

pub(crate) async fn formatter_status() -> Json<Vec<Value>> {
    Json(Vec::new())
}
