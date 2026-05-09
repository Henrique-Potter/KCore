use axum::{response::IntoResponse, Json};
use serde_json::json;

pub(crate) async fn indexing_status() -> impl IntoResponse {
    Json(json!({
        "state": "Disabled",
        "message": "Semantic indexing is not available in the Rust sidecar yet.",
        "processedFiles": 0,
        "totalFiles": 0,
        "percent": 0,
    }))
}
