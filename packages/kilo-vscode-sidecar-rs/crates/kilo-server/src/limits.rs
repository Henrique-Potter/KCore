//! Resource-limit constants and route-layer enforcement helpers.
//!
//! Per **Operational invariants → 5** of the migration plan: every limit
//! lives in this module so changes show up in code review and the oracle
//! can assert them. Each over-limit response uses a stable
//! lower-snake-case `name` field in the same `NamedError` envelope that
//! `error.rs` already produces, so the SDK error type union can tag them
//! reliably across releases.
//!
//! Allowed limit names (load-bearing — keep in sync with
//! [`CONTRACT.md`](../../../CONTRACT.md)):
//!
//! | name | trigger | status |
//! |---|---|---|
//! | `request_too_large` | request body > [`MAX_REQUEST_BODY_BYTES`] | 413 |
//! | `sse_capacity_exceeded` | concurrent SSE clients > [`MAX_SSE_CLIENTS`] | 503 |
//! | `session_quota_exceeded` | sessions in workspace > [`MAX_SESSIONS_PER_WORKSPACE`] | 507 |
//! | `message_part_too_large` | single message part > [`MAX_MESSAGE_PART_BYTES`] | 413 |
//! | `tool_output_truncated` | tool output > [`MAX_TOOL_OUTPUT_BYTES`] | not surfaced — sentinel inside output |

use axum::{http::StatusCode, response::Response};

use crate::error::named_response;

/// 16 MiB. Applied via [`axum::extract::DefaultBodyLimit::max`] in
/// [`http::build_router`](super::http::build_router). Bun's request handler
/// has no explicit limit but is bounded by the runtime's default; this caps
/// pathological payloads (e.g. accidental binary upload through the file
/// route) before they reach a deserializer.
pub(crate) const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// 32 per workspace. Enforced by the SSE acquire path in
/// [`crate::http::sse`] using an `Arc<Semaphore>` on `AppState`. Beyond this
/// the route returns 503 with [`sse_capacity_exceeded`]. Multi-window VS Code
/// + Agent Manager + sidebar all share this pool, so the cap is sized for
/// 6+ concurrent panes per window with headroom.
pub(crate) const MAX_SSE_CLIENTS: usize = 32;

/// 1000 per workspace. Older sessions are archived rather than deleted when
/// the cap trips. The session-create path enforces this; archive/cleanup is
/// a M14 concern. Returns 507 with [`session_quota_exceeded`].
pub(crate) const MAX_SESSIONS_PER_WORKSPACE: usize = 1000;

/// 1 MiB per message part. Tool calls that emit larger payloads are
/// truncated with the [`TRUNCATION_SENTINEL`] before being persisted as a
/// message part. Direct user input over this size is rejected with
/// [`message_part_too_large`].
#[allow(dead_code)]
pub(crate) const MAX_MESSAGE_PART_BYTES: usize = 1024 * 1024;

/// 1 MiB. Captured tool output above this is truncated with
/// [`TRUNCATION_SENTINEL`] appended. Truncation is best-effort and not
/// surfaced as an error — the sentinel is the user-visible signal.
///
/// The `bash` tool has a tighter local cap (`MAX_BASH_OUTPUT_BYTES = 64 KiB`)
/// in [`agent::tools::bash`]; this is the global ceiling that newer tools
/// (write, edit, apply_patch result payloads) MUST stay under.
#[allow(dead_code)]
pub(crate) const MAX_TOOL_OUTPUT_BYTES: usize = 1024 * 1024;

/// Appended to truncated tool output / message-part payloads. Stable string
/// so the UI can recognize and render it.
#[allow(dead_code)]
pub(crate) const TRUNCATION_SENTINEL: &str = "\n…[output truncated by sidecar]\n";

/// Reserved: surface 413 with stable name `request_too_large` from a route
/// that wants to size-check before calling the body extractor. axum's
/// [`DefaultBodyLimit`] surfaces oversize bodies as a generic 413 already,
/// so this helper is for routes that wrap a streaming body or that want
/// the named-error envelope. Kept here so the contract has one home.
#[allow(dead_code)]
pub(crate) fn request_too_large_error(actual_bytes: Option<usize>) -> Response {
    named_status(
        StatusCode::PAYLOAD_TOO_LARGE,
        "request_too_large",
        match actual_bytes {
            Some(n) => {
                format!("Request body of {n} bytes exceeds {MAX_REQUEST_BODY_BYTES} byte limit")
            }
            None => format!("Request body exceeds {MAX_REQUEST_BODY_BYTES} byte limit"),
        },
    )
}

pub(crate) fn sse_capacity_exceeded_error() -> Response {
    named_status(
        StatusCode::SERVICE_UNAVAILABLE,
        "sse_capacity_exceeded",
        format!("SSE client capacity ({MAX_SSE_CLIENTS}) reached; close another stream and retry"),
    )
}

pub(crate) fn session_quota_exceeded_error() -> Response {
    named_status(
        StatusCode::INSUFFICIENT_STORAGE,
        "session_quota_exceeded",
        format!(
            "Workspace has reached the {MAX_SESSIONS_PER_WORKSPACE} session limit; archive older sessions"
        ),
    )
}

/// Reserved: surface 413 with stable name `message_part_too_large` from
/// the message-part write path when callers persist user-supplied content.
/// Wired-up sites: TBD when M8 hardens the message-write path.
#[allow(dead_code)]
pub(crate) fn message_part_too_large_error(actual_bytes: usize) -> Response {
    named_status(
        StatusCode::PAYLOAD_TOO_LARGE,
        "message_part_too_large",
        format!("Message part of {actual_bytes} bytes exceeds {MAX_MESSAGE_PART_BYTES} byte limit"),
    )
}

/// Truncate a UTF-8 string to at most `MAX_TOOL_OUTPUT_BYTES` and append the
/// truncation sentinel if any bytes were dropped. Truncation snaps to a
/// char boundary so we never produce invalid UTF-8.
/// Truncate captured tool-output text to [`MAX_TOOL_OUTPUT_BYTES`] and
/// append [`TRUNCATION_SENTINEL`] if any bytes were dropped. Snaps to the
/// nearest char boundary to keep output valid UTF-8. The bash tool already
/// has a tighter local truncation; this helper exists for the M8/M11 tools
/// that produce large captured payloads.
#[allow(dead_code)]
pub(crate) fn truncate_tool_output(mut output: String) -> String {
    if output.len() <= MAX_TOOL_OUTPUT_BYTES {
        return output;
    }
    let mut cut = MAX_TOOL_OUTPUT_BYTES;
    while !output.is_char_boundary(cut) && cut > 0 {
        cut -= 1;
    }
    output.truncate(cut);
    output.push_str(TRUNCATION_SENTINEL);
    output
}

fn named_status(status: StatusCode, name: &str, message: String) -> Response {
    named_response(status, name, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_tool_output_passes_short_input_through() {
        let s = "hello".to_string();
        assert_eq!(truncate_tool_output(s.clone()), s);
    }

    #[test]
    fn truncate_tool_output_appends_sentinel_when_oversize() {
        let s = "x".repeat(MAX_TOOL_OUTPUT_BYTES + 100);
        let out = truncate_tool_output(s);
        assert!(out.ends_with(TRUNCATION_SENTINEL));
        assert!(out.len() <= MAX_TOOL_OUTPUT_BYTES + TRUNCATION_SENTINEL.len());
    }

    #[test]
    fn truncate_tool_output_snaps_to_char_boundary() {
        let mut s = "x".repeat(MAX_TOOL_OUTPUT_BYTES - 1);
        // Push a 4-byte UTF-8 char that would straddle the cut.
        s.push('🌍');
        s.push_str(&"y".repeat(50));
        let out = truncate_tool_output(s);
        // Must be valid UTF-8 (Rust enforces this on String) and end with
        // sentinel.
        assert!(out.ends_with(TRUNCATION_SENTINEL));
    }

    #[test]
    fn limit_constants_are_sane() {
        assert!(MAX_REQUEST_BODY_BYTES >= MAX_MESSAGE_PART_BYTES);
        assert!(MAX_SESSIONS_PER_WORKSPACE >= MAX_SSE_CLIENTS);
    }
}
