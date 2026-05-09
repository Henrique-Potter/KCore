//! OpenAI Responses API streaming (Codex endpoint).
//!
//! The wire surface is documented at
//! <https://platform.openai.com/docs/api-reference/responses-streaming>.
//! Bun reaches it through the AI SDK's `streamText` transport — see
//! [`session/llm.ts:155-174`](../../../../../opencode/src/session/llm.ts:155).
//! We reach it directly because Rust has no AI SDK equivalent on the
//! OAuth path.
//!
//! Two pieces of behavior matter for product fidelity:
//!
//! 1. **Tool-call dispatch order.** Per the parallel-execution invariant
//!    in the migration plan, the Rust consumer must surface
//!    `tool-call-complete` events as soon as each call is parsed out of
//!    the SSE stream — not at a final drain. The caller wires a
//!    `JoinSet` against [`ResponsesStreamPart::ToolCallComplete`] to get
//!    parallel tool execution. See
//!    [`processor.ts:301-302, 334-335`](../../../../../opencode/src/session/processor.ts:301)
//!    for the Bun reference.
//! 2. **Soul / instructions placement.** The `instructions` field on
//!    the request body carries the Kilo soul prompt prepended to the
//!    system strings — see [`llm.ts:155-159`](../../../../../opencode/src/session/llm.ts:155).
//!    The kilo-server caller composes that string; we just wire it onto
//!    the request.
//!
//! The bytes-level SSE parser already lives in [`crate::parse_stream`]
//! (it powers the existing `stream_openai_oauth` path). This module
//! re-exports it under a richer, AI-SDK-shaped enum so future tool-call
//! dispatch logic can consume parts as they arrive.

use crate::{ChatToolCall, ChatUsage, StreamEvent};
use serde_json::Value;

pub const RESPONSES_PATH: &str = "/responses";
pub const CODEX_ENDPOINT: &str = "https://chatgpt.com/backend-api/codex/responses";

/// A single normalized event from the Responses SSE stream. This is the
/// Rust equivalent of the AI SDK's `streamText` part union. Naming and
/// granularity follow that vocabulary so the kilo-session turn assembler
/// can dispatch tool calls as they arrive (per the parallel-execution
/// invariant).
#[derive(Clone, Debug, PartialEq)]
pub enum ResponsesStreamPart {
    /// `response.output_text.delta` — incremental assistant text.
    TextDelta(String),
    /// `response.output_item.added` with `type=function_call` — the
    /// model committed to a tool call. `id` is `call_id`.
    ToolCallStart { id: String, name: String },
    /// `response.function_call_arguments.delta` — incremental JSON
    /// argument bytes for a previously-started tool call.
    ToolCallArgsDelta { id: String, delta: String },
    /// Either `response.output_item.done` (when the item is a
    /// `function_call`) or `response.function_call_arguments.done` —
    /// the call is fully assembled and ready to dispatch.
    ToolCallComplete {
        id: String,
        name: String,
        args: Value,
    },
    /// Terminal event. Mirrors AI SDK's `finish` part.
    Finish {
        reason: String,
        usage: Option<ChatUsage>,
    },
    /// Either an HTTP-level error or an in-stream `response.failed` /
    /// `response.error` event. Normalized to the Bun NamedError shape
    /// (`{name: "APIError", data: {message}}`) by `error_envelope`.
    Error { message: String },
}

/// Convert a [`StreamEvent`] (the existing `parse_stream` output) into
/// the richer [`ResponsesStreamPart`] vocabulary. Call this in the
/// per-event consumer loop — the existing `parse_stream` already
/// segments text deltas, tool-call deltas, completed tool calls, usage,
/// finish, and error events; we just relabel them.
///
/// `usage_buf` is mutated to remember the most recent `Usage` event so
/// it can be folded into the eventual `Finish` part. The Bun stream
/// emits `usage` as a separate AI-SDK part; in our surface we attach it
/// to `Finish` for symmetry with how the kilo-server `assistant_info`
/// expects to read it.
pub fn classify_event(
    event: StreamEvent,
    usage_buf: &mut Option<ChatUsage>,
) -> Option<ResponsesStreamPart> {
    match event {
        StreamEvent::TextDelta(delta) => Some(ResponsesStreamPart::TextDelta(delta)),
        StreamEvent::ReasoningStart { .. }
        | StreamEvent::ReasoningDelta { .. }
        | StreamEvent::ReasoningEnd { .. } => None,
        StreamEvent::ToolDelta {
            id,
            name: Some(name),
            arguments,
        } if arguments.is_empty() => Some(ResponsesStreamPart::ToolCallStart { id, name }),
        StreamEvent::ToolDelta {
            id,
            name: _,
            arguments,
        } => Some(ResponsesStreamPart::ToolCallArgsDelta {
            id,
            delta: arguments,
        }),
        StreamEvent::ToolCall(ChatToolCall { id, name, input }) => {
            Some(ResponsesStreamPart::ToolCallComplete {
                id,
                name,
                args: input,
            })
        }
        StreamEvent::Usage(usage) => {
            *usage_buf = Some(usage);
            None
        }
        StreamEvent::Finish(reason) => Some(ResponsesStreamPart::Finish {
            reason,
            usage: usage_buf.take(),
        }),
        StreamEvent::Error(message) => Some(ResponsesStreamPart::Error { message }),
    }
}

/// Run a full byte-stream through the SSE parser and return the
/// normalized event sequence. Used directly by the unit tests; the
/// kilo-server runtime path uses `crate::stream_openai_oauth` which
/// emits the underlying [`StreamEvent`]s as they arrive (so tool-call
/// dispatch is incremental, not drain).
pub fn classify_stream(raw: &str) -> Result<Vec<ResponsesStreamPart>, crate::ProviderError> {
    let events = crate::parse_stream(raw)?;
    let mut out = Vec::new();
    let mut usage_buf = None;
    for event in events {
        if let Some(part) = classify_event(event, &mut usage_buf) {
            out.push(part);
        }
    }
    // `Finish` may have already drained `usage_buf`; if it didn't (e.g.
    // because the stream ended without a finish event), we surface the
    // dangling usage as a synthetic Finish so the kilo-server caller
    // can persist token counts.
    if let Some(usage) = usage_buf.take() {
        out.push(ResponsesStreamPart::Finish {
            reason: "stop".to_string(),
            usage: Some(usage),
        });
    }
    Ok(out)
}

/// Build the Bun-compatible NamedError envelope for a Responses API
/// error. Mirrors the M3 fix at
/// [`server/middleware.ts:17-38`](../../../../../opencode/src/server/middleware.ts:17).
pub fn error_envelope(message: impl Into<String>) -> Value {
    serde_json::json!({
        "name": "APIError",
        "data": { "message": message.into() }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_pure_text_stream() {
        let raw = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hel\"}\n\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"lo\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}}\n\n\
data: [DONE]\n\n";
        let parts = classify_stream(raw).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], ResponsesStreamPart::TextDelta("Hel".into()));
        assert_eq!(parts[1], ResponsesStreamPart::TextDelta("lo".into()));
        match &parts[2] {
            ResponsesStreamPart::Finish { reason, usage } => {
                assert_eq!(reason, "stop");
                let usage = usage.as_ref().unwrap();
                assert_eq!(usage.input, 2);
                assert_eq!(usage.output, 1);
                assert_eq!(usage.total, 3);
            }
            other => panic!("expected Finish, got {other:?}"),
        }
    }

    #[test]
    fn classifies_single_tool_call_with_delta_and_done() {
        // The AI-SDK-equivalent shape for a single tool call: started,
        // arg deltas, finalized. Bun emits these as separate stream
        // parts — we mirror that.
        let raw = "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\":\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"\\\"note.txt\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\"}\n\n";
        let parts = classify_stream(raw).unwrap();
        // Two arg-delta parts followed by a complete part.
        assert!(matches!(
            parts[0],
            ResponsesStreamPart::ToolCallArgsDelta { .. }
        ));
        assert!(matches!(
            parts[1],
            ResponsesStreamPart::ToolCallArgsDelta { .. }
        ));
        match &parts[2] {
            ResponsesStreamPart::ToolCallComplete { id, name, args } => {
                assert_eq!(id, "call_read");
                assert_eq!(name, "read");
                assert_eq!(args, &json!({ "filePath": "note.txt" }));
            }
            other => panic!("expected ToolCallComplete, got {other:?}"),
        }
    }

    #[test]
    fn classifies_two_parallel_tool_calls() {
        // Interleaved arg deltas across two output_index slots; both must
        // resolve cleanly. This is the parallel-execution invariant in
        // its smallest form: two distinct call_ids reach
        // ToolCallComplete, in start order.
        let raw = "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_a\",\"name\":\"read\",\"delta\":\"{\\\"x\\\":1}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"call_id\":\"call_b\",\"name\":\"grep\",\"delta\":\"{\\\"p\\\":\\\"q\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_a\",\"name\":\"read\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":1,\"call_id\":\"call_b\",\"name\":\"grep\"}\n\n";
        let parts = classify_stream(raw).unwrap();
        let completes: Vec<_> = parts
            .iter()
            .filter_map(|p| match p {
                ResponsesStreamPart::ToolCallComplete { id, name, args } => {
                    Some((id.clone(), name.clone(), args.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(completes.len(), 2);
        assert_eq!(completes[0].0, "call_a");
        assert_eq!(completes[0].1, "read");
        assert_eq!(completes[0].2, json!({ "x": 1 }));
        assert_eq!(completes[1].0, "call_b");
        assert_eq!(completes[1].1, "grep");
        assert_eq!(completes[1].2, json!({ "p": "q" }));
    }

    #[test]
    fn classifies_error_event_mid_stream() {
        let raw = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"oops\"}\n\n\
data: {\"type\":\"error\",\"error\":{\"message\":\"upstream blew up\"}}\n\n";
        let parts = classify_stream(raw).unwrap();
        assert!(matches!(parts[0], ResponsesStreamPart::TextDelta(_)));
        match &parts[1] {
            ResponsesStreamPart::Error { message } => {
                assert_eq!(message, "upstream blew up");
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn error_envelope_matches_bun_named_error() {
        let env = error_envelope("blown");
        assert_eq!(env["name"], "APIError");
        assert_eq!(env["data"]["message"], "blown");
    }

    #[test]
    fn dangling_usage_synthesizes_finish() {
        // Some upstreams emit `response.completed` with usage but no
        // explicit finish-reason event; our classifier must still
        // surface the usage downstream.
        let raw = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":0,\"total_tokens\":1}}}\n\n";
        let parts = classify_stream(raw).unwrap();
        // Two events: TextDelta + Finish (synthesized or real).
        assert_eq!(parts.len(), 2);
        match &parts[1] {
            ResponsesStreamPart::Finish { usage, .. } => {
                let usage = usage.as_ref().unwrap();
                assert_eq!(usage.input, 1);
                assert_eq!(usage.total, 1);
            }
            other => panic!("expected Finish, got {other:?}"),
        }
    }
}
