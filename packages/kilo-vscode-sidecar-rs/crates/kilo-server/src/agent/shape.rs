//! Shared message-shape helpers used by both the OpenAI streaming
//! pipeline (`agent::openai_stream`) and the assistant message / tool
//! part assembly (`agent::parts`). Lifted verbatim out of
//! `agent::openai_stream` to break the `parts` <-> `openai_stream`
//! cyclic import. No behavior change.

use kilo_provider::ChatUsage;
use serde_json::{json, Value};

use crate::Repair;

pub(crate) fn step_finish_part(sid: &str, mid: &str, part: &Value) -> Value {
    let pid = part["id"].as_str().unwrap_or("prt_fake");
    json!({
        "id": format!("{pid}_step_finish"),
        "type": "step-finish",
        "messageID": mid,
        "sessionID": sid,
        "reason": "stop",
        "cost": 0,
        "tokens": {
            "input": 0,
            "output": 0,
            "reasoning": 0,
            "total": 0,
            "cache": { "read": 0, "write": 0 }
        }
    })
}

pub(crate) fn step_finish_part_usage(
    sid: &str,
    mid: &str,
    part: &Value,
    usage: Option<&ChatUsage>,
    reason: Option<&str>,
) -> Value {
    let mut part = step_finish_part(sid, mid, part);
    if let Some(usage) = usage {
        part["tokens"] = tokens_value(usage);
    }
    if let Some(reason) = reason {
        part["reason"] = json!(reason);
    }
    part
}

pub(crate) fn tokens_value(usage: &ChatUsage) -> Value {
    json!({
        "input": usage.input,
        "output": usage.output,
        "reasoning": usage.reasoning,
        "total": usage.total,
        "cache": { "read": usage.cache_read, "write": usage.cache_write }
    })
}

/// Sum a per-iteration usage record into a turn-level accumulator. Bun's
/// `Session.getUsage` (`session.ts:308-385`) computes the per-step usage and
/// the `step-finish` writer adds it into `assistantMessage.tokens`
/// (`processor.ts:443-447`). This is the Rust equivalent.
pub(crate) fn usage_accumulate(acc: &mut ChatUsage, next: &ChatUsage) {
    acc.input = acc.input.saturating_add(next.input);
    acc.output = acc.output.saturating_add(next.output);
    acc.total = acc.total.saturating_add(next.total);
    acc.reasoning = acc.reasoning.saturating_add(next.reasoning);
    acc.cache_read = acc.cache_read.saturating_add(next.cache_read);
    acc.cache_write = acc.cache_write.saturating_add(next.cache_write);
}

/// Per-iteration `step-start` part appended at the top of each agent loop
/// iteration. Bun emits this from the AI SDK `start-step` event
/// (`processor.ts:402-412`); we generate it directly from the loop index.
pub(crate) fn step_start_part(sid: &str, mid: &str, pid: &str, iteration: usize) -> Value {
    json!({
        "id": format!("{pid}_step_start_{iteration}"),
        "type": "step-start",
        "messageID": mid,
        "sessionID": sid,
    })
}

/// Per-iteration `step-finish` factory. Differs from [`step_finish_part`]
/// in that it uses the iteration-scoped synthetic part id, so multiple
/// step-finishes can co-exist on a single assistant message (Bun emits one
/// per `finish-step` event).
pub(crate) fn step_finish_part_iter(
    sid: &str,
    mid: &str,
    pid: &str,
    iteration: usize,
    usage: Option<&ChatUsage>,
    reason: Option<&str>,
) -> Value {
    let mut part = json!({
        "id": format!("{pid}_step_finish_{iteration}"),
        "type": "step-finish",
        "messageID": mid,
        "sessionID": sid,
        "reason": reason.unwrap_or("stop"),
        "cost": 0,
        "tokens": match usage {
            Some(u) => tokens_value(u),
            None => json!({
                "input": 0, "output": 0, "reasoning": 0, "total": 0,
                "cache": { "read": 0, "write": 0 }
            }),
        }
    });
    if let Some(reason) = reason {
        part["reason"] = json!(reason);
    }
    part
}

pub(crate) fn repair_tool_name(name: &str, known: &[&str]) -> Repair {
    known
        .iter()
        .find(|tool| tool.eq_ignore_ascii_case(name))
        .map(|tool| Repair::Valid((*tool).to_string()))
        .unwrap_or_else(|| Repair::Invalid(name.to_string()))
}
