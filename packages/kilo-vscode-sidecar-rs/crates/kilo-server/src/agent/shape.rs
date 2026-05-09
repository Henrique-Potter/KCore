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

pub(crate) fn usage_cost(model: Option<&Value>, usage: &ChatUsage) -> f64 {
    let Some(cost) = model.and_then(|value| value.get("cost")) else {
        return 0.0;
    };
    let input = usage
        .input
        .saturating_sub(usage.cache_read)
        .saturating_sub(usage.cache_write);
    let output = usage.output.saturating_sub(usage.reasoning);
    let over = input.saturating_add(usage.cache_read) > 200_000;
    let cost = if over {
        cost.get("experimentalOver200K").unwrap_or(cost)
    } else {
        cost
    };
    let rate = |path: &[&str]| -> f64 {
        let mut value = cost;
        for key in path {
            let Some(next) = value.get(*key) else {
                return 0.0;
            };
            value = next;
        }
        value.as_f64().unwrap_or(0.0)
    };
    let amount = (input as f64 * rate(&["input"])
        + output as f64 * rate(&["output"])
        + usage.reasoning as f64 * rate(&["output"])
        + usage.cache_read as f64 * rate(&["cache", "read"])
        + usage.cache_write as f64 * rate(&["cache", "write"]))
        / 1_000_000.0;
    if amount.is_finite() {
        amount
    } else {
        0.0
    }
}

pub(crate) fn usage_cost_value(model: Option<&Value>, usage: &ChatUsage) -> Value {
    let cost = usage_cost(model, usage);
    if cost == 0.0 {
        json!(0)
    } else {
        json!(cost)
    }
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
    model: Option<&Value>,
    usage: Option<&ChatUsage>,
    reason: Option<&str>,
) -> Value {
    let mut part = json!({
        "id": format!("{pid}_step_finish_{iteration}"),
        "type": "step-finish",
        "messageID": mid,
        "sessionID": sid,
        "reason": reason.unwrap_or("stop"),
        "cost": usage.map(|u| usage_cost_value(model, u)).unwrap_or_else(|| json!(0)),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_cost_uses_model_rates_and_cache_buckets() {
        let model = json!({
            "cost": {
                "input": 1.0,
                "output": 10.0,
                "cache": { "read": 0.1, "write": 2.0 }
            }
        });
        let usage = ChatUsage {
            input: 120,
            output: 50,
            total: 170,
            reasoning: 10,
            cache_read: 20,
            cache_write: 5,
        };

        let cost = usage_cost(Some(&model), &usage);

        assert!((cost - 0.000607).abs() < 0.0000001);
    }
}
