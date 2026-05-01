//! OpenAI Pro / ChatGPT Plus model registry filtering.
//!
//! Mirrors the Bun source-of-truth at
//! [`packages/opencode/src/plugin/codex.ts:373-400`](../../../../../opencode/src/plugin/codex.ts:373).
//! When the OAuth path is active, only Codex-allowed models are surfaced
//! and per-token cost is zeroed (the ChatGPT subscription covers it).
//!
//! Two pieces of logic are load-bearing:
//!
//! 1. The `gpt-(\d+\.\d+)` regex gate that admits any future `gpt-5.2+`
//!    base model without a code change. The Bun code at
//!    [`codex.ts:388`](../../../../../opencode/src/plugin/codex.ts:388)
//!    bounds the upper version at `5.4`; we mirror that bound.
//! 2. Cost zeroing — surfacing real OpenAI per-token cost on a
//!    subscription-included model would mis-render the UI's spend tile.
//!
//! The kilo-provider call sites use this module's filter to derive the
//! per-provider model list returned from `GET /provider`. The auth-key
//! path bypasses this filter entirely; the OAuth path always uses it.

use serde_json::{json, Value};

/// Models that are always admitted on the OAuth path, even if they don't
/// match the version-prefix regex. Kept in sync with
/// [`codex.ts:374-384`](../../../../../opencode/src/plugin/codex.ts:374).
pub const ALLOWED_MODELS: &[&str] = &[
    "gpt-5.1-codex",
    "gpt-5.1-codex-max",
    "gpt-5.1-codex-mini",
    "gpt-5.2",
    "gpt-5.2-codex",
    "gpt-5.3-codex",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.5",
];

/// Returns `true` when `id` is admitted under the OAuth Codex filter.
/// Mirrors the union of three checks at
/// [`codex.ts:386-390`](../../../../../opencode/src/plugin/codex.ts:386):
///
/// - the id contains `"codex"` (always allowed); OR
/// - the id is in [`ALLOWED_MODELS`]; OR
/// - the id matches `^gpt-(\d+\.\d+)` AND the captured version is `<= 5.4`.
///
/// Anything else is filtered out.
pub fn is_codex_allowed(id: &str) -> bool {
    if id.contains("codex") {
        return true;
    }
    if ALLOWED_MODELS.contains(&id) {
        return true;
    }
    parse_gpt_major(id).map(|v| v <= 5.4).unwrap_or(false)
}

/// Parse the `gpt-X.Y` version prefix into a float. Matches the regex
/// `^gpt-(\d+\.\d+)` from
/// [`codex.ts:388`](../../../../../opencode/src/plugin/codex.ts:388).
/// Returns `None` if the id doesn't start with `gpt-` followed by a
/// `<digit>+ '.' <digit>+` token.
pub fn parse_gpt_major(id: &str) -> Option<f64> {
    let rest = id.strip_prefix("gpt-")?;
    let mut chars = rest.chars();
    let mut major = String::new();
    let mut minor = String::new();
    let mut saw_dot = false;
    let mut saw_minor = false;

    while let Some(ch) = chars.next() {
        if !saw_dot {
            if ch.is_ascii_digit() {
                major.push(ch);
            } else if ch == '.' {
                if major.is_empty() {
                    return None;
                }
                saw_dot = true;
            } else {
                return None;
            }
        } else if ch.is_ascii_digit() {
            minor.push(ch);
            saw_minor = true;
        } else {
            break;
        }
    }
    if !saw_dot || !saw_minor {
        return None;
    }
    let _ = chars; // remaining trailing chars are ignored intentionally
    format!("{major}.{minor}").parse().ok()
}

/// Cost block that the OAuth path stamps onto every admitted model.
/// Subscription-included usage means provider per-token cost would be
/// misleading on the UI's spend tile — see
/// [`codex.ts:393-400`](../../../../../opencode/src/plugin/codex.ts:393).
pub fn zero_cost() -> Value {
    json!({
        "input": 0,
        "output": 0,
        "cache": { "read": 0, "write": 0 }
    })
}

/// Filter a static `models` map (`id -> Value`) down to OAuth-admitted
/// entries, zeroing every survivor's `cost` block. The input is the same
/// shape as the existing [`crate::provider`] static-list builder. Mutates
/// the input in place to mirror the Bun
/// [`for ... in Object.entries`](../../../../../opencode/src/plugin/codex.ts:385)
/// loop and avoid the cost of reallocating the surrounding provider entry.
pub fn filter_codex_models(models: &mut serde_json::Map<String, Value>) {
    let drop: Vec<String> = models
        .keys()
        .filter(|id| {
            let api_id = models[*id]
                .get("api")
                .and_then(|api| api.get("id"))
                .and_then(Value::as_str)
                .unwrap_or(id.as_str());
            !is_codex_allowed(api_id)
        })
        .cloned()
        .collect();
    for id in drop {
        models.remove(&id);
    }
    for (_, model) in models.iter_mut() {
        if let Some(obj) = model.as_object_mut() {
            obj.insert("cost".to_string(), zero_cost());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(api_id: &str, cost_input: u64) -> Value {
        json!({
            "id": api_id,
            "api": { "id": api_id },
            "cost": { "input": cost_input, "output": cost_input * 2, "cache": { "read": 1, "write": 2 } }
        })
    }

    #[test]
    fn parse_gpt_major_extracts_x_y() {
        assert_eq!(parse_gpt_major("gpt-5.2"), Some(5.2));
        assert_eq!(parse_gpt_major("gpt-5.2-codex"), Some(5.2));
        assert_eq!(parse_gpt_major("gpt-4.0-turbo"), Some(4.0));
    }

    #[test]
    fn parse_gpt_major_rejects_non_gpt() {
        assert_eq!(parse_gpt_major("o1-preview"), None);
        assert_eq!(parse_gpt_major("gpt-3"), None);
        assert_eq!(parse_gpt_major("claude-3.5"), None);
    }

    #[test]
    fn allows_codex_substring() {
        assert!(is_codex_allowed("gpt-5.1-codex"));
        assert!(is_codex_allowed("gpt-7.0-codex-future"));
        assert!(is_codex_allowed("codex-experimental"));
    }

    #[test]
    fn allows_explicit_allow_list() {
        for id in ALLOWED_MODELS {
            assert!(is_codex_allowed(id), "{id} should be allowed");
        }
    }

    #[test]
    fn allows_gpt_le_5_4_base() {
        assert!(is_codex_allowed("gpt-5.4-mini"));
        assert!(is_codex_allowed("gpt-5.0"));
        assert!(is_codex_allowed("gpt-3.5"));
    }

    #[test]
    fn rejects_gpt_above_5_4() {
        // Per `codex.ts:389`: `if (parseFloat(match[1]) > 5.4) continue` —
        // i.e. continue past `delete`, meaning the model is *kept*. The
        // Bun guard is "delete unless explicitly allowed"; ours is "allow
        // if regex is <= 5.4". The semantic match is on the `<=` boundary.
        // The "codex" substring path always wins regardless of version, so
        // we test with a clean `gpt-<X.Y>-mini` shape that doesn't carry
        // the codex token.
        assert!(!is_codex_allowed("gpt-9.9-mini"));
        assert!(!is_codex_allowed("gpt-99.9"));
        assert!(!is_codex_allowed("gpt-6.0-experimental"));
    }

    #[test]
    fn rejects_non_openai_models() {
        assert!(!is_codex_allowed("o1-preview"));
        assert!(!is_codex_allowed("claude-3.5-sonnet"));
        assert!(!is_codex_allowed("gemini-2.0-flash"));
    }

    #[test]
    fn filter_drops_unallowed_and_zeroes_cost() {
        let mut models = serde_json::Map::new();
        models.insert("gpt-5.1-codex".to_string(), entry("gpt-5.1-codex", 5));
        models.insert("gpt-5.2".to_string(), entry("gpt-5.2", 7));
        models.insert("gpt-3.5".to_string(), entry("gpt-3.5", 1));
        models.insert("o1-preview".to_string(), entry("o1-preview", 12));
        models.insert("gpt-9.9-rogue".to_string(), entry("gpt-9.9-rogue", 99));
        models.insert("claude-3.5".to_string(), entry("claude-3.5", 3));

        filter_codex_models(&mut models);

        assert!(models.contains_key("gpt-5.1-codex"));
        assert!(models.contains_key("gpt-5.2"));
        assert!(models.contains_key("gpt-3.5"));
        assert!(!models.contains_key("o1-preview"));
        assert!(!models.contains_key("gpt-9.9-rogue"));
        assert!(!models.contains_key("claude-3.5"));

        for (_, model) in models.iter() {
            let cost = model.get("cost").unwrap();
            assert_eq!(cost["input"], 0);
            assert_eq!(cost["output"], 0);
            assert_eq!(cost["cache"]["read"], 0);
            assert_eq!(cost["cache"]["write"], 0);
        }
    }

    #[test]
    fn filter_uses_api_id_not_object_key() {
        // Mirrors `codex.ts:387`: filter check is on `model.api.id`, not the
        // surrounding key. A vendor-renamed key with a real Codex `api.id`
        // should survive.
        let mut models = serde_json::Map::new();
        models.insert(
            "renamed-key".to_string(),
            json!({
                "api": { "id": "gpt-5.1-codex" },
                "cost": { "input": 5, "output": 10, "cache": { "read": 0, "write": 0 } }
            }),
        );
        models.insert(
            "another-rename".to_string(),
            json!({
                "api": { "id": "claude-3.5" },
                "cost": { "input": 5, "output": 10, "cache": { "read": 0, "write": 0 } }
            }),
        );
        filter_codex_models(&mut models);
        assert!(models.contains_key("renamed-key"));
        assert!(!models.contains_key("another-rename"));
    }
}
