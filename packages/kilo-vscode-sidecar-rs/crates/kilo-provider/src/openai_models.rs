//! OpenAI Pro / ChatGPT Plus model registry filtering.
//!
//! Mirrors the Bun source-of-truth at
//! [`packages/opencode/src/plugin/codex.ts:373-400`](../../../../../opencode/src/plugin/codex.ts:373).
//! When the OAuth path is active, only Codex-allowed models are surfaced
//! and per-token cost is zeroed (the ChatGPT subscription covers it).
//!
//! Two pieces of logic are load-bearing:
//!
//! 1. The `gpt-(\d+\.\d+)` regex gate that admits future post-`gpt-5.4`
//!    base models without a code change. The Bun code at
//!    [`codex.ts:388`](../../../../../opencode/src/plugin/codex.ts:388)
//!    keeps models whose parsed version is greater than `5.4`; we mirror
//!    that comparison exactly.
//! 2. Cost zeroing — surfacing real OpenAI per-token cost on a
//!    subscription-included model would mis-render the UI's spend tile.
//!
//! The kilo-provider call sites use this module's filter to derive the
//! per-provider model list returned from `GET /provider`. The auth-key
//! path bypasses this filter entirely; the OAuth path always uses it.

use serde_json::{json, Value};

#[derive(Clone, Copy)]
pub struct ModelSpec {
    id: &'static str,
    name: &'static str,
    family: &'static str,
    attachment: bool,
    reasoning: bool,
    toolcall: bool,
    temperature: bool,
    pdf: bool,
    input: f64,
    output: f64,
    cache: f64,
    context: u64,
    limit: u64,
    out: u64,
    release: &'static str,
}

/// Static OpenAI registry used by the Rust sidecar. It is intentionally a
/// compact subset of the upstream OpenAI snapshot: the Codex/ChatGPT Pro
/// models users can pick after OAuth, plus a few regular OpenAI API models
/// that prove the API-key path stays unfiltered and priced.
pub const REGISTRY: &[ModelSpec] = &[
    ModelSpec {
        id: "gpt-5.1-codex-max",
        name: "GPT-5.1 Codex Max",
        family: "gpt-codex",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: false,
        input: 1.25,
        output: 10.0,
        cache: 0.125,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2025-11-13",
    },
    ModelSpec {
        id: "gpt-5.1-codex",
        name: "GPT-5.1 Codex",
        family: "gpt-codex",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: false,
        input: 1.25,
        output: 10.0,
        cache: 0.13,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2025-11-13",
    },
    ModelSpec {
        id: "gpt-5.1-codex-mini",
        name: "GPT-5.1 Codex mini",
        family: "gpt-codex",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: false,
        input: 0.25,
        output: 2.0,
        cache: 0.025,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2025-11-13",
    },
    ModelSpec {
        id: "gpt-5.2",
        name: "GPT-5.2",
        family: "gpt",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: false,
        input: 1.75,
        output: 14.0,
        cache: 0.175,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2025-12-11",
    },
    ModelSpec {
        id: "gpt-5.2-codex",
        name: "GPT-5.2 Codex",
        family: "gpt-codex",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: true,
        input: 1.75,
        output: 14.0,
        cache: 0.175,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2025-12-11",
    },
    ModelSpec {
        id: "gpt-5.3-codex",
        name: "GPT-5.3 Codex",
        family: "gpt-codex",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: true,
        input: 1.75,
        output: 14.0,
        cache: 0.175,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2026-02-05",
    },
    ModelSpec {
        id: "gpt-5.4",
        name: "GPT-5.4",
        family: "gpt",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: true,
        input: 2.5,
        output: 15.0,
        cache: 0.25,
        context: 1050000,
        limit: 922000,
        out: 128000,
        release: "2026-03-05",
    },
    ModelSpec {
        id: "gpt-5.4-mini",
        name: "GPT-5.4 mini",
        family: "gpt-mini",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: false,
        input: 0.75,
        output: 4.5,
        cache: 0.075,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2026-03-17",
    },
    ModelSpec {
        id: "gpt-5.5",
        name: "GPT-5.5",
        family: "gpt",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: true,
        input: 2.5,
        output: 15.0,
        cache: 0.25,
        context: 1050000,
        limit: 922000,
        out: 128000,
        release: "",
    },
    ModelSpec {
        id: "gpt-5.1",
        name: "GPT-5.1",
        family: "gpt",
        attachment: true,
        reasoning: true,
        toolcall: true,
        temperature: false,
        pdf: false,
        input: 1.25,
        output: 10.0,
        cache: 0.13,
        context: 400000,
        limit: 272000,
        out: 128000,
        release: "2025-11-13",
    },
    ModelSpec {
        id: "gpt-4.1",
        name: "GPT-4.1",
        family: "gpt",
        attachment: true,
        reasoning: false,
        toolcall: true,
        temperature: true,
        pdf: true,
        input: 2.0,
        output: 8.0,
        cache: 0.5,
        context: 1047576,
        limit: 1047576,
        out: 32768,
        release: "2025-04-14",
    },
    ModelSpec {
        id: "o1-preview",
        name: "o1-preview",
        family: "o",
        attachment: false,
        reasoning: true,
        toolcall: false,
        temperature: true,
        pdf: false,
        input: 15.0,
        output: 60.0,
        cache: 7.5,
        context: 128000,
        limit: 128000,
        out: 32768,
        release: "2024-09-12",
    },
];

pub fn registry(npm: &str) -> serde_json::Map<String, Value> {
    REGISTRY
        .iter()
        .map(|spec| (spec.id.to_string(), spec.value(npm)))
        .collect()
}

impl ModelSpec {
    fn value(&self, npm: &str) -> Value {
        json!({
            "id": self.id,
            "providerID": "openai",
            "api": { "id": self.id, "url": "", "npm": npm },
            "name": self.name,
            "family": self.family,
            "capabilities": {
                "temperature": self.temperature,
                "reasoning": self.reasoning,
                "attachment": self.attachment,
                "toolcall": self.toolcall,
                "structured_output": true,
                "input": { "text": true, "audio": false, "image": self.attachment, "video": false, "pdf": self.pdf },
                "output": { "text": true, "audio": false, "image": false, "video": false, "pdf": false },
                "interleaved": false
            },
            "cost": { "input": self.input, "output": self.output, "cache": { "read": self.cache, "write": 0 } },
            "limit": { "context": self.context, "input": self.limit, "output": self.out },
            "status": "active",
            "options": {},
            "headers": {},
            "release_date": self.release,
            "variants": {}
        })
    }
}

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
/// - the id matches `^gpt-(\d+\.\d+)` AND the captured version is `> 5.4`.
///
/// Anything else is filtered out.
pub fn is_codex_allowed(id: &str) -> bool {
    if id.contains("codex") {
        return true;
    }
    if ALLOWED_MODELS.contains(&id) {
        return true;
    }
    parse_gpt_major(id).map(|v| v > 5.4).unwrap_or(false)
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
    fn allows_future_gpt_above_5_4_base() {
        assert!(is_codex_allowed("gpt-5.5"));
        assert!(is_codex_allowed("gpt-5.6-mini"));
        assert!(is_codex_allowed("gpt-9.9"));
    }

    #[test]
    fn rejects_gpt_at_or_below_5_4_unless_explicit_or_codex() {
        // Per `codex.ts:389`: `if (parseFloat(match[1]) > 5.4) continue` —
        // i.e. continue past `delete`, meaning the model is *kept* only when
        // it is above the boundary. At-or-below models need either an
        // explicit allow-list entry or the `codex` substring path.
        // The "codex" substring path always wins regardless of version, so
        // we test with a clean `gpt-<X.Y>-mini` shape that doesn't carry
        // the codex token.
        assert!(!is_codex_allowed("gpt-5.1"));
        assert!(!is_codex_allowed("gpt-4.1"));
        assert!(!is_codex_allowed("gpt-3.5"));
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
        models.insert("gpt-5.5".to_string(), entry("gpt-5.5", 1));
        models.insert("o1-preview".to_string(), entry("o1-preview", 12));
        models.insert("gpt-4.1".to_string(), entry("gpt-4.1", 99));
        models.insert("claude-3.5".to_string(), entry("claude-3.5", 3));

        filter_codex_models(&mut models);

        assert!(models.contains_key("gpt-5.1-codex"));
        assert!(models.contains_key("gpt-5.2"));
        assert!(models.contains_key("gpt-5.5"));
        assert!(!models.contains_key("o1-preview"));
        assert!(!models.contains_key("gpt-4.1"));
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
