//! Normalization of volatile fields in fixtures.
//!
//! Recordings are useful only if they round-trip across machines and runs.
//! Bun emits a great deal of run-specific data (random session IDs,
//! per-process ports, machine-local paths, monotonic timestamps). We strip or
//! collapse all of that into stable placeholders before writing fixtures.
//!
//! The same normalizer is run on **read** as well, so a normalized-on-disk
//! fixture stays normalized after a round trip — the operation is required to
//! be idempotent.
//!
//! Design notes:
//!
//! - Order matters. The longest replacement (workspace path) is tried first
//!   so we don't accidentally substitute a substring twice.
//! - Identifier replacement is **stable per [`Normalizer`] instance**. The
//!   first session ID seen becomes `<SESSION_ID:1>`, the second
//!   `<SESSION_ID:2>`, etc. The mapping is exposed via [`Normalizer::map`] so a
//!   reviewer can recover the original IDs from a sidecar `.map.json` file.
//! - The implementation is intentionally regex-only. We do not crack JSON open
//!   ourselves; we treat each frame as a string and substitute. This keeps the
//!   normalizer trivially extendable.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};

/// Inputs to the normalizer that depend on the current run/host.
#[derive(Debug, Clone, Default)]
pub struct Redactions {
    pub server_password: Option<String>,
    pub port: Option<u16>,
    pub workspace_root: Option<PathBuf>,
    pub home: Option<PathBuf>,
    pub pid: Option<u32>,
}

impl Redactions {
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.server_password = Some(password.into());
        self
    }
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
    pub fn with_workspace<P: Into<PathBuf>>(mut self, p: P) -> Self {
        self.workspace_root = Some(p.into());
        self
    }
    pub fn with_home<P: Into<PathBuf>>(mut self, p: P) -> Self {
        self.home = Some(p.into());
        self
    }
    pub fn with_pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }
}

/// Identifier mapping recorded alongside a fixture.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentityMap {
    pub session_ids: BTreeMap<String, String>,
    pub message_ids: BTreeMap<String, String>,
    pub part_ids: BTreeMap<String, String>,
    #[serde(default)]
    pub event_ids: BTreeMap<String, String>,
    pub request_ids: BTreeMap<String, String>,
    pub uuids: BTreeMap<String, String>,
    pub timestamps: BTreeMap<String, String>,
}

/// The normalizer applies redactions in a fixed order so output is
/// deterministic.
#[derive(Debug, Clone)]
pub struct Normalizer {
    redactions: Redactions,
    map: IdentityMap,
}

impl Normalizer {
    pub fn new(redactions: Redactions) -> Self {
        Self {
            redactions,
            map: IdentityMap::default(),
        }
    }

    pub fn map(&self) -> &IdentityMap {
        &self.map
    }

    /// Apply normalization to a single string. Idempotent.
    pub fn normalize_str(&mut self, s: &str) -> String {
        let mut out = s.to_string();

        // 1. Workspace and home path prefixes — longest first.
        out = redact_path_prefix(
            &out,
            self.redactions.workspace_root.as_deref(),
            "<WORKSPACE>",
        );
        out = redact_path_prefix(&out, self.redactions.home.as_deref(), "<HOME>");

        // 2. Path separator normalization (Windows -> POSIX) for any path that
        //    survived the prefix substitution. Only after prefix replacement,
        //    so we don't damage URLs.
        out = normalize_path_separators_in_placeholders(&out);

        // 3. Server password (must come before generic hex tokens).
        if let Some(pw) = &self.redactions.server_password {
            if !pw.is_empty() {
                out = out.replace(pw, "<KILO_SERVER_PASSWORD>");
            }
        }

        // 4. Port — replace `:<port>/` and `:<port>"` patterns to avoid eating
        //    unrelated digit runs.
        if let Some(port) = self.redactions.port {
            let needle = format!(":{port}");
            out = out.replace(&needle, ":<PORT>");
        }

        // 5. PID.
        if let Some(pid) = self.redactions.pid {
            out = out.replace(&pid.to_string(), "<PID>");
        }

        // 6. Stable identifier renames.
        out = stable_replace(
            &out,
            &SESSION_ID_RE,
            "<SESSION_ID",
            &mut self.map.session_ids,
        );
        out = stable_replace(
            &out,
            &MESSAGE_ID_RE,
            "<MESSAGE_ID",
            &mut self.map.message_ids,
        );
        out = stable_replace(&out, &PART_ID_RE, "<PART_ID", &mut self.map.part_ids);
        out = stable_replace(&out, &EVENT_ID_RE, "<EVENT_ID", &mut self.map.event_ids);
        out = stable_replace(
            &out,
            &REQUEST_ID_RE,
            "<REQUEST_ID",
            &mut self.map.request_ids,
        );
        out = stable_replace(&out, &UUID_RE, "<UUID", &mut self.map.uuids);

        // 7. Common timestamp-ish numeric runs in JSON value positions.
        //    We deliberately avoid touching numbers in other positions (tokens,
        //    line numbers, etc.) by anchoring on quoted-key context.
        out = stable_replace(&out, &TIMESTAMP_RE, "<TIMESTAMP", &mut self.map.timestamps);

        out
    }

    /// Apply normalization to a `serde_json::Value`.
    ///
    /// Strings pass through [`Normalizer::normalize_str`] (where regex-based
    /// substitution is safe). For non-string nodes we walk the tree recursively
    /// so we can:
    ///
    /// - Replace numeric timestamp values (`created`, `updated`, `time`, …)
    ///   with stable string placeholders without producing invalid JSON.
    /// - Replace numeric `port` values with the canonical `<PORT>` string
    ///   token used by the startup fixture.
    /// - Replace known-volatile string fields (`version`, `bun_version`,
    ///   `recorded_at_iso`) with their canonical placeholders even when the
    ///   *value* would not otherwise match a regex — these are keyed entirely
    ///   off the field name.
    ///
    /// Idempotent: running the result through `normalize_value` again must
    /// produce an equal `Value`.
    pub fn normalize_value(&mut self, v: &serde_json::Value) -> serde_json::Value {
        self.normalize_value_at(v, None)
    }

    /// Internal recursive walker. `key` is the key under which `v` is stored
    /// in its parent object (if any), and is used to apply field-keyed
    /// redactions like `<TIMESTAMP:N>` for numeric timestamp values.
    fn normalize_value_at(
        &mut self,
        v: &serde_json::Value,
        key: Option<&str>,
    ) -> serde_json::Value {
        use serde_json::Value;
        match v {
            Value::String(s) => {
                // Field-keyed redactions for known-volatile strings.
                if let Some(k) = key {
                    if let Some(ph) = volatile_string_placeholder_for_key(k) {
                        return Value::String(ph.to_string());
                    }
                }
                Value::String(self.normalize_str(s))
            }
            Value::Number(n) => {
                if let Some(k) = key {
                    // Numeric port → canonical "<PORT>" string placeholder.
                    if k == "port" {
                        if n.as_u64().is_some() || n.as_i64().is_some() {
                            return Value::String("<PORT>".to_string());
                        }
                    }
                    // Numeric timestamps. We require ≥10 digits to mirror the
                    // string-form timestamp regex (small ints like indices
                    // must be left alone).
                    if is_timestamp_key(k) {
                        if let Some(u) = n.as_u64() {
                            if u >= 1_000_000_000 {
                                let placeholder = self.timestamp_placeholder_for(&u.to_string());
                                return Value::String(placeholder);
                            }
                        } else if let Some(i) = n.as_i64() {
                            if i >= 1_000_000_000 {
                                let placeholder = self.timestamp_placeholder_for(&i.to_string());
                                return Value::String(placeholder);
                            }
                        }
                    }
                }
                v.clone()
            }
            Value::Array(items) => Value::Array(
                items
                    .iter()
                    .map(|item| self.normalize_value_at(item, None))
                    .collect(),
            ),
            Value::Object(map) => {
                let mut out = serde_json::Map::with_capacity(map.len());
                for (k, child) in map {
                    out.insert(k.clone(), self.normalize_value_at(child, Some(k)));
                }
                Value::Object(out)
            }
            Value::Null | Value::Bool(_) => v.clone(),
        }
    }

    /// Look up or assign a stable `<TIMESTAMP:N>` placeholder for a numeric
    /// timestamp key. The mapping is shared with the string-form regex
    /// substitution so a payload that contains the same value as both a
    /// string and a number gets the same placeholder.
    fn timestamp_placeholder_for(&mut self, key: &str) -> String {
        if let Some(existing) = self.map.timestamps.get(key) {
            return existing.clone();
        }
        let placeholder = format!("<TIMESTAMP:{}>", self.map.timestamps.len() + 1);
        self.map
            .timestamps
            .insert(key.to_string(), placeholder.clone());
        placeholder
    }

    /// Normalize a wall-clock duration in milliseconds. We bucket to 1-second
    /// granularity so heartbeat traces stay stable across slow machines.
    pub fn normalize_duration_ms(ms: u64) -> u64 {
        // Round to nearest 1000.
        ((ms + 500) / 1000) * 1000
    }
}

// ---------------------------------------------------------------------------
// Internal helpers

static SESSION_ID_RE: Lazy<Regex> = Lazy::new(|| {
    // Bun emits "ses_<base32-ish>" or "sess_<id>". We accept both.
    Regex::new(r"\b(?:ses|sess)_[A-Za-z0-9]{6,}").unwrap()
});

static MESSAGE_ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bmsg_[A-Za-z0-9]{6,}").unwrap());

static PART_ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bprt_[A-Za-z0-9]{6,}").unwrap());

static EVENT_ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\bevt_[A-Za-z0-9]{6,}").unwrap());

static REQUEST_ID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b(?:perm|qst)_[A-Za-z0-9]{6,}").unwrap());

static UUID_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
        .unwrap()
});

// Match "<key>": <number> for known timestamp-like keys. The number must be
// large enough to plausibly be a unix-epoch ms (≥10 digits) so we don't damage
// small ints like message indices.
//
// NOTE: this regex is no longer the primary timestamp redactor for
// `normalize_value` (which now walks the parsed tree to avoid corrupting JSON
// number nodes — see `normalize_value_at`). It is still used by
// `normalize_str` for log lines and other free-form text where the input is
// not parsed JSON. Keep the key list in sync with `is_timestamp_key`.
static TIMESTAMP_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#""(?:created|updated|time|ts|timestamp|completedAt|startedAt)"\s*:\s*(\d{10,})"#)
        .unwrap()
});

/// Keys whose numeric values are interpreted as timestamps and replaced with
/// `<TIMESTAMP:N>` during the recursive walk. Mirrors the alternation in
/// `TIMESTAMP_RE` exactly.
fn is_timestamp_key(key: &str) -> bool {
    static SET: Lazy<BTreeSet<&'static str>> = Lazy::new(|| {
        [
            "created",
            "updated",
            "time",
            "ts",
            "timestamp",
            "completedAt",
            "startedAt",
        ]
        .into_iter()
        .collect()
    });
    SET.contains(key)
}

/// Known string fields whose value is replaced wholesale by a canonical
/// placeholder regardless of content. These are fields where the *presence
/// and shape* of the field is the contract, not the literal value (e.g.
/// `version` differs between Bun and Rust; `recorded_at_iso` is wall-clock).
fn volatile_string_placeholder_for_key(key: &str) -> Option<&'static str> {
    match key {
        "version" => Some("<VERSION>"),
        "bun_version" => Some("<VERSION>"),
        "recorded_at_iso" => Some("<TIMESTAMP:0>"),
        _ => None,
    }
}

fn stable_replace(
    input: &str,
    re: &Regex,
    placeholder_prefix: &str,
    map: &mut BTreeMap<String, String>,
) -> String {
    re.replace_all(input, |caps: &Captures<'_>| {
        let full_match = caps.get(0).unwrap();
        let original = full_match.as_str();

        // If a capture group 1 exists, only the digits/value within it should
        // be replaced (used for the timestamp regex which carries a key
        // prefix like `"created":`). Otherwise the whole match is replaced.
        if let Some(digits) = caps.get(1) {
            let key = digits.as_str().to_string();
            if let Some(existing) = map.get(&key) {
                let prefix_part = &original[..digits.start() - full_match.start()];
                return format!("{prefix_part}{existing}");
            }
            let placeholder = format!("{placeholder_prefix}:{}>", map.len() + 1);
            map.insert(key, placeholder.clone());
            let prefix_part = &original[..digits.start() - full_match.start()];
            return format!("{prefix_part}{placeholder}");
        }

        if let Some(existing) = map.get(original) {
            return existing.clone();
        }
        let placeholder = format!("{placeholder_prefix}:{}>", map.len() + 1);
        map.insert(original.to_string(), placeholder.clone());
        placeholder
    })
    .into_owned()
}

/// Substitute an absolute-path prefix with `placeholder` everywhere it
/// appears in the input. We try both the exact form and a JSON-encoded form
/// (with `\\` escaping on Windows).
fn redact_path_prefix(input: &str, prefix: Option<&Path>, placeholder: &str) -> String {
    let Some(prefix) = prefix else {
        return input.to_string();
    };
    let prefix_str = prefix.to_string_lossy();
    if prefix_str.is_empty() {
        return input.to_string();
    }

    let mut out = input.replace(prefix_str.as_ref(), placeholder);

    // Windows: also handle the JSON-escaped form.
    if prefix_str.contains('\\') {
        let escaped = prefix_str.replace('\\', "\\\\");
        out = out.replace(&escaped, placeholder);
    }

    // POSIX: replace forward-slash form even on Windows recordings.
    let posix = prefix_str.replace('\\', "/");
    if posix != prefix_str {
        out = out.replace(&posix, placeholder);
    }
    out
}

/// After path-prefix substitution we still have backslashes left over in the
/// "tail" (e.g. `<WORKSPACE>\packages\kilo-vscode`). Convert those to forward
/// slashes so fixtures look the same on Windows and macOS/Linux.
fn normalize_path_separators_in_placeholders(input: &str) -> String {
    static TAIL_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r#"<(WORKSPACE|HOME)>(?:\\\\|\\)([^"]*)"#).unwrap());

    TAIL_RE
        .replace_all(input, |caps: &Captures<'_>| {
            let placeholder = &caps[1];
            let tail = caps[2].replace("\\\\", "/").replace('\\', "/");
            format!("<{placeholder}>/{tail}")
        })
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalize_password_and_port() {
        let mut n = Normalizer::new(
            Redactions::default()
                .with_password("supersecret")
                .with_port(54321),
        );
        let out = n.normalize_str("password=supersecret host=127.0.0.1:54321/foo");
        assert_eq!(
            out,
            "password=<KILO_SERVER_PASSWORD> host=127.0.0.1:<PORT>/foo"
        );
    }

    #[test]
    fn normalize_session_ids_are_stable() {
        let mut n = Normalizer::new(Redactions::default());
        let a = n.normalize_str("ses_abcdef0123 followed by ses_abcdef0123");
        // Same ID gets the same placeholder.
        assert_eq!(a, "<SESSION_ID:1> followed by <SESSION_ID:1>");

        let b = n.normalize_str("now ses_zzzzzz9999");
        assert_eq!(b, "now <SESSION_ID:2>");
    }

    #[test]
    fn normalize_message_part_request() {
        let mut n = Normalizer::new(Redactions::default());
        let out = n.normalize_str("msg_aaaaaa prt_bbbbbb evt_cccccc perm_dddddd qst_eeeeee");
        assert_eq!(
            out,
            "<MESSAGE_ID:1> <PART_ID:1> <EVENT_ID:1> <REQUEST_ID:1> <REQUEST_ID:2>"
        );
    }

    #[test]
    fn normalize_uuid() {
        let mut n = Normalizer::new(Redactions::default());
        let out = n.normalize_str("id=01234567-89ab-cdef-0123-456789abcdef");
        assert_eq!(out, "id=<UUID:1>");
    }

    #[test]
    fn normalize_timestamp_keeps_key_intact() {
        let mut n = Normalizer::new(Redactions::default());
        let out = n.normalize_str(r#"{"created":1717182000000,"name":"x"}"#);
        // Numeric digits are replaced; key syntax is preserved.
        assert!(out.starts_with(r#"{"created":<TIMESTAMP:"#));
        assert!(out.ends_with(r#""name":"x"}"#));
    }

    #[test]
    fn normalize_value_round_trips() {
        let mut n = Normalizer::new(Redactions::default());
        let v = json!({
            "id": "ses_abc123def",
            "messages": ["msg_x123abcd", "msg_x123abcd"],
        });
        let out = n.normalize_value(&v);
        let again = n.normalize_value(&out);
        assert_eq!(out, again, "normalize must be idempotent");
    }

    #[test]
    fn duration_buckets_to_seconds() {
        assert_eq!(Normalizer::normalize_duration_ms(0), 0);
        assert_eq!(Normalizer::normalize_duration_ms(450), 0);
        assert_eq!(Normalizer::normalize_duration_ms(550), 1000);
        assert_eq!(Normalizer::normalize_duration_ms(10_024), 10_000);
        assert_eq!(Normalizer::normalize_duration_ms(10_500), 11_000);
    }

    #[test]
    fn normalize_value_does_not_produce_normalize_broke_json_marker() {
        // Regression for the "regex over serialized text" bug: a numeric
        // timestamp value would be substituted with a bare `<TIMESTAMP:N>`
        // token where a JSON number was required, breaking re-parse and
        // landing in the `<NORMALIZE_BROKE_JSON>` recovery path. After fix,
        // numeric timestamps survive as a string token and the value
        // re-parses cleanly.
        let mut n = Normalizer::new(Redactions::default());
        let v = json!({
            "id": "ses_abc123def",
            "created": 1_717_182_000_000_u64,
            "updated": 1_717_182_001_000_u64,
            "nested": {
                "ts": 1_717_182_002_000_u64,
                "frame": 7,
            }
        });
        let out = n.normalize_value(&v);
        // Walk every node and assert no string anywhere starts with the
        // recovery marker.
        fn assert_no_marker(v: &serde_json::Value) {
            match v {
                serde_json::Value::String(s) => {
                    assert!(
                        !s.starts_with("<NORMALIZE_BROKE_JSON>"),
                        "found NORMALIZE_BROKE_JSON marker: {s}"
                    );
                }
                serde_json::Value::Array(arr) => arr.iter().for_each(assert_no_marker),
                serde_json::Value::Object(obj) => obj.values().for_each(assert_no_marker),
                _ => {}
            }
        }
        assert_no_marker(&out);
        // Should be a real object, not a `Value::String("<NORMALIZE_BROKE_JSON>:…")`.
        assert!(out.is_object(), "expected object, got {out}");
        let created = out.get("created").expect("created field present");
        assert_eq!(created.as_str(), Some("<TIMESTAMP:1>"));
        let nested_frame = out
            .get("nested")
            .and_then(|n| n.get("frame"))
            .expect("frame field present");
        assert_eq!(nested_frame.as_u64(), Some(7), "small ints unaffected");
    }

    #[test]
    fn normalize_value_handles_realistic_sse_payload_with_numeric_timestamps() {
        // Mirrors the real-shape `data:` payload Bun emits for a session
        // event: `directory` + `payload.{type, properties}`, where
        // `properties` carries numeric `created`/`updated` fields. The
        // resulting Value must be well-formed JSON and the placeholder must
        // appear as a string token.
        let mut n = Normalizer::new(Redactions::default());
        let v = json!({
            "directory": null,
            "payload": {
                "type": "session.updated",
                "properties": {
                    "sessionID": "ses_realwxyz0123",
                    "created": 1_717_182_000_000_u64,
                    "updated": 1_717_182_001_500_u64,
                    "messageCount": 4
                }
            }
        });
        let out = n.normalize_value(&v);
        assert!(out.is_object(), "expected object, got {out}");
        let props = out
            .get("payload")
            .and_then(|p| p.get("properties"))
            .expect("properties present");
        assert_eq!(
            props.get("sessionID").and_then(|s| s.as_str()),
            Some("<SESSION_ID:1>")
        );
        let created = props.get("created").expect("created");
        assert!(
            created.is_string() && created.as_str().unwrap().starts_with("<TIMESTAMP:"),
            "expected string TIMESTAMP placeholder, got: {created}"
        );
        let updated = props.get("updated").expect("updated");
        assert!(
            updated.is_string() && updated.as_str().unwrap().starts_with("<TIMESTAMP:"),
            "expected string TIMESTAMP placeholder, got: {updated}"
        );
        assert_eq!(
            props.get("messageCount").and_then(|n| n.as_u64()),
            Some(4),
            "non-timestamp small integer must be preserved as a number"
        );
        // And the result round-trips through serde_json without loss.
        let s = serde_json::to_string(&out).expect("serialize");
        let again: serde_json::Value = serde_json::from_str(&s).expect("re-parse");
        assert_eq!(again, out);
    }

    #[test]
    fn normalize_value_redacts_numeric_port_and_volatile_strings() {
        // H1: the recorder produces `{ "parsed": { "port": <number> } }` and
        // `{ "version": "0.1.2" }`. The shipped golden uses `"<PORT>"` and
        // `"<VERSION>"`, so the normalizer must produce those tokens.
        let mut n = Normalizer::new(Redactions::default().with_port(54321));
        let v = json!({
            "parsed": { "host": "127.0.0.1", "port": 54321u32 },
            "health_response_body": { "healthy": true, "version": "0.1.2-bun" },
            "bun_version": "1.1.30",
            "recorded_at_iso": "2025-01-02T03:04:05Z",
        });
        let out = n.normalize_value(&v);
        assert_eq!(
            out.get("parsed")
                .and_then(|p| p.get("port"))
                .and_then(|p| p.as_str()),
            Some("<PORT>")
        );
        assert_eq!(
            out.get("health_response_body")
                .and_then(|h| h.get("version"))
                .and_then(|v| v.as_str()),
            Some("<VERSION>")
        );
        assert_eq!(
            out.get("bun_version").and_then(|v| v.as_str()),
            Some("<VERSION>")
        );
        assert_eq!(
            out.get("recorded_at_iso").and_then(|v| v.as_str()),
            Some("<TIMESTAMP:0>")
        );
    }

    #[test]
    fn workspace_path_prefix_replaced() {
        let mut n = Normalizer::new(
            Redactions::default()
                .with_workspace("D:/Projects/kilocode")
                .with_home("C:/Users/HPotter"),
        );
        let out = n.normalize_str(
            r#"{"path":"D:/Projects/kilocode/foo","home":"C:/Users/HPotter/.kilo"}"#,
        );
        assert_eq!(out, r#"{"path":"<WORKSPACE>/foo","home":"<HOME>/.kilo"}"#);
    }
}
