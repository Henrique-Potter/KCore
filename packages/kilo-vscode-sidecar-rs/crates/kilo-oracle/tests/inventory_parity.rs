//! Inventory ↔ SDK parity test.
//!
//! Asserts that every (method, path) row listed in
//! `packages/kilo-vscode-sidecar-rs/docs/route-inventory.md` corresponds to
//! a real call site emitted by the generated SDK at
//! `packages/sdk/js/src/v2/gen/sdk.gen.ts`. Catches the kind of mismatch
//! that surfaced in the recent ada-reviewer pass for `Pty.update`
//! (inventory said PATCH, SDK emits PUT).
//!
//! The parity is one-directional: the SDK is the source of truth, and the
//! inventory is the human-curated whitelist of "what the extension uses".
//! It is fine for the SDK to expose more routes than the inventory lists.
//! It is **not** fine for the inventory to claim a method/path the SDK
//! does not emit.

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    // <crate>/../../../../   →  packages/kilo-vscode-sidecar-rs/crates/kilo-oracle
    // We need three "parent()" calls to reach the repo root:
    //   manifest = .../packages/kilo-vscode-sidecar-rs/crates/kilo-oracle
    //   parent   = .../packages/kilo-vscode-sidecar-rs/crates
    //   parent   = .../packages/kilo-vscode-sidecar-rs
    //   parent   = .../packages
    //   parent   = .../<repo root>
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn sdk_path() -> PathBuf {
    repo_root().join("packages/sdk/js/src/v2/gen/sdk.gen.ts")
}

fn inventory_path() -> PathBuf {
    repo_root().join("packages/kilo-vscode-sidecar-rs/docs/route-inventory.md")
}

/// Extract `(METHOD, "/url")` pairs from the SDK's `.<method><...>({ url: "..." ... })`
/// emission shape. The generator is uniform: every operation ends with
/// `<method>(<options>)` followed within ~6 lines by `url: "..."`.
fn parse_sdk_calls(src: &str) -> HashSet<(String, String)> {
    static RE: Lazy<Regex> = Lazy::new(|| {
        // `.<method><...>(` then any chars (incl. newline + indented `{`),
        // then a `url: "..."` literal. The SDK generator alternates between
        // single-line `({ url: ... })` and multi-line `(\n  {\n    url: ... })`
        // shapes; both are accepted.
        Regex::new(
            r#"\.(get|post|put|patch|delete|head)<[^>]*>\(\s*\{[\s\S]{0,400}?url:\s*"([^"]+)""#,
        )
        .unwrap()
    });
    let mut out: HashSet<(String, String)> = HashSet::new();
    for cap in RE.captures_iter(src) {
        let method = cap[1].to_uppercase();
        let url = cap[2].to_string();
        out.insert((method, url));
    }
    out
}

/// Extract `(METHOD, /path)` pairs from inventory markdown table rows.
/// Rows look like `| GET | \`/global/health\` | ... |`.
///
/// Rows whose final "Call sites" column starts with `Rust sidecar` are
/// recognized as **Rust-only baseline routes** (M11+ surfaces that exist
/// in the Rust sidecar but are not yet emitted by the generated SDK).
/// Those rows are excluded from the SDK-parity check — they have no SDK
/// counterpart by design.
fn parse_inventory_rows(src: &str) -> Vec<(String, String, usize)> {
    static ROW: Lazy<Regex> = Lazy::new(|| {
        // Method is a known HTTP verb; path is wrapped in single backticks
        // and starts with `/`. We restrict to those rows so the markdown
        // table-header rows (`| M | Path | ... |`) are skipped.
        Regex::new(r#"^\|\s*(GET|POST|PUT|PATCH|DELETE|HEAD)\s*\|\s*`(/[^`]+)`"#).unwrap()
    });
    static RUST_ONLY: Lazy<Regex> = Lazy::new(|| {
        // Last "Call sites" cell. `\|([^|]+)\|\s*$` captures the final
        // table cell; we then trim and check the "Rust sidecar" prefix.
        Regex::new(r#"\|([^|]+)\|\s*$"#).unwrap()
    });
    let mut out = Vec::new();
    for (i, line) in src.lines().enumerate() {
        let Some(cap) = ROW.captures(line) else {
            continue;
        };
        if let Some(last) = RUST_ONLY.captures(line) {
            let cell = last[1].trim();
            if cell.starts_with("Rust sidecar") || cell.starts_with("Rust-only") {
                continue;
            }
        }
        out.push((cap[1].to_string(), cap[2].to_string(), i + 1));
    }
    out
}

#[test]
fn route_inventory_methods_match_sdk_emission() {
    let sdk_src = std::fs::read_to_string(sdk_path())
        .unwrap_or_else(|e| panic!("read SDK at {}: {e}", sdk_path().display()));
    let inventory_src = std::fs::read_to_string(inventory_path())
        .unwrap_or_else(|e| panic!("read inventory at {}: {e}", inventory_path().display()));

    let sdk_calls = parse_sdk_calls(&sdk_src);
    let inventory_rows = parse_inventory_rows(&inventory_src);
    assert!(
        !inventory_rows.is_empty(),
        "expected to find at least one inventory row in {}",
        inventory_path().display()
    );
    assert!(
        !sdk_calls.is_empty(),
        "expected to extract at least one (method, url) pair from {}",
        sdk_path().display()
    );

    let mut mismatches: Vec<String> = Vec::new();
    for (method, path, line) in &inventory_rows {
        // Inventory uses `/path/{param}`; SDK emits `/path/{param}` too —
        // both come from the same OpenAPI schema. Exact match.
        let key = (method.clone(), path.clone());
        if !sdk_calls.contains(&key) {
            // Did the SDK emit *some* method for this path? Surface that
            // for diagnostic clarity.
            let other_methods: Vec<&str> = sdk_calls
                .iter()
                .filter(|(_, p)| p == path)
                .map(|(m, _)| m.as_str())
                .collect();
            mismatches.push(format!(
                "  {}:{} `{} {}` — SDK emits {:?} for this path",
                inventory_path().display(),
                line,
                method,
                path,
                other_methods,
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "route-inventory.md disagrees with packages/sdk/js/src/v2/gen/sdk.gen.ts:\n{}",
        mismatches.join("\n")
    );
}

#[test]
fn pty_update_is_put_in_both_sources() {
    // Targeted regression test for the H3 finding: if a future inventory
    // edit reverts to PATCH or the SDK ever changes, we want a focused
    // failure rather than a generic mismatch dump.
    let sdk_src = std::fs::read_to_string(sdk_path()).expect("read SDK");
    let inventory_src = std::fs::read_to_string(inventory_path()).expect("read inventory");

    let sdk_calls = parse_sdk_calls(&sdk_src);
    assert!(
        sdk_calls.contains(&("PUT".to_string(), "/pty/{ptyID}".to_string())),
        "SDK must emit PUT /pty/{{ptyID}} for Pty.update",
    );

    let inventory_rows = parse_inventory_rows(&inventory_src);
    let pty_update_rows: Vec<_> = inventory_rows
        .iter()
        .filter(|(_, path, _)| path == "/pty/{ptyID}")
        .collect();
    assert!(
        !pty_update_rows.is_empty(),
        "inventory must list /pty/{{ptyID}} (Pty.update + Pty.delete)"
    );
    let has_put = pty_update_rows.iter().any(|(m, _, _)| m == "PUT");
    let has_patch = pty_update_rows.iter().any(|(m, _, _)| m == "PATCH");
    assert!(
        has_put,
        "inventory row for /pty/{{ptyID}} must include PUT for Pty.update"
    );
    assert!(
        !has_patch,
        "inventory row for /pty/{{ptyID}} must NOT include PATCH (Pty.update is PUT)"
    );
}
