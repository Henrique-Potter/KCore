//! Lightweight syntax-diagnostic surface for the file edit tools.
//!
//! Bun's edit tool returns diagnostics from the LSP layer
//! (`packages/opencode/src/lsp`). The Rust port doesn't have an LSP
//! client yet, so we ship a smaller, dependency-free surface: per-file
//! syntax checks that catch the most common edit mistakes (broken JSON
//! configs, unmatched braces in code, etc.). The result shape mirrors
//! Bun's:
//!
//! ```json
//! [{ "severity": "error", "line": 3, "column": 12, "message": "..." }]
//! ```
//!
//! Returning an empty list is fine — the agent should not block on
//! the absence of diagnostics. Returning a non-empty list lets the
//! model self-correct on the next turn.
//!
//! Future expansion: tree-sitter parse errors for many languages, and
//! eventually a real LSP client. For now the heuristics are deliberately
//! conservative — false positives are worse than no signal.

use serde_json::{json, Value};
use std::path::Path;

/// Run all syntax checks applicable to `path`'s extension against the
/// post-edit `content`. Returns a JSON array suitable for the
/// `metadata.diagnostics` field on `tool` parts. The array is empty
/// when no checks ran or all checks passed.
pub(crate) fn diagnostics_for(path: &str, content: &str) -> Value {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let issues: Vec<Value> = match ext.as_str() {
        "json" => check_json(content),
        "rs" | "go" | "ts" | "tsx" | "js" | "jsx" | "java" | "c" | "cpp" | "cs" => {
            check_balanced_brackets(content)
        }
        _ => Vec::new(),
    };
    Value::Array(issues)
}

/// Parse `content` as JSON. Failure produces a single diagnostic with
/// the parser's reported line/column. Successful parses return empty.
fn check_json(content: &str) -> Vec<Value> {
    if content.trim().is_empty() {
        return Vec::new();
    }
    match serde_json::from_str::<Value>(content) {
        Ok(_) => Vec::new(),
        Err(err) => vec![json!({
            "severity": "error",
            "line": err.line() as i64,
            "column": err.column() as i64,
            "message": err.to_string(),
            "source": "kilo-syntax-check",
        })],
    }
}

/// Surface unbalanced `()`, `[]`, `{}` in source files. Walks the text
/// once, ignores brackets inside string literals (`"..."` and `'...'`)
/// and line/block comments. Reports the line of the first mismatch.
/// This catches the most common edit-tool failure mode (a runaway
/// regex or an off-by-one substitution that orphaned a brace) without
/// pulling in a parser.
fn check_balanced_brackets(content: &str) -> Vec<Value> {
    let bytes = content.as_bytes();
    let mut stack: Vec<(u8, usize, usize)> = Vec::new(); // (char, line, col)
    let mut line = 1usize;
    let mut col = 1usize;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\n' => {
                line += 1;
                col = 1;
                i += 1;
                continue;
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                // Line comment — skip to newline.
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                    col += 1;
                }
                continue;
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                // Block comment — skip to */.
                i += 2;
                col += 2;
                while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    if bytes[i] == b'\n' {
                        line += 1;
                        col = 1;
                    } else {
                        col += 1;
                    }
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
                col += 2;
                continue;
            }
            b'"' | b'\'' => {
                // String literal — skip with backslash escapes.
                let quote = b;
                i += 1;
                col += 1;
                while i < bytes.len() && bytes[i] != quote {
                    if bytes[i] == b'\\' && i + 1 < bytes.len() {
                        i += 2;
                        col += 2;
                        continue;
                    }
                    if bytes[i] == b'\n' {
                        line += 1;
                        col = 1;
                    } else {
                        col += 1;
                    }
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1;
                    col += 1;
                }
                continue;
            }
            b'(' | b'[' | b'{' => {
                stack.push((b, line, col));
            }
            b')' | b']' | b'}' => {
                let want = match b {
                    b')' => b'(',
                    b']' => b'[',
                    b'}' => b'{',
                    _ => unreachable!(),
                };
                match stack.pop() {
                    Some((opener, _, _)) if opener == want => {}
                    Some((opener, oline, ocol)) => {
                        return vec![json!({
                            "severity": "error",
                            "line": line as i64,
                            "column": col as i64,
                            "message": format!(
                                "mismatched bracket: closing `{}` does not match opener `{}` at line {oline} col {ocol}",
                                b as char, opener as char,
                            ),
                            "source": "kilo-syntax-check",
                        })];
                    }
                    None => {
                        return vec![json!({
                            "severity": "error",
                            "line": line as i64,
                            "column": col as i64,
                            "message": format!("unexpected closing `{}`", b as char),
                            "source": "kilo-syntax-check",
                        })];
                    }
                }
            }
            _ => {}
        }
        i += 1;
        col += 1;
    }
    if let Some((opener, oline, ocol)) = stack.first() {
        return vec![json!({
            "severity": "error",
            "line": *oline as i64,
            "column": *ocol as i64,
            "message": format!("unclosed bracket `{}` opened here", *opener as char),
            "source": "kilo-syntax-check",
        })];
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_clean_returns_empty() {
        let diags = diagnostics_for("a.json", r#"{"a":1,"b":[1,2]}"#);
        assert_eq!(diags, json!([]));
    }

    #[test]
    fn json_broken_returns_diagnostic() {
        let diags = diagnostics_for("a.json", r#"{"a":}"#);
        let arr = diags.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["severity"], "error");
        assert_eq!(arr[0]["source"], "kilo-syntax-check");
    }

    #[test]
    fn rs_balanced_returns_empty() {
        let diags = diagnostics_for(
            "x.rs",
            "fn main() { let s = \"a)b}c\"; if true { return; } }",
        );
        assert_eq!(diags, json!([]));
    }

    #[test]
    fn rs_unbalanced_returns_diagnostic() {
        let diags = diagnostics_for("x.rs", "fn main() { let x = (1 + 2; }");
        let arr = diags.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["severity"], "error");
        assert!(arr[0]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("`(`"));
    }

    #[test]
    fn rs_string_brackets_are_ignored() {
        // Brackets inside strings should not affect balance.
        let diags = diagnostics_for(
            "x.rs",
            "fn f() { let s = \"this has unbalanced ) brackets ]\"; }",
        );
        assert_eq!(diags, json!([]));
    }

    #[test]
    fn rs_line_comment_brackets_are_ignored() {
        let diags = diagnostics_for("x.rs", "fn f() {\n  // ) ] }\n}");
        assert_eq!(diags, json!([]));
    }

    #[test]
    fn unknown_extension_returns_empty() {
        let diags = diagnostics_for("a.txt", "anything goes ) ] }");
        assert_eq!(diags, json!([]));
    }

    #[test]
    fn empty_json_returns_empty() {
        let diags = diagnostics_for("a.json", "");
        assert_eq!(diags, json!([]));
    }
}
