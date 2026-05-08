//! Bun-shape line diff used by the fake-tool runtime.
//!
//! Step 7 of the kilo-server module split: verbatim cut from `lib.rs`. The
//! `slash` / `resolve_under` path helpers now live in `util::paths`.

pub(crate) fn text_diff(path: &str, before: &str, after: &str) -> String {
    let mut out = vec![format!("--- {path}"), format!("+++ {path}")];
    if before == after {
        out.push("@@ no changes @@".to_string());
        return out.join("\n");
    }
    out.push("@@ before @@".to_string());
    out.extend(before.lines().map(|line| format!("-{line}")));
    out.push("@@ after @@".to_string());
    out.extend(after.lines().map(|line| format!("+{line}")));
    out.join("\n")
}

pub(crate) fn diff_stats(before: &str, after: &str) -> (usize, usize) {
    if before == after {
        return (0, 0);
    }
    (after.lines().count(), before.lines().count())
}
