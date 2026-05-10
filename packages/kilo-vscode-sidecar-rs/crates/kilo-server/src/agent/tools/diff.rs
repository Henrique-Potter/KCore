//! GNU-style unified diff producer.
//!
//! Replaces the prior Bun-shape envelope with a hand-rolled LCS line diff so
//! webview renderers and downstream tooling can parse the output as a
//! standard unified patch.

const CONTEXT: usize = 3;
const MERGE_GAP: usize = 6;
const MAX_DIFF_CELLS: usize = 2_000_000;

pub(crate) fn text_diff(path: &str, before: &str, after: &str) -> String {
    unified_diff(path, path, before, after)
}

pub(crate) fn diff_stats(before: &str, after: &str) -> (usize, usize) {
    if before == after {
        return (0, 0);
    }
    if too_large(before, after) {
        return (line_count(after), line_count(before));
    }
    let old = split_lines(before);
    let new = split_lines(after);
    let ops = diff_ops(&old, &new);
    let mut adds = 0usize;
    let mut dels = 0usize;
    for op in &ops {
        match op {
            Op::Insert(_) => adds += 1,
            Op::Delete(_) => dels += 1,
            Op::Equal(_, _) => {}
        }
    }
    (adds, dels)
}

pub(crate) fn unified_diff(old_path: &str, new_path: &str, old: &str, new: &str) -> String {
    if old == new {
        return String::new();
    }
    if too_large(old, new) {
        return omitted_diff(old_path, new_path, old, new);
    }
    let old_lines = split_lines(old);
    let new_lines = split_lines(new);
    let ops = diff_ops(&old_lines, &new_lines);
    let hunks = build_hunks(&ops);
    if hunks.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    if old.is_empty() {
        out.push_str("--- /dev/null\n");
    } else {
        out.push_str(&format!("--- old/{old_path}\n"));
    }
    if new.is_empty() {
        out.push_str("+++ /dev/null\n");
    } else {
        out.push_str(&format!("+++ new/{new_path}\n"));
    }

    render_all(&mut out, &hunks, &ops, &old_lines, &new_lines);
    out
}

fn too_large(old: &str, new: &str) -> bool {
    let old = line_count(old).saturating_add(1);
    let new = line_count(new).saturating_add(1);
    old.saturating_mul(new) > MAX_DIFF_CELLS
}

fn line_count(input: &str) -> usize {
    if input.is_empty() {
        return 0;
    }
    let lines = input
        .as_bytes()
        .iter()
        .filter(|byte| **byte == b'\n')
        .count();
    if input.ends_with('\n') {
        lines
    } else {
        lines + 1
    }
}

fn omitted_diff(old_path: &str, new_path: &str, old: &str, new: &str) -> String {
    let old_count = line_count(old);
    let new_count = line_count(new);
    let mut out = String::new();
    if old.is_empty() {
        out.push_str("--- /dev/null\n");
    } else {
        out.push_str(&format!("--- old/{old_path}\n"));
    }
    if new.is_empty() {
        out.push_str("+++ /dev/null\n");
    } else {
        out.push_str(&format!("+++ new/{new_path}\n"));
    }
    out.push_str(&format!("@@ -1,{old_count} +1,{new_count} @@\n"));
    out.push_str("-[diff omitted: input too large for in-memory diff]\n");
    out.push_str("+[diff omitted: input too large for in-memory diff]\n");
    out
}

#[derive(Clone, Debug)]
struct Line {
    text: String,
    eol: bool,
}

fn split_lines(input: &str) -> Vec<Line> {
    if input.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\n' {
            out.push(Line {
                text: input[start..i].to_string(),
                eol: true,
            });
            i += 1;
            start = i;
        } else {
            i += 1;
        }
    }
    if start < bytes.len() {
        out.push(Line {
            text: input[start..].to_string(),
            eol: false,
        });
    }
    out
}

#[derive(Clone, Debug)]
enum Op {
    Equal(usize, usize),
    Delete(usize),
    Insert(usize),
}

fn diff_ops(old: &[Line], new: &[Line]) -> Vec<Op> {
    let n = old.len();
    let m = new.len();
    if n == 0 && m == 0 {
        return Vec::new();
    }
    if n == 0 {
        return (0..m).map(Op::Insert).collect();
    }
    if m == 0 {
        return (0..n).map(Op::Delete).collect();
    }

    let row = m + 1;
    let mut dp = vec![0u32; (n + 1) * row];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            let idx = i * row + j;
            dp[idx] = if old[i].text == new[j].text && old[i].eol == new[j].eol {
                dp[(i + 1) * row + (j + 1)] + 1
            } else {
                std::cmp::max(dp[(i + 1) * row + j], dp[i * row + (j + 1)])
            };
        }
    }

    let mut ops = Vec::with_capacity(n + m);
    let mut i = 0usize;
    let mut j = 0usize;
    while i < n && j < m {
        if old[i].text == new[j].text && old[i].eol == new[j].eol {
            ops.push(Op::Equal(i, j));
            i += 1;
            j += 1;
        } else if dp[(i + 1) * row + j] >= dp[i * row + (j + 1)] {
            ops.push(Op::Delete(i));
            i += 1;
        } else {
            ops.push(Op::Insert(j));
            j += 1;
        }
    }
    while i < n {
        ops.push(Op::Delete(i));
        i += 1;
    }
    while j < m {
        ops.push(Op::Insert(j));
        j += 1;
    }
    ops
}

#[derive(Debug)]
struct Hunk {
    start: usize,
    end: usize,
}

fn build_hunks(ops: &[Op]) -> Vec<Hunk> {
    let mut changes = Vec::new();
    for (idx, op) in ops.iter().enumerate() {
        if !matches!(op, Op::Equal(_, _)) {
            changes.push(idx);
        }
    }
    if changes.is_empty() {
        return Vec::new();
    }

    let mut hunks: Vec<Hunk> = Vec::new();
    for idx in changes {
        let start = idx.saturating_sub(CONTEXT);
        let end = std::cmp::min(ops.len(), idx + CONTEXT + 1);
        match hunks.last_mut() {
            Some(prev) if start <= prev.end + MERGE_GAP => {
                if end > prev.end {
                    prev.end = end;
                }
            }
            _ => hunks.push(Hunk { start, end }),
        }
    }
    hunks
}

fn render_all(out: &mut String, hunks: &[Hunk], ops: &[Op], old: &[Line], new: &[Line]) {
    for hunk in hunks {
        let mut old_start = 0usize;
        let mut new_start = 0usize;
        let mut old_count = 0usize;
        let mut new_count = 0usize;
        let mut have_old = false;
        let mut have_new = false;

        for op in &ops[hunk.start..hunk.end] {
            match op {
                Op::Equal(i, j) => {
                    if !have_old {
                        old_start = i + 1;
                        have_old = true;
                    }
                    if !have_new {
                        new_start = j + 1;
                        have_new = true;
                    }
                    old_count += 1;
                    new_count += 1;
                }
                Op::Delete(i) => {
                    if !have_old {
                        old_start = i + 1;
                        have_old = true;
                    }
                    old_count += 1;
                }
                Op::Insert(j) => {
                    if !have_new {
                        new_start = j + 1;
                        have_new = true;
                    }
                    new_count += 1;
                }
            }
        }

        let old_header = if old_count == 0 { 0 } else { old_start };
        let new_header = if new_count == 0 { 0 } else { new_start };
        out.push_str(&format!(
            "@@ -{old_header},{old_count} +{new_header},{new_count} @@\n"
        ));

        for op in &ops[hunk.start..hunk.end] {
            match op {
                Op::Equal(i, _) => {
                    let line = &old[*i];
                    out.push(' ');
                    out.push_str(&line.text);
                    out.push('\n');
                    if !line.eol {
                        out.push_str("\\ No newline at end of file\n");
                    }
                }
                Op::Delete(i) => {
                    let line = &old[*i];
                    out.push('-');
                    out.push_str(&line.text);
                    out.push('\n');
                    if !line.eol {
                        out.push_str("\\ No newline at end of file\n");
                    }
                }
                Op::Insert(j) => {
                    let line = &new[*j];
                    out.push('+');
                    out.push_str(&line.text);
                    out.push('\n');
                    if !line.eol {
                        out.push_str("\\ No newline at end of file\n");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unified_diff_matches_gnu_format() {
        let old = "alpha\nbravo\ncharlie\ndelta\necho\n";
        let new = "alpha\nbravo\nCHARLIE\ndelta\necho\n";
        let out = unified_diff("note.txt", "note.txt", old, new);
        let expected = "\
--- old/note.txt
+++ new/note.txt
@@ -1,5 +1,5 @@
 alpha
 bravo
-charlie
+CHARLIE
 delta
 echo
";
        assert_eq!(out, expected);
    }

    #[test]
    fn same_input_produces_empty_diff() {
        let s = "one\ntwo\nthree\n";
        assert_eq!(unified_diff("a", "a", s, s), "");
        assert_eq!(text_diff("a", s, s), "");
        assert_eq!(diff_stats(s, s), (0, 0));
    }

    #[test]
    fn multiple_hunks_in_one_file() {
        let old = "\
l01
l02
l03
l04
l05
l06
l07
l08
l09
l10
l11
l12
l13
l14
l15
l16
l17
l18
l19
l20
";
        let new = "\
l01
l02
L03
l04
l05
l06
l07
l08
l09
l10
l11
l12
l13
l14
l15
l16
l17
L18
l19
l20
";
        let out = unified_diff("f", "f", old, new);
        let hunks: Vec<&str> = out.lines().filter(|l| l.starts_with("@@")).collect();
        assert_eq!(hunks.len(), 2, "expected two hunks, got: {out}");
    }

    #[test]
    fn respects_three_lines_of_context() {
        let old = "a\nb\nc\nd\ne\nf\ng\nh\ni\n";
        let new = "a\nb\nc\nD\ne\nf\ng\nh\ni\n";
        let out = unified_diff("f", "f", old, new);
        let ctx_before: Vec<&str> = out
            .lines()
            .skip_while(|l| !l.starts_with("@@"))
            .skip(1)
            .take_while(|l| l.starts_with(' '))
            .collect();
        assert_eq!(ctx_before, vec![" a", " b", " c"]);
    }

    #[test]
    fn handles_empty_old() {
        let out = unified_diff("note.txt", "note.txt", "", "hello\nworld\n");
        assert!(out.starts_with("--- /dev/null\n"), "got: {out}");
        assert!(out.contains("+++ new/note.txt\n"));
        assert!(out.contains("@@ -0,0 +1,2 @@\n"));
        assert!(out.contains("+hello\n"));
        assert!(out.contains("+world\n"));
    }

    #[test]
    fn handles_empty_new() {
        let out = unified_diff("note.txt", "note.txt", "hello\nworld\n", "");
        assert!(out.contains("--- old/note.txt\n"));
        assert!(out.contains("+++ /dev/null\n"));
        assert!(out.contains("@@ -1,2 +0,0 @@\n"));
        assert!(out.contains("-hello\n"));
        assert!(out.contains("-world\n"));
    }

    #[test]
    fn diff_stats_counts_inserts_and_deletes() {
        let old = "a\nb\nc\n";
        let new = "a\nB\nc\nd\n";
        let (adds, dels) = diff_stats(old, new);
        assert_eq!(adds, 2);
        assert_eq!(dels, 1);
    }

    #[test]
    fn large_diff_omits_quadratic_work() {
        let old = (0..2000).map(|i| format!("old {i}\n")).collect::<String>();
        let new = (0..2000).map(|i| format!("new {i}\n")).collect::<String>();
        let out = unified_diff("big.txt", "big.txt", &old, &new);
        assert!(out.contains("[diff omitted: input too large for in-memory diff]"));
        assert_eq!(diff_stats(&old, &new), (2000, 2000));
    }

    #[test]
    fn no_newline_at_end_marker() {
        let out = unified_diff("f", "f", "alpha\nbravo", "alpha\nBRAVO");
        assert!(
            out.contains("\\ No newline at end of file"),
            "expected no-newline marker, got: {out}"
        );
    }
}
