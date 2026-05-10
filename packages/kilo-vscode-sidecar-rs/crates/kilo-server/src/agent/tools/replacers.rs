//! Multi-strategy edit replacer chain — Rust port of Bun's
//! `tool/edit.ts` 9-replacer fallback chain.
//!
//! Each strategy is a free function returning `Option<String>` (the
//! replaced content) — `None` means the strategy did not apply. The
//! [`replace`] dispatcher tries them in declaration order and returns
//! on the first match.
//!
//! Line-ending policy: the dominant ending of `content` is detected
//! up front. `old`/`new` are normalized to LF for matching, then the
//! produced output is converted back to `content`'s native ending so
//! a CRLF file stays CRLF after an edit.

#[derive(Debug, PartialEq, Eq)]
pub enum ReplaceError {
    NotFound,
    MultipleMatches { count: usize },
    EmptyOld,
}

/// Public entrypoint. See module docs.
pub fn replace(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Result<String, ReplaceError> {
    if old.is_empty() {
        return Err(ReplaceError::EmptyOld);
    }
    if old == new {
        return Ok(content.to_string());
    }

    let ending = detect_ending(content);
    let content_lf = to_lf(content);
    let old_lf = to_lf(old);
    let new_lf = to_lf(new);

    let strategies: &[fn(&str, &str, &str, bool) -> Option<Result<String, ReplaceError>>] = &[
        simple,
        line_trimmed,
        block_anchor,
        whitespace_normalized,
        indentation_flexible,
        escape_normalized,
        trimmed_boundary,
        context_aware,
        multi_occurrence,
    ];

    // When a strategy reports MultipleMatches we fall through to the next
    // (smarter) strategy — a more constrained anchor may disambiguate to
    // a single hit. Only if every strategy gives up do we surface the
    // multi-match error (keeping the highest count seen).
    let mut multi: Option<ReplaceError> = None;
    for strategy in strategies {
        match strategy(&content_lf, &old_lf, &new_lf, replace_all) {
            Some(Ok(out)) => return Ok(from_lf(&out, ending)),
            Some(Err(ReplaceError::MultipleMatches { count })) => {
                let keep = match multi {
                    Some(ReplaceError::MultipleMatches { count: prev }) if prev >= count => prev,
                    _ => count,
                };
                multi = Some(ReplaceError::MultipleMatches { count: keep });
            }
            Some(Err(err)) => return Err(err),
            None => {}
        }
    }

    Err(multi.unwrap_or(ReplaceError::NotFound))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ending {
    Lf,
    Crlf,
    Cr,
}

fn detect_ending(text: &str) -> Ending {
    let mut crlf = 0usize;
    let mut lf = 0usize;
    let mut cr = 0usize;
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\r' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\n' {
                crlf += 1;
                i += 2;
                continue;
            }
            cr += 1;
        } else if bytes[i] == b'\n' {
            lf += 1;
        }
        i += 1;
    }
    if crlf >= lf && crlf >= cr && crlf > 0 {
        Ending::Crlf
    } else if cr > lf && cr > 0 {
        Ending::Cr
    } else {
        Ending::Lf
    }
}

fn to_lf(s: &str) -> String {
    // Hot path: most files (and almost all model-authored `old`/`new`)
    // are already LF. Skip both `replace` allocations when there is no
    // `\r` to convert.
    if !s.as_bytes().contains(&b'\r') {
        return s.to_string();
    }
    s.replace("\r\n", "\n").replace('\r', "\n")
}

fn from_lf(s: &str, ending: Ending) -> String {
    match ending {
        Ending::Lf => s.to_string(),
        Ending::Crlf => s.replace('\n', "\r\n"),
        Ending::Cr => s.replace('\n', "\r"),
    }
}

/// Apply a found `search` slice against `content`, honoring `replace_all`
/// and the "exactly one match" rule when not.
fn apply(
    content: &str,
    search: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let first = content.find(search)?;
    if replace_all {
        return Some(Ok(content.replace(search, new)));
    }
    let last = content.rfind(search).unwrap_or(first);
    if first != last {
        // ambiguous — let later strategies try, but record count
        let count = content.matches(search).count();
        return Some(Err(ReplaceError::MultipleMatches { count }));
    }
    let mut out = String::with_capacity(content.len() - search.len() + new.len());
    out.push_str(&content[..first]);
    out.push_str(new);
    out.push_str(&content[first + search.len()..]);
    Some(Ok(out))
}

// 1. Simple: literal find.
fn simple(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    apply(content, old, new, replace_all)
}

// 2. LineTrimmed: match line-blocks where each line equals after trim.
fn line_trimmed(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let original_lines: Vec<&str> = content.split('\n').collect();
    let mut search_lines: Vec<&str> = old.split('\n').collect();
    if search_lines.last() == Some(&"") {
        search_lines.pop();
    }
    if search_lines.is_empty() {
        return None;
    }

    let mut matches: Vec<(usize, usize)> = Vec::new();
    if original_lines.len() < search_lines.len() {
        return None;
    }
    for i in 0..=original_lines.len() - search_lines.len() {
        let ok =
            (0..search_lines.len()).all(|j| original_lines[i + j].trim() == search_lines[j].trim());
        if ok {
            let start = byte_index_of_line(&original_lines, i);
            let end = byte_index_end_of_block(&original_lines, i, search_lines.len());
            matches.push((start, end));
        }
    }
    if matches.is_empty() {
        return None;
    }
    if !replace_all && matches.len() > 1 {
        return Some(Err(ReplaceError::MultipleMatches {
            count: matches.len(),
        }));
    }
    Some(Ok(splice_matches(content, &matches, new, replace_all)))
}

// 3. BlockAnchor: 3+ line search; anchor by first/last trimmed.
fn block_anchor(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let original_lines: Vec<&str> = content.split('\n').collect();
    let mut search_lines: Vec<&str> = old.split('\n').collect();
    if search_lines.last() == Some(&"") {
        search_lines.pop();
    }
    if search_lines.len() < 3 {
        return None;
    }
    let first = search_lines[0].trim();
    let last = search_lines[search_lines.len() - 1].trim();

    let mut matches: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < original_lines.len() {
        if original_lines[i].trim() == first {
            let mut j = i + 2;
            while j < original_lines.len() {
                if original_lines[j].trim() == last {
                    let start = byte_index_of_line(&original_lines, i);
                    let end = byte_index_end_of_block(&original_lines, i, j - i + 1);
                    matches.push((start, end));
                    break;
                }
                j += 1;
            }
        }
        i += 1;
    }
    if matches.is_empty() {
        return None;
    }
    if !replace_all && matches.len() > 1 {
        return Some(Err(ReplaceError::MultipleMatches {
            count: matches.len(),
        }));
    }
    Some(Ok(splice_matches(content, &matches, new, replace_all)))
}

// 4. WhitespaceNormalized: collapse whitespace runs.
fn whitespace_normalized(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let target = collapse_ws(old);
    if target.is_empty() {
        return None;
    }
    let original_lines: Vec<&str> = content.split('\n').collect();
    let find_lines: Vec<&str> = old.split('\n').collect();

    if find_lines.len() <= 1 {
        let mut matches: Vec<(usize, usize)> = Vec::new();
        let mut cursor = 0usize;
        for line in &original_lines {
            let line_len = line.len();
            if collapse_ws(line) == target {
                matches.push((cursor, cursor + line_len));
            }
            cursor += line_len + 1;
        }
        if matches.is_empty() {
            return None;
        }
        if !replace_all && matches.len() > 1 {
            return Some(Err(ReplaceError::MultipleMatches {
                count: matches.len(),
            }));
        }
        return Some(Ok(splice_matches(content, &matches, new, replace_all)));
    }

    let mut matches: Vec<(usize, usize)> = Vec::new();
    if original_lines.len() < find_lines.len() {
        return None;
    }
    for i in 0..=original_lines.len() - find_lines.len() {
        let block: Vec<&str> = original_lines[i..i + find_lines.len()].to_vec();
        if collapse_ws(&block.join("\n")) == target {
            let start = byte_index_of_line(&original_lines, i);
            let end = byte_index_end_of_block(&original_lines, i, find_lines.len());
            matches.push((start, end));
        }
    }
    if matches.is_empty() {
        return None;
    }
    if !replace_all && matches.len() > 1 {
        return Some(Err(ReplaceError::MultipleMatches {
            count: matches.len(),
        }));
    }
    Some(Ok(splice_matches(content, &matches, new, replace_all)))
}

// 5. IndentationFlexible: strip common leading indent on both sides.
fn indentation_flexible(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let normalized_find = strip_common_indent(old);
    if normalized_find.is_empty() {
        return None;
    }
    let original_lines: Vec<&str> = content.split('\n').collect();
    let find_lines: Vec<&str> = old.split('\n').collect();
    if original_lines.len() < find_lines.len() {
        return None;
    }
    let mut matches: Vec<(usize, usize)> = Vec::new();
    for i in 0..=original_lines.len() - find_lines.len() {
        let block_lines: Vec<&str> = original_lines[i..i + find_lines.len()].to_vec();
        let block = block_lines.join("\n");
        if strip_common_indent(&block) == normalized_find {
            let start = byte_index_of_line(&original_lines, i);
            let end = byte_index_end_of_block(&original_lines, i, find_lines.len());
            matches.push((start, end));
        }
    }
    if matches.is_empty() {
        return None;
    }
    if !replace_all && matches.len() > 1 {
        return Some(Err(ReplaceError::MultipleMatches {
            count: matches.len(),
        }));
    }
    Some(Ok(splice_matches(content, &matches, new, replace_all)))
}

// 6. EscapeNormalized: unescape \n \t \\ in old, then simple match.
fn escape_normalized(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let unescaped = unescape(old);
    if unescaped == old {
        return None;
    }
    apply(content, &unescaped, new, replace_all)
}

// 7. TrimmedBoundary: trim leading/trailing whitespace from old.
fn trimmed_boundary(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let trimmed = old.trim();
    if trimmed == old || trimmed.is_empty() {
        return None;
    }
    apply(content, trimmed, new, replace_all)
}

// 8. ContextAware: same anchor logic as line-trimmed but allows up to 2
//    interior lines to differ (50% similarity threshold per Bun).
fn context_aware(
    content: &str,
    old: &str,
    new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    let mut find_lines: Vec<&str> = old.split('\n').collect();
    if find_lines.last() == Some(&"") {
        find_lines.pop();
    }
    if find_lines.len() < 3 {
        return None;
    }
    let original_lines: Vec<&str> = content.split('\n').collect();
    let first = find_lines[0].trim();
    let last = find_lines[find_lines.len() - 1].trim();

    let mut matches: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < original_lines.len() {
        if original_lines[i].trim() != first {
            i += 1;
            continue;
        }
        let mut j = i + 2;
        while j < original_lines.len() {
            if original_lines[j].trim() == last {
                let block_len = j - i + 1;
                if block_len == find_lines.len() {
                    let mut matching = 0usize;
                    let mut total = 0usize;
                    for k in 1..block_len - 1 {
                        let bl = original_lines[i + k].trim();
                        let fl = find_lines[k].trim();
                        if !bl.is_empty() || !fl.is_empty() {
                            total += 1;
                            if bl == fl {
                                matching += 1;
                            }
                        }
                    }
                    let pass = total == 0 || matching * 2 >= total;
                    if pass {
                        let start = byte_index_of_line(&original_lines, i);
                        let end = byte_index_end_of_block(&original_lines, i, block_len);
                        matches.push((start, end));
                    }
                }
                break;
            }
            j += 1;
        }
        i += 1;
    }
    if matches.is_empty() {
        return None;
    }
    if !replace_all && matches.len() > 1 {
        return Some(Err(ReplaceError::MultipleMatches {
            count: matches.len(),
        }));
    }
    Some(Ok(splice_matches(content, &matches, new, replace_all)))
}

// 9. MultiOccurrence: when replace_all=false but multiple exact matches,
//    surface a structured error.
fn multi_occurrence(
    content: &str,
    old: &str,
    _new: &str,
    replace_all: bool,
) -> Option<Result<String, ReplaceError>> {
    if replace_all {
        return None;
    }
    let count = content.matches(old).count();
    if count > 1 {
        return Some(Err(ReplaceError::MultipleMatches { count }));
    }
    None
}

// ---- helpers -----------------------------------------------------------

fn byte_index_of_line(lines: &[&str], i: usize) -> usize {
    let mut idx = 0;
    for line in lines.iter().take(i) {
        idx += line.len() + 1;
    }
    idx
}

fn byte_index_end_of_block(lines: &[&str], start: usize, count: usize) -> usize {
    let mut idx = byte_index_of_line(lines, start);
    for k in 0..count {
        idx += lines[start + k].len();
        if k < count - 1 {
            idx += 1;
        }
    }
    idx
}

/// Replace each `(start, end)` range in `content` with `new`. When
/// `replace_all` is false, `matches` should already contain exactly one
/// entry. Iterates in reverse to keep earlier indices valid as we
/// rewrite from the tail.
fn splice_matches(
    content: &str,
    matches: &[(usize, usize)],
    new: &str,
    replace_all: bool,
) -> String {
    let take = if replace_all { matches.len() } else { 1 };
    let mut out = content.to_string();
    for (start, end) in matches.iter().take(take).rev() {
        out.replace_range(*start..*end, new);
    }
    out
}

fn collapse_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_ws = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !last_ws && !out.is_empty() {
                out.push(' ');
            }
            last_ws = true;
        } else {
            out.push(ch);
            last_ws = false;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

fn strip_common_indent(text: &str) -> String {
    let lines: Vec<&str> = text.split('\n').collect();
    let min_indent = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.bytes()
                .take_while(|b| *b == b' ' || *b == b'\t')
                .count()
        })
        .min()
        .unwrap_or(0);
    lines
        .iter()
        .map(|line| {
            if line.trim().is_empty() {
                line.to_string()
            } else {
                line.chars().skip(min_indent).collect::<String>()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.peek().copied() {
            Some('n') => {
                chars.next();
                out.push('\n');
            }
            Some('t') => {
                chars.next();
                out.push('\t');
            }
            Some('r') => {
                chars.next();
                out.push('\r');
            }
            Some('\\') => {
                chars.next();
                out.push('\\');
            }
            Some('\'') => {
                chars.next();
                out.push('\'');
            }
            Some('"') => {
                chars.next();
                out.push('"');
            }
            Some('`') => {
                chars.next();
                out.push('`');
            }
            _ => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_first_match_only() {
        let out = replace("aaa bbb aaa", "bbb", "XXX", false).unwrap();
        assert_eq!(out, "aaa XXX aaa");
    }

    #[test]
    fn simple_replace_all() {
        let out = replace("foo bar foo", "foo", "X", true).unwrap();
        assert_eq!(out, "X bar X");
    }

    #[test]
    fn line_trimmed_handles_trailing_spaces() {
        let content = "line one  \n  line two\n";
        let old = "line one\nline two";
        let new = "ALPHA\nBETA";
        let out = replace(content, old, new, false).unwrap();
        assert_eq!(out, "ALPHA\nBETA\n");
    }

    #[test]
    fn block_anchor_three_plus_lines() {
        let content = "fn foo() {\n    let inner = different;\n    let mid = 2;\n}\n";
        let old = "fn foo() {\n    let inner = ORIGINAL;\n    let mid = 2;\n}";
        let new = "fn foo() { /* replaced */ }";
        let out = replace(content, old, new, false).unwrap();
        assert!(out.contains("/* replaced */"));
    }

    #[test]
    fn whitespace_normalized_collapses_runs() {
        let content = "let   x  =  1;\n";
        let old = "let x = 1;";
        let new = "let x = 2;";
        let out = replace(content, old, new, false).unwrap();
        assert_eq!(out, "let x = 2;\n");
    }

    #[test]
    fn indentation_flexible_strips_common_indent() {
        let content = "        if (cond) {\n            do_thing();\n        }\n";
        let old = "if (cond) {\n    do_thing();\n}";
        let new = "if (cond) { done(); }";
        let out = replace(content, old, new, false).unwrap();
        assert!(out.contains("if (cond) { done(); }"));
    }

    #[test]
    fn escape_normalized_unescapes_old() {
        let content = "first\nsecond\nthird\n";
        let old = "first\\nsecond";
        let new = "FIRST\nSECOND";
        let out = replace(content, old, new, false).unwrap();
        assert_eq!(out, "FIRST\nSECOND\nthird\n");
    }

    #[test]
    fn trimmed_boundary_strips_outer_ws() {
        let content = "alpha beta gamma";
        let old = "  beta  ";
        let new = "BETA";
        let out = replace(content, old, new, false).unwrap();
        assert_eq!(out, "alpha BETA gamma");
    }

    #[test]
    fn context_aware_tolerates_interior_drift() {
        let content = "fn entry() {\n    a;\n    b;\n}\n";
        let old = "fn entry() {\n    A_DIFFERENT;\n    b;\n}";
        let new = "fn entry() { rewritten }";
        let out = replace(content, old, new, false).unwrap();
        assert!(out.contains("rewritten"));
    }

    #[test]
    fn multi_occurrence_errors_when_not_replace_all() {
        let err = replace("dup\ndup\n", "dup", "X", false).unwrap_err();
        match err {
            ReplaceError::MultipleMatches { count } => assert_eq!(count, 2),
            _ => panic!("expected MultipleMatches"),
        }
    }

    #[test]
    fn empty_old_errors() {
        let err = replace("anything", "", "X", false).unwrap_err();
        assert_eq!(err, ReplaceError::EmptyOld);
    }

    #[test]
    fn not_found_errors() {
        let err = replace("hello", "absent", "X", false).unwrap_err();
        assert_eq!(err, ReplaceError::NotFound);
    }

    #[test]
    fn crlf_preserved_when_old_and_new_use_lf() {
        let content = "alpha\r\nbeta\r\ngamma\r\n";
        let old = "beta";
        let new = "BETA";
        let out = replace(content, old, new, false).unwrap();
        assert_eq!(out, "alpha\r\nBETA\r\ngamma\r\n");
    }

    #[test]
    fn crlf_multiline_replacement() {
        let content = "one\r\ntwo\r\nthree\r\n";
        let old = "one\ntwo";
        let new = "X\nY";
        let out = replace(content, old, new, false).unwrap();
        assert_eq!(out, "X\r\nY\r\nthree\r\n");
    }

    /// Audit ref: replacers.rs:40-58. Before the fix the dispatcher
    /// returned `Err(MultipleMatches)` as soon as `simple` reported it,
    /// even though a later, more constrained strategy could disambiguate
    /// to a single hit. Here `simple` finds `foo` twice; `line_trimmed`
    /// (strategy #2) only matches the line that trims to exactly `foo`.
    #[test]
    fn replacer_chain_falls_through_multi_match_to_smarter_strategy() {
        let content = "foo\nfoo bar\n";
        let out = replace(content, "foo", "BAZ", false)
            .expect("smarter strategy should disambiguate the multi-match from `simple`");
        assert_eq!(out, "BAZ\nfoo bar\n");
    }

    /// Audit ref: F-E2. `to_lf` used to do two full `replace` passes
    /// (two allocations) even when the input was already LF. Verify the
    /// short-circuit returns a string whose capacity matches the input
    /// length exactly — `String::with_capacity(len)` is what
    /// `str::to_string` produces, and `replace` would over-allocate.
    #[test]
    fn to_lf_short_circuits_when_no_carriage_return() {
        let input = "alpha\nbeta\ngamma\n";
        let out = to_lf(input);
        assert_eq!(out, input);
        // Heuristic single-allocation budget: capacity must equal length
        // (the `to_string` fast path), not be inflated by a no-op
        // `replace("\r\n", "\n")` pass that pre-grows the buffer.
        assert_eq!(out.capacity(), input.len());
    }
}
