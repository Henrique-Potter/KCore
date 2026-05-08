//! `read`, `grep`, `write`, `edit` fake-tool runtime.
//!
//! Step 7 of the kilo-server module split: verbatim cut from `lib.rs`. The
//! `slash` / `resolve_under` path helpers now live in `util::paths`; the
//! search/walk helpers (`is_binary`, `list_nodes`, `search_text_target`)
//! live in `routes::files` (re-exported at lib.rs scope).

use std::{fs, path::Path as FsPath};

use serde_json::{json, Value};

use crate::agent::tools::common::{title, tool_usize};
use crate::agent::tools::diff::{diff_stats, text_diff};
use crate::routes::files::{is_binary, list_nodes, search_text_target};
use crate::util::paths::{resolve_under, slash};

pub(crate) const DEFAULT_READ_LIMIT: usize = 2000;
pub(crate) const DEFAULT_GREP_LIMIT: usize = 100;
pub(crate) const MAX_GREP_LINE: usize = 2000;

pub(crate) fn fake_read(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    // Audit Fix 8: schema only advertises `filePath`. Don't accept a `path`
    // synonym at the dispatch site either.
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    let meta = fs::metadata(&target).map_err(|err| format!("Unable to read {path}: {err}"))?;
    if meta.is_dir() {
        return fake_read_dir(root, &target, input);
    }

    fake_read_file(root, &target, input)
}

fn fake_read_dir(
    root: &FsPath,
    dir: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let offset = tool_usize(input, "offset", 1)?;
    let limit = tool_usize(input, "limit", DEFAULT_READ_LIMIT)?;
    let entries = list_nodes(root, dir)
        .into_iter()
        .filter_map(|node| node.get("path").and_then(Value::as_str).map(str::to_string))
        .collect::<Vec<_>>();
    let start = offset - 1;
    let sliced = entries
        .iter()
        .skip(start)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let truncated = start + sliced.len() < entries.len();
    let note = if truncated {
        format!(
            "\n(Showing {} of {} entries. Use 'offset' parameter to read beyond entry {})",
            sliced.len(),
            entries.len(),
            offset + sliced.len()
        )
    } else {
        format!("\n({} entries)", entries.len())
    };
    let output = [
        format!("<path>{}</path>", dir.to_string_lossy()),
        "<type>directory</type>".to_string(),
        "<entries>".to_string(),
        sliced.join("\n"),
        note,
        "</entries>".to_string(),
    ]
    .join("\n");
    let metadata = json!({
        "preview": sliced.iter().take(20).cloned().collect::<Vec<_>>().join("\n"),
        "truncated": truncated,
        "loaded": [],
    });

    Ok((title(root, dir), output, metadata))
}

fn fake_read_file(
    root: &FsPath,
    file: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let offset = tool_usize(input, "offset", 1)?;
    let limit = tool_usize(input, "limit", DEFAULT_READ_LIMIT)?;
    let bytes = fs::read(file)
        .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
    if is_binary(file, &bytes) {
        return Err(format!(
            "Cannot read binary file: {}",
            file.to_string_lossy()
        ));
    }

    let text = String::from_utf8_lossy(&bytes);
    let lines = text.lines().collect::<Vec<_>>();
    let count = lines.len();
    let start = offset - 1;
    if start >= count && !(count == 0 && offset == 1) {
        return Err(format!(
            "Offset {offset} is out of range for this file ({count} lines)"
        ));
    }

    let raw = lines
        .iter()
        .skip(start)
        .take(limit)
        .copied()
        .collect::<Vec<_>>();
    let mut output = [
        format!("<path>{}</path>", file.to_string_lossy()),
        "<type>file</type>".to_string(),
        "<content>\n".to_string(),
    ]
    .join("\n");
    output.push_str(
        &raw.iter()
            .enumerate()
            .map(|(idx, line)| format!("{}: {line}", idx + offset))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let last = offset + raw.len().saturating_sub(1);
    let next = last + 1;
    let truncated = start + raw.len() < count;
    if truncated {
        output.push_str(&format!(
            "\n\n(Showing lines {offset}-{last} of {count}. Use offset={next} to continue.)"
        ));
    } else {
        output.push_str(&format!("\n\n(End of file - total {count} lines)"));
    }
    output.push_str("\n</content>");
    let metadata = json!({
        "preview": raw.iter().take(20).copied().collect::<Vec<_>>().join("\n"),
        "truncated": truncated,
        "loaded": [],
    });

    Ok((title(root, file), output, metadata))
}

pub(crate) fn fake_grep(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let pattern = input
        .get("pattern")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "pattern is required".to_string())?;
    let path = input.get("path").and_then(Value::as_str).unwrap_or(".");
    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    if !target.exists() {
        return Ok((
            pattern.to_string(),
            "No files found".to_string(),
            json!({ "matches": 0, "truncated": false }),
        ));
    }

    let matches = search_text_target(root, &target, pattern, DEFAULT_GREP_LIMIT + 1);
    if matches.is_empty() {
        return Ok((
            pattern.to_string(),
            "No files found".to_string(),
            json!({ "matches": 0, "truncated": false }),
        ));
    }

    let total = matches.len();
    let truncated = total > DEFAULT_GREP_LIMIT;
    let final_matches = matches.iter().take(DEFAULT_GREP_LIMIT).collect::<Vec<_>>();
    let mut output = vec![format!(
        "Found {total} matches{}",
        if truncated {
            format!(" (showing first {DEFAULT_GREP_LIMIT})")
        } else {
            String::new()
        }
    )];
    let mut current = String::new();
    for item in final_matches {
        let rel = item["path"]["text"].as_str().unwrap_or_default();
        let path = slash(&root.join(rel));
        if current != path {
            if !current.is_empty() {
                output.push(String::new());
            }
            current = path.clone();
            output.push(format!("{path}:"));
        }
        let line = item["line_number"].as_u64().unwrap_or_default();
        let mut text = item["lines"]["text"]
            .as_str()
            .unwrap_or_default()
            .trim_end_matches(['\r', '\n'])
            .to_string();
        if text.len() > MAX_GREP_LINE {
            text = format!("{}...", &text[..MAX_GREP_LINE]);
        }
        output.push(format!("  Line {line}: {text}"));
    }
    if truncated {
        output.push(String::new());
        output.push(format!(
            "(Results truncated: showing {DEFAULT_GREP_LIMIT} of {total} matches ({} hidden). Consider using a more specific path or pattern.)",
            total - DEFAULT_GREP_LIMIT
        ));
    }

    Ok((
        pattern.to_string(),
        output.join("\n"),
        json!({ "matches": total, "truncated": truncated }),
    ))
}

pub(crate) fn fake_write(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let content = input
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| "content is required".to_string())?;
    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    let exists = target.exists();
    let before = if exists {
        fs::read_to_string(&target)
            .map_err(|err| format!("Unable to read {}: {err}", target.to_string_lossy()))?
    } else {
        String::new()
    };
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
    }
    fs::write(&target, content)
        .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?;

    let diff = text_diff(path, &before, content);
    let (add, del) = diff_stats(&before, content);
    let diagnostics = crate::agent::diagnostics::diagnostics_for(path, content);
    let metadata = json!({
        "filepath": slash(&target),
        "file": path,
        "path": path,
        "exists": exists,
        "diff": diff,
        "filediff": diff,
        "additions": add,
        "deletions": del,
        "diagnostics": diagnostics,
    });

    Ok((
        title(root, &target),
        "Wrote file successfully.".to_string(),
        metadata,
    ))
}

pub(crate) fn fake_edit(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let old = input
        .get("oldString")
        .and_then(Value::as_str)
        .ok_or_else(|| "oldString is required".to_string())?;
    let new = input
        .get("newString")
        .and_then(Value::as_str)
        .ok_or_else(|| "newString is required".to_string())?;
    if old == new {
        return Err("oldString and newString must be different".to_string());
    }

    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    let before = if old.is_empty() {
        fs::read_to_string(&target).unwrap_or_default()
    } else {
        fs::read_to_string(&target)
            .map_err(|err| format!("Unable to read {}: {err}", target.to_string_lossy()))?
    };
    let after = if old.is_empty() {
        new.to_string()
    } else if input
        .get("replaceAll")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let count = before.matches(old).count();
        if count == 0 {
            return Err("oldString was not found".to_string());
        }
        before.replace(old, new)
    } else {
        let count = before.matches(old).count();
        if count == 0 {
            return Err("oldString was not found".to_string());
        }
        if count > 1 {
            return Err(format!(
                "oldString matched {count} times; set replaceAll to true or provide a unique match"
            ));
        }
        before.replacen(old, new, 1)
    };

    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
    }
    fs::write(&target, &after)
        .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?;

    let diff = text_diff(path, &before, &after);
    let (add, del) = diff_stats(&before, &after);
    let diagnostics = crate::agent::diagnostics::diagnostics_for(path, &after);
    let metadata = json!({
        "file": path,
        "path": path,
        "diff": diff,
        "filediff": diff,
        "additions": add,
        "deletions": del,
        "diagnostics": diagnostics,
    });

    Ok((
        title(root, &target),
        "Edit applied successfully.".to_string(),
        metadata,
    ))
}
