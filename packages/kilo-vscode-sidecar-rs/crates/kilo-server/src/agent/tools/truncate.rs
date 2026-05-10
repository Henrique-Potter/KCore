//! Tool-output post-processing parity for Bun's `Truncate.Service`.
//!
//! Bun's [`tool/tool.ts:91-130`] post-processes every tool result: when the
//! output exceeds 50 KiB or 2000 lines, the full text is written to disk
//! under `<state_dir>/kilo/truncate/<id>.txt` and the tool result keeps
//! only a preview plus `metadata.outputPath` pointing at the file. The
//! Bun helper that does the disk write lives at
//! [`tool/truncate.ts`](../../../../../../opencode/src/tool/truncate.ts).
//!
//! This module is the Rust port. It runs inside [`tool_completed`]
//! (`agent::parts`) before the existing 1 MiB ceiling
//! ([`crate::limits::truncate_tool_output`]) — which now serves as a hard
//! safety net for the rare case where a tool emits a >1 MiB **preview**
//! (impossible by construction here, kept for defense in depth).
//!
//! `bash` already populates `metadata.truncated` itself; per Bun parity
//! ("`if (result.metadata.truncated !== undefined) return result`") any
//! caller that pre-set the flag (true OR false) is left alone — that is
//! how the bash tool keeps its tighter 64 KiB local cap without us
//! double-truncating.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::limits::TRUNCATION_SENTINEL;

/// 50 KiB. Output above this size is moved to disk.
pub(crate) const MAX_BYTES: usize = 50 * 1024;
/// 2000 lines. Output above this line count is moved to disk.
pub(crate) const MAX_LINES: usize = 2000;

/// Result of post-processing a tool output.
pub(crate) struct TruncateResult {
    pub preview: String,
    pub output_path: Option<PathBuf>,
    pub truncated: bool,
    pub original_bytes: usize,
    pub original_lines: usize,
}

/// Post-process a tool's textual output.
///
/// Returns the input unchanged when the output fits both limits. When it
/// exceeds either limit, writes the full text to
/// `<state_dir>/kilo/truncate/<unique>.txt` and returns a preview plus a
/// footer pointing at the saved file. The footer ends with
/// [`TRUNCATION_SENTINEL`] so the existing UI/test detection of
/// truncation keeps working without modification.
///
/// `state_dir` is typically [`kilo_store::Store::resolve_state_dir`]; the
/// caller passes it in so unit tests can redirect to a tmpdir without
/// touching env vars.
pub(crate) fn truncate_for_tool(state_dir: &Path, output: &str) -> TruncateResult {
    // Cheap line count: count `\n` and add one for the trailing partial line
    // when the text is non-empty. Matches Bun's `text.split("\n").length`.
    let original_bytes = output.len();
    let original_lines = if output.is_empty() {
        0
    } else {
        output.bytes().filter(|b| *b == b'\n').count() + 1
    };

    if original_bytes <= MAX_BYTES && original_lines <= MAX_LINES {
        return TruncateResult {
            preview: output.to_string(),
            output_path: None,
            truncated: false,
            original_bytes,
            original_lines,
        };
    }

    let dir = state_dir.join("kilo").join("truncate");
    let file = dir.join(format!("{}.txt", unique_id()));
    let path_for_footer = match write_full_output(&dir, &file, output) {
        Ok(()) => Some(file.clone()),
        Err(_) => None,
    };

    let preview_body = preview(output);
    let path_str = path_for_footer
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "<unavailable>".to_string());
    let dropped_bytes = original_bytes.saturating_sub(preview_body.len());
    let dropped_lines = original_lines.saturating_sub(line_count(&preview_body));
    let footer = format!(
        "\n\n[Output truncated: {dropped_bytes} more bytes / {dropped_lines} more lines. Full content at {path_str}]{TRUNCATION_SENTINEL}"
    );
    let mut preview_out = preview_body;
    preview_out.push_str(&footer);

    TruncateResult {
        preview: preview_out,
        output_path: path_for_footer,
        truncated: true,
        original_bytes,
        original_lines,
    }
}

fn preview(output: &str) -> String {
    // First MAX_LINES split-elements OR first MAX_BYTES, whichever runs
    // out first. Bun computes `lines = text.split("\n")` and keeps
    // `lines.length <= maxLines` — so a string with N newlines maps to
    // N+1 split-elements. We accept up to `MAX_LINES - 1` newlines in
    // the preview so the resulting array length matches Bun. Snap to
    // a char boundary to stay valid UTF-8.
    let mut newlines_taken = 0usize;
    let mut end = 0usize;
    for (i, ch) in output.char_indices() {
        let next = i + ch.len_utf8();
        if next > MAX_BYTES {
            break;
        }
        if ch == '\n' {
            if newlines_taken + 1 >= MAX_LINES {
                // Stop before this newline so the body has exactly
                // MAX_LINES - 1 newlines = MAX_LINES split-elements.
                break;
            }
            newlines_taken += 1;
        }
        end = next;
    }
    output[..end].to_string()
}

fn line_count(s: &str) -> usize {
    if s.is_empty() {
        0
    } else {
        s.bytes().filter(|b| *b == b'\n').count() + 1
    }
}

fn write_full_output(dir: &Path, file: &Path, output: &str) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    fs::write(file, output)?;
    Ok(())
}

/// Generate a unique filename without pulling in the `uuid` crate.
/// `<unix_nanos>_<process_seq>` is collision-free across processes (nanos
/// differ) and within a process (atomic counter).
fn unique_id() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("tool_{nanos}_{seq}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp_state_dir() -> PathBuf {
        static IDS: AtomicU64 = AtomicU64::new(0);
        let seq = IDS.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("kilo-truncate-test-{nanos}-{seq}"))
    }

    #[test]
    fn small_output_passes_through_unchanged() {
        let dir = tmp_state_dir();
        let out = "hello\nworld";
        let result = truncate_for_tool(&dir, out);
        assert!(!result.truncated);
        assert!(result.output_path.is_none());
        assert_eq!(result.preview, out);
        assert_eq!(result.original_bytes, out.len());
        assert_eq!(result.original_lines, 2);
        // Should NOT have created the directory for a short input.
        assert!(!dir.join("kilo").join("truncate").exists());
    }

    #[test]
    fn output_over_50kb_writes_to_disk_and_returns_preview() {
        let dir = tmp_state_dir();
        let big = "x".repeat(MAX_BYTES + 4096);
        let result = truncate_for_tool(&dir, &big);
        assert!(result.truncated);
        let path = result.output_path.expect("output path");
        assert!(path.exists(), "full output must be persisted on disk");
        let on_disk = fs::read_to_string(&path).unwrap();
        assert_eq!(on_disk, big);
        // Preview body is bounded by MAX_BYTES; full preview includes a
        // small footer on top.
        assert!(result.preview.len() <= MAX_BYTES + 1024);
        assert_eq!(result.original_bytes, big.len());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn output_over_2000_lines_writes_to_disk_and_returns_preview() {
        let dir = tmp_state_dir();
        // 3000 short lines — well under 50 KiB.
        let big = (0..3000)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(big.len() < MAX_BYTES);
        let result = truncate_for_tool(&dir, &big);
        assert!(result.truncated);
        assert!(result.output_path.is_some());
        assert_eq!(result.original_lines, 3000);
        // Preview keeps at most MAX_LINES; we count newlines on the body
        // before the footer to verify (footer adds \n\n).
        let body_end = result
            .preview
            .find("\n\n[Output truncated:")
            .expect("footer present");
        let body = &result.preview[..body_end];
        let body_lines = body.bytes().filter(|b| *b == b'\n').count() + 1;
        assert!(
            body_lines <= MAX_LINES,
            "body lines {} exceeds MAX_LINES {}",
            body_lines,
            MAX_LINES
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn preview_contains_truncation_footer_with_path() {
        let dir = tmp_state_dir();
        let big = "x".repeat(MAX_BYTES + 100);
        let result = truncate_for_tool(&dir, &big);
        assert!(result.truncated);
        let path = result.output_path.expect("output path");
        assert!(
            result.preview.contains("[Output truncated:"),
            "missing truncation footer"
        );
        assert!(
            result.preview.contains(&path.display().to_string()),
            "footer must reference saved file path"
        );
        // Existing UI sentinel must remain at the end so the
        // `tool_completed_truncates_large_outputs_at_common_boundary`
        // test (and any UI consumer) keeps working.
        assert!(result.preview.ends_with(TRUNCATION_SENTINEL));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unique_id_does_not_collide_within_process() {
        let a = unique_id();
        let b = unique_id();
        assert_ne!(a, b);
    }

    /// Bun parity for `result.metadata.truncated !== undefined`: a tool
    /// that already manages its own truncation (e.g. `bash`) sets the
    /// flag on the metadata. The post-processor wrapper in
    /// `agent::parts::tool_completed` MUST treat that as a no-op. This
    /// test exercises that wrapper's metadata branch directly.
    #[test]
    fn preserves_truncated_flag_from_caller() {
        use crate::agent::parts::tool_completed;
        use serde_json::json;

        // Output huge enough that the post-processor would normally
        // truncate, but caller already declared `truncated: false`.
        let big = "y".repeat(MAX_BYTES + 4096);
        let part = tool_completed(
            "msg",
            "prt",
            0,
            "bash",
            "call",
            &json!({}),
            "title".to_string(),
            big.clone(),
            json!({ "truncated": false, "exit": 0 }),
            1,
        );

        // The metadata flag the caller set must survive verbatim, and
        // the post-processor must NOT have added an `outputPath` /
        // `originalBytes` / `originalLines` overlay.
        let meta = &part["state"]["metadata"];
        assert_eq!(meta["truncated"], json!(false));
        assert!(meta.get("outputPath").is_none());
        assert!(meta.get("originalBytes").is_none());
        assert!(meta.get("originalLines").is_none());
        // Output is unchanged by the truncate post-processor (the 1 MiB
        // safety net does not trip at 50 KiB+).
        let out = part["state"]["output"].as_str().unwrap();
        assert_eq!(out.len(), big.len());
    }
}
