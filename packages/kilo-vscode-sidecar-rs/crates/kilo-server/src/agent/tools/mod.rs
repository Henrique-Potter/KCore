//! Fake-provider tool runtime.
//!
//! Step 7 of the kilo-server module split: the six fake tools, the
//! `ChatTool` schema definitions, and the apply-patch parser were lifted
//! out of `lib.rs` into this subtree.
//!
//! [`dispatch`] is the only surface `agent::fake` consumes — it match-on-
//! name selects the right implementation. Keeping the seam tight means
//! the real OAuth path (`real_safe_tool_part` / `real_mutating_tool_part`
//! in `agent::parts`) can later be migrated to call `dispatch` too,
//! collapsing the duplication between the live and fake dispatch tables.
//!
//! `agent::fake` MUST NOT import `tools::fs::fake_read` etc. directly —
//! the whole point of `dispatch` is to centralise the match arm.

use std::{path::Path as FsPath, sync::atomic::AtomicBool};

use serde_json::Value;

pub(crate) mod bash;
pub(crate) mod common;
pub(crate) mod defs;
pub(crate) mod diff;
pub(crate) mod encoding;
pub(crate) mod fs;
pub(crate) mod patch;
pub(crate) mod replacers;
pub(crate) mod truncate;
pub(crate) mod webfetch;

use bash::{fake_bash, fake_bash_with_cancel};
use fs::{fake_edit, fake_glob, fake_glob_cancel, fake_grep, fake_read, fake_write};
use patch::fake_apply_patch;

/// The single seam `agent::fake` reaches into. Matches on the canonical
/// (lower-cased, post-repair) tool name and routes to the right impl.
/// Returns the same `(title, output, metadata)` triple the underlying
/// fake-tool functions produce, so the caller can wrap into either a
/// completed or error tool part with the existing `tool_completed` /
/// `tool_error` helpers in `agent::parts`.
///
/// Unknown / `"invalid"` arms are intentionally left to the caller —
/// `agent::fake::fake_tool_part` carries the FakeCall-specific shape
/// (e.g. the `invalid: Some(name)` field) that informs the error
/// message; reproducing it here would just be a forwarding wrapper.
pub(crate) fn dispatch_with_cancel(
    name: &str,
    input: &Value,
    root: &FsPath,
    cancel: Option<&AtomicBool>,
) -> Option<Result<(String, String, Value), String>> {
    Some(match name {
        "read" => fake_read(root, input),
        "glob" => match cancel {
            Some(cancel) => fake_glob_cancel(root, input, Some(cancel)),
            None => fake_glob(root, input),
        },
        "grep" => fake_grep(root, input),
        "write" => fake_write(root, input),
        "edit" => fake_edit(root, input),
        "apply_patch" => fake_apply_patch(root, input),
        "bash" => match cancel {
            Some(cancel) => fake_bash_with_cancel(root, input, Some(cancel)),
            None => fake_bash(root, input),
        },
        _ => return None,
    })
}
