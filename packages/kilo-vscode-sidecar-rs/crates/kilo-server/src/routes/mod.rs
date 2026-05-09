//! HTTP route handlers split by concern. Every submodule's items are
//! `pub(crate)` and re-exported at lib.rs scope so the `app()` router
//! and the inline test module (`use super::*`) keep resolving the bare
//! handler / helper names. Cross-cutting helpers (resolve_under, slash,
//! git_text, etc.) stay in lib.rs.

pub(crate) mod compat;
pub(crate) mod config;
pub(crate) mod enhance;
pub(crate) mod files;
pub(crate) mod health;
pub(crate) mod indexing;
pub(crate) mod mcp;
pub(crate) mod messages;
pub(crate) mod network;
pub(crate) mod permissions;
pub(crate) mod prompt;
pub(crate) mod pty;
pub(crate) mod registry;
pub(crate) mod sessions;
pub(crate) mod worktree;
