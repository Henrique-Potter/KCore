//! Per-concern test modules for `kilo-server`.
//!
//! The original `tests.rs` carried 162 tests + ~50 helpers in a single
//! 7k LoC file. The architecture doc forbids that layout (300–800 soft /
//! 1.2k hard limit); this directory splits the suite by concern.
//!
//! Helpers shared across two or more files live in `common`. MCP
//! fixture helpers stayed alongside the local-MCP tests in `mcp_local`
//! and are re-exported (`pub(super)`) so `mcp_remote` can reach them.

mod common;

mod protocol;

mod mcp_local;
mod mcp_remote;

mod permissions;

mod auth_oauth;

mod sessions;

mod files;

mod vcs;

mod worktree;

mod agent_basics;
mod agent_fake_tools;
mod agent_mcp;
mod agent_misc;
mod agent_oauth_stream;
