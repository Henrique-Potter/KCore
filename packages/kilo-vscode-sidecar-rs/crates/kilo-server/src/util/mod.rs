//! Cross-cutting utilities used by handlers, the agent loop, and the OAuth
//! flow. Step 8 of the kilo-server module split lifted these out of `lib.rs`
//! into typed seams so the crate root stays narrow.

pub(crate) mod cursor;
pub(crate) mod encoding;
pub(crate) mod git;
pub(crate) mod paths;
