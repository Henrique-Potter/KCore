//! OpenAI OAuth flow — listener, PKCE/JWT crypto, token exchange, URL helpers.
//!
//! Step 5 of the kilo-server module split. The two route handlers
//! (`oauth_authorize`, `oauth_callback`) live in `routes::config`; everything
//! else they call into is in this module tree.
//!
//! Shape:
//! - `listener` — loopback HTTP server that catches the browser redirect.
//! - `crypto` — PKCE secret/challenge + JWT claim parsing.
//! - `tokens` — exchange/refresh + auth-blob shaping.
//! - `url` — query parsing and URL encoding for OAuth requests.
//!
//! Constants for the upstream OpenAI endpoints + pending-flow TTL live here
//! at module scope because they are shared across submodules. They are not
//! re-exported from `lib.rs`; nothing outside `oauth::` (and `routes::config`,
//! which uses them via `super`/`crate::oauth::*`) reads them.

use std::time::Duration;

pub(crate) const OPENAI_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub(crate) const OPENAI_ISSUER: &str = "https://auth.openai.com";
pub(crate) const OPENAI_REDIRECT: &str = "http://localhost:1455/auth/callback";
pub(crate) const OAUTH_PENDING_TTL: Duration = Duration::from_secs(600);

pub(crate) mod crypto;
pub(crate) mod listener;
pub(crate) mod tokens;
pub(crate) mod url;
