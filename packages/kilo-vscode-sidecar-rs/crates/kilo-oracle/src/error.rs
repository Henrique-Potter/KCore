//! Crate-level error type.
//!
//! The oracle is a test harness, not a production runtime, so we lean on a
//! single `OracleError` enum rather than per-module error types. Anything that
//! cannot be neatly categorized lands in [`OracleError::Other`] with a static
//! message, keeping call sites readable.

use std::io;
use std::path::PathBuf;

use thiserror::Error;

/// Result alias used throughout the crate.
pub type OracleResult<T> = Result<T, OracleError>;

#[derive(Debug, Error)]
pub enum OracleError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },

    #[error("io error: {0}")]
    IoBare(#[from] io::Error),

    #[error("sidecar process failed to become ready within {timeout_secs}s")]
    ReadyTimeout { timeout_secs: u64 },

    #[error("sidecar exited before becoming ready (code = {code:?}); stderr = {stderr}")]
    EarlyExit { code: Option<i32>, stderr: String },

    #[error("could not parse readiness line: {raw}")]
    UnparseableReadyLine { raw: String },

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP status {status} for {method} {path}: {body}")]
    HttpStatus {
        method: String,
        path: String,
        status: u16,
        body: String,
    },

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("regex error: {0}")]
    Regex(#[from] regex::Error),

    #[error("scenario aborted: {0}")]
    ScenarioAborted(&'static str),

    #[error("unsupported operation in M0: {0}")]
    UnsupportedM0(&'static str),

    #[error("{0}")]
    Other(String),
}

impl OracleError {
    /// Convenience constructor for free-form errors.
    pub fn other<S: Into<String>>(msg: S) -> Self {
        OracleError::Other(msg.into())
    }
}
