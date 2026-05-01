//! Parser for the Bun sidecar's readiness stdout line.
//!
//! The Bun sidecar prints exactly:
//!
//! ```text
//! kilo server listening on http://127.0.0.1:<port>
//! ```
//!
//! See `packages/opencode/src/cli/cmd/serve.ts` (the `console.log` near the
//! top of the file). The VS Code extension parses the same line in
//! `packages/kilo-vscode/src/services/cli-backend/server-utils.ts` with the
//! regex `listening on http:\/\/[\w.]+:(\d+)`. We mirror that regex exactly
//! so the oracle and the extension never disagree on what "ready" means.

use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::error::{OracleError, OracleResult};

static READY_RE: Lazy<Regex> = Lazy::new(|| {
    // Mirrors parseServerPort() in server-utils.ts. We additionally capture
    // the host because later milestones may want to assert loopback only.
    Regex::new(r"listening on http://(?P<host>[\w.]+):(?P<port>\d+)")
        .expect("static readiness regex must compile")
});

/// Structured form of a parsed readiness line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyLine {
    pub host: String,
    pub port: u16,
    /// The raw line, including any leading text before "listening on".
    pub raw: String,
}

impl ReadyLine {
    /// Build a normalized form suitable for golden fixtures.
    ///
    /// The host is preserved (we want to assert it's loopback), but the port
    /// and any trailing whitespace are replaced with placeholders.
    pub fn into_fixture_string(&self) -> String {
        format!("kilo server listening on http://{}:<PORT>", self.host)
    }
}

/// Parse the first readiness line found in `output`. Returns `None` if no
/// readiness signature is present yet (the caller is typically reading
/// stdout incrementally).
pub fn parse_ready_line(output: &str) -> Option<ReadyLine> {
    for line in output.lines() {
        if let Some(captures) = READY_RE.captures(line) {
            let host = captures
                .name("host")
                .expect("host capture group is mandatory")
                .as_str()
                .to_string();
            let port: u16 = captures
                .name("port")
                .expect("port capture group is mandatory")
                .as_str()
                .parse()
                .ok()?;
            return Some(ReadyLine {
                host,
                port,
                raw: line.to_string(),
            });
        }
    }
    None
}

/// Strict variant: parses one buffer and errors if no readiness line is found.
pub fn parse_ready_line_strict(output: &str) -> OracleResult<ReadyLine> {
    parse_ready_line(output).ok_or_else(|| OracleError::UnparseableReadyLine {
        raw: output.to_string(),
    })
}

/// Hosts the contract considers loopback. Per
/// [`CONTRACT.md`](../CONTRACT.md), the preview Rust server binds loopback
/// only and the readiness line must reflect that.
const LOOPBACK_HOSTS: &[&str] = &["127.0.0.1", "::1", "localhost"];

/// Strict-loopback variant of [`parse_ready_line_strict`].
///
/// Behaves like the strict parser but additionally rejects readiness lines
/// whose host is not in [`LOOPBACK_HOSTS`]. This is opt-in (the harness
/// continues to use `parse_ready_line` / `parse_ready_line_strict` so the
/// permissive behavior on Bun output never changes) and is intended for
/// test code that wants to assert the bind contract.
pub fn parse_ready_line_strict_loopback(output: &str) -> OracleResult<ReadyLine> {
    let line = parse_ready_line_strict(output)?;
    if !LOOPBACK_HOSTS.contains(&line.host.as_str()) {
        return Err(OracleError::Other(format!(
            "readiness host is not loopback: {} (raw: {})",
            line.host, line.raw
        )));
    }
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_line() {
        let line = "kilo server listening on http://127.0.0.1:54321";
        let parsed = parse_ready_line(line).expect("must parse");
        assert_eq!(parsed.host, "127.0.0.1");
        assert_eq!(parsed.port, 54321);
        assert_eq!(parsed.raw, line);
    }

    #[test]
    fn parses_line_among_noise() {
        let buf = "warming up...\nkilo server listening on http://127.0.0.1:9999\nready\n";
        let parsed = parse_ready_line(buf).expect("must parse");
        assert_eq!(parsed.port, 9999);
    }

    #[test]
    fn fixture_form_redacts_port() {
        let parsed = ReadyLine {
            host: "127.0.0.1".into(),
            port: 12345,
            raw: "kilo server listening on http://127.0.0.1:12345".into(),
        };
        assert_eq!(
            parsed.into_fixture_string(),
            "kilo server listening on http://127.0.0.1:<PORT>"
        );
    }

    #[test]
    fn rejects_unrelated_line() {
        assert!(parse_ready_line("hello world").is_none());
        assert!(parse_ready_line("listening on https://127.0.0.1:1234").is_none());
        // https is not bun's output
    }

    #[test]
    fn strict_errors_on_missing_line() {
        let err = parse_ready_line_strict("nothing useful").unwrap_err();
        match err {
            OracleError::UnparseableReadyLine { raw } => assert_eq!(raw, "nothing useful"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn strict_loopback_accepts_loopback_hosts() {
        for host in &["127.0.0.1", "localhost"] {
            let line = format!("kilo server listening on http://{host}:54321");
            let parsed = parse_ready_line_strict_loopback(&line)
                .unwrap_or_else(|e| panic!("must accept {host}: {e}"));
            assert_eq!(parsed.host, *host);
        }
    }

    #[test]
    fn strict_loopback_rejects_external_hosts() {
        let line = "kilo server listening on http://10.0.0.5:54321";
        let err = parse_ready_line_strict_loopback(line).unwrap_err();
        match err {
            OracleError::Other(msg) => assert!(msg.contains("10.0.0.5"), "msg = {msg}"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn strict_loopback_propagates_unparseable_error() {
        let err = parse_ready_line_strict_loopback("garbage").unwrap_err();
        match err {
            OracleError::UnparseableReadyLine { raw } => assert_eq!(raw, "garbage"),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
