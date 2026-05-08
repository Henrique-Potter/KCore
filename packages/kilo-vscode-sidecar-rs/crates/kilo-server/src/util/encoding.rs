//! Encoding + time helpers shared by middleware, OAuth, and route handlers.
//!
//! Step 8 of the kilo-server module split: `decode_query`, `hex`,
//! `unix_millis`, and `loopback` were free functions in `lib.rs`. They
//! moved here so callers (`http::middleware::auth_token`, `oauth::url`,
//! `oauth::tokens`, `routes::sessions`, `lib.rs::serve`) reach them via a
//! typed seam instead of the crate root.

/// Percent-decode a query value. Decodes into a byte buffer first and
/// converts to UTF-8 at the end, matching `URLSearchParams` semantics:
/// `%C3%A9` is the two bytes of UTF-8-encoded `é` (U+00E9), not the
/// Latin-1 pair `Ã©`. The previous `out.push(byte as char)` cast each
/// byte to a Unicode codepoint, which silently mangled any non-ASCII
/// auth token. Falls back to lossy UTF-8 conversion if the decoded bytes
/// aren't valid UTF-8 — that matches what a browser-side caller would
/// see and avoids crashing an auth check on malformed input.
pub(crate) fn decode_query(value: &str) -> String {
    let mut bytes = Vec::with_capacity(value.len());
    let mut iter = value.as_bytes().iter().copied();
    while let Some(ch) = iter.next() {
        if ch == b'+' {
            bytes.push(b' ');
            continue;
        }

        if ch != b'%' {
            bytes.push(ch);
            continue;
        }

        let Some(a) = iter.next() else {
            bytes.push(b'%');
            continue;
        };
        let Some(b) = iter.next() else {
            bytes.push(b'%');
            bytes.push(a);
            continue;
        };
        match hex(a).and_then(|hi| hex(b).map(|lo| (hi << 4) | lo)) {
            Some(byte) => bytes.push(byte),
            None => {
                bytes.push(b'%');
                bytes.push(a);
                bytes.push(b);
            }
        }
    }

    String::from_utf8(bytes)
        .unwrap_or_else(|err| String::from_utf8_lossy(&err.into_bytes()).into_owned())
}

pub(crate) fn hex(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'a'..=b'f' => Some(ch - b'a' + 10),
        b'A'..=b'F' => Some(ch - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or(0)
}

/// Always bind loopback regardless of the requested hostname. Matches the
/// CONTRACT.md invariant — non-loopback flags are normalized to loopback
/// at the listener boundary. The `hostname` arg is accepted (so callers
/// can pass through `--host` / `--hostname`) but ignored: a future binding
/// layer can use it for diagnostics, but the listener address is fixed.
///
/// Audit Fix 10: when the requested hostname is not a recognized loopback
/// alias and not empty, emit a one-shot warning so an operator can see why
/// their `--host 0.0.0.0` flag was silently coerced.
pub(crate) fn loopback(hostname: &str) -> [u8; 4] {
    let trimmed = hostname.trim();
    let is_loopback = trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("127.0.0.1")
        || trimmed.eq_ignore_ascii_case("localhost");
    if !is_loopback {
        eprintln!(
            "[kilo-server] WARN: --host {trimmed} is not a loopback address; using 127.0.0.1 (sidecar binds loopback only)"
        );
    }
    [127, 0, 0, 1]
}
