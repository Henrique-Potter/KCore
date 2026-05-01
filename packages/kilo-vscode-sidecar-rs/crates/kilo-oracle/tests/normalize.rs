//! Integration tests for the volatile-field normalizer.
//!
//! These tests exercise the public `Normalizer` surface end-to-end and assert
//! that running it twice produces byte-identical output (idempotency), which
//! is the core invariant fixtures rely on.

use kilo_oracle::{Normalizer, Redactions};
use serde_json::json;

#[test]
fn normalize_password_and_port_in_url() {
    let mut n = Normalizer::new(
        Redactions::default()
            .with_password("hunter2")
            .with_port(54321),
    );
    let out = n.normalize_str("http://kilo:hunter2@127.0.0.1:54321/global/event");
    assert_eq!(
        out,
        "http://kilo:<KILO_SERVER_PASSWORD>@127.0.0.1:<PORT>/global/event"
    );
}

#[test]
fn normalize_session_message_part_request_uuid() {
    let mut n = Normalizer::new(Redactions::default());
    let out = n.normalize_str(
        "ses_aaaaaa msg_bbbbbb prt_cccccc perm_dddddd qst_eeeeee 01234567-89ab-cdef-0123-456789abcdef",
    );
    assert_eq!(
        out,
        "<SESSION_ID:1> <MESSAGE_ID:1> <PART_ID:1> <REQUEST_ID:1> <REQUEST_ID:2> <UUID:1>"
    );
}

#[test]
fn normalize_value_round_trip_is_idempotent() {
    let mut n = Normalizer::new(
        Redactions::default()
            .with_password("supersecret")
            .with_port(11111)
            .with_workspace("D:/Projects/kilocode")
            .with_home("C:/Users/test"),
    );

    let v = json!({
        "session": "ses_abcdef0123",
        "messages": ["msg_01234567", "msg_01234567"],
        "url": "http://kilo:supersecret@127.0.0.1:11111/v2/global/event",
        "workspace": "D:/Projects/kilocode/packages/kilo-vscode",
        "home": "C:/Users/test/.kilo",
        "createdAt": 1717182000000_u64,
        "uuid": "01234567-89ab-cdef-0123-456789abcdef"
    });

    let first = n.normalize_value(&v);
    // Re-running through a fresh Normalizer with the same redactions must
    // produce a stable result (idempotent on the *output*, not on the input
    // map state — the inputs already contain placeholders).
    let mut n2 = Normalizer::new(Redactions::default());
    let second = n2.normalize_value(&first);
    assert_eq!(first, second, "normalize_value must be idempotent");
}

#[test]
fn duration_buckets_to_seconds() {
    assert_eq!(Normalizer::normalize_duration_ms(0), 0);
    assert_eq!(Normalizer::normalize_duration_ms(450), 0);
    assert_eq!(Normalizer::normalize_duration_ms(550), 1000);
    assert_eq!(Normalizer::normalize_duration_ms(10_024), 10_000);
    assert_eq!(Normalizer::normalize_duration_ms(10_500), 11_000);
    assert_eq!(Normalizer::normalize_duration_ms(20_499), 20_000);
    assert_eq!(Normalizer::normalize_duration_ms(20_500), 21_000);
}

#[test]
fn workspace_path_with_backslashes_normalizes_to_forward_slash() {
    let mut n = Normalizer::new(
        Redactions::default()
            .with_workspace("D:\\Projects\\kilocode")
            .with_home("C:\\Users\\HPotter"),
    );
    // Both forms appear in real Windows captures (literal and JSON-escaped).
    let s = r#"{"path":"D:\\Projects\\kilocode\\foo\\bar","other":"D:/Projects/kilocode/baz"}"#;
    let out = n.normalize_str(s);
    assert!(
        out.contains("<WORKSPACE>/foo/bar"),
        "expected workspace placeholder + forward slashes, got: {out}"
    );
    assert!(
        out.contains("<WORKSPACE>/baz"),
        "expected forward-slash form to also be redacted, got: {out}"
    );
    assert!(
        !out.contains("D:\\Projects"),
        "literal workspace path should not survive normalization, got: {out}"
    );
}

#[test]
fn timestamp_keys_get_redacted_only_when_payload_is_long_enough() {
    let mut n = Normalizer::new(Redactions::default());
    // 13-digit ms-since-epoch should be redacted.
    let out = n.normalize_str(r#"{"created":1717182000000,"index":3}"#);
    assert!(out.starts_with(r#"{"created":<TIMESTAMP:"#));
    assert!(out.ends_with(r#""index":3}"#)); // index is unaffected.

    // 4-digit number on a non-timestamp key is left alone.
    let mut n2 = Normalizer::new(Redactions::default());
    let out = n2.normalize_str(r#"{"frame":42}"#);
    assert_eq!(out, r#"{"frame":42}"#);
}
