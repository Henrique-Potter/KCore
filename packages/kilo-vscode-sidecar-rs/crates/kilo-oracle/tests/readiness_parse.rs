//! Integration tests for the readiness-line parser.
//!
//! These tests cross-check that the parser stays bidirectional with the shipped
//! `fixtures/startup/ready-line.json` fixture: a parsed real readiness line,
//! normalized via `ReadyLine::into_fixture_string`, must match the
//! `stdout_line` field stored in the fixture.

use std::path::PathBuf;

use kilo_oracle::{parse_ready_line, parse_ready_line_strict, OracleError, ReadyLine};

fn fixtures_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures")
}

#[test]
fn canonical_line_round_trips_through_fixture_form() {
    let line = "kilo server listening on http://127.0.0.1:54321";
    let parsed = parse_ready_line(line).expect("must parse");
    assert_eq!(parsed.host, "127.0.0.1");
    assert_eq!(parsed.port, 54321);

    let fixture_form = parsed.into_fixture_string();
    assert_eq!(
        fixture_form,
        "kilo server listening on http://127.0.0.1:<PORT>"
    );
}

#[test]
fn shipped_startup_fixture_matches_parser_output() {
    let fixture_path = fixtures_root().join("startup/ready-line.json");
    let raw = std::fs::read_to_string(&fixture_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", fixture_path.display()));
    let value: serde_json::Value = serde_json::from_str(&raw).expect("fixture must be valid JSON");

    let stdout_line = value
        .get("stdout_line")
        .and_then(|v| v.as_str())
        .expect("stdout_line must be a string");

    // Pretend `<PORT>` is a digit-string by substituting a real port and parsing,
    // then re-normalizing back to fixture form.
    let realized = stdout_line.replace("<PORT>", "12345");
    let parsed = parse_ready_line(&realized).expect("must parse realized line");
    let normalized = parsed.into_fixture_string();
    assert_eq!(
        normalized, stdout_line,
        "the shipped fixture's stdout_line must equal ReadyLine::into_fixture_string"
    );
}

#[test]
fn rejects_https_or_unrelated_lines() {
    assert!(parse_ready_line("hello world").is_none());
    assert!(parse_ready_line("listening on https://127.0.0.1:1234").is_none());
}

#[test]
fn strict_returns_typed_error_on_miss() {
    let err = parse_ready_line_strict("nothing useful").unwrap_err();
    match err {
        OracleError::UnparseableReadyLine { raw } => assert_eq!(raw, "nothing useful"),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn parses_among_noise() {
    let buf = concat!(
        "warming up...\n",
        "kilo server listening on http://127.0.0.1:9999\n",
        "ready\n"
    );
    let parsed = parse_ready_line(buf).expect("must parse");
    assert_eq!(parsed.port, 9999);
}

#[test]
fn ready_line_serializes_stably() {
    // ReadyLine is exposed as Serialize/Deserialize so it can be embedded
    // directly in fixtures. Round-trip must be lossless.
    let parsed = ReadyLine {
        host: "127.0.0.1".to_string(),
        port: 42,
        raw: "kilo server listening on http://127.0.0.1:42".to_string(),
    };
    let s = serde_json::to_string(&parsed).unwrap();
    let back: ReadyLine = serde_json::from_str(&s).unwrap();
    assert_eq!(parsed, back);
}
