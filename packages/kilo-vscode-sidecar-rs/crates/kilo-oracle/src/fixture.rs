//! Fixture file format and IO.
//!
//! Two file formats are used:
//!
//! - `*.json` — single document. Used for the startup fixture and any other
//!   shape with no streaming component.
//! - `*.jsonl` — line-per-frame. Used for SSE traces and store hash dumps. One
//!   JSON object per line, no trailing comma, terminating newline.
//!
//! All fixture writes pass through [`crate::normalize::Normalizer`] before
//! being committed to disk. All fixture reads also re-normalize on the way
//! out, which makes the operation idempotent (loading a normalized fixture
//! and writing it back must produce byte-identical output).

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{OracleError, OracleResult};
use crate::normalize::{IdentityMap, Normalizer};

/// A single frame inside a JSONL fixture (typically an SSE event).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FixtureFrame {
    /// Monotonic position in the stream.
    pub frame_index: u64,
    /// Bucketed wall-clock offset from the start of the stream, ms.
    /// Bucketed via [`Normalizer::normalize_duration_ms`] before write.
    pub wall_offset_ms: u64,
    /// The raw `data:` line as received from Bun, stripped of the
    /// `data: ` prefix but otherwise byte-identical to the wire form.
    pub raw: String,
    /// `raw` parsed as JSON, after normalization. May be `null` if parsing
    /// failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// The directory this event was scoped to, if Bun scoped it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
    /// Set if `raw` could not be parsed as JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parse_error: Option<String>,
}

/// Top-level metadata for a fixture file. Fixture frames are stored separately
/// in JSONL form alongside this metadata file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureMeta {
    pub contract_version: String,
    pub scenario: String,
    pub recorded_at_iso: String,
    pub bun_version: Option<String>,
    pub identity_map: IdentityMap,
    /// `true` when the fixture was produced by spawning a real `kilo.exe` (M0
    /// live recording); `false`/absent for synthetic placeholders. Written by
    /// the live recorder under `tests/record_fixtures.rs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured: Option<bool>,
    /// ISO-8601 UTC timestamp of when the live recording session ran. Distinct
    /// from `recorded_at_iso`, which is the redacted `<TIMESTAMP:0>` token —
    /// `captured_at_iso` carries the real wall-clock timestamp because it is
    /// provenance metadata, not contract data. May be absent for synthetic
    /// fixtures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_at_iso: Option<String>,
    /// SHA-256 (lowercase hex) of the `kilo.exe` (or `kilo`) binary that this
    /// recording was captured from. Lets reviewers tell whether a fixture is
    /// stale relative to the current bundled binary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bun_binary_sha256: Option<String>,
    /// `version` field returned by `GET /global/health` at recording time.
    /// Captured raw (not redacted to `<VERSION>`) so reviewers can correlate
    /// with `bun_binary_sha256` and the `.cli-version` file under
    /// `packages/kilo-vscode/bin/`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kilo_server_version: Option<String>,
    /// Observed wall-clock interval (ms) between consecutive `server.heartbeat`
    /// frames in this recording. Useful for catching heartbeat-cadence drift
    /// between Bun and Rust without re-reading the frames file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_heartbeat_interval_ms: Option<u64>,
}

impl FixtureMeta {
    pub fn new(scenario: impl Into<String>, identity_map: IdentityMap) -> Self {
        Self {
            // Tracks the same value as CONTRACT.md
            contract_version: "kilo-vscode-sidecar.preview.0".to_string(),
            scenario: scenario.into(),
            recorded_at_iso: "<TIMESTAMP:0>".to_string(), // never written verbatim; meta is normalized
            bun_version: None,
            identity_map,
            captured: None,
            captured_at_iso: None,
            bun_binary_sha256: None,
            kilo_server_version: None,
            observed_heartbeat_interval_ms: None,
        }
    }
}

/// Helper for writing/reading a paired meta+frames fixture.
pub struct Fixture {
    pub meta_path: PathBuf,
    pub frames_path: PathBuf,
}

impl Fixture {
    /// Construct a Fixture from a base path. The meta file is `<base>.meta.json`
    /// and the frames file is `<base>.jsonl`.
    pub fn at(base: impl AsRef<Path>) -> Self {
        let base = base.as_ref().to_path_buf();
        let meta_path = base.with_extension("meta.json");
        let frames_path = base.with_extension("jsonl");
        Self {
            meta_path,
            frames_path,
        }
    }

    pub fn write(
        &self,
        meta: &FixtureMeta,
        frames: &[FixtureFrame],
        normalizer: &mut Normalizer,
    ) -> OracleResult<()> {
        if let Some(parent) = self.meta_path.parent() {
            fs::create_dir_all(parent).map_err(|e| OracleError::Io {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }

        // Normalize meta as a value to catch any leftover paths/timestamps.
        let meta_value = serde_json::to_value(meta)?;
        let meta_normalized = normalizer.normalize_value(&meta_value);
        let meta_json = serde_json::to_string_pretty(&meta_normalized)?;
        fs::write(&self.meta_path, meta_json).map_err(|e| OracleError::Io {
            path: self.meta_path.clone(),
            source: e,
        })?;

        let mut file = fs::File::create(&self.frames_path).map_err(|e| OracleError::Io {
            path: self.frames_path.clone(),
            source: e,
        })?;
        for frame in frames {
            let value = serde_json::to_value(frame)?;
            let normalized = normalizer.normalize_value(&value);
            let line = serde_json::to_string(&normalized)?;
            file.write_all(line.as_bytes())
                .map_err(|e| OracleError::Io {
                    path: self.frames_path.clone(),
                    source: e,
                })?;
            file.write_all(b"\n").map_err(|e| OracleError::Io {
                path: self.frames_path.clone(),
                source: e,
            })?;
        }
        Ok(())
    }

    pub fn read_frames(&self) -> OracleResult<Vec<FixtureFrame>> {
        let f = fs::File::open(&self.frames_path).map_err(|e| OracleError::Io {
            path: self.frames_path.clone(),
            source: e,
        })?;
        let reader = BufReader::new(f);
        let mut out = Vec::new();
        for (idx, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| OracleError::Io {
                path: self.frames_path.clone(),
                source: e,
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let frame: FixtureFrame = serde_json::from_str(&line).map_err(|e| {
                OracleError::Other(format!(
                    "fixture {}: line {}: {e}",
                    self.frames_path.display(),
                    idx + 1
                ))
            })?;
            out.push(frame);
        }
        Ok(out)
    }

    pub fn read_meta(&self) -> OracleResult<FixtureMeta> {
        let s = fs::read_to_string(&self.meta_path).map_err(|e| OracleError::Io {
            path: self.meta_path.clone(),
            source: e,
        })?;
        let meta: FixtureMeta = serde_json::from_str(&s)?;
        Ok(meta)
    }
}

/// Single-document fixture (used for the startup fixture). The on-disk shape
/// is just a pretty-printed JSON object; no JSONL.
pub struct FixtureFile {
    pub path: PathBuf,
}

impl FixtureFile {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn write_value(&self, v: &Value, normalizer: &mut Normalizer) -> OracleResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| OracleError::Io {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }
        let normalized = normalizer.normalize_value(v);
        let s = serde_json::to_string_pretty(&normalized)?;
        fs::write(&self.path, s).map_err(|e| OracleError::Io {
            path: self.path.clone(),
            source: e,
        })?;
        Ok(())
    }

    pub fn read_value(&self) -> OracleResult<Value> {
        let s = fs::read_to_string(&self.path).map_err(|e| OracleError::Io {
            path: self.path.clone(),
            source: e,
        })?;
        Ok(serde_json::from_str(&s)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize::Redactions;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn round_trip_fixture_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("startup.json");
        let file = FixtureFile::new(&path);
        let mut n = Normalizer::new(Redactions::default());
        let v = json!({
            "stdout_line": "kilo server listening on http://127.0.0.1:54321",
            "session": "ses_abc123def",
        });
        file.write_value(&v, &mut n).unwrap();

        // Read back, normalize again — should be the same.
        let read = file.read_value().unwrap();
        let mut n2 = Normalizer::new(Redactions::default());
        let read_normalized = n2.normalize_value(&read);
        assert_eq!(read_normalized, read);
    }

    #[test]
    fn round_trip_jsonl_fixture() {
        let dir = tempdir().unwrap();
        let base = dir.path().join("trace");
        let fixture = Fixture::at(&base);

        let mut n = Normalizer::new(Redactions::default());
        let meta = FixtureMeta::new("test-scenario", IdentityMap::default());
        let frames = vec![
            FixtureFrame {
                frame_index: 0,
                wall_offset_ms: 0,
                raw: r#"{"directory":null,"payload":{"type":"server.connected","properties":{}}}"#
                    .to_string(),
                payload: Some(json!({"type":"server.connected","properties":{}})),
                directory: None,
                parse_error: None,
            },
            FixtureFrame {
                frame_index: 1,
                wall_offset_ms: 10_000,
                raw: r#"{"directory":null,"payload":{"type":"server.heartbeat","properties":{}}}"#
                    .to_string(),
                payload: Some(json!({"type":"server.heartbeat","properties":{}})),
                directory: None,
                parse_error: None,
            },
        ];

        fixture.write(&meta, &frames, &mut n).unwrap();
        let read = fixture.read_frames().unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[1].wall_offset_ms, 10_000);
    }
}
