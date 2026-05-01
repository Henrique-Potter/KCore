//! Line-by-line SSE recorder.
//!
//! The Bun `/global/event` endpoint emits SSE in the canonical
//! `data: <json>\n\n` form. The SDK tolerates `event:`, `id:`, `retry:`
//! prefixes too, but Bun does not currently emit them. We accept all four
//! per the SSE spec so future Bun changes do not silently drop frames.
//!
//! Recording is **lossless** for the wire bytes: we keep the raw `data:`
//! payload alongside a normalized JSON view. If a frame fails JSON parsing
//! it still ends up in the trace with `parse_error` set, so a regression
//! from the Rust side is visible rather than silently absorbed.

use std::time::{Duration, Instant};

use futures_util::StreamExt;
use reqwest::Response;
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::time::sleep;

use crate::error::{OracleError, OracleResult};
use crate::fixture::FixtureFrame;
use crate::normalize::Normalizer;

/// One decoded SSE event.
#[derive(Debug, Clone)]
pub struct SseFrame {
    pub event: Option<String>,
    pub data: String,
    pub id: Option<String>,
    pub retry_ms: Option<u64>,
    pub wall_offset_ms: u64,
}

/// Determines when the recorder should stop reading the stream.
pub enum StopCondition {
    /// Stop after capturing N data frames (excludes comment lines and empty
    /// flushes).
    Frames(usize),
    /// Stop after this much wall clock has elapsed since recording started.
    Duration(Duration),
    /// Stop when a captured frame's parsed JSON has `payload.type == kind`.
    EventType(String),
    /// Stop when the caller-supplied closure returns `true`.
    Predicate(Box<dyn Fn(&SseFrame, &Option<Value>) -> bool + Send + Sync>),
}

impl std::fmt::Debug for StopCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StopCondition::Frames(n) => write!(f, "Frames({n})"),
            StopCondition::Duration(d) => write!(f, "Duration({d:?})"),
            StopCondition::EventType(s) => write!(f, "EventType({s})"),
            StopCondition::Predicate(_) => write!(f, "Predicate(<fn>)"),
        }
    }
}

/// Records frames from a streaming reqwest [`Response`] until a [`StopCondition`]
/// fires.
pub struct SseRecorder {
    started_at: Instant,
    abort_rx: Option<oneshot::Receiver<()>>,
}

impl SseRecorder {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            abort_rx: None,
        }
    }

    pub fn with_abort(mut self, rx: oneshot::Receiver<()>) -> Self {
        self.abort_rx = Some(rx);
        self
    }

    /// Drive the stream to completion or until the stop condition fires.
    /// Returns the captured frames.
    pub async fn record(
        mut self,
        response: Response,
        stop: StopCondition,
    ) -> OracleResult<Vec<SseFrame>> {
        let mut frames: Vec<SseFrame> = Vec::new();
        let mut buf = String::new();
        let started_at = self.started_at;

        let body_stream = response.bytes_stream();
        tokio::pin!(body_stream);

        // We don't have a separate timer for `Duration` to avoid an extra
        // tokio task; we just check the elapsed time after each chunk.
        loop {
            // 1. Stop?
            if check_stop(&stop, &frames, started_at, None).await {
                break;
            }
            if let Some(rx) = self.abort_rx.as_mut() {
                if rx.try_recv().is_ok() {
                    break;
                }
            }

            // 2. Get the next chunk with a generous timeout. The default Bun
            //    heartbeat is 10 s, so 30 s is well above the noise floor.
            let next = tokio::select! {
                chunk = body_stream.next() => chunk,
                _ = sleep(Duration::from_secs(30)) => {
                    return Err(OracleError::ScenarioAborted("sse stream stalled for 30s"));
                }
            };

            let Some(chunk) = next else {
                break; // server closed
            };
            let chunk = chunk.map_err(OracleError::Http)?;
            buf.push_str(&String::from_utf8_lossy(&chunk));

            // 3. Drain any complete frames out of the buffer. Frames are
            //    separated by a blank line per the SSE spec.
            while let Some(idx) = buf.find("\n\n") {
                let frame_text = buf[..idx].to_string();
                buf.drain(..idx + 2);

                let Some(frame) = decode_frame(&frame_text, started_at) else {
                    continue;
                };
                let parsed: Option<Value> = serde_json::from_str(&frame.data).ok();
                let stop_now =
                    check_stop(&stop, &frames, started_at, Some((&frame, &parsed))).await;
                frames.push(frame);
                if stop_now {
                    return Ok(frames);
                }
            }
        }

        Ok(frames)
    }

    /// Convert recorded frames into [`FixtureFrame`]s ready for serialization.
    /// `wall_offset_ms` is bucketed via [`Normalizer::normalize_duration_ms`]
    /// so heartbeat traces stay stable across runs.
    pub fn to_fixture_frames(frames: Vec<SseFrame>) -> Vec<FixtureFrame> {
        frames
            .into_iter()
            .enumerate()
            .map(|(i, f)| {
                let parsed: Option<Value> = serde_json::from_str(&f.data).ok();
                let directory = parsed
                    .as_ref()
                    .and_then(|v| v.get("directory"))
                    .and_then(|d| d.as_str().map(|s| s.to_string()));
                let payload = parsed.as_ref().and_then(|v| v.get("payload")).cloned();
                let parse_error = if parsed.is_none() {
                    Some("data line was not JSON".to_string())
                } else {
                    None
                };
                FixtureFrame {
                    frame_index: i as u64,
                    wall_offset_ms: Normalizer::normalize_duration_ms(f.wall_offset_ms),
                    raw: f.data,
                    payload,
                    directory,
                    parse_error,
                }
            })
            .collect()
    }
}

fn decode_frame(text: &str, started_at: Instant) -> Option<SseFrame> {
    let mut event: Option<String> = None;
    let mut data_lines: Vec<&str> = Vec::new();
    let mut id: Option<String> = None;
    let mut retry_ms: Option<u64> = None;

    for line in text.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue; // comment / heartbeat-keepalive line
        }
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim_start());
        } else if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("id:") {
            id = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("retry:") {
            retry_ms = rest.trim().parse().ok();
        }
    }

    if data_lines.is_empty() {
        return None;
    }
    let data = data_lines.join("\n");
    let wall_offset_ms = started_at.elapsed().as_millis() as u64;
    Some(SseFrame {
        event,
        data,
        id,
        retry_ms,
        wall_offset_ms,
    })
}

async fn check_stop(
    cond: &StopCondition,
    frames: &[SseFrame],
    started_at: Instant,
    last: Option<(&SseFrame, &Option<Value>)>,
) -> bool {
    match cond {
        StopCondition::Frames(n) => frames.len() >= *n,
        StopCondition::Duration(d) => started_at.elapsed() >= *d,
        StopCondition::EventType(kind) => {
            let Some((_, parsed)) = last else {
                return false;
            };
            let Some(parsed) = parsed.as_ref() else {
                return false;
            };
            parsed
                .get("payload")
                .and_then(|p| p.get("type"))
                .and_then(|t| t.as_str())
                .map(|t| t == kind)
                .unwrap_or(false)
        }
        StopCondition::Predicate(f) => {
            let Some((frame, parsed)) = last else {
                return false;
            };
            f(frame, parsed)
        }
    }
}

impl Default for SseRecorder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_basic_frame() {
        let text = "data: {\"hello\":\"world\"}";
        let frame = decode_frame(text, Instant::now()).unwrap();
        assert_eq!(frame.data, "{\"hello\":\"world\"}");
        assert!(frame.event.is_none());
    }

    #[test]
    fn decode_multiline_data() {
        let text = "event: hi\ndata: line1\ndata: line2\nid: 5";
        let frame = decode_frame(text, Instant::now()).unwrap();
        assert_eq!(frame.event.as_deref(), Some("hi"));
        assert_eq!(frame.data, "line1\nline2");
        assert_eq!(frame.id.as_deref(), Some("5"));
    }

    #[test]
    fn ignores_comments_and_blanks() {
        let text = ": ping\n\n";
        let frame = decode_frame(text, Instant::now());
        assert!(frame.is_none());
    }
}
