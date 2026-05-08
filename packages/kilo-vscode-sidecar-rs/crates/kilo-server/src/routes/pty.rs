//! Real PTY backend for the Agent Manager terminal routes
//! (Bun parity: `packages/opencode/src/server/routes/pty/*`).
//!
//! Routes implemented here:
//!
//! * `POST /pty` — spawn a shell with a pty pair, return `{ id, ... }`.
//! * `PUT /pty/:id` — write input bytes and/or resize the tty. Body
//!   accepts `{ input: "..." }` and/or `{ size: { cols, rows } }`.
//! * `DELETE /pty/:id` — kill the child process and free the master.
//!
//! Output is streamed onto the SSE bus as `pty.output` events with
//! `{ id, data }` (`data` is utf-8 lossy text). Consumers filter by `id`.
//!
//! The `portable-pty` crate (workspace-pinned) handles the platform
//! differences. On Windows it uses ConPTY; on Unix the `posix_openpt`
//! family. The reader runs on a dedicated `spawn_blocking` thread so
//! the tokio runtime is never blocked on a synchronous `read()`.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use kilo_protocol::GlobalEvent;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde_json::{json, Value};

use crate::AppState;

/// One pty session. The master `writer` is wrapped behind a Mutex so
/// the route handler thread can write input bytes without coordinating
/// with the reader. The `child` is kept so DELETE can kill it; the
/// `master` is kept so `resize` can be issued.
pub(crate) struct PtyHandle {
    pub(crate) master: Box<dyn portable_pty::MasterPty + Send>,
    pub(crate) writer: Mutex<Box<dyn std::io::Write + Send>>,
    pub(crate) child: Mutex<Box<dyn portable_pty::Child + Send + Sync>>,
}

/// `state.pty: Mutex<BTreeMap<String, PtyHandle>>` is registered in
/// `AppState` (see `state.rs`). Use this alias to keep the type tidy.
pub(crate) type PtyMap = BTreeMap<String, PtyHandle>;

/// `POST /pty` — spawn a shell. Body shape:
/// `{ "cwd": "...", "title": "...", "command": [...], "env": {...}, "size": { "cols", "rows" } }`.
/// All fields optional. Defaults: cwd = workspace root, command = the
/// platform shell (`%COMSPEC%` on Windows, `$SHELL` or `/bin/sh` on
/// Unix), size = 80x24.
pub(crate) async fn pty_create(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    let cwd = input
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| state.store.paths().directory.clone());
    let cols = input
        .pointer("/size/cols")
        .and_then(Value::as_u64)
        .filter(|n| *n > 0 && *n < 1024)
        .unwrap_or(80) as u16;
    let rows = input
        .pointer("/size/rows")
        .and_then(Value::as_u64)
        .filter(|n| *n > 0 && *n < 1024)
        .unwrap_or(24) as u16;
    let command_argv = input
        .get("command")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .unwrap_or_else(default_shell_argv);
    let env = input
        .get("env")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<BTreeMap<String, String>>()
        })
        .unwrap_or_default();

    let pty_system = native_pty_system();
    let pair = match pty_system.openpty(PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(p) => p,
        Err(err) => {
            return pty_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RustPtyOpenError",
                &format!("openpty failed: {err}"),
            );
        }
    };

    let mut cmd = CommandBuilder::new(&command_argv[0]);
    for arg in &command_argv[1..] {
        cmd.arg(arg);
    }
    cmd.cwd(cwd);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(err) => {
            return pty_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RustPtySpawnError",
                &format!("spawn failed: {err}"),
            );
        }
    };
    drop(pair.slave); // close child end on the parent side
    let writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(err) => {
            return pty_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RustPtyWriterError",
                &format!("take_writer failed: {err}"),
            );
        }
    };
    let reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(err) => {
            return pty_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RustPtyReaderError",
                &format!("try_clone_reader failed: {err}"),
            );
        }
    };

    // Generate a stable id and stash the handle.
    let id = format!(
        "pty_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let handle = PtyHandle {
        master: pair.master,
        writer: Mutex::new(writer),
        child: Mutex::new(child),
    };
    state.pty.lock().unwrap().insert(id.clone(), handle);

    // Reader thread: pump stdout/stderr bytes onto the SSE bus until
    // EOF. Spawned via `spawn_blocking` because `read()` is sync.
    let bus_state = state.clone();
    let bus_id = id.clone();
    tokio::task::spawn_blocking(move || {
        let mut reader = reader;
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let data = String::from_utf8_lossy(&buf[..n]).to_string();
                    crate::http::sse::publish(
                        &bus_state,
                        GlobalEvent::bus("pty.output", json!({ "id": bus_id, "data": data })),
                    );
                }
                Err(_) => break,
            }
        }
        crate::http::sse::publish(
            &bus_state,
            GlobalEvent::bus("pty.exit", json!({ "id": bus_id })),
        );
    });

    Json(json!({
        "id": id,
        "cols": cols,
        "rows": rows,
    }))
    .into_response()
}

/// `PUT /pty/:id` — write input and/or resize.
pub(crate) async fn pty_update(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    let map = state.pty.lock().unwrap();
    let Some(handle) = map.get(&id) else {
        return pty_error(
            StatusCode::NOT_FOUND,
            "RustPtyNotFoundError",
            &format!("PTY {id} not found"),
        );
    };
    if let Some(text) = input.get("input").and_then(Value::as_str) {
        if let Ok(mut writer) = handle.writer.lock() {
            if let Err(err) = writer.write_all(text.as_bytes()) {
                return pty_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "RustPtyWriteError",
                    &format!("write failed: {err}"),
                );
            }
            let _ = writer.flush();
        }
    }
    if let Some(size) = input.get("size") {
        let cols = size
            .get("cols")
            .and_then(Value::as_u64)
            .filter(|n| *n > 0 && *n < 1024)
            .unwrap_or(80) as u16;
        let rows = size
            .get("rows")
            .and_then(Value::as_u64)
            .filter(|n| *n > 0 && *n < 1024)
            .unwrap_or(24) as u16;
        if let Err(err) = handle.master.resize(PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        }) {
            return pty_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "RustPtyResizeError",
                &format!("resize failed: {err}"),
            );
        }
    }
    Json(true).into_response()
}

/// `DELETE /pty/:id` — kill the child + drop the handle.
pub(crate) async fn pty_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(handle) = state.pty.lock().unwrap().remove(&id) else {
        return pty_error(
            StatusCode::NOT_FOUND,
            "RustPtyNotFoundError",
            &format!("PTY {id} not found"),
        );
    };
    if let Ok(mut child) = handle.child.lock() {
        let _ = child.kill();
    }
    Json(true).into_response()
}

/// Default platform shell argv. Windows: `%COMSPEC%` (typically
/// `cmd.exe`) or `pwsh.exe` if `%COMSPEC%` is unset. Unix: `$SHELL` or
/// `/bin/sh`. Always launched as a login/interactive shell — callers
/// override via the `command` request field.
fn default_shell_argv() -> Vec<String> {
    if cfg!(windows) {
        let exe = std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_string());
        vec![exe]
    } else {
        let exe = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
        vec![exe]
    }
}

fn pty_error(status: StatusCode, name: &str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "name": name,
            "data": { "message": message },
        })),
    )
        .into_response()
}
