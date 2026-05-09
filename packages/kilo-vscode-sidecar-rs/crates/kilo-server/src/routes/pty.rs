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
///
/// `title`, `command`, `args`, `cwd`, `pid` are captured at spawn time
/// to satisfy the SDK `Pty` shape returned by create/update handlers
/// (`packages/sdk/js/src/gen/types.gen.ts:658-666`).
pub(crate) struct PtyHandle {
    pub(crate) master: Box<dyn portable_pty::MasterPty + Send>,
    pub(crate) writer: Mutex<Box<dyn std::io::Write + Send>>,
    pub(crate) child: Mutex<Box<dyn portable_pty::Child + Send + Sync>>,
    pub(crate) title: String,
    pub(crate) command: String,
    pub(crate) args: Vec<String>,
    pub(crate) cwd: String,
    pub(crate) pid: Option<u32>,
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
    let request_env = input
        .get("env")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<BTreeMap<String, String>>()
        })
        .unwrap_or_default();
    let env = build_pty_env(request_env);

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

    let command = command_argv[0].clone();
    let args: Vec<String> = command_argv[1..].to_vec();
    let title = input
        .get("title")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| cwd_basename(&cwd));

    let mut cmd = CommandBuilder::new(&command);
    for arg in &args {
        cmd.arg(arg);
    }
    cmd.cwd(&cwd);
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
    let pid = child.process_id();
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
        title: title.clone(),
        command: command.clone(),
        args: args.clone(),
        cwd: cwd.clone(),
        pid,
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
            GlobalEvent::bus("pty.exited", json!({ "id": bus_id })),
        );
    });

    Json(pty_info(&id, &title, &command, &args, &cwd, "running", pid)).into_response()
}

/// SDK `Pty` shape:
/// `{ id, title, command, args, cwd, status: "running" | "exited", pid }`
/// (`packages/sdk/js/src/gen/types.gen.ts:658-666`).
fn pty_info(
    id: &str,
    title: &str,
    command: &str,
    args: &[String],
    cwd: &str,
    status: &str,
    pid: Option<u32>,
) -> Value {
    json!({
        "id": id,
        "title": title,
        "command": command,
        "args": args,
        "cwd": cwd,
        "status": status,
        "pid": pid.unwrap_or(0),
    })
}

fn cwd_basename(cwd: &str) -> String {
    std::path::Path::new(cwd)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| cwd.to_string())
}

/// `PUT /pty/:id` — write input and/or resize. Returns the SDK `Pty`
/// info shape on success.
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
    let status = if handle
        .child
        .lock()
        .ok()
        .and_then(|mut child| child.try_wait().ok().flatten())
        .is_some()
    {
        "exited"
    } else {
        "running"
    };
    Json(pty_info(
        &id,
        &handle.title,
        &handle.command,
        &handle.args,
        &handle.cwd,
        status,
        handle.pid,
    ))
    .into_response()
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
        // Tree-kill first so grandchildren forked by the shell (npm
        // post-install scripts, nested REPLs, child shells) don't
        // outlive the pty. Mirrors Bun's `Shell.killTree`
        // (`packages/opencode/src/shell/shell.ts:15-44`): on Windows
        // `taskkill /T /F /PID`, on POSIX SIGTERM the process group.
        // Falls back to a direct `child.kill()` if the tree-kill
        // helper failed to run.
        let tree_killed = kill_tree(handle.pid);
        if !tree_killed {
            let _ = child.kill();
        }
        // Always reap the direct child so portable-pty drops the
        // master/slave fds even if `taskkill` already terminated it.
        let _ = child.wait();
    }
    Json(true).into_response()
}

/// Default platform shell argv. Mirrors Bun's `Shell.preferred`
/// (`packages/opencode/src/shell/shell.ts:55-91,93-109`):
///
/// * Windows: `$KILO_GIT_BASH_PATH` if set and exists -> `pwsh.exe`
///   (PATH or known install dirs) -> `powershell.exe` (PATH) ->
///   git-bash at the common install paths -> `$COMSPEC` -> `cmd.exe`.
/// * POSIX: `$SHELL` if set, else `/bin/sh`.
///
/// Always returns a single-element argv; callers override via the
/// `command` request field.
fn default_shell_argv() -> Vec<String> {
    vec![resolve_default_shell()]
}

#[cfg(windows)]
fn resolve_default_shell() -> String {
    if let Ok(p) = std::env::var("KILO_GIT_BASH_PATH") {
        if !p.is_empty() && std::path::Path::new(&p).is_file() {
            return p;
        }
    }
    if let Some(p) = find_on_path("pwsh.exe") {
        return p;
    }
    for candidate in [
        r"C:\Program Files\PowerShell\7\pwsh.exe",
        r"C:\Program Files (x86)\PowerShell\7\pwsh.exe",
    ] {
        if std::path::Path::new(candidate).is_file() {
            return candidate.to_string();
        }
    }
    if let Some(p) = find_on_path("powershell.exe") {
        return p;
    }
    for candidate in [
        r"C:\Program Files\Git\bin\bash.exe",
        r"C:\Program Files\Git\usr\bin\bash.exe",
        r"C:\Program Files (x86)\Git\bin\bash.exe",
    ] {
        if std::path::Path::new(candidate).is_file() {
            return candidate.to_string();
        }
    }
    std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_string())
}

#[cfg(not(windows))]
fn resolve_default_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
}

/// Lightweight `which` replacement so the crate doesn't pull a new
/// dependency. Splits `$PATH` with the platform separator and probes
/// each entry for the exe.
#[cfg(windows)]
fn find_on_path(exe: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(exe);
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

/// Build the env passed to the spawned shell. Order matches Bun
/// (`packages/opencode/src/pty/index.ts:185-208`):
///
/// 1. Start from the parent process env.
/// 2. Layer the request body's overrides on top.
/// 3. Strip `KILO_SERVER_PASSWORD` / `KILO_SERVER_USERNAME` so the
///    sidecar's auth credential never reaches user shells (npm
///    post-install scripts, `curl | bash`, supply-chain-compromised
///    tools would otherwise see it for free).
/// 4. Inject `TERM=xterm-256color` (only if not already set) and
///    `KILO_TERMINAL=1`. On Windows additionally force the UTF-8
///    locale (`LANG`/`LC_ALL=en_US.UTF-8`) so PowerShell/git-bash
///    output renders with non-ASCII bytes intact.
fn build_pty_env(request_env: BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut env: BTreeMap<String, String> = std::env::vars().collect();
    for (k, v) in request_env {
        env.insert(k, v);
    }
    env.remove("KILO_SERVER_PASSWORD");
    env.remove("KILO_SERVER_USERNAME");
    env.entry("TERM".to_string())
        .or_insert_with(|| "xterm-256color".to_string());
    env.insert("KILO_TERMINAL".to_string(), "1".to_string());
    if cfg!(windows) {
        env.insert("LANG".to_string(), "en_US.UTF-8".to_string());
        env.insert("LC_ALL".to_string(), "en_US.UTF-8".to_string());
    }
    env
}

/// Tree-kill the pty's child process tree. Returns `true` if the
/// helper successfully ran (regardless of whether every grandchild
/// died — caller still falls back to `child.kill()` on `false`).
///
/// * Windows: spawn `taskkill /F /T /PID <pid>` with `CREATE_NO_WINDOW`
///   so VS Code doesn't flash a console window.
/// * POSIX: send SIGTERM to the negative pid (process group). No `nix`
///   dep in this workspace, so we shell out to `/bin/kill -- -<pid>`.
fn kill_tree(pid: Option<u32>) -> bool {
    let Some(pid) = pid else { return false };
    if pid == 0 {
        return false;
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let status = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            // CREATE_NO_WINDOW = 0x08000000 (matches lock.rs).
            .creation_flags(0x0800_0000)
            .status();
        matches!(status, Ok(s) if s.success())
    }
    #[cfg(not(windows))]
    {
        let status = std::process::Command::new("/bin/kill")
            .args(["--", &format!("-{pid}")])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        matches!(status, Ok(s) if s.success())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env-mutating tests serialize on this guard so concurrent
    /// `set_var`/`remove_var` calls don't corrupt one another's
    /// observed state. `cargo test` runs with multiple threads by
    /// default, and we need TERM/KILO_SERVER_PASSWORD/SHELL to be
    /// stable for the duration of each test body.
    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        match LOCK.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Env scrub: parent baseline carries the credential, request
    /// override layers more env on top, both auth keys must be gone
    /// from the result, and the injected vars must be present.
    #[test]
    fn build_pty_env_scrubs_credentials_and_injects_defaults() {
        let _g = env_lock();
        std::env::set_var("KILO_SERVER_PASSWORD", "parent-secret");
        std::env::set_var("KILO_SERVER_USERNAME", "parent-user");
        std::env::remove_var("TERM");

        let mut request = BTreeMap::new();
        request.insert(
            "KILO_SERVER_PASSWORD".to_string(),
            "request-secret".to_string(),
        );
        request.insert("CUSTOM_VAR".to_string(), "value".to_string());
        let env = build_pty_env(request);

        std::env::remove_var("KILO_SERVER_PASSWORD");
        std::env::remove_var("KILO_SERVER_USERNAME");

        assert!(
            !env.contains_key("KILO_SERVER_PASSWORD"),
            "KILO_SERVER_PASSWORD must be stripped (request override included)"
        );
        assert!(
            !env.contains_key("KILO_SERVER_USERNAME"),
            "KILO_SERVER_USERNAME must be stripped"
        );
        assert_eq!(env.get("CUSTOM_VAR").map(String::as_str), Some("value"));
        assert_eq!(
            env.get("TERM").map(String::as_str),
            Some("xterm-256color"),
            "TERM injected when not already set"
        );
        assert_eq!(env.get("KILO_TERMINAL").map(String::as_str), Some("1"));
    }

    #[test]
    fn build_pty_env_preserves_existing_term() {
        let _g = env_lock();
        std::env::set_var("TERM", "screen-256color");
        let env = build_pty_env(BTreeMap::new());
        // Caller's own TERM must win; we only inject when missing.
        assert_eq!(env.get("TERM").map(String::as_str), Some("screen-256color"));
        std::env::remove_var("TERM");
    }

    #[cfg(windows)]
    #[test]
    fn build_pty_env_forces_utf8_locale_on_windows() {
        let _g = env_lock();
        let env = build_pty_env(BTreeMap::new());
        assert_eq!(env.get("LANG").map(String::as_str), Some("en_US.UTF-8"));
        assert_eq!(env.get("LC_ALL").map(String::as_str), Some("en_US.UTF-8"));
    }

    /// Windows shell selection priority. Empty/scrubbed env should
    /// still fall through the chain to a real shell — at minimum the
    /// `cmd.exe` fallback that the original implementation used.
    /// Asserting the resolver's return shape (single non-empty string
    /// pointing at an exe-like path) without spawning anything.
    #[cfg(windows)]
    #[test]
    fn resolve_default_shell_falls_back_to_comspec_or_cmd() {
        let _g = env_lock();
        // Force every preferred candidate to be unreachable: clear
        // env vars and the resolver chain returns the COMSPEC/cmd
        // fallback. We can't realistically clear PATH here (the test
        // harness needs it), so we assert the result is a non-empty
        // shell path that ends in a known shell exe name. The chain
        // is deterministic given env state.
        std::env::remove_var("KILO_GIT_BASH_PATH");
        let shell = resolve_default_shell();
        assert!(!shell.is_empty(), "resolver must produce a non-empty path");
        let lower = shell.to_lowercase();
        let known = ["pwsh.exe", "powershell.exe", "bash.exe", "cmd.exe"];
        assert!(
            known.iter().any(|n| lower.ends_with(n)),
            "resolver returned unexpected shell: {shell:?}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn resolve_default_shell_honors_kilo_git_bash_path() {
        let _g = env_lock();
        // When KILO_GIT_BASH_PATH points at a real file, the
        // resolver returns it unchanged (Bun parity). Use the test
        // binary itself as a stand-in — it definitely exists.
        let exe = std::env::current_exe().expect("current_exe");
        let exe_str = exe.to_string_lossy().into_owned();
        std::env::set_var("KILO_GIT_BASH_PATH", &exe_str);
        let shell = resolve_default_shell();
        std::env::remove_var("KILO_GIT_BASH_PATH");
        assert_eq!(shell, exe_str);
    }

    #[cfg(not(windows))]
    #[test]
    fn resolve_default_shell_uses_shell_env_or_sh() {
        let _g = env_lock();
        // POSIX path: prefer $SHELL, fall back to /bin/sh.
        std::env::remove_var("SHELL");
        assert_eq!(resolve_default_shell(), "/bin/sh");
        std::env::set_var("SHELL", "/usr/bin/zsh");
        assert_eq!(resolve_default_shell(), "/usr/bin/zsh");
        std::env::remove_var("SHELL");
    }
}
