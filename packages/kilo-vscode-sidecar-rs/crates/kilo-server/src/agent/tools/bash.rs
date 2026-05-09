//! `bash` fake-tool runtime.
//!
//! Step 7 of the kilo-server module split: verbatim cut from `lib.rs`. The
//! `resolve_under` path helper now lives in `util::paths`.

use std::{
    io::Read,
    path::Path as FsPath,
    process::{Child, Command, Stdio},
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

use serde_json::{json, Value};

use crate::util::paths::resolve_under;

pub(crate) const MAX_BASH_OUTPUT_BYTES: usize = 64 * 1024;
pub(crate) const DEFAULT_BASH_TIMEOUT_MS: u64 = 60_000;

#[allow(clippy::too_many_arguments)]
pub(crate) fn fake_bash(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    fake_bash_with_cancel(root, input, None)
}

pub(crate) fn fake_bash_with_cancel(
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    let command = input
        .get("command")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "command is required".to_string())?;
    let dir = input.get("workdir").and_then(Value::as_str).unwrap_or("");
    let cwd = resolve_under(root, dir).map_err(|_| format!("Unsafe workdir: {dir}"))?;
    let timeout = tool_timeout(input)?;
    let description = input
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or(command);
    let (mut child, started) = shell_command(command, &cwd)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Unable to capture command stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Unable to capture command stderr".to_string())?;
    let out = read_pipe(stdout);
    let err = read_pipe(stderr);
    let mut expired = false;
    let mut cancelled = false;
    let code = loop {
        let status = child
            .try_wait()
            .map_err(|err| format!("Unable to wait for command: {err}"))?;
        if let Some(status) = status {
            break status.code();
        }
        if cancel.is_some_and(crate::agent::is_canceled) {
            cancelled = true;
            kill_child(&mut child)
                .map_err(|err| format!("Unable to kill cancelled command: {err}"))?;
            child
                .wait()
                .map_err(|err| format!("Unable to wait for cancelled command: {err}"))?;
            break None;
        }
        if started.elapsed() >= timeout {
            expired = true;
            kill_child(&mut child)
                .map_err(|err| format!("Unable to kill timed out command: {err}"))?;
            child
                .wait()
                .map_err(|err| format!("Unable to wait for timed out command: {err}"))?;
            break None;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = out
        .join()
        .map_err(|_| "Unable to collect command stdout".to_string())?;
    let stderr = err
        .join()
        .map_err(|_| "Unable to collect command stderr".to_string())?;
    let text = combined_output(&stdout, &stderr);
    let (preview, truncated) = truncate_output(&text);
    let output = if preview.is_empty() {
        "(no output)".to_string()
    } else {
        preview
    };
    let metadata = json!({
        "output": output,
        "exit": code,
        "description": description,
        "truncated": truncated,
        "timeout": expired,
        "cancelled": cancelled,
    });

    Ok((description.to_string(), output, metadata))
}

fn kill_child(child: &mut Child) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        let killed = Command::new("taskkill")
            .args(["/PID", &child.id().to_string(), "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if killed {
            let _ = child.kill();
            return Ok(());
        }
    }
    child.kill()
}

fn shell_command(command: &str, cwd: &FsPath) -> Result<(std::process::Child, Instant), String> {
    let mut cmd = build_shell_command(command)?;
    let child = cmd
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("Unable to run command: {err}"))?;
    Ok((child, Instant::now()))
}

/// Build the per-platform `Command` that runs a bash-shaped command line.
///
/// Per migration plan **Cross-shell tool execution** (M8): the bash tool
/// surface must work on Windows, where bash is not the default shell.
/// Resolution order on Windows:
///
/// 1. WSL `bash` (`bash.exe`) if available.
/// 2. Git Bash if installed (detect via `where bash` and `git --exec-path`).
/// 3. cmd.exe-wrapped `bash.exe` if Git for Windows is on PATH.
/// 4. Fail with a stable error name (`shell_unavailable`) if none resolve.
///
/// PowerShell parity is NOT a goal — Bun's tool semantics assume bash
/// quoting, redirects, and environment expansion; pretending PowerShell
/// is interchangeable is a worse failure mode than refusing to run.
fn build_shell_command(command: &str) -> Result<Command, String> {
    if !cfg!(windows) {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", command]);
        return Ok(cmd);
    }
    match resolve_windows_bash() {
        Some(WindowsBash::Direct(path)) => {
            let mut cmd = Command::new(path);
            cmd.args(["-c", command]);
            Ok(cmd)
        }
        Some(WindowsBash::CmdWrapped(bash)) => {
            // Run cmd.exe with `bash.exe -c "<command>"`. cmd.exe handles
            // PATH resolution and we hand the whole bash invocation off
            // to it as a single string so quoting stays bash's job.
            let mut cmd = Command::new("cmd.exe");
            cmd.args([
                "/C",
                &format!("{bash} -c \"{}\"", command.replace('"', "\\\"")),
            ]);
            Ok(cmd)
        }
        None => Err(SHELL_UNAVAILABLE_ERROR.to_string()),
    }
}

/// Stable error name (lower-snake-case) returned to the SDK when the
/// bash tool can't find a usable shell on Windows. Matches the entry in
/// `error::ALLOWED_INTERNAL_ERROR_NAMES`.
pub(crate) const SHELL_UNAVAILABLE_ERROR: &str = "shell_unavailable";

#[derive(Debug, PartialEq, Eq)]
enum WindowsBash {
    /// Path to a bash executable spawnable directly via `Command::new(path)`.
    Direct(std::path::PathBuf),
    /// bash.exe is reachable via cmd.exe's PATH resolution (Git for Windows
    /// installs typically expose this).
    CmdWrapped(String),
}

#[cfg(windows)]
fn resolve_windows_bash() -> Option<WindowsBash> {
    use std::path::PathBuf;

    // 1. WSL — `bash.exe` shipped with Windows resolves via PATH on
    //    standard installs. Prefer it because it provides a real Linux
    //    userland.
    if let Some(p) = find_in_path("bash.exe") {
        // Filter out a Git Bash shim that lands ahead of WSL — accept
        // either, but prefer the System32 one if present.
        let lossy = p.to_string_lossy().to_lowercase();
        if lossy.contains("system32") || lossy.contains("wsl") {
            return Some(WindowsBash::Direct(p));
        }
        // Anything else is likely Git for Windows.
        return Some(WindowsBash::Direct(p));
    }
    // 2. Git for Windows fallback — try `git --exec-path` to locate the
    //    install and reach `bin/bash.exe` from there.
    if let Some(git) = find_in_path("git.exe") {
        if let Some(parent) = git.parent() {
            let candidate: PathBuf = parent.join("bash.exe");
            if candidate.exists() {
                return Some(WindowsBash::Direct(candidate));
            }
            // Look one level up for `bin/bash.exe` (typical Git for Windows
            // layout: `cmd/git.exe` and `bin/bash.exe`).
            if let Some(grandparent) = parent.parent() {
                let candidate = grandparent.join("bin").join("bash.exe");
                if candidate.exists() {
                    return Some(WindowsBash::Direct(candidate));
                }
            }
        }
        // Last resort on Windows: ask cmd.exe to find bash via PATH.
        return Some(WindowsBash::CmdWrapped("bash.exe".to_string()));
    }
    None
}

#[cfg(not(windows))]
fn resolve_windows_bash() -> Option<WindowsBash> {
    None
}

#[cfg(windows)]
fn find_in_path(name: &str) -> Option<std::path::PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

fn read_pipe(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let _ = pipe.read_to_end(&mut out);
        out
    })
}

fn combined_output(stdout: &[u8], stderr: &[u8]) -> String {
    let out = String::from_utf8_lossy(stdout);
    let err = String::from_utf8_lossy(stderr);
    if out.is_empty() {
        return err.to_string();
    }
    if err.is_empty() {
        return out.to_string();
    }
    format!("{out}{err}")
}

fn truncate_output(text: &str) -> (String, bool) {
    let bytes = text.as_bytes();
    if bytes.len() <= MAX_BASH_OUTPUT_BYTES {
        return (text.to_string(), false);
    }
    let mut end = MAX_BASH_OUTPUT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), true)
}

fn tool_timeout(input: &Value) -> Result<Duration, String> {
    let Some(value) = input.get("timeout") else {
        return Ok(Duration::from_millis(DEFAULT_BASH_TIMEOUT_MS));
    };
    let Some(ms) = value.as_i64() else {
        return Err("timeout must be greater than or equal to 0".to_string());
    };
    if ms < 0 {
        return Err("timeout must be greater than or equal to 0".to_string());
    }
    Ok(Duration::from_millis(
        (ms as u64).min(DEFAULT_BASH_TIMEOUT_MS),
    ))
}
