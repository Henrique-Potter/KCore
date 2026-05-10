//! Bun sidecar process spawn + readiness handshake.
//!
//! The oracle starts the same Bun binary the VS Code extension launches, with
//! the same args (`serve --port 0`) and a curated subset of the env vars the
//! extension passes. See `packages/kilo-vscode/src/services/cli-backend/server-manager.ts`.
//!
//! Behavioral notes:
//!
//! - We bind to a fresh `tempdir` for the working directory rather than
//!   inheriting the developer's repo. This keeps the per-test `.kilo` store
//!   self-contained.
//! - We point `KILO_HOME` at the same tempdir so Bun does not write into the
//!   user's real `~/.kilo`.
//! - We deliberately filter out a small allow-list of env vars that could
//!   leak global state (`KILO_HOME`, `KILO_SERVER_PASSWORD`, anything
//!   matching `KILOCODE_*`).
//! - On Drop, we issue an immediate kill (Tokio's `kill_on_drop` plus
//!   `start_kill`). Graceful SIGTERM matches the extension's eventual
//!   behavior; for M0 we follow the contract note in `CONTRACT.md` that
//!   "Windows process termination from the extension may still be immediate
//!   during this skeleton stage" and keep the dependency surface small.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

use crate::error::{OracleError, OracleResult};
use crate::readiness::{parse_ready_line, ReadyLine};

/// Default startup timeout in seconds. Mirrors the extension's
/// `STARTUP_TIMEOUT_SECONDS = 30`.
pub const DEFAULT_READY_TIMEOUT_SECS: u64 = 30;

/// Configuration for spawning the Bun sidecar.
#[derive(Debug, Clone)]
pub struct SpawnConfig {
    pub binary: PathBuf,
    /// Working directory for the spawned process. Default: a fresh tempdir.
    pub cwd: Option<PathBuf>,
    /// Override the password. Default: a random 64-character ASCII
    /// alphanumeric string produced by [`random_password`] (L4: not hex —
    /// the previous doc comment claimed 32-byte hex, but the actual
    /// generator emits `[A-Za-z0-9]{64}`).
    pub password: Option<String>,
    /// Maximum time to wait for the readiness line.
    pub ready_timeout: Duration,
    /// Extra env vars layered on top of defaults.
    pub extra_env: HashMap<String, String>,
    /// Inherit *no* env from the parent except this allowlist + the defaults
    /// that this struct sets explicitly. By default we allow `PATH`,
    /// `SystemRoot`, `USERPROFILE`/`HOME`, and `TEMP`.
    pub env_allowlist: HashSet<String>,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        let mut allow: HashSet<String> = HashSet::new();
        for k in [
            "PATH",
            "Path",
            "PATHEXT",
            "SystemRoot",
            "SYSTEMROOT",
            "USERPROFILE",
            "HOME",
            "TEMP",
            "TMP",
            "ComSpec",
            "windir",
        ] {
            allow.insert(k.to_string());
        }
        Self {
            binary: default_binary_path(),
            cwd: None,
            password: None,
            ready_timeout: Duration::from_secs(DEFAULT_READY_TIMEOUT_SECS),
            extra_env: HashMap::new(),
            env_allowlist: allow,
        }
    }
}

impl SpawnConfig {
    pub fn with_binary(mut self, p: impl Into<PathBuf>) -> Self {
        self.binary = p.into();
        self
    }

    pub fn with_cwd(mut self, p: impl Into<PathBuf>) -> Self {
        self.cwd = Some(p.into());
        self
    }

    pub fn with_password(mut self, p: impl Into<String>) -> Self {
        self.password = Some(p.into());
        self
    }

    pub fn with_extra_env(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.extra_env.insert(k.into(), v.into());
        self
    }
}

/// Default location of the Bun binary, matching the VS Code extension's
/// `getCliPath()`: `<repo>/packages/kilo-vscode/bin/kilo[.exe]`.
///
/// The repo root is discovered relative to the current working directory by
/// walking upward looking for `Cargo.toml`. When this fails (e.g. running
/// from outside the repo) the caller must override via `SpawnConfig::binary`
/// or the `KILO_ORACLE_BINARY` env var.
pub fn default_binary_path() -> PathBuf {
    if let Ok(env) = std::env::var("KILO_ORACLE_BINARY") {
        return PathBuf::from(env);
    }
    let bin_name = if cfg!(windows) { "kilo.exe" } else { "kilo" };
    if let Some(repo) = find_repo_root() {
        return repo
            .join("packages")
            .join("kilo-vscode")
            .join("bin")
            .join(bin_name);
    }
    PathBuf::from(bin_name)
}

fn find_repo_root() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let mut cursor: &Path = cwd.as_path();
    loop {
        if cursor.join("packages").join("kilo-vscode").exists()
            && cursor
                .join("packages")
                .join("kilo-vscode-sidecar-rs")
                .exists()
        {
            return Some(cursor.to_path_buf());
        }
        cursor = cursor.parent()?;
    }
}

/// Cap the post-readiness stdout ring buffer so a long-lived recording can't
/// retain unbounded memory. Anything past this is discarded.
const STDOUT_RING_CAPACITY_BYTES: usize = 64 * 1024;

/// A live Bun sidecar process plus the bookkeeping needed to stop it cleanly.
pub struct SidecarHandle {
    pub ready: ReadyLine,
    pub password: String,
    /// Operating-system process id for external resource sampling.
    pub pid: u32,
    /// The cwd the process is running in. Tests use this to point
    /// fixture-storing utilities at the right `.kilo` directory.
    pub cwd: PathBuf,
    child: Option<Child>,
    /// Holds the tempdir alive for the lifetime of the sidecar.
    _temp: Option<TempDir>,
    /// Bounded ring of stdout bytes captured after the readiness line has
    /// been parsed. The drain task continues running for the lifetime of
    /// the handle to prevent the OS pipe from filling and stalling the
    /// child. Exposed for diagnostics via [`Self::stdout_tail`].
    stdout_ring: Arc<Mutex<String>>,
}

impl SidecarHandle {
    /// Launch a sidecar according to `cfg`. Resolves once the readiness line
    /// has been observed on stdout.
    pub async fn spawn(cfg: SpawnConfig) -> OracleResult<Self> {
        let (cwd, _temp) = match &cfg.cwd {
            Some(path) => (path.clone(), None),
            None => {
                let tmp = tempfile::tempdir()
                    .map_err(|e| OracleError::Other(format!("create tempdir: {e}")))?;
                (tmp.path().to_path_buf(), Some(tmp))
            }
        };

        let password = cfg.password.clone().unwrap_or_else(random_password);
        let env = build_env(&cfg, &cwd, &password);

        let mut command = Command::new(&cfg.binary);
        command
            .arg("serve")
            .arg("--port")
            .arg("0")
            .current_dir(&cwd)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|e| {
            OracleError::Other(format!(
                "failed to spawn {} ({}): {}",
                cfg.binary.display(),
                cfg.binary.exists().then_some("exists").unwrap_or("missing"),
                e
            ))
        })?;
        let pid = child
            .id()
            .ok_or_else(|| OracleError::other("spawned sidecar did not expose a process id"))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| OracleError::other("child stdout was not piped"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| OracleError::other("child stderr was not piped"))?;

        // Spool stderr in the background so the pipe doesn't fill and stall
        // the child. We hand the buffer back if we need to report an early
        // exit.
        let (stderr_tx, stderr_rx) = oneshot::channel::<String>();
        tokio::spawn(async move {
            let mut buf = String::new();
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                buf.push_str(&line);
                buf.push('\n');
            }
            let _ = stderr_tx.send(buf);
        });

        // Read stdout line by line until we see the readiness signature or
        // hit the timeout / process death. We hand the same `Lines` iterator
        // back out of the future so the post-readiness drain task can pick
        // up exactly where readiness parsing stopped (no bytes lost between
        // the two phases).
        let timeout_dur = cfg.ready_timeout;
        let reader = BufReader::new(stdout).lines();
        let ready_future = async move {
            let mut reader = reader;
            while let Some(line) = reader.next_line().await? {
                if let Some(parsed) = parse_ready_line(&line) {
                    return OracleResult::Ok((parsed, reader));
                }
            }
            Err(OracleError::other("stdout closed before readiness"))
        };

        let (parsed, reader) = match timeout(timeout_dur, ready_future).await {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => {
                // Wait briefly for any final stderr output and the child's
                // exit code so we can report a useful message.
                let _ = sleep(Duration::from_millis(100)).await;
                let code = child.try_wait().ok().flatten().and_then(|s| s.code());
                let stderr_buf = stderr_rx.await.unwrap_or_default();
                if !stderr_buf.is_empty() {
                    return Err(OracleError::EarlyExit {
                        code,
                        stderr: stderr_buf,
                    });
                }
                return Err(e);
            }
            Err(_elapsed) => {
                let _ = child.kill().await;
                let stderr_buf = stderr_rx.await.unwrap_or_default();
                tracing::warn!(stderr = %stderr_buf, "sidecar failed to become ready");
                return Err(OracleError::ReadyTimeout {
                    timeout_secs: timeout_dur.as_secs(),
                });
            }
        };

        // Continue draining stdout in a background task for the lifetime of
        // this `SidecarHandle`. Without this, long recordings stall the
        // child once the OS pipe buffer fills (Bun keeps logging diagnostics
        // after readiness). Mirrors the stderr drain pattern above. We
        // retain the tail of the output in a bounded ring buffer for
        // diagnostics; older bytes are dropped.
        let stdout_ring = Arc::new(Mutex::new(String::new()));
        let stdout_ring_writer = Arc::clone(&stdout_ring);
        tokio::spawn(async move {
            let mut reader = reader;
            while let Ok(Some(line)) = reader.next_line().await {
                if let Ok(mut guard) = stdout_ring_writer.lock() {
                    let needed = line.len() + 1;
                    if guard.len() + needed > STDOUT_RING_CAPACITY_BYTES {
                        let drop_to =
                            (guard.len() + needed).saturating_sub(STDOUT_RING_CAPACITY_BYTES);
                        // Drop from the front in chunks aligned on UTF-8
                        // boundaries (lines), not by byte index, so we
                        // never split a multi-byte codepoint.
                        if drop_to >= guard.len() {
                            guard.clear();
                        } else {
                            // Find the first newline at or past `drop_to`.
                            if let Some(pos) = guard[drop_to..].find('\n') {
                                let end = drop_to + pos + 1;
                                guard.drain(..end);
                            } else {
                                guard.clear();
                            }
                        }
                    }
                    guard.push_str(&line);
                    guard.push('\n');
                }
            }
        });

        Ok(Self {
            ready: parsed,
            password,
            pid,
            cwd,
            child: Some(child),
            _temp,
            stdout_ring,
        })
    }

    /// Snapshot of the most recent stdout bytes captured after readiness.
    /// Bounded by [`STDOUT_RING_CAPACITY_BYTES`]; useful for diagnostics
    /// when a scenario fails partway through.
    pub fn stdout_tail(&self) -> String {
        self.stdout_ring
            .lock()
            .map(|s| s.clone())
            .unwrap_or_default()
    }

    /// Stop the sidecar process. M0 uses an immediate kill matching the
    /// "Windows process termination ... may still be immediate" note in
    /// `CONTRACT.md`. A future SIGTERM-with-grace path can land alongside
    /// the same upgrade in the extension.
    pub async fn shutdown(&mut self) -> OracleResult<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        let _ = child.start_kill();
        let killed = timeout(Duration::from_secs(5), child.wait()).await;
        if killed.is_err() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        Ok(())
    }
}

impl Drop for SidecarHandle {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // tokio's Child has kill_on_drop set, so this is just belt and
            // suspenders. We can't await here.
            let _ = child.start_kill();
        }
    }
}

fn random_password() -> String {
    let mut rng = StdRng::from_entropy();
    (0..64)
        .map(|_| char::from(rng.sample(Alphanumeric)))
        .collect()
}

fn build_env(cfg: &SpawnConfig, cwd: &Path, password: &str) -> HashMap<String, String> {
    let mut env: HashMap<String, String> = HashMap::new();

    // 1. Inherit allowlist.
    for (k, v) in std::env::vars() {
        if cfg.env_allowlist.contains(&k) {
            env.insert(k, v);
        }
    }

    // 2. Force a self-contained KILO_HOME inside the cwd so the sidecar does
    //    not touch the user's real `.kilo` folder.
    let kilo_home = cwd.join(".kilo");
    env.insert(
        "KILO_HOME".to_string(),
        kilo_home.to_string_lossy().to_string(),
    );

    // 3. Mirror the extension's defaults (subset relevant for VS Code).
    env.insert("KILO_SERVER_PASSWORD".to_string(), password.to_string());
    env.insert("KILO_CLIENT".to_string(), "vscode".to_string());
    env.insert("KILO_ENABLE_QUESTION_TOOL".to_string(), "true".to_string());
    env.insert(
        "KILOCODE_FEATURE".to_string(),
        "vscode-extension".to_string(),
    );
    env.insert("KILO_TELEMETRY_LEVEL".to_string(), "off".to_string());
    env.insert("KILO_APP_NAME".to_string(), "kilo-code".to_string());
    env.insert("KILO_PLATFORM".to_string(), "vscode".to_string());
    env.insert("KILO_DISABLE_CLAUDE_CODE".to_string(), "true".to_string());
    // Bun-specific: forces mimalloc to release pages immediately. Mirrors
    // `MIMALLOC_PURGE_DELAY` in `server-manager.ts`.
    env.insert("MIMALLOC_PURGE_DELAY".to_string(), "0".to_string());

    // 4. Caller overrides (highest precedence).
    for (k, v) in &cfg.extra_env {
        env.insert(k.clone(), v.clone());
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_env_sets_required_vars() {
        let cfg = SpawnConfig::default();
        let env = build_env(&cfg, Path::new("/tmp/mockcwd"), "secret");
        assert_eq!(
            env.get("KILO_SERVER_PASSWORD").map(String::as_str),
            Some("secret")
        );
        assert_eq!(env.get("KILO_CLIENT").map(String::as_str), Some("vscode"));
        assert_eq!(
            env.get("MIMALLOC_PURGE_DELAY").map(String::as_str),
            Some("0")
        );
        let kilo_home = env.get("KILO_HOME").expect("KILO_HOME must be set");
        assert!(kilo_home.ends_with(".kilo"));
    }

    #[test]
    fn build_env_overrides_apply_last() {
        let mut cfg = SpawnConfig::default();
        cfg.extra_env
            .insert("KILO_PLATFORM".to_string(), "test".to_string());
        let env = build_env(&cfg, Path::new("/tmp/mockcwd"), "secret");
        assert_eq!(env.get("KILO_PLATFORM").map(String::as_str), Some("test"));
    }

    #[test]
    fn random_password_has_expected_length() {
        let pw = random_password();
        assert_eq!(pw.len(), 64);
        assert!(pw.chars().all(|c| c.is_ascii_alphanumeric()));
    }
}
