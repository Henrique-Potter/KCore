//! Single-sidecar-per-user coordination via a lock file.
//!
//! Per migration plan **Operational invariants → 3**: two VS Code windows
//! on the same workspace must not spawn two writer processes against the
//! same `kilo.db`. SQLite WAL handles concurrent writes safely at the
//! file level, but the per-Store writer mutex inside `kilo-store` does
//! not coordinate across processes — split-brain session state is the
//! failure mode.
//!
//! Strategy:
//!
//! 1. On startup, read `<state_dir>/kilo/sidecar.lock` if it exists.
//! 2. If the prior PID is alive AND the recorded port answers a health
//!    probe, this is a "join" — return [`LockOutcome::Join`] with the
//!    existing port so the new process can either re-emit the readiness
//!    line and exit, or hand the port back to its caller.
//! 3. Otherwise, the prior owner is dead/unreachable. This is a
//!    "takeover" — overwrite the lock atomically and continue startup.
//! 4. The lock file is updated with the live PID and port once the
//!    server has bound, via [`SidecarLock::record`]. On graceful
//!    shutdown the file is removed (best-effort).
//!
//! The lock content is JSON `{ "pid": u32, "port": u16, "started_at_ms": i64 }`.
//! Atomic write goes through `tempfile + rename`; rename within the same
//! filesystem is atomic on Windows, macOS, and Linux.

use std::{
    fs,
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};

/// The lock filename inside `<state_dir>/kilo/`.
const LOCK_FILENAME: &str = "sidecar.lock";

/// How long [`SidecarLock::probe_health`] waits before declaring the prior
/// owner dead. Short enough not to delay startup noticeably.
const HEALTH_PROBE_TIMEOUT_MS: u64 = 1500;

#[derive(Debug)]
pub(crate) struct SidecarLock {
    path: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LockOutcome {
    /// No live owner. Caller should proceed with startup and call
    /// [`SidecarLock::record`] once the server is listening.
    Takeover,
    /// A live owner is already serving on `port`. Caller should hand
    /// this port back instead of spawning another sidecar.
    Join { port: u16 },
}

impl SidecarLock {
    /// Build the lock handle from the resolved state directory. Does
    /// not touch the filesystem yet.
    pub(crate) fn at(state_dir: &Path) -> Self {
        Self {
            path: state_dir.join("kilo").join(LOCK_FILENAME),
        }
    }

    /// Inspect any prior lock and decide whether to take over or join.
    /// Side-effects: ensures the parent directory exists; never deletes
    /// the lock file.
    pub(crate) fn acquire(&self) -> std::io::Result<LockOutcome> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        let prior = match read_lock(&self.path) {
            Ok(prior) => prior,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(LockOutcome::Takeover),
            // Corrupt JSON / permission errors — treat as no prior. The
            // self-healing invariant says partial state must converge.
            Err(_) => return Ok(LockOutcome::Takeover),
        };
        let alive = prior.pid.map(process_alive).unwrap_or(false);
        let port_responsive = prior.port.map(probe_health).unwrap_or(false);
        if alive && port_responsive {
            return Ok(LockOutcome::Join {
                port: prior.port.unwrap_or_default(),
            });
        }
        Ok(LockOutcome::Takeover)
    }

    /// Atomically record the live sidecar's PID and port. Called after
    /// the server has bound and the readiness line has been printed, so
    /// concurrent late starters see a valid lock.
    pub(crate) fn record(&self, port: u16) -> std::io::Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }
        let payload = json!({
            "pid": std::process::id(),
            "port": port,
            "started_at_ms": now_millis(),
        });
        let temp = self.path.with_extension("tmp");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;
        file.write_all(payload.to_string().as_bytes())?;
        file.sync_all()?;
        drop(file);
        // Rename is atomic within a single filesystem on Windows, macOS,
        // and Linux. Falling back to non-atomic write+remove would leave
        // a window where readers see a half-written file.
        fs::rename(&temp, &self.path)
    }

    /// Remove the lock file on graceful shutdown. Best-effort: a missing
    /// file is not an error.
    pub(crate) fn release(&self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug, Default)]
struct LockContent {
    pid: Option<u32>,
    port: Option<u16>,
}

fn read_lock(path: &Path) -> std::io::Result<LockContent> {
    let mut buf = String::new();
    fs::File::open(path)?.read_to_string(&mut buf)?;
    let value: Value = match serde_json::from_str(&buf) {
        Ok(value) => value,
        Err(_) => return Ok(LockContent::default()),
    };
    Ok(LockContent {
        pid: value
            .get("pid")
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok()),
        port: value
            .get("port")
            .and_then(Value::as_u64)
            .and_then(|n| u16::try_from(n).ok()),
    })
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    // Stdlib-only probe: a directory entry under /proc on Linux, or a
    // `kill -0 $pid` shell-out on macOS. Linux is the common case; on
    // platforms where /proc isn't present we fall back to the shell path.
    if cfg!(target_os = "linux") {
        return std::path::Path::new(&format!("/proc/{pid}")).exists();
    }
    let status = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .status();
    match status {
        Ok(status) => status.success(),
        Err(_) => true, // be conservative — treat as alive
    }
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    // Pure-stdlib check: shell out to tasklist. Slower than OpenProcess
    // but avoids a winapi dependency for one syscall.
    use std::process::{Command, Stdio};
    let output = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .creation_flags_no_window()
        .output();
    let Ok(output) = output else {
        // tasklist unavailable — be conservative and treat as alive so we
        // don't accidentally double-spawn.
        return true;
    };
    if !output.status.success() {
        return true;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    // tasklist with no match prints "INFO: No tasks are running…" to
    // stderr (suppressed) and produces empty stdout for our filter.
    stdout.contains(&format!("\"{pid}\""))
}

#[cfg(not(any(unix, windows)))]
fn process_alive(_pid: u32) -> bool {
    // Unknown platform — be conservative.
    true
}

/// Fire a synchronous TCP connect to the recorded port and read the
/// response to `GET /global/health`. We don't need the full HTTP parse;
/// any 200-flavored response is enough to confirm a live owner.
fn probe_health(port: u16) -> bool {
    use std::io::{BufRead, BufReader};
    use std::net::TcpStream;
    use std::time::Duration;

    let addr = format!("127.0.0.1:{port}");
    let timeout = Duration::from_millis(HEALTH_PROBE_TIMEOUT_MS);
    let Ok(mut stream) = TcpStream::connect_timeout(
        &match addr.parse() {
            Ok(addr) => addr,
            Err(_) => return false,
        },
        timeout,
    ) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let req =
        format!("GET /global/health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",);
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut reader = BufReader::new(stream);
    let mut status = String::new();
    if reader.read_line(&mut status).is_err() {
        return false;
    }
    // Auth-protected sidecars return 401, which is still proof-of-life.
    status.contains("HTTP/1.1 200")
        || status.contains("HTTP/1.1 401")
        || status.contains("HTTP/1.0 200")
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

#[cfg(windows)]
trait CommandExtNoWindow {
    fn creation_flags_no_window(&mut self) -> &mut Self;
}

#[cfg(windows)]
impl CommandExtNoWindow for std::process::Command {
    fn creation_flags_no_window(&mut self) -> &mut Self {
        use std::os::windows::process::CommandExt;
        // CREATE_NO_WINDOW = 0x08000000. Suppresses the console flash
        // when spawning child processes from a windowed VS Code instance,
        // matching the migration plan's Cross-platform path handling
        // section.
        self.creation_flags(0x08000000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn temp_dir(name: &str) -> PathBuf {
        let mut p = env::temp_dir();
        p.push(format!(
            "kilo-lock-test-{name}-{}-{}",
            std::process::id(),
            now_millis()
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn acquire_with_no_prior_lock_returns_takeover() {
        let dir = temp_dir("no-prior");
        let lock = SidecarLock::at(&dir);
        let outcome = lock.acquire().unwrap();
        assert_eq!(outcome, LockOutcome::Takeover);
    }

    #[test]
    fn record_then_release_cleans_up() {
        let dir = temp_dir("record-release");
        let lock = SidecarLock::at(&dir);
        lock.record(54321).unwrap();
        assert!(lock.path.exists());
        lock.release();
        assert!(!lock.path.exists());
    }

    #[test]
    fn corrupt_lock_falls_back_to_takeover() {
        let dir = temp_dir("corrupt");
        let lock = SidecarLock::at(&dir);
        fs::create_dir_all(lock.path.parent().unwrap()).unwrap();
        fs::write(&lock.path, "this is not json").unwrap();
        let outcome = lock.acquire().unwrap();
        assert_eq!(outcome, LockOutcome::Takeover);
    }

    #[test]
    fn dead_pid_falls_back_to_takeover() {
        let dir = temp_dir("dead-pid");
        let lock = SidecarLock::at(&dir);
        fs::create_dir_all(lock.path.parent().unwrap()).unwrap();
        // PID 0 is never a live process on either Unix or Windows.
        fs::write(
            &lock.path,
            json!({ "pid": 0u32, "port": 1u16, "started_at_ms": 0 }).to_string(),
        )
        .unwrap();
        let outcome = lock.acquire().unwrap();
        assert_eq!(outcome, LockOutcome::Takeover);
    }
}
