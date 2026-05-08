use clap::{Parser, Subcommand};
use kilo_server::{serve, ServeOptions};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser)]
#[command(author, version, about = "Kilo VS Code Rust sidecar preview")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value_t = 0)]
        port: u16,
        #[arg(long, default_value = "127.0.0.1")]
        hostname: String,
        #[arg(long)]
        host: Option<String>,
        #[arg(long, default_value_t = false)]
        mdns: bool,
        #[arg(long = "mdns-domain")]
        mdns_domain: Option<String>,
        #[arg(long)]
        cors: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Tracing must be initialized before any structured emit. `KILO_LOG`
    // takes precedence over `RUST_LOG` so kilo's logs can be tuned
    // without affecting other crates' diagnostic levels in the host
    // process. Default level is `info` if neither env var is set.
    //
    // Per migration plan **Operational invariants → 1**: structured logs
    // land at `<state_dir>/kilo/log/sidecar.log` with size-based rotation.
    // We layer a non-blocking file appender alongside stderr — stderr
    // continues to feed the extension's diagnostics surface.
    let _log_guard = init_tracing();

    match Cli::parse().command {
        Command::Serve {
            port,
            hostname,
            host,
            mdns: _,
            mdns_domain: _,
            cors: _,
        } => {
            let hostname = host.unwrap_or(hostname);
            serve(ServeOptions { hostname, port }, shutdown()).await?;
        }
    }

    Ok(())
}

/// Initialize the tracing subscriber. Returns a guard that must live as
/// long as the process so the non-blocking file appender flushes on
/// drop. Safe to call once per process; subsequent calls are no-ops
/// because `tracing` uses a single global subscriber.
fn init_tracing() -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let filter = EnvFilter::try_from_env("KILO_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // File destination: <state_dir>/kilo/log/sidecar.log. We resolve the
    // store paths the same way `kilo_server::serve` does so the lock
    // file, the SQLite store, and the log all colocate under one
    // `<state_dir>/kilo/` tree.
    let log_dir = match log_dir() {
        Some(dir) => dir,
        None => {
            // Fall back to stderr-only if we can't resolve a state dir.
            // This keeps the binary usable in CI/headless without a
            // writable HOME.
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().with_writer(std::io::stderr))
                .init();
            return None;
        }
    };
    let _ = std::fs::create_dir_all(&log_dir);

    let file_appender = tracing_appender::rolling::daily(&log_dir, "sidecar.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(std::io::stderr))
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false))
        .init();
    Some(guard)
}

fn log_dir() -> Option<std::path::PathBuf> {
    // `resolve_state_dir` only does env-var lookups + path joins; it does
    // not construct a writer mutex or read `current_dir()`. Cheaper than
    // `Store::new().paths()` which is overkill for a one-shot lookup.
    let state = kilo_store::Store::resolve_state_dir();
    if state.as_os_str().is_empty() {
        return None;
    }
    Some(state.join("kilo").join("log"))
}

async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let ctrl_c = tokio::signal::ctrl_c();
        let term = async {
            let Ok(mut signal) = signal(SignalKind::terminate()) else {
                return;
            };
            signal.recv().await;
        };

        tokio::select! {
            _ = ctrl_c => {},
            _ = term => {},
        }
    }

    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close};

        // Listen for the three signals VS Code's spawn flow can send. Only
        // `Ctrl-C` was wired before, so a `taskkill /T` (which sends
        // CTRL_CLOSE) or a service-stop (CTRL_BREAK) bypassed graceful
        // shutdown — orphaning child tasks and SSE streams. Match the
        // M2 exit gate: "drain/cancel tasks, exit without orphaning
        // children" on Windows too.
        let mut close = match ctrl_close() {
            Ok(handle) => handle,
            Err(err) => {
                eprintln!("[kilo-server] failed to register ctrl_close handler: {err}");
                return;
            }
        };
        let mut brk = match ctrl_break() {
            Ok(handle) => handle,
            Err(err) => {
                eprintln!("[kilo-server] failed to register ctrl_break handler: {err}");
                return;
            }
        };
        let mut c = match ctrl_c() {
            Ok(handle) => handle,
            Err(err) => {
                eprintln!("[kilo-server] failed to register ctrl_c handler: {err}");
                return;
            }
        };

        tokio::select! {
            _ = c.recv() => {},
            _ = brk.recv() => {},
            _ = close.recv() => {},
        }
    }

    // Non-Unix non-Windows fallback (rare). Use the cross-platform
    // ctrl_c entry point and accept that other signals may not arrive.
    #[cfg(not(any(unix, windows)))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
