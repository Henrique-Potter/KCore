use clap::{Parser, Subcommand};
use kilo_server::{serve, ServeOptions};

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
