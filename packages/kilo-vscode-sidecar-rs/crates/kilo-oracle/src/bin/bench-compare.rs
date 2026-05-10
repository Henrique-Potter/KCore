use std::path::PathBuf;

use kilo_oracle::{
    default_binary_path, default_rust_binary_path, BenchCompareConfig, BenchRuntime, BenchScenario,
    BenchSuite, OracleError, OracleResult,
};

#[tokio::main]
async fn main() -> OracleResult<()> {
    let cfg = parse_args()?;
    let report = BenchSuite::new(cfg.clone()).run().await?;
    let md = cfg.output.with_extension("md");
    report.write_markdown(&md)?;
    println!("{}", report.markdown());
    println!("wrote raw JSONL: {}", report.output.display());
    println!("wrote summary: {}", md.display());
    Ok(())
}

fn parse_args() -> OracleResult<BenchCompareConfig> {
    let mut cfg = BenchCompareConfig::default();
    cfg.bun_binary = std::env::var_os("KILO_BENCH_BUN_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(default_binary_path);
    cfg.rust_binary = std::env::var_os("KILO_BENCH_RUST_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(default_rust_binary_path);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--runtime" => {
                let value = next(&mut args, "--runtime")?;
                cfg.runtimes = BenchRuntime::parse(&value)?;
            }
            "--scenario" => {
                let value = next(&mut args, "--scenario")?;
                cfg.scenarios = BenchScenario::parse(&value)?;
            }
            "--trials" => {
                let value = next(&mut args, "--trials")?;
                cfg.trials = parse_usize("--trials", &value)?;
            }
            "--warmups" => {
                let value = next(&mut args, "--warmups")?;
                cfg.warmups = parse_usize("--warmups", &value)?;
            }
            "--workspace-seed" => {
                cfg.workspace_seed = next(&mut args, "--workspace-seed")?;
            }
            "--output" => {
                cfg.output = PathBuf::from(next(&mut args, "--output")?);
            }
            "--bun-binary" => {
                cfg.bun_binary = PathBuf::from(next(&mut args, "--bun-binary")?);
            }
            "--rust-binary" => {
                cfg.rust_binary = PathBuf::from(next(&mut args, "--rust-binary")?);
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                return Err(OracleError::other(format!(
                    "unknown argument {other:?}; run with --help"
                )));
            }
        }
    }
    Ok(cfg)
}

fn next(args: &mut impl Iterator<Item = String>, flag: &str) -> OracleResult<String> {
    args.next()
        .ok_or_else(|| OracleError::other(format!("{flag} requires a value")))
}

fn parse_usize(flag: &str, value: &str) -> OracleResult<usize> {
    value
        .parse()
        .map_err(|err| OracleError::other(format!("{flag} must be a number: {err}")))
}

fn print_help() {
    println!(
        "bench-compare\n\
\n\
Options:\n\
  --runtime bun|rust|both          Runtime selection (default: both)\n\
  --scenario smoke|all|<name>      Scenario selection (default: smoke)\n\
  --trials N                       Measured trials per runtime/scenario (default: 30)\n\
  --warmups N                      Warmup trials per runtime/scenario (default: 3)\n\
  --workspace-seed small|medium    Workspace seed size (default: small)\n\
  --output PATH                    Raw JSONL destination\n\
  --bun-binary PATH                Bun sidecar binary override\n\
  --rust-binary PATH               Rust sidecar binary override\n"
    );
}
