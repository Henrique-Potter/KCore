use kilo_oracle::{
    default_rust_binary_path, BenchCompareConfig, BenchRuntime, BenchScenario, BenchSuite,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_compare_smoke() {
    let rust = default_rust_binary_path();
    if !rust.exists() {
        eprintln!(
            "[bench_compare_smoke] skipping: Rust sidecar binary is missing at {}",
            rust.display()
        );
        return;
    }
    let root = tempfile::tempdir().expect("bench compare tempdir");
    let output = root.path().join("bench.jsonl");
    let cfg = BenchCompareConfig {
        runtimes: vec![BenchRuntime::Rust],
        scenarios: vec![
            BenchScenario::ColdStartReady,
            BenchScenario::TurnTextSmall,
            BenchScenario::TaskSingleChild,
        ],
        trials: 1,
        warmups: 0,
        output: output.clone(),
        rust_binary: rust,
        ..Default::default()
    };
    let report = BenchSuite::new(cfg).run().await.expect("bench compare run");
    assert_eq!(report.trials.len(), 3);
    assert!(
        report.trials.iter().all(|trial| trial.ok),
        "{:#?}",
        report.trials
    );
    assert!(output.exists());
    let md = report.markdown();
    assert!(md.contains("turn_text_small"), "{md}");
    assert!(md.contains("task_single_child"), "{md}");
}
