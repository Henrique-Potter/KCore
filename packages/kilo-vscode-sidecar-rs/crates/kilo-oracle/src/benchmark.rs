use std::env;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Pass,
    Fail,
    NotMeasured,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GateMetric {
    pub name: String,
    pub status: GateStatus,
    pub value_ms: Option<u128>,
    pub value_bytes: Option<u64>,
    pub threshold_ms: Option<u128>,
    pub threshold_bytes: Option<u64>,
    pub note: Option<String>,
}

impl GateMetric {
    pub fn duration(name: &'static str, value: u128, threshold: u128) -> Self {
        Self {
            name: name.to_string(),
            status: if value <= threshold {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            value_ms: Some(value),
            value_bytes: None,
            threshold_ms: Some(threshold),
            threshold_bytes: None,
            note: None,
        }
    }

    pub fn bytes(name: &'static str, value: u64, threshold: u64) -> Self {
        Self {
            name: name.to_string(),
            status: if value <= threshold {
                GateStatus::Pass
            } else {
                GateStatus::Fail
            },
            value_ms: None,
            value_bytes: Some(value),
            threshold_ms: None,
            threshold_bytes: Some(threshold),
            note: None,
        }
    }

    pub fn not_measured(name: &'static str, note: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            status: GateStatus::NotMeasured,
            value_ms: None,
            value_bytes: None,
            threshold_ms: None,
            threshold_bytes: None,
            note: Some(note.into()),
        }
    }

    pub fn unsupported(name: &'static str, note: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            status: GateStatus::Unsupported,
            value_ms: None,
            value_bytes: None,
            threshold_ms: None,
            threshold_bytes: None,
            note: Some(note.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkThresholds {
    pub cold_start_ms: u128,
    pub first_token_ms: u128,
    pub shutdown_ms: u128,
    pub shutdown_hard_ms: u128,
    pub package_size_bytes: Option<u64>,
    /// Optional RSS gates. Both default to `None` (sample-and-report only)
    /// because portable RSS thresholds are workload-dependent. CI sets
    /// `KILO_M13_IDLE_RSS_BYTES` / `KILO_M13_ACTIVE_RSS_BYTES` to enable
    /// gating.
    pub idle_rss_bytes: Option<u64>,
    pub active_rss_bytes: Option<u64>,
    /// Idle CPU percentage threshold. Sampled with the SSE bus connected
    /// but no work in flight; the plan target is "<5% (preview)". `None`
    /// means sample-and-report only. Set via `KILO_M13_IDLE_CPU_PCT`.
    pub idle_cpu_pct: Option<u32>,
}

impl Default for BenchmarkThresholds {
    fn default() -> Self {
        Self {
            cold_start_ms: 5_000,
            first_token_ms: 5_000,
            shutdown_ms: 1_000,
            shutdown_hard_ms: 5_000,
            package_size_bytes: None,
            idle_rss_bytes: None,
            active_rss_bytes: None,
            idle_cpu_pct: None,
        }
    }
}

impl BenchmarkThresholds {
    pub fn from_env() -> Self {
        let base = Self::default();
        Self {
            cold_start_ms: env_ms("KILO_M13_COLD_START_MS", base.cold_start_ms),
            first_token_ms: env_ms("KILO_M13_FIRST_TOKEN_MS", base.first_token_ms),
            shutdown_ms: env_ms("KILO_M13_SHUTDOWN_MS", base.shutdown_ms),
            shutdown_hard_ms: env_ms("KILO_M13_SHUTDOWN_HARD_MS", base.shutdown_hard_ms),
            package_size_bytes: env_u64("KILO_M13_PACKAGE_SIZE_BYTES"),
            idle_rss_bytes: env_u64("KILO_M13_IDLE_RSS_BYTES"),
            active_rss_bytes: env_u64("KILO_M13_ACTIVE_RSS_BYTES"),
            idle_cpu_pct: env_u64("KILO_M13_IDLE_CPU_PCT").and_then(|n| u32::try_from(n).ok()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkGate {
    pub thresholds: BenchmarkThresholds,
    pub metrics: Vec<GateMetric>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkReport {
    pub milestone: &'static str,
    pub passed: bool,
    pub gate: BenchmarkGate,
}

impl BenchmarkReport {
    pub fn new(thresholds: BenchmarkThresholds, metrics: Vec<GateMetric>) -> Self {
        let passed = metrics
            .iter()
            .all(|metric| metric.status != GateStatus::Fail);
        Self {
            milestone: "M13 benchmark gates",
            passed,
            gate: BenchmarkGate {
                thresholds,
                metrics,
            },
        }
    }
}

fn env_ms(key: &str, fallback: u128) -> u128 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(fallback)
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_metric_fails_above_threshold() {
        let metric = GateMetric::duration("shutdown", 5_001, 5_000);
        assert_eq!(metric.status, GateStatus::Fail);
    }

    #[test]
    fn unsupported_metrics_do_not_fail_report() {
        let report = BenchmarkReport::new(
            BenchmarkThresholds::default(),
            vec![GateMetric::unsupported("idle_rss", "platform-specific")],
        );
        assert!(report.passed);
    }
}
