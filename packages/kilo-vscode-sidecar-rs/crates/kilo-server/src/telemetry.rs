//! Telemetry seam: a trait + structured event types so failure-class
//! emit-points exist in code today and a future opt-in implementation
//! can slot in without rewiring call sites.
//!
//! Per migration plan **Operational invariants → 2**: even without
//! shipping telemetry day-one, the seam exists with a no-op default and
//! one structured emit-point per failure class (sidecar startup error,
//! SSE disconnect-then-reconnect, schema bootstrap, OAuth refresh
//! failure, panic).

use std::sync::Arc;

/// One enum variant per failure class the migration plan calls out as a
/// telemetry-relevant event. Adding a new variant is the same as
/// declaring a new failure class — keep the set small and meaningful.
#[derive(Debug, Clone)]
#[allow(dead_code)] // surfaced incrementally as wiring lands
pub(crate) enum TelemetryEvent {
    /// Sidecar failed to bind/start. `reason` is a human-readable
    /// description; the actual error chain should also be logged via
    /// `tracing::error!` for diagnosis.
    SidecarStartupError { reason: String },
    /// An SSE client reconnected after a disconnect. Frequency of this
    /// event correlates with bus channel saturation.
    SseReconnect { stream: &'static str },
    /// The schema migration runner advanced the on-disk version.
    /// `from_version` and `to_version` make schema-bump tracking easy.
    SchemaBootstrap { from_version: u32, to_version: u32 },
    /// OAuth refresh failed. `provider_id` identifies which credentials
    /// hit a refresh failure.
    OAuthRefreshFailure { provider_id: String, reason: String },
    /// A spawned task panicked. `where_in_code` is a short tag describing
    /// the location (e.g. `"agent::turn"`).
    Panic { where_in_code: String },
}

/// Trait every failure-class emit-point calls. The default implementation
/// is a no-op — the binary builds and ships with no telemetry until an
/// implementation is registered.
pub(crate) trait Telemetry: Send + Sync {
    #[allow(dead_code)] // wired in once first emit-point lands in M11+
    fn emit(&self, event: TelemetryEvent);
}

/// Default no-op implementation. Always installed at the start of
/// `serve()`; replace by calling [`set_telemetry`] from a future opt-in
/// shim if/when telemetry shipping is decided.
#[derive(Default)]
pub(crate) struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    fn emit(&self, _event: TelemetryEvent) {
        // Intentional no-op. The seam exists so call sites don't have to
        // gate on a feature flag; opt-in is a single function call.
    }
}

/// Process-global telemetry handle. Reads of [`telemetry()`] are lock-free
/// after first install. Tests can override via [`set_telemetry`].
static TELEMETRY: std::sync::OnceLock<Arc<dyn Telemetry>> = std::sync::OnceLock::new();

#[allow(dead_code)] // public for future test harnesses
pub(crate) fn set_telemetry(impl_: Arc<dyn Telemetry>) {
    let _ = TELEMETRY.set(impl_);
}

#[allow(dead_code)] // wired in once first emit-point lands in M11+
pub(crate) fn telemetry() -> Arc<dyn Telemetry> {
    TELEMETRY
        .get_or_init(|| Arc::new(NoopTelemetry::default()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct CapturingTelemetry(Arc<Mutex<Vec<TelemetryEvent>>>);

    impl Telemetry for CapturingTelemetry {
        fn emit(&self, event: TelemetryEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    #[test]
    fn noop_telemetry_swallows_events() {
        let t = NoopTelemetry::default();
        t.emit(TelemetryEvent::SidecarStartupError {
            reason: "test".to_string(),
        });
        // No assertion — just verifies the call doesn't panic.
    }

    #[test]
    fn capturing_telemetry_records_each_event() {
        let captured: Arc<Mutex<Vec<TelemetryEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let t = CapturingTelemetry(captured.clone());
        t.emit(TelemetryEvent::SchemaBootstrap {
            from_version: 0,
            to_version: 1,
        });
        t.emit(TelemetryEvent::Panic {
            where_in_code: "test".into(),
        });
        let recorded = captured.lock().unwrap();
        assert_eq!(recorded.len(), 2);
    }
}
