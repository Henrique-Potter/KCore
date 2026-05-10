use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kilo_provider::ProviderError;
use serde_json::{json, Value};

const INITIAL_MS: u64 = 2_000;
const FACTOR: u64 = 2;
const MAX_NO_HEADER_MS: u64 = 30_000;
const MAX_MS: u64 = i32::MAX as u64;

#[derive(Clone, Debug)]
pub(crate) struct RetryPolicy {
    limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct RetryState {
    policy: RetryPolicy,
    attempt: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetryWait {
    pub(crate) attempt: usize,
    pub(crate) message: String,
    pub(crate) delay: Duration,
    pub(crate) next: u64,
}

impl RetryPolicy {
    pub(crate) fn from_env() -> Self {
        Self {
            limit: retry_limit(),
        }
    }
}

impl RetryState {
    pub(crate) fn new(policy: RetryPolicy) -> Self {
        Self { policy, attempt: 0 }
    }

    pub(crate) fn next(&mut self, err: &ProviderError) -> Option<RetryWait> {
        if !err.is_retryable() {
            return None;
        }
        let next = self.attempt + 1;
        if self.policy.limit.is_some_and(|limit| next > limit) {
            return None;
        }
        self.attempt = next;
        let delay = delay_for(next, err);
        Some(RetryWait {
            attempt: next,
            message: retry_message(err),
            delay,
            next: now_ms().saturating_add(delay.as_millis() as u64),
        })
    }
}

pub(crate) fn retry_status(wait: &RetryWait) -> Value {
    json!({
        "type": "retry",
        "attempt": wait.attempt,
        "message": wait.message,
        "next": wait.next,
    })
}

pub(crate) async fn sleep_or_cancel(cancel: &AtomicBool, delay: Duration) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        if cancel.load(Ordering::SeqCst) {
            return true;
        }
        tokio::select! {
            _ = &mut sleep => return cancel.load(Ordering::SeqCst),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}

fn retry_limit() -> Option<usize> {
    std::env::var("KILO_SESSION_RETRY_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn delay_for(attempt: usize, err: &ProviderError) -> Duration {
    let ms = err.retry_after_ms().unwrap_or_else(|| {
        let exp = INITIAL_MS.saturating_mul(FACTOR.saturating_pow((attempt - 1) as u32));
        exp.min(MAX_NO_HEADER_MS)
    });
    Duration::from_millis(ms.min(MAX_MS))
}

fn retry_message(err: &ProviderError) -> String {
    let text = err.to_string();
    if text.contains("Overloaded") || text.contains("overloaded") {
        return "Provider is overloaded".to_string();
    }
    text
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kilo_provider::ApiStatusError;

    #[test]
    fn retry_state_uses_retry_after_and_limit() {
        let mut state = RetryState::new(RetryPolicy { limit: Some(1) });
        let err = ProviderError::ApiStatus(ApiStatusError {
            status: 429,
            message: "rate limited".to_string(),
            body: "{}".to_string(),
            retry_after_ms: Some(123),
            retryable: true,
        });

        let wait = state.next(&err).expect("first retry");
        assert_eq!(wait.attempt, 1);
        assert_eq!(wait.delay, Duration::from_millis(123));
        assert!(state.next(&err).is_none());
    }

    #[test]
    fn retry_state_skips_non_retryable_errors() {
        let mut state = RetryState::new(RetryPolicy { limit: None });
        assert!(state
            .next(&ProviderError::Api("bad request".to_string()))
            .is_none());
    }
}
