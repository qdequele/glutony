//! Bounded per-request retry of transient upstream failures.
//!
//! A rate limit (429) or a 5xx (Jev's 529 "overloaded" among them) on one document
//! used to fail the whole `execute`, and the
//! activity retry then re-sent every document from the start, one to two seconds later,
//! well inside the provider's rate-limit window. Here each request gets a few more tries
//! of its own, with exponential backoff and jitter, honouring `Retry-After`; only once
//! that budget is spent does the `Retryable` error reach Temporal.
//!
//! Only transient failures are retried here: 429, 5xx and a dropped connection. A Lab
//! Gateway 401 is retryable too, but only with a fresh key, which the worker fetches
//! between activity attempts, so it is never retried in-plugin with the same key. A
//! request that timed out is not retried either: another full timeout would eat the
//! step's `start_to_close` budget.
//!
//! Waits are bounded (per wait and in total) so they fit inside the step timeout, send
//! a heartbeat before sleeping, and end early on cancellation. Failed attempts record
//! no usage.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use meili_ingest_plugin_sdk::prelude::*;

/// How one attempt failed.
pub(crate) enum Failure {
    /// 429 / 5xx / connection error: worth another try, after `retry_after` when the
    /// server named a delay.
    Transient {
        error: PluginError,
        retry_after: Option<Duration>,
    },
    /// Anything else, surfaced as is (it may still be `Retryable` at activity level).
    Other(PluginError),
}

/// How many times, and how patiently, a transient failure is retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryPolicy {
    /// Total attempts, the first included.
    pub attempts: u32,
    /// First backoff; doubled on every further retry.
    pub base_delay: Duration,
    /// Longest single wait. A `Retry-After` above it is not waited out here: the
    /// error goes to the activity retry instead of hammering the provider early.
    pub max_delay: Duration,
    /// Longest total wait for one request.
    pub max_total_wait: Duration,
}

impl RetryPolicy {
    /// Up to 4 attempts, waiting about 1 s, 2 s, 4 s (at most 20 s each, 30 s in
    /// all): well inside the default 300 s step timeout and the 60 s heartbeat timeout.
    pub(crate) const DEFAULT: Self = Self {
        attempts: 4,
        base_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(20),
        max_total_wait: Duration::from_secs(30),
    };

    /// The same attempts without waiting (tests).
    #[cfg(test)]
    pub(crate) const IMMEDIATE: Self = Self {
        attempts: 4,
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        max_total_wait: Duration::ZERO,
    };

    /// The wait before retry number `retry` (1-based), or `None` when the server asks
    /// for longer than [`Self::max_delay`]. `jitter` in `0.0..=1.0` spreads the
    /// exponential delay over its upper half, so concurrent requests do not retry in
    /// lockstep.
    pub(crate) fn delay(
        &self,
        retry: u32,
        retry_after: Option<Duration>,
        jitter: f64,
    ) -> Option<Duration> {
        if let Some(after) = retry_after {
            return (after <= self.max_delay).then_some(after);
        }
        let exp = self
            .base_delay
            .saturating_mul(2u32.saturating_pow(retry.saturating_sub(1)))
            .min(self.max_delay);
        Some(exp.mul_f64(0.5 + 0.5 * jitter.clamp(0.0, 1.0)))
    }
}

/// Run `attempt` until it succeeds, fails for good, or the policy's budget is spent.
pub(crate) async fn with_retry<T, F, Fut>(
    policy: &RetryPolicy,
    ctx: &ActivityContext,
    what: &str,
    mut attempt: F,
) -> Result<T, PluginError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Failure>>,
{
    let mut waited = Duration::ZERO;
    let mut tries = 1u32;
    loop {
        let (error, retry_after) = match attempt().await {
            Ok(value) => return Ok(value),
            Err(Failure::Other(error)) => return Err(error),
            Err(Failure::Transient { error, retry_after }) => (error, retry_after),
        };
        if tries >= policy.attempts {
            return Err(error);
        }
        let Some(delay) = policy.delay(tries, retry_after, jitter()) else {
            return Err(error);
        };
        if waited + delay > policy.max_total_wait {
            return Err(error);
        }
        waited += delay;
        tracing::debug!(job_id = %ctx.job_id(), what, retry = tries, delay_ms = delay.as_millis() as u64, %error, "transient upstream failure, retrying");
        ctx.heartbeat(format!(
            "{what}: upstream busy, retry {tries} in {} ms",
            delay.as_millis()
        ));
        sleep_unless_cancelled(ctx, delay).await?;
        tries += 1;
    }
}

/// Sleep for `delay`, checking for cancellation at least every 250 ms.
async fn sleep_unless_cancelled(ctx: &ActivityContext, delay: Duration) -> Result<(), PluginError> {
    const SLICE: Duration = Duration::from_millis(250);
    let mut left = delay;
    loop {
        ctx.check_cancelled()?;
        if left.is_zero() {
            return Ok(());
        }
        let step = left.min(SLICE);
        tokio::time::sleep(step).await;
        left -= step;
    }
}

/// A fresh random fraction in `0.0..1.0` (std's per-process hash keys; no extra crate).
fn jitter() -> f64 {
    let mut h = RandomState::new().build_hasher();
    h.write_u8(0);
    (h.finish() % 10_000) as f64 / 10_000.0
}

/// The delay in a `Retry-After` header given in seconds (the HTTP-date form is ignored).
/// A value too large for a `Duration` saturates to `Duration::MAX`: the header is
/// untrusted, and such a delay is simply beyond any cap, so it goes to the activity retry.
pub(crate) fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let secs: f64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    (secs.is_finite() && secs >= 0.0)
        .then(|| Duration::try_from_secs_f64(secs).unwrap_or(Duration::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_grow_exponentially_and_are_capped() {
        let p = RetryPolicy::DEFAULT;
        assert_eq!(p.delay(1, None, 1.0), Some(Duration::from_secs(1)));
        assert_eq!(p.delay(2, None, 1.0), Some(Duration::from_secs(2)));
        assert_eq!(p.delay(3, None, 1.0), Some(Duration::from_secs(4)));
        assert_eq!(p.delay(30, None, 1.0), Some(p.max_delay));
        // Jitter only ever shortens, down to half.
        assert_eq!(p.delay(2, None, 0.0), Some(Duration::from_secs(1)));
    }

    #[test]
    fn retry_after_is_honoured_up_to_the_cap() {
        let p = RetryPolicy::DEFAULT;
        let five = Some(Duration::from_secs(5));
        assert_eq!(p.delay(1, five, 0.0), five);
        assert_eq!(p.delay(1, Some(Duration::from_secs(3600)), 0.0), None);
    }

    #[test]
    fn the_default_budget_fits_the_step_and_heartbeat_timeouts() {
        let p = RetryPolicy::DEFAULT;
        assert!(p.max_delay < Duration::from_secs(60), "heartbeat timeout");
        assert!(p.max_total_wait < Duration::from_secs(300), "step timeout");
    }

    #[test]
    fn retry_after_header_parses_seconds_only() {
        let mut h = reqwest::header::HeaderMap::new();
        assert_eq!(retry_after(&h), None);
        h.insert(reqwest::header::RETRY_AFTER, "7".parse().unwrap());
        assert_eq!(retry_after(&h), Some(Duration::from_secs(7)));
        h.insert(reqwest::header::RETRY_AFTER, "0.5".parse().unwrap());
        assert_eq!(retry_after(&h), Some(Duration::from_millis(500)));
        h.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(retry_after(&h), None);
        // Too big for a `Duration`: saturates (beyond any cap) instead of panicking.
        h.insert(
            reqwest::header::RETRY_AFTER,
            "100000000000000000000".parse().unwrap(),
        );
        assert_eq!(retry_after(&h), Some(Duration::MAX));
        assert_eq!(RetryPolicy::DEFAULT.delay(1, retry_after(&h), 0.5), None);
        h.insert(reqwest::header::RETRY_AFTER, "-1".parse().unwrap());
        assert_eq!(retry_after(&h), None);
    }
}
