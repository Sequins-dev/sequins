//! Wall-clock time provider — injectable for deterministic testing.
//!
//! All code that needs "what time is it now?" should accept a `&dyn NowTime`
//! or `Arc<dyn NowTime>` rather than calling `SystemTime::now()` directly.
//!
//! In production, pass `Arc::new(SystemNowTime)`.
//!
//! In tests, pass `Arc::new(MockNowTime::new(base_ns))` and control time
//! with `tokio::time::advance()`.  Because `MockNowTime` reads from
//! `tokio::time::Instant` internally, a single `advance()` call simultaneously
//! moves heartbeat timers, flush intervals, AND the mock wall-clock epoch.

use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// NowTime trait
// ---------------------------------------------------------------------------

/// Provides the current wall-clock epoch nanoseconds.
///
/// Production code uses [`SystemNowTime`].  Tests use [`MockNowTime`] to
/// make wall-clock-dependent logic fully deterministic.
pub trait NowTime: Send + Sync + 'static {
    /// Current time as nanoseconds since UNIX epoch.
    fn now_ns(&self) -> u64;
}

// ---------------------------------------------------------------------------
// SystemNowTime — production implementation
// ---------------------------------------------------------------------------

/// Production time provider delegating to [`SystemTime::now`].
pub struct SystemNowTime;

impl NowTime for SystemNowTime {
    fn now_ns(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// MockNowTime — deterministic test implementation
// ---------------------------------------------------------------------------

/// Deterministic time provider for tests, backed by tokio's monotonic clock.
///
/// `MockNowTime` stores a base epoch offset and a `tokio::time::Instant`
/// captured at construction.  `now_ns()` returns:
/// ```text
///     base_ns + (Instant::now() - start).as_nanos()
/// ```
///
/// In `#[tokio::test(start_paused = true)]`, calling `tokio::time::advance()`
/// advances **both** tokio timers/intervals **and** `MockNowTime` uniformly:
/// a single `advance(Duration::from_secs(60))` moves heartbeat timers, flush
/// intervals, and the wall-clock epoch forward by exactly 60 s.
///
/// # Construction
///
/// Must be called inside a tokio runtime (e.g. inside `#[tokio::test]`).
///
/// ```ignore
/// let clock = Arc::new(MockNowTime::new(1_700_000_000_000_000_000));
/// ```
pub struct MockNowTime {
    /// Epoch nanosecond offset — the "wall clock" value at construction time.
    base_ns: u64,
    /// Tokio monotonic instant captured at construction.
    start: tokio::time::Instant,
}

impl MockNowTime {
    /// Create a mock time provider starting at `base_ns` epoch nanoseconds.
    ///
    /// Must be called inside a tokio runtime.
    pub fn new(base_ns: u64) -> Self {
        Self {
            base_ns,
            start: tokio::time::Instant::now(),
        }
    }

    /// Current epoch nanoseconds (same as `now_ns()`, no side effects).
    pub fn current(&self) -> u64 {
        let elapsed = tokio::time::Instant::now() - self.start;
        self.base_ns + elapsed.as_nanos() as u64
    }
}

impl NowTime for MockNowTime {
    fn now_ns(&self) -> u64 {
        self.current()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn system_now_time_returns_nonzero() {
        let clock = SystemNowTime;
        assert!(
            clock.now_ns() > 0,
            "SystemNowTime should return non-zero epoch"
        );
    }

    #[test]
    fn system_now_time_is_monotonically_increasing() {
        let clock = SystemNowTime;
        let t1 = clock.now_ns();
        let t2 = clock.now_ns();
        assert!(t2 >= t1, "two consecutive calls should be non-decreasing");
    }

    #[tokio::test(start_paused = true)]
    async fn mock_now_time_starts_at_base() {
        let base_ns = 1_700_000_000_000_000_000u64;
        let clock = MockNowTime::new(base_ns);
        // At construction, Instant::now() - start == 0, so current() == base_ns.
        assert_eq!(clock.now_ns(), base_ns);
    }

    #[tokio::test(start_paused = true)]
    async fn mock_now_time_advances_with_tokio_time() {
        let base_ns = 1_700_000_000_000_000_000u64;
        let clock = MockNowTime::new(base_ns);

        let before = clock.now_ns();
        tokio::time::advance(std::time::Duration::from_secs(60)).await;
        let after = clock.now_ns();

        let delta_ns = after - before;
        // Should have advanced by ~60 seconds.
        assert!(
            (59_000_000_000..=61_000_000_000).contains(&delta_ns),
            "MockNowTime should advance by ~60s when tokio time is advanced by 60s, got {}ns",
            delta_ns
        );
    }

    #[tokio::test(start_paused = true)]
    async fn mock_now_time_implements_now_time_trait() {
        let base_ns = 1_700_000_000_000_000_000u64;
        let clock: Arc<dyn NowTime> = Arc::new(MockNowTime::new(base_ns));
        assert_eq!(clock.now_ns(), base_ns);
    }

    #[tokio::test(start_paused = true)]
    async fn mock_now_time_current_equals_now_ns() {
        let base_ns = 1_000_000_000_000u64;
        let clock = MockNowTime::new(base_ns);
        assert_eq!(clock.current(), clock.now_ns());
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert_eq!(clock.current(), clock.now_ns());
    }
}
