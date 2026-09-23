//! How long to wait before trying again.

use std::time::Duration;

/// Delay before trying again: `first`, multiplied by `factor` on each consecutive failure, up to `limit`.
///
/// A body that raised waits this long before it runs again, and so does a node that is owed an answer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    pub first: Duration,
    pub limit: Duration,
    pub factor: f64,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            first: Duration::from_millis(100),
            limit: Duration::from_secs(10),
            factor: 2.0,
        }
    }
}

impl Backoff {
    /// The delay after `held`, which never grows past the limit.
    ///
    /// A product no `Duration` holds, which a factor of `inf` or of `1e300` makes, is past the limit too.
    #[must_use]
    pub fn next(&self, held: Duration) -> Duration {
        Duration::try_from_secs_f64(held.as_secs_f64() * self.factor)
            .map_or(self.limit, |grown| grown.min(self.limit))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Backoff;

    #[test]
    fn a_backoff_stops_at_its_limit_whatever_the_factor() {
        let second = Duration::from_secs(1);
        for factor in [2.0, 1e300, f64::INFINITY, f64::NAN] {
            let backoff = Backoff {
                first: second,
                limit: Duration::from_secs(10),
                factor,
            };
            let held = backoff.next(backoff.next(second));
            assert!(held <= backoff.limit, "{factor}: {held:?}");
        }
        assert_eq!(Backoff::default().next(second), Duration::from_secs(2));
    }
}
