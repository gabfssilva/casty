//! Sizes and periods of a connection.

use core::time::Duration;

/// `frame` bounds each frame on the wire and each decompressed frame, `message` bounds an envelope payload, and
/// `window` is the credit each side starts with on every stream.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub frame: usize,
    pub message: usize,
    pub window: usize,
    pub keepalive_after: Duration,
    pub keepalive_timeout: Duration,
    pub handshake: Duration,
    pub dial: Duration,
    pub backoff_first: Duration,
    pub backoff_limit: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            frame: 256 * 1024,
            message: 4 * 1024 * 1024,
            window: 256 * 1024,
            keepalive_after: Duration::from_secs(15),
            keepalive_timeout: Duration::from_secs(5),
            handshake: Duration::from_secs(5),
            dial: Duration::from_secs(5),
            backoff_first: Duration::from_millis(100),
            backoff_limit: Duration::from_secs(15),
        }
    }
}
