//! Sizes and periods of a connection.

use core::time::Duration;

use casty_core::backoff::Backoff;

/// `frame` bounds each frame on the wire and each decompressed frame, `message` bounds an envelope payload, and
/// `window` is the credit each side starts with on every stream. `redial` is the wait before dialing again an address
/// the last dial to failed, which grows on each failure in a row and is spread by `redial_jitter` of itself either
/// way. `mapping` is how long a dial waits for the address map to answer. `read_buffer` is how much a connection reads
/// from its socket at once, and is allocated for every connection.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub frame: usize,
    pub message: usize,
    pub window: usize,
    pub keepalive_after: Duration,
    pub keepalive_timeout: Duration,
    pub handshake: Duration,
    pub dial: Duration,
    pub redial: Backoff,
    pub redial_jitter: f64,
    pub mapping: Duration,
    pub read_buffer: usize,
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
            redial: Backoff {
                first: Duration::from_millis(100),
                limit: Duration::from_secs(15),
                factor: 2.0,
            },
            redial_jitter: 0.25,
            mapping: Duration::from_secs(5),
            read_buffer: 256 * 1024,
        }
    }
}
