//! How fast a connection sends and how much it keeps in flight, without I/O: about what the link carries.
//!
//! Bytes beyond what the link carries wait in the buffers along the path (the kernel's, a relay's), first in first
//! out, and whatever is written after them waits for them: a heartbeat behind sixteen megabytes on a link that carries
//! two a second arrives eight seconds late. The credit a peer returns is the acknowledgement. From it come the round
//! trip, the shortest of the last seconds, and the rate bytes are delivered at, the highest of the last rounds, as in
//! BBR. The sender is paced at that rate times a gain, and the bytes in flight are held under one and a half times
//! what the link holds in a round trip, which absorbs late acknowledgements and bounds what a rate measured too high
//! can queue. The gain starts at one and a half, growing the rate by half each round until it stops growing or the
//! round trip grows with a queue; it then drains what that queued and cycles: a round above one to find more room, a
//! round below to drain what that queued, and rounds at one. When no round trip near the shortest has been measured
//! for a while, the connection holds almost nothing in flight for a round to measure one again. Time comes in as
//! monotonic seconds.

use std::collections::VecDeque;

/// The window before the first measures, and the least it ever is: a link this fast in one round trip queues nothing.
pub const FLOOR: usize = 256 * 1024;
/// The window over what the link holds in a round trip: room for acknowledgements that arrive late, and the most a
/// rate measured too high can queue.
const WINDOW_GAIN: f64 = 1.5;
/// Seconds of the paced rate a sender may send at once after waiting, and the least it may, in bytes.
const BURST: f64 = 0.002;
const BURST_AT_LEAST: f64 = 32.0 * 1024.0;
/// Rounds the delivery rate is remembered for.
const RATE_ROUNDS: u64 = 10;
/// Seconds the shortest round trip is taken over. Each cycle drains the queue for a round, so this window always
/// holds a round trip measured with nothing queued.
const TRIPS_FOR: f64 = 10.0;
const STARTUP: f64 = 1.5;
const DRAIN: f64 = 0.5;
/// A round whose shortest trip is this much longer than the shortest one ends the startup: the rate has outgrown the
/// link and fills a queue. `QUEUED_AT_LEAST` keeps the jitter of a short link from reading as one.
const QUEUED: f64 = 0.25;
const QUEUED_AT_LEAST: f64 = 0.005;
/// Rounds without the rate growing by a quarter after which the startup ends.
const FLAT_ROUNDS: u32 = 3;
const CYCLE: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
/// Seconds without a round trip near the shortest after which the connection drains what it has in flight to measure
/// one: a queue that never empties would otherwise pass for the length of the link. Shorter than `TRIPS_FOR`, so that
/// the shortest is measured again before it expires.
const PROBE_AFTER: f64 = 5.0;
/// A round trip within this much of the shortest counts as measured with nothing queued.
const NEAR: f64 = 0.05;
/// What the window holds while the connection drains to measure the round trip, and the least time that lasts.
const PROBING: usize = 4 * 16 * 1024;
const PROBE_FOR: f64 = 0.2;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    Startup {
        best: f64,
        flat: u32,
    },
    /// What the startup queued drains before the cycle starts.
    Drain,
    Cycle {
        step: usize,
    },
    /// In flight held to `PROBING` until `until` and a round after it, to measure the round trip with nothing queued.
    Probe {
        until: f64,
        round: Option<u64>,
    },
}

/// A frame in flight. `delivered` and `delivered_at` are the delivery count and its time when it went out, and
/// `first_at` the send time of the frame acknowledged last by then: the rate it measures is over the longer of the
/// two intervals, so that acknowledgements that arrive bunched do not read as a faster link.
#[derive(Debug, Clone, Copy)]
struct Sent {
    end: u64,
    at: f64,
    delivered: u64,
    delivered_at: f64,
    first_at: f64,
    limited: bool,
}

/// What was sent on one stream, which the credit of that stream acknowledges in order.
#[derive(Debug, Default)]
struct Track {
    sent: u64,
    acked: u64,
    frames: VecDeque<Sent>,
}

#[derive(Debug)]
pub struct Congestion {
    tracks: Vec<Track>,
    in_flight: usize,
    delivered: u64,
    delivered_at: f64,
    acked_sent_at: f64,
    /// While nonzero, the sender ran out of bytes before the window did, and what it sends cannot show the rate of
    /// the link; it ends once the bytes in flight then are delivered.
    limited_until: u64,
    round: u64,
    round_ends: u64,
    /// The shortest trip measured in this round.
    round_trip: f64,
    /// `(round, rate)`, rates decreasing: the first is the highest of the last rounds.
    rates: VecDeque<(u64, f64)>,
    /// `(at, trip)`, trips increasing: the first is the shortest of the last `TRIPS_FOR` seconds.
    trips: VecDeque<(f64, f64)>,
    /// When a round trip near the shortest was last measured.
    fresh_at: f64,
    phase: Phase,
    /// Bytes the pace allows now, as of `paced_at`.
    tokens: f64,
    paced_at: f64,
}

impl Congestion {
    /// The window of a connection whose credit flows on `streams` streams.
    #[must_use]
    pub fn new(streams: usize) -> Self {
        Self {
            tracks: (0..streams).map(|_| Track::default()).collect(),
            in_flight: 0,
            delivered: 0,
            delivered_at: 0.0,
            acked_sent_at: 0.0,
            limited_until: 0,
            round: 0,
            round_ends: 0,
            round_trip: f64::INFINITY,
            rates: VecDeque::new(),
            trips: VecDeque::new(),
            fresh_at: 0.0,
            phase: Phase::Startup { best: 0.0, flat: 0 },
            tokens: 0.0,
            paced_at: 0.0,
        }
    }

    /// Bytes the window has room for now, whatever the pace.
    #[must_use]
    pub fn room(&self) -> usize {
        self.window().saturating_sub(self.in_flight)
    }

    /// The most the pace allows sending at `now`, whatever the window.
    pub fn paced(&mut self, now: f64) -> usize {
        let Some(pace) = self.pace() else {
            return usize::MAX;
        };
        let burst = (pace * BURST).max(BURST_AT_LEAST);
        self.tokens = (self.tokens + pace * (now - self.paced_at).max(0.0)).min(burst);
        self.paced_at = now;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let paced = self.tokens.max(0.0) as usize;
        paced
    }

    /// The most that may be sent at `now`: the room in the window, and what the pace allows.
    pub fn allowance(&mut self, now: f64) -> usize {
        self.paced(now).min(self.room())
    }

    /// When the pace will allow `bytes`, if it is the pace that holds them back: also the window, if `windowed`.
    #[must_use]
    pub fn ready_at(&self, bytes: usize, windowed: bool) -> Option<f64> {
        if windowed && self.window() <= self.in_flight {
            return None;
        }
        let pace = self.pace()?;
        #[allow(clippy::cast_precision_loss)]
        let short = bytes as f64 - self.tokens;
        (short > 0.0).then(|| self.paced_at + short / pace)
    }

    #[must_use]
    pub fn window(&self) -> usize {
        // The startup paces above the link on purpose, and what the window allows past the link is what it queues.
        let gain = match self.phase {
            Phase::Startup { .. } => STARTUP,
            Phase::Drain | Phase::Cycle { .. } => WINDOW_GAIN,
            Phase::Probe { .. } => return PROBING,
        };
        self.held().map_or(FLOOR, |held| {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let window = (gain * held) as usize;
            window.max(FLOOR)
        })
    }

    pub fn sent(&mut self, stream: usize, bytes: usize, now: f64) {
        if self.in_flight == 0 {
            self.delivered_at = now;
            self.acked_sent_at = now;
        }
        #[allow(clippy::cast_precision_loss)]
        let spent = bytes as f64;
        self.tokens -= spent;
        let track = &mut self.tracks[stream];
        track.sent += bytes as u64;
        track.frames.push_back(Sent {
            end: track.sent,
            at: now,
            delivered: self.delivered,
            delivered_at: self.delivered_at,
            first_at: self.acked_sent_at,
            limited: self.limited_until > 0,
        });
        self.in_flight += bytes;
    }

    /// The peer returned `bytes` of credit on `stream`.
    pub fn acked(&mut self, stream: usize, bytes: usize, now: f64) {
        self.delivered += bytes as u64;
        self.delivered_at = now;
        self.in_flight = self.in_flight.saturating_sub(bytes);
        let track = &mut self.tracks[stream];
        track.acked += bytes as u64;
        let mut last = None;
        while track
            .frames
            .front()
            .is_some_and(|sent| sent.end <= track.acked)
        {
            last = track.frames.pop_front();
        }
        if self.limited_until > 0 && self.delivered > self.limited_until {
            self.limited_until = 0;
        }
        if let Some(sent) = last {
            self.sample(sent, now);
        }
        #[allow(clippy::cast_precision_loss)]
        let in_flight = self.in_flight as f64;
        if self.phase == Phase::Drain && self.held().is_some_and(|held| in_flight <= held) {
            self.phase = Phase::Cycle { step: 0 };
        }
    }

    /// A new connection takes up `stream` with `sent` bytes sent on it and `acked` of them credited: what was in flight
    /// on the last one is no longer, and only what the peer has not credited of what it received is. The round trip
    /// and the rate measured stay, which is what spares a resumed connection the startup.
    pub fn resume(&mut self, stream: usize, sent: u64, acked: u64) {
        let track = &mut self.tracks[stream];
        track.frames.clear();
        track.sent = sent;
        track.acked = acked;
        self.in_flight = self
            .tracks
            .iter()
            .map(|track| {
                usize::try_from(track.sent - track.acked)
                    .expect("in flight is bounded by the window")
            })
            .sum();
    }

    /// The sender has nothing more to send while the window has room.
    pub fn idle(&mut self) {
        self.limited_until = (self.delivered + self.in_flight as u64).max(1);
    }

    /// Bytes the link holds in one round trip, once there is a measure of it.
    fn held(&self) -> Option<f64> {
        let &(_, shortest) = self.trips.front()?;
        let &(_, rate) = self.rates.front()?;
        Some(rate * shortest)
    }

    /// Bytes a second to send at, once there is a measure of the rate: never less than the floor of the window in a
    /// round trip, which a rate measured while there was little to send would otherwise hold back.
    fn pace(&self) -> Option<f64> {
        let &(_, rate) = self.rates.front()?;
        let &(_, shortest) = self.trips.front()?;
        #[allow(clippy::cast_precision_loss)]
        let rate = rate.max(FLOOR as f64 / shortest);
        let gain = match self.phase {
            Phase::Startup { .. } => STARTUP,
            Phase::Drain => DRAIN,
            Phase::Cycle { step } => CYCLE[step],
            Phase::Probe { .. } => 1.0,
        };
        Some(gain * rate)
    }

    fn sample(&mut self, sent: Sent, now: f64) {
        let trip = now - sent.at;
        while self.trips.back().is_some_and(|&(_, held)| held >= trip) {
            self.trips.pop_back();
        }
        self.trips.push_back((now, trip));
        while self
            .trips
            .front()
            .is_some_and(|&(at, _)| now - at > TRIPS_FOR)
        {
            self.trips.pop_front();
        }
        self.round_trip = self.round_trip.min(trip);
        if trip <= self.trips.front().map_or(trip, |&(_, shortest)| shortest) * (1.0 + NEAR) {
            self.fresh_at = now;
        }
        match self.phase {
            Phase::Cycle { .. } if now - self.fresh_at > PROBE_AFTER => {
                self.phase = Phase::Probe {
                    until: now
                        + PROBE_FOR.max(self.trips.front().map_or(0.0, |&(_, shortest)| shortest)),
                    round: None,
                };
            }
            Phase::Probe { until, round: None } if now >= until => {
                self.phase = Phase::Probe {
                    until,
                    round: Some(self.round),
                };
            }
            _ => {}
        }
        self.acked_sent_at = sent.at;
        if sent.delivered >= self.round_ends {
            self.round += 1;
            self.round_ends = self.delivered;
            self.next_round(sent.limited);
        }
        let interval = (now - sent.delivered_at).max(sent.at - sent.first_at);
        if interval <= 0.0 {
            return;
        }
        #[allow(clippy::cast_precision_loss)]
        let rate = (self.delivered - sent.delivered) as f64 / interval;
        let highest = self.rates.front().map_or(0.0, |&(_, rate)| rate);
        if sent.limited && rate < highest {
            return;
        }
        while self.rates.back().is_some_and(|&(_, held)| held <= rate) {
            self.rates.pop_back();
        }
        self.rates.push_back((self.round, rate));
        while self
            .rates
            .front()
            .is_some_and(|&(round, _)| round + RATE_ROUNDS < self.round)
        {
            self.rates.pop_front();
        }
    }

    /// `limited`: the round ended on a frame sent with less to send than the window allowed, which shows nothing about
    /// whether the rate has stopped growing.
    fn next_round(&mut self, limited: bool) {
        let rate = self.rates.front().map_or(0.0, |&(_, rate)| rate);
        let shortest = self.trips.front().map_or(0.0, |&(_, trip)| trip);
        let queued = self.round_trip - shortest > (shortest * QUEUED).max(QUEUED_AT_LEAST);
        self.round_trip = f64::INFINITY;
        self.phase = match self.phase {
            Phase::Startup { .. } if limited => self.phase,
            Phase::Startup { .. } if queued => Phase::Drain,
            Phase::Startup { best, .. } if rate >= best * 1.25 => Phase::Startup {
                best: rate,
                flat: 0,
            },
            Phase::Startup { flat, .. } if flat + 1 >= FLAT_ROUNDS => Phase::Drain,
            Phase::Startup { best, flat } => Phase::Startup {
                best,
                flat: flat + 1,
            },
            Phase::Drain => Phase::Drain,
            Phase::Cycle { step } => Phase::Cycle {
                step: (step + 1) % CYCLE.len(),
            },
            Phase::Probe {
                round: Some(round), ..
            } if self.round > round => {
                self.fresh_at = self.trips.back().map_or(self.fresh_at, |&(at, _)| at);
                Phase::Cycle { step: 2 }
            }
            Phase::Probe { .. } => self.phase,
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::{Congestion, FLOOR, Phase};

    const FRAME: usize = 16 * 1024;

    /// A link of `rate` bytes a second and `trip` seconds of round trip, with a buffer deep enough to hold anything.
    struct Link {
        rate: f64,
        trip: f64,
        /// When the last byte queued so far leaves the bottleneck.
        free: f64,
        /// When each acknowledgement arrives, with its bytes.
        acks: VecDeque<(f64, usize)>,
    }

    impl Link {
        fn new(rate: f64, trip: f64) -> Self {
            Self {
                rate,
                trip,
                free: 0.0,
                acks: VecDeque::new(),
            }
        }

        /// Send `bytes` at `now`, giving back how long they wait in the queue.
        fn send(&mut self, congestion: &mut Congestion, bytes: usize, now: f64) -> f64 {
            let start = self.free.max(now);
            #[allow(clippy::cast_precision_loss)]
            let leaves = start + bytes as f64 / self.rate;
            self.free = leaves;
            self.acks.push_back((leaves + self.trip, bytes));
            congestion.sent(0, bytes, now);
            start - now
        }

        /// Deliver the next acknowledgement, giving back its time.
        fn ack(&mut self, congestion: &mut Congestion) -> f64 {
            let (at, bytes) = self.acks.pop_front().expect("bytes in flight");
            congestion.acked(0, bytes, at);
            at
        }

        /// Send what the window and the pace allow at `now`, then move on to the next acknowledgement or the moment
        /// the pace allows a frame, whichever is first: the time it moved to, and the longest a frame sent waited.
        fn step(&mut self, congestion: &mut Congestion, now: f64) -> (f64, f64) {
            let mut queued = 0.0_f64;
            while congestion.allowance(now) >= FRAME {
                queued = queued.max(self.send(congestion, FRAME, now));
            }
            let acked = self.acks.front().map(|&(at, _)| at);
            match (acked, congestion.ready_at(FRAME, true)) {
                // A hair past the moment, which the sum of the tokens may miss by a rounding.
                (Some(acked), Some(ready)) if ready < acked => (ready + 1e-9, queued),
                (Some(_), _) => (self.ack(congestion), queued),
                (None, Some(ready)) => (ready + 1e-9, queued),
                (None, None) => panic!("nothing in flight and nothing allowed"),
            }
        }
    }

    /// A sender that always has more to send, over `seconds`: the rate delivered after the first third, and the
    /// longest time a byte spent queued then.
    fn busy(congestion: &mut Congestion, link: &mut Link, seconds: f64) -> (f64, f64) {
        let mut now = 0.0;
        let mut queued = 0.0_f64;
        let mut delivered = None;
        while now < seconds {
            let (next, waited) = link.step(congestion, now);
            if now > seconds / 3.0 {
                queued = queued.max(waited);
                delivered.get_or_insert(congestion.delivered);
            }
            now = next;
        }
        #[allow(clippy::cast_precision_loss)]
        let rate =
            (congestion.delivered - delivered.unwrap_or_default()) as f64 / (now - seconds / 3.0);
        (rate, queued)
    }

    #[test]
    fn a_busy_sender_fills_the_link_and_queues_little() {
        for (rate, trip) in [(2e6, 0.3), (10e6, 0.3), (10e6, 0.05), (100e6, 0.3)] {
            let (delivered, queued) =
                busy(&mut Congestion::new(1), &mut Link::new(rate, trip), 60.0);
            assert!(
                delivered >= rate * 0.9,
                "{rate} B/s over {trip} s: {delivered} B/s delivered"
            );
            assert!(
                queued <= trip * 0.35,
                "{rate} B/s over {trip} s: a byte waited {queued} s in the queue"
            );
        }
    }

    #[test]
    fn a_link_that_grows_longer_is_measured_again() {
        let mut congestion = Congestion::new(1);
        let mut link = Link::new(2e6, 0.3);
        let mut now = 0.0;
        while now < 30.0 {
            now = link.step(&mut congestion, now).0;
        }
        link.trip = 0.45;
        let mut probed = false;
        while now < 36.0 {
            now = link.step(&mut congestion, now).0;
            probed |= matches!(congestion.phase, Phase::Probe { .. });
        }
        assert!(
            probed,
            "no round trip near the shortest for six seconds, and no probe"
        );
        while now < 45.0 {
            now = link.step(&mut congestion, now).0;
        }
        let &(_, shortest) = congestion.trips.front().expect("measured");
        assert!(
            shortest >= 0.44,
            "the shortest round trip is still {shortest} s"
        );
    }

    #[test]
    fn the_startup_queues_about_half_a_round_trip_at_most() {
        for (rate, trip) in [(2e6, 0.3), (10e6, 0.3), (100e6, 0.05)] {
            let mut congestion = Congestion::new(1);
            let mut link = Link::new(rate, trip);
            let mut now = 0.0;
            let mut queued = 0.0_f64;
            while now < 5.0 {
                let (next, waited) = link.step(&mut congestion, now);
                queued = queued.max(waited);
                now = next;
            }
            assert!(
                queued <= trip * 0.6,
                "{rate} B/s over {trip} s: a byte waited {queued} s in the queue"
            );
        }
    }

    #[test]
    fn a_fast_short_link_keeps_the_floor() {
        let mut congestion = Congestion::new(1);
        busy(&mut congestion, &mut Link::new(1e9, 0.0001), 1.0);
        assert!(congestion.window() >= FLOOR);
    }

    #[test]
    fn a_connection_idle_before_its_first_load_still_finds_the_rate() {
        let mut congestion = Congestion::new(1);
        let mut link = Link::new(2e6, 0.3);
        for second in 0..20 {
            congestion.idle();
            link.send(&mut congestion, 100, f64::from(second));
            link.ack(&mut congestion);
        }
        link.free = 20.0;
        let mut now = 20.0;
        while now < 25.0 {
            now = link.step(&mut congestion, now).0;
        }
        #[allow(clippy::cast_precision_loss)]
        let held = congestion.window() as f64 / (2e6 * 0.3);
        assert!(
            held >= 1.4,
            "five seconds in, the window holds {held} of the link"
        );
    }

    #[test]
    fn an_idle_sender_keeps_the_rate_it_measured() {
        let mut congestion = Congestion::new(1);
        let mut link = Link::new(10e6, 0.3);
        busy(&mut congestion, &mut link, 30.0);
        #[allow(clippy::cast_precision_loss)]
        let window = congestion.window() as f64;
        while !link.acks.is_empty() {
            link.ack(&mut congestion);
        }
        // A heartbeat a second for a minute, with room to spare.
        let mut now = 31.0;
        for _ in 0..60 {
            congestion.idle();
            link.send(&mut congestion, 100, now);
            link.ack(&mut congestion);
            now += 1.0;
        }
        #[allow(clippy::cast_precision_loss)]
        let kept = congestion.window() as f64 / window;
        assert!(kept >= 0.7, "kept {kept} of the window");
    }
}
