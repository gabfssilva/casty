//! The streams of one connection over frames, without I/O.
//!
//! An envelope is `u32 size | u8 name size | name | payload`, where size counts the bytes after it. Each data frame
//! starts with a lane byte. A large envelope is sent on one of a stream's lanes, a byte stream that carries one
//! envelope after another, interleaved with the other lanes a quantum each in turn; envelopes small enough for one
//! frame go whole, in frames of lane `WHOLE` that hold only complete envelopes, ahead of the quanta of large ones. An
//! envelope waits for every one queued before it with the same order to have gone out, so envelopes of one order
//! arrive in the order they were queued; the others overtake each other. An envelope given up while queued goes no
//! further: one not started is dropped, and one partly sent is abandoned with an empty frame on its lane.
//!
//! Each side may send on a stream only the credit the other side gave it, counted in uncompressed frame bytes. Credit
//! for complete envelopes returns when they are consumed; the lane byte and the bytes of an incomplete envelope are
//! credited on arrival, so an envelope larger than the window cannot stall its stream. On top of the credit, what a
//! connection sends is paced and held near what the link carries (`Congestion`): the control and membership streams
//! go out ahead of that, and the others share it.

use std::collections::{HashSet, VecDeque};

use crate::compress::Name;
use crate::congestion::Congestion;
use crate::frame::{Frame, ProtocolError};
use crate::limits::Limits;

/// The handshake.
pub const CONTROL: u32 = 1;
/// Messages to actors and their answers: those to one key keep their order, and the rest overtake each other.
pub const MESSAGES: u32 = 3;
/// Replication on its own, so that moving state never delays a message to an actor.
pub const REPLICATION: u32 = 5;
/// Membership on its own: a heartbeat queued behind large messages would arrive only after them, and a peer whose
/// link is busy but alive would be suspected for it.
pub const MEMBERSHIP: u32 = 7;

/// In the order `output` writes them.
const STREAMS: [u32; 4] = [CONTROL, MEMBERSHIP, MESSAGES, REPLICATION];
/// The streams before this one go out ahead of the pace and the window: they are small, and late they fail a
/// handshake or get a live peer suspected.
const URGENT: usize = 2;
/// Large envelopes a stream sends at once.
const LANES: usize = 16;
/// The lane of frames that carry whole envelopes.
const WHOLE: u8 = 255;
/// The most a lane sends before the next one takes its turn, and the largest envelope that goes whole.
const QUANTUM: usize = 16 * 1024;
/// One quantum in this many goes to the envelope that started first.
const OLDEST: usize = 4;
/// The most one `output` writes of the streams that share the window, so that what is queued meanwhile is not
/// written behind a long write.
const BATCH: usize = 64 * 1024;
const SIZE: usize = 4;

/// An envelope received on `stream`. Its `credit` returns to the peer once the envelope is consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub stream: u32,
    pub name: String,
    pub payload: Vec<u8>,
    pub credit: usize,
}

#[derive(Debug)]
pub struct Mux {
    pub compressor: Option<Name>,
    limits: Limits,
    min_compressed: usize,
    streams: [Stream; STREAMS.len()],
    control: Vec<u8>,
    congestion: Congestion,
    /// Quanta sent on the lanes of the streams after `URGENT`.
    turn: usize,
    /// Envelopes put on a lane, which orders them by when they started.
    started: u64,
}

impl Mux {
    #[must_use]
    pub fn new(limits: Limits, min_compressed: usize) -> Self {
        Self {
            compressor: None,
            limits,
            min_compressed,
            streams: STREAMS.map(|_| Stream::new(limits.window)),
            control: Vec::new(),
            congestion: Congestion::new(STREAMS.len()),
            turn: 0,
            started: 0,
        }
    }

    /// Queue an envelope; `output` frames it as credit allows. Envelopes of one `order` arrive in the order queued, and
    /// one with a `tag` can be given up with `retract`.
    pub fn send(&mut self, stream: u32, order: u64, tag: Option<u64>, name: &str, payload: &[u8]) {
        let at = Self::index(stream).expect("a stream this side writes to is one of its own");
        let named = name.as_bytes();
        let size = 1 + named.len() + payload.len();
        let mut bytes = Vec::with_capacity(SIZE + size);
        #[allow(clippy::cast_possible_truncation)]
        bytes.extend_from_slice(&(size as u32).to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.push(named.len() as u8);
        bytes.extend_from_slice(named);
        bytes.extend_from_slice(payload);
        self.streams[at]
            .pending
            .push_back(Envelope { order, tag, bytes });
    }

    /// Give up the envelopes queued with `tag` that have not gone out whole: nobody waits for them any more.
    pub fn retract(&mut self, tag: u64) {
        for stream in &mut self.streams[URGENT..] {
            stream.pending.retain(|envelope| envelope.tag != Some(tag));
            for lane in &mut stream.lanes {
                if let Some(active) = lane
                    && active.tag == Some(tag)
                {
                    if active.sent == 0 {
                        *lane = None;
                    } else {
                        active.bytes.truncate(active.sent);
                        active.abandoned = true;
                    }
                }
            }
        }
    }

    /// `now` is monotonic seconds, as for every method that takes it.
    pub fn receive(&mut self, frame: Frame, now: f64) -> Result<Vec<Record>, ProtocolError> {
        match frame {
            Frame::Data {
                stream,
                payload,
                compressed,
            } => {
                let at = Self::stream(stream)?;
                let payload = if compressed {
                    let Some(compressor) = self.compressor else {
                        return Err(ProtocolError::new(
                            "compressed frame on a connection without compression",
                        ));
                    };
                    compressor.decompress(&payload, self.limits.frame)?
                } else {
                    payload
                };
                if payload.len() > self.streams[at].window {
                    return Err(ProtocolError::new(format!(
                        "stream {stream} exceeded its window"
                    )));
                }
                let Some((&lane, data)) = payload.split_first() else {
                    return Err(ProtocolError::new(format!(
                        "data frame without a lane on stream {stream}"
                    )));
                };
                if lane != WHOLE && usize::from(lane) >= LANES {
                    return Err(ProtocolError::new(format!(
                        "unknown lane {lane} on stream {stream}"
                    )));
                }
                self.streams[at].window -= payload.len();
                self.streams[at].due += 1;
                self.records(stream, at, lane, data)
            }
            Frame::WindowUpdate { stream, credit } => {
                let at = Self::stream(stream)?;
                self.streams[at].credit += credit as usize;
                self.congestion.acked(at, credit as usize, now);
                Ok(Vec::new())
            }
            Frame::Ping => {
                Frame::Pong.write(&mut self.control);
                Ok(Vec::new())
            }
            Frame::Pong | Frame::GoAway => Ok(Vec::new()),
        }
    }

    /// Give back the credit of an envelope the node has taken.
    pub fn consume(&mut self, record: &Record) {
        let at = Self::index(record.stream).expect("a record comes from a stream of this mux");
        self.streams[at].due += record.credit;
    }

    pub fn ping(&mut self) {
        Frame::Ping.write(&mut self.control);
    }

    /// Bytes to write: pongs, pings, credit for the peer, the urgent streams up to their credit, and the others up to
    /// their credit and what the pace and the window allow, small envelopes first and then a quantum of each lane in
    /// turn. A frame waits until it can go whole: cut to whatever the pace allowed at each wakeup, frames would be
    /// mostly headers.
    pub fn output(&mut self, now: f64) -> Vec<u8> {
        let mut out = core::mem::take(&mut self.control);
        for (at, number) in STREAMS.into_iter().enumerate() {
            let due = self.streams[at].due;
            if due > 0 {
                #[allow(clippy::cast_possible_truncation)]
                Frame::WindowUpdate {
                    stream: number,
                    credit: due as u32,
                }
                .write(&mut out);
                self.streams[at].window += due;
                self.streams[at].due = 0;
            }
        }
        for at in 0..URGENT {
            while self.whole(at, usize::MAX, now, &mut out) > 0 {}
            self.streams[at].activate(&mut self.started);
            for lane in 0..LANES {
                while self.quantum(at, lane, usize::MAX, now, &mut out) > 0 {}
            }
        }
        // Small envelopes are held to the pace but not to the window, which never fills on their account: they need
        // not wait for what large ones put in flight, as when the window shrinks to measure the round trip.
        let mut budget = self.congestion.paced(now).min(BATCH);
        for at in URGENT..STREAMS.len() {
            self.abandon(at, now, &mut out);
            loop {
                let written = self.whole(at, budget, now, &mut out);
                if written == 0 {
                    break;
                }
                budget -= written;
            }
            self.streams[at].activate(&mut self.started);
        }
        budget = budget.min(self.congestion.room());
        while let Some((at, lane, size)) = self.next_lane() {
            if size + 1 > budget {
                break;
            }
            budget -= self.quantum(at, lane, budget, now, &mut out);
            self.turn += 1;
            if self.streams[at].lanes[lane].is_none() {
                self.streams[at].activate(&mut self.started);
            }
        }
        if self.ready() == (None, None) && self.congestion.room() > 0 {
            self.congestion.idle();
        }
        out
    }

    /// When `output` will have more to write without anything arriving first, if it is the pace that holds back what
    /// is queued: credit and room in the window arrive with the peer's frames. It wakes for the smallest frame waiting,
    /// which may still have to wait for its turn.
    #[must_use]
    pub fn wake(&self) -> Option<f64> {
        let (whole, lane) = self.ready();
        let whole = whole.and_then(|size| self.congestion.ready_at(size, false));
        let lane = lane.and_then(|size| self.congestion.ready_at(size, true));
        whole.into_iter().chain(lane).min_by(f64::total_cmp)
    }

    /// Whether anything is still waiting for credit, which is what a shutdown waits on.
    #[must_use]
    pub fn pending(&self) -> bool {
        self.streams
            .iter()
            .any(|stream| !stream.pending.is_empty() || stream.lanes.iter().any(Option::is_some))
    }

    /// The lane of the streams that share the window that sends next, with what it would send: the envelope with the
    /// fewest bytes left, so that large envelopes finish one after another and smaller ones pass them, and every
    /// `OLDEST` quanta the one that started first, so that a large one is never passed for good.
    fn next_lane(&self) -> Option<(usize, usize, usize)> {
        let sending = (URGENT..STREAMS.len()).flat_map(|at| {
            (0..LANES).filter_map(move |lane| {
                let size = self.streams[at].sendable(lane, self.limits.frame);
                let active = self.streams[at].lanes[lane].as_ref()?;
                (size > 0).then_some((at, lane, size, active))
            })
        });
        let chosen = if self.turn % OLDEST == OLDEST - 1 {
            sending.min_by_key(|&(.., active)| active.started)
        } else {
            sending.min_by_key(|&(.., active)| (active.bytes.len() - active.sent, active.started))
        };
        chosen.map(|(at, lane, size, _)| (at, lane, size))
    }

    /// The smallest frames the streams that share the window could send now, whole and on a lane, if any.
    fn ready(&self) -> (Option<usize>, Option<usize>) {
        let whole = self.streams[URGENT..]
            .iter()
            .flat_map(|stream| {
                stream
                    .eligible()
                    .into_iter()
                    .map(|index| stream.pending[index].bytes.len())
                    .filter(|&size| size <= QUANTUM && size < stream.credit)
            })
            .min();
        let lane = self.streams[URGENT..]
            .iter()
            .flat_map(|stream| (0..LANES).map(|lane| stream.sendable(lane, self.limits.frame)))
            .filter(|&size| size > 0)
            .min();
        (whole.map(|size| size + 1), lane.map(|size| size + 1))
    }

    /// Frame whole the small envelopes of stream `at` that may go now, up to `most` bytes, giving back the frame's
    /// payload size.
    fn whole(&mut self, at: usize, most: usize, now: f64, out: &mut Vec<u8>) -> usize {
        let room = most
            .min(self.streams[at].credit)
            .min(self.limits.frame)
            .min(QUANTUM + 1);
        let mut payload = vec![WHOLE];
        let stream = &mut self.streams[at];
        let mut blocked: HashSet<u64> = stream
            .lanes
            .iter()
            .flatten()
            .map(|active| active.order)
            .collect();
        let mut kept = VecDeque::with_capacity(stream.pending.len());
        for envelope in core::mem::take(&mut stream.pending) {
            if !blocked.contains(&envelope.order)
                && envelope.bytes.len() <= QUANTUM
                && payload.len() + envelope.bytes.len() <= room
            {
                payload.extend_from_slice(&envelope.bytes);
            } else {
                blocked.insert(envelope.order);
                kept.push_back(envelope);
            }
        }
        stream.pending = kept;
        if payload.len() == 1 {
            return 0;
        }
        self.write(at, payload, now, out)
    }

    /// Tell the peer to drop what it holds of the envelopes abandoned on the lanes of stream `at`, freeing the lanes.
    fn abandon(&mut self, at: usize, now: f64, out: &mut Vec<u8>) {
        for lane in 0..LANES {
            let abandoned = self.streams[at].lanes[lane]
                .as_ref()
                .is_some_and(|active| active.abandoned);
            if abandoned && self.streams[at].credit > 0 {
                self.streams[at].lanes[lane] = None;
                #[allow(clippy::cast_possible_truncation)]
                self.write(at, vec![lane as u8], now, out);
            }
        }
    }

    /// Frame up to `most` bytes of the envelope on one lane, giving back the frame's payload size.
    fn quantum(
        &mut self,
        at: usize,
        lane: usize,
        most: usize,
        now: f64,
        out: &mut Vec<u8>,
    ) -> usize {
        let stream = &mut self.streams[at];
        let size = stream
            .sendable(lane, self.limits.frame)
            .min(most.saturating_sub(1));
        if size == 0 {
            return 0;
        }
        let mut payload = Vec::with_capacity(1 + size);
        #[allow(clippy::cast_possible_truncation)]
        payload.push(lane as u8);
        stream.take(lane, size, &mut payload);
        self.write(at, payload, now, out)
    }

    fn write(&mut self, at: usize, payload: Vec<u8>, now: f64, out: &mut Vec<u8>) -> usize {
        let written = payload.len();
        self.streams[at].credit -= written;
        self.congestion.sent(at, written, now);
        self.data(STREAMS[at], payload).write(out);
        written
    }

    fn data(&self, stream: u32, chunk: Vec<u8>) -> Frame {
        if let Some(compressor) = self.compressor
            && chunk.len() >= self.min_compressed
        {
            let compressed = compressor.compress(&chunk);
            if compressed.len() < chunk.len() {
                return Frame::Data {
                    stream,
                    payload: compressed,
                    compressed: true,
                };
            }
        }
        Frame::Data {
            stream,
            payload: chunk,
            compressed: false,
        }
    }

    fn stream(number: u32) -> Result<usize, ProtocolError> {
        Self::index(number).ok_or_else(|| ProtocolError::new(format!("unknown stream {number}")))
    }

    fn index(number: u32) -> Option<usize> {
        STREAMS.iter().position(|held| *held == number)
    }

    fn records(
        &mut self,
        number: u32,
        at: usize,
        lane: u8,
        data: &[u8],
    ) -> Result<Vec<Record>, ProtocolError> {
        let message = self.limits.message;
        let stream = &mut self.streams[at];
        if lane == WHOLE {
            let (records, end) = parse(number, message, data, 0)?;
            if end != data.len() {
                return Err(ProtocolError::new(format!(
                    "incomplete envelope in a whole frame on stream {number}"
                )));
            }
            return Ok(records);
        }
        let inbound = &mut stream.inbound[usize::from(lane)];
        // Its bytes were credited as they came.
        if data.is_empty() {
            inbound.received.clear();
            inbound.ahead = 0;
            return Ok(Vec::new());
        }
        inbound.received.extend_from_slice(data);
        let (records, end) = parse(number, message, &inbound.received, inbound.ahead)?;
        if !records.is_empty() {
            inbound.ahead = 0;
        }
        inbound.received.drain(..end);
        if inbound.received.len() > inbound.ahead {
            stream.due += inbound.received.len() - inbound.ahead;
            inbound.ahead = inbound.received.len();
        }
        Ok(records)
    }
}

/// The complete envelopes at the start of `held`, and where they end. `ahead` bytes of the first were credited
/// already.
fn parse(
    number: u32,
    message: usize,
    held: &[u8],
    ahead: usize,
) -> Result<(Vec<Record>, usize), ProtocolError> {
    let mut records = Vec::new();
    let mut start = 0;
    while held.len() - start >= SIZE {
        let size = u32::from_be_bytes([
            held[start],
            held[start + 1],
            held[start + 2],
            held[start + 3],
        ]) as usize;
        if size == 0 || size > 1 + 255 + message {
            return Err(ProtocolError::new(format!(
                "envelope of {size} bytes on stream {number}"
            )));
        }
        let end = start + SIZE + size;
        if held.len() < end {
            break;
        }
        let named = held[start + SIZE] as usize;
        let payload = start + SIZE + 1 + named;
        if payload > end || end - payload > message {
            return Err(ProtocolError::new(format!(
                "malformed envelope on stream {number}"
            )));
        }
        let name = core::str::from_utf8(&held[start + SIZE + 1..payload])
            .map_err(|_| {
                ProtocolError::new(format!("envelope name is not UTF-8 on stream {number}"))
            })?
            .to_owned();
        let credited = if start == 0 { ahead } else { 0 };
        records.push(Record {
            stream: number,
            name,
            payload: held[payload..end].to_vec(),
            credit: end - start - credited,
        });
        start = end;
    }
    Ok((records, start))
}

/// `credit`: bytes this side may still send. `window`: bytes the peer may still send. `due`: credit to announce in
/// the next output. `pending`: envelopes not started, in the order queued. `lanes`: the large envelope each lane is
/// sending.
#[derive(Debug)]
struct Stream {
    credit: usize,
    window: usize,
    due: usize,
    pending: VecDeque<Envelope>,
    lanes: Vec<Option<Sending>>,
    inbound: Vec<Inbound>,
}

impl Stream {
    fn new(window: usize) -> Self {
        Self {
            credit: window,
            window,
            due: 0,
            pending: VecDeque::new(),
            lanes: (0..LANES).map(|_| None).collect(),
            inbound: (0..LANES).map(|_| Inbound::default()).collect(),
        }
    }

    /// The pending envelopes that may start now: none of their order is on a lane or queued before them.
    fn eligible(&self) -> Vec<usize> {
        let mut blocked: HashSet<u64> = self
            .lanes
            .iter()
            .flatten()
            .map(|active| active.order)
            .collect();
        let mut eligible = Vec::new();
        for (index, envelope) in self.pending.iter().enumerate() {
            if blocked.insert(envelope.order) {
                eligible.push(index);
            }
        }
        eligible
    }

    /// Put the first envelopes that may start on the lanes that are free.
    fn activate(&mut self, started: &mut u64) {
        while let Some(lane) = self.lanes.iter().position(Option::is_none) {
            let Some(&index) = self.eligible().first() else {
                return;
            };
            let envelope = self
                .pending
                .remove(index)
                .expect("an eligible envelope is pending");
            *started += 1;
            self.lanes[lane] = Some(Sending {
                order: envelope.order,
                tag: envelope.tag,
                bytes: envelope.bytes,
                sent: 0,
                started: *started,
                abandoned: false,
            });
        }
    }

    /// Bytes of a lane the next frame would carry, as its credit allows.
    fn sendable(&self, lane: usize, frame: usize) -> usize {
        self.lanes[lane].as_ref().map_or(0, |active| {
            (active.bytes.len() - active.sent)
                .min(self.credit.saturating_sub(1))
                .min(frame - 1)
                .min(QUANTUM)
        })
    }

    fn take(&mut self, lane: usize, size: usize, into: &mut Vec<u8>) {
        let active = self.lanes[lane]
            .as_mut()
            .expect("a lane sends only what it holds");
        into.extend_from_slice(&active.bytes[active.sent..active.sent + size]);
        active.sent += size;
        if active.sent == active.bytes.len() {
            self.lanes[lane] = None;
        }
    }
}

/// An envelope queued, its bytes as the wire carries them.
#[derive(Debug)]
struct Envelope {
    order: u64,
    tag: Option<u64>,
    bytes: Vec<u8>,
}

/// An envelope on a lane, of which `sent` bytes went out, the `started`th put on a lane. An `abandoned` one holds only
/// what went out, and its lane is freed once the peer is told.
#[derive(Debug)]
struct Sending {
    order: u64,
    tag: Option<u64>,
    bytes: Vec<u8>,
    sent: usize,
    started: u64,
    abandoned: bool,
}

/// What arrived on a lane: `received` holds the incomplete envelope at its start, of which `ahead` bytes were already
/// credited.
#[derive(Debug, Default)]
struct Inbound {
    received: Vec<u8>,
    ahead: usize,
}

#[cfg(test)]
mod tests {
    use super::{MEMBERSHIP, MESSAGES, Mux, REPLICATION, Record};
    use crate::compress::Name;
    use crate::congestion::FLOOR;
    use crate::frame::{Decoder, Frame};
    use crate::limits::Limits;

    /// The frames one side writes, read back by the other, with the records the second one takes.
    fn carry(from: &mut Mux, to: &mut Mux) -> Vec<Record> {
        let written = from.output(0.0);
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);
        let mut records = Vec::new();
        while let Some(frame) = decoder.frame().unwrap() {
            records.extend(to.receive(frame, 0.0).unwrap());
        }
        for record in &records {
            to.consume(record);
        }
        records
    }

    fn pair(limits: Limits) -> (Mux, Mux) {
        (Mux::new(limits, 1 << 30), Mux::new(limits, 1 << 30))
    }

    #[test]
    fn envelopes_arrive_whole_and_in_order_within_a_stream() {
        let (mut a, mut b) = pair(Limits::default());
        a.send(MESSAGES, 0, None, "actors", b"one");
        a.send(MESSAGES, 0, None, "replies", b"two");
        a.send(REPLICATION, 0, None, "replication", &vec![7; 3_000]);

        let records = carry(&mut a, &mut b);

        assert_eq!(
            records
                .iter()
                .map(|record| (record.stream, record.name.as_str()))
                .collect::<Vec<_>>(),
            [
                (MESSAGES, "actors"),
                (MESSAGES, "replies"),
                (REPLICATION, "replication")
            ]
        );
        assert_eq!(records[0].payload, b"one");
        assert_eq!(records[2].payload, vec![7; 3_000]);
    }

    #[test]
    fn an_envelope_larger_than_the_window_still_crosses() {
        let limits = Limits {
            frame: 1_024,
            window: 4_096,
            ..Limits::default()
        };
        let (mut a, mut b) = pair(limits);
        let payload = vec![3; 50_000];
        a.send(MESSAGES, 0, None, "actors", &payload);

        let mut received = Vec::new();
        for _ in 0..64 {
            received.extend(carry(&mut a, &mut b));
            // The credit the receiver gave back travels the other way before the sender writes again.
            carry(&mut b, &mut a);
        }

        assert_eq!(received.len(), 1);
        assert_eq!(received[0].payload, payload);
    }

    #[test]
    fn membership_queued_behind_a_large_message_goes_out_first() {
        let (mut a, mut b) = pair(Limits::default());
        a.send(MESSAGES, 0, None, "actors", &vec![1; 4 << 20]);
        a.send(MEMBERSHIP, 0, None, "membership", b"heartbeat");

        let records = carry(&mut a, &mut b);

        assert_eq!(
            records
                .iter()
                .map(|record| (record.stream, record.payload.as_slice()))
                .collect::<Vec<_>>(),
            [(MEMBERSHIP, b"heartbeat".as_slice())]
        );
    }

    #[test]
    fn a_small_envelope_crosses_while_a_large_one_of_another_order_waits() {
        let (mut a, mut b) = pair(Limits::default());
        a.send(MESSAGES, 1, None, "actors", &vec![1; 4 << 20]);
        a.send(MESSAGES, 2, None, "actors", b"ping");

        let records = carry(&mut a, &mut b);

        assert_eq!(
            records
                .iter()
                .map(|record| record.payload.as_slice())
                .collect::<Vec<_>>(),
            [b"ping".as_slice()]
        );
    }

    #[test]
    fn a_small_envelope_crosses_while_more_large_ones_wait_than_there_are_lanes() {
        let (mut a, mut b) = pair(Limits {
            window: 64 << 20,
            message: 1 << 30,
            ..Limits::default()
        });
        for order in 0..40 {
            a.send(MESSAGES, order, None, "actors", &vec![1; 1 << 20]);
        }
        a.send(MESSAGES, 99, None, "actors", b"ping");

        let records = carry(&mut a, &mut b);

        assert_eq!(
            records
                .iter()
                .map(|record| record.payload.as_slice())
                .collect::<Vec<_>>(),
            [b"ping".as_slice()]
        );
    }

    #[test]
    fn large_envelopes_finish_one_after_another_and_smaller_ones_pass_them() {
        let (mut a, mut b) = pair(Limits {
            window: 64 << 20,
            message: 1 << 30,
            ..Limits::default()
        });
        a.send(MESSAGES, 1, None, "actors", &vec![1; 2 << 20]);
        a.send(MESSAGES, 2, None, "actors", &vec![2; 2 << 20]);
        a.send(MESSAGES, 3, None, "actors", &vec![3; 64 << 10]);

        // Each envelope with the exchange it arrived in.
        let mut received = Vec::new();
        for exchange in 0.. {
            if received.len() == 3 {
                break;
            }
            for record in carry(&mut a, &mut b) {
                received.push((record.payload[0], exchange));
            }
            carry(&mut b, &mut a);
        }

        let order: Vec<u8> = received.iter().map(|&(payload, _)| payload).collect();
        assert_eq!(order, [3, 1, 2]);
        let (first, last) = (received[1].1, received[2].1);
        assert!(
            first * 10 <= last * 6,
            "the first large envelope arrived in exchange {first}, the second in {last}"
        );
    }

    #[test]
    fn envelopes_of_one_order_keep_it_behind_a_large_one() {
        let (mut a, mut b) = pair(Limits::default());
        a.send(MESSAGES, 1, None, "actors", &vec![1; 1 << 20]);
        a.send(MESSAGES, 1, None, "actors", b"after");
        a.send(MESSAGES, 2, None, "actors", b"other");

        let mut received = Vec::new();
        while received.len() < 3 {
            received.extend(carry(&mut a, &mut b));
            carry(&mut b, &mut a);
        }

        assert_eq!(
            received
                .iter()
                .map(|record| record.payload.len())
                .collect::<Vec<_>>(),
            [5, 1 << 20, 5]
        );
        assert_eq!(received[2].payload, b"after");
    }

    #[test]
    fn what_is_in_flight_stays_within_the_window_but_small_envelopes_go_out() {
        let (mut a, _) = pair(Limits {
            window: 64 << 20,
            ..Limits::default()
        });
        a.send(MESSAGES, 1, None, "actors", &vec![1; 16 << 20]);
        let mut written = 0;
        for _ in 0..1_000 {
            written += a.output(0.0).len();
        }
        assert!(
            written < FLOOR + 1024,
            "{written} bytes written unacknowledged"
        );

        a.send(MEMBERSHIP, 0, None, "membership", b"heartbeat");
        a.send(MESSAGES, 2, None, "actors", b"ping");
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&a.output(0.0));
        let mut streams = Vec::new();
        while let Some(Frame::Data { stream, .. }) = decoder.frame().unwrap() {
            streams.push(stream);
        }
        assert_eq!(streams, [MEMBERSHIP, MESSAGES]);
    }

    #[test]
    fn a_retracted_envelope_goes_no_further_and_what_follows_it_arrives_whole() {
        let (mut a, mut b) = pair(Limits::default());
        a.send(MESSAGES, 1, Some(7), "actors", &vec![1; 1 << 20]);
        a.send(MESSAGES, 1, Some(8), "actors", &vec![2; 1 << 20]);
        a.send(MESSAGES, 1, None, "actors", b"after");
        assert_eq!(carry(&mut a, &mut b), []);

        a.retract(7);
        a.retract(8);
        a.send(MESSAGES, 2, None, "actors", &vec![3; 1 << 20]);
        let mut received = Vec::new();
        while received.len() < 2 {
            received.extend(carry(&mut a, &mut b));
            carry(&mut b, &mut a);
        }
        let payloads: Vec<_> = received
            .iter()
            .map(|record| record.payload.clone())
            .collect();
        assert_eq!(payloads, [b"after".to_vec(), vec![3; 1 << 20]]);
        assert!(!a.pending());
    }

    #[test]
    fn a_stream_that_writes_past_its_credit_is_refused() {
        let limits = Limits {
            window: 16,
            ..Limits::default()
        };
        let (mut a, mut b) = pair(limits);
        a.send(MESSAGES, 0, None, "actors", &[1; 64]);
        let written = a.output(0.0);
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);

        // One frame fits the window; a second one, forged, does not.
        let frame = decoder.frame().unwrap().unwrap();
        assert!(b.receive(frame, 0.0).is_ok());
        let error = b
            .receive(
                Frame::Data {
                    stream: MESSAGES,
                    payload: [0; 64].to_vec(),
                    compressed: false,
                },
                0.0,
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "stream 3 exceeded its window");
    }

    #[test]
    fn compression_is_marked_per_frame_and_read_back() {
        let (mut a, mut b) = (
            Mux::new(Limits::default(), 64),
            Mux::new(Limits::default(), 64),
        );
        a.compressor = Some(Name::Zstd);
        b.compressor = Some(Name::Zstd);
        let payload = b"a line that repeats itself. ".repeat(1_000);
        a.send(MESSAGES, 0, None, "actors", &payload);

        let written = a.output(0.0);
        assert!(written.len() < payload.len() / 4, "nothing was compressed");
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);
        let mut records = Vec::new();
        while let Some(frame) = decoder.frame().unwrap() {
            records.extend(b.receive(frame, 0.0).unwrap());
        }

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, payload);
    }

    #[test]
    fn a_ping_is_answered_and_an_unknown_stream_or_lane_is_refused() {
        let mut mux = Mux::new(Limits::default(), 1 << 30);
        mux.receive(Frame::Ping, 0.0).unwrap();
        let written = mux.output(0.0);
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);
        assert_eq!(decoder.frame().unwrap(), Some(Frame::Pong));

        let error = mux
            .receive(
                Frame::Data {
                    stream: 9,
                    payload: Vec::new(),
                    compressed: false,
                },
                0.0,
            )
            .unwrap_err();
        assert_eq!(error.to_string(), "unknown stream 9");

        let error = mux
            .receive(
                Frame::Data {
                    stream: MEMBERSHIP,
                    payload: vec![16, 0, 0, 0, 1, 0],
                    compressed: false,
                },
                0.0,
            )
            .unwrap_err();
        assert_eq!(error.to_string(), "unknown lane 16 on stream 7");
    }

    #[test]
    fn a_malformed_envelope_ends_the_connection() {
        let mut mux = Mux::new(Limits::default(), 1 << 30);
        let refused = |mux: &mut Mux, payload: Vec<u8>| {
            mux.receive(
                Frame::Data {
                    stream: MESSAGES,
                    payload,
                    compressed: false,
                },
                0.0,
            )
            .unwrap_err()
            .to_string()
        };
        assert_eq!(
            refused(&mut mux, vec![0, 0, 0, 0, 0]),
            "envelope of 0 bytes on stream 3"
        );

        let mut other = Mux::new(Limits::default(), 1 << 30);
        // A name longer than the envelope it is in.
        let mut payload = vec![0, 0, 0, 0, 2, 9];
        payload.push(b'x');
        assert_eq!(
            refused(&mut other, payload),
            "malformed envelope on stream 3"
        );

        let mut third = Mux::new(Limits::default(), 1 << 30);
        assert_eq!(
            refused(&mut third, vec![0, 0, 0, 0, 2, 1, 0xff]),
            "envelope name is not UTF-8 on stream 3"
        );
    }
}
