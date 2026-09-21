//! The streams of one connection over frames, without I/O.
//!
//! Inside a stream, each envelope is `u32 size | u8 name size | name | payload`, where size counts the bytes after
//! it. Each side may send on a stream only the credit the other side gave it, counted in uncompressed bytes. Credit
//! for complete envelopes returns when they are consumed; the bytes of an incomplete envelope are credited on
//! arrival, so an envelope larger than the window cannot stall its stream.

use crate::compress::Name;
use crate::frame::{Frame, ProtocolError};
use crate::limits::Limits;

/// The handshake.
pub const CONTROL: u32 = 1;
/// Everything but replication, which shares one stream.
pub const MESSAGES: u32 = 3;
/// Replication on its own, so that moving state never delays a message to an actor.
pub const REPLICATION: u32 = 5;

const STREAMS: [u32; 3] = [CONTROL, MESSAGES, REPLICATION];
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
    streams: [Stream; 3],
    control: Vec<u8>,
}

impl Mux {
    #[must_use]
    pub fn new(limits: Limits, min_compressed: usize) -> Self {
        Self {
            compressor: None,
            limits,
            min_compressed,
            streams: [
                Stream::new(limits.window),
                Stream::new(limits.window),
                Stream::new(limits.window),
            ],
            control: Vec::new(),
        }
    }

    /// Queue an envelope; `output` frames it as credit allows.
    pub fn send(&mut self, stream: u32, name: &str, payload: &[u8]) {
        let at = Self::index(stream).expect("a stream this side writes to is one of its own");
        let named = name.as_bytes();
        let size = 1 + named.len() + payload.len();
        let unsent = &mut self.streams[at].unsent;
        #[allow(clippy::cast_possible_truncation)]
        unsent.extend_from_slice(&(size as u32).to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        unsent.push(named.len() as u8);
        unsent.extend_from_slice(named);
        unsent.extend_from_slice(payload);
    }

    pub fn receive(&mut self, frame: Frame) -> Result<Vec<Record>, ProtocolError> {
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
                self.streams[at].window -= payload.len();
                self.records(stream, at, &payload)
            }
            Frame::WindowUpdate { stream, credit } => {
                let at = Self::stream(stream)?;
                self.streams[at].credit += credit as usize;
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

    /// Bytes to write: pongs, pings, credit for the peer, and queued envelopes up to each stream's credit.
    pub fn output(&mut self) -> Vec<u8> {
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
            loop {
                let held = self.streams[at].held();
                if held == 0 || self.streams[at].credit == 0 {
                    break;
                }
                let size = held.min(self.streams[at].credit).min(self.limits.frame);
                let chunk = self.streams[at].take(size);
                self.streams[at].credit -= size;
                self.data(number, chunk).write(&mut out);
            }
        }
        out
    }

    /// Whether anything is still waiting for credit, which is what a shutdown waits on.
    #[must_use]
    pub fn pending(&self) -> bool {
        self.streams.iter().any(|stream| stream.held() > 0)
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
        data: &[u8],
    ) -> Result<Vec<Record>, ProtocolError> {
        let message = self.limits.message;
        let stream = &mut self.streams[at];
        stream.received.extend_from_slice(data);
        let mut records = Vec::new();
        let mut start = 0;
        while stream.received.len() - start >= SIZE {
            let size = u32::from_be_bytes([
                stream.received[start],
                stream.received[start + 1],
                stream.received[start + 2],
                stream.received[start + 3],
            ]) as usize;
            if size == 0 || size > 1 + 255 + message {
                return Err(ProtocolError::new(format!(
                    "envelope of {size} bytes on stream {number}"
                )));
            }
            let end = start + SIZE + size;
            if stream.received.len() < end {
                break;
            }
            let named = stream.received[start + SIZE] as usize;
            let payload = start + SIZE + 1 + named;
            if payload > end || end - payload > message {
                return Err(ProtocolError::new(format!(
                    "malformed envelope on stream {number}"
                )));
            }
            let name = core::str::from_utf8(&stream.received[start + SIZE + 1..payload])
                .map_err(|_| {
                    ProtocolError::new(format!("envelope name is not UTF-8 on stream {number}"))
                })?
                .to_owned();
            records.push(Record {
                stream: number,
                name,
                payload: stream.received[payload..end].to_vec(),
                credit: end - start - stream.ahead,
            });
            stream.ahead = 0;
            start = end;
        }
        stream.received.drain(..start);
        if stream.received.len() > stream.ahead {
            stream.due += stream.received.len() - stream.ahead;
            stream.ahead = stream.received.len();
        }
        Ok(records)
    }
}

/// `credit`: bytes this side may still send. `window`: bytes the peer may still send. `ahead`: credit already
/// returned for the incomplete envelope at the start of `received`. `due`: credit to announce in the next output.
#[derive(Debug)]
struct Stream {
    credit: usize,
    window: usize,
    unsent: Vec<u8>,
    sent: usize,
    received: Vec<u8>,
    ahead: usize,
    due: usize,
}

impl Stream {
    fn new(window: usize) -> Self {
        Self {
            credit: window,
            window,
            unsent: Vec::new(),
            sent: 0,
            received: Vec::new(),
            ahead: 0,
            due: 0,
        }
    }

    fn held(&self) -> usize {
        self.unsent.len() - self.sent
    }

    fn take(&mut self, size: usize) -> Vec<u8> {
        let chunk = self.unsent[self.sent..self.sent + size].to_vec();
        self.sent += size;
        if self.sent == self.unsent.len() {
            self.unsent.clear();
            self.sent = 0;
        }
        chunk
    }
}

#[cfg(test)]
mod tests {
    use super::{MESSAGES, Mux, REPLICATION, Record};
    use crate::compress::Name;
    use crate::frame::{Decoder, Frame};
    use crate::limits::Limits;

    /// The frames one side writes, read back by the other, with the records the second one takes.
    fn carry(from: &mut Mux, to: &mut Mux) -> Vec<Record> {
        let written = from.output();
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);
        let mut records = Vec::new();
        while let Some(frame) = decoder.frame().unwrap() {
            records.extend(to.receive(frame).unwrap());
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
        a.send(MESSAGES, "actors", b"one");
        a.send(MESSAGES, "replies", b"two");
        a.send(REPLICATION, "replication", &vec![7; 3_000]);

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
        a.send(MESSAGES, "actors", &payload);

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
    fn a_stream_that_writes_past_its_credit_is_refused() {
        let limits = Limits {
            window: 16,
            ..Limits::default()
        };
        let (mut a, mut b) = pair(limits);
        a.send(MESSAGES, "actors", &[1; 64]);
        let written = a.output();
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);

        // One frame fits the window; a second one, forged, does not.
        let frame = decoder.frame().unwrap().unwrap();
        assert!(b.receive(frame).is_ok());
        let error = b
            .receive(Frame::Data {
                stream: MESSAGES,
                payload: [0; 64].to_vec(),
                compressed: false,
            })
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
        a.send(MESSAGES, "actors", &payload);

        let written = a.output();
        assert!(written.len() < payload.len() / 4, "nothing was compressed");
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);
        let mut records = Vec::new();
        while let Some(frame) = decoder.frame().unwrap() {
            records.extend(b.receive(frame).unwrap());
        }

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, payload);
    }

    #[test]
    fn a_ping_is_answered_and_an_unknown_stream_is_refused() {
        let mut mux = Mux::new(Limits::default(), 1 << 30);
        mux.receive(Frame::Ping).unwrap();
        let written = mux.output();
        let mut decoder = Decoder::new(1 << 20);
        decoder.feed(&written);
        assert_eq!(decoder.frame().unwrap(), Some(Frame::Pong));

        let error = mux
            .receive(Frame::Data {
                stream: 7,
                payload: Vec::new(),
                compressed: false,
            })
            .unwrap_err();
        assert_eq!(error.to_string(), "unknown stream 7");
    }

    #[test]
    fn a_malformed_envelope_ends_the_connection() {
        let mut mux = Mux::new(Limits::default(), 1 << 30);
        let refused = |mux: &mut Mux, payload: Vec<u8>| {
            mux.receive(Frame::Data {
                stream: MESSAGES,
                payload,
                compressed: false,
            })
            .unwrap_err()
            .to_string()
        };
        assert_eq!(
            refused(&mut mux, vec![0, 0, 0, 0]),
            "envelope of 0 bytes on stream 3"
        );

        let mut other = Mux::new(Limits::default(), 1 << 30);
        // A name longer than the envelope it is in.
        let mut payload = vec![0, 0, 0, 2, 9];
        payload.push(b'x');
        assert_eq!(
            refused(&mut other, payload),
            "malformed envelope on stream 3"
        );

        let mut third = Mux::new(Limits::default(), 1 << 30);
        assert_eq!(
            refused(&mut third, vec![0, 0, 0, 2, 1, 0xff]),
            "envelope name is not UTF-8 on stream 3"
        );
    }
}
