//! Frames of a multiplexed connection: a twelve byte header and a payload.
//!
//! The header is version, type, flags, stream and length, in that order and big endian. A window update carries its
//! credit in the length field. It has no payload, and neither do ping, pong and go-away.

use core::fmt;

pub const VERSION: u8 = 1;

const DATA: u8 = 0;
const WINDOW_UPDATE: u8 = 1;
const PING: u8 = 2;
const GO_AWAY: u8 = 3;
const PONG: u8 = 4;
const COMPRESSED: u16 = 0x10;
const HEADER: usize = 12;

/// `Frame::GoAway` as written, which is the last thing a connection writes.
pub const GOODBYE: [u8; HEADER] = [VERSION, GO_AWAY, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

/// The peer broke the wire protocol, so the connection closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(String);

impl ProtocolError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl core::error::Error for ProtocolError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Data {
        stream: u32,
        payload: Vec<u8>,
        compressed: bool,
    },
    WindowUpdate {
        stream: u32,
        credit: u32,
    },
    Ping,
    Pong,
    GoAway,
}

impl Frame {
    /// Append the frame to `out`, header first.
    pub fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::Data {
                stream,
                payload,
                compressed,
            } => {
                let flags = if *compressed { COMPRESSED } else { 0 };
                header(out, DATA, flags, *stream, length(payload.len()));
                out.extend_from_slice(payload);
            }
            Self::WindowUpdate { stream, credit } => {
                header(out, WINDOW_UPDATE, 0, *stream, *credit);
            }
            Self::Ping => header(out, PING, 0, 0, 0),
            Self::Pong => header(out, PONG, 0, 0, 0),
            Self::GoAway => out.extend_from_slice(&GOODBYE),
        }
    }
}

/// The length field of a frame, which the frame limit keeps far below what it holds.
fn length(len: usize) -> u32 {
    u32::try_from(len).expect("a frame is bounded well below four gigabytes")
}

fn header(out: &mut Vec<u8>, kind: u8, flags: u16, stream: u32, length: u32) {
    out.push(VERSION);
    out.push(kind);
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(&length.to_be_bytes());
}

/// Frames out of bytes that arrive in arbitrary pieces.
#[derive(Debug)]
pub struct Decoder {
    buffer: Vec<u8>,
    at: usize,
    max_frame: usize,
}

impl Decoder {
    #[must_use]
    pub fn new(max_frame: usize) -> Self {
        Self {
            buffer: Vec::new(),
            at: 0,
            max_frame,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        if self.at > 0 && self.at == self.buffer.len() {
            self.buffer.clear();
            self.at = 0;
        }
        self.buffer.extend_from_slice(data);
    }

    /// The next complete frame, or nothing until more bytes are fed.
    pub fn frame(&mut self) -> Result<Option<Frame>, ProtocolError> {
        let held = &self.buffer[self.at..];
        if held.len() < HEADER {
            return Ok(None);
        }
        let version = held[0];
        let kind = held[1];
        let flags = u16::from_be_bytes([held[2], held[3]]);
        let stream = u32::from_be_bytes([held[4], held[5], held[6], held[7]]);
        let length = u32::from_be_bytes([held[8], held[9], held[10], held[11]]);
        if version != VERSION {
            return Err(ProtocolError::new(format!(
                "unknown frame version {version}"
            )));
        }
        if kind == WINDOW_UPDATE {
            if flags != 0 || length == 0 {
                return Err(ProtocolError::new("malformed window update"));
            }
            self.take(HEADER);
            return Ok(Some(Frame::WindowUpdate {
                stream,
                credit: length,
            }));
        }
        let length = length as usize;
        if length > self.max_frame {
            return Err(ProtocolError::new(format!(
                "frame of {length} bytes exceeds {}",
                self.max_frame
            )));
        }
        if held.len() < HEADER + length {
            return Ok(None);
        }
        let payload = held[HEADER..HEADER + length].to_vec();
        self.take(HEADER + length);
        match (kind, flags) {
            (DATA, 0) => Ok(Some(Frame::Data {
                stream,
                payload,
                compressed: false,
            })),
            (DATA, COMPRESSED) => Ok(Some(Frame::Data {
                stream,
                payload,
                compressed: true,
            })),
            (PING, 0) if length == 0 => Ok(Some(Frame::Ping)),
            (PONG, 0) if length == 0 => Ok(Some(Frame::Pong)),
            (GO_AWAY, 0) => Ok(Some(Frame::GoAway)),
            _ => Err(ProtocolError::new(format!(
                "unknown frame type {kind} with flags {flags:#x}"
            ))),
        }
    }

    fn take(&mut self, len: usize) {
        self.at += len;
        // The read cursor walks the buffer, which is reset once what is held has been read to the end.
        if self.at > 64 * 1024 && self.at * 2 > self.buffer.len() {
            self.buffer.drain(..self.at);
            self.at = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Decoder, Frame, ProtocolError, VERSION};

    fn encoded(frame: &Frame) -> Vec<u8> {
        let mut out = Vec::new();
        frame.write(&mut out);
        out
    }

    #[test]
    fn writes_each_frame_as_the_wire_format_lays_it_out() {
        let data = Frame::Data {
            stream: 3,
            payload: b"hi".to_vec(),
            compressed: false,
        };
        assert_eq!(
            encoded(&data),
            [VERSION, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 2, b'h', b'i']
        );
        assert_eq!(
            encoded(&Frame::WindowUpdate {
                stream: 5,
                credit: 256,
            }),
            [VERSION, 1, 0, 0, 0, 0, 0, 5, 0, 0, 1, 0]
        );
        assert_eq!(
            encoded(&Frame::Ping),
            [VERSION, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            encoded(&Frame::Pong),
            [VERSION, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            encoded(&Frame::GoAway),
            [VERSION, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            encoded(&Frame::Data {
                stream: 1,
                payload: b"z".to_vec(),
                compressed: true,
            })[..4],
            [VERSION, 0, 0, 0x10]
        );
    }

    #[test]
    fn reads_frames_out_of_bytes_that_arrive_in_pieces() {
        let frames = [
            Frame::Data {
                stream: 3,
                payload: vec![7; 300],
                compressed: false,
            },
            Frame::WindowUpdate {
                stream: 3,
                credit: 9,
            },
            Frame::Ping,
            Frame::Pong,
            Frame::GoAway,
        ];
        let mut written = Vec::new();
        for frame in &frames {
            frame.write(&mut written);
        }

        let mut decoder = Decoder::new(1024);
        let mut read = Vec::new();
        for chunk in written.chunks(7) {
            decoder.feed(chunk);
            while let Some(frame) = decoder.frame().unwrap() {
                read.push(frame);
            }
        }
        assert_eq!(read, frames);
    }

    #[test]
    fn refuses_what_the_protocol_does_not_allow() {
        let refused = |bytes: &[u8]| {
            let mut decoder = Decoder::new(16);
            decoder.feed(bytes);
            decoder.frame().unwrap_err()
        };
        assert_eq!(
            refused(&[9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            ProtocolError::new("unknown frame version 9")
        );
        assert_eq!(
            refused(&[VERSION, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]),
            ProtocolError::new("malformed window update")
        );
        assert_eq!(
            refused(&[VERSION, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 17]),
            ProtocolError::new("frame of 17 bytes exceeds 16")
        );
        assert_eq!(
            refused(&[VERSION, 7, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]),
            ProtocolError::new("unknown frame type 7 with flags 0x0")
        );
    }

    #[test]
    fn a_frame_that_has_not_arrived_whole_waits_for_the_rest() {
        let mut decoder = Decoder::new(1024);
        decoder.feed(
            &encoded(&Frame::Data {
                stream: 3,
                payload: vec![1, 2, 3],
                compressed: false,
            })[..13],
        );
        assert_eq!(decoder.frame(), Ok(None));
        decoder.feed(&[2, 3]);
        assert_eq!(
            decoder.frame(),
            Ok(Some(Frame::Data {
                stream: 3,
                payload: vec![1, 2, 3],
                compressed: false,
            }))
        );
    }
}
