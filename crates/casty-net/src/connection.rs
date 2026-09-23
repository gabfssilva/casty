//! One connection to a peer: a handshake on the control stream, then envelopes on the other two.
//!
//! Reading and writing are separate tasks over the two halves of the socket, and the mux between them is what holds
//! the credit of each stream. A single task would deadlock: a write that the peer is not draining would stop this
//! side from draining what the peer is writing.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use casty_core::node::NodeId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, watch};

use crate::compress::Name;
use crate::frame::{Decoder, Frame, ProtocolError};
use crate::handshake::{self, Message};
use crate::limits::Limits;
use crate::mux::{CONTROL, MESSAGES, Mux, REPLICATION, Record};

const CHUNK: usize = 256 * 1024;

/// A socket, with or without TLS on top of it.
pub trait Socket: AsyncRead + AsyncWrite + Unpin + Send + 'static {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> Socket for T {}

/// What went wrong on a connection. All of them end it.
#[derive(Debug)]
pub enum Broken {
    Io(io::Error),
    Protocol(ProtocolError),
    Closed,
    Timeout,
}

impl From<io::Error> for Broken {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for Broken {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl core::fmt::Display for Broken {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "{error}"),
            Self::Protocol(error) => write!(formatter, "{error}"),
            Self::Closed => formatter.write_str("the peer closed the connection"),
            Self::Timeout => formatter.write_str("the peer did not answer in time"),
        }
    }
}

/// The bytes the connections of one transport wrote to their sockets and read from them, since it started.
#[derive(Debug, Default)]
pub struct Bytes {
    sent: AtomicU64,
    received: AtomicU64,
}

impl Bytes {
    #[must_use]
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }

    fn wrote(&self, count: usize) {
        self.sent.fetch_add(count as u64, Ordering::Relaxed);
    }

    fn read(&self, count: usize) {
        self.received.fetch_add(count as u64, Ordering::Relaxed);
    }
}

/// An envelope that arrived, with the connection whose credit it holds until it is taken.
#[derive(Debug)]
pub struct Arrival {
    pub record: Record,
    pub from: Arc<Connection>,
}

/// What reaches the node from the wire.
#[derive(Debug)]
pub enum Incoming {
    /// An envelope this node sent to itself, which never touches a socket.
    Local {
        name: String,
        payload: Vec<u8>,
    },
    Remote(Arrival),
    /// A seed that would not have this node, which is what a join fails with.
    Refused(String),
}

/// A socket that has not shaken hands yet, or has just done so.
#[derive(Debug)]
pub struct Greeting<S: Socket> {
    socket: S,
    mux: Mux,
    frames: Decoder,
    limits: Limits,
    /// What the transport counts the bytes of this socket in, from the handshake on.
    bytes: Arc<Bytes>,
}

impl<S: Socket> Greeting<S> {
    #[must_use]
    pub fn new(socket: S, limits: Limits, min_compressed: usize, bytes: Arc<Bytes>) -> Self {
        Self {
            socket,
            mux: Mux::new(limits, min_compressed),
            frames: Decoder::new(limits.frame),
            limits,
            bytes,
        }
    }

    /// Write a handshake message, ahead of any envelope queued after it.
    pub async fn say(&mut self, message: &Message) -> Result<(), Broken> {
        let (name, payload) = handshake::encode(message);
        self.mux.send(CONTROL, name, &payload);
        let out = self.mux.output();
        self.socket.write_all(&out).await?;
        self.bytes.wrote(out.len());
        Ok(())
    }

    /// Write a rejection and go away, which is the last thing this connection does.
    pub async fn reject(mut self, message: &Message) {
        let _ = self.say(message).await;
        let _ = self.socket.write_all(&Frame::GoAway.encoded()).await;
        let _ = self.socket.shutdown().await;
    }

    /// The next handshake message from the peer. Frames after it stay buffered for the connection.
    pub async fn hear(&mut self) -> Result<Message, Broken> {
        let mut buffer = vec![0_u8; CHUNK];
        loop {
            while let Some(frame) = self.frames.frame()? {
                if matches!(frame, Frame::GoAway) {
                    return Err(Broken::Closed);
                }
                // A handshake message is one record, and whatever came with it is the peer breaking the order.
                if let Some(record) = self.mux.receive(frame)?.into_iter().next() {
                    if record.stream != CONTROL {
                        return Err(ProtocolError::new(format!(
                            "envelope on stream {} before the handshake",
                            record.stream
                        ))
                        .into());
                    }
                    self.mux.consume(&record);
                    return Ok(handshake::decode(&record.name, &record.payload)?);
                }
            }
            let read = self.socket.read(&mut buffer).await?;
            if read == 0 {
                return Err(Broken::Closed);
            }
            self.bytes.read(read);
            self.frames.feed(&buffer[..read]);
        }
    }

    pub fn compress(&mut self, name: Option<Name>) {
        self.mux.compressor = name;
    }

    /// Start exchanging envelopes with `peer`, the node the handshake named, giving back the handle the pool keeps.
    #[must_use]
    pub fn run(self, peer: NodeId, inbound: mpsc::UnboundedSender<Incoming>) -> Arc<Connection> {
        let Self {
            socket,
            mux,
            frames,
            limits,
            bytes,
        } = self;
        let connection = Arc::new(Connection {
            peer,
            mux: Mutex::new(mux),
            writable: Notify::new(),
            closing: AtomicBool::new(false),
            over: watch::channel(false).0,
            bytes,
        });
        let (reader, writer) = tokio::io::split(socket);
        let writing = Arc::clone(&connection);
        tokio::spawn(async move { writing.write(writer).await });
        let reading = Arc::clone(&connection);
        let handle = Arc::clone(&connection);
        tokio::spawn(async move {
            let _ = reading
                .read(reader, frames, limits, &inbound, &handle)
                .await;
            reading.end();
        });
        connection
    }
}

/// A connection that is exchanging envelopes.
#[derive(Debug)]
pub struct Connection {
    peer: NodeId,
    mux: Mutex<Mux>,
    writable: Notify,
    closing: AtomicBool,
    over: watch::Sender<bool>,
    bytes: Arc<Bytes>,
}

impl Connection {
    /// The node at the other end.
    #[must_use]
    pub fn peer(&self) -> &NodeId {
        &self.peer
    }

    /// Queue an envelope. It goes out as the peer's credit allows, and is lost if the connection dies first.
    pub fn send(&self, name: &str, payload: &[u8]) {
        let stream = if name == "replication" {
            REPLICATION
        } else {
            MESSAGES
        };
        self.held().send(stream, name, payload);
        self.writable.notify_one();
    }

    /// Return the credit of a received envelope, once it has been handed over.
    pub fn consume(&self, record: &Record) {
        self.held().consume(record);
        self.writable.notify_one();
    }

    /// Write what the peer's credit allows, then go away.
    pub fn close(&self) {
        self.closing.store(true, Ordering::SeqCst);
        self.writable.notify_one();
    }

    /// End the connection now, losing what has not reached the socket.
    pub fn end(&self) {
        // Stored even with nobody subscribed, which is what `alive` reads.
        self.over.send_replace(true);
    }

    #[must_use]
    pub fn alive(&self) -> bool {
        !*self.over.borrow()
    }

    /// Return once the connection is over, however it ended, at once if it already is.
    pub async fn over(&self) {
        let _ = self.over.subscribe().wait_for(|over| *over).await;
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Mux> {
        self.mux
            .lock()
            .expect("the connection lock is never poisoned")
    }

    async fn write(&self, mut writer: impl AsyncWrite + Unpin + Send) {
        loop {
            if !self.alive() {
                return;
            }
            loop {
                let out = self.held().output();
                if out.is_empty() {
                    break;
                }
                let written = tokio::select! {
                    written = writer.write_all(&out) => written,
                    () = self.over() => return,
                };
                if written.is_err() {
                    self.end();
                    return;
                }
                self.bytes.wrote(out.len());
            }
            if self.closing.load(Ordering::SeqCst) {
                let _ = writer.write_all(&Frame::GoAway.encoded()).await;
                let _ = writer.shutdown().await;
                return;
            }
            tokio::select! {
                () = self.writable.notified() => {}
                () = self.over() => return,
            }
        }
    }

    async fn read(
        &self,
        mut reader: impl AsyncRead + Unpin + Send,
        mut frames: Decoder,
        limits: Limits,
        inbound: &mpsc::UnboundedSender<Incoming>,
        handle: &Arc<Connection>,
    ) -> Result<(), Broken> {
        let mut buffer = vec![0_u8; CHUNK];
        loop {
            loop {
                let frame = {
                    let next = frames.frame()?;
                    match next {
                        None => break,
                        Some(Frame::GoAway) => return Ok(()),
                        Some(frame) => frame,
                    }
                };
                let records = self.held().receive(frame)?;
                for record in records {
                    if record.stream == CONTROL {
                        return Err(
                            ProtocolError::new("handshake message after the handshake").into()
                        );
                    }
                    if inbound
                        .send(Incoming::Remote(Arrival {
                            record,
                            from: Arc::clone(handle),
                        }))
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            self.writable.notify_one();
            let read = tokio::select! {
                read = self.receive(&mut reader, &mut buffer, limits) => read?,
                () = self.over() => return Ok(()),
            };
            if read == 0 {
                return Ok(());
            }
            self.bytes.read(read);
            frames.feed(&buffer[..read]);
        }
    }

    /// Read, pinging once the connection has been quiet, and giving up if the ping is not answered.
    async fn receive(
        &self,
        reader: &mut (impl AsyncRead + Unpin + Send),
        buffer: &mut [u8],
        limits: Limits,
    ) -> Result<usize, Broken> {
        if let Ok(read) = tokio::time::timeout(limits.keepalive_after, reader.read(buffer)).await {
            return Ok(read?);
        }
        self.held().ping();
        self.writable.notify_one();
        match tokio::time::timeout(limits.keepalive_timeout, reader.read(buffer)).await {
            Ok(read) => Ok(read?),
            Err(_) => Err(Broken::Timeout),
        }
    }
}
