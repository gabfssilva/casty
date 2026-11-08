//! One connection to a peer: a handshake on the control stream, then envelopes on the others.
//!
//! Reading and writing are separate tasks over the two halves of the socket, and the mux between them is what holds
//! the credit of each stream. A single task would deadlock: a write that the peer is not draining would stop this
//! side from draining what the peer is writing.
//!
//! The mux belongs to the `Session` with the peer, which one connection carries after another. A connection that
//! takes a session up fences the one before it in the same lock it reads the offsets of the mux in: from then on the
//! old one neither reads into the mux nor writes out of it, so bytes it still delivers late are dropped, and nothing
//! counted in the offsets is received again.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use casty_core::node::NodeId;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, watch};
use tokio::time::Instant;

use crate::compress::Name;
use crate::frame::{Decoder, Frame, GOODBYE, ProtocolError};
use crate::handshake::{self, Message};
use crate::limits::Limits;
use crate::mux::{
    CONTROL, MEMBERSHIP, MESSAGES, Mux, Offset, REPLICATION, RESUMED, Record, Unsent,
};

/// A socket, with or without TLS on top of it.
pub trait Socket: AsyncRead + AsyncWrite + Unpin + Send + core::fmt::Debug + 'static {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + core::fmt::Debug + 'static> Socket for T {}

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
pub struct Greeting {
    socket: Box<dyn Socket>,
    mux: Mux,
    frames: Decoder,
    limits: Limits,
    /// What the transport counts the bytes of this socket in, from the handshake on.
    bytes: Arc<Bytes>,
    /// What the mux reads its clock against.
    epoch: Instant,
}

impl Greeting {
    #[must_use]
    pub fn new(
        socket: Box<dyn Socket>,
        limits: Limits,
        min_compressed: usize,
        bytes: Arc<Bytes>,
    ) -> Self {
        Self {
            socket,
            mux: Mux::new(limits, min_compressed),
            frames: Decoder::new(limits.frame),
            limits,
            bytes,
            epoch: Instant::now(),
        }
    }

    /// Write a handshake message, ahead of any envelope queued after it.
    pub async fn say(&mut self, message: &Message) -> Result<(), Broken> {
        let (name, payload) = handshake::encode(message);
        self.mux.send(CONTROL, 0, None, name, &payload);
        let out = self.mux.output(seconds(self.epoch));
        self.socket.write_all(&out).await?;
        self.bytes.wrote(out.len());
        Ok(())
    }

    /// Write a rejection and go away, which is the last thing this connection does.
    pub async fn reject(mut self, message: &Message) {
        let _ = self.say(message).await;
        let _ = self.socket.write_all(&GOODBYE).await;
        let _ = self.socket.shutdown().await;
    }

    /// The next handshake message from the peer. Frames after it stay buffered for the connection.
    pub async fn hear(&mut self) -> Result<Message, Broken> {
        let mut buffer = vec![0_u8; self.limits.read_buffer];
        loop {
            while let Some(frame) = self.frames.frame()? {
                if matches!(frame, Frame::GoAway) {
                    return Err(Broken::Closed);
                }
                // A handshake message is one record, and whatever came with it is the peer breaking the order.
                if let Some(record) = self
                    .mux
                    .receive(frame, seconds(self.epoch))?
                    .into_iter()
                    .next()
                {
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

    /// Start exchanging envelopes with `peer`, the node the handshake named, in a session of their own, giving back the
    /// handle the pool keeps.
    #[must_use]
    pub fn run(self, peer: NodeId, inbound: mpsc::UnboundedSender<Incoming>) -> Arc<Connection> {
        let Self {
            socket,
            mux,
            frames,
            limits,
            bytes,
            epoch,
        } = self;
        let writable = Arc::new(Notify::new());
        let session = Arc::new(Session {
            peer,
            held: Mutex::new(Carried {
                mux,
                turn: 0,
                writable: Arc::clone(&writable),
                over: false,
                theirs: 0,
            }),
            discarded: watch::channel(false).0,
            epoch,
        });
        Self::start(socket, frames, limits, bytes, session, 0, writable, inbound)
    }

    /// Carry `session` from here on, which `Session::rewind` prepared under `turn`, unless another connection took it
    /// up since or it was given up.
    #[must_use]
    pub fn resume(
        self,
        session: &Arc<Session>,
        turn: u64,
        inbound: mpsc::UnboundedSender<Incoming>,
    ) -> Option<Arc<Connection>> {
        let writable = Arc::new(Notify::new());
        {
            let mut carried = session.carried(turn)?;
            carried.mux.compressor = self.mux.compressor;
            carried.writable = Arc::clone(&writable);
        }
        let Self {
            socket,
            frames,
            limits,
            bytes,
            ..
        } = self;
        Some(Self::start(
            socket,
            frames,
            limits,
            bytes,
            Arc::clone(session),
            turn,
            writable,
            inbound,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn start(
        socket: Box<dyn Socket>,
        frames: Decoder,
        limits: Limits,
        bytes: Arc<Bytes>,
        session: Arc<Session>,
        turn: u64,
        writable: Arc<Notify>,
        inbound: mpsc::UnboundedSender<Incoming>,
    ) -> Arc<Connection> {
        let connection = Arc::new(Connection {
            session,
            turn,
            writable,
            closing: AtomicBool::new(false),
            broke: AtomicBool::new(false),
            over: watch::channel(false).0,
            bytes,
            read: AtomicBool::new(false),
        });
        let (reader, writer) = tokio::io::split(socket);
        let writing = Arc::clone(&connection);
        tokio::spawn(async move { writing.write(writer).await });
        let reading = Arc::clone(&connection);
        let handle = Arc::clone(&connection);
        tokio::spawn(async move {
            match reading
                .read(reader, frames, limits, &inbound, &handle)
                .await
            {
                Ok(Ending::Broke) | Err(Broken::Io(_) | Broken::Timeout) => reading.broke(),
                Ok(Ending::Ended) | Err(Broken::Protocol(_) | Broken::Closed) => reading.end(),
            }
        });
        connection
    }
}

/// What a node and one peer exchange, carried by one connection after another while both are the incarnations they
/// were: the envelopes queued, what was sent and not yet credited, what arrived of envelopes not yet complete, and the
/// measures of the link.
#[derive(Debug)]
pub struct Session {
    peer: NodeId,
    held: Mutex<Carried>,
    /// Set once the session is given up.
    discarded: watch::Sender<bool>,
    /// What the mux reads its clock against, for every connection that carries it.
    epoch: Instant,
}

#[derive(Debug)]
struct Carried {
    mux: Mux,
    /// Taken anew by each connection that takes the session up: one that carried it before reads and writes nothing
    /// more.
    turn: u64,
    /// What wakes the writer of the connection that carries the session now.
    writable: Arc<Notify>,
    /// Given up: no connection takes it up again.
    over: bool,
    /// The newest turn the peer took the session up at, as far as this side has seen: a hello fenced at it or before is
    /// one the peer sent before it took up a connection this side holds.
    theirs: u64,
}

/// Where a session stood on this side when a connection took it up.
#[derive(Debug, Clone, Copy)]
pub struct Fence {
    pub turn: u64,
    pub offsets: [Offset; RESUMED],
}

impl Session {
    /// The node at the other end.
    #[must_use]
    pub fn peer(&self) -> &NodeId {
        &self.peer
    }

    /// Queue an envelope. It goes out as the peer's credit allows, on whichever connection carries the session then,
    /// and is lost if the session is given up first. Envelopes of one `order` arrive in the order they were queued;
    /// the others may overtake each other. One with a `tag` can be given up with `retract` until it has gone out whole.
    pub fn send(&self, name: &str, order: u64, tag: Option<u64>, payload: &[u8]) {
        let stream = match name {
            "replication" => REPLICATION,
            "membership" => MEMBERSHIP,
            _ => MESSAGES,
        };
        let mut carried = self.held();
        carried.mux.send(stream, order, tag, name, payload);
        carried.writable.notify_one();
    }

    /// Give up what has not gone out of the envelopes queued with `tag`.
    pub fn retract(&self, tag: u64) {
        let mut carried = self.held();
        carried.mux.retract(tag);
        carried.writable.notify_one();
    }

    /// Return the credit of a received envelope, once it has been handed over.
    pub fn consume(&self, record: &Record) {
        let mut carried = self.held();
        carried.mux.consume(record);
        carried.writable.notify_one();
    }

    /// Fence whatever connection carries the session, and say where it stands on this side, unless it was given up.
    pub fn fence(&self) -> Option<Fence> {
        let mut carried = self.held();
        if carried.over {
            return None;
        }
        carried.turn += 1;
        Some(Fence {
            turn: carried.turn,
            offsets: carried.mux.offsets(),
        })
    }

    /// Send again what the peer did not receive, from where it says each stream stands as of `their_turn`, the turn it
    /// took the session up at. False when the session was given up or fenced again since `turn`, or when the peer
    /// stands where this side never sent to.
    pub fn rewind(&self, turn: u64, their_turn: u64, theirs: &[Offset; RESUMED]) -> bool {
        self.carried(turn).is_some_and(|mut carried| {
            let resumed = carried.mux.resume(theirs).is_ok();
            if resumed {
                carried.theirs = carried.theirs.max(their_turn);
            }
            resumed
        })
    }

    /// Whether a hello that took the session up at `turn` was sent before the peer took up a connection this side
    /// holds: two nodes that dialed each other at once, and the later dial arrived after the earlier one won.
    #[must_use]
    pub fn stale(&self, turn: u64) -> bool {
        turn <= self.held().theirs
    }

    /// No connection takes the session up again, and the one that carries it, if any, reads and writes nothing more.
    /// What it had queued and never started is given back.
    pub fn discard(&self) -> Unsent {
        let mut carried = self.held();
        carried.over = true;
        carried.turn += 1;
        carried.writable.notify_one();
        self.discarded.send_replace(true);
        carried.mux.unsent()
    }

    /// Return once the session is given up, at once if it already is.
    pub async fn discarded(&self) {
        let _ = self.discarded.subscribe().wait_for(|over| *over).await;
    }

    /// Queue what a session given up with the same peer never started, ahead of what this one queued since.
    pub fn requeue(&self, unsent: Unsent) {
        let mut carried = self.held();
        carried.mux.requeue(unsent);
        carried.writable.notify_one();
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Carried> {
        self.held
            .lock()
            .expect("the session lock is never poisoned")
    }

    /// The session, if `turn` is the connection that carries it now.
    fn carried(&self, turn: u64) -> Option<std::sync::MutexGuard<'_, Carried>> {
        let carried = self.held();
        (carried.turn == turn && !carried.over).then_some(carried)
    }
}

/// How the reading side of a connection ended when it was not by an error: the peer said goodbye or this node let the
/// connection go, or the socket closed without a goodbye.
enum Ending {
    Ended,
    Broke,
}

/// A connection that is exchanging envelopes.
#[derive(Debug)]
pub struct Connection {
    session: Arc<Session>,
    turn: u64,
    writable: Arc<Notify>,
    closing: AtomicBool,
    /// It ended without the peer saying goodbye, which is what a session is taken up again after.
    broke: AtomicBool,
    over: watch::Sender<bool>,
    bytes: Arc<Bytes>,
    /// Whether anything was read from the peer since `heard` was last asked.
    read: AtomicBool,
}

impl Connection {
    /// The node at the other end.
    #[must_use]
    pub fn peer(&self) -> &NodeId {
        &self.session.peer
    }

    /// The session this connection carries, or carried.
    #[must_use]
    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }

    /// Whether anything arrived from the peer since the last time this was asked: bytes of an envelope still on its
    /// way show the peer alive as well as a whole one does.
    #[must_use]
    pub fn heard(&self) -> bool {
        self.read.swap(false, Ordering::Relaxed)
    }

    /// Whether the connection ended without a goodbye: the socket closed or failed, or the peer stopped answering.
    #[must_use]
    pub fn broken(&self) -> bool {
        self.broke.load(Ordering::SeqCst)
    }

    /// Whether another connection took the session up since this one did, or the session was given up.
    #[must_use]
    pub fn superseded(&self) -> bool {
        self.session.carried(self.turn).is_none()
    }

    /// Queue an envelope on the session, as `Session::send` does.
    pub fn send(&self, name: &str, order: u64, tag: Option<u64>, payload: &[u8]) {
        self.session.send(name, order, tag, payload);
    }

    /// Return the credit of a received envelope, once it has been handed over.
    pub fn consume(&self, record: &Record) {
        self.session.consume(record);
    }

    /// Write what the peer's credit allows, then go away.
    pub fn close(&self) {
        self.closing.store(true, Ordering::SeqCst);
        self.writable.notify_one();
    }

    /// End the connection now, without a goodbye, as one that broke: its session waits to be taken up.
    fn broke(&self) {
        self.broke.store(true, Ordering::SeqCst);
        self.end();
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

    fn seconds(&self) -> f64 {
        self.session.epoch.elapsed().as_secs_f64()
    }

    async fn write(&self, mut writer: impl AsyncWrite + Unpin + Send) {
        loop {
            if !self.alive() {
                return;
            }
            loop {
                let output = self
                    .session
                    .carried(self.turn)
                    .map(|mut carried| carried.mux.output(self.seconds()));
                let Some(out) = output else {
                    return self.end();
                };
                if out.is_empty() {
                    break;
                }
                let written = tokio::select! {
                    written = writer.write_all(&out) => written,
                    () = self.over() => return,
                };
                if written.is_err() {
                    self.broke();
                    return;
                }
                self.bytes.wrote(out.len());
            }
            // What the pace holds back goes out when it allows, without anything arriving to wake the writer.
            let Some(wake) = self
                .session
                .carried(self.turn)
                .map(|carried| carried.mux.wake())
            else {
                return self.end();
            };
            if wake.is_none() && self.closing.load(Ordering::SeqCst) {
                let _ = writer.write_all(&GOODBYE).await;
                let _ = writer.shutdown().await;
                return;
            }
            let paced = async {
                match wake {
                    Some(at) => {
                        tokio::time::sleep_until(self.session.epoch + Duration::from_secs_f64(at))
                            .await;
                    }
                    None => core::future::pending().await,
                }
            };
            tokio::select! {
                () = self.writable.notified() => {}
                () = paced => {}
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
    ) -> Result<Ending, Broken> {
        let mut buffer = vec![0_u8; limits.read_buffer];
        loop {
            loop {
                let frame = {
                    let next = frames.frame()?;
                    match next {
                        None => break,
                        Some(Frame::GoAway) => return Ok(Ending::Ended),
                        Some(frame) => frame,
                    }
                };
                // Counted and handed over in one lock: a connection that takes the session up meanwhile counts what
                // this one read, and the envelopes reach the node in the order they were sent.
                let Some(mut carried) = self.session.carried(self.turn) else {
                    return Ok(Ending::Ended);
                };
                let records = carried.mux.receive(frame, self.seconds())?;
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
                        return Ok(Ending::Ended);
                    }
                }
            }
            self.writable.notify_one();
            let read = tokio::select! {
                read = self.receive(&mut reader, &mut buffer, limits) => read?,
                () = self.over() => return Ok(Ending::Ended),
            };
            if read == 0 {
                return Ok(Ending::Broke);
            }
            self.bytes.read(read);
            self.read.store(true, Ordering::Relaxed);
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
        if let Some(mut carried) = self.session.carried(self.turn) {
            carried.mux.ping();
        }
        self.writable.notify_one();
        match tokio::time::timeout(limits.keepalive_timeout, reader.read(buffer)).await {
            Ok(read) => Ok(read?),
            Err(_) => Err(Broken::Timeout),
        }
    }
}

fn seconds(epoch: Instant) -> f64 {
    epoch.elapsed().as_secs_f64()
}
