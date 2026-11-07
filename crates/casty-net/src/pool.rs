//! Connections to other nodes, one per pair, opened by the first side that sends.
//!
//! An envelope for a `NodeId` reaches only that incarnation: it is dropped when another incarnation answers at the
//! node's address, and a node without an address is reachable only through a connection it opened. An envelope for a
//! seed reaches whichever incarnation answers. Dial failures drop the queued envelopes, and the next dial to the same
//! address waits with exponential backoff and jitter.
//!
//! When two nodes dial each other at once, both keep the connection opened by the smaller incarnation: that node
//! rejects the other hello as duplicate, and the other node hands the queue of its dial to the connection it accepts.
//!
//! What two nodes exchange is a session, which outlives the connection that carries it. A connection that breaks
//! without a goodbye leaves its session waiting: the side that knows the address of the other dials it again at once,
//! and a hello that names the session takes it up where it stopped, so a reconnection loses nothing that was in
//! flight. A session ends as a connection used to, with the envelopes it holds: the peer said goodbye or broke the
//! protocol, the dial that would take it up failed or reached another incarnation, the peer came back without it, the
//! cluster gave the peer up (`forsake`), or the peer has no address and only it could dial again.

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use casty_core::node::NodeId;
use casty_core::rolls::Rolls;
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};

use crate::connection::{Broken, Bytes, Connection, Fence, Greeting, Incoming, Session, Socket};
use crate::handshake::{Hello, Message, Resume, answer};
use crate::limits::Limits;
use crate::mux::Unsent;
use crate::tls::Identity;

/// Where an envelope goes: a node of a known incarnation, or whichever one answers at an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Node(NodeId),
    Seed(String),
}

impl Target {
    #[must_use]
    pub fn address(&self) -> Option<&str> {
        match self {
            Self::Node(node) => node.address.as_deref(),
            Self::Seed(address) => Some(address),
        }
    }
}

type Queued = (Target, String, u64, Option<u64>, Vec<u8>);

/// What turns an advertised address into the one that is dialed: a tunnel, a NAT, or a proxy of a test. It answers
/// with a future, so a map that has to ask someone else, like the event loop of a binding, holds no thread while it
/// waits. One that does not answer within `Limits::mapping` leaves the address as it was, for that dial only.
pub type AddressMap = Arc<dyn Fn(&str) -> Dialing + Send + Sync>;

/// The address a dial goes to, once the map has said.
pub type Dialing = Pin<Box<dyn Future<Output = String> + Send>>;

/// What the pool tells of a peer while this node is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Peer {
    /// The connection to it ended.
    Lost(NodeId),
    /// Envelopes for it were dropped: a dial to its address failed, or another incarnation answers there.
    Unreached(NodeId),
}

/// What hears of peers, called from the task that saw what happened to them.
pub type Heard = Arc<dyn Fn(Peer) + Send + Sync>;

/// How a peer is reached, and which side opened it.
#[derive(Debug)]
struct Link {
    connection: Arc<Connection>,
    initiated: bool,
}

#[derive(Debug)]
struct Dial {
    queue: Vec<Queued>,
    cancel: Arc<Notify>,
}

#[derive(Debug, Clone, Copy)]
struct Backoff {
    delay: Duration,
    retry_at: Instant,
}

impl State {
    /// A dial to `address` for a session waiting on it, unless one is on its way already.
    fn redial(&mut self, address: &str) -> Option<(String, Arc<Notify>)> {
        if self.closed || self.dials.contains_key(address) {
            return None;
        }
        let cancel = Arc::new(Notify::new());
        self.dials.insert(
            address.to_owned(),
            Dial {
                queue: Vec::new(),
                cancel: Arc::clone(&cancel),
            },
        );
        Some((address.to_owned(), cancel))
    }
}

#[derive(Debug, Default)]
struct State {
    links: HashMap<NodeId, Link>,
    /// The session with each peer, whether a connection carries it now or it waits for one.
    sessions: HashMap<NodeId, Arc<Session>>,
    addresses: HashMap<String, Arc<Connection>>,
    dials: HashMap<String, Dial>,
    backoff: HashMap<String, Backoff>,
    closed: bool,
}

/// What the pool needs to open a connection and to answer one.
#[derive(Clone)]
pub struct Settings {
    pub local: Hello,
    pub limits: Limits,
    pub min_compressed: usize,
    pub tls: Option<Identity>,
    pub address_map: Option<AddressMap>,
    pub heard: Option<Heard>,
}

impl core::fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Settings")
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

/// What a transport holds and carried, read at one moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Traffic {
    /// Connections to peers that are open now.
    pub connections: usize,
    /// Bytes written to the sockets of every connection since the transport started, handshakes included.
    pub sent: u64,
    /// Bytes read from them.
    pub received: u64,
}

#[derive(Debug)]
pub struct Pool {
    settings: Settings,
    inbound: mpsc::UnboundedSender<Incoming>,
    state: Mutex<State>,
    /// Woken when the pool closes, which is what ends a handshake that is still in the air.
    closing: Arc<Notify>,
    /// What every connection of the pool counts its bytes in.
    bytes: Arc<Bytes>,
}

impl Pool {
    #[must_use]
    pub fn new(settings: Settings, inbound: mpsc::UnboundedSender<Incoming>) -> Arc<Self> {
        Arc::new(Self {
            settings,
            inbound,
            state: Mutex::new(State::default()),
            closing: Arc::new(Notify::new()),
            bytes: Arc::new(Bytes::default()),
        })
    }

    /// The connections open now, and the bytes every connection of the pool carried so far.
    #[must_use]
    pub fn traffic(&self) -> Traffic {
        let connections = self
            .held()
            .links
            .values()
            .filter(|link| link.connection.alive())
            .count();
        Traffic {
            connections,
            sent: self.bytes.sent(),
            received: self.bytes.received(),
        }
    }

    /// Queue an envelope for `target`, opening the connection it needs when there is none.
    ///
    /// Looking a peer up and queueing for it happen under one lock: between them, a dial that has just finished
    /// would take the queue with it and leave this envelope on a connection that is about to be dropped.
    pub fn send(
        self: &Arc<Self>,
        target: &Target,
        name: &str,
        order: u64,
        tag: Option<u64>,
        payload: &[u8],
    ) {
        enum Next {
            Send(Arc<Connection>),
            Waiting(Arc<Session>, Option<(String, Arc<Notify>)>),
            Dial(String, Arc<Notify>),
            Queued,
            Drop,
            Replaced(NodeId),
        }
        let next = {
            let mut state = self.held();
            let held = match target {
                Target::Node(node) => state
                    .links
                    .get(node)
                    .map(|link| Arc::clone(&link.connection)),
                Target::Seed(address) => state.addresses.get(address).map(Arc::clone),
            };
            let waiting = match target {
                Target::Node(node) => state.sessions.get(node).map(Arc::clone),
                Target::Seed(_) => None,
            };
            match (held, target) {
                (Some(connection), _) => Next::Send(connection),
                // The session waits for a connection to take it up, which a dial to the peer brings.
                (None, Target::Node(node)) if let Some(session) = waiting => {
                    let dial = node
                        .address
                        .as_deref()
                        .and_then(|address| state.redial(address));
                    Next::Waiting(session, dial)
                }
                // Another incarnation answers at the address of the node, so this envelope has nowhere to go.
                (None, Target::Node(node))
                    if node
                        .address
                        .as_ref()
                        .is_some_and(|address| state.addresses.contains_key(address)) =>
                {
                    Next::Replaced(node.clone())
                }
                (None, _) => match target.address() {
                    None => Next::Drop,
                    Some(_) if state.closed => Next::Drop,
                    Some(address) => {
                        let queued = (
                            target.clone(),
                            name.to_owned(),
                            order,
                            tag,
                            payload.to_vec(),
                        );
                        if let Some(dial) = state.dials.get_mut(address) {
                            dial.queue.push(queued);
                            Next::Queued
                        } else {
                            let cancel = Arc::new(Notify::new());
                            state.dials.insert(
                                address.to_owned(),
                                Dial {
                                    queue: vec![queued],
                                    cancel: Arc::clone(&cancel),
                                },
                            );
                            Next::Dial(address.to_owned(), cancel)
                        }
                    }
                },
            }
        };
        match next {
            Next::Send(connection) => connection.send(name, order, tag, payload),
            Next::Waiting(session, dial) => {
                session.send(name, order, tag, payload);
                if let Some((address, cancel)) = dial {
                    self.start(address, cancel);
                }
            }
            Next::Dial(address, cancel) => self.start(address, cancel),
            Next::Queued | Next::Drop => {}
            Next::Replaced(node) => self.tell(Peer::Unreached(node)),
        }
    }

    /// Give up what has not gone out of the envelopes queued for `target` with `tag`, on its connection or its dial.
    pub fn retract(&self, target: &Target, tag: u64) {
        let connection = {
            let mut state = self.held();
            if let Some(dial) = target
                .address()
                .and_then(|address| state.dials.get_mut(address))
            {
                dial.queue
                    .retain(|(_, _, _, queued, _)| *queued != Some(tag));
            }
            match target {
                Target::Node(node) => state.sessions.get(node).map(Arc::clone),
                Target::Seed(address) => state
                    .addresses
                    .get(address)
                    .map(|connection| Arc::clone(connection.session())),
            }
        };
        if let Some(session) = connection {
            session.retract(tag);
        }
    }

    /// Give up the session with `node`, which the cluster gave up, unless a connection carries it: nothing is waiting
    /// for a dial that would take it up any more, and the connection it had is lost.
    pub fn forsake(&self, node: &NodeId) {
        let session = {
            let mut state = self.held();
            if state.links.contains_key(node) {
                return;
            }
            state.sessions.remove(node)
        };
        if let Some(session) = session {
            let _ = session.discard();
            self.tell(Peer::Lost(node.clone()));
        }
    }

    fn start(self: &Arc<Self>, address: String, cancel: Arc<Notify>) {
        let pool = Arc::clone(self);
        let closing = Arc::clone(&self.closing);
        tokio::spawn(async move {
            // Biased, so that a dial cancelled while it was connecting never goes on to write its hello.
            tokio::select! {
                biased;
                () = cancel.notified() => {}
                () = closing.notified() => {}
                () = pool.dial(address) => {}
            }
        });
    }

    /// Take a connection the listener accepted, and answer its hello.
    pub fn accept(self: &Arc<Self>, socket: Box<dyn Socket>) {
        if self.held().closed {
            return;
        }
        let pool = Arc::clone(self);
        let closing = Arc::clone(&self.closing);
        tokio::spawn(async move {
            let greeting = Greeting::new(
                socket,
                pool.settings.limits,
                pool.settings.min_compressed,
                Arc::clone(&pool.bytes),
            );
            tokio::select! {
                () = pool.greet(greeting) => {}
                () = closing.notified() => {}
            }
        });
    }

    /// Stop dialing and accepting. Connections write what they can before going away, unless `abort`.
    pub fn close(&self, abort: bool) {
        let links = {
            let mut state = self.held();
            state.closed = true;
            state.dials.clear();
            for session in state.sessions.values() {
                let _ = session.discard();
            }
            state.sessions.clear();
            state
                .links
                .values()
                .map(|link| Arc::clone(&link.connection))
                .collect::<Vec<_>>()
        };
        self.closing.notify_waiters();
        for connection in links {
            if abort {
                connection.end();
            } else {
                connection.close();
            }
        }
    }

    /// Whether anything is still on its way out, which an orderly shutdown waits for.
    #[must_use]
    pub fn idle(&self) -> bool {
        let state = self.held();
        state.dials.is_empty() && state.links.values().all(|link| !link.connection.alive())
    }

    fn held(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("the pool lock is never poisoned")
    }

    async fn greet(self: Arc<Self>, mut greeting: Greeting) {
        let heard = tokio::time::timeout(self.settings.limits.handshake, greeting.hear()).await;
        let Ok(Ok(Message::Hello(hello))) = heard else {
            return;
        };
        let mut ack = match answer(&hello, &self.settings.local) {
            Err(reason) => return greeting.reject(&Message::Reject(reason)).await,
            Ok(_) if self.keeps_own(&hello.node) => {
                return greeting.reject(&Message::Duplicate).await;
            }
            Ok(ack) => ack,
        };
        let session = self.held().sessions.get(&hello.node).map(Arc::clone);
        let resumed = match (&hello.resume, &session) {
            (Some(resume), Some(session))
                if resume.peer == self.settings.local.node.incarnation =>
            {
                session
                    .fence()
                    .filter(|fence| session.rewind(fence.turn, &resume.offsets))
            }
            _ => None,
        };
        // The peer came back without the session, or with one this side cannot take up: what it had in flight is lost.
        let unsent = match &session {
            Some(session) if resumed.is_none() => self.abandon(session, false),
            _ => Unsent::default(),
        };
        ack.resumed = resumed.map(|fence| fence.offsets);
        let compression = ack.compression;
        if greeting.say(&Message::Ack(ack)).await.is_err() {
            return;
        }
        greeting.compress(compression);
        let connection = if let (Some(session), Some(fence)) = (session, resumed) {
            // None when another connection took the session up meanwhile; the peer finds this one closed.
            let Some(connection) = greeting.resume(&session, fence.turn, self.inbound.clone())
            else {
                return;
            };
            connection
        } else {
            let connection = greeting.run(hello.node.clone(), self.inbound.clone());
            connection.session().requeue(unsent);
            connection
        };
        self.register(&connection, &hello.node, false, None);
    }

    async fn dial(self: Arc<Self>, address: String) {
        let wait = self
            .held()
            .backoff
            .get(&address)
            .map(|backoff| backoff.retry_at.saturating_duration_since(Instant::now()));
        if let Some(wait) = wait
            && !wait.is_zero()
        {
            tokio::time::sleep(wait).await;
        }
        let waiting = self
            .held()
            .sessions
            .values()
            .find(|session| session.peer().address.as_deref() == Some(address.as_str()))
            .map(Arc::clone);
        let resuming: Option<(Arc<Session>, Fence)> =
            waiting.and_then(|session| session.fence().map(|fence| (session, fence)));
        let hello = Hello {
            resume: resuming.as_ref().map(|(session, fence)| Resume {
                peer: session.peer().incarnation,
                offsets: fence.offsets,
            }),
            ..self.settings.local.clone()
        };
        let Ok((mut greeting, message)) = self.initiate(&address, hello).await else {
            return self.unreached(&address);
        };
        match message {
            Message::Ack(ack) => {
                let peer = ack.node.clone();
                greeting.compress(ack.compression);
                let connection = match (resuming, ack.resumed) {
                    (Some((session, fence)), Some(theirs)) if *session.peer() == peer => {
                        let resumed = session
                            .rewind(fence.turn, &theirs)
                            .then(|| greeting.resume(&session, fence.turn, self.inbound.clone()));
                        let Some(Some(connection)) = resumed else {
                            // The peer took up a session this side could not: dropping the connection makes it give
                            // the session up when it dials again.
                            return self.unreached(&address);
                        };
                        connection
                    }
                    // A peer that answers for a session it was not asked to resume broke the handshake.
                    (_, Some(_)) => return self.unreached(&address),
                    (resuming, None) => {
                        let connection = greeting.run(peer.clone(), self.inbound.clone());
                        if let Some((session, _)) = resuming {
                            // What never left goes to the peer that came back without the session, and nowhere when
                            // another incarnation answers.
                            let same = *session.peer() == peer;
                            let unsent = self.abandon(&session, !same);
                            if same {
                                connection.session().requeue(unsent);
                            }
                        }
                        connection
                    }
                };
                self.register(&connection, &peer, true, Some(&address));
            }
            Message::Duplicate => {
                // The peer keeps the connection it is opening to this node, and accepting it supersedes this dial.
                tokio::time::sleep(self.settings.limits.handshake).await;
                self.fail(&address);
            }
            Message::Reject(reason) => {
                let seeded = self.held().dials.get(&address).is_some_and(|dial| {
                    dial.queue
                        .iter()
                        .any(|(target, ..)| matches!(target, Target::Seed(_)))
                });
                if seeded {
                    let _ = self.inbound.send(Incoming::Refused(format!(
                        "{address} refused the connection: {reason}"
                    )));
                }
                self.unreached(&address);
            }
            Message::Hello(_) => self.unreached(&address),
        }
    }

    async fn initiate(&self, address: &str, hello: Hello) -> Result<(Greeting, Message), Broken> {
        let dialed = match &self.settings.address_map {
            None => address.to_owned(),
            Some(map) => tokio::time::timeout(self.settings.limits.mapping, map(address))
                .await
                .unwrap_or_else(|_| address.to_owned()),
        };
        let socket = tokio::time::timeout(self.settings.limits.dial, TcpStream::connect(&dialed))
            .await
            .map_err(|_| Broken::Timeout)??;
        socket.set_nodelay(true).ok();
        let socket: Box<dyn Socket> = match &self.settings.tls {
            None => Box::new(socket),
            Some(identity) => {
                let name = rustls::pki_types::ServerName::try_from("casty")
                    .expect("a fixed name is always valid")
                    .to_owned();
                let connector = tokio_rustls::TlsConnector::from(Arc::clone(&identity.client));
                let stream = tokio::time::timeout(
                    self.settings.limits.handshake,
                    connector.connect(name, socket),
                )
                .await
                .map_err(|_| Broken::Timeout)??;
                Box::new(stream)
            }
        };
        let mut greeting = Greeting::new(
            socket,
            self.settings.limits,
            self.settings.min_compressed,
            Arc::clone(&self.bytes),
        );
        let said = async {
            greeting.say(&Message::Hello(hello)).await?;
            greeting.hear().await
        };
        let heard = tokio::time::timeout(self.settings.limits.handshake, said)
            .await
            .map_err(|_| Broken::Timeout)??;
        Ok((greeting, heard))
    }

    /// Whether a connection this node opened, or is opening, to `peer` wins over the one `peer` opened.
    fn keeps_own(&self, peer: &NodeId) -> bool {
        if self.settings.local.node.incarnation >= peer.incarnation {
            return false;
        }
        let state = self.held();
        if let Some(link) = state.links.get(peer) {
            return link.initiated;
        }
        let Some(address) = &peer.address else {
            return false;
        };
        if !state.dials.contains_key(address) {
            return false;
        }
        state
            .backoff
            .get(address)
            .is_none_or(|backoff| backoff.retry_at <= Instant::now())
    }

    /// Put `connection` in the place of whatever reached `peer` until now, and hand it what was queued for it.
    ///
    /// The dial this connection finishes, and the one it supersedes, are taken in the same lock that registers it,
    /// so that nothing queues for a dial that is already over.
    fn register(
        self: &Arc<Self>,
        connection: &Arc<Connection>,
        peer: &NodeId,
        initiated: bool,
        dialed: Option<&str>,
    ) {
        let (previous, replaced) = {
            let mut state = self.held();
            if state.closed {
                connection.end();
                return;
            }
            let mut queue = Vec::new();
            for address in [peer.address.as_deref(), dialed].into_iter().flatten() {
                if let Some(dial) = state.dials.remove(address) {
                    // A permit rather than a wakeup: the task of the dial may not be waiting for it yet.
                    dial.cancel.notify_one();
                    queue.extend(dial.queue);
                }
                state.backoff.remove(address);
                state
                    .addresses
                    .insert(address.to_owned(), Arc::clone(connection));
            }
            let previous = state.links.insert(
                peer.clone(),
                Link {
                    connection: Arc::clone(connection),
                    initiated,
                },
            );
            state
                .sessions
                .insert(peer.clone(), Arc::clone(connection.session()));
            // Queued under the same lock that made the connection reachable: a send that arrives in between would
            // otherwise go out ahead of what was waiting for the dial to finish.
            let mut replaced = BTreeSet::new();
            for (target, name, order, tag, payload) in queue {
                match target {
                    Target::Node(node) if node != *peer => {
                        replaced.insert(node);
                    }
                    _ => connection.send(&name, order, tag, &payload),
                }
            }
            (previous, replaced)
        };
        if let Some(previous) = previous {
            previous.connection.end();
        }
        for node in replaced {
            self.tell(Peer::Unreached(node));
        }
        let pool = Arc::clone(self);
        let watched = Arc::clone(connection);
        let peer = peer.clone();
        tokio::spawn(async move {
            watched.over().await;
            pool.forget(&peer, &watched);
        });
    }

    fn forget(self: &Arc<Self>, peer: &NodeId, connection: &Arc<Connection>) {
        enum Next {
            Nothing,
            Dial(String, Arc<Notify>),
            Lost,
        }
        let next = {
            let mut state = self.held();
            let current = state
                .links
                .get(peer)
                .is_some_and(|link| Arc::ptr_eq(&link.connection, connection));
            if current {
                state.links.remove(peer);
            }
            state
                .addresses
                .retain(|_, held| !Arc::ptr_eq(held, connection));
            let session = connection.session();
            let held = state
                .sessions
                .get(peer)
                .is_some_and(|held| Arc::ptr_eq(held, session));
            // A connection another one replaced reaches the peer still, one whose session another connection is
            // taking up hands it over, and one this node is closing is not lost.
            if !current || !held || connection.superseded() || state.closed {
                Next::Nothing
            } else if connection.broken()
                && let Some(address) = &peer.address
            {
                state
                    .redial(address)
                    .map_or(Next::Nothing, |(address, cancel)| {
                        Next::Dial(address, cancel)
                    })
            } else {
                state.sessions.remove(peer);
                let _ = session.discard();
                Next::Lost
            }
        };
        match next {
            Next::Nothing => {}
            Next::Dial(address, cancel) => self.start(address, cancel),
            Next::Lost => self.tell(Peer::Lost(peer.clone())),
        }
    }

    /// Give up `session`, which no connection will take up: what it had in flight is lost, and with `unreached`, another
    /// incarnation answers where its peer was. What it never started is given back.
    fn abandon(&self, session: &Arc<Session>, unreached: bool) -> Unsent {
        let peer = session.peer().clone();
        {
            let mut state = self.held();
            if state
                .sessions
                .get(&peer)
                .is_some_and(|held| Arc::ptr_eq(held, session))
            {
                state.sessions.remove(&peer);
            }
        }
        let unsent = session.discard();
        self.tell(Peer::Lost(peer.clone()));
        if unreached {
            self.tell(Peer::Unreached(peer));
        }
        unsent
    }

    fn tell(&self, peer: Peer) {
        if let Some(heard) = &self.settings.heard {
            heard(peer);
        }
    }

    /// Give up a dial that reached no node, and tell of the nodes whose envelopes it drops.
    fn unreached(&self, address: &str) {
        let mut dropped = self.fail(address);
        let waiting: Vec<Arc<Session>> = {
            let mut state = self.held();
            let nodes: Vec<NodeId> = state
                .sessions
                .keys()
                .filter(|node| {
                    node.address.as_deref() == Some(address) && !state.links.contains_key(*node)
                })
                .cloned()
                .collect();
            nodes
                .iter()
                .filter_map(|node| state.sessions.remove(node))
                .collect()
        };
        for session in waiting {
            let _ = session.discard();
            self.tell(Peer::Lost(session.peer().clone()));
            dropped.insert(session.peer().clone());
        }
        for node in dropped {
            self.tell(Peer::Unreached(node));
        }
    }

    /// Give up a dial, and answer the nodes its queue held envelopes for.
    fn fail(&self, address: &str) -> BTreeSet<NodeId> {
        let mut state = self.held();
        let dropped = state
            .dials
            .remove(address)
            .map_or_else(BTreeSet::new, |dial| {
                dial.queue
                    .into_iter()
                    .filter_map(|(target, ..)| match target {
                        Target::Node(node) => Some(node),
                        Target::Seed(_) => None,
                    })
                    .collect()
            });
        let previous = state.backoff.get(address).copied();
        let redial = self.settings.limits.redial;
        let delay = previous.map_or(redial.first, |backoff| redial.next(backoff.delay));
        // A point of `-1..=1`, in thousandths, so that nodes that lost a peer at once do not all dial it again at once.
        let drawn = u32::try_from(Rolls::fresh().between(0, 2000)).expect("at most 2000");
        let spread = 1.0 + self.settings.limits.redial_jitter * (f64::from(drawn) / 1000.0 - 1.0);
        state.backoff.insert(
            address.to_owned(),
            Backoff {
                delay,
                retry_at: Instant::now() + delay.mul_f64(spread),
            },
        );
        dropped
    }
}
