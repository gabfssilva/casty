//! Connections to other nodes, one per pair, opened by the first side that sends.
//!
//! An envelope for a `NodeId` reaches only that incarnation: it is dropped when another incarnation answers at the
//! node's address, and a node without an address is reachable only through a connection it opened. An envelope for a
//! seed reaches whichever incarnation answers. Dial failures drop the queued envelopes, and the next dial to the same
//! address waits with exponential backoff and jitter.
//!
//! When two nodes dial each other at once, both keep the connection opened by the smaller incarnation: that node
//! rejects the other hello as duplicate, and the other node hands the queue of its dial to the connection it accepts.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use casty_core::node::NodeId;
use casty_core::rolls::Rolls;
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc};

use crate::connection::{Broken, Bytes, Connection, Greeting, Incoming, Socket};
use crate::handshake::{Hello, Message, answer};
use crate::limits::Limits;
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

type Queued = (Target, String, Vec<u8>);

/// What turns an advertised address into the one that is dialed: a tunnel, a NAT, or a proxy of a test.
pub type AddressMap = Arc<dyn Fn(&str) -> String + Send + Sync>;

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

#[derive(Debug, Default)]
struct State {
    links: HashMap<NodeId, Link>,
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
    pub fn send(self: &Arc<Self>, target: &Target, name: &str, payload: &[u8]) {
        enum Next {
            Send(Arc<Connection>),
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
            match (held, target) {
                (Some(connection), _) => Next::Send(connection),
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
                        let queued = (target.clone(), name.to_owned(), payload.to_vec());
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
            Next::Send(connection) => connection.send(name, payload),
            Next::Dial(address, cancel) => self.start(address, cancel),
            Next::Queued | Next::Drop => {}
            Next::Replaced(node) => self.tell(Peer::Unreached(node)),
        }
    }

    fn start(self: &Arc<Self>, address: String, cancel: Arc<Notify>) {
        let pool = Arc::clone(self);
        let closing = Arc::clone(&self.closing);
        tokio::spawn(async move {
            tokio::select! {
                () = pool.dial(address) => {}
                () = cancel.notified() => {}
                () = closing.notified() => {}
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
        let ack = match answer(&hello, &self.settings.local) {
            Err(reason) => return greeting.reject(&Message::Reject(reason)).await,
            Ok(_) if self.keeps_own(&hello.node) => {
                return greeting.reject(&Message::Duplicate).await;
            }
            Ok(ack) => ack,
        };
        let compression = ack.compression;
        if greeting.say(&Message::Ack(ack)).await.is_err() {
            return;
        }
        greeting.compress(compression);
        let connection = greeting.run(hello.node.clone(), self.inbound.clone());
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
        let Ok((mut greeting, message)) = self.initiate(&address).await else {
            return self.unreached(&address);
        };
        match message {
            Message::Ack(ack) => {
                let peer = ack.node.clone();
                greeting.compress(ack.compression);
                let connection = greeting.run(peer.clone(), self.inbound.clone());
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
                        .any(|(target, _, _)| matches!(target, Target::Seed(_)))
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

    async fn initiate(&self, address: &str) -> Result<(Greeting, Message), Broken> {
        let dialed = match &self.settings.address_map {
            None => address.to_owned(),
            Some(map) => map(address),
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
            greeting
                .say(&Message::Hello(self.settings.local.clone()))
                .await?;
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
                    dial.cancel.notify_waiters();
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
            // Queued under the same lock that made the connection reachable: a send that arrives in between would
            // otherwise go out ahead of what was waiting for the dial to finish.
            let mut replaced = BTreeSet::new();
            for (target, name, payload) in queue {
                match target {
                    Target::Node(node) if node != *peer => {
                        replaced.insert(node);
                    }
                    _ => connection.send(&name, &payload),
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

    fn forget(&self, peer: &NodeId, connection: &Arc<Connection>) {
        let lost = {
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
            // A connection another one replaced reaches the peer still, and one this node is closing is not lost.
            current && !state.closed
        };
        if lost {
            self.tell(Peer::Lost(peer.clone()));
        }
    }

    fn tell(&self, peer: Peer) {
        if let Some(heard) = &self.settings.heard {
            heard(peer);
        }
    }

    /// Give up a dial that reached no node, and tell of the nodes whose envelopes it drops.
    fn unreached(&self, address: &str) {
        for node in self.fail(address) {
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
                    .filter_map(|(target, _, _)| match target {
                        Target::Node(node) => Some(node),
                        Target::Seed(_) => None,
                    })
                    .collect()
            });
        let previous = state.backoff.get(address).copied();
        let first = self.settings.limits.backoff_first;
        let limit = self.settings.limits.backoff_limit;
        let delay = previous.map_or(first, |backoff| (backoff.delay * 2).min(limit));
        let jitter = u32::try_from(Rolls::fresh().between(750, 1250)).expect("at most 1250");
        state.backoff.insert(
            address.to_owned(),
            Backoff {
                delay,
                retry_at: Instant::now() + delay * jitter / 1000,
            },
        );
        dropped
    }
}
