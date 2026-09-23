//! The transport of one node: a listener, the pool of connections, and the envelopes that arrive.
//!
//! Without `bind` this is a client: no listener, and a node without an address, reachable only through the
//! connections it opens.
//!
//! Everything it starts ends with it. `close` lets the connections finish what they are writing; an endpoint dropped
//! without it, as the task of a node that crashed drops it, stops listening and ends its connections where they are,
//! so that a runtime other systems go on running on keeps neither its port nor its peers.

use std::io;
use std::sync::Arc;

use casty_core::node::NodeId;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::compress::{Name, PREFERENCE};
use crate::connection::Incoming;
use crate::handshake::Hello;
use crate::limits::Limits;
use crate::pool::{AddressMap, Heard, Pool, Settings, Target, Traffic};
use crate::tls::Tls;

/// How a node reaches the others.
#[derive(Clone)]
pub struct Config {
    pub bind: Option<String>,
    pub advertise: Option<String>,
    pub cluster: String,
    pub tls: Option<Tls>,
    /// The compressors offered, in order of preference. Nothing means every one this build has.
    pub compression: Option<Vec<Name>>,
    pub min_compressed: usize,
    pub address_map: Option<AddressMap>,
    pub limits: Limits,
    /// What hears of peers: a connection that ended, and envelopes dropped for a peer that was not reached. Nothing
    /// listens by default.
    pub heard: Option<Heard>,
}

impl core::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Config")
            .field("bind", &self.bind)
            .field("advertise", &self.advertise)
            .field("cluster", &self.cluster)
            .finish_non_exhaustive()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: None,
            advertise: None,
            cluster: "casty".to_owned(),
            tls: None,
            compression: None,
            min_compressed: 4096,
            address_map: None,
            limits: Limits::default(),
            heard: None,
        }
    }
}

/// An envelope that arrived for this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub name: String,
    pub payload: Vec<u8>,
    /// The peer it came from, or nothing for an envelope this node sent itself.
    pub from: Option<NodeId>,
}

/// A payload larger than the limit of one message, which never reaches the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooLarge(pub String);

#[derive(Debug)]
pub struct Endpoint {
    sender: Sender,
    inbound: mpsc::UnboundedReceiver<Incoming>,
    listening: Option<JoinHandle<()>>,
    released: Option<oneshot::Receiver<()>>,
    limits: Limits,
}

/// What resolves once the listener of an endpoint has let its address go, however it went.
#[derive(Debug)]
pub struct Released(oneshot::Receiver<()>);

impl Released {
    pub async fn wait(self) {
        let _ = self.0.await;
    }
}

/// The listener, and the word that it is gone. Fields drop in their order: the port is free before anyone hears.
struct Bound {
    listener: TcpListener,
    _freed: oneshot::Sender<()>,
}

impl Endpoint {
    /// Bind the listener, if there is one, and start taking envelopes.
    pub async fn start(config: Config) -> io::Result<Self> {
        let identity = match &config.tls {
            None => None,
            Some(tls) => Some(tls.identity()?),
        };
        let (inbound, receiver) = mpsc::unbounded_channel();
        let (node, listener) = match &config.bind {
            None => (NodeId::fresh(None), None),
            Some(bind) => {
                let listener = TcpListener::bind(bind).await?;
                let bound = listener.local_addr()?;
                let host = bind
                    .rsplit_once(':')
                    .map_or(bind.as_str(), |(host, _)| host);
                let address = config
                    .advertise
                    .clone()
                    .unwrap_or_else(|| format!("{host}:{}", bound.port()));
                (NodeId::fresh(Some(address)), Some(listener))
            }
        };
        let local = Hello {
            cluster: config.cluster.clone(),
            node: node.clone(),
            compression: config
                .compression
                .clone()
                .unwrap_or_else(|| PREFERENCE.to_vec()),
            sizes: [
                config.limits.frame,
                config.limits.message,
                config.limits.window,
            ],
        };
        let pool = Pool::new(
            Settings {
                local,
                limits: config.limits,
                min_compressed: config.min_compressed,
                tls: identity.clone(),
                address_map: config.address_map.clone(),
                heard: config.heard.clone(),
            },
            inbound.clone(),
        );
        let (freed, released) = oneshot::channel();
        let listening = listener.map(|listener| {
            let pool = Arc::clone(&pool);
            let identity = identity.clone();
            let handshake = config.limits.handshake;
            let bound = Bound {
                listener,
                _freed: freed,
            };
            tokio::spawn(async move {
                // Taken whole: a block that named only the listener would capture only it, and the word that it is
                // gone would be dropped here, before it is.
                let bound = bound;
                while let Ok((socket, _)) = bound.listener.accept().await {
                    socket.set_nodelay(true).ok();
                    take(&pool, socket, identity.clone(), handshake);
                }
            })
        });
        Ok(Self {
            sender: Sender {
                node,
                pool,
                loopback: inbound,
                message: config.limits.message,
            },
            inbound: receiver,
            released: listening.is_some().then_some(released),
            listening,
            limits: config.limits,
        })
    }

    /// What says the address this endpoint listens on is free again, taken once: what binds it next waits for it.
    /// Nothing for a client, which listens on none.
    pub fn released(&mut self) -> Option<Released> {
        self.released.take().map(Released)
    }

    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.sender.node
    }

    /// A handle that only reads what the transport holds and carried, from any thread.
    #[must_use]
    pub fn meter(&self) -> Meter {
        Meter {
            pool: Arc::clone(&self.sender.pool),
        }
    }

    /// A handle that only sends, which is what every thread but the one reading holds.
    #[must_use]
    pub fn sender(&self) -> Sender {
        self.sender.clone()
    }

    /// Queue an envelope. It never waits, and it is lost if the connection it needs never opens.
    pub fn send(&self, to: &Target, name: &str, payload: &[u8]) -> Result<(), TooLarge> {
        self.sender.send(to, name, payload)
    }

    /// The next envelope for this node, or the refusal of a seed that would not have it.
    pub async fn recv(&mut self) -> Option<Result<Received, String>> {
        match self.inbound.recv().await? {
            Incoming::Local { name, payload } => Some(Ok(Received {
                name,
                payload,
                from: None,
            })),
            Incoming::Remote(arrival) => {
                arrival.from.consume(&arrival.record);
                Some(Ok(Received {
                    name: arrival.record.name,
                    payload: arrival.record.payload,
                    from: Some(arrival.from.peer().clone()),
                }))
            }
            Incoming::Refused(reason) => Some(Err(reason)),
        }
    }

    /// Stop listening and let the connections go. With `abort` they end at once, instead of writing what the peers'
    /// credit allows and saying goodbye.
    pub async fn close(mut self, abort: bool) {
        if let Some(listening) = self.listening.take() {
            listening.abort();
            // Waited for, not just asked: the port is free only once the task that owns the listener has dropped it.
            let _ = listening.await;
        }
        self.sender.pool.close(abort);
        if abort {
            return;
        }
        // What the peers' credit allows goes out first, up to the deadline of the shutdown.
        let deadline = tokio::time::Instant::now() + self.limits.handshake;
        while !self.sender.pool.idle() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
    }
}

impl Drop for Endpoint {
    // After `close` this ends what outlived its deadline; without it, everything the endpoint started.
    fn drop(&mut self) {
        if let Some(listening) = self.listening.take() {
            listening.abort();
        }
        self.sender.pool.close(true);
    }
}

/// Take a connection the listener accepted, with TLS on top of it when the cluster has it.
fn take(
    pool: &Arc<Pool>,
    socket: TcpStream,
    identity: Option<crate::tls::Identity>,
    handshake: core::time::Duration,
) {
    let Some(identity) = identity else {
        pool.accept(Box::new(socket));
        return;
    };
    let pool = Arc::clone(pool);
    tokio::spawn(async move {
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::clone(&identity.server));
        if let Ok(Ok(stream)) = tokio::time::timeout(handshake, acceptor.accept(socket)).await {
            pool.accept(Box::new(stream));
        }
    });
}

/// What reads the counts of a transport without being able to send.
#[derive(Debug, Clone)]
pub struct Meter {
    pool: Arc<Pool>,
}

impl Meter {
    /// The connections open now and the bytes every connection carried so far.
    #[must_use]
    pub fn traffic(&self) -> Traffic {
        self.pool.traffic()
    }
}

/// What sends envelopes, on its own so that reading and sending are not the same borrow.
#[derive(Debug, Clone)]
pub struct Sender {
    node: NodeId,
    pool: Arc<Pool>,
    loopback: mpsc::UnboundedSender<Incoming>,
    message: usize,
}

impl Sender {
    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.node
    }

    /// Queue an envelope. It never waits, and it is lost if the connection it needs never opens.
    pub fn send(&self, to: &Target, name: &str, payload: &[u8]) -> Result<(), TooLarge> {
        if payload.len() > self.message {
            return Err(TooLarge(format!(
                "payload of {} bytes exceeds the limit of {}",
                payload.len(),
                self.message
            )));
        }
        if *to == Target::Node(self.node.clone()) {
            let _ = self.loopback.send(Incoming::Local {
                name: name.to_owned(),
                payload: payload.to_vec(),
            });
            return Ok(());
        }
        self.pool.send(to, name, payload);
        Ok(())
    }
}
