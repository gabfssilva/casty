//! The transport of one node: a listener, the pool of connections, and the envelopes that arrive.
//!
//! Without `bind` this is a client: no listener, and a node without an address, reachable only through the
//! connections it opens.

use std::io;
use std::sync::Arc;

use casty_core::node::NodeId;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::compress::{Name, offered};
use crate::connection::{Incoming, Socket};
use crate::frame::VERSION;
use crate::handshake::{Hello, Role, VERSIONS};
use crate::limits::Limits;
use crate::pool::{AddressMap, Pool, Settings, Target};
use crate::tls::Tls;

/// How a node reaches the others.
#[derive(Clone)]
pub struct Config {
    pub bind: Option<String>,
    pub advertise: Option<String>,
    pub cluster: String,
    pub codec: String,
    pub tls: Option<Tls>,
    /// The compressors offered, in order of preference. Nothing means every one this build has.
    pub compression: Option<Vec<Name>>,
    pub min_compressed: usize,
    pub address_map: Option<AddressMap>,
    pub limits: Limits,
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
            codec: "msgpack".to_owned(),
            tls: None,
            compression: None,
            min_compressed: 4096,
            address_map: None,
            limits: Limits::default(),
        }
    }
}

/// An envelope that arrived for this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub name: String,
    pub payload: Vec<u8>,
}

/// A payload larger than the limit of one message, which never reaches the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooLarge(pub String);

#[derive(Debug)]
pub struct Endpoint {
    node: NodeId,
    pool: Arc<Pool>,
    inbound: mpsc::UnboundedReceiver<Incoming>,
    loopback: mpsc::UnboundedSender<Incoming>,
    listening: Option<JoinHandle<()>>,
    limits: Limits,
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
            versions: VERSIONS.to_vec(),
            cluster: config.cluster.clone(),
            codec: config.codec.clone(),
            node: node.clone(),
            role: if node.address.is_none() {
                Role::Client
            } else {
                Role::Member
            },
            compression: offered(config.compression.as_deref())
                .into_iter()
                .map(|name| name.name().to_owned())
                .collect(),
        };
        let pool = Pool::new(
            Settings {
                local,
                ours: offered(config.compression.as_deref()),
                limits: config.limits,
                min_compressed: config.min_compressed,
                tls: identity.clone(),
                address_map: config.address_map.clone(),
            },
            inbound.clone(),
        );
        let listening = listener.map(|listener| {
            let pool = Arc::clone(&pool);
            let identity = identity.clone();
            let handshake = config.limits.handshake;
            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    socket.set_nodelay(true).ok();
                    take(&pool, socket, identity.clone(), handshake);
                }
            })
        });
        // The version of the frame header is what the handshake negotiates, and there is one so far.
        debug_assert_eq!(VERSION, 1);
        Ok(Self {
            node,
            pool,
            inbound: receiver,
            loopback: inbound,
            listening,
            limits: config.limits,
        })
    }

    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.node
    }

    /// A handle that only sends, which is what every thread but the one reading holds.
    #[must_use]
    pub fn sender(&self) -> Sender {
        Sender {
            node: self.node.clone(),
            pool: Arc::clone(&self.pool),
            loopback: self.loopback.clone(),
            message: self.limits.message,
        }
    }

    /// Queue an envelope. It never waits, and it is lost if the connection it needs never opens.
    pub fn send(&self, to: &Target, name: &str, payload: &[u8]) -> Result<(), TooLarge> {
        self.sender().send(to, name, payload)
    }

    /// The next envelope for this node, or the refusal of a seed that would not have it.
    pub async fn recv(&mut self) -> Option<Result<Received, String>> {
        match self.inbound.recv().await? {
            Incoming::Local { name, payload } => Some(Ok(Received { name, payload })),
            Incoming::Remote(arrival) => {
                arrival.from.consume(&arrival.record);
                Some(Ok(Received {
                    name: arrival.record.name,
                    payload: arrival.record.payload,
                }))
            }
            Incoming::Refused(reason) => Some(Err(reason)),
        }
    }

    /// Stop listening and let the connections go. Leaving by an exception aborts them instead of saying goodbye.
    pub async fn close(mut self, abort: bool) {
        if let Some(listening) = self.listening.take() {
            listening.abort();
            // Waited for, not just asked: the port is free only once the task that owns the listener has dropped it.
            let _ = listening.await;
        }
        self.pool.close(abort);
        if abort {
            return;
        }
        // What the peers' credit allows goes out first, up to the deadline of the shutdown.
        let deadline = tokio::time::Instant::now() + self.limits.handshake;
        while !self.pool.idle() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(core::time::Duration::from_millis(5)).await;
        }
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
        pool.accept(socket);
        return;
    };
    let pool = Arc::clone(pool);
    tokio::spawn(async move {
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::clone(&identity.server));
        if let Ok(Ok(stream)) = tokio::time::timeout(handshake, acceptor.accept(socket)).await {
            let stream: Box<dyn Socket> = Box::new(stream);
            pool.accept(stream);
        }
    });
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
