//! The membership of a node running over the transport: one task, the service, and the clock.
//!
//! Everything the component touches lives in this task, so there is no lock to take and no order to respect: what
//! arrives and what the timers say are handled one at a time.

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use casty_core::backoff::Backoff;
use casty_core::membership::views::Overlay;
use casty_core::node::NodeId;
use casty_net::compress::Name;
use casty_net::endpoint::{Config, Endpoint};
use casty_net::limits::Limits;
use casty_net::pool::{AddressMap, Heard, Peer};
use casty_net::tls::Tls;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::service::{Member, Membership, Timings};
use super::wire::encode;

/// The name of the component, which is the address every membership envelope carries.
const NAME: &str = "membership";

/// How a node reaches the cluster it belongs to.
#[derive(Clone)]
pub struct Cluster {
    pub bind: String,
    pub advertise: Option<String>,
    pub seeds: Vec<String>,
    pub name: String,
    pub timings: Timings,
    pub overlay: Overlay,
    pub tls: Option<Tls>,
    pub compression: Option<Vec<Name>>,
    /// The smallest piece of a message that is compressed; anything shorter goes as it is.
    pub min_compressed: usize,
    pub address_map: Option<AddressMap>,
    pub limits: Limits,
    /// How long an activation or a write waits for the replicas of its key.
    pub write_timeout: Duration,
    /// How long a node waits before asking again for what another one owes it.
    pub backoff: Backoff,
    /// How long a node that is leaving waits for the nodes that replicate its keys now to take them.
    pub leave_timeout: Duration,
}

impl core::fmt::Debug for Cluster {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Cluster")
            .field("bind", &self.bind)
            .field("seeds", &self.seeds)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Cluster {
    #[must_use]
    pub fn at(bind: &str) -> Self {
        Self {
            bind: bind.to_owned(),
            advertise: None,
            seeds: Vec::new(),
            name: "casty".to_owned(),
            timings: Timings::default(),
            overlay: Overlay::default(),
            tls: None,
            compression: None,
            min_compressed: 4096,
            address_map: None,
            limits: Limits::default(),
            write_timeout: Duration::from_secs(5),
            backoff: Backoff::default(),
            leave_timeout: Duration::from_secs(30),
        }
    }
}

/// What the caller asks of the component while it runs.
#[derive(Debug)]
enum Command {
    Know(BTreeSet<String>),
    /// The transport dropped what was sent to a node, which it says from a task of its own.
    Unreached(NodeId),
    Leave,
    Depart,
    Stop,
}

/// A node in a cluster: what it is, who it sees, and how to take it out.
#[derive(Debug)]
pub struct Joined {
    node: NodeId,
    members: watch::Receiver<Vec<Member>>,
    removed: watch::Receiver<bool>,
    commands: mpsc::UnboundedSender<Command>,
    running: JoinHandle<()>,
}

impl Joined {
    /// Start the node and wait until it is in a cluster, or until a seed refuses it.
    ///
    /// Without seeds the node is a cluster of its own from the start; with them, it waits for one to answer, or for
    /// another node to reach it first.
    pub async fn start(cluster: Cluster, types: BTreeSet<String>) -> io::Result<Self> {
        let (commands, taking) = mpsc::unbounded_channel();
        // Weak, so that the transport holding it does not keep the task waiting for commands forever.
        let reporting = commands.downgrade();
        let heard: Heard = Arc::new(move |peer: Peer| {
            if let (Peer::Unreached(node), Some(commands)) = (peer, reporting.upgrade()) {
                let _ = commands.send(Command::Unreached(node));
            }
        });
        let endpoint = Endpoint::start(Config {
            bind: Some(cluster.bind.clone()),
            advertise: cluster.advertise.clone(),
            cluster: cluster.name.clone(),
            tls: cluster.tls.clone(),
            compression: cluster.compression.clone(),
            min_compressed: cluster.min_compressed,
            address_map: cluster.address_map.clone(),
            limits: cluster.limits,
            heard: Some(heard),
        })
        .await?;
        let node = endpoint.node().clone();
        let service = Membership::new(
            node.clone(),
            types,
            cluster.seeds.clone(),
            cluster.timings,
            cluster.overlay,
        );
        let (members, watching) = watch::channel(service.members());
        let (removed, gone) = watch::channel(false);
        let (entered, joining) = oneshot::channel();
        let timings = cluster.timings;
        let running = tokio::spawn(run(
            endpoint, service, timings, members, removed, taking, entered,
        ));
        match joining.await {
            Ok(Ok(())) => Ok(Self {
                node,
                members: watching,
                removed: gone,
                commands,
                running,
            }),
            Ok(Err(refused)) => Err(io::Error::other(refused)),
            Err(_) => Err(io::Error::other("the node stopped before it joined")),
        }
    }

    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.node
    }

    /// The members this node knows right now.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.members.borrow().clone()
    }

    /// Wait until what this node sees satisfies `settled`, or until the node stops.
    pub async fn until(&mut self, settled: impl Fn(&[Member]) -> bool) {
        loop {
            if settled(&self.members.borrow_and_update()) {
                return;
            }
            if self.members.changed().await.is_err() {
                return;
            }
        }
    }

    /// Whether the cluster declared this node gone, which only a new identity undoes.
    #[must_use]
    pub fn removed(&self) -> bool {
        *self.removed.borrow()
    }

    /// Tell the cluster of a type this node met, so every member has it before a ring gives it keys.
    pub fn know(&self, types: BTreeSet<String>) {
        let _ = self.commands.send(Command::Know(types));
    }

    /// Say goodbye and wait for the node to be out.
    pub async fn leave(self) {
        let _ = self.commands.send(Command::Leave);
        let _ = self.commands.send(Command::Depart);
        let _ = self.commands.send(Command::Stop);
        let _ = self.running.await;
    }

    /// Stop without a word, which is what a machine going away looks like to the others.
    pub async fn crash(self) {
        self.running.abort();
        let _ = self.running.await;
    }
}

async fn run(
    mut endpoint: Endpoint,
    mut service: Membership,
    timings: Timings,
    members: watch::Sender<Vec<Member>>,
    removed: watch::Sender<bool>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    entered: oneshot::Sender<Result<(), String>>,
) {
    let started = tokio::time::Instant::now();
    let now = move || started.elapsed().as_secs_f64();
    let mut heartbeat = tokio::time::interval(timings.heartbeat);
    let mut graft = tokio::time::interval(timings.graft_after);
    let mut shuffle = tokio::time::interval(timings.shuffle_every);
    let mut anti_entropy = tokio::time::interval(timings.anti_entropy);
    let sender = endpoint.sender();
    let mut entered = Some(entered);
    service.join();
    flush(&sender, &mut service, &members, &removed, &mut entered);
    loop {
        tokio::select! {
            arrived = endpoint.recv() => match arrived {
                Some(Ok(envelope)) => {
                    if let Some(from) = &envelope.from {
                        service.heard(from, now());
                    }
                    if envelope.name == NAME {
                        service.receive(&envelope.payload, now());
                    }
                }
                // A seed that would not have this node is what a join fails with.
                Some(Err(refused)) => {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(Err(refused));
                    }
                }
                None => break,
            },
            _ = heartbeat.tick() => {
                service.probe(now());
                service.expire(now());
            }
            _ = graft.tick() => service.graft(now()),
            _ = shuffle.tick() => service.shuffle(now()),
            _ = anti_entropy.tick() => service.anti_entropy(),
            command = commands.recv() => match command {
                Some(Command::Know(types)) => service.know(&types, now()),
                Some(Command::Unreached(node)) => service.unreached(&node, now()),
                Some(Command::Leave) => service.leave(now()),
                Some(Command::Depart) => service.depart(now()),
                Some(Command::Stop) | None => {
                    flush(&sender, &mut service, &members, &removed, &mut entered);
                    break;
                }
            },
        }
        flush(&sender, &mut service, &members, &removed, &mut entered);
    }
    endpoint.close(false).await;
}

fn flush(
    sender: &casty_net::endpoint::Sender,
    service: &mut Membership,
    members: &watch::Sender<Vec<Member>>,
    removed: &watch::Sender<bool>,
    entered: &mut Option<oneshot::Sender<Result<(), String>>>,
) {
    for out in service.take() {
        let _ = sender.send(&out.to, NAME, &encode(&out.message));
    }
    if service.changed {
        service.changed = false;
        let _ = members.send(service.members());
    }
    // The table keeps its transitions until they are taken, and this runner has nobody to report them to.
    service.transitions();
    if service.removed {
        let _ = removed.send(true);
    }
    if service.joined
        && let Some(entered) = entered.take()
    {
        let _ = entered.send(Ok(()));
    }
}
