//! A node: the transport, the components on it, and the one task that owns them all.
//!
//! Everything the components touch lives in that task. What reaches it from outside — a message a body sent, an
//! answer, a type the process met — arrives as a request on a channel, so there is no lock to take and no order to
//! respect. The host is what runs the bodies; it is called from the task and never calls back into it except
//! through the same channel.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use casty_core::backoff::Backoff;
use casty_core::chain::Chain;
use casty_core::handoff::sweep::{Kept, sweep};
use casty_core::mailbox::{Command, Deliver, Start};
use casty_core::membership::table::Status;
use casty_core::membership::views::Overlay;
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_core::placement::pinned;
use casty_core::replication::messages::Write;
use casty_core::store::{Durable, Held as State, Pages};
use casty_net::compress::Name;
use casty_net::endpoint::{Config, Endpoint, Meter, Sender, TooLarge};
use casty_net::limits::Limits;
use casty_net::pool::{AddressMap, Heard, Peer, Target as Destination};
use casty_net::tls::Tls;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::events::{Event, Operation, abandoned};
use crate::handoff::service::Handoff;
use crate::membership::Members;
use crate::membership::directory::Directory;
use crate::membership::service::{Member, Membership, Timings};
use crate::membership::wire as membership;
use crate::placement::{Counts, Placement};
use crate::replication::service::{
    Around, Entity, Failure, Replication, StoreAnswer, Storing, Written,
};
use crate::replication::wire::{self as replication};
use crate::routing::service::{Decision, Routing, cancelling};
use crate::routing::wire::{self as routing, Answer, Cancel, Routed};

const MEMBERSHIP: &str = "membership";
const ACTORS: &str = "actors";
const REPLIES: &str = "replies";
const REPLICATION: &str = "replication";

/// A type this process runs: what a node needs of it to place its keys and write their state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kind {
    pub actor: String,
    pub replicas: usize,
    pub write: Write,
    /// How long an operation over a key of the type waits for its replicas. Nothing means the node's.
    pub write_timeout: Option<core::time::Duration>,
    /// Whether each key runs on the node its key names, which leaves the type out of the ranges of the ring.
    pub pinned: bool,
    /// When the store of the system keeps the writes of the type. Nothing keeps them in memory only.
    pub durable: Option<Durable>,
}

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
    pub write_timeout: core::time::Duration,
    /// How long a node waits before asking again for what another one owes it.
    pub backoff: Backoff,
    /// How long a node that is leaving waits for the nodes that replicate its keys now to take them.
    pub leave_timeout: core::time::Duration,
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
            write_timeout: core::time::Duration::from_secs(5),
            backoff: Backoff::default(),
            leave_timeout: core::time::Duration::from_secs(30),
        }
    }
}

/// What runs the bodies of the keys this node hosts.
///
/// Every method is called from the task of the node. A host that lives on another thread — an event loop, say —
/// hands the work over and returns; it must not wait for the node, because the node is what would answer it.
pub trait Host: Send + Sync + 'static {
    /// Take a command for the activation of its key, starting one if there is none.
    ///
    /// The host answers the request itself when it does not have the type: only it knows what this process holds.
    fn hand(&self, node: &Node, command: Command);

    /// Answer the request numbered `id`, which something on this node is waiting for.
    fn settle(&self, id: i64, outcome: Outcome);

    /// The members changed, which is what a watcher acts on.
    fn members(&self, members: Vec<Member>) {
        let _ = members;
    }

    /// A type named by a message that arrived. The host resolves it and answers with `learn` or with `gave_up`.
    fn meet(&self, node: &Node, actor: &str) {
        let _ = (node, actor);
    }

    /// Run the key again here: its copy carries the active mark and this node owns it now.
    fn attach(&self, node: &Node, actor: &str, key: &str) {
        let _ = (node, actor, key);
    }

    /// End the activation of a key whose owner moved: a write from it would be refused by the replicas anyway.
    fn release(&self, actor: &str, key: &str) {
        let _ = (actor, key);
    }

    /// The caller of `request`, sent to `(actor, key)`, stopped waiting for its answer: it timed out or was cancelled.
    ///
    /// It arrives whether or not this node owns the key, and nothing answers it. A host with no activation of the key,
    /// or whose activation already answered the request or never received it, drops it.
    fn cancel(&self, actor: &str, key: &str, request: &Target) {
        let _ = (actor, key, request);
    }

    /// The cluster declared this node left. It never comes back under the same identity: what it knew is gone.
    fn removed(&self) {}

    /// Something the node saw or did, for whoever watches it. A host that nobody watches lets it go.
    fn observe(&self, event: Event) {
        let _ = event;
    }

    /// Carry `request` out on the store of the system, and answer it with `Node::from_store` once the store has, within
    /// `request.within`, or with the failure that says why it did not.
    ///
    /// A host without a store answers every request with that failure, which is what a durable type meets on it.
    fn store(&self, node: &Node, request: Storing) {
        node.from_store(
            request.id,
            Err("this node has no store, which a durable type needs".to_owned()),
        );
    }
}

/// What the node is asked to do from outside its task.
#[derive(Debug)]
enum Ask {
    Route(Command),
    Answer {
        target: Target,
        outcome: Outcome,
    },
    Toward {
        id: i64,
        node: NodeId,
        failure: Outcome,
    },
    Cancel(Cancel),
    Learn(Kind),
    GaveUp(String),
    Activate {
        actor: String,
        key: String,
        initial: Option<Pages>,
        answer: oneshot::Sender<Result<Option<State>, Failure>>,
    },
    Commit {
        actor: String,
        key: String,
        lease: u64,
        pages: Pages,
        active: bool,
        answer: oneshot::Sender<Result<(), Failure>>,
    },
    Delete {
        actor: String,
        key: String,
        lease: u64,
        active: bool,
        answer: oneshot::Sender<Result<(), Failure>>,
    },
    Release {
        actor: String,
        key: String,
        lease: u64,
    },
    Placed {
        actor: String,
        key: String,
        answer: oneshot::Sender<Placed>,
    },
    Stored(oneshot::Sender<Vec<(String, String, bool)>>),
    /// What the store of the system answered to the request `id`.
    FromStore {
        id: u64,
        kept: StoreAnswer,
    },
    Attached {
        actor: String,
        key: String,
    },
    Detached {
        actor: String,
        key: String,
    },
    Handed(oneshot::Sender<Vec<Entity>>),
    /// The leave stopped waiting for `Handed`: what is still owed is what the node goes without.
    Abandon,
    /// What happened to a peer, which the transport says from a task of its own.
    Heard(Peer),
    Leave,
    Depart,
    Stop,
}

/// A node of a cluster, reachable from any thread.
#[derive(Debug, Clone)]
pub struct Node {
    inner: Arc<Reach>,
}

#[derive(Debug)]
struct Reach {
    id: NodeId,
    asks: mpsc::UnboundedSender<Ask>,
    ids: AtomicI64,
    members: watch::Receiver<Vec<Member>>,
    /// The largest message between two nodes, which every command and answer is measured against.
    message: usize,
    /// What the transport counts, and what the replication counts of the writes it drove.
    meter: Meter,
    written: Arc<Written>,
}

/// What a node has counted, read at one moment: the connections it holds now, the bytes they carried, and how many
/// writes of the keys it owns the replicas confirmed and how many failed, since it started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Tally {
    pub connections: usize,
    pub sent: u64,
    pub received: u64,
    pub confirmed: u64,
    pub failed: u64,
}

/// Where a key is, as a node sees it at one moment: the nodes that keep it, in the order the ring gives them, and the
/// first of them that is up, which is where a message to the key goes. A pinned key is kept by the member advertised
/// at its address.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Placed {
    pub owner: Option<NodeId>,
    pub replicas: Vec<NodeId>,
}

impl Node {
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.inner.id
    }

    /// What this node has counted so far. The transport and the replication count as they go, on their own threads,
    /// so this reads their counters and asks the task of the node nothing.
    #[must_use]
    pub fn tally(&self) -> Tally {
        let traffic = self.inner.meter.traffic();
        Tally {
            connections: traffic.connections,
            sent: traffic.sent,
            received: traffic.received,
            confirmed: self.inner.written.confirmed(),
            failed: self.inner.written.failed(),
        }
    }

    /// The members this node knows right now.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.inner.members.borrow().clone()
    }

    /// The next id of a request, which is what its answer comes back addressed to.
    #[must_use]
    pub fn take(&self) -> i64 {
        self.inner.ids.fetch_add(1, Ordering::Relaxed)
    }

    /// Where this node waits for an answer of its own.
    #[must_use]
    pub fn waiting(&self, id: i64) -> Target {
        Target::Reply {
            node: self.inner.id.clone(),
            id,
        }
    }

    /// Send a message to the entity `(actor, key)`, answering `reply` when there is one waiting, with the `chain` of
    /// the bodies that wait for that answer.
    ///
    /// A message larger than one envelope between two nodes carries is refused here, before it is routed, so that it
    /// fails the same way wherever its key is.
    pub fn deliver(
        &self,
        actor: &str,
        key: &str,
        message: Vec<u8>,
        reply: Option<Target>,
        chain: Chain,
    ) -> Result<(), TooLarge> {
        self.route(Command::Deliver(Deliver {
            actor: actor.to_owned(),
            key: key.to_owned(),
            message,
            reply,
            chain,
        }))
    }

    /// Create the key, from `state` or from the default of its type, and activate it. Nothing answers.
    ///
    /// A state larger than one envelope between two nodes carries is refused here, as a message is.
    pub fn start(&self, actor: &str, key: &str, state: Option<Vec<u8>>) -> Result<(), TooLarge> {
        self.route(Command::Start(Start {
            actor: actor.to_owned(),
            key: key.to_owned(),
            state,
        }))
    }

    fn route(&self, command: Command) -> Result<(), TooLarge> {
        let size = routing::routed_size(&command, &self.inner.id);
        if size > self.inner.message {
            let what = match &command {
                Command::Deliver(_) => "the message to",
                Command::Start(_) => "the initial state of",
            };
            return Err(TooLarge(format!(
                "{what} {}/{} takes {size} bytes between two nodes, over limits.message of {}",
                command.actor(),
                command.key(),
                self.inner.message
            )));
        }
        self.ask(Ask::Route(command));
        Ok(())
    }

    /// Answer the request `target` waits for, wherever it waits. An answer that does not fit is replaced by the
    /// refusal `oversized` gives.
    pub fn answer(&self, target: Target, outcome: Outcome) {
        let outcome = self.oversized(&target, &outcome).unwrap_or(outcome);
        self.ask(Ask::Answer { target, outcome });
    }

    /// What answers `target` in place of `outcome` when that is larger than one envelope between two nodes carries.
    ///
    /// It is measured whether or not the caller is on this node, so that an answer does not arrive or fail by where
    /// the key landed. A host that settles an answer to a caller of its own without `answer` measures it here.
    #[must_use]
    pub fn oversized(&self, target: &Target, outcome: &Outcome) -> Option<Outcome> {
        let Target::Reply { id, .. } = target else {
            return None;
        };
        let size = routing::answer_size(*id, outcome);
        (size > self.inner.message).then(|| {
            Outcome::TooLarge(format!(
                "the answer takes {size} bytes between two nodes, over limits.message of {}",
                self.inner.message
            ))
        })
    }

    /// Note the node a request went to, and how it ends if that node never answers.
    pub fn toward(&self, id: i64, node: NodeId, failure: Outcome) {
        self.ask(Ask::Toward { id, node, failure });
    }

    /// Stop waiting for the answer of `request`, and tell the key `(actor, key)` it was sent to that nobody waits for
    /// it any more. A timeout or a cancellation does this.
    pub fn cancel(&self, actor: &str, key: &str, request: Target) {
        self.ask(Ask::Cancel(Cancel {
            actor: actor.to_owned(),
            key: key.to_owned(),
            request,
        }));
    }

    /// Tell the node of a type this process has, so that the cluster and the rings know of it.
    pub fn learn(&self, kind: Kind) {
        self.ask(Ask::Learn(kind));
    }

    /// Tell the node this process cannot import a type it was asked about, so it stops waiting to hear of it.
    pub fn gave_up(&self, actor: &str) {
        self.ask(Ask::GaveUp(actor.to_owned()));
    }

    /// Take the key over: promise an epoch, choose the state among the replicas and write it with the mark.
    ///
    /// Nothing means no replica has a state for the key and the call gave no initial one.
    pub async fn activate(
        &self,
        actor: &str,
        key: &str,
        initial: Option<Pages>,
    ) -> Result<Option<State>, Failure> {
        let (answer, answered) = oneshot::channel();
        self.ask(Ask::Activate {
            actor: actor.to_owned(),
            key: key.to_owned(),
            initial,
            answer,
        });
        stopped(answered.await, actor, key)
    }

    /// Write the state of a key this node owns, from the activation `lease` names. Without `active` the key lets go
    /// of it instead.
    pub async fn commit(
        &self,
        actor: &str,
        key: &str,
        lease: u64,
        pages: Pages,
        active: bool,
    ) -> Result<(), Failure> {
        let (answer, answered) = oneshot::channel();
        self.ask(Ask::Commit {
            actor: actor.to_owned(),
            key: key.to_owned(),
            lease,
            pages,
            active,
            answer,
        });
        stopped(answered.await, actor, key)
    }

    /// Delete the state of a key this node owns, at the write level of its type. The key is activated again from
    /// `initial`, as one nothing ever wrote. Without `active` the key lets go with it, as with a release.
    pub async fn delete(
        &self,
        actor: &str,
        key: &str,
        lease: u64,
        active: bool,
    ) -> Result<(), Failure> {
        let (answer, answered) = oneshot::channel();
        self.ask(Ask::Delete {
            actor: actor.to_owned(),
            key: key.to_owned(),
            lease,
            active,
            answer,
        });
        stopped(answered.await, actor, key)
    }

    /// Drop the owner of a key this node stopped holding, while it still serves the activation `lease` names. Its
    /// replica keeps the state.
    pub fn release(&self, actor: &str, key: &str, lease: u64) {
        self.ask(Ask::Release {
            actor: actor.to_owned(),
            key: key.to_owned(),
            lease,
        });
    }

    /// Where `(actor, key)` is as this node sees it now, which is what it routes a message to the key by: a key of a
    /// range still arriving here is placed on the ring before the change, as its messages are.
    ///
    /// Nothing, once the node has stopped.
    pub async fn placed(&self, actor: &str, key: &str) -> Placed {
        let (answer, answered) = oneshot::channel();
        self.ask(Ask::Placed {
            actor: actor.to_owned(),
            key: key.to_owned(),
            answer,
        });
        answered.await.unwrap_or_default()
    }

    /// Every key the replica of this node keeps, and whether what it keeps is the tombstone of a deletion.
    ///
    /// Nothing, once the node has stopped.
    pub async fn stored(&self) -> Vec<(String, String, bool)> {
        let (answer, answered) = oneshot::channel();
        self.ask(Ask::Stored(answer));
        answered.await.unwrap_or_default()
    }

    /// What the store of the system answered to the request `id` the node handed to the host.
    pub fn from_store(&self, id: u64, kept: StoreAnswer) {
        self.ask(Ask::FromStore { id, kept });
    }

    /// Tell the cluster this node is going, so that nothing new is routed to it while it finishes what it is on.
    pub fn leaving(&self) {
        self.ask(Ask::Leave);
    }

    /// An activation started here. The sweep ends it once its key belongs to another node, and does not start a second
    /// one while it runs.
    pub fn attached(&self, actor: &str, key: &str) {
        self.ask(Ask::Attached {
            actor: actor.to_owned(),
            key: key.to_owned(),
        });
    }

    /// An activation ended here.
    pub fn detached(&self, actor: &str, key: &str) {
        self.ask(Ask::Detached {
            actor: actor.to_owned(),
            key: key.to_owned(),
        });
    }

    fn ask(&self, ask: Ask) {
        let _ = self.inner.asks.send(ask);
    }
}

/// A node that stopped never answers, and what was waiting for it hears that instead of waiting forever.
fn stopped<T>(
    answered: Result<Result<T, Failure>, oneshot::error::RecvError>,
    actor: &str,
    key: &str,
) -> Result<T, Failure> {
    answered.unwrap_or_else(|_| {
        Err(Failure::Unavailable(format!(
            "{actor}/{key}: this node has stopped"
        )))
    })
}

/// A node that is running, and the handle that stops it.
#[derive(Debug)]
pub struct Running {
    pub node: Node,
    task: JoinHandle<()>,
    /// How long leaving waits for the nodes that replicate its keys now to take them.
    handover: core::time::Duration,
}

impl Running {
    /// Start a node and wait until it is in a cluster, or until a seed refuses it.
    pub async fn start(
        cluster: Cluster,
        host: Arc<dyn Host>,
        types: Vec<Kind>,
    ) -> io::Result<Self> {
        Self::began(cluster, host, types, true).await
    }

    /// Start a client: it sends to the actors of a cluster without hosting keys or joining the membership.
    ///
    /// It returns after the first member table has arrived from a seed, or after a seed refused it.
    pub async fn client(
        cluster: Cluster,
        host: Arc<dyn Host>,
        types: Vec<Kind>,
    ) -> io::Result<Self> {
        Self::began(cluster, host, types, false).await
    }

    async fn began(
        cluster: Cluster,
        host: Arc<dyn Host>,
        types: Vec<Kind>,
        member: bool,
    ) -> io::Result<Self> {
        let (asks, taking) = mpsc::unbounded_channel();
        // Weak, so that the transport holding it does not keep the task of the node waiting for asks forever.
        let reporting = asks.downgrade();
        let heard: Heard = Arc::new(move |peer: Peer| {
            if let Some(asks) = reporting.upgrade() {
                let _ = asks.send(Ask::Heard(peer));
            }
        });
        let endpoint = Endpoint::start(Config {
            bind: member.then(|| cluster.bind.clone()),
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
        let id = endpoint.node().clone();
        let Declared {
            counts,
            kinds,
            write_timeouts,
            durables,
        } = Declared::of(types);
        let membership: Box<dyn Members> = if member {
            Box::new(Membership::new(
                id.clone(),
                counts.known(),
                cluster.seeds.clone(),
                cluster.timings,
                cluster.overlay,
            ))
        } else {
            Box::new(Directory::new(id.clone(), cluster.seeds.clone()))
        };
        let (members, watching) = watch::channel(membership.members());
        let (entered, joining) = oneshot::channel();
        // A tombstone lingers as long as a handover is given: an older copy of the key it fences may be on its way to
        // a replica for that long.
        let replication = Replication::new(
            id.clone(),
            cluster.limits.message - ENVELOPE,
            cluster.write_timeout,
            cluster.leave_timeout,
        );
        let node = Node {
            inner: Arc::new(Reach {
                id,
                asks,
                ids: AtomicI64::new(0),
                members: watching,
                message: cluster.limits.message,
                meter: endpoint.meter(),
                written: replication.written(),
            }),
        };
        let task = tokio::spawn(run(
            endpoint,
            Held {
                node: node.clone(),
                host,
                membership,
                // A client keeps no range, so it answers from the ring of the table without waiting for anything.
                placement: Placement::new(member.then(|| node.id().clone())),
                counts,
                handoff: Handoff::new(
                    node.id().clone(),
                    cluster.limits.message - ENVELOPE,
                    cluster.backoff,
                ),
                routing: Routing::new(node.id().clone()),
                replication,
                kinds,
                write_timeouts,
                durables,
                running: BTreeSet::new(),
                handed: None,
                told: false,
                standing: Standing::new(cluster.timings.dead_after / 2),
                toward: HashMap::new(),
                timings: cluster.timings,
            },
            members,
            taking,
            entered,
        ));
        match joining.await {
            Ok(Ok(())) => Ok(Self {
                node,
                task,
                handover: cluster.leave_timeout,
            }),
            Ok(Err(refused)) => Err(io::Error::other(refused)),
            Err(_) => Err(io::Error::other("the node stopped before it joined")),
        }
    }

    /// Say goodbye and wait for the node to be out, giving away what it keeps first.
    ///
    /// The keys it gives back are the ones it left behind: nobody took them in time, so they stay where they were.
    pub async fn leave(self) -> Vec<Entity> {
        // `leaving` takes the node out of the choice of owner, so nothing new is routed here. `left` takes it out of
        // the ring, which is what makes the keys it keeps belong to other nodes, and only then is there a handover.
        self.node.ask(Ask::Leave);
        self.node.ask(Ask::Depart);
        let owed = self.handed().await;
        self.node.ask(Ask::Stop);
        let _ = self.task.await;
        owed
    }

    /// Wait until every key this node stopped replicating has been taken, up to the deadline of the handover, and
    /// give back what is still owed then.
    async fn handed(&self) -> Vec<Entity> {
        let (answer, mut answered) = oneshot::channel();
        self.node.ask(Ask::Handed(answer));
        if let Ok(settled) = tokio::time::timeout(self.handover, &mut answered).await {
            // Everything was taken, or the node stopped: either way nothing is left to report.
            return settled.unwrap_or_default();
        }
        self.node.ask(Ask::Abandon);
        answered.await.unwrap_or_default()
    }

    /// Stop without a word, which is what a machine going away looks like to the others.
    pub async fn crash(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// Everything the task of a node owns.
struct Held {
    node: Node,
    host: Arc<dyn Host>,
    membership: Box<dyn Members>,
    placement: Placement,
    counts: Counts,
    handoff: Handoff,
    routing: Routing,
    replication: Replication,
    /// What this node knows of each type it has: how many replicas its keys have and who confirms a write.
    kinds: HashMap<String, crate::handoff::service::Kind>,
    /// How long an operation over a key waits for its replicas, for the types that set it themselves.
    write_timeouts: HashMap<String, core::time::Duration>,
    /// When the store of the system keeps the writes of each durable type.
    durables: HashMap<String, Durable>,
    /// The keys with an activation here, which the host reports as they come and go.
    running: BTreeSet<Entity>,
    /// Who is waiting for every key this node stopped replicating to be taken by the nodes that replicate it now.
    handed: Option<oneshot::Sender<Vec<Entity>>>,
    /// Whether the host was already told the cluster declared this node left.
    told: bool,
    /// Whether this node acts as the owner of the keys the ring gives it.
    standing: Standing,
    /// The node each request went to, and how it ends if that node never answers.
    toward: HashMap<i64, (NodeId, Outcome)>,
    timings: Timings,
}

/// Whether a node acts as the owner of the keys the ring gives it: it does while it sees a majority of the members
/// alive, and for a `grace` after it stopped seeing one.
///
/// A member goes `suspect` after a silence and comes back `alive` on the next answer, so the majority a node sees
/// flickers around the threshold, at times several times within a few milliseconds. A node that stepped down on
/// every flicker would end and start its activations again each time, and the ones it ended would linger in the
/// host beside the ones it started. The grace is half the `dead_after` the other side of a partition waits before
/// it takes the keys over, which leaves the other half for the step-down itself.
#[derive(Debug)]
struct Standing {
    /// When the node stopped seeing a majority, while it does not.
    lost: Option<Instant>,
    grace: core::time::Duration,
    acting: bool,
}

impl Standing {
    fn new(grace: core::time::Duration) -> Self {
        Self {
            lost: None,
            grace,
            acting: true,
        }
    }

    /// Note whether the node sees a majority `now`, and say whether it changed what the node acts as.
    fn observe(&mut self, majority: bool, now: Instant) -> bool {
        if majority {
            self.lost = None;
        } else {
            self.lost.get_or_insert(now);
        }
        let acting = self.lost.is_none_or(|at| now < at + self.grace);
        let changed = acting != self.acting;
        self.acting = acting;
        changed
    }

    /// When the grace ends, while the node is in it.
    fn due(&self) -> Option<Instant> {
        self.lost.filter(|_| self.acting).map(|at| at + self.grace)
    }
}

/// What the types a node starts with declare, split into the tables the node task keeps.
#[derive(Default)]
struct Declared {
    counts: Counts,
    kinds: HashMap<String, crate::handoff::service::Kind>,
    write_timeouts: HashMap<String, core::time::Duration>,
    durables: HashMap<String, Durable>,
}

impl Declared {
    fn of(types: Vec<Kind>) -> Self {
        let mut declared = Self::default();
        for kind in types {
            declared.counts.learn(&kind.actor, kind.replicas);
            if let Some(timeout) = kind.write_timeout {
                declared.write_timeouts.insert(kind.actor.clone(), timeout);
            }
            if let Some(durable) = kind.durable {
                declared.durables.insert(kind.actor.clone(), durable);
            }
            declared.kinds.insert(
                kind.actor,
                crate::handoff::service::Kind {
                    replicas: kind.replicas,
                    write: kind.write,
                    pinned: kind.pinned,
                },
            );
        }
        declared
    }
}

impl Held {
    fn owner(&self, actor: &str, key: &str) -> Option<NodeId> {
        acting(
            self.placement
                .owner(actor, key, &self.counts, &self.handoff),
            key,
            self.node.id(),
            self.standing.acting,
        )
    }

    /// The replica set of a key and what an operation over it needs to know, or nothing if no member hosts the type.
    fn around(&self, actor: &str, key: &str) -> Option<Around> {
        let replicas = self
            .placement
            .replicas(actor, key, &self.counts, &self.handoff);
        if replicas.is_empty() {
            return None;
        }
        // A `leaving` member counts: it stays in the ring and keeps replicating until it departs, so it can be the
        // only replica of the new set holding what the owner before this one confirmed.
        let answering = self
            .membership
            .members()
            .into_iter()
            .filter(|member| member.status != Status::Dead)
            .map(|member| member.node)
            .collect::<BTreeSet<_>>();
        let wanted = replicas
            .iter()
            .filter(|replica| answering.contains(*replica))
            .count()
            .max(1);
        Some(Around {
            wanted,
            write: self
                .kinds
                .get(actor)
                .map_or(Write::Majority, |kind| kind.write),
            write_timeout: self.write_timeouts.get(actor).copied(),
            durable: self.durables.get(actor).copied(),
            replicas,
        })
    }

    /// Send a command toward the owner of its key, remembering where a request went so a lost node can fail it.
    fn route(&mut self, sender: &Sender, command: Command) {
        let owner = self.owner(command.actor(), command.key());
        if let (Some(owner), Some(Target::Reply { id, .. })) = (owner.as_ref(), command.reply()) {
            let failure = Outcome::unreached(command.actor(), command.key());
            self.toward.insert(*id, (owner.clone(), failure));
        }
        let routed = Routed {
            command,
            origin: self.node.id().clone(),
            attempt: 1,
        };
        let decision = self.routing.route(routed, owner);
        self.act(sender, decision);
    }

    /// Stop waiting for a request of this node, and send its cancellation toward whoever runs the key.
    fn cancel(&mut self, sender: &Sender, cancel: Cancel) {
        if let Target::Reply { node, id } = &cancel.request
            && node == self.node.id()
        {
            self.toward.remove(id);
        }
        let owner = self.owner(&cancel.actor, &cancel.key);
        if let Some(decision) = cancelling(cancel, owner) {
            self.act(sender, decision);
        }
    }

    /// Take in what a type declares, and start counting it the first time it is seen.
    fn learn(&mut self, kind: Kind, now: f64) {
        self.kinds.insert(
            kind.actor.clone(),
            crate::handoff::service::Kind {
                replicas: kind.replicas,
                write: kind.write,
                pinned: kind.pinned,
            },
        );
        match kind.write_timeout {
            Some(timeout) => self.write_timeouts.insert(kind.actor.clone(), timeout),
            None => self.write_timeouts.remove(&kind.actor),
        };
        match kind.durable {
            Some(durable) => self.durables.insert(kind.actor.clone(), durable),
            None => self.durables.remove(&kind.actor),
        };
        if !self.counts.counted(&kind.actor) {
            self.counts.learn(&kind.actor, kind.replicas);
            self.placement.learned(&kind.actor);
            self.membership.know(&BTreeSet::from([kind.actor]), now);
            self.membership.mark();
        }
    }

    /// Activate a key from its replicas, or fail at once when no member hosts its type.
    fn activate(
        &mut self,
        actor: String,
        key: String,
        initial: Option<Pages>,
        answer: oneshot::Sender<Result<Option<State>, Failure>>,
    ) {
        if let Some(around) = self.around(&actor, &key) {
            self.replication
                .activate(&actor, &key, initial, &around, answer, Instant::now());
        } else {
            let failure = Failure::Unavailable(format!("no member hosts {actor}"));
            let _ = answer.send(Err(failure.clone()));
            self.host.observe(Event::WriteFailed {
                actor,
                key,
                operation: Operation::Activate,
                failure,
            });
        }
    }

    /// Carry out what the routing decided.
    fn act(&mut self, sender: &Sender, decision: Decision) {
        match decision {
            Decision::Send { to, message } => {
                // Never too large: `Node::route` measured the command in its largest form before it came in.
                let _ = sender.send(&to, ACTORS, &routing::encode(&message));
            }
            Decision::Hand(command) => {
                let host = Arc::clone(&self.host);
                host.hand(&self.node, command);
            }
            Decision::Cancel(cancel) => {
                self.host
                    .cancel(&cancel.actor, &cancel.key, &cancel.request);
            }
            // A message nobody waits for ends here, and the sender was told delivery is at most once.
            Decision::Refuse {
                command,
                outcome,
                why,
            } => match command.reply().cloned() {
                Some(target) => self.answer(sender, &target, outcome),
                None => self.host.observe(Event::MessageDropped {
                    actor: command.actor().to_owned(),
                    key: command.key().to_owned(),
                    reason: why,
                }),
            },
        }
    }

    fn answer(&mut self, sender: &Sender, target: &Target, outcome: Outcome) {
        let Target::Reply { node, id } = target else {
            return;
        };
        if *node == *self.node.id() {
            self.toward.remove(id);
            self.host.settle(*id, outcome);
            return;
        }
        let payload = routing::encode_answer(&Answer { id: *id, outcome });
        // Never too large: `Node::answer` replaced an answer that does not fit, and a refusal is a few names.
        let _ = sender.send(&Destination::Node(node.clone()), REPLIES, &payload);
    }

    /// Bring a key back when a write from another node left the active mark on the copy kept here.
    ///
    /// It covers the owner that receives the mark after its own sweep already ran, which is the common order when a
    /// key moves: the write of the node that had it crosses the change.
    fn arrived(&mut self, actor: &str, key: &str) {
        if self.replication.replica().marked(actor, key)
            && self.owner(actor, key).as_ref() == Some(self.node.id())
        {
            self.host.attach(&self.node, actor, key);
        }
    }

    /// What the transport says of a peer.
    fn heard(&mut self, peer: Peer, now: f64) {
        match peer {
            Peer::Lost(node) => {
                self.membership.lost(&node);
                self.host.observe(Event::ConnectionLost { node });
            }
            // A client may no longer list the node, one that left or that another process took the address of, and
            // still wait on it.
            Peer::Unreached(node) => {
                if self.membership.unreached(&node, now) {
                    self.forsake(&[node]);
                }
            }
        }
    }

    /// Fail every request sent to a node the cluster gave up on, instead of waiting for the deadline.
    fn unreachable(&mut self, members: &[Member]) {
        let gone: Vec<NodeId> = members
            .iter()
            .filter(|member| member.status == Status::Dead)
            .map(|member| member.node.clone())
            .collect();
        self.forsake(&gone);
    }

    /// Fail the requests sent toward `gone`, whose answers will not come.
    fn forsake(&mut self, gone: &[NodeId]) {
        if gone.is_empty() {
            return;
        }
        let failed: Vec<(i64, Outcome)> = self
            .toward
            .iter()
            .filter(|(_, (node, _))| gone.contains(node))
            .map(|(id, (_, failure))| (*id, failure.clone()))
            .collect();
        for (id, failure) in failed {
            self.toward.remove(&id);
            self.host.settle(id, failure);
        }
    }
}

async fn run(
    mut endpoint: Endpoint,
    mut held: Held,
    members: watch::Sender<Vec<Member>>,
    mut asks: mpsc::UnboundedReceiver<Ask>,
    entered: oneshot::Sender<Result<(), String>>,
) {
    let started = tokio::time::Instant::now();
    let now = move || started.elapsed().as_secs_f64();
    let timings = held.timings;
    let mut heartbeat = tokio::time::interval(timings.heartbeat);
    let mut graft = tokio::time::interval(timings.graft_after);
    let mut shuffle = tokio::time::interval(timings.shuffle_every);
    let mut anti_entropy = tokio::time::interval(timings.anti_entropy);
    let sender = endpoint.sender();
    let mut entered = Some(entered);
    held.membership.join();
    held.membership.mark();
    flush(&sender, &mut held, &members, &mut entered);
    loop {
        tokio::select! {
            arrived = endpoint.recv() => match arrived {
                Some(Ok(envelope)) => {
                    if let Some(from) = &envelope.from {
                        held.membership.heard(from, now());
                    }
                    received(&sender, &mut held, &envelope.name, &envelope.payload, now());
                }
                Some(Err(refused)) => {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(Err(refused));
                    }
                }
                None => break,
            },
            _ = heartbeat.tick() => {
                held.membership.probe(now());
                held.membership.expire(now());
            }
            _ = graft.tick() => held.membership.graft(now()),
            _ = shuffle.tick() => held.membership.shuffle(now()),
            _ = anti_entropy.tick() => held.membership.anti_entropy(),
            () = deadline(earliest(&held)) => operations(&mut held),
            ask = asks.recv() => match ask {
                Some(ask) => {
                    if !asked(&sender, &mut held, ask, now()) {
                        flush(&sender, &mut held, &members, &mut entered);
                        break;
                    }
                }
                None => break,
            },
        }
        flush(&sender, &mut held, &members, &mut entered);
    }
    endpoint.close(false).await;
}

/// What arrived on one of the bands of the node.
fn received(sender: &Sender, held: &mut Held, name: &str, payload: &[u8], now: f64) {
    match name {
        MEMBERSHIP => held.membership.receive(payload, now),
        ACTORS => {
            let Ok(message) = routing::decode(payload) else {
                return;
            };
            // A client is in no ring, so nothing is routed to it: what arrives says its table is out of date.
            if matches!(message, routing::Message::WrongOwner(_)) {
                held.membership.refresh();
            }
            let owner = match &message {
                routing::Message::Routed(routed) | routing::Message::WrongOwner(routed) => {
                    held.owner(routed.command.actor(), routed.command.key())
                }
                routing::Message::Cancel(cancel) => held.owner(&cancel.actor, &cancel.key),
            };
            let decision = held.routing.receive(message, owner);
            held.act(sender, decision);
        }
        REPLIES => {
            let Ok(answer) = routing::decode_answer(payload) else {
                return;
            };
            held.toward.remove(&answer.id);
            held.host.settle(answer.id, answer.outcome);
        }
        REPLICATION => {
            let Ok(message) = replication::decode(payload) else {
                return;
            };
            // A node meets a type by keeping copies of it too, and not only by running it.
            if !held.counts.met(message_actor(&message)) {
                held.host.meet(&held.node, message_actor(&message));
            }
            match message {
                replication::Message::Request(request) => {
                    let receiving = held.handoff.arriving(request.actor(), request.key());
                    held.replication.request(request, receiving);
                }
                replication::Message::Reply(reply) => held.replication.answer(reply),
                replication::Message::Pull(pull) => {
                    held.handoff.receive(pull, held.replication.replica_mut());
                }
            }
        }
        _ => {}
    }
}

/// What the host asked of the node. `false` says the node is to stop.
fn asked(sender: &Sender, held: &mut Held, ask: Ask, now: f64) -> bool {
    match ask {
        Ask::Route(command) => held.route(sender, command),
        Ask::Answer { target, outcome } => held.answer(sender, &target, outcome),
        Ask::Toward { id, node, failure } => {
            held.toward.insert(id, (node, failure));
        }
        Ask::Cancel(cancel) => held.cancel(sender, cancel),
        Ask::Learn(kind) => held.learn(kind, now),
        Ask::GaveUp(actor) => held.counts.give_up(&actor),
        Ask::Activate {
            actor,
            key,
            initial,
            answer,
        } => held.activate(actor, key, initial, answer),
        Ask::Commit {
            actor,
            key,
            lease,
            pages,
            active,
            answer,
        } => {
            let around = held.around(&actor, &key);
            let entity = (actor, key);
            held.replication.commit(
                &entity,
                lease,
                pages,
                active,
                around.as_ref(),
                answer,
                Instant::now(),
            );
        }
        Ask::Delete {
            actor,
            key,
            lease,
            active,
            answer,
        } => {
            let around = held.around(&actor, &key);
            let entity = (actor, key);
            held.replication.delete(
                &entity,
                lease,
                active,
                around.as_ref(),
                answer,
                Instant::now(),
            );
        }
        Ask::Release { actor, key, lease } => held.replication.forget(&actor, &key, lease),
        Ask::Placed { actor, key, answer } => {
            let _ = answer.send(Placed {
                owner: held.owner(&actor, &key),
                replicas: held
                    .placement
                    .replicas(&actor, &key, &held.counts, &held.handoff),
            });
        }
        Ask::Stored(answer) => {
            let _ = answer.send(held.replication.replica().kept());
        }
        Ask::FromStore { id, kept } => held.replication.stored(id, kept),
        Ask::Attached { actor, key } => {
            // Handed here before a change moved the key, and started after the sweep of that change read what runs.
            if held.owner(&actor, &key).as_ref() != Some(held.node.id()) {
                held.host.release(&actor, &key);
            }
            held.running.insert((actor, key));
        }
        Ask::Detached { actor, key } => {
            held.running.remove(&(actor, key));
        }
        Ask::Handed(answer) => held.handed = Some(answer),
        Ask::Abandon => {
            if let Some(handed) = held.handed.take() {
                let owed = held.handoff.owed();
                for event in abandoned(&owed) {
                    held.host.observe(event);
                }
                let _ = handed.send(owed);
            }
        }
        Ask::Heard(peer) => held.heard(peer, now),
        Ask::Leave => {
            held.routing.stopped = true;
            held.membership.leave(now);
        }
        Ask::Depart => held.membership.depart(now),
        Ask::Stop => return false,
    }
    true
}

/// Wait for the earliest deadline the operations have, or forever while there is none.
async fn deadline(due: Option<Instant>) {
    match due {
        Some(due) => tokio::time::sleep_until(due).await,
        None => core::future::pending().await,
    }
}

/// The earliest moment anything on this node has to be looked at again.
fn earliest(held: &Held) -> Option<Instant> {
    [
        held.replication.due(),
        held.handoff.due(),
        held.standing.due(),
    ]
    .into_iter()
    .flatten()
    .min()
}

/// Carry out the deadlines that came due, start again the activations another owner fenced, and ask about the
/// tombstones that have lingered.
fn operations(held: &mut Held) {
    let now = Instant::now();
    for entity in held.replication.fired(now) {
        let Some(around) = held.around(&entity.0, &entity.1) else {
            continue;
        };
        held.replication.again(&entity, &around);
    }
    for entity in held.replication.burying(now) {
        // A range of this node that is still arriving may hand it an older copy of the key yet, and a tombstone
        // forgotten before that copy lands would let it back in. It is asked about again one linger later.
        if held.handoff.arriving(&entity.0, &entity.1) {
            continue;
        }
        let Some(around) = held.around(&entity.0, &entity.1) else {
            continue;
        };
        held.replication
            .bury(&entity, &around.replicas, around.durable.is_some());
    }
    held.handoff.fired(now, held.replication.replica());
}

fn flush(
    sender: &Sender,
    held: &mut Held,
    members: &watch::Sender<Vec<Member>>,
    entered: &mut Option<oneshot::Sender<Result<(), String>>>,
) {
    for out in held.membership.take() {
        let _ = sender.send(&out.to, MEMBERSHIP, &membership::encode(&out.message));
    }
    // A write from another node left the active mark on a copy kept here: the key comes back if this node owns it.
    for (actor, key) in held.replication.arrived() {
        held.arrived(&actor, &key);
    }
    // A deletion, a copy or a burial laid a tombstone here, which lingers from now on.
    held.replication.lay(Instant::now());
    let changed = held.membership.changed();
    if changed {
        let seen = held.membership.members();
        held.handoff.knows(
            held.kinds.clone(),
            seen.iter()
                .filter(|member| !matches!(member.status, Status::Dead | Status::Left))
                .map(|member| member.node.clone())
                .collect(),
        );
        held.placement
            .update(&seen, &held.counts, &mut held.handoff);
        held.unreachable(&seen);
        let _ = members.send(seen.clone());
        held.host.members(seen);
        // After the table itself, so that an observer reading the members finds them as the event says.
        for moved in held.membership.transitions() {
            held.host.observe(moved.into());
        }
    }
    let standing = held
        .standing
        .observe(held.membership.majority(), Instant::now());
    // A ring, an owner or a copy that arrived may have moved a key, so what this node keeps is decided again; so
    // does a node that stopped or started acting as an owner.
    if held.handoff.resweep() || changed || standing {
        held.placement.settle(&mut held.handoff);
        swept(held);
    }
    for out in held.handoff.take() {
        held.replication.hand(out);
    }
    for out in held.replication.take() {
        let payload = replication::encode(&out.message);
        let _ = sender.send(&Destination::Node(out.to), REPLICATION, &payload);
    }
    // The host carries what the replication asks of the store out, and answers through the channel of the node.
    for request in held.replication.storing() {
        held.host.store(&held.node, request);
    }
    for event in held
        .replication
        .observed()
        .into_iter()
        .chain(held.handoff.observed())
    {
        held.host.observe(event);
    }
    if !held.handoff.owing()
        && let Some(handed) = held.handed.take()
    {
        let _ = handed.send(Vec::new());
    }
    if held.membership.removed() && !held.told {
        held.told = true;
        held.host.removed();
    }
    if held.membership.joined()
        && let Some(entered) = entered.take()
    {
        let _ = entered.send(Ok(()));
    }
}

/// The type a message on the band of `replication` belongs to.
fn message_actor(message: &replication::Message) -> &str {
    match message {
        replication::Message::Request(request) => request.actor(),
        replication::Message::Reply(reply) => reply.actor(),
        replication::Message::Pull(pull) => pull.actor(),
    }
}

/// Room left around the pages of a message for the envelope that carries them.
pub(crate) const ENVELOPE: usize = 64 * 1024;

/// Decide again what this node keeps: the keys to bring back, the activations to end, and the keys to give away.
fn swept(held: &mut Held) {
    let kept: Vec<Kept> = held
        .counts
        .known()
        .iter()
        .flat_map(|actor| {
            held.replication
                .replica()
                .keys(actor)
                .into_iter()
                .map(|key| Kept {
                    active: held.replication.replica().marked(actor, &key),
                    actor: actor.clone(),
                    key,
                })
        })
        .collect();
    let decided = sweep(
        &kept,
        &held.running,
        held.node.id(),
        &Where {
            placement: &held.placement,
            counts: &held.counts,
            handoff: &held.handoff,
            node: held.node.id(),
            majority: held.standing.acting,
        },
    );
    for (actor, key) in decided.end {
        held.host.release(&actor, &key);
    }
    for (actor, key) in decided.reattach {
        held.host.attach(&held.node, &actor, &key);
    }
    held.handoff
        .give(decided.given, held.replication.replica(), Instant::now());
}

/// Where the keys are, as the sweep reads it.
struct Where<'a> {
    placement: &'a Placement,
    counts: &'a Counts,
    handoff: &'a Handoff,
    node: &'a NodeId,
    majority: bool,
}

/// The owner of `key` as `node` acts on it: nobody, where the ring gives the key to `node` and `node` does not see a
/// majority of the members alive (`Standing`).
///
/// Such a node may be on the small side of a partition, whose other side takes its keys over once it declares it dead.
/// It stops seeing the others `alive` a `dead_after` before that, and gives its keys up then, so that an activation it
/// kept does not answer from a state the new owner has written past. A pinned key has no other node to go to.
fn acting(owner: Option<NodeId>, key: &str, node: &NodeId, majority: bool) -> Option<NodeId> {
    match owner {
        Some(owner) if owner == *node && !majority && pinned(key).is_none() => None,
        owner => owner,
    }
}

impl casty_core::handoff::sweep::Placement for Where<'_> {
    fn replicas(&self, actor: &str, key: &str) -> Vec<NodeId> {
        self.placement
            .replicas(actor, key, self.counts, self.handoff)
    }

    fn owner(&self, actor: &str, key: &str) -> Option<NodeId> {
        acting(
            self.placement.owner(actor, key, self.counts, self.handoff),
            key,
            self.node,
            self.majority,
        )
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use tokio::time::Instant;

    use super::Standing;

    #[test]
    fn a_node_keeps_acting_through_a_majority_that_flickers_and_steps_down_once_it_stays_lost() {
        let grace = Duration::from_secs(5);
        let mut standing = Standing::new(grace);
        let start = Instant::now();
        assert!(standing.acting);
        assert_eq!(standing.due(), None);

        // Lost and seen again within the grace: the node acts on, and no sweep is asked for.
        assert!(!standing.observe(false, start));
        assert_eq!(standing.due(), Some(start + grace));
        assert!(!standing.observe(true, start + Duration::from_millis(20)));
        assert!(standing.acting);
        assert_eq!(standing.due(), None);

        // Lost for the whole grace: the node steps down when it ends, and not before.
        let lost = start + Duration::from_secs(1);
        assert!(!standing.observe(false, lost));
        assert!(!standing.observe(false, lost + grace - Duration::from_millis(1)));
        assert!(standing.acting);
        assert!(standing.observe(false, lost + grace));
        assert!(!standing.acting);
        assert_eq!(standing.due(), None);

        // Seen again after that: it acts at once.
        assert!(standing.observe(true, lost + grace + Duration::from_secs(3)));
        assert!(standing.acting);
    }
}
