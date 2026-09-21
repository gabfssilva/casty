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

use casty_core::handoff::sweep::{Kept, sweep};
use casty_core::mailbox::{Command, Deliver, Start};
use casty_core::membership::table::Status;
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_core::replication::messages::Write;
use casty_core::store::{Held as State, Pages};
use casty_net::endpoint::{Config, Endpoint, Sender};
use casty_net::pool::Target as Destination;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::handoff::service::Handoff;
use crate::membership::directory::Directory;
use crate::membership::runner::Cluster;
use crate::membership::service::{Member, Membership, Outgoing};
use crate::membership::wire as membership;
use crate::placement::{Counts, Placement};
use crate::replication::service::{Around, Entity, Failure, Replication};
use crate::replication::wire::{self as replication};
use crate::routing::service::{Decision, Routing};
use crate::routing::wire::{self as routing, Answer, Routed};

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

    /// The cluster declared this node left. It never comes back under the same identity: what it knew is gone.
    fn removed(&self) {}
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
    Forget(i64),
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
        pages: Pages,
        active: bool,
        answer: oneshot::Sender<Result<(), Failure>>,
    },
    Release {
        actor: String,
        key: String,
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
}

impl Node {
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.inner.id
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

    /// Send a message to the entity `(actor, key)`, answering `reply` when there is one waiting.
    pub fn deliver(&self, actor: &str, key: &str, message: Vec<u8>, reply: Option<Target>) {
        self.ask(Ask::Route(Command::Deliver(Deliver {
            actor: actor.to_owned(),
            key: key.to_owned(),
            message,
            reply,
        })));
    }

    /// Create the key, from `state` or from the default of its type, and activate it. Nothing answers.
    pub fn start(&self, actor: &str, key: &str, state: Option<Vec<u8>>) {
        self.ask(Ask::Route(Command::Start(Start {
            actor: actor.to_owned(),
            key: key.to_owned(),
            state,
        })));
    }

    /// Answer the request `target` waits for, wherever it waits.
    pub fn answer(&self, target: Target, outcome: Outcome) {
        self.ask(Ask::Answer { target, outcome });
    }

    /// Note the node a request went to, and how it ends if that node never answers.
    pub fn toward(&self, id: i64, node: NodeId, failure: Outcome) {
        self.ask(Ask::Toward { id, node, failure });
    }

    /// Stop waiting for the answer of `id`, which a timeout or a cancellation does.
    pub fn forget(&self, id: i64) {
        self.ask(Ask::Forget(id));
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

    /// Write the state of a key this node owns. Without `active` the key lets go of it instead.
    pub async fn commit(
        &self,
        actor: &str,
        key: &str,
        pages: Pages,
        active: bool,
    ) -> Result<(), Failure> {
        let (answer, answered) = oneshot::channel();
        self.ask(Ask::Commit {
            actor: actor.to_owned(),
            key: key.to_owned(),
            pages,
            active,
            answer,
        });
        stopped(answered.await, actor, key)
    }

    /// Drop the owner of a key this node stopped holding. Its replica keeps the state.
    pub fn release(&self, actor: &str, key: &str) {
        self.ask(Ask::Release {
            actor: actor.to_owned(),
            key: key.to_owned(),
        });
    }

    /// Tell the cluster this node is going, so that nothing new is routed to it while it finishes what it is on.
    pub fn leaving(&self) {
        self.ask(Ask::Leave);
    }

    /// An activation started here, which is what keeps a sweep from starting a second one.
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
    members: watch::Receiver<Vec<Member>>,
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
        let endpoint = Endpoint::start(Config {
            bind: member.then(|| cluster.bind.clone()),
            advertise: cluster.advertise.clone(),
            cluster: cluster.name.clone(),
            codec: "msgpack".to_owned(),
            tls: cluster.tls.clone(),
            compression: cluster.compression.clone(),
            min_compressed: 4096,
            address_map: cluster.address_map.clone(),
            limits: cluster.limits,
        })
        .await?;
        let id = endpoint.node().clone();
        let mut counts = Counts::default();
        let mut kinds = HashMap::new();
        for kind in types {
            counts.learn(&kind.actor, kind.replicas);
            kinds.insert(
                kind.actor,
                crate::handoff::service::Kind {
                    replicas: kind.replicas,
                    write: kind.write,
                },
            );
        }
        let membership = if member {
            Table::Member(Box::new(Membership::new(
                id.clone(),
                counts.known(),
                cluster.seeds.clone(),
                cluster.timings,
                cluster.overlay,
            )))
        } else {
            Table::Client(Box::new(Directory::new(id.clone(), cluster.seeds.clone())))
        };
        let (members, watching) = watch::channel(membership.members());
        let (asks, taking) = mpsc::unbounded_channel();
        let (entered, joining) = oneshot::channel();
        let node = Node {
            inner: Arc::new(Reach {
                id,
                asks,
                ids: AtomicI64::new(0),
                members: watching.clone(),
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
                replication: Replication::new(
                    node.id().clone(),
                    cluster.limits.message - ENVELOPE,
                    cluster.write_timeout,
                ),
                kinds,
                running: BTreeSet::new(),
                handed: None,
                told: false,
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
                members: watching,
                task,
                handover: cluster.leave_timeout,
            }),
            Ok(Err(refused)) => Err(io::Error::other(refused)),
            Err(_) => Err(io::Error::other("the node stopped before it joined")),
        }
    }

    /// Wait until what this node sees satisfies `settled`, or until it stops.
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

    /// Wait until every key this node stopped replicating has been taken, up to the deadline of the handover.
    async fn handed(&self) -> Vec<Entity> {
        let (answer, answered) = oneshot::channel();
        self.node.ask(Ask::Handed(answer));
        match tokio::time::timeout(self.handover, answered).await {
            Ok(Ok(owed)) => owed,
            // The node stopped, or nobody took what it owed: either way it goes without it.
            Ok(Err(_)) | Err(_) => Vec::new(),
        }
    }

    /// Stop without a word, which is what a machine going away looks like to the others.
    pub async fn crash(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// Where a node gets the member table: gossip, or asking a member for the whole of it.
///
/// A client is in no ring and hosts nothing. It keeps the table to know where to send, asks for it again when a
/// message comes back from the wrong owner, and is never in it itself.
enum Table {
    Member(Box<Membership>),
    Client(Box<Directory>),
}

impl Table {
    fn members(&self) -> Vec<Member> {
        match self {
            Self::Member(membership) => membership.members(),
            Self::Client(directory) => directory.members(),
        }
    }

    fn take(&mut self) -> Vec<Outgoing> {
        match self {
            Self::Member(membership) => membership.take(),
            Self::Client(directory) => directory.take(),
        }
    }

    fn receive(&mut self, payload: &[u8], now: f64) {
        match self {
            Self::Member(membership) => membership.receive(payload, now),
            Self::Client(directory) => directory.receive(payload),
        }
    }

    fn changed(&mut self) -> bool {
        let changed = match self {
            Self::Member(membership) => &mut membership.changed,
            Self::Client(directory) => &mut directory.changed,
        };
        core::mem::take(changed)
    }

    fn mark(&mut self) {
        match self {
            Self::Member(membership) => membership.changed = true,
            Self::Client(directory) => directory.changed = true,
        }
    }

    fn joined(&self) -> bool {
        match self {
            Self::Member(membership) => membership.joined,
            Self::Client(directory) => directory.joined,
        }
    }

    fn removed(&self) -> bool {
        match self {
            Self::Member(membership) => membership.removed,
            Self::Client(_) => false,
        }
    }

    /// Ask for the whole table again, which only a client does: the one it holds is older than the owner's.
    fn refresh(&mut self) {
        if let Self::Client(directory) = self {
            directory.ask();
        }
    }
}

/// Everything the task of a node owns.
struct Held {
    node: Node,
    host: Arc<dyn Host>,
    membership: Table,
    placement: Placement,
    counts: Counts,
    handoff: Handoff,
    routing: Routing,
    replication: Replication,
    /// What this node knows of each type it has: how many replicas its keys have and who confirms a write.
    kinds: HashMap<String, crate::handoff::service::Kind>,
    /// The keys with an activation here, which the host reports as they come and go.
    running: BTreeSet<Entity>,
    /// Who is waiting for every key this node stopped replicating to be taken by the nodes that replicate it now.
    handed: Option<oneshot::Sender<Vec<Entity>>>,
    /// Whether the host was already told the cluster declared this node left.
    told: bool,
    /// The node each request went to, and how it ends if that node never answers.
    toward: HashMap<i64, (NodeId, Outcome)>,
    timings: crate::membership::service::Timings,
}

impl Held {
    fn owner(&self, actor: &str, key: &str) -> Option<NodeId> {
        self.placement
            .owner(actor, key, &self.counts, &self.handoff)
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
            replicas,
        })
    }

    /// Carry out what the routing decided.
    fn act(&mut self, sender: &Sender, decision: Decision) {
        match decision {
            Decision::Send { to, message } => {
                let _ = sender.send(&to, ACTORS, &routing::encode(&message));
            }
            Decision::Hand(command) => {
                let host = Arc::clone(&self.host);
                host.hand(&self.node, command);
            }
            // A message nobody waits for ends here, and the sender was told delivery is at most once.
            Decision::Refuse {
                command, outcome, ..
            } => {
                if let Some(target) = command.reply().cloned() {
                    self.answer(sender, &target, outcome);
                }
            }
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

    /// Fail every request sent to a node the cluster gave up on, instead of waiting for the deadline.
    fn unreachable(&mut self, members: &[Member]) {
        let gone: Vec<NodeId> = members
            .iter()
            .filter(|member| member.status == Status::Dead)
            .map(|member| member.node.clone())
            .collect();
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
    match &mut held.membership {
        Table::Member(membership) => membership.join(),
        // A client is in no table: it asks a member for the one it holds, and again every `anti_entropy`.
        Table::Client(directory) => directory.ask(),
    }
    held.membership.mark();
    flush(&sender, &mut held, &members, &mut entered);
    loop {
        tokio::select! {
            arrived = endpoint.recv() => match arrived {
                Some(Ok(envelope)) => received(&sender, &mut held, &envelope.name, &envelope.payload, now()),
                Some(Err(refused)) => {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(Err(refused));
                    }
                }
                None => break,
            },
            _ = heartbeat.tick() => {
                if let Table::Member(membership) = &mut held.membership {
                    membership.probe(now());
                    membership.expire(now());
                }
            }
            _ = graft.tick() => {
                if let Table::Member(membership) = &mut held.membership {
                    membership.graft(now());
                }
            }
            _ = shuffle.tick() => {
                if let Table::Member(membership) = &mut held.membership {
                    membership.shuffle(now());
                }
            }
            _ = anti_entropy.tick() => match &mut held.membership {
                Table::Member(membership) => membership.anti_entropy(),
                Table::Client(directory) => directory.ask(),
            },
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
        Ask::Route(command) => {
            let owner = held.owner(command.actor(), command.key());
            if let (Some(owner), Some(Target::Reply { id, .. })) = (owner.as_ref(), command.reply())
            {
                let failure = Outcome::unreached(command.actor(), command.key());
                held.toward.insert(*id, (owner.clone(), failure));
            }
            let routed = Routed {
                command,
                origin: held.node.id().clone(),
                attempt: 1,
            };
            let decision = held.routing.route(routed, owner);
            held.act(sender, decision);
        }
        Ask::Answer { target, outcome } => held.answer(sender, &target, outcome),
        Ask::Toward { id, node, failure } => {
            held.toward.insert(id, (node, failure));
        }
        Ask::Forget(id) => {
            held.toward.remove(&id);
        }
        Ask::Learn(kind) => {
            held.kinds.insert(
                kind.actor.clone(),
                crate::handoff::service::Kind {
                    replicas: kind.replicas,
                    write: kind.write,
                },
            );
            if !held.counts.counted(&kind.actor) {
                held.counts.learn(&kind.actor, kind.replicas);
                held.placement.learned(&kind.actor);
                if let Table::Member(membership) = &mut held.membership {
                    membership.know(&BTreeSet::from([kind.actor]), now);
                }
                held.membership.mark();
            }
        }
        Ask::GaveUp(actor) => held.counts.give_up(&actor),
        Ask::Activate {
            actor,
            key,
            initial,
            answer,
        } => match held.around(&actor, &key) {
            Some(around) => {
                held.replication
                    .activate(&actor, &key, initial, &around, answer, Instant::now());
            }
            None => {
                let _ = answer.send(Err(Failure::Unavailable(format!(
                    "no member hosts {actor}"
                ))));
            }
        },
        Ask::Commit {
            actor,
            key,
            pages,
            active,
            answer,
        } => held
            .replication
            .commit(&actor, &key, pages, active, answer, Instant::now()),
        Ask::Release { actor, key } => held.replication.forget(&actor, &key),
        Ask::Attached { actor, key } => {
            held.running.insert((actor, key));
        }
        Ask::Detached { actor, key } => {
            held.running.remove(&(actor, key));
        }
        Ask::Handed(answer) => held.handed = Some(answer),
        Ask::Leave => {
            held.routing.stopped = true;
            if let Table::Member(membership) = &mut held.membership {
                membership.leave(now);
            }
        }
        Ask::Depart => {
            if let Table::Member(membership) = &mut held.membership {
                membership.depart(now);
            }
        }
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
    match (held.replication.due(), held.handoff.due()) {
        (Some(one), Some(other)) => Some(one.min(other)),
        (due, None) | (None, due) => due,
    }
}

/// Carry out the deadlines that came due, and start again the activations another owner fenced.
fn operations(held: &mut Held) {
    let now = Instant::now();
    for entity in held.replication.fired(now) {
        let Some(around) = held.around(&entity.0, &entity.1) else {
            continue;
        };
        held.replication.again(&entity, &around);
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
    }
    // A ring, an owner or a range that arrived may have moved a key, so what this node keeps is decided again.
    if held.handoff.resweep() || changed {
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
        replication::Message::Reply(reply) => replication::actor_of(reply),
        replication::Message::Pull(pull) => pull.actor(),
    }
}

/// Room left around the pages of a message for the envelope that carries them.
const ENVELOPE: usize = 64 * 1024;

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
        },
    );
    for (actor, key) in decided.end {
        held.host.release(&actor, &key);
    }
    for (actor, key) in decided.reattach {
        held.host.attach(&held.node, &actor, &key);
    }
    held.handoff.give(decided.given, Instant::now());
}

/// Where the keys are, as the sweep reads it.
struct Where<'a> {
    placement: &'a Placement,
    counts: &'a Counts,
    handoff: &'a Handoff,
}

impl casty_core::handoff::sweep::Placement for Where<'_> {
    fn replicas(&self, actor: &str, key: &str) -> Vec<NodeId> {
        self.placement
            .replicas(actor, key, self.counts, self.handoff)
    }

    fn owner(&self, actor: &str, key: &str) -> Option<NodeId> {
        self.placement.owner(actor, key, self.counts, self.handoff)
    }
}
