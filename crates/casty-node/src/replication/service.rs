//! The component `replication`: the replica this node keeps, and the keys it owns.
//!
//! The cores decide; this drives them. An operation ends when a core returns an outcome, when the replicas take too
//! long, or when a later one starts — an answer that arrives after that is dropped by its epoch or its stamp.
//!
//! A deleted key leaves a tombstone on each replica. The replica keeps it for `linger`, the time an older copy of the
//! key may still be on its way to it in a handover or a range, then asks the other replicas of the key whether they
//! keep anything older (`Bury`), and forgets it once every one of them has answered that they do not. A replica that
//! missed the deletion takes the tombstone in place of its copy when it is asked, so no copy of the state is left to
//! bring the key back. Until then, a replica that does not answer keeps every other one holding the tombstone.
//!
//! The keys of a durable type are kept by the store of the system too. What an owner asks of it, and the drop of a
//! deletion a replica is about to forget, go out as numbered requests (`storing`) that the host performs and answers
//! (`stored`). A write of a type that saves every write is confirmed once the store kept it; one that saves on a
//! schedule is confirmed at once, and its latest write is saved once the period is over.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use casty_core::node::NodeId;
use casty_core::replication::messages::{Reply, Request, Stamp, Write};
use casty_core::replication::owner::{Outcome, Owner, Send, Step, TooLarge};
use casty_core::replication::replica::Replica;
use casty_core::store::{Durable, Held, Pages, Storage, Stored};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::wire::{Message, actor_of, key_of};
use crate::events::{self, Event};

/// A key, by the type it belongs to and its name.
pub type Entity = (String, String);

/// Why an operation did not go through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// Too few replicas took part, or they did not answer in time.
    Unavailable(String),
    /// Another owner took the key. It never reaches the body: the activation ends on it.
    Fencing(String),
    /// A page of the state has a name that leaves no room in a message for its data. A page larger than a message
    /// is not one: it travels across as many as it takes.
    TooLarge(String),
}

/// A request of this node to one replica of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    pub to: NodeId,
    pub message: Message,
}

/// The replica set of a key and what an operation over it needs to know.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Around {
    pub replicas: Vec<NodeId>,
    /// How many replicas an activation waits for before choosing a state, and never fewer than one.
    pub wanted: usize,
    pub write: Write,
    /// How long an operation over the key waits for the replicas, when its type sets it. Nothing means the node's.
    pub write_timeout: Option<Duration>,
    /// When the store of the system keeps the writes of the key, nothing for a type it does not keep.
    pub durable: Option<Durable>,
}

/// A request of this node to the store of its system, which the host performs and answers with `Node::from_store`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Storing {
    pub id: u64,
    pub actor: String,
    pub key: String,
    pub storage: Storage,
    /// How long the store has to answer, after which the host gives up on the call and answers that it failed.
    pub within: Duration,
}

/// What the store answered: the record it keeps for a `Load`, nothing for the others, or why the call failed.
pub type StoreAnswer = Result<Option<Stored>, String>;

/// What is waiting for an operation over one key, and the deadlines that end it.
#[derive(Debug)]
struct Operation {
    waiting: Waiting,
    /// When the whole call gives up, retries included.
    deadline: Instant,
    /// When an activation decides with the promises it has, instead of waiting for replicas that are not answering.
    grace: Option<Instant>,
    /// When a fenced activation starts again, above the round the rival promised.
    retry: Option<Instant>,
    /// The request to the store the operation waits for.
    storing: Option<u64>,
    /// How the operation ended, held until the store has kept the write: a type that saves every write confirms one
    /// only once the store has it.
    held: Option<Outcome>,
}

impl Operation {
    fn new(waiting: Waiting, deadline: Instant, grace: Option<Instant>) -> Self {
        Self {
            waiting,
            deadline,
            grace,
            retry: None,
            storing: None,
            held: None,
        }
    }

    /// The failure an operation whose deadline passed ends with, naming what it was waiting for.
    fn late(&self, actor: &str, key: &str) -> Failure {
        let what = match (&self.waiting, self.storing) {
            (_, Some(_)) => "the store did not answer in time",
            (Waiting::Activate { .. }, None) => "the replicas did not answer in time",
            (Waiting::Commit { .. }, None) => "the replicas did not confirm in time",
        };
        Failure::Unavailable(format!("{actor}/{key}: {what}"))
    }
}

/// What a request to the store that has not been answered is for.
#[derive(Debug)]
enum Awaited {
    /// The record an activation of the key goes on with.
    Load(Entity),
    /// A confirmed write of the key. `period` is the one of a type that saves on a schedule, after which a save that
    /// failed is tried again.
    Save {
        entity: Entity,
        stored: Stored,
        period: Option<Duration>,
    },
    /// A deletion every replica answered for, which the store forgets before this replica forgets its tombstone.
    Drop { entity: Entity, stamp: Stamp },
}

/// The latest confirmed write of a key of a type that saves on a schedule, and when it is saved.
#[derive(Debug)]
struct Unsaved {
    at: Instant,
    stored: Stored,
    period: Duration,
}

#[derive(Debug)]
enum Waiting {
    Activate {
        initial: Option<Pages>,
        answer: oneshot::Sender<Result<Option<Held>, Failure>>,
    },
    /// A write. `ending` says it is the last one of the activation, a release or a deletion: the owner has nothing
    /// left to write once it ends, and what it keeps of the state would outlive the activation on this node.
    Commit {
        answer: oneshot::Sender<Result<(), Failure>>,
        ending: bool,
    },
}

impl Waiting {
    /// The operation this is, as a failure of it is reported.
    fn operation(&self) -> events::Operation {
        match self {
            Self::Activate { .. } => events::Operation::Activate,
            Self::Commit { .. } => events::Operation::Write,
        }
    }

    /// Whether the owner of the key goes when this ends, however it ends.
    fn ending(&self) -> bool {
        matches!(self, Self::Commit { ending: true, .. })
    }

    /// Hand an outcome of the core to whoever is waiting, in the shape that call answers in.
    ///
    /// What comes back is the failure it was told, when the outcome was one.
    fn settle(self, actor: &str, key: &str, outcome: Outcome, lease: u64) -> Option<Failure> {
        match (self, outcome) {
            (Self::Activate { answer, .. }, Outcome::Activated { state, created }) => {
                let _ = answer.send(Ok(Some(Held {
                    pages: state,
                    created,
                    lease,
                })));
                None
            }
            (Self::Activate { answer, .. }, Outcome::Missing) => {
                let _ = answer.send(Ok(None));
                None
            }
            (Self::Commit { answer, .. }, Outcome::Saved) => {
                let _ = answer.send(Ok(()));
                None
            }
            (waiting @ Self::Activate { .. }, Outcome::Insufficient) => Some(waiting.fail(
                Failure::Unavailable(format!("{actor}/{key}: too few replicas answered")),
            )),
            (waiting @ Self::Commit { .. }, Outcome::Fenced) => Some(waiting.fail(
                Failure::Fencing(format!("{actor}/{key} moved to another owner")),
            )),
            (waiting @ Self::Commit { .. }, Outcome::Insufficient) => {
                Some(waiting.fail(Failure::Unavailable(format!(
                    "{actor}/{key}: too few replicas confirmed the write"
                ))))
            }
            // An activation cannot end in a plain save, and a write cannot end in an activation.
            (waiting, outcome) => {
                debug_assert!(
                    matches!(
                        (&waiting, &outcome),
                        (Waiting::Activate { .. }, Outcome::Fenced)
                    ),
                    "{actor}/{key}: {outcome:?} is not an outcome of this operation"
                );
                let _ = (waiting, outcome);
                None
            }
        }
    }

    /// Tell whoever is waiting that the operation failed, giving back the failure it was told.
    fn fail(self, failure: Failure) -> Failure {
        match self {
            Self::Activate { answer, .. } => {
                let _ = answer.send(Err(failure.clone()));
            }
            Self::Commit { answer, .. } => {
                let _ = answer.send(Err(failure.clone()));
            }
        }
        failure
    }
}

/// How many writes of the keys this node owns the replicas confirmed, and how many failed, since it started.
///
/// A write is a save of the state, a deletion, or the write that lets a key go; taking a key over is not one. The
/// counts are shared, so that whoever measures the node reads them from any thread while this one adds to them.
#[derive(Debug, Default)]
pub struct Written {
    confirmed: AtomicU64,
    failed: AtomicU64,
}

impl Written {
    #[must_use]
    pub fn confirmed(&self) -> u64 {
        self.confirmed.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
}

/// A tombstone kept here, and the replicas of its key that answered they keep nothing older than the deletion.
/// `dropping` says the store was asked to forget the deletion, which it does before the tombstone goes.
#[derive(Debug)]
struct Burial {
    stamp: Stamp,
    answered: BTreeSet<NodeId>,
    dropping: bool,
}

#[derive(Debug)]
pub struct Replication {
    node: NodeId,
    replica: Replica,
    owners: HashMap<Entity, Owner>,
    /// The lease of the activation each owner serves: the last `activate` of the key here, whose writes are the only
    /// ones the owner takes. `leased` is the last lease given out.
    leases: HashMap<Entity, u64>,
    leased: u64,
    operations: HashMap<Entity, Operation>,
    sends: Vec<Outgoing>,
    /// Keys a write from another node just marked active, which is what brings one back to the node that owns it.
    arrived: Vec<Entity>,
    /// The operations that failed since the node last asked, for whoever watches it.
    observed: Vec<Event>,
    /// The writes that ended, for whoever measures the node.
    written: Arc<Written>,
    /// The tombstones kept here, by key.
    burials: HashMap<Entity, Burial>,
    /// When each tombstone is asked about next, in that order: every one lingers as long, so the order they were
    /// laid in is the order they come due. An entry whose stamp is not its burial's any more is let go of.
    lingering: VecDeque<(Instant, Entity, Stamp)>,
    /// The requests to the store since the node last asked, for the host to perform.
    storing: Vec<Storing>,
    /// What each request to the store that has not been answered is for.
    awaiting: HashMap<u64, Awaited>,
    /// The id of the last request to the store.
    asked: u64,
    /// The latest confirmed write of each key of a type that saves on a schedule, until it is saved.
    unsaved: HashMap<Entity, Unsaved>,
    limit: usize,
    write_timeout: Duration,
    linger: Duration,
}

impl Replication {
    /// `linger` is how long a tombstone stays before this node asks the other replicas of its key about it, and again
    /// after that while one of them has not answered: the time a copy older than the deletion may still take to reach
    /// a replica in a handover or a range.
    #[must_use]
    pub fn new(node: NodeId, limit: usize, write_timeout: Duration, linger: Duration) -> Self {
        Self {
            replica: Replica::new(node.clone(), limit),
            node,
            owners: HashMap::new(),
            leases: HashMap::new(),
            leased: 0,
            operations: HashMap::new(),
            sends: Vec::new(),
            arrived: Vec::new(),
            observed: Vec::new(),
            written: Arc::new(Written::default()),
            burials: HashMap::new(),
            lingering: VecDeque::new(),
            storing: Vec::new(),
            awaiting: HashMap::new(),
            asked: 0,
            unsaved: HashMap::new(),
            limit,
            write_timeout,
            linger,
        }
    }

    #[must_use]
    pub fn name() -> &'static str {
        "replication"
    }

    /// The copies this node keeps, which a range transfer reads from and fills.
    #[must_use]
    pub fn replica(&self) -> &Replica {
        &self.replica
    }

    pub fn replica_mut(&mut self) -> &mut Replica {
        &mut self.replica
    }

    /// Put a message of the handoff on the band, which is the one the pulls travel in.
    pub fn hand(&mut self, out: Outgoing) {
        self.sends.push(out);
    }

    /// What is waiting to go out on the band of the component.
    pub fn take(&mut self) -> Vec<Outgoing> {
        core::mem::take(&mut self.sends)
    }

    /// Keys a write from another node marked active since the last call, for the handoff to bring back.
    pub fn arrived(&mut self) -> Vec<Entity> {
        core::mem::take(&mut self.arrived)
    }

    /// The operations that failed since the last call, each one reported once.
    pub fn observed(&mut self) -> Vec<Event> {
        core::mem::take(&mut self.observed)
    }

    /// The counts of the writes that ended, which go on growing after this call.
    #[must_use]
    pub fn written(&self) -> Arc<Written> {
        Arc::clone(&self.written)
    }

    /// The requests to the store of the system since the last call, for the host to perform and answer with `stored`.
    pub fn storing(&mut self) -> Vec<Storing> {
        core::mem::take(&mut self.storing)
    }

    /// What the store answered to the request `id`. An answer nothing waits for any more is let go of.
    pub fn stored(&mut self, id: u64, kept: StoreAnswer) {
        let Some(awaited) = self.awaiting.remove(&id) else {
            return;
        };
        match awaited {
            Awaited::Load(entity) => self.loaded(&entity, id, kept),
            Awaited::Save {
                entity,
                stored,
                period,
            } => self.saved(&entity, id, stored, period, kept),
            Awaited::Drop { entity, stamp } => self.dropped(&entity, &stamp, kept),
        }
    }

    /// The record an activation asked the store for, which its owner goes on with, or why the store could not give it.
    fn loaded(&mut self, entity: &Entity, id: u64, kept: StoreAnswer) {
        let Some(operation) = self
            .operations
            .get_mut(entity)
            .filter(|operation| operation.storing == Some(id))
        else {
            return;
        };
        operation.storing = None;
        match kept {
            Ok(stored) => {
                let Some(owner) = self.owners.get_mut(entity) else {
                    return;
                };
                let step = owner.loaded(stored);
                self.apply(entity, step);
            }
            Err(why) => self.abort(
                entity,
                Failure::Unavailable(format!(
                    "{}/{}: the store could not be read: {why}",
                    entity.0, entity.1
                )),
            ),
        }
    }

    /// What the store answered to a save: the write is confirmed to whoever waits for it once the store has it, and a
    /// save of a type that saves on a schedule that did not go through is tried again one period later.
    fn saved(
        &mut self,
        entity: &Entity,
        id: u64,
        stored: Stored,
        period: Option<Duration>,
        kept: StoreAnswer,
    ) {
        if self
            .operations
            .get(entity)
            .is_some_and(|operation| operation.storing == Some(id))
        {
            match kept {
                Ok(_) => {
                    if let Some(mut operation) = self.operations.remove(entity)
                        && let Some(outcome) = operation.held.take()
                    {
                        self.settled(entity, operation, outcome);
                    }
                }
                Err(why) => self.abort(
                    entity,
                    Failure::Unavailable(format!(
                        "{}/{}: the replicas confirmed the write and the store did not keep it: {why}",
                        entity.0, entity.1
                    )),
                ),
            }
            return;
        }
        let Err(why) = kept else {
            return;
        };
        self.reported(
            entity,
            Failure::Unavailable(format!(
                "{}/{}: the store did not keep a confirmed write: {why}",
                entity.0, entity.1
            )),
        );
        // A key this node let go is saved by the owner that took it over, which catches the store up at its
        // activation: a retry here could land after that owner deleted the key and the store forgot it.
        let Some(period) = period.filter(|_| self.owners.contains_key(entity)) else {
            return;
        };
        match self.unsaved.entry(entity.clone()) {
            Entry::Occupied(mut due) => {
                if due.get().stored.stamp.before(&stored.stamp) {
                    due.get_mut().stored = stored;
                }
            }
            Entry::Vacant(free) => {
                free.insert(Unsaved {
                    at: Instant::now() + period,
                    stored,
                    period,
                });
            }
        }
    }

    /// What the store answered to the drop of a deletion: the tombstone goes once the store has forgotten it too, and
    /// stays to be asked about again otherwise.
    fn dropped(&mut self, entity: &Entity, stamp: &Stamp, kept: StoreAnswer) {
        let Some(burial) = self
            .burials
            .get_mut(entity)
            .filter(|burial| burial.stamp == *stamp)
        else {
            return;
        };
        burial.dropping = false;
        match kept {
            Ok(_) => self.forget_tombstone(entity),
            Err(why) => self.reported(
                entity,
                Failure::Unavailable(format!(
                    "{}/{}: the store did not forget the deletion: {why}",
                    entity.0, entity.1
                )),
            ),
        }
    }

    /// Put a request to the store on its way, noting what its answer is for.
    fn ask(
        &mut self,
        entity: &Entity,
        storage: Storage,
        within: Duration,
        awaited: Awaited,
    ) -> u64 {
        self.asked += 1;
        let id = self.asked;
        self.awaiting.insert(id, awaited);
        self.storing.push(Storing {
            id,
            actor: entity.0.clone(),
            key: entity.1.clone(),
            storage,
            within,
        });
        id
    }

    fn save(
        &mut self,
        entity: &Entity,
        stored: Stored,
        period: Option<Duration>,
        within: Duration,
    ) -> u64 {
        self.ask(
            entity,
            Storage::Save(stored.clone()),
            within,
            Awaited::Save {
                entity: entity.clone(),
                stored,
                period,
            },
        )
    }

    /// Ask the store for the record an activation of the key goes on with.
    fn load(&mut self, entity: &Entity) {
        if !self.operations.contains_key(entity) {
            return;
        }
        let within = self.remaining(entity);
        let id = self.ask(entity, Storage::Load, within, Awaited::Load(entity.clone()));
        if let Some(operation) = self.operations.get_mut(entity) {
            operation.storing = Some(id);
        }
    }

    /// Hand a confirmed write to the store as the type of the key says, giving back the request whoever made the
    /// write waits for.
    ///
    /// A type that saves every write has a write confirmed once the store has it. One that saves on a schedule has it
    /// confirmed at once: its latest write is saved once the period is over, and the write a key lets go with or is
    /// deleted by right away, so that the store has a deletion long before the replicas forget it.
    fn keep(&mut self, entity: &Entity, stored: Stored, ending: bool) -> Option<u64> {
        match self.owners.get(entity).and_then(Owner::durable)? {
            Durable::Write => {
                let waited = self.operations.contains_key(entity);
                let within = self.remaining(entity);
                let id = self.save(entity, stored, None, within);
                waited.then_some(id)
            }
            Durable::Every(period) => {
                if ending || stored.pages.is_none() {
                    self.unsaved.remove(entity);
                    self.save(entity, stored, Some(period), self.write_timeout);
                    return None;
                }
                match self.unsaved.entry(entity.clone()) {
                    Entry::Occupied(mut due) => due.get_mut().stored = stored,
                    Entry::Vacant(free) => {
                        free.insert(Unsaved {
                            at: Instant::now() + period,
                            stored,
                            period,
                        });
                    }
                }
                None
            }
        }
    }

    /// What is left of the time the operation over the key has, which bounds what it asks of the store.
    fn remaining(&self, entity: &Entity) -> Duration {
        self.operations
            .get(entity)
            .map_or(self.write_timeout, |operation| {
                operation.deadline.saturating_duration_since(Instant::now())
            })
    }

    /// Report a failure nobody was waiting for, which counts as no failed write.
    fn reported(&mut self, entity: &Entity, failure: Failure) {
        self.observed.push(Event::WriteFailed {
            actor: entity.0.clone(),
            key: entity.1.clone(),
            operation: events::Operation::Write,
            failure,
        });
    }

    fn failed(&mut self, entity: &Entity, operation: events::Operation, failure: Failure) {
        if operation == events::Operation::Write {
            self.written.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.observed.push(Event::WriteFailed {
            actor: entity.0.clone(),
            key: entity.1.clone(),
            operation,
            failure,
        });
    }

    /// Take the key over: promise an epoch, choose the state among the replicas and write it with the mark.
    pub fn activate(
        &mut self,
        actor: &str,
        key: &str,
        initial: Option<Pages>,
        around: &Around,
        answer: oneshot::Sender<Result<Option<Held>, Failure>>,
        now: Instant,
    ) {
        let entity = (actor.to_owned(), key.to_owned());
        let operation = Operation::new(
            Waiting::Activate {
                initial: initial.clone(),
                answer,
            },
            now + around.write_timeout.unwrap_or(self.write_timeout),
            Some(now + GRACE),
        );
        self.operations.insert(entity.clone(), operation);
        // A lease per activation, not per owner: an activation that reuses the owner of the one before it is still
        // the only one whose writes the key takes from now on.
        self.leased += 1;
        self.leases.insert(entity.clone(), self.leased);
        let owner = self.owner(&entity, around);
        let started = owner.activate(initial, around.wanted);
        self.start(&entity, started);
    }

    /// Write the state of a key this node owns, with the active mark unless the key is letting go.
    ///
    /// `around` is where the key is now. An activation outlives the rings it was started under, and the write goes
    /// to the replicas the last of them gives the key, not to the ones that had it when the body took over.
    ///
    /// A key that lets go has nothing more to write, so its owner goes once the write ends: the replicas keep the
    /// state, and a new activation builds a new owner from what they promised.
    #[allow(clippy::too_many_arguments)]
    pub fn commit(
        &mut self,
        entity: &Entity,
        lease: u64,
        pages: Pages,
        active: bool,
        around: Option<&Around>,
        answer: oneshot::Sender<Result<(), Failure>>,
        now: Instant,
    ) {
        self.write(entity, lease, around, answer, now, !active, |owner| {
            if active {
                owner.save(pages)
            } else {
                owner.release(pages)
            }
        });
    }

    /// Delete the state of a key this node owns: its tombstone takes the place of the state on the replicas, at the
    /// write level of any write.
    ///
    /// With `active` the activation goes on, and may write the key again, so the owner stays. Without it the key lets
    /// go with the deletion, and the owner goes once it ends, as after a release.
    pub fn delete(
        &mut self,
        entity: &Entity,
        lease: u64,
        active: bool,
        around: Option<&Around>,
        answer: oneshot::Sender<Result<(), Failure>>,
        now: Instant,
    ) {
        self.write(entity, lease, around, answer, now, !active, |owner| {
            Ok(owner.delete())
        });
    }

    /// `lease` is the activation the write comes from. One a later activation of the key replaced here is fenced the
    /// way a write from a node the key moved off is: what it holds in memory is not the state of the key any more.
    #[allow(clippy::too_many_arguments)]
    fn write(
        &mut self,
        entity: &Entity,
        lease: u64,
        around: Option<&Around>,
        answer: oneshot::Sender<Result<(), Failure>>,
        now: Instant,
        ending: bool,
        started: impl FnOnce(&mut Owner) -> Result<Vec<Send>, TooLarge>,
    ) {
        let Some(owner) = self.owners.get_mut(entity) else {
            let failure = Failure::Unavailable(format!(
                "{}/{} is not held by this node",
                entity.0, entity.1
            ));
            let _ = answer.send(Err(failure.clone()));
            self.failed(entity, events::Operation::Write, failure);
            return;
        };
        if self.leases.get(entity) != Some(&lease) {
            let failure = Failure::Fencing(format!(
                "{}/{} was taken over by a later activation on this node",
                entity.0, entity.1
            ));
            let _ = answer.send(Err(failure.clone()));
            self.failed(entity, events::Operation::Write, failure);
            return;
        }
        let moved = around.is_some_and(|around| owner.replicas() != around.replicas);
        if let Some(around) = around.filter(|_| moved) {
            owner.moved(around.replicas.clone(), around.write, around.wanted);
        }
        let started = started(owner);
        // A write to replicas the key just moved to promises on them first, and decides with the promises it has
        // once the alive ones had their chance, the way an activation does.
        self.operations.insert(
            entity.clone(),
            Operation::new(
                Waiting::Commit { answer, ending },
                now + around
                    .and_then(|around| around.write_timeout)
                    .unwrap_or(self.write_timeout),
                moved.then(|| now + GRACE),
            ),
        );
        self.start(entity, started);
    }

    /// Drop the owner of a key this node stopped holding, when it still serves the activation `lease`. Its replica
    /// keeps the state.
    pub fn forget(&mut self, actor: &str, key: &str, lease: u64) {
        let entity = (actor.to_owned(), key.to_owned());
        if self.leases.get(&entity) == Some(&lease) {
            self.drop_owner(&entity);
        }
    }

    /// A request of an owner, which the replica of this node answers.
    ///
    /// `receiving` says a range this node is still filling holds the key, which keeps the reply out of the quorums.
    pub fn request(&mut self, request: Request, receiving: bool) {
        let owner = owner_of(&request).clone();
        let Some(reply) = self.replica.receive(request, receiving) else {
            return;
        };
        if let Reply::Accepted { stamp, .. } = &reply
            && stamp.epoch.node != self.node
        {
            // Only a write from another node: this one's own write is the activation already running here.
            self.arrived
                .push((actor_of(&reply).to_owned(), key_of(&reply).to_owned()));
        }
        self.sends.push(Outgoing {
            to: owner,
            message: Message::Reply(reply),
        });
    }

    /// An answer of a replica, for the owner that is waiting for it, or for the tombstone this node asked about.
    pub fn answer(&mut self, reply: Reply) {
        if let Reply::Buried {
            actor,
            key,
            stamp,
            replica,
            receiving,
        } = reply
        {
            self.buried(&(actor, key), &stamp, replica, receiving);
            return;
        }
        let entity = (actor_of(&reply).to_owned(), key_of(&reply).to_owned());
        let Some(owner) = self.owners.get_mut(&entity) else {
            return;
        };
        let step = owner.receive(reply);
        self.apply(&entity, step);
    }

    /// Start the linger of every tombstone laid here since the last call, whatever laid it: a deletion, a copy that
    /// came in with one, or a burial another replica asked for.
    pub fn lay(&mut self, now: Instant) {
        for (actor, key, stamp) in self.replica.laid() {
            let entity = (actor, key);
            if self
                .burials
                .get(&entity)
                .is_some_and(|burial| burial.stamp == stamp)
            {
                continue;
            }
            self.lingering
                .push_back((now + self.linger, entity.clone(), stamp.clone()));
            self.burials.insert(
                entity,
                Burial {
                    stamp,
                    answered: BTreeSet::new(),
                    dropping: false,
                },
            );
        }
    }

    /// The tombstones whose linger is over, which the caller asks the other replicas of each key about with `bury`.
    ///
    /// Each comes due again one linger later, until it is forgotten. One that is not kept here any more, because a
    /// handover took it away or a write came after it, is let go of.
    pub fn burying(&mut self, now: Instant) -> Vec<Entity> {
        let mut due = Vec::new();
        let mut again = Vec::new();
        while self.lingering.front().is_some_and(|(at, _, _)| *at <= now) {
            let Some((_, entity, stamp)) = self.lingering.pop_front() else {
                break;
            };
            if self
                .burials
                .get(&entity)
                .is_none_or(|burial| burial.stamp != stamp)
            {
                continue;
            }
            if self.replica.deleted(&entity.0, &entity.1) != Some(&stamp) {
                self.burials.remove(&entity);
                continue;
            }
            again.push((now + self.linger, entity.clone(), stamp));
            due.push(entity);
        }
        self.lingering.extend(again);
        due
    }

    /// Forget the tombstone of a key once every other replica of it has answered that it keeps nothing older, and ask
    /// the ones that have not. `replicas` is where the ring puts the key now.
    ///
    /// A node the ring took the key off neither asks nor forgets: the handover gives its tombstone to the nodes that
    /// replicate the key now, and drops it once they have it. That is also why an answer is only noted when it
    /// arrives, and the tombstone forgotten here, against the ring of this call.
    ///
    /// The tombstone of a `durable` key goes only once the store has forgotten the deletion too, and with it any state
    /// before it the store may keep: the save of the deletion may never have reached it.
    pub fn bury(&mut self, entity: &Entity, replicas: &[NodeId], durable: bool) {
        if !replicas.contains(&self.node) {
            return;
        }
        let others: BTreeSet<NodeId> = replicas
            .iter()
            .filter(|node| **node != self.node)
            .cloned()
            .collect();
        let Some(burial) = self.burials.get_mut(entity) else {
            return;
        };
        if others.is_subset(&burial.answered) {
            if !durable {
                self.forget_tombstone(entity);
            } else if !core::mem::replace(&mut burial.dropping, true) {
                let stamp = burial.stamp.clone();
                self.ask(
                    entity,
                    Storage::Drop(stamp.clone()),
                    self.write_timeout,
                    Awaited::Drop {
                        entity: entity.clone(),
                        stamp,
                    },
                );
            }
            return;
        }
        let asking: Vec<NodeId> = others.difference(&burial.answered).cloned().collect();
        let stamp = burial.stamp.clone();
        for to in asking {
            self.sends.push(Outgoing {
                to,
                message: Message::Request(Request::Bury {
                    actor: entity.0.clone(),
                    key: entity.1.clone(),
                    stamp: stamp.clone(),
                    node: self.node.clone(),
                }),
            });
        }
    }

    /// A replica answered for a tombstone of this node. A replica still filling the range of the key may yet be handed
    /// an older copy, so its answer counts for nothing, and it is asked again one linger later.
    fn buried(&mut self, entity: &Entity, stamp: &Stamp, replica: NodeId, receiving: bool) {
        if let Some(burial) = self.burials.get_mut(entity)
            && burial.stamp == *stamp
            && !receiving
        {
            burial.answered.insert(replica);
        }
    }

    fn forget_tombstone(&mut self, entity: &Entity) {
        if let Some(burial) = self.burials.remove(entity) {
            self.replica.purge(&entity.0, &entity.1, &burial.stamp);
        }
    }

    /// The earliest deadline any operation, tombstone or save on a schedule has, which is when this must be looked at
    /// again.
    #[must_use]
    pub fn due(&self) -> Option<Instant> {
        self.operations
            .values()
            .flat_map(|operation| {
                [Some(operation.deadline), operation.grace, operation.retry]
                    .into_iter()
                    .flatten()
            })
            .chain(self.lingering.front().map(|(at, _, _)| *at))
            .chain(self.unsaved.values().map(|unsaved| unsaved.at))
            .min()
    }

    /// Carry out the deadlines that came due, save the writes whose period is over, and say which activations must
    /// start again.
    ///
    /// A retry is the one thing this cannot do by itself: only the caller knows the replicas of the key by now.
    pub fn fired(&mut self, now: Instant) -> Vec<Entity> {
        let mut expired = Vec::new();
        let mut settling = Vec::new();
        let mut again = Vec::new();
        for (entity, operation) in &mut self.operations {
            if operation.deadline <= now {
                expired.push(entity.clone());
            } else if operation.retry.is_some_and(|at| at <= now) {
                operation.retry = None;
                again.push(entity.clone());
            } else if operation.grace.is_some_and(|at| at <= now) {
                operation.grace = None;
                settling.push(entity.clone());
            }
        }
        for entity in expired {
            if let Some(failure) = self
                .operations
                .get(&entity)
                .map(|operation| operation.late(&entity.0, &entity.1))
            {
                self.abort(&entity, failure);
            }
        }
        let saving: Vec<Entity> = self
            .unsaved
            .iter()
            .filter(|(_, unsaved)| unsaved.at <= now)
            .map(|(entity, _)| entity.clone())
            .collect();
        for entity in saving {
            if let Some(Unsaved { stored, period, .. }) = self.unsaved.remove(&entity) {
                self.save(&entity, stored, Some(period), self.write_timeout);
            }
        }
        for entity in settling {
            // A replica that is still receiving the range of the key did answer, so the grace it was given is not
            // what it is short of: what its answer needs is the range, and the operation asks again until it has it.
            if self.owners.get(&entity).is_some_and(Owner::receiving) {
                if let Some(operation) = self.operations.get_mut(&entity) {
                    operation.retry = Some(now + RETRY);
                }
                continue;
            }
            // Stop waiting for replicas that did not answer, once the ones that are alive had their chance.
            if let Some(owner) = self.owners.get_mut(&entity) {
                let step = owner.settle();
                self.apply(&entity, step);
            }
        }
        again
    }

    /// Start a fenced activation again, above the round the rival promised; or ask again for the promises a write
    /// to replicas the key moved to waits on.
    pub fn again(&mut self, entity: &Entity, around: &Around) {
        let Some(operation) = self.operations.get_mut(entity) else {
            return;
        };
        let Waiting::Activate { initial, .. } = &operation.waiting else {
            let sends = self
                .owners
                .get_mut(entity)
                .map(Owner::again)
                .unwrap_or_default();
            self.queue(sends);
            return;
        };
        let initial = initial.clone();
        // The try that starts now asks the store again, and what the one before asked of it is answered to nobody.
        operation.storing = None;
        let owner = self.owner(entity, around);
        let started = owner.activate(initial, around.wanted);
        self.start(entity, started);
    }

    /// The owner of a key, rebuilt when the replicas or the durability changed, always above the round its replica
    /// promised.
    fn owner(&mut self, entity: &Entity, around: &Around) -> &mut Owner {
        let (actor, key) = entity;
        let held = self.owners.get(entity).is_some_and(|owner| {
            owner.replicas() == around.replicas && owner.durable() == around.durable
        });
        if !held {
            let promised = self
                .replica
                .promised(actor, key)
                .map_or(0, |epoch| epoch.round);
            let current = self
                .owners
                .get(entity)
                .map_or(0, |owner| owner.epoch().round);
            let owner = Owner::new(
                actor,
                key,
                self.node.clone(),
                around.replicas.clone(),
                around.write,
                self.limit,
                promised.max(current),
                around.durable,
            );
            self.owners.insert(entity.clone(), owner);
        }
        self.owners
            .get_mut(entity)
            .expect("the owner was just put there")
    }

    /// The save a schedule holds for the key goes with the owner: the one that takes the key over reads the write
    /// from the replicas and saves it at its activation, and a save from here could land after it deleted the key.
    fn drop_owner(&mut self, entity: &Entity) {
        self.owners.remove(entity);
        self.leases.remove(entity);
        self.unsaved.remove(entity);
    }

    /// Send what the core asked for, or end the operation on a state no message can carry.
    fn start(&mut self, entity: &Entity, started: Result<Vec<Send>, TooLarge>) {
        match started {
            Ok(sends) => self.queue(sends),
            Err(TooLarge(reason)) => self.abort(entity, Failure::TooLarge(reason)),
        }
    }

    fn apply(&mut self, entity: &Entity, step: Step) {
        let Step {
            sends,
            outcome,
            store,
        } = step;
        self.queue(sends);
        let Some(outcome) = outcome else {
            if matches!(store, Some(Storage::Load)) {
                self.load(entity);
                return;
            }
            // The answer of a replica that has not got the range of the key yet counts for nothing, and the replica
            // does not answer again by itself once it has it. So the operation asks again, until its own deadline.
            if self.owners.get(entity).is_some_and(Owner::receiving)
                && let Some(operation) = self.operations.get_mut(entity)
                && operation.retry.is_none()
            {
                operation.retry = Some(Instant::now() + RETRY);
            }
            return;
        };
        let saving = match store {
            Some(Storage::Save(stored)) => Some(stored),
            _ => None,
        };
        let Some(operation) = self.operations.get_mut(entity) else {
            // Confirmed after whoever waited for it gave up: the replicas have the write, and so does the store.
            if let Some(stored) = saving {
                self.keep(entity, stored, false);
            }
            return;
        };
        // The core kept its round, so the next try is above the one the rival promised.
        if matches!(outcome, Outcome::Fenced)
            && matches!(operation.waiting, Waiting::Activate { .. })
        {
            operation.grace = None;
            operation.retry = Some(Instant::now() + RETRY);
            return;
        }
        let ending = operation.waiting.ending();
        if let Some(stored) = saving
            && let Some(id) = self.keep(entity, stored, ending)
        {
            if let Some(operation) = self.operations.get_mut(entity) {
                operation.storing = Some(id);
                operation.held = Some(outcome);
            }
            return;
        }
        let operation = self
            .operations
            .remove(entity)
            .expect("the operation was just there");
        self.settled(entity, operation, outcome);
    }

    /// Hand the outcome of an operation to whoever waits for it, and let the owner go after the last write of its
    /// activation.
    fn settled(&mut self, entity: &Entity, operation: Operation, outcome: Outcome) {
        let performed = operation.waiting.operation();
        let ending = operation.waiting.ending();
        // Counted before the answer goes out, so that whoever it wakes finds the write counted.
        if performed == events::Operation::Write && matches!(outcome, Outcome::Saved) {
            self.written.confirmed.fetch_add(1, Ordering::Relaxed);
        }
        let lease = self.leases.get(entity).copied().unwrap_or_default();
        if let Some(failure) = operation
            .waiting
            .settle(&entity.0, &entity.1, outcome, lease)
        {
            self.failed(entity, performed, failure);
        }
        if ending {
            self.drop_owner(entity);
        }
    }

    /// End an operation that did not go through, telling whoever waits for it why.
    fn abort(&mut self, entity: &Entity, failure: Failure) {
        let Some(operation) = self.operations.remove(entity) else {
            return;
        };
        let performed = operation.waiting.operation();
        let ending = operation.waiting.ending();
        let failure = operation.waiting.fail(failure);
        self.failed(entity, performed, failure);
        if ending {
            self.drop_owner(entity);
        }
    }

    fn queue(&mut self, sends: Vec<Send>) {
        self.sends.extend(sends.into_iter().map(|send| Outgoing {
            to: send.to,
            message: Message::Request(send.message),
        }));
    }
}

fn owner_of(request: &Request) -> &NodeId {
    match request {
        Request::Prepare { epoch, .. } | Request::FetchPages { epoch, .. } => &epoch.node,
        Request::Accept { stamp, .. } => &stamp.epoch.node,
        Request::Bury { node, .. } => node,
    }
}

const GRACE: Duration = Duration::from_millis(200);
const RETRY: Duration = Duration::from_millis(50);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use casty_core::replication::messages::{Epoch, Request, Stamp, Write};
    use casty_core::replication::replica::Replica;
    use casty_core::rolls::Rolls;
    use casty_core::store::{Durable, Pages, Storage, Stored, version};
    use tokio::sync::oneshot;

    use super::{
        Around, Duration, Entity, Instant, Message, NodeId, Outgoing, Replication, Storing,
    };

    const ACTOR: &str = "tests.app:ledger";
    const KEY: &str = "key-1";
    const LIMIT: usize = 1024;
    const LINGER: Duration = Duration::from_secs(1);

    /// Deliver everything on the band until nothing is left, the requests this node sends itself going to its own
    /// replica and every answer back to it.
    fn settle(
        replication: &mut Replication,
        own: &NodeId,
        replicas: &mut HashMap<NodeId, Replica>,
    ) {
        let mut pending = replication.take();
        while !pending.is_empty() {
            for out in pending {
                match out.message {
                    Message::Request(request) if out.to == *own => {
                        replication.request(request, false);
                    }
                    Message::Request(request) => {
                        if let Some(reply) = replicas
                            .get_mut(&out.to)
                            .and_then(|replica| replica.receive(request, false))
                        {
                            replication.answer(reply);
                        }
                    }
                    Message::Reply(reply) => replication.answer(reply),
                    Message::Pull(_) => {}
                }
            }
            pending = replication.take();
        }
    }

    /// A node that took the key over with `initial`, over its own replica and the two others.
    fn taken(ids: &[NodeId], replicas: &mut HashMap<NodeId, Replica>, now: Instant) -> Replication {
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let (answer, mut activated) = oneshot::channel();
        replication.activate(ACTOR, KEY, Some(pages(b"one")), &around(ids), answer, now);
        settle(&mut replication, &ids[0], replicas);
        assert!(activated.try_recv().is_ok_and(|held| held.is_ok()));
        replication
    }

    /// A deletion that reaches a majority leaves the third replica with the state. Once the tombstone has lingered,
    /// asking about it puts it in place of that copy, and with every replica answered for, it is forgotten.
    #[test]
    fn a_tombstone_is_forgotten_once_every_other_replica_answered_that_it_keeps_nothing_older() {
        let ids = Rolls::seeded(75).nodes(3);
        let mut replicas: HashMap<NodeId, Replica> = ids[1..]
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let now = Instant::now();
        let mut replication = taken(&ids, &mut replicas, now);

        let (answer, mut deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        let reaching: Vec<Outgoing> = replication
            .take()
            .into_iter()
            .filter(|out| out.to != ids[2])
            .collect();
        for out in reaching {
            replication.hand(out);
        }
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(deleted.try_recv(), Ok(Ok(())));
        assert!(replication.replica().deleted(ACTOR, KEY).is_some());
        assert!(
            kept(replication.replica()).is_none(),
            "the state outlived its deletion"
        );
        assert_eq!(kept(&replicas[&ids[2]]).as_deref(), Some(&b"one"[..]));

        replication.lay(now);
        assert!(
            replication.burying(now).is_empty(),
            "a tombstone was asked about before it lingered"
        );
        assert_eq!(replication.due(), Some(now + LINGER));
        assert_eq!(replication.burying(now + LINGER), vec![entity()]);
        replication.bury(&entity(), &ids, false);
        settle(&mut replication, &ids[0], &mut replicas);

        assert!(replicas[&ids[2]].deleted(ACTOR, KEY).is_some());
        assert!(
            kept(&replicas[&ids[2]]).is_none(),
            "the copy that missed the deletion survived it"
        );
        // The answers are in, and the tombstone goes when it comes due again, against the ring of that moment.
        assert!(replication.replica().deleted(ACTOR, KEY).is_some());
        assert_eq!(replication.burying(now + LINGER * 2), vec![entity()]);
        replication.bury(&entity(), &ids, false);
        assert!(
            replication.take().is_empty(),
            "a replica that answered was asked again"
        );
        assert!(
            replication.replica().kept().is_empty(),
            "the tombstone outlived every answer"
        );
        assert!(replication.burying(now + LINGER * 4).is_empty());
    }

    /// A replica still filling the range of the key may yet be handed a copy older than the deletion, so its answer
    /// does not count: the tombstone stays until it answers again with the range in.
    #[test]
    fn a_replica_still_receiving_its_range_keeps_the_tombstone_where_it_is() {
        let ids = Rolls::seeded(76).nodes(3);
        let mut replicas: HashMap<NodeId, Replica> = ids[1..]
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let now = Instant::now();
        let mut replication = taken(&ids, &mut replicas, now);
        let (answer, _deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        replication.lay(now);

        replication.burying(now + LINGER);
        replication.bury(&entity(), &ids, false);
        for out in replication.take() {
            let Message::Request(request) = out.message else {
                continue;
            };
            let receiving = out.to == ids[2];
            if let Some(reply) = replicas
                .get_mut(&out.to)
                .and_then(|replica| replica.receive(request, receiving))
            {
                replication.answer(reply);
            }
        }
        assert!(replication.replica().deleted(ACTOR, KEY).is_some());

        // Asked again, only the one that did not count is asked, and once it answers the tombstone goes.
        assert_eq!(replication.burying(now + LINGER * 2), vec![entity()]);
        replication.bury(&entity(), &ids, false);
        let asked = replication.take();
        assert!(asked.iter().all(|out| out.to == ids[2]), "{asked:?}");
        for out in asked {
            replication.hand(out);
        }
        settle(&mut replication, &ids[0], &mut replicas);
        assert!(replication.replica().deleted(ACTOR, KEY).is_some());
        assert_eq!(replication.burying(now + LINGER * 3), vec![entity()]);
        replication.bury(&entity(), &ids, false);
        assert!(replication.replica().kept().is_empty());
    }

    /// A node the ring took the key off leaves its tombstone to the handover, which drops it once the nodes that
    /// replicate the key have it.
    #[test]
    fn a_node_that_stopped_replicating_the_key_neither_asks_nor_forgets() {
        let ids = Rolls::seeded(79).nodes(3);
        let mut replicas: HashMap<NodeId, Replica> = ids[1..]
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let now = Instant::now();
        let mut replication = taken(&ids, &mut replicas, now);
        let (answer, _deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        replication.lay(now);

        assert_eq!(replication.burying(now + LINGER), vec![entity()]);
        replication.bury(&entity(), &ids[1..], false);

        assert!(replication.take().is_empty());
        assert!(replication.replica().deleted(ACTOR, KEY).is_some());

        // The handover took it away, and the tombstone is not asked about again.
        replication.replica_mut().drop(ACTOR, KEY);
        assert!(replication.burying(now + LINGER * 2).is_empty());
        assert_eq!(replication.due(), None);
    }

    #[test]
    fn a_key_with_one_replica_forgets_its_tombstone_once_it_lingered() {
        let ids = Rolls::seeded(77).nodes(1);
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let mut none = HashMap::new();
        let now = Instant::now();
        let (answer, _activated) = oneshot::channel();
        replication.activate(ACTOR, KEY, Some(pages(b"one")), &around(&ids), answer, now);
        settle(&mut replication, &ids[0], &mut none);
        let (answer, mut deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut none);
        assert_eq!(deleted.try_recv(), Ok(Ok(())));
        replication.lay(now);

        assert_eq!(replication.burying(now + LINGER), vec![entity()]);
        replication.bury(&entity(), &ids, false);

        assert!(
            replication.take().is_empty(),
            "a key with no other replica asked someone"
        );
        assert!(replication.replica().kept().is_empty());
    }

    /// The owner of a key keeps the last state it wrote. A key that lets go has nothing left to write, so its owner
    /// goes, and with it that copy; a deleted key keeps its owner, holding no page, for as long as its activation.
    #[test]
    fn an_owner_goes_with_the_release_and_stays_after_a_deletion() {
        let ids = Rolls::seeded(78).nodes(3);
        let mut replicas: HashMap<NodeId, Replica> = ids[1..]
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let now = Instant::now();
        let mut replication = taken(&ids, &mut replicas, now);

        let (answer, mut released) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"two"),
            false,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(released.try_recv(), Ok(Ok(())));
        assert!(
            replication.owners.is_empty(),
            "the owner outlived the release"
        );
        assert_eq!(kept(&replicas[&ids[1]]).as_deref(), Some(&b"two"[..]));

        let (answer, mut activated) = oneshot::channel();
        replication.activate(ACTOR, KEY, None, &around(&ids), answer, now);
        settle(&mut replication, &ids[0], &mut replicas);
        assert!(
            activated
                .try_recv()
                .is_ok_and(|held| held.is_ok_and(|held| held.is_some()))
        );
        let (answer, mut deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(deleted.try_recv(), Ok(Ok(())));
        assert!(replication.owners.contains_key(&entity()));

        // A write after the deletion goes on from the tombstone, as a key written again.
        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"three"),
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(written.try_recv(), Ok(Ok(())));
        assert_eq!(kept(&replicas[&ids[2]]).as_deref(), Some(&b"three"[..]));

        // A deletion the key lets go with is its last write, like a release.
        let (answer, mut deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            false,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(deleted.try_recv(), Ok(Ok(())));
        assert!(
            replication.owners.is_empty(),
            "the owner outlived the last deletion"
        );
        assert!(kept(&replicas[&ids[2]]).is_none());
    }

    /// Two activations of the key on one node, the second taking it over from the first, leave the first holding a
    /// state that is not the key's any more: the node refuses its writes and keeps the owner it built for the second.
    #[test]
    fn a_write_from_the_activation_a_later_one_replaced_on_this_node_is_fenced() {
        let ids = Rolls::seeded(79).nodes(3);
        let mut replicas: HashMap<NodeId, Replica> = ids[1..]
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let now = Instant::now();
        let mut replication = taken(&ids, &mut replicas, now);
        let first = replication.leased;

        let (answer, mut activated) = oneshot::channel();
        replication.activate(ACTOR, KEY, None, &around(&ids), answer, now);
        settle(&mut replication, &ids[0], &mut replicas);
        let second = activated
            .try_recv()
            .expect("the activation was answered")
            .expect("it took the key over")
            .expect("it has a state")
            .lease;
        assert_ne!(
            first, second,
            "the second activation got the lease of the first"
        );

        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            first,
            pages(b"stale"),
            false,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert!(
            matches!(written.try_recv(), Ok(Err(super::Failure::Fencing(_)))),
            "the write of the replaced activation went through"
        );
        replication.forget(ACTOR, KEY, first);
        assert!(
            replication.owners.contains_key(&entity()),
            "the release of the replaced activation dropped the owner of the later one"
        );
        assert_eq!(kept(&replicas[&ids[1]]).as_deref(), Some(&b"one"[..]));

        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            second,
            pages(b"two"),
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(written.try_recv(), Ok(Ok(())));
        assert_eq!(kept(&replicas[&ids[1]]).as_deref(), Some(&b"two"[..]));
    }

    fn entity() -> Entity {
        (ACTOR.to_owned(), KEY.to_owned())
    }

    fn around(replicas: &[NodeId]) -> Around {
        Around {
            replicas: replicas.to_vec(),
            wanted: replicas.len(),
            write: Write::Majority,
            write_timeout: None,
            durable: None,
        }
    }

    fn pages(state: &[u8]) -> Pages {
        Pages::from([("entries".to_owned(), state.to_vec())])
    }

    /// The `entries` page as one replica keeps it.
    fn kept(replica: &Replica) -> Option<Vec<u8>> {
        replica
            .copies(ACTOR, KEY)
            .into_iter()
            .find_map(|copy| copy.pages.get("entries").cloned())
    }

    /// Answer every request that went out, until nothing is left on the band.
    fn exchange(replication: &mut Replication, replicas: &mut HashMap<NodeId, Replica>) {
        let mut pending = replication.take();
        while !pending.is_empty() {
            for out in pending {
                let Message::Request(request) = out.message else {
                    continue;
                };
                let Some(replica) = replicas.get_mut(&out.to) else {
                    continue;
                };
                if let Some(reply) = replica.receive(request, false) {
                    replication.answer(reply);
                }
            }
            pending = replication.take();
        }
    }

    /// The store of the system as these tests hold it: one record per key, kept as a store must.
    #[derive(Debug, Default)]
    struct Records(HashMap<Entity, Stored>);

    impl Records {
        /// Answer every request to the store that went out, and give back what each one asked.
        fn answer(&mut self, replication: &mut Replication) -> Vec<Storage> {
            let mut asked = Vec::new();
            for request in replication.storing() {
                let entity = (request.actor.clone(), request.key.clone());
                let kept = match &request.storage {
                    Storage::Load => Ok(self.0.get(&entity).cloned()),
                    Storage::Save(stored) => {
                        if self
                            .0
                            .get(&entity)
                            .is_none_or(|record| record.version() <= stored.version())
                        {
                            self.0.insert(entity, stored.clone());
                        }
                        Ok(None)
                    }
                    Storage::Drop(stamp) => {
                        if self
                            .0
                            .get(&entity)
                            .is_some_and(|record| record.version() <= version(stamp))
                        {
                            self.0.remove(&entity);
                        }
                        Ok(None)
                    }
                };
                asked.push(request.storage);
                replication.stored(request.id, kept);
            }
            asked
        }

        fn state(&self) -> Option<Pages> {
            self.0
                .get(&entity())
                .and_then(|record| record.pages.clone())
        }
    }

    /// `settle`, with `records` answering every request to the store as it is made. What was asked comes back.
    fn settle_stored(
        replication: &mut Replication,
        own: &NodeId,
        replicas: &mut HashMap<NodeId, Replica>,
        records: &mut Records,
    ) -> Vec<Storage> {
        let mut asked = Vec::new();
        loop {
            settle(replication, own, replicas);
            let answered = records.answer(replication);
            if answered.is_empty() {
                return asked;
            }
            asked.extend(answered);
        }
    }

    fn durable(replicas: &[NodeId], policy: Durable) -> Around {
        Around {
            durable: Some(policy),
            ..around(replicas)
        }
    }

    fn others(ids: &[NodeId]) -> HashMap<NodeId, Replica> {
        ids[1..]
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect()
    }

    /// A node that took over a key of a durable type that nothing held, over its own replica and the two others.
    fn taken_durably(
        ids: &[NodeId],
        replicas: &mut HashMap<NodeId, Replica>,
        records: &mut Records,
        policy: Durable,
        now: Instant,
    ) -> Replication {
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let (answer, mut activated) = oneshot::channel();
        replication.activate(
            ACTOR,
            KEY,
            Some(pages(b"one")),
            &durable(ids, policy),
            answer,
            now,
        );
        settle_stored(&mut replication, &ids[0], replicas, records);
        assert!(activated.try_recv().is_ok_and(|held| held.is_ok()));
        replication
    }

    /// The criterion of 14.2, through the service: no replica holds the key, and it comes up from what a cluster
    /// before this one saved, not from the state the activation brings.
    #[test]
    fn a_durable_key_no_replica_holds_comes_up_from_the_store() {
        let ids = Rolls::seeded(81).nodes(3);
        let mut replicas = others(&ids);
        let mut records = Records::default();
        let before = Rolls::seeded(82).nodes(1).remove(0);
        records.0.insert(
            entity(),
            Stored {
                stamp: Stamp {
                    epoch: Epoch {
                        round: 40,
                        node: before,
                    },
                    version: 2,
                },
                pages: Some(pages(b"kept")),
            },
        );
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let (answer, mut activated) = oneshot::channel();

        replication.activate(
            ACTOR,
            KEY,
            Some(pages(b"initial")),
            &durable(&ids, Durable::Write),
            answer,
            Instant::now(),
        );
        let asked = settle_stored(&mut replication, &ids[0], &mut replicas, &mut records);

        let held = activated
            .try_recv()
            .expect("the activation ended")
            .expect("it took the key")
            .expect("the key has a state");
        assert_eq!(held.pages, pages(b"kept"));
        assert!(!held.created);
        assert_eq!(asked, vec![Storage::Load]);
        assert_eq!(kept(&replicas[&ids[1]]).as_deref(), Some(&b"kept"[..]));
    }

    #[test]
    fn an_activation_the_store_cannot_answer_fails_unavailable() {
        let ids = Rolls::seeded(83).nodes(3);
        let mut replicas = others(&ids);
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let (answer, mut activated) = oneshot::channel();
        replication.activate(
            ACTOR,
            KEY,
            Some(pages(b"one")),
            &durable(&ids, Durable::Write),
            answer,
            Instant::now(),
        );
        settle(&mut replication, &ids[0], &mut replicas);

        let asked = replication.storing();
        assert!(matches!(
            asked.as_slice(),
            [Storing {
                storage: Storage::Load,
                ..
            }]
        ));
        replication.stored(asked[0].id, Err("the database is down".to_owned()));

        assert!(matches!(
            activated.try_recv(),
            Ok(Err(super::Failure::Unavailable(why))) if why.contains("the database is down")
        ));
        assert!(matches!(
            replication.observed().as_slice(),
            [super::Event::WriteFailed {
                operation: super::events::Operation::Activate,
                ..
            }]
        ));
    }

    #[test]
    fn a_write_of_a_type_that_saves_every_write_is_confirmed_once_the_store_kept_it() {
        let ids = Rolls::seeded(84).nodes(3);
        let mut replicas = others(&ids);
        let mut records = Records::default();
        let now = Instant::now();
        let mut replication = taken_durably(&ids, &mut replicas, &mut records, Durable::Write, now);
        let around = durable(&ids, Durable::Write);

        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"two"),
            true,
            Some(&around),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        // The replicas confirmed it, and it waits for the store.
        assert!(written.try_recv().is_err());
        let asked = records.answer(&mut replication);
        assert!(matches!(
            asked.as_slice(),
            [Storage::Save(Stored { pages: Some(_), .. })]
        ));
        assert_eq!(written.try_recv(), Ok(Ok(())));
        assert_eq!(records.state(), Some(pages(b"two")));

        // A store that does not keep it fails the write, which the replicas have all the same.
        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"three"),
            true,
            Some(&around),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        let asked = replication.storing();
        replication.stored(asked[0].id, Err("disk full".to_owned()));
        assert!(matches!(
            written.try_recv(),
            Ok(Err(super::Failure::Unavailable(why))) if why.contains("disk full")
        ));
        assert_eq!(records.state(), Some(pages(b"two")));
        assert_eq!(kept(&replicas[&ids[1]]).as_deref(), Some(&b"three"[..]));
        assert_eq!(replication.written().failed(), 1);
    }

    /// The criterion of 14.2, through the service: a later owner promised a higher round on every replica, and the
    /// write of the owner it fenced is refused there and never asked of the store.
    #[test]
    fn a_fenced_owner_asks_the_store_to_keep_nothing() {
        let ids = Rolls::seeded(85).nodes(3);
        let mut replicas = others(&ids);
        let mut records = Records::default();
        let now = Instant::now();
        let mut replication = taken_durably(&ids, &mut replicas, &mut records, Durable::Write, now);
        let around = durable(&ids, Durable::Write);
        let (answer, _written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"two"),
            true,
            Some(&around),
            answer,
            now,
        );
        settle_stored(&mut replication, &ids[0], &mut replicas, &mut records);
        assert_eq!(records.state(), Some(pages(b"two")));

        let prepare = Request::Prepare {
            actor: ACTOR.to_owned(),
            key: KEY.to_owned(),
            epoch: Epoch {
                round: 50,
                node: ids[1].clone(),
            },
        };
        for replica in replicas.values_mut() {
            replica.receive(prepare.clone(), false);
        }
        replication.request(prepare, false);
        let _ = replication.take();

        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"three"),
            true,
            Some(&around),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);

        assert!(matches!(
            written.try_recv(),
            Ok(Err(super::Failure::Fencing(_)))
        ));
        assert!(
            replication.storing().is_empty(),
            "a write the replicas refused was saved"
        );
        assert_eq!(records.state(), Some(pages(b"two")));
    }

    #[test]
    fn a_type_that_saves_on_a_schedule_is_confirmed_at_once_and_saved_once_its_period_is_over() {
        let ids = Rolls::seeded(86).nodes(3);
        let mut replicas = others(&ids);
        let mut records = Records::default();
        let period = Duration::from_secs(2);
        let now = Instant::now();
        let mut replication = taken_durably(
            &ids,
            &mut replicas,
            &mut records,
            Durable::Every(period),
            now,
        );
        let around = durable(&ids, Durable::Every(period));

        for state in [&b"two"[..], &b"three"[..]] {
            let (answer, mut written) = oneshot::channel();
            replication.commit(
                &entity(),
                replication.leased,
                pages(state),
                true,
                Some(&around),
                answer,
                now,
            );
            settle(&mut replication, &ids[0], &mut replicas);
            assert_eq!(written.try_recv(), Ok(Ok(())));
        }
        assert!(
            replication.storing().is_empty(),
            "a save came before its period"
        );

        let due = replication.due().expect("a save is planned");
        replication.fired(due - Duration::from_millis(1));
        assert!(replication.storing().is_empty());
        replication.fired(due);
        let asked = records.answer(&mut replication);
        assert_eq!(asked.len(), 1, "{asked:?}");
        assert_eq!(records.state(), Some(pages(b"three")));

        // The write the key lets go with is saved at once.
        let (answer, mut released) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"four"),
            false,
            Some(&around),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(released.try_recv(), Ok(Ok(())));
        records.answer(&mut replication);
        assert_eq!(records.state(), Some(pages(b"four")));
        assert_eq!(replication.due(), None);
    }

    /// The owner that takes the key over saves the write at its activation, and a save from the node that let the
    /// key go could land after that owner deleted the key and the store forgot it.
    #[test]
    fn a_scheduled_save_goes_with_the_owner_when_the_key_moves_off_this_node() {
        let ids = Rolls::seeded(88).nodes(3);
        let mut replicas = others(&ids);
        let mut records = Records::default();
        let period = Duration::from_secs(2);
        let now = Instant::now();
        let mut replication = taken_durably(
            &ids,
            &mut replicas,
            &mut records,
            Durable::Every(period),
            now,
        );
        let around = durable(&ids, Durable::Every(period));
        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"two"),
            true,
            Some(&around),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(written.try_recv(), Ok(Ok(())));
        let due = replication.due().expect("a save is planned");

        replication.forget(&entity().0, &entity().1, replication.leased);
        assert_eq!(replication.due(), None);
        replication.fired(due);
        assert!(
            replication.storing().is_empty(),
            "the node saved a key it let go"
        );
        assert!(records.0.is_empty(), "the store has {:?}", records.0);
    }

    /// A tombstone of a durable key goes once the store has forgotten the deletion too, and stays while it has not.
    #[test]
    fn a_durable_tombstone_goes_only_once_the_store_forgot_the_deletion() {
        let ids = Rolls::seeded(87).nodes(3);
        let mut replicas = others(&ids);
        let mut records = Records::default();
        let now = Instant::now();
        let mut replication = taken_durably(&ids, &mut replicas, &mut records, Durable::Write, now);
        let (answer, mut deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            true,
            Some(&durable(&ids, Durable::Write)),
            answer,
            now,
        );
        settle_stored(&mut replication, &ids[0], &mut replicas, &mut records);
        assert_eq!(deleted.try_recv(), Ok(Ok(())));
        assert!(records.0.contains_key(&entity()));
        assert_eq!(records.state(), None);

        replication.lay(now);
        assert_eq!(replication.burying(now + LINGER), vec![entity()]);
        replication.bury(&entity(), &ids, true);
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!(replication.burying(now + LINGER * 2), vec![entity()]);
        replication.bury(&entity(), &ids, true);

        // Every replica answered: the store is asked to forget the deletion first, and it fails.
        let asked = replication.storing();
        assert!(matches!(
            asked.as_slice(),
            [Storing {
                storage: Storage::Drop(_),
                ..
            }]
        ));
        replication.stored(asked[0].id, Err("unreachable".to_owned()));
        assert!(replication.replica().deleted(ACTOR, KEY).is_some());

        // Asked again one linger later, it forgets it, and the tombstone goes with it.
        assert_eq!(replication.burying(now + LINGER * 3), vec![entity()]);
        replication.bury(&entity(), &ids, true);
        records.answer(&mut replication);
        assert!(replication.replica().kept().is_empty());
        assert!(records.0.is_empty(), "the store kept the deletion");
    }

    /// An activation outlives the rings it was started under. A write that kept going to the replicas it began with
    /// could be confirmed by a node that keeps the key only until the ones that replicate it now have taken it, and
    /// then goes away with the copy that node drops.
    #[test]
    fn a_write_goes_to_the_replicas_the_ring_gives_the_key_now() {
        let ids = Rolls::seeded(71).nodes(4);
        let mut replicas: HashMap<NodeId, Replica> = ids
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let (answer, mut activated) = oneshot::channel();
        replication.activate(
            ACTOR,
            KEY,
            Some(pages(b"one")),
            &around(&ids[..3]),
            answer,
            Instant::now(),
        );
        exchange(&mut replication, &mut replicas);
        assert!(activated.try_recv().is_ok_and(|held| held.is_ok()));

        // The ring took the key off the third node and put it on the fourth, and the body wrote again.
        let moved = [ids[0].clone(), ids[1].clone(), ids[3].clone()];
        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"two"),
            true,
            Some(&around(&moved)),
            answer,
            Instant::now(),
        );
        let sends = replication.take();
        assert!(
            sends.iter().all(|out| out.to != ids[2]),
            "the write went to a node that stopped keeping the key"
        );
        for out in sends {
            replication.hand(out);
        }
        exchange(&mut replication, &mut replicas);

        assert_eq!(written.try_recv(), Ok(Ok(())));
        assert_eq!(kept(&replicas[&ids[3]]).as_deref(), Some(&b"two"[..]));
        assert_eq!(kept(&replicas[&ids[2]]).as_deref(), Some(&b"one"[..]));
    }

    /// A type that sets its own write timeout gives up on silent replicas at it, and not at the node's.
    #[test]
    fn a_write_ends_at_the_timeout_of_its_type() {
        let ids = Rolls::seeded(72).nodes(3);
        let mut replicas: HashMap<NodeId, Replica> = ids
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let now = Instant::now();
        let (answer, mut activated) = oneshot::channel();
        replication.activate(ACTOR, KEY, Some(pages(b"one")), &around(&ids), answer, now);
        exchange(&mut replication, &mut replicas);
        assert!(activated.try_recv().is_ok_and(|held| held.is_ok()));

        let own = Around {
            write_timeout: Some(Duration::from_millis(300)),
            ..around(&ids)
        };
        let (answer, mut written) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"two"),
            true,
            Some(&own),
            answer,
            now,
        );

        assert_eq!(replication.due(), Some(now + Duration::from_millis(300)));
        replication.fired(now + Duration::from_millis(299));
        assert!(written.try_recv().is_err());
        replication.fired(now + Duration::from_millis(300));
        assert!(matches!(
            written.try_recv(),
            Ok(Err(super::Failure::Unavailable(_)))
        ));
    }

    /// A write the node cannot even start is told to the body and reported to whoever watches the node, once.
    #[test]
    fn a_write_of_a_key_this_node_does_not_hold_is_reported() {
        let ids = Rolls::seeded(73).nodes(3);
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let (answer, mut written) = oneshot::channel();

        replication.commit(
            &entity(),
            replication.leased,
            pages(b"one"),
            true,
            Some(&around(&ids)),
            answer,
            Instant::now(),
        );

        assert!(matches!(
            written.try_recv(),
            Ok(Err(super::Failure::Unavailable(_)))
        ));
        assert!(matches!(
            replication.observed().as_slice(),
            [super::Event::WriteFailed {
                operation: super::events::Operation::Write,
                failure: super::Failure::Unavailable(_),
                ..
            }]
        ));
        assert!(
            replication.observed().is_empty(),
            "the failure was reported twice"
        );
    }

    #[test]
    fn an_activation_the_replicas_never_answer_is_reported_when_it_expires() {
        let ids = Rolls::seeded(74).nodes(3);
        let mut replication =
            Replication::new(ids[0].clone(), LIMIT, Duration::from_secs(5), LINGER);
        let now = Instant::now();
        let (answer, mut activated) = oneshot::channel();

        replication.activate(ACTOR, KEY, Some(pages(b"one")), &around(&ids), answer, now);
        let _ = replication.take();
        assert!(replication.observed().is_empty());
        replication.fired(now + Duration::from_secs(5));

        assert!(matches!(
            activated.try_recv(),
            Ok(Err(super::Failure::Unavailable(_)))
        ));
        assert!(matches!(
            replication.observed().as_slice(),
            [super::Event::WriteFailed {
                operation: super::events::Operation::Activate,
                ..
            }]
        ));
    }

    /// Every write that ends is counted once, confirmed or failed, and taking a key over is not a write.
    #[test]
    fn the_writes_that_end_are_counted_once_each() {
        let ids = Rolls::seeded(80).nodes(3);
        let mut replicas: HashMap<NodeId, Replica> = ids[1..]
            .iter()
            .map(|node| (node.clone(), Replica::new(node.clone(), LIMIT)))
            .collect();
        let now = Instant::now();
        let mut replication = taken(&ids, &mut replicas, now);
        let written = replication.written();
        assert_eq!((written.confirmed(), written.failed()), (0, 0));

        let (answer, _saved) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"two"),
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        let (answer, _deleted) = oneshot::channel();
        replication.delete(
            &entity(),
            replication.leased,
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        settle(&mut replication, &ids[0], &mut replicas);
        assert_eq!((written.confirmed(), written.failed()), (2, 0));

        // Nobody answers this one, so it fails at its deadline.
        let (answer, _expired) = oneshot::channel();
        replication.commit(
            &entity(),
            replication.leased,
            pages(b"three"),
            true,
            Some(&around(&ids)),
            answer,
            now,
        );
        let _ = replication.take();
        replication.fired(now + Duration::from_secs(5));
        // And this key is not held here at all.
        let elsewhere = (ACTOR.to_owned(), "key-2".to_owned());
        let (answer, _refused) = oneshot::channel();
        replication.commit(
            &elsewhere,
            replication.leased,
            pages(b"one"),
            true,
            Some(&around(&ids)),
            answer,
            now,
        );

        assert_eq!((written.confirmed(), written.failed()), (2, 2));
    }
}
