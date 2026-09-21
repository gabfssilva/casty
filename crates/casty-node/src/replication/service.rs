//! The component `replication`: the replica this node keeps, and the keys it owns.
//!
//! The cores decide; this drives them. An operation ends when a core returns an outcome, when the replicas take too
//! long, or when a later one starts — an answer that arrives after that is dropped by its epoch or its stamp.

use std::collections::HashMap;
use std::time::Duration;

use casty_core::node::NodeId;
use casty_core::replication::messages::{Reply, Request, Write};
use casty_core::replication::owner::{Outcome, Owner, Send, Step, TooLarge};
use casty_core::replication::replica::Replica;
use casty_core::store::{Held, Pages};
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::wire::{Message, actor_of, key_of};

/// A key, by the type it belongs to and its name.
pub type Entity = (String, String);

/// Why an operation did not go through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// Too few replicas took part, or they did not answer in time.
    Unavailable(String),
    /// Another owner took the key. It never reaches the body: the activation ends on it.
    Fencing(String),
    /// A page of the state does not fit in one message.
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
}

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
}

#[derive(Debug)]
enum Waiting {
    Activate {
        initial: Option<Pages>,
        answer: oneshot::Sender<Result<Option<Held>, Failure>>,
    },
    Commit {
        answer: oneshot::Sender<Result<(), Failure>>,
    },
}

impl Waiting {
    /// Hand an outcome of the core to whoever is waiting, in the shape that call answers in.
    fn settle(self, actor: &str, key: &str, outcome: Outcome) {
        match (self, outcome) {
            (Self::Activate { answer, .. }, Outcome::Activated { state, created }) => {
                let _ = answer.send(Ok(Some(Held {
                    pages: state,
                    created,
                })));
            }
            (Self::Activate { answer, .. }, Outcome::Missing) => {
                let _ = answer.send(Ok(None));
            }
            (Self::Activate { answer, .. }, Outcome::Insufficient) => {
                let _ = answer.send(Err(Failure::Unavailable(format!(
                    "{actor}/{key}: too few replicas answered"
                ))));
            }
            (Self::Commit { answer }, Outcome::Saved) => {
                let _ = answer.send(Ok(()));
            }
            (Self::Commit { answer }, Outcome::Fenced) => {
                let _ = answer.send(Err(Failure::Fencing(format!(
                    "{actor}/{key} moved to another owner"
                ))));
            }
            (Self::Commit { answer }, Outcome::Insufficient) => {
                let _ = answer.send(Err(Failure::Unavailable(format!(
                    "{actor}/{key}: too few replicas confirmed the write"
                ))));
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
            }
        }
    }

    fn expire(self, actor: &str, key: &str) {
        let reason = match &self {
            Self::Activate { .. } => format!("{actor}/{key}: the replicas did not answer in time"),
            Self::Commit { .. } => format!("{actor}/{key}: the replicas did not confirm in time"),
        };
        self.fail(Failure::Unavailable(reason));
    }

    fn fail(self, failure: Failure) {
        match self {
            Self::Activate { answer, .. } => {
                let _ = answer.send(Err(failure));
            }
            Self::Commit { answer } => {
                let _ = answer.send(Err(failure));
            }
        }
    }
}

#[derive(Debug)]
pub struct Replication {
    node: NodeId,
    replica: Replica,
    owners: HashMap<Entity, Owner>,
    operations: HashMap<Entity, Operation>,
    sends: Vec<Outgoing>,
    /// Keys a write from another node just marked active, which is what brings one back to the node that owns it.
    arrived: Vec<Entity>,
    limit: usize,
    write_timeout: Duration,
}

impl Replication {
    #[must_use]
    pub fn new(node: NodeId, limit: usize, write_timeout: Duration) -> Self {
        Self {
            replica: Replica::new(node.clone(), limit),
            node,
            owners: HashMap::new(),
            operations: HashMap::new(),
            sends: Vec::new(),
            arrived: Vec::new(),
            limit,
            write_timeout,
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
        let operation = Operation {
            waiting: Waiting::Activate {
                initial: initial.clone(),
                answer,
            },
            deadline: now + self.write_timeout,
            grace: Some(now + GRACE),
            retry: None,
        };
        self.operations.insert(entity.clone(), operation);
        let owner = self.owner(&entity, around);
        let started = owner.activate(initial, around.wanted);
        self.start(&entity, started);
    }

    /// Write the state of a key this node owns, with the active mark unless the key is letting go.
    pub fn commit(
        &mut self,
        actor: &str,
        key: &str,
        pages: Pages,
        active: bool,
        answer: oneshot::Sender<Result<(), Failure>>,
        now: Instant,
    ) {
        let entity = (actor.to_owned(), key.to_owned());
        let Some(owner) = self.owners.get_mut(&entity) else {
            let _ = answer.send(Err(Failure::Unavailable(format!(
                "{actor}/{key} is not held by this node"
            ))));
            return;
        };
        let started = if active {
            owner.save(pages)
        } else {
            owner.release(pages)
        };
        self.operations.insert(
            entity.clone(),
            Operation {
                waiting: Waiting::Commit { answer },
                deadline: now + self.write_timeout,
                grace: None,
                retry: None,
            },
        );
        self.start(&entity, started);
    }

    /// Drop the owner of a key this node stopped holding. Its replica keeps the state.
    pub fn forget(&mut self, actor: &str, key: &str) {
        self.owners.remove(&(actor.to_owned(), key.to_owned()));
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

    /// An answer of a replica, for the owner that is waiting for it.
    pub fn answer(&mut self, reply: Reply) {
        let entity = (actor_of(&reply).to_owned(), key_of(&reply).to_owned());
        let Some(owner) = self.owners.get_mut(&entity) else {
            return;
        };
        let step = owner.receive(reply);
        self.apply(&entity, step);
    }

    /// The earliest deadline any operation has, which is when this must be looked at again.
    #[must_use]
    pub fn due(&self) -> Option<Instant> {
        self.operations
            .values()
            .flat_map(|operation| {
                [Some(operation.deadline), operation.grace, operation.retry]
                    .into_iter()
                    .flatten()
            })
            .min()
    }

    /// Carry out the deadlines that came due, and say which activations must start again.
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
            if let Some(operation) = self.operations.remove(&entity) {
                operation.waiting.expire(&entity.0, &entity.1);
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

    /// Start a fenced activation again, above the round the rival promised.
    pub fn again(&mut self, entity: &Entity, around: &Around) {
        let Some(operation) = self.operations.get(entity) else {
            return;
        };
        let Waiting::Activate { initial, .. } = &operation.waiting else {
            return;
        };
        let initial = initial.clone();
        let owner = self.owner(entity, around);
        let started = owner.activate(initial, around.wanted);
        self.start(entity, started);
    }

    /// The owner of a key, rebuilt when the replicas changed, always above the round its replica promised.
    fn owner(&mut self, entity: &Entity, around: &Around) -> &mut Owner {
        let (actor, key) = entity;
        let held = self
            .owners
            .get(entity)
            .is_some_and(|owner| owner.replicas() == around.replicas);
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
            );
            self.owners.insert(entity.clone(), owner);
        }
        self.owners
            .get_mut(entity)
            .expect("the owner was just put there")
    }

    /// Send what the core asked for, or end the operation on a state that does not fit.
    fn start(&mut self, entity: &Entity, started: Result<Vec<Send>, TooLarge>) {
        match started {
            Ok(sends) => self.queue(sends),
            Err(TooLarge(reason)) => {
                if let Some(operation) = self.operations.remove(entity) {
                    operation.waiting.fail(Failure::TooLarge(reason));
                }
            }
        }
    }

    fn apply(&mut self, entity: &Entity, step: Step) {
        self.queue(step.sends);
        let Some(outcome) = step.outcome else {
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
        let Some(operation) = self.operations.get_mut(entity) else {
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
        let operation = self
            .operations
            .remove(entity)
            .expect("the operation was just there");
        operation.waiting.settle(&entity.0, &entity.1, outcome);
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
    }
}

const GRACE: Duration = Duration::from_millis(200);
const RETRY: Duration = Duration::from_millis(50);
