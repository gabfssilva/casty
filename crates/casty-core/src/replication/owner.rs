//! The activation and the saves of a key, by the node that owns it.
//!
//! The owner of a durable key also decides what the store of the system is asked: its record, before an activation
//! chooses the state it goes on from, and to keep each write the replicas confirmed. The caller asks it and answers
//! with `loaded`; nothing else of the store reaches the owner.

use std::collections::{BTreeSet, HashMap};

use super::messages::{ACTIVE, DELETED, Epoch, Reply, Request, Stamp, Write, tombstone};
use super::parts::{append, cost, split};
use crate::node::{NodeId, Send};
use crate::store::{Durable, Pages, Storage, Stored};

/// How an operation ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// W replicas accepted the state with the active mark. `created` says no replica had a state for the key.
    Activated {
        state: Pages,
        created: bool,
    },
    /// No replica has a state for the key, and the activation had no initial one.
    Missing,
    Saved,
    /// A replica promised a later epoch.
    Fenced,
    /// Too few replicas counted toward the quorum: for an activation, by the time the caller stopped waiting for
    /// the ones that had not answered; for a write, once every replica had answered and the ones that were still
    /// receiving their range left it short. A write is not asked again, so waiting further would not help it.
    Insufficient,
}

/// What a step of the protocol produces: what to send, the outcome once it is known, and what the store of the system
/// is asked.
///
/// `store` is `Load` while an activation of a durable key waits for the record of the store, and `Save` beside the
/// outcome of a write the store is to keep: never a write the replicas refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub sends: Vec<Send<Request>>,
    pub outcome: Option<Outcome>,
    pub store: Option<Storage>,
}

impl Step {
    fn waiting() -> Self {
        Self::sending(Vec::new())
    }

    fn sending(sends: Vec<Send<Request>>) -> Self {
        Self {
            sends,
            outcome: None,
            store: None,
        }
    }
}

/// How many replicas confirm a write, and how many are read before a state is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quorums {
    pub writes: usize,
    pub reads: usize,
}

#[must_use]
pub fn quorums(write: Write, replicas: usize) -> Quorums {
    let majority = replicas / 2 + 1;
    match write {
        Write::One => Quorums {
            writes: 1,
            reads: 1,
        },
        Write::Majority => Quorums {
            writes: majority,
            reads: majority,
        },
        Write::All => Quorums {
            writes: replicas,
            reads: 1,
        },
    }
}

/// A page whose name leaves no room in a message for any of its bytes, which no write can carry.
///
/// A page larger than a message is not one: it is cut across as many as it takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TooLarge(pub String);

#[derive(Debug, Clone)]
struct Written {
    stamp: Stamp,
    base: Option<Stamp>,
    state: Pages,
    changed: Pages,
    dropped: Vec<String>,
}

#[derive(Debug)]
enum Phase {
    /// `pending` is the write of a running activation that waits for this promise, when the ring moved the replicas
    /// of the key: it goes out once the new replicas are known to hold nothing later than what the owner confirmed.
    Preparing {
        initial: Option<Pages>,
        wanted: usize,
        promises: HashMap<NodeId, Promised>,
        store: Lookup,
        pending: Option<Pending>,
    },
    /// Reading the latest write from `source`, one part at a time: `part` is the one asked for. `behind` says the store
    /// keeps an older write than that one, so it is saved once the activation is confirmed.
    Fetching {
        stamp: Stamp,
        source: NodeId,
        names: Vec<String>,
        part: u32,
        state: Pages,
        behind: bool,
    },
    /// `save` says the store keeps the write once it is confirmed.
    Writing {
        write: Written,
        created: Option<bool>,
        acks: BTreeSet<NodeId>,
        answers: BTreeSet<NodeId>,
        save: bool,
    },
}

/// A write held back until the owner has promised an epoch on the replicas the ring moved the key to.
#[derive(Debug, Clone)]
enum Pending {
    /// A save or a release: the state as it goes, with or without the mark.
    Write(Pages),
    Delete,
}

/// Where an activation stands with the store of the system.
#[derive(Debug, Clone)]
enum Lookup {
    /// The key is not durable, or its replicas have not answered enough for the activation to decide yet.
    Unasked,
    Asked,
    /// The record the store keeps, if any.
    Answered(Option<Stored>),
}

/// A promise as the owner keeps it while it waits for the others.
#[derive(Debug, Clone)]
struct Promised {
    replica: NodeId,
    accepted: Option<Stamp>,
    sizes: Vec<(String, usize)>,
    pages: Pages,
    receiving: bool,
}

/// `replicas` are the R replicas of the key, this node included when it is one of them. `limit` is the number of
/// bytes of page names and data that fit in one message, and `round` the highest round this node knows for the key.
/// `durable` is when the store of the system keeps the writes of the key, nothing for a key it does not keep.
///
/// `activate`, `save`, `release` and `delete` start an operation, and `receive` gives its outcome once it is known.
/// Starting an operation abandons the one in flight.
#[derive(Debug)]
pub struct Owner {
    actor: String,
    key: String,
    replicas: Vec<NodeId>,
    limit: usize,
    quorums: Quorums,
    durable: Option<Durable>,
    epoch: Epoch,
    round: u64,
    version: u64,
    phase: Option<Phase>,
    confirmed: Option<Written>,
    latest: Option<Written>,
    /// How many of the replicas the ring moved the key to are awaited before the next write goes to them.
    moved: Option<usize>,
}

impl Owner {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor: &str,
        key: &str,
        node: NodeId,
        replicas: Vec<NodeId>,
        write: Write,
        limit: usize,
        round: u64,
        durable: Option<Durable>,
    ) -> Self {
        let quorums = quorums(write, replicas.len());
        Self {
            actor: actor.to_owned(),
            key: key.to_owned(),
            replicas,
            limit,
            quorums,
            durable,
            epoch: Epoch { round, node },
            round,
            version: 0,
            phase: None,
            confirmed: None,
            latest: None,
            moved: None,
        }
    }

    /// Epoch of the latest activation.
    #[must_use]
    pub fn epoch(&self) -> &Epoch {
        &self.epoch
    }

    /// When the store of the system keeps the writes of the key.
    #[must_use]
    pub fn durable(&self) -> Option<Durable> {
        self.durable
    }

    /// The replica set this owner writes to, which a ring change replaces by a new owner.
    #[must_use]
    pub fn replicas(&self) -> &[NodeId] {
        &self.replicas
    }

    /// Promise a new epoch, choose the latest state among the replicas that answer, and write it with the mark.
    ///
    /// `initial` is written when no replica has a state. `wanted` replicas are awaited before choosing, so that the
    /// owner reads every replica known to be alive and not only the read quorum: `W + P > R` holds inside one replica
    /// set, and when the set changes it is reading all of the new one that finds a write the old owner confirmed.
    /// `settle` decides with fewer, down to the read quorum, when the caller stops waiting.
    ///
    /// A durable key reads the store too, once enough replicas answered to decide (`Step.store` is `Load`), and goes on
    /// from the later of the replicas' latest write and the record of the store.
    pub fn activate(
        &mut self,
        initial: Option<Pages>,
        wanted: usize,
    ) -> Result<Vec<Send<Request>>, TooLarge> {
        self.check(initial.as_ref().unwrap_or(&Pages::new()))?;
        self.moved = None;
        Ok(self.prepare(initial, wanted, Lookup::Unasked, None))
    }

    /// Promise an epoch in a round above any this owner knows, and ask every replica for what it holds.
    fn prepare(
        &mut self,
        initial: Option<Pages>,
        wanted: usize,
        store: Lookup,
        pending: Option<Pending>,
    ) -> Vec<Send<Request>> {
        self.round += 1;
        self.epoch = Epoch {
            round: self.round,
            node: self.epoch.node.clone(),
        };
        self.phase = Some(Phase::Preparing {
            initial,
            wanted,
            promises: HashMap::new(),
            store,
            pending,
        });
        let prepare = Request::Prepare {
            actor: self.actor.clone(),
            key: self.key.clone(),
            epoch: self.epoch.clone(),
        };
        self.replicas
            .iter()
            .map(|replica| Send {
                to: replica.clone(),
                message: prepare.clone(),
            })
            .collect()
    }

    /// The record the store of the system keeps of the key, which an activation that asked for it goes on with.
    ///
    /// A record written in a round this owner has not passed makes it promise again above that round, the record kept:
    /// the store keeps no write older than its record, so every write of this owner has to come after it.
    pub fn loaded(&mut self, stored: Option<Stored>) -> Step {
        let Some(Phase::Preparing {
            initial,
            wanted,
            store: store @ Lookup::Asked,
            pending,
            ..
        }) = &mut self.phase
        else {
            return Step::waiting();
        };
        let above = stored
            .as_ref()
            .filter(|record| !record.stamp.epoch.before(&self.epoch))
            .map(|record| record.stamp.epoch.round);
        if let Some(round) = above {
            let (initial, wanted, pending) = (initial.take(), *wanted, pending.take());
            self.round = self.round.max(round);
            return Step::sending(self.prepare(initial, wanted, Lookup::Answered(stored), pending));
        }
        *store = Lookup::Answered(stored);
        self.decide(false)
    }

    /// Whether a replica answered that it has not got the range of this key yet.
    ///
    /// Its answer counts for no quorum while that lasts, so a caller that would otherwise give up asks again
    /// instead: what ends the wait is the deadline of the operation, not this answer.
    #[must_use]
    pub fn receiving(&self) -> bool {
        match &self.phase {
            Some(Phase::Preparing { promises, .. }) => {
                promises.values().any(|promise| promise.receiving)
            }
            Some(Phase::Writing { acks, answers, .. }) => acks.len() < answers.len(),
            _ => false,
        }
    }

    /// Decide an activation that is still waiting for replicas, with the promises it has.
    pub fn settle(&mut self) -> Step {
        if !matches!(self.phase, Some(Phase::Preparing { .. })) {
            return Step::waiting();
        }
        self.decide(true)
    }

    /// Take the replicas the ring gives the key now, keeping the last write this owner confirmed.
    ///
    /// A write goes to the replicas of the key, and a ring that moved them must not leave the owner writing to the
    /// ones that had them: those keep the key only until the nodes that replicate it now have taken it, and a write
    /// they confirm after that goes away with the copy they drop.
    ///
    /// The next write promises a new epoch on `wanted` of the new replicas first, and goes out only if none of them
    /// accepted a write later than the one this owner confirmed. The promises this owner holds are on the replicas
    /// it had, and a write to the new ones without them meets no quorum of another owner's on that set: one that
    /// took the key over there could have confirmed writes this owner never read, which its own write would then
    /// take the place of.
    pub fn moved(&mut self, replicas: Vec<NodeId>, write: Write, wanted: usize) {
        self.quorums = quorums(write, replicas.len());
        self.replicas = replicas;
        self.moved = Some(wanted);
    }

    /// Ask the replicas the key moved to again for the promise a write waits on: one that was still receiving its
    /// range answers for the quorum only once it has it.
    pub fn again(&mut self) -> Vec<Send<Request>> {
        let Some(Phase::Preparing {
            pending: pending @ Some(_),
            wanted,
            ..
        }) = &mut self.phase
        else {
            return Vec::new();
        };
        let (wanted, pending) = (*wanted, pending.take());
        self.prepare(None, wanted, Lookup::Answered(None), pending)
    }

    /// Write `state` as a delta against the last confirmed write.
    pub fn save(&mut self, state: Pages) -> Result<Vec<Send<Request>>, TooLarge> {
        let mut marked = state;
        marked.insert(ACTIVE.to_owned(), Vec::new());
        self.next(marked)
    }

    /// Write `state` without the active mark, which is how a key that stopped being active lets go.
    ///
    /// Every other write carries the mark, so dropping it is an operation of its own and not a plain save.
    pub fn release(&mut self, state: Pages) -> Result<Vec<Send<Request>>, TooLarge> {
        self.next(state)
    }

    /// Write the deletion of the key: its tombstone takes the place of the state, at the write level of any write.
    ///
    /// It goes whole, with no base, so that a replica whatever it holds ends up with the tombstone and nothing else;
    /// and it carries no mark, so that nothing brings the key back. `Saved` is how it ends when it is confirmed.
    pub fn delete(&mut self) -> Vec<Send<Request>> {
        if let Some(wanted) = self.moved.take() {
            return self.prepare(None, wanted, Lookup::Answered(None), Some(Pending::Delete));
        }
        self.version += 1;
        self.write(
            tombstone(),
            None,
            &Pages::new(),
            None,
            self.durable.is_some(),
        )
    }

    pub fn receive(&mut self, reply: Reply) -> Step {
        match reply {
            Reply::Rejected { promised, .. } => self.rejected(&promised),
            Reply::Promise {
                epoch,
                replica,
                accepted,
                sizes,
                pages,
                receiving,
                ..
            } => self.promised(
                &epoch,
                Promised {
                    replica,
                    accepted,
                    sizes,
                    pages,
                    receiving,
                },
            ),
            Reply::Pages {
                epoch,
                accepted,
                part,
                final_part,
                pages,
                ..
            } => self.fetched(&epoch, accepted.as_ref(), part, final_part, pages),
            Reply::Accepted {
                stamp,
                replica,
                receiving,
                ..
            } => self.accepted(&stamp, replica, receiving),
            Reply::NeedFull { stamp, replica, .. } => self.resend(&stamp, &replica),
            // An answer to a replica that keeps a tombstone, which no owner asks for.
            Reply::Buried { .. } => Step::waiting(),
        }
    }

    fn next(&mut self, state: Pages) -> Result<Vec<Send<Request>>, TooLarge> {
        self.check(&state)?;
        if let Some(wanted) = self.moved.take() {
            return Ok(self.prepare(
                None,
                wanted,
                Lookup::Answered(None),
                Some(Pending::Write(state)),
            ));
        }
        Ok(self.following(state))
    }

    /// Write `state` as a delta against the last confirmed write.
    fn following(&mut self, state: Pages) -> Vec<Send<Request>> {
        let confirmed = self
            .confirmed
            .clone()
            .expect("a write before the activation");
        self.version += 1;
        self.write(
            state,
            confirmed.stamp.clone().into(),
            &confirmed.state,
            None,
            self.durable.is_some(),
        )
    }

    /// Go on with the write that waited for the promise of the replicas the key moved to.
    ///
    /// `latest` is the latest write among what they hold. One later than the last write this owner confirmed is
    /// another owner's, whose promise would have refused this one's already unless its round was raised by an
    /// answer that came late: the state in memory is not the one a new activation would go on from, and the write
    /// is fenced. One older is a write no replica of the new set has got yet, which this write carries whole.
    fn resume(&mut self, pending: Pending, latest: Option<&Stamp>) -> Step {
        let confirmed = self
            .confirmed
            .as_ref()
            .expect("a write before the activation");
        if latest.is_some_and(|stamp| confirmed.stamp.before(stamp)) {
            return self.end(Outcome::Fenced);
        }
        let sends = match pending {
            Pending::Write(state) => self.following(state),
            Pending::Delete => self.delete(),
        };
        Step::sending(sends)
    }

    fn rejected(&mut self, promised: &Epoch) -> Step {
        self.round = self.round.max(promised.round);
        if self.phase.is_none() || !self.epoch.before(promised) {
            return Step::waiting();
        }
        self.end(Outcome::Fenced)
    }

    fn promised(&mut self, epoch: &Epoch, promise: Promised) -> Step {
        let Some(Phase::Preparing {
            wanted, promises, ..
        }) = &mut self.phase
        else {
            return Step::waiting();
        };
        if *epoch != self.epoch {
            return Step::waiting();
        }
        promises.insert(promise.replica.clone(), promise);
        if promises.len() < (*wanted).min(self.replicas.len()) {
            return Step::waiting();
        }
        self.decide(false)
    }

    fn decide(&mut self, final_answer: bool) -> Step {
        let Some(Phase::Preparing {
            initial,
            promises,
            store,
            pending,
            ..
        }) = &mut self.phase
        else {
            return Step::waiting();
        };
        let held: Vec<Promised> = promises.values().cloned().collect();
        let counted = held.iter().filter(|promise| !promise.receiving).count();
        if counted < self.quorums.reads {
            // A replica that has not got the range of the key yet answered, and its answer is not countable and not
            // a refusal either: the range is on its way. Every replica having answered is the last word only when
            // none of them is in that state, so here the owner waits and asks again.
            let receiving = counted != held.len();
            if final_answer || (held.len() == self.replicas.len() && !receiving) {
                return self.end(Outcome::Insufficient);
            }
            return Step::waiting();
        }
        if let Some(pending) = pending.take() {
            let latest = latest_of(&held);
            return self.resume(pending, latest.as_ref());
        }
        // The store is read once the replicas have answered enough to decide, so that an activation that cannot go on
        // costs it nothing.
        let record = match &*store {
            Lookup::Answered(record) => record.clone(),
            Lookup::Asked => return Step::waiting(),
            Lookup::Unasked => {
                if self.durable.is_some() {
                    *store = Lookup::Asked;
                    return Step {
                        store: Some(Storage::Load),
                        ..Step::waiting()
                    };
                }
                None
            }
        };
        let initial = initial.clone();
        // Every answer is read, the ones that count for the quorum and the ones that do not. A replica still filling
        // its range is no part of the quorum, but its copy is a state like any other, and a state can only move the
        // choice forward: it may be the last one holding the write the owner before this one confirmed, which is the
        // case when the ring took the key off it and gave it back before the transfer was through.
        let latest = latest_of(&held);
        // The store keeps a later write than any replica that answered, which is every replica holding it lost.
        if let Some(record) = record.as_ref().filter(|record| {
            latest
                .as_ref()
                .is_none_or(|stamp| stamp.before(&record.stamp))
        }) {
            return match (record.pages.clone(), initial) {
                (Some(pages), _) => self.commit(pages, None, Some(false), false),
                (None, Some(initial)) => self.commit(initial, None, Some(true), false),
                (None, None) => self.end(Outcome::Missing),
            };
        }
        // The store keeps an older write than the one the key goes on from, or none: it takes the activation once it
        // is confirmed.
        let behind = self.durable.is_some()
            && latest.as_ref().is_some_and(|stamp| {
                record
                    .as_ref()
                    .is_none_or(|record| record.stamp.before(stamp))
            });
        // A latest write that deleted the key leaves nothing to go on from, and the key starts over as one nothing
        // wrote. The older copies it fences are no state either: the new write takes their place too.
        let latest = latest.filter(|stamp| {
            !held.iter().any(|promise| {
                promise.accepted.as_ref() == Some(stamp)
                    && promise.sizes.iter().any(|(name, _)| name == DELETED)
            })
        });
        let Some(latest) = latest else {
            let Some(initial) = initial else {
                return self.end(Outcome::Missing);
            };
            return self.commit(initial, None, Some(true), behind);
        };
        let sources: Vec<Promised> = held
            .into_iter()
            .filter(|promise| promise.accepted.as_ref() == Some(&latest))
            .collect();
        for source in &sources {
            let named: BTreeSet<&String> = source.pages.keys().collect();
            let indexed: BTreeSet<&String> = source.sizes.iter().map(|(name, _)| name).collect();
            if named == indexed {
                return self.commit(source.pages.clone(), Some(latest), Some(false), behind);
            }
        }
        let source = sources
            .into_iter()
            .next()
            .expect("a source of the latest write");
        self.phase = Some(Phase::Fetching {
            stamp: latest,
            source: source.replica,
            names: source.sizes.into_iter().map(|(name, _)| name).collect(),
            part: 0,
            state: Pages::new(),
            behind,
        });
        Step::sending(self.fetch())
    }

    /// Ask the source of the latest write for the part of its pages that comes next.
    ///
    /// One part at a time: the replica cuts them the same way on every request, and a part that went missing ends the
    /// wait at the deadline of the operation instead of leaving a page with a hole in it.
    fn fetch(&self) -> Vec<Send<Request>> {
        let Some(Phase::Fetching {
            source,
            names,
            part,
            ..
        }) = &self.phase
        else {
            return Vec::new();
        };
        vec![Send {
            to: source.clone(),
            message: Request::FetchPages {
                actor: self.actor.clone(),
                key: self.key.clone(),
                epoch: self.epoch.clone(),
                names: names.clone(),
                part: *part,
            },
        }]
    }

    fn fetched(
        &mut self,
        epoch: &Epoch,
        accepted: Option<&Stamp>,
        part: u32,
        final_part: bool,
        pages: Pages,
    ) -> Step {
        let Some(Phase::Fetching {
            stamp,
            part: asked,
            state,
            behind,
            ..
        }) = &mut self.phase
        else {
            return Step::waiting();
        };
        if *epoch != self.epoch || part != *asked {
            return Step::waiting();
        }
        if accepted != Some(&*stamp) {
            return self.end(Outcome::Fenced);
        }
        append(state, pages);
        if !final_part {
            *asked += 1;
            return Step::sending(self.fetch());
        }
        let (stamp, state, behind) = (stamp.clone(), core::mem::take(state), *behind);
        self.commit(state, Some(stamp), Some(false), behind)
    }

    fn accepted(&mut self, stamp: &Stamp, replica: NodeId, receiving: bool) -> Step {
        let replicas = self.replicas.len();
        let writes = self.quorums.writes;
        let done = match &mut self.phase {
            Some(Phase::Writing {
                write,
                created,
                acks,
                answers,
                save,
            }) if write.stamp == *stamp => {
                answers.insert(replica.clone());
                if !receiving {
                    acks.insert(replica);
                }
                if acks.len() >= writes {
                    Some(Some((write.clone(), *created, *save)))
                } else if answers.len() == replicas {
                    Some(None)
                } else {
                    None
                }
            }
            _ => return Step::waiting(),
        };
        match done {
            None => Step::waiting(),
            Some(None) => self.end(Outcome::Insufficient),
            Some(Some((write, created, save))) => {
                self.confirmed = Some(write.clone());
                let store = save.then(|| Storage::Save(record(&write)));
                let step = match created {
                    None => self.end(Outcome::Saved),
                    Some(created) => {
                        let mut state = write.state;
                        state.remove(ACTIVE);
                        self.end(Outcome::Activated { state, created })
                    }
                };
                Step { store, ..step }
            }
        }
    }

    fn resend(&mut self, stamp: &Stamp, replica: &NodeId) -> Step {
        let Some(latest) = self.latest.clone() else {
            return Step::waiting();
        };
        if latest.stamp != *stamp {
            return Step::waiting();
        }
        Step::sending(
            self.parts(&latest, true)
                .into_iter()
                .map(|message| Send {
                    to: replica.clone(),
                    message,
                })
                .collect(),
        )
    }

    /// Write the state an activation goes on from, with the mark. `save` says the store takes it once it is confirmed.
    fn commit(
        &mut self,
        state: Pages,
        base: Option<Stamp>,
        created: Option<bool>,
        save: bool,
    ) -> Step {
        self.version = base.as_ref().map_or(0, |held| held.version) + 1;
        let before = if base.is_some() {
            state.clone()
        } else {
            Pages::new()
        };
        let mut marked = state;
        marked.insert(ACTIVE.to_owned(), Vec::new());
        Step::sending(self.write(marked, base, &before, created, save))
    }

    fn write(
        &mut self,
        state: Pages,
        base: Option<Stamp>,
        before: &Pages,
        created: Option<bool>,
        save: bool,
    ) -> Vec<Send<Request>> {
        let changed: Pages = state
            .iter()
            .filter(|(name, data)| before.get(*name) != Some(*data))
            .map(|(name, data)| (name.clone(), data.clone()))
            .collect();
        let dropped: Vec<String> = before
            .keys()
            .filter(|name| !state.contains_key(*name))
            .cloned()
            .collect();
        let full = base.is_none();
        let write = Written {
            stamp: Stamp {
                epoch: self.epoch.clone(),
                version: self.version,
            },
            base,
            state,
            changed,
            dropped,
        };
        let parts = self.parts(&write, full);
        self.latest = Some(write.clone());
        self.phase = Some(Phase::Writing {
            write,
            created,
            acks: BTreeSet::new(),
            answers: BTreeSet::new(),
            save,
        });
        self.replicas
            .iter()
            .flat_map(|replica| {
                parts.iter().map(move |part| Send {
                    to: replica.clone(),
                    message: part.clone(),
                })
            })
            .collect()
    }

    fn parts(&self, write: &Written, full: bool) -> Vec<Request> {
        let pages = if full { &write.state } else { &write.changed };
        let grouped = split(pages, self.limit);
        let last = grouped.len() - 1;
        grouped
            .into_iter()
            .enumerate()
            .map(|(part, pages)| Request::Accept {
                actor: self.actor.clone(),
                key: self.key.clone(),
                stamp: write.stamp.clone(),
                base: if full { None } else { write.base.clone() },
                #[allow(clippy::cast_possible_truncation)]
                part: part as u32,
                final_part: part == last,
                pages,
                dropped: if part == last && !full {
                    write.dropped.clone()
                } else {
                    Vec::new()
                },
            })
            .collect()
    }

    fn check(&self, state: &Pages) -> Result<(), TooLarge> {
        for name in state.keys() {
            if cost(name, 1) > self.limit {
                return Err(TooLarge(format!(
                    "{}/{}: the field {name:?} has a name of {} bytes, which leaves no room for its data in the {} \
                     bytes a message has for the state; raise `Limits.message` on every node and client",
                    self.actor,
                    self.key,
                    name.len(),
                    self.limit
                )));
            }
        }
        Ok(())
    }

    fn end(&mut self, outcome: Outcome) -> Step {
        self.phase = None;
        Step {
            outcome: Some(outcome),
            ..Step::waiting()
        }
    }
}

/// The latest write among what the replicas that promised hold.
fn latest_of(held: &[Promised]) -> Option<Stamp> {
    held.iter()
        .filter_map(|promise| promise.accepted.clone())
        .reduce(|kept, other| if kept.before(&other) { other } else { kept })
}

/// A confirmed write as the store keeps it: its state without the mark, or nothing for a deletion.
fn record(write: &Written) -> Stored {
    let pages = (!write.state.contains_key(DELETED)).then(|| {
        let mut pages = write.state.clone();
        pages.remove(ACTIVE);
        pages
    });
    Stored {
        stamp: write.stamp.clone(),
        pages,
    }
}

#[cfg(test)]
mod tests {
    use super::super::messages::{ACTIVE, Epoch, Reply, Request, Stamp, Write, tombstone};
    use super::super::parts::{append, cost, packed};
    use super::super::replica::{Arriving, Replica};
    use super::{Outcome, Owner, Step};
    use crate::node::{NodeId, Send};
    use crate::rolls::Rolls;
    use crate::store::{Durable, Pages, Storage, Stored};

    const LIMIT: usize = 64;

    fn pages(entries: &[(&str, &[u8])]) -> Pages {
        entries
            .iter()
            .map(|(name, data)| ((*name).to_owned(), (*data).to_vec()))
            .collect()
    }

    /// `size` bytes that differ from one another from `from` on, so that a piece out of place shows.
    fn long(size: usize, from: usize) -> Vec<u8> {
        (0..=u8::MAX).cycle().skip(from).take(size).collect()
    }

    /// What the pages a message carries take out of the message limit.
    fn carried(pages: &Pages) -> usize {
        pages
            .iter()
            .map(|(name, data)| cost(name, data.len()))
            .sum()
    }

    /// Three replicas and the owner of a key on the first of them.
    struct Cluster {
        ids: Vec<NodeId>,
        replicas: Vec<Replica>,
    }

    impl Cluster {
        fn new(size: usize, seed: u64) -> Self {
            let ids = Rolls::seeded(seed).nodes(size);
            let replicas = ids
                .iter()
                .map(|node| Replica::new(node.clone(), LIMIT))
                .collect();
            Self { ids, replicas }
        }

        fn owner(&self, at: usize, round: u64, write: Write) -> Owner {
            let over: Vec<usize> = (0..self.ids.len()).collect();
            self.owner_over(at, &over, round, write)
        }

        /// An owner of the key on `at`, replicated on the nodes `over` names, which is a ring that placed it there.
        fn owner_over(&self, at: usize, over: &[usize], round: u64, write: Write) -> Owner {
            Owner::new(
                "account",
                "a",
                self.ids[at].clone(),
                over.iter().map(|index| self.ids[*index].clone()).collect(),
                write,
                LIMIT,
                round,
                None,
            )
        }

        /// An owner on `at` over the three first nodes, of a key whose every write the store of the system keeps.
        fn durable(&self, at: usize, round: u64) -> Owner {
            self.durable_over(at, &[0, 1, 2], round)
        }

        fn durable_over(&self, at: usize, over: &[usize], round: u64) -> Owner {
            Owner::new(
                "account",
                "a",
                self.ids[at].clone(),
                over.iter().map(|index| self.ids[*index].clone()).collect(),
                Write::Majority,
                LIMIT,
                round,
                Some(Durable::Write),
            )
        }

        fn at(&self, node: &NodeId) -> usize {
            self.ids
                .iter()
                .position(|held| held == node)
                .expect("a replica of this key")
        }

        /// Run every request to the end, feeding the answers back, and give the outcome the owner reached.
        ///
        /// Everything is delivered even after the outcome is known: the quorum is what the owner waits for, and the
        /// replicas outside it still take the write. No message on the way carries more pages than fit in one.
        fn run(&mut self, owner: &mut Owner, sends: Vec<Send<Request>>) -> Option<Outcome> {
            self.kept(owner, sends, &mut Kept::default())
        }

        /// `run`, with `store` answering what the owner asks of the store of the system as soon as it asks.
        fn kept(
            &mut self,
            owner: &mut Owner,
            sends: Vec<Send<Request>>,
            store: &mut Kept,
        ) -> Option<Outcome> {
            let mut pending = sends;
            let mut reached = None;
            while !pending.is_empty() {
                let mut next = Vec::new();
                for send in pending {
                    if let Request::Accept { pages, .. } = &send.message {
                        assert!(carried(pages) <= LIMIT, "a write of {}", carried(pages));
                    }
                    let at = self.at(&send.to);
                    let Some(reply) = self.replicas[at].receive(send.message, false) else {
                        continue;
                    };
                    if let Reply::Promise { pages, .. } | Reply::Pages { pages, .. } = &reply {
                        assert!(carried(pages) <= LIMIT, "an answer of {}", carried(pages));
                    }
                    let mut step = owner.receive(reply);
                    // What the owner does with the record is a step of its own, which may send again.
                    while let Some(Storage::Load) = step.store {
                        next.extend(step.sends);
                        step = owner.loaded(store.record.clone());
                    }
                    let Step {
                        sends,
                        outcome,
                        store: asked,
                    } = step;
                    if let Some(Storage::Save(stored)) = asked {
                        store.saves.push(stored.clone());
                        store.keep(stored);
                    }
                    reached = outcome.or(reached);
                    next.extend(sends);
                }
                pending = next;
            }
            reached
        }
    }

    /// The store of the system as these tests hold it, for the one key they write: the record it keeps, kept as a store
    /// must, and every save it was asked for in the order it was asked.
    #[derive(Debug, Default)]
    struct Kept {
        record: Option<Stored>,
        saves: Vec<Stored>,
    }

    impl Kept {
        fn keep(&mut self, stored: Stored) {
            if self
                .record
                .as_ref()
                .is_none_or(|record| record.version() <= stored.version())
            {
                self.record = Some(stored);
            }
        }

        fn state(&self) -> Option<Pages> {
            self.record.as_ref().and_then(|record| record.pages.clone())
        }
    }

    #[test]
    fn a_key_no_replica_has_is_created_from_the_state_the_activation_brings() {
        let mut cluster = Cluster::new(3, 31);
        let mut owner = cluster.owner(0, 0, Write::Majority);
        let sends = owner
            .activate(Some(pages(&[("balance", b"0")])), 3)
            .expect("it fits");

        let outcome = cluster.run(&mut owner, sends);

        assert_eq!(
            outcome,
            Some(Outcome::Activated {
                state: pages(&[("balance", b"0")]),
                created: true
            })
        );
        // The mark is on the replicas, so a sweep knows the key is active, and not in what the body sees.
        assert!(cluster.replicas[1].marked("account", "a"));
    }

    #[test]
    fn a_key_without_a_state_anywhere_and_no_initial_is_missing() {
        let mut cluster = Cluster::new(3, 32);
        let mut owner = cluster.owner(0, 0, Write::Majority);
        let sends = owner.activate(None, 3).expect("nothing to fit");

        assert_eq!(cluster.run(&mut owner, sends), Some(Outcome::Missing));
    }

    #[test]
    fn an_activation_finds_the_latest_write_and_a_save_goes_on_from_it() {
        let mut cluster = Cluster::new(3, 33);
        let mut first = cluster.owner(0, 0, Write::Majority);
        let sends = first
            .activate(Some(pages(&[("balance", b"1")])), 3)
            .expect("it fits");
        cluster.run(&mut first, sends);
        let sends = first.save(pages(&[("balance", b"7")])).expect("it fits");
        assert_eq!(cluster.run(&mut first, sends), Some(Outcome::Saved));

        // Another node takes the key over and must find what the first one confirmed.
        let mut second = cluster.owner(1, 1, Write::Majority);
        let sends = second
            .activate(Some(pages(&[("balance", b"0")])), 3)
            .expect("it fits");
        let outcome = cluster.run(&mut second, sends);

        assert_eq!(
            outcome,
            Some(Outcome::Activated {
                state: pages(&[("balance", b"7")]),
                created: false
            })
        );
    }

    #[test]
    fn an_owner_a_replica_has_fenced_is_told_so_and_writes_nothing() {
        let mut cluster = Cluster::new(3, 34);
        let mut first = cluster.owner(0, 0, Write::Majority);
        let sends = first
            .activate(Some(pages(&[("balance", b"1")])), 3)
            .expect("it fits");
        cluster.run(&mut first, sends);

        // A later owner promises a higher round on every replica.
        let mut second = cluster.owner(1, 5, Write::Majority);
        let sends = second.activate(None, 3).expect("nothing to fit");
        cluster.run(&mut second, sends);

        let sends = first.save(pages(&[("balance", b"9")])).expect("it fits");
        let outcome = cluster.run(&mut first, sends);

        assert_eq!(outcome, Some(Outcome::Fenced));
        // What the fenced owner tried to write is nowhere: the replicas kept the write of the later term.
        let copy = &cluster.replicas[2].copies("account", "a")[0];
        assert_eq!(
            copy.pages.get("balance").map(Vec::as_slice),
            Some(&b"1"[..])
        );
    }

    #[test]
    fn a_state_larger_than_a_message_is_fetched_in_parts_and_written_back_whole() {
        let mut cluster = Cluster::new(3, 35);
        let big = pages(&[("one", &[1; 40]), ("two", &[2; 40]), ("three", &[3; 40])]);
        let mut first = cluster.owner(0, 0, Write::Majority);
        let sends = first
            .activate(Some(big.clone()), 3)
            .expect("each page fits");
        assert_eq!(
            cluster.run(&mut first, sends),
            Some(Outcome::Activated {
                state: big.clone(),
                created: true
            })
        );

        let mut second = cluster.owner(1, 1, Write::Majority);
        let sends = second.activate(None, 3).expect("nothing to fit");
        let outcome = cluster.run(&mut second, sends);

        assert_eq!(
            outcome,
            Some(Outcome::Activated {
                state: big,
                created: false
            })
        );
    }

    #[test]
    fn a_page_several_times_a_message_is_written_in_parts_and_read_back_whole() {
        let mut cluster = Cluster::new(3, 36);
        let large = pages(&[
            ("entries", long(5 * LIMIT, 0).as_slice()),
            ("owner", b"ana"),
        ]);
        let mut first = cluster.owner(0, 0, Write::Majority);
        let sends = first
            .activate(Some(large.clone()), 3)
            .expect("its names fit");
        assert_eq!(
            cluster.run(&mut first, sends),
            Some(Outcome::Activated {
                state: large,
                created: true
            })
        );
        assert!(
            cluster.replicas[2].copies("account", "a").len() > 5,
            "a page five times a message was kept in fewer parts"
        );

        // A save that grows the large page sends it as a delta, cut the same way.
        let grown = pages(&[
            ("entries", long(7 * LIMIT, 1).as_slice()),
            ("owner", b"ana"),
        ]);
        let sends = first.save(grown.clone()).expect("its names fit");
        assert_eq!(cluster.run(&mut first, sends), Some(Outcome::Saved));

        // No promise can carry it, so the next owner fetches it one part after another.
        let mut second = cluster.owner(1, 1, Write::Majority);
        let sends = second.activate(None, 3).expect("nothing to fit");
        assert_eq!(
            cluster.run(&mut second, sends),
            Some(Outcome::Activated {
                state: grown,
                created: false
            })
        );
    }

    /// A key moves to a node that did not have it the way a range or a handover carries it: in copies cut to fit in a
    /// message, put back together there before anything of them is installed.
    #[test]
    fn a_page_several_times_a_message_survives_a_transfer_to_a_node_that_did_not_have_it() {
        let mut cluster = Cluster::new(4, 47);
        let large = pages(&[
            ("entries", long(5 * LIMIT, 2).as_slice()),
            ("owner", b"ana"),
        ]);
        let mut before = cluster.owner_over(0, &[0, 1, 2], 0, Write::Majority);
        let sends = before
            .activate(Some(large.clone()), 3)
            .expect("its names fit");
        cluster.run(&mut before, sends);

        let copies = cluster.replicas[2].copies("account", "a");
        let messages = packed(copies, LIMIT);
        assert!(
            messages.len() > 5,
            "a page five times a message crossed in fewer"
        );
        let mut arriving = Arriving::default();
        let mut landed = Vec::new();
        let target = &mut cluster.replicas[3];
        for (part, message) in messages.into_iter().enumerate() {
            let part = u32::try_from(part).expect("a few messages");
            landed.extend(arriving.take("account", 1, part, message, target));
        }
        assert_eq!(landed, vec!["a".to_owned()]);
        assert!(arriving.whole());

        // Only the node that took the key is read, so what the activation finds is what the transfer carried.
        let mut after = cluster.owner_over(3, &[3], 1, Write::One);
        let sends = after.activate(None, 1).expect("nothing to fit");
        assert_eq!(
            cluster.run(&mut after, sends),
            Some(Outcome::Activated {
                state: large,
                created: false
            })
        );
    }

    #[test]
    fn a_page_whose_name_leaves_no_room_in_a_message_is_refused_naming_the_field_and_the_limit() {
        let cluster = Cluster::new(3, 37);
        let mut owner = cluster.owner(0, 0, Write::Majority);
        let name = "n".repeat(LIMIT);

        let refused = owner.activate(Some(pages(&[(name.as_str(), b"0")])), 3);

        let Err(super::TooLarge(why)) = refused else {
            panic!("a page no message can carry was taken");
        };
        assert!(
            why.contains(&name) && why.contains("Limits.message"),
            "{why}"
        );
    }

    #[test]
    fn an_activation_waits_for_the_replicas_that_are_still_receiving_their_range() {
        let mut cluster = Cluster::new(3, 37);
        let mut owner = cluster.owner(0, 0, Write::Majority);
        let state = pages(&[("balance", b"0")]);
        let sends = owner.activate(Some(state.clone()), 3).expect("it fits");

        // Every replica answers, and every answer says it is still receiving its range.
        let mut outcome = None;
        for send in sends {
            let at = cluster.at(&send.to);
            if let Some(reply) = cluster.replicas[at].receive(send.message, true) {
                outcome = owner.receive(reply).outcome.or(outcome);
            }
        }

        // None of those answers counts, and none of them refuses either: the ranges are on their way.
        assert_eq!(outcome, None);
        assert!(owner.receiving());

        // Asking again once they have their ranges is what decides it.
        let sends = owner.activate(Some(state.clone()), 3).expect("it fits");
        assert_eq!(
            cluster.run(&mut owner, sends),
            Some(Outcome::Activated {
                state,
                created: true
            })
        );
    }

    /// A replica the ring took the key off and gave back before its transfer was through keeps the copy it always
    /// had, which can be the only one left with the last write the owner before this one confirmed. It counts for no
    /// quorum while the range is arriving, and reading it all the same is what keeps that write.
    #[test]
    fn a_replica_still_filling_its_range_counts_for_no_quorum_and_is_read_all_the_same() {
        let mut cluster = Cluster::new(4, 44);
        let mut before = cluster.owner_over(0, &[0, 1, 2], 0, Write::Majority);
        let sends = before
            .activate(Some(pages(&[("entries", b"one")])), 3)
            .expect("it fits");
        cluster.run(&mut before, sends);

        // The write reaches the majority that confirms it, the first and the third node, and not the second.
        let sends = before.save(pages(&[("entries", b"two")])).expect("it fits");
        let reaching: Vec<Send<Request>> = sends
            .into_iter()
            .filter(|send| send.to != cluster.ids[1])
            .collect();
        assert_eq!(cluster.run(&mut before, reaching), Some(Outcome::Saved));

        // The ring put the key on the last three nodes, and the third one, the only one that has the write, is
        // filling the range that gave it the key back.
        let mut after = cluster.owner_over(1, &[1, 2, 3], 1, Write::Majority);
        let sends = after
            .activate(None, 3)
            .expect("nothing of its own to write");
        let mut pending = Vec::new();
        for send in sends {
            let at = cluster.at(&send.to);
            if let Some(reply) = cluster.replicas[at].receive(send.message, at == 2) {
                pending.extend(after.receive(reply).sends);
            }
        }

        assert_eq!(
            cluster.run(&mut after, pending),
            Some(Outcome::Activated {
                state: pages(&[("entries", b"two")]),
                created: false
            })
        );
    }

    #[test]
    fn an_activation_too_few_replicas_answered_ends_once_the_caller_stops_waiting() {
        let mut cluster = Cluster::new(3, 38);
        let mut owner = cluster.owner(0, 0, Write::Majority);
        let sends = owner
            .activate(Some(pages(&[("balance", b"0")])), 3)
            .expect("it fits");

        // One replica of the three answers, which is short of the read quorum of a majority.
        let send = sends.into_iter().next().expect("a replica to ask");
        let at = cluster.at(&send.to);
        let reply = cluster.replicas[at]
            .receive(send.message, false)
            .expect("it promised");
        assert_eq!(owner.receive(reply).outcome, None);
        assert!(!owner.receiving());

        assert_eq!(owner.settle().outcome, Some(Outcome::Insufficient));
    }

    #[test]
    fn settling_early_decides_with_the_promises_it_has() {
        let mut cluster = Cluster::new(3, 38);
        let mut owner = cluster.owner(0, 0, Write::Majority);
        let sends = owner
            .activate(Some(pages(&[("balance", b"3")])), 3)
            .expect("it fits");

        // Only two of the three replicas answer, which is the read quorum but fewer than it waited for.
        let mut pending = Vec::new();
        for send in sends.into_iter().take(2) {
            let at = cluster.at(&send.to);
            if let Some(reply) = cluster.replicas[at].receive(send.message, false) {
                pending.extend(owner.receive(reply).sends);
            }
        }
        let settled = owner.settle();
        let outcome = cluster.run(&mut owner, [pending, settled.sends].concat());

        assert_eq!(
            outcome,
            Some(Outcome::Activated {
                state: pages(&[("balance", b"3")]),
                created: true
            })
        );
    }

    #[test]
    fn a_replica_that_missed_the_base_of_a_delta_gets_the_whole_state() {
        let mut cluster = Cluster::new(3, 39);
        let mut owner = cluster.owner(0, 0, Write::One);
        let sends = owner
            .activate(Some(pages(&[("balance", b"1")])), 3)
            .expect("it fits");
        cluster.run(&mut owner, sends);

        // One replica forgets everything, so the delta that follows has no base there.
        cluster.replicas[2].drop("account", "a");
        let sends = owner.save(pages(&[("balance", b"4")])).expect("it fits");
        cluster.run(&mut owner, sends);

        let copy = &cluster.replicas[2].copies("account", "a")[0];
        assert_eq!(
            copy.pages.get("balance").map(Vec::as_slice),
            Some(&b"4"[..])
        );
        assert!(copy.pages.contains_key(ACTIVE));
    }

    #[test]
    fn releasing_a_key_takes_the_mark_off_every_replica() {
        let mut cluster = Cluster::new(3, 40);
        let mut owner = cluster.owner(0, 0, Write::Majority);
        let sends = owner
            .activate(Some(pages(&[("balance", b"2")])), 3)
            .expect("it fits");
        cluster.run(&mut owner, sends);

        let sends = owner.release(pages(&[("balance", b"2")])).expect("it fits");
        assert_eq!(cluster.run(&mut owner, sends), Some(Outcome::Saved));

        for replica in &cluster.replicas {
            assert!(
                !replica.marked("account", "a"),
                "the mark survived the release"
            );
            let copy = &replica.copies("account", "a")[0];
            assert_eq!(
                copy.pages.get("balance").map(Vec::as_slice),
                Some(&b"2"[..])
            );
        }
    }

    #[test]
    fn of_two_owners_racing_the_later_term_wins_and_no_confirmed_write_is_lost() {
        for seed in 1..=6_u64 {
            let mut cluster = Cluster::new(3, seed);
            let mut first = cluster.owner(0, 0, Write::Majority);
            let sends = first
                .activate(Some(pages(&[("balance", b"0")])), 3)
                .expect("it fits");
            cluster.run(&mut first, sends);
            let sends = first.save(pages(&[("balance", b"1")])).expect("it fits");
            assert_eq!(cluster.run(&mut first, sends), Some(Outcome::Saved));

            // A second owner takes over while the first keeps writing, and their requests interleave.
            let mut second = cluster.owner(1, 1, Write::Majority);
            let taking = second.activate(None, 3).expect("nothing to fit");
            let writing = first.save(pages(&[("balance", b"2")])).expect("it fits");
            let mut rolls = Rolls::seeded(seed.wrapping_mul(977));
            let mut pending: Vec<(bool, Send<Request>)> = taking
                .into_iter()
                .map(|send| (false, send))
                .chain(writing.into_iter().map(|send| (true, send)))
                .collect();
            let mut outcomes: Vec<(bool, Outcome)> = Vec::new();
            while !pending.is_empty() {
                let at = rolls.upto(pending.len());
                let (from_first, send) = pending.remove(at);
                let replica = cluster.at(&send.to);
                let Some(reply) = cluster.replicas[replica].receive(send.message, false) else {
                    continue;
                };
                let owner = if from_first { &mut first } else { &mut second };
                let Step { sends, outcome, .. } = owner.receive(reply);
                pending.extend(sends.into_iter().map(|send| (from_first, send)));
                if let Some(outcome) = outcome {
                    outcomes.push((from_first, outcome));
                }
            }

            let confirmed = outcomes
                .iter()
                .any(|(from_first, outcome)| *from_first && *outcome == Outcome::Saved);
            let found = outcomes
                .iter()
                .find_map(|(from_first, outcome)| match outcome {
                    Outcome::Activated { state, .. } if !from_first => Some(state.clone()),
                    _ => None,
                });
            let Some(state) = found else {
                // The second owner was fenced itself, or read too few replicas; either way it wrote nothing.
                assert!(
                    outcomes.iter().any(|(from_first, _)| !from_first),
                    "seed {seed}: the second owner neither took over nor gave up"
                );
                continue;
            };
            let balance = state.get("balance").map(Vec::as_slice);
            if confirmed {
                // A write the first owner confirmed reached a quorum, so the one that reads a quorum finds it.
                assert_eq!(
                    balance,
                    Some(&b"2"[..]),
                    "seed {seed}: it took over past a confirmed write"
                );
            } else {
                assert!(
                    balance == Some(&b"1"[..]) || balance == Some(&b"2"[..]),
                    "seed {seed}: it took over with {balance:?}"
                );
            }
        }
    }

    /// The promises an owner holds are on the replicas it had, and a write it sends to the ones the ring moved the
    /// key to meets no promise another owner took on them.
    #[test]
    fn a_write_after_the_ring_moved_the_key_promises_on_the_new_replicas_first() {
        let mut cluster = Cluster::new(4, 41);
        let mut owner = cluster.owner_over(0, &[0, 1, 2], 10, Write::Majority);
        let sends = owner
            .activate(Some(pages(&[("entries", b"one")])), 3)
            .expect("it fits");
        cluster.run(&mut owner, sends);
        let promised = owner.epoch().clone();

        // The ring moves the key to the last three nodes, and the fourth has nothing of it: the save asks the three
        // for a promise before anything else, and the write follows once they answered.
        owner.moved(cluster.ids[1..].to_vec(), Write::Majority, 3);
        let sends = owner.save(pages(&[("entries", b"two")])).expect("it fits");
        assert!(
            sends
                .iter()
                .all(|send| matches!(send.message, Request::Prepare { .. })),
            "{sends:?}"
        );
        let asked: Vec<&NodeId> = sends.iter().map(|send| &send.to).collect();
        assert_eq!(asked, cluster.ids[1..].iter().collect::<Vec<_>>());
        assert_eq!(cluster.run(&mut owner, sends), Some(Outcome::Saved));
        assert!(promised.before(owner.epoch()));
        assert_eq!(
            whole(&cluster.replicas[3]).get("entries"),
            Some(&b"two".to_vec())
        );

        // Another owner takes the key over on those replicas and writes past that. The ring moves this one's
        // replicas once more, and its next write, promising again, is refused by the promise the other holds.
        let mut other = cluster.owner_over(3, &[1, 2, 3], 20, Write::Majority);
        let sends = other.activate(None, 3).expect("nothing of its own");
        assert_eq!(
            cluster.run(&mut other, sends),
            Some(Outcome::Activated {
                state: pages(&[("entries", b"two")]),
                created: false
            })
        );
        let sends = other
            .save(pages(&[("entries", b"three")]))
            .expect("it fits");
        assert_eq!(cluster.run(&mut other, sends), Some(Outcome::Saved));

        owner.moved(cluster.ids[..3].to_vec(), Write::Majority, 3);
        let sends = owner.save(pages(&[("entries", b"four")])).expect("it fits");
        assert_eq!(cluster.run(&mut owner, sends), Some(Outcome::Fenced));
        for replica in &cluster.replicas[1..] {
            assert_eq!(whole(replica).get("entries"), Some(&b"three".to_vec()));
        }
    }

    /// What one replica keeps of the key, its parts put back together.
    fn whole(replica: &Replica) -> Pages {
        let mut rebuilt = Pages::new();
        for copy in replica.copies("account", "a") {
            append(&mut rebuilt, copy.pages);
        }
        rebuilt
    }

    /// An owner on `at` over the three first nodes that took the key from `initial` and then wrote `state`.
    fn written(cluster: &mut Cluster, at: usize, initial: &[u8], state: &[u8]) -> Owner {
        let mut owner = cluster.owner_over(at, &[0, 1, 2], 0, Write::Majority);
        let sends = owner
            .activate(Some(pages(&[("balance", initial)])), 3)
            .expect("it fits");
        cluster.run(&mut owner, sends);
        let sends = owner.save(pages(&[("balance", state)])).expect("it fits");
        assert_eq!(cluster.run(&mut owner, sends), Some(Outcome::Saved));
        owner
    }

    #[test]
    fn a_deleted_key_keeps_no_page_anywhere_and_starts_again_from_its_initial_state() {
        let mut cluster = Cluster::new(3, 51);
        let mut first = written(&mut cluster, 0, b"1", b"7");

        let sends = first.delete();
        assert_eq!(cluster.run(&mut first, sends), Some(Outcome::Saved));

        // Every replica keeps the tombstone and nothing else: no page of the state, and no mark to bring it back.
        for replica in &cluster.replicas {
            assert_eq!(whole(replica), tombstone());
            assert!(replica.deleted("account", "a").is_some());
            assert!(!replica.marked("account", "a"));
        }
        let mut second = cluster.owner(1, 1, Write::Majority);
        let sends = second.activate(None, 3).expect("nothing to fit");
        assert_eq!(cluster.run(&mut second, sends), Some(Outcome::Missing));
        let sends = second
            .activate(Some(pages(&[("balance", b"0")])), 3)
            .expect("it fits");
        assert_eq!(
            cluster.run(&mut second, sends),
            Some(Outcome::Activated {
                state: pages(&[("balance", b"0")]),
                created: true
            })
        );
        // The key written again took the place of every tombstone.
        for replica in &cluster.replicas {
            assert!(replica.deleted("account", "a").is_none());
            assert_eq!(
                whole(replica).get("balance").map(Vec::as_slice),
                Some(&b"0"[..])
            );
        }
    }

    #[test]
    fn a_save_after_a_deletion_writes_the_key_again_on_top_of_its_tombstone() {
        let mut cluster = Cluster::new(3, 54);
        let mut owner = written(&mut cluster, 0, b"1", b"7");
        let sends = owner.delete();
        cluster.run(&mut owner, sends);

        let sends = owner.save(pages(&[("balance", b"3")])).expect("it fits");
        assert_eq!(cluster.run(&mut owner, sends), Some(Outcome::Saved));

        for replica in &cluster.replicas {
            assert_eq!(
                whole(replica),
                pages(&[("balance", b"3"), (ACTIVE, b"")]),
                "the tombstone outlived the write that followed it"
            );
        }
    }

    #[test]
    fn a_replica_that_missed_the_deletion_does_not_bring_the_key_back() {
        let mut cluster = Cluster::new(3, 52);
        let mut first = written(&mut cluster, 0, b"1", b"7");

        // The deletion reaches the majority that confirms it, and not the third replica, which keeps the state.
        let missed = cluster.ids[2].clone();
        let reaching: Vec<Send<Request>> = first
            .delete()
            .into_iter()
            .filter(|send| send.to != missed)
            .collect();
        assert_eq!(cluster.run(&mut first, reaching), Some(Outcome::Saved));
        assert_eq!(
            whole(&cluster.replicas[2])
                .get("balance")
                .map(Vec::as_slice),
            Some(&b"7"[..])
        );

        // It comes back, and the owner reads it with only one of the replicas that took the deletion: the read quorum,
        // and no more. The tombstone is the later write, so the key starts over and the stale copy is written over.
        let away = cluster.ids[0].clone();
        let mut second = cluster.owner(2, 1, Write::Majority);
        let sends = second
            .activate(Some(pages(&[("balance", b"0")])), 3)
            .expect("it fits");
        let mut pending = Vec::new();
        for send in sends.into_iter().filter(|send| send.to != away) {
            let at = cluster.at(&send.to);
            if let Some(reply) = cluster.replicas[at].receive(send.message, false) {
                pending.extend(second.receive(reply).sends);
            }
        }
        let settled = second.settle();
        assert_eq!(
            cluster.run(&mut second, [pending, settled.sends].concat()),
            Some(Outcome::Activated {
                state: pages(&[("balance", b"0")]),
                created: true
            })
        );
        assert_eq!(
            whole(&cluster.replicas[2])
                .get("balance")
                .map(Vec::as_slice),
            Some(&b"0"[..])
        );
    }

    /// A deleted key reaches a node that did not have it the way a range or a handover carries it: as its tombstone,
    /// which fences the older copy another source still hands over, and lands as no state at all.
    #[test]
    fn a_deleted_key_moves_as_its_tombstone_and_never_as_a_state() {
        let mut cluster = Cluster::new(4, 53);
        let mut first = written(&mut cluster, 0, b"1", b"7");
        let missed = cluster.ids[2].clone();
        let reaching: Vec<Send<Request>> = first
            .delete()
            .into_iter()
            .filter(|send| send.to != missed)
            .collect();
        cluster.run(&mut first, reaching);

        // The fourth node takes the key from the replica that missed the deletion and from one that took it, in both
        // orders: the tombstone is the later write, and what it keeps.
        for stale_first in [true, false] {
            cluster.replicas[3].drop("account", "a");
            let stale = cluster.replicas[2].copies("account", "a");
            let deleted = cluster.replicas[1].copies("account", "a");
            let arriving = if stale_first {
                [stale, deleted]
            } else {
                [deleted, stale]
            };
            for copy in arriving.into_iter().flatten() {
                cluster.replicas[3].install("account", copy);
            }
            assert_eq!(whole(&cluster.replicas[3]), tombstone());
            assert!(!cluster.replicas[3].marked("account", "a"));
            assert!(
                cluster.replicas[3]
                    .laid()
                    .iter()
                    .any(|(_, key, _)| key == "a"),
                "the tombstone that came in does not linger"
            );
        }

        // Only the fourth node is read, so what the activation finds is what the transfer carried: nothing to go on
        // from.
        let mut after = cluster.owner_over(3, &[3], 1, Write::One);
        let sends = after.activate(None, 1).expect("nothing to fit");
        assert_eq!(cluster.run(&mut after, sends), Some(Outcome::Missing));
        let sends = after
            .activate(Some(pages(&[("balance", b"0")])), 1)
            .expect("it fits");
        assert_eq!(
            cluster.run(&mut after, sends),
            Some(Outcome::Activated {
                state: pages(&[("balance", b"0")]),
                created: true
            })
        );
    }

    fn balance(value: &[u8]) -> Pages {
        pages(&[("balance", value)])
    }

    /// A durable owner on the first node that took the key from `initial` and wrote `state`, `store` keeping what it
    /// saved.
    fn durably_written(
        cluster: &mut Cluster,
        store: &mut Kept,
        initial: &[u8],
        state: &[u8],
    ) -> Owner {
        let mut owner = cluster.durable(0, 0);
        let sends = owner.activate(Some(balance(initial)), 3).expect("it fits");
        cluster.kept(&mut owner, sends, store);
        let sends = owner.save(balance(state)).expect("it fits");
        assert_eq!(cluster.kept(&mut owner, sends, store), Some(Outcome::Saved));
        owner
    }

    /// Every replica is gone, as after a restart of the whole cluster, and the store holds what a cluster before this
    /// one saved, in a round far above the one the owner starts from.
    #[test]
    fn a_durable_key_no_replica_holds_comes_up_from_the_store_and_not_from_initial() {
        let mut cluster = Cluster::new(3, 61);
        let before = Rolls::seeded(62).nodes(1).remove(0);
        let mut store = Kept {
            record: Some(Stored {
                stamp: Stamp {
                    epoch: Epoch {
                        round: 57,
                        node: before,
                    },
                    version: 4,
                },
                pages: Some(balance(b"9")),
            }),
            ..Kept::default()
        };
        let mut owner = cluster.durable(0, 0);
        let sends = owner.activate(Some(balance(b"0")), 3).expect("it fits");

        let outcome = cluster.kept(&mut owner, sends, &mut store);

        assert_eq!(
            outcome,
            Some(Outcome::Activated {
                state: balance(b"9"),
                created: false
            })
        );
        assert!(
            store.saves.is_empty(),
            "the store was asked to keep what it had"
        );
        // It promised above the round of the record, so what it writes from here on is what the store keeps.
        assert!(owner.epoch().round > 57);
        let sends = owner.save(balance(b"10")).expect("it fits");
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Saved)
        );
        assert_eq!(store.state(), Some(balance(b"10")));
    }

    #[test]
    fn a_key_nothing_holds_asks_the_store_and_starts_from_initial_when_it_keeps_nothing_either() {
        let mut cluster = Cluster::new(3, 69);
        let mut store = Kept::default();
        let mut owner = cluster.durable(0, 0);

        let sends = owner.activate(None, 3).expect("nothing to fit");
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Missing)
        );
        let sends = owner.activate(Some(balance(b"0")), 3).expect("it fits");
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Activated {
                state: balance(b"0"),
                created: true
            })
        );
        assert!(
            store.saves.is_empty(),
            "a key at its initial state was saved"
        );
    }

    /// Two of the three replicas confirmed the last write and lost it; the third holds the write before it. The store
    /// has the last one, and the activation goes on from it.
    #[test]
    fn a_write_the_replicas_lost_while_an_older_copy_survived_comes_back_from_the_store() {
        let mut cluster = Cluster::new(3, 70);
        let mut store = Kept::default();
        let mut first = durably_written(&mut cluster, &mut store, b"0", b"1");
        let behind = cluster.ids[2].clone();
        let reaching: Vec<Send<Request>> = first
            .save(balance(b"2"))
            .expect("it fits")
            .into_iter()
            .filter(|send| send.to != behind)
            .collect();
        assert_eq!(
            cluster.kept(&mut first, reaching, &mut store),
            Some(Outcome::Saved)
        );
        cluster.replicas[0].drop("account", "a");
        cluster.replicas[1].drop("account", "a");

        let mut second = cluster.durable(2, 1);
        let sends = second.activate(None, 3).expect("nothing to fit");

        assert_eq!(
            cluster.kept(&mut second, sends, &mut store),
            Some(Outcome::Activated {
                state: balance(b"2"),
                created: false
            })
        );
    }

    /// The owner confirmed a write and never saved it. The next activation goes on from the replicas, and the store,
    /// behind them, takes what it goes on from.
    #[test]
    fn the_store_catches_up_with_a_write_the_replicas_hold_and_it_does_not() {
        let mut cluster = Cluster::new(3, 63);
        let mut store = Kept::default();
        durably_written(&mut cluster, &mut Kept::default(), b"1", b"5");

        let mut owner = cluster.durable(1, 1);
        let sends = owner.activate(None, 3).expect("nothing to fit");
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Activated {
                state: balance(b"5"),
                created: false
            })
        );
        assert_eq!(store.state(), Some(balance(b"5")));
        assert_eq!(store.saves.len(), 1);

        // Read again, the store has what the replicas have, and nothing is saved again.
        let mut next = cluster.durable(2, 2);
        let sends = next.activate(None, 3).expect("nothing to fit");
        assert!(matches!(
            cluster.kept(&mut next, sends, &mut store),
            Some(Outcome::Activated { .. })
        ));
        assert_eq!(store.saves.len(), 1, "a store up to date was asked again");
    }

    /// The criterion of 14.2: a write the replicas refused never reaches the store.
    #[test]
    fn a_fenced_owner_s_write_never_reaches_the_store() {
        let mut cluster = Cluster::new(3, 64);
        let mut store = Kept::default();
        let mut first = durably_written(&mut cluster, &mut store, b"1", b"2");
        assert_eq!(store.state(), Some(balance(b"2")));

        // A later owner promises a higher round on every replica, and goes on from what the first one saved.
        let mut second = cluster.durable(1, 5);
        let sends = second.activate(None, 3).expect("nothing to fit");
        assert!(matches!(
            cluster.kept(&mut second, sends, &mut store),
            Some(Outcome::Activated { .. })
        ));
        let saved = store.saves.len();

        let sends = first.save(balance(b"9")).expect("it fits");

        assert_eq!(
            cluster.kept(&mut first, sends, &mut store),
            Some(Outcome::Fenced)
        );
        assert_eq!(
            store.saves.len(),
            saved,
            "a write the replicas refused was saved"
        );
        assert_eq!(store.state(), Some(balance(b"2")));
    }

    /// The criterion of 14.2: the ring moved the key to other nodes, the owner there wrote it, and nothing the owner
    /// before saved is read back afterwards, not even a save of it that reaches the store last.
    #[test]
    fn a_key_handed_to_other_nodes_is_read_back_from_their_write_and_not_from_an_older_save() {
        let mut cluster = Cluster::new(4, 65);
        let mut store = Kept::default();
        let mut before = cluster.durable_over(0, &[0, 1, 2], 0);
        let sends = before.activate(Some(balance(b"1")), 3).expect("it fits");
        cluster.kept(&mut before, sends, &mut store);
        // The save of this write is slow: it is held back, to reach the store after everything else.
        let mut slow = Kept::default();
        let sends = before.save(balance(b"2")).expect("it fits");
        assert_eq!(
            cluster.kept(&mut before, sends, &mut slow),
            Some(Outcome::Saved)
        );
        let late = slow
            .saves
            .pop()
            .expect("the confirmed write was to be saved");

        // The ring puts the key on the last three nodes; the fourth takes it the way a handoff carries it, and the owner
        // there writes.
        for copy in cluster.replicas[1].copies("account", "a") {
            cluster.replicas[3].install("account", copy);
        }
        let mut after = cluster.durable_over(1, &[1, 2, 3], 1);
        let sends = after.activate(None, 3).expect("nothing to fit");
        assert!(matches!(
            cluster.kept(&mut after, sends, &mut store),
            Some(Outcome::Activated { .. })
        ));
        let sends = after.save(balance(b"3")).expect("it fits");
        assert_eq!(
            cluster.kept(&mut after, sends, &mut store),
            Some(Outcome::Saved)
        );

        // The save of the owner before arrives last, and the store keeps the later write.
        let kept = store.record.clone().expect("the store keeps the key");
        assert!(late.version() < kept.version());
        store.keep(late);
        assert_eq!(store.state(), Some(balance(b"3")));

        // Every node is gone, and the key comes back from the store as the nodes it moved to left it.
        let mut cluster = Cluster::new(4, 66);
        let mut restarted = cluster.durable(0, 0);
        let sends = restarted.activate(Some(balance(b"0")), 3).expect("it fits");
        assert_eq!(
            cluster.kept(&mut restarted, sends, &mut store),
            Some(Outcome::Activated {
                state: balance(b"3"),
                created: false
            })
        );
    }

    #[test]
    fn a_deletion_is_saved_as_a_record_without_a_state_and_the_key_starts_over_after_it() {
        let mut cluster = Cluster::new(3, 67);
        let mut store = Kept::default();
        let mut owner = durably_written(&mut cluster, &mut store, b"1", b"7");

        let sends = owner.delete();
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Saved)
        );
        let deletion = store.record.clone().expect("the deletion was saved");
        assert_eq!(deletion.pages, None);

        // Every replica is lost: the deletion is what the store gives back, and the key starts over.
        let mut cluster = Cluster::new(3, 68);
        let mut owner = cluster.durable(0, 0);
        let sends = owner.activate(None, 3).expect("nothing to fit");
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Missing)
        );
        let sends = owner.activate(Some(balance(b"0")), 3).expect("it fits");
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Activated {
                state: balance(b"0"),
                created: true
            })
        );
        let sends = owner.save(balance(b"4")).expect("it fits");
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut store),
            Some(Outcome::Saved)
        );
        assert!(deletion.version() < store.record.as_ref().expect("a record").version());
        assert_eq!(store.state(), Some(balance(b"4")));
    }

    /// The deletion never reached the store, which still keeps the state before it: the tombstone on the replicas is
    /// the later write, and the key starts over, the store taking the new start in place of the older state.
    #[test]
    fn a_tombstone_on_the_replicas_wins_over_an_older_state_the_store_keeps() {
        let mut cluster = Cluster::new(3, 71);
        let mut store = Kept::default();
        let mut owner = durably_written(&mut cluster, &mut store, b"1", b"7");
        let sends = owner.delete();
        assert_eq!(
            cluster.kept(&mut owner, sends, &mut Kept::default()),
            Some(Outcome::Saved)
        );
        assert_eq!(store.state(), Some(balance(b"7")));

        let mut next = cluster.durable(1, 1);
        let sends = next.activate(Some(balance(b"0")), 3).expect("it fits");

        assert_eq!(
            cluster.kept(&mut next, sends, &mut store),
            Some(Outcome::Activated {
                state: balance(b"0"),
                created: true
            })
        );
        assert_eq!(store.state(), Some(balance(b"0")));
    }
}
