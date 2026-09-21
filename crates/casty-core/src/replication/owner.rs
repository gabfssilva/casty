//! The activation and the saves of a key, by the node that owns it.

use std::collections::{BTreeSet, HashMap};

use super::messages::{ACTIVE, Epoch, Reply, Request, Stamp, Write};
use super::replica::{chunks, cost};
use crate::node::NodeId;
use crate::store::Pages;

/// A request the caller must send to `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Send {
    pub to: NodeId,
    pub message: Request,
}

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

/// What a step of the protocol produces: what to send, and the outcome once it is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub sends: Vec<Send>,
    pub outcome: Option<Outcome>,
}

impl Step {
    fn waiting() -> Self {
        Self {
            sends: Vec::new(),
            outcome: None,
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

/// A page that does not fit in one message, which no write can carry.
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
    Preparing {
        initial: Option<Pages>,
        wanted: usize,
        promises: HashMap<NodeId, Promised>,
    },
    Fetching {
        stamp: Stamp,
        missing: BTreeSet<String>,
        state: Pages,
    },
    Writing {
        write: Written,
        created: Option<bool>,
        acks: BTreeSet<NodeId>,
        answers: BTreeSet<NodeId>,
    },
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
///
/// `activate` and `save` start an operation, and `receive` gives its outcome once it is known. Starting an operation
/// abandons the one in flight.
#[derive(Debug)]
pub struct Owner {
    actor: String,
    key: String,
    replicas: Vec<NodeId>,
    limit: usize,
    quorums: Quorums,
    epoch: Epoch,
    round: u64,
    version: u64,
    phase: Option<Phase>,
    confirmed: Option<Written>,
    latest: Option<Written>,
}

impl Owner {
    #[must_use]
    pub fn new(
        actor: &str,
        key: &str,
        node: NodeId,
        replicas: Vec<NodeId>,
        write: Write,
        limit: usize,
        round: u64,
    ) -> Self {
        let quorums = quorums(write, replicas.len());
        Self {
            actor: actor.to_owned(),
            key: key.to_owned(),
            replicas,
            limit,
            quorums,
            epoch: Epoch { round, node },
            round,
            version: 0,
            phase: None,
            confirmed: None,
            latest: None,
        }
    }

    /// Epoch of the latest activation.
    #[must_use]
    pub fn epoch(&self) -> &Epoch {
        &self.epoch
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
    pub fn activate(
        &mut self,
        initial: Option<Pages>,
        wanted: usize,
    ) -> Result<Vec<Send>, TooLarge> {
        self.check(initial.as_ref().unwrap_or(&Pages::new()))?;
        self.round += 1;
        self.epoch = Epoch {
            round: self.round,
            node: self.epoch.node.clone(),
        };
        self.phase = Some(Phase::Preparing {
            initial,
            wanted,
            promises: HashMap::new(),
        });
        let prepare = Request::Prepare {
            actor: self.actor.clone(),
            key: self.key.clone(),
            epoch: self.epoch.clone(),
        };
        Ok(self
            .replicas
            .iter()
            .map(|replica| Send {
                to: replica.clone(),
                message: prepare.clone(),
            })
            .collect())
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

    /// Write `state` as a delta against the last confirmed write.
    pub fn save(&mut self, state: Pages) -> Result<Vec<Send>, TooLarge> {
        let mut marked = state;
        marked.insert(ACTIVE.to_owned(), Vec::new());
        self.next(marked)
    }

    /// Write `state` without the active mark, which is how a key that stopped being active lets go.
    ///
    /// Every other write carries the mark, so dropping it is an operation of its own and not a plain save.
    pub fn release(&mut self, state: Pages) -> Result<Vec<Send>, TooLarge> {
        self.next(state)
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
                pages,
                ..
            } => self.fetched(&epoch, accepted.as_ref(), pages),
            Reply::Accepted {
                stamp,
                replica,
                receiving,
                ..
            } => self.accepted(&stamp, replica, receiving),
            Reply::NeedFull { stamp, replica, .. } => self.resend(&stamp, &replica),
        }
    }

    fn next(&mut self, state: Pages) -> Result<Vec<Send>, TooLarge> {
        let confirmed = self
            .confirmed
            .clone()
            .expect("a write before the activation");
        self.check(&state)?;
        self.version += 1;
        Ok(self.write(
            state,
            confirmed.stamp.clone().into(),
            &confirmed.state,
            None,
        ))
    }

    fn rejected(&mut self, promised: &Epoch) -> Step {
        self.round = self.round.max(promised.round);
        if self.phase.is_none() || !self.epoch.before(promised) {
            return Step::waiting();
        }
        self.end(Outcome::Fenced)
    }

    fn promised(&mut self, epoch: &Epoch, promise: Promised) -> Step {
        let wanted = match &mut self.phase {
            Some(Phase::Preparing {
                wanted, promises, ..
            }) if *epoch == self.epoch => {
                promises.insert(promise.replica.clone(), promise);
                (*wanted).min(self.replicas.len())
            }
            _ => return Step::waiting(),
        };
        let Some(Phase::Preparing { promises, .. }) = &self.phase else {
            unreachable!("the phase was just matched");
        };
        if promises.len() < wanted {
            return Step::waiting();
        }
        self.decide(false)
    }

    fn decide(&mut self, final_answer: bool) -> Step {
        let Some(Phase::Preparing {
            initial, promises, ..
        }) = &self.phase
        else {
            return Step::waiting();
        };
        let counted: Vec<Promised> = promises
            .values()
            .filter(|promise| !promise.receiving)
            .cloned()
            .collect();
        if counted.len() < self.quorums.reads {
            // A replica that has not got the range of the key yet answered, and its answer is not countable and not
            // a refusal either: the range is on its way. Every replica having answered is the last word only when
            // none of them is in that state, so here the owner waits and asks again.
            let receiving = counted.len() != promises.len();
            if final_answer || (promises.len() == self.replicas.len() && !receiving) {
                return self.end(Outcome::Insufficient);
            }
            return Step::waiting();
        }
        let latest = counted
            .iter()
            .filter_map(|promise| promise.accepted.clone())
            .reduce(|held, other| if held.before(&other) { other } else { held });
        let Some(latest) = latest else {
            let Some(initial) = initial.clone() else {
                return self.end(Outcome::Missing);
            };
            return self.commit(initial, None, Some(true));
        };
        let sources: Vec<Promised> = counted
            .into_iter()
            .filter(|promise| promise.accepted.as_ref() == Some(&latest))
            .collect();
        for source in &sources {
            let named: BTreeSet<&String> = source.pages.keys().collect();
            let indexed: BTreeSet<&String> = source.sizes.iter().map(|(name, _)| name).collect();
            if named == indexed {
                return self.commit(source.pages.clone(), Some(latest), Some(false));
            }
        }
        let source = sources
            .into_iter()
            .next()
            .expect("a source of the latest write");
        let fetches = chunks(&source.sizes, self.limit)
            .into_iter()
            .map(|names| Send {
                to: source.replica.clone(),
                message: Request::FetchPages {
                    actor: self.actor.clone(),
                    key: self.key.clone(),
                    epoch: self.epoch.clone(),
                    names,
                },
            })
            .collect();
        self.phase = Some(Phase::Fetching {
            stamp: latest,
            missing: source.sizes.iter().map(|(name, _)| name.clone()).collect(),
            state: Pages::new(),
        });
        Step {
            sends: fetches,
            outcome: None,
        }
    }

    fn fetched(&mut self, epoch: &Epoch, accepted: Option<&Stamp>, pages: Pages) -> Step {
        let Some(Phase::Fetching {
            stamp,
            missing,
            state,
        }) = &mut self.phase
        else {
            return Step::waiting();
        };
        if *epoch != self.epoch {
            return Step::waiting();
        }
        if accepted != Some(&*stamp) {
            return self.end(Outcome::Fenced);
        }
        for (name, data) in pages {
            missing.remove(&name);
            state.insert(name, data);
        }
        if !missing.is_empty() {
            return Step::waiting();
        }
        let (stamp, state) = (stamp.clone(), state.clone());
        self.commit(state, Some(stamp), Some(false))
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
            }) if write.stamp == *stamp => {
                answers.insert(replica.clone());
                if !receiving {
                    acks.insert(replica);
                }
                if acks.len() >= writes {
                    Some(Some((write.clone(), *created)))
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
            Some(Some((write, created))) => {
                self.confirmed = Some(write.clone());
                match created {
                    None => self.end(Outcome::Saved),
                    Some(created) => {
                        let mut state = write.state;
                        state.remove(ACTIVE);
                        self.end(Outcome::Activated { state, created })
                    }
                }
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
        Step {
            sends: self
                .parts(&latest, true)
                .into_iter()
                .map(|message| Send {
                    to: replica.clone(),
                    message,
                })
                .collect(),
            outcome: None,
        }
    }

    fn commit(&mut self, state: Pages, base: Option<Stamp>, created: Option<bool>) -> Step {
        self.version = base.as_ref().map_or(0, |held| held.version) + 1;
        let before = if base.is_some() {
            state.clone()
        } else {
            Pages::new()
        };
        let mut marked = state;
        marked.insert(ACTIVE.to_owned(), Vec::new());
        Step {
            sends: self.write(marked, base, &before, created),
            outcome: None,
        }
    }

    fn write(
        &mut self,
        state: Pages,
        base: Option<Stamp>,
        before: &Pages,
        created: Option<bool>,
    ) -> Vec<Send> {
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
        let sizes: Vec<(String, usize)> = pages
            .iter()
            .map(|(name, data)| (name.clone(), data.len()))
            .collect();
        let grouped = chunks(&sizes, self.limit);
        let last = grouped.len() - 1;
        grouped
            .into_iter()
            .enumerate()
            .map(|(part, names)| Request::Accept {
                actor: self.actor.clone(),
                key: self.key.clone(),
                stamp: write.stamp.clone(),
                base: if full { None } else { write.base.clone() },
                #[allow(clippy::cast_possible_truncation)]
                part: part as u32,
                final_part: part == last,
                pages: names
                    .into_iter()
                    .filter_map(|name| pages.get(&name).map(|data| (name, data.clone())))
                    .collect(),
                dropped: if part == last && !full {
                    write.dropped.clone()
                } else {
                    Vec::new()
                },
            })
            .collect()
    }

    fn check(&self, state: &Pages) -> Result<(), TooLarge> {
        for (name, data) in state {
            let held = cost(name, data.len());
            if held > self.limit {
                return Err(TooLarge(format!(
                    "{}/{}: page {name:?} takes {held} bytes, over the {} that fit in a message",
                    self.actor, self.key, self.limit
                )));
            }
        }
        Ok(())
    }

    fn end(&mut self, outcome: Outcome) -> Step {
        self.phase = None;
        Step {
            sends: Vec::new(),
            outcome: Some(outcome),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::messages::{ACTIVE, Write};
    use super::super::replica::Replica;
    use super::{Outcome, Owner, Send, Step};
    use crate::node::NodeId;
    use crate::rolls::Rolls;
    use crate::store::Pages;

    const LIMIT: usize = 64;

    fn pages(entries: &[(&str, &[u8])]) -> Pages {
        entries
            .iter()
            .map(|(name, data)| ((*name).to_owned(), (*data).to_vec()))
            .collect()
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
            Owner::new(
                "account",
                "a",
                self.ids[at].clone(),
                self.ids.clone(),
                write,
                LIMIT,
                round,
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
        /// replicas outside it still take the write.
        fn run(&mut self, owner: &mut Owner, sends: Vec<Send>) -> Option<Outcome> {
            let mut pending = sends;
            let mut reached = None;
            while !pending.is_empty() {
                let mut next = Vec::new();
                for send in pending {
                    let at = self.at(&send.to);
                    let Some(reply) = self.replicas[at].receive(send.message, false) else {
                        continue;
                    };
                    let Step { sends, outcome } = owner.receive(reply);
                    reached = outcome.or(reached);
                    next.extend(sends);
                }
                pending = next;
            }
            reached
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
    fn a_page_that_does_not_fit_in_a_message_is_refused_before_anything_is_written() {
        let cluster = Cluster::new(3, 36);
        let mut owner = cluster.owner(0, 0, Write::Majority);

        let refused = owner.activate(Some(pages(&[("balance", &[0; 1_000])])), 3);

        let Err(super::TooLarge(why)) = refused else {
            panic!("a page over the limit was taken");
        };
        assert!(why.contains("over the 64"), "{why}");
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
            let mut pending: Vec<(bool, Send)> = taking
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
                let Step { sends, outcome } = owner.receive(reply);
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
}
