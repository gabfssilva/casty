//! Filling the token ranges a ring change gave this node, and answering the pulls of the nodes that gained ranges
//! here.
//!
//! It is not a component of its own: the pulls travel in the band of `replication`, which hands them over.
//!
//! A range is asked of every node of the previous ring that is still in the cluster, and not only of the replicas of
//! the range: a range spans several vnodes of that ring, so its keys do not all have the same replicas there. Asking
//! a node that keeps nothing there costs one empty answer; what has to be exact is the threshold, and that one is
//! counted per key, against the replicas that key had in the previous ring.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use casty_core::handoff::messages::Pull;
use casty_core::handoff::ranges::{Transfer, overlap};
use casty_core::handoff::sweep::Handover;
use casty_core::mailbox::Backoff;
use casty_core::node::NodeId;
use casty_core::placement::token;
use casty_core::replication::messages::{Copy, Write};
use casty_core::replication::owner::quorums;
use casty_core::replication::replica::{Replica, cost};
use casty_core::store::Pages;
use tokio::time::Instant;

use crate::placement::{Step, Transfers};
use crate::replication::service::{Entity, Outgoing};
use crate::replication::wire::Message;

/// A type this process runs, as the handoff needs it: how many replicas its keys have and who confirms a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Kind {
    pub replicas: usize,
    pub write: Write,
}

/// A range this node is filling, and when it asks for it again.
#[derive(Debug)]
struct Hold {
    step: Step,
    transfer: Transfer,
    at: Instant,
    delay: Duration,
}

/// A key this node stopped replicating: the nodes that replicate it now, and the ones that already took it.
#[derive(Debug)]
struct Handing {
    replicas: BTreeSet<NodeId>,
    took: BTreeSet<NodeId>,
}

impl Handing {
    fn owes(&self, node: &NodeId) -> bool {
        self.replicas.contains(node) && !self.took.contains(node)
    }

    /// Whether a node the cluster still has has yet to take this key.
    ///
    /// A replica that went away owes nothing. Nothing is sent to it any more, so counting it is waiting out the
    /// handover budget for an answer that cannot come; the key it did not take belongs to another node now, and
    /// the ring its going changed is what hands it there.
    fn owing(&self, present: &BTreeSet<NodeId>) -> bool {
        self.replicas
            .iter()
            .any(|node| !self.took.contains(node) && present.contains(node))
    }
}

/// A handover being sent, and when the keys the other node has not taken go out again.
#[derive(Debug)]
struct Giving {
    at: Instant,
    delay: Duration,
}

/// A handover being taken in: the part of a key waiting for the rest of it, and the keys already installed.
#[derive(Debug, Default)]
struct Taking {
    part: Option<Copy>,
    keys: Vec<String>,
}

#[derive(Debug)]
pub struct Handoff {
    node: NodeId,
    holds: Vec<Hold>,
    /// The steps whose ranges have all arrived, which the placement takes on its next pass.
    settled: Vec<Step>,
    /// A key split over parts of an answer, by the transfer and the source it came from.
    parts: HashMap<(u64, NodeId), Copy>,
    handing: HashMap<Entity, Handing>,
    giving: HashMap<(String, NodeId), Giving>,
    taking: HashMap<(String, NodeId), Taking>,
    kinds: HashMap<String, Kind>,
    present: BTreeSet<NodeId>,
    sends: Vec<Outgoing>,
    /// Ranges that arrived since the last pass, which is when what this node keeps is decided again.
    swept: bool,
    ids: u64,
    limit: usize,
    backoff: Backoff,
}

impl Handoff {
    #[must_use]
    pub fn new(node: NodeId, limit: usize, backoff: Backoff) -> Self {
        Self {
            node,
            holds: Vec::new(),
            settled: Vec::new(),
            parts: HashMap::new(),
            handing: HashMap::new(),
            giving: HashMap::new(),
            taking: HashMap::new(),
            kinds: HashMap::new(),
            present: BTreeSet::new(),
            sends: Vec::new(),
            swept: false,
            ids: 0,
            limit,
            backoff,
        }
    }

    /// The types this node has and the members a transfer can reach, which every decision here reads.
    ///
    /// A node the cluster gave up on is not one of them, even while the ring still places keys on it: nothing it is
    /// sent is answered, so asking it for a range or waiting for it to take one is waiting for the deadline.
    pub fn knows(&mut self, kinds: HashMap<String, Kind>, present: BTreeSet<NodeId>) {
        self.kinds = kinds;
        self.present = present;
    }

    /// What is waiting to go out on the band of `replication`.
    pub fn take(&mut self) -> Vec<Outgoing> {
        core::mem::take(&mut self.sends)
    }

    /// Whether what this node keeps is to be decided again, which a range arriving asks for.
    pub fn resweep(&mut self) -> bool {
        core::mem::take(&mut self.swept)
    }

    /// Whether anything is still owed, which is what a node leaving the cluster waits for.
    #[must_use]
    pub fn owing(&self) -> bool {
        self.handing
            .values()
            .any(|handing| handing.owing(&self.present))
    }

    /// The keys this node is going without, which stay where they were.
    #[must_use]
    pub fn owed(&self) -> Vec<Entity> {
        let mut owed: Vec<Entity> = self
            .handing
            .iter()
            .filter(|(_, handing)| handing.owing(&self.present))
            .map(|(entity, _)| entity.clone())
            .collect();
        owed.sort();
        owed
    }

    /// Whether `key` is in a range still arriving, whichever step of the ring gave it to this node.
    ///
    /// This is what keeps a replica out of the quorums until it has the range: it answers, and the owner does not
    /// count the answer.
    #[must_use]
    pub fn arriving(&self, actor: &str, key: &str) -> bool {
        let point = token(actor, key);
        self.holds
            .iter()
            .any(|held| held.transfer.actor == actor && held.transfer.receiving(point))
    }

    /// What arrived on the band, whichever side of a transfer it belongs to.
    pub fn receive(&mut self, pull: Pull, replica: &mut Replica) {
        match pull {
            Pull::PullRange {
                actor,
                node,
                transfer,
                ranges,
            } => {
                let receiving = self.holds.iter().any(|held| {
                    held.transfer.actor == actor && overlap(&ranges, &held.transfer.ranges)
                });
                let keys: Vec<String> = replica
                    .keys(&actor)
                    .into_iter()
                    .filter(|key| {
                        let point = token(&actor, key);
                        ranges.iter().any(|span| span.holds(point))
                    })
                    .collect();
                let copies: Vec<Copy> = keys
                    .iter()
                    .flat_map(|key| replica.copies(&actor, key))
                    .collect();
                let parts = packed(copies, self.limit);
                let last = parts.len() - 1;
                for (part, copies) in parts.into_iter().enumerate() {
                    self.send(
                        node.clone(),
                        Pull::RangeKeys {
                            actor: actor.clone(),
                            replica: self.node.clone(),
                            transfer,
                            part: u32::try_from(part).unwrap_or(u32::MAX),
                            final_part: part == last,
                            receiving,
                            keys: copies,
                        },
                    );
                }
            }
            Pull::RangeKeys { .. } => self.install(pull, replica),
            Pull::HandKeys { .. } => self.take_over(pull, replica),
            Pull::TookKeys {
                actor,
                replica: from,
                keys,
            } => self.took(&actor, &from, &keys, replica),
        }
    }

    /// Take in the keys of one part of an answer: a key split over parts is put together before it reaches the replica.
    fn install(&mut self, answer: Pull, replica: &mut Replica) {
        let Pull::RangeKeys {
            actor,
            replica: from,
            transfer,
            final_part,
            receiving,
            keys,
            ..
        } = answer
        else {
            return;
        };
        let Some(index) = self
            .holds
            .iter()
            .position(|held| held.transfer.id == transfer && held.transfer.sources.contains(&from))
        else {
            return;
        };
        let at = (transfer, from.clone());
        absorb(&actor, keys, &mut self.parts, &at, replica);
        if !final_part {
            return;
        }
        self.parts.remove(&at);
        let held = &mut self.holds[index];
        held.transfer.answered.insert(from.clone());
        if !receiving {
            held.transfer.settled.insert(from);
        }
        if held.transfer.pending().is_empty() {
            let step = self.holds.remove(index).step;
            self.settled.push(step);
            self.swept = true;
        } else if !receiving {
            // The threshold is per key, so this answer may have let keys through ahead of the step.
            self.swept = true;
        }
    }

    /// Take in a handover, answering for the whole of it once the sender says it has nothing more.
    fn take_over(&mut self, hand: Pull, replica: &mut Replica) {
        let Pull::HandKeys {
            actor,
            node,
            final_part,
            keys,
        } = hand
        else {
            return;
        };
        let at = (actor.clone(), node.clone());
        let mut taking = self.taking.remove(&at).unwrap_or_default();
        let mut part = taking.part.take();
        taking
            .keys
            .extend(installed(&actor, keys, &mut part, replica));
        taking.part = part;
        if !final_part {
            self.taking.insert(at, taking);
            return;
        }
        self.send(
            node,
            Pull::TookKeys {
                actor,
                replica: self.node.clone(),
                keys: taking.keys,
            },
        );
    }

    /// Drop the copy of every key that every node replicating it now has taken.
    fn took(&mut self, actor: &str, from: &NodeId, keys: &[String], replica: &mut Replica) {
        for key in keys {
            let entity = (actor.to_owned(), key.clone());
            let Some(handing) = self.handing.get_mut(&entity) else {
                continue;
            };
            handing.took.insert(from.clone());
            if !handing.owing(&self.present) {
                self.handing.remove(&entity);
                replica.drop(actor, key);
            }
        }
    }

    /// Start handing away the keys this node stopped replicating, one sender per node that is to take them.
    pub fn give(&mut self, given: Vec<Handover>, now: Instant) {
        let wanted: HashMap<Entity, BTreeSet<NodeId>> = given
            .into_iter()
            .map(|handover| {
                (
                    (handover.actor, handover.key),
                    handover.replicas.into_iter().collect(),
                )
            })
            .collect();
        self.handing.retain(|entity, _| wanted.contains_key(entity));
        for (entity, replicas) in wanted {
            let held = self.handing.get(&entity);
            if held.is_none_or(|handing| handing.replicas != replicas) {
                for replica in &replicas {
                    self.giving
                        .entry((entity.0.clone(), replica.clone()))
                        .or_insert(Giving {
                            at: now,
                            delay: self.backoff.first,
                        });
                }
                self.handing.insert(
                    entity,
                    Handing {
                        replicas,
                        took: BTreeSet::new(),
                    },
                );
            }
        }
    }

    /// The earliest moment anything here has to be sent again.
    #[must_use]
    pub fn due(&self) -> Option<Instant> {
        let pulls = self.holds.iter().map(|held| held.at);
        let handovers = self.giving.values().map(|giving| giving.at);
        pulls.chain(handovers).min()
    }

    /// Ask again for what is owed: the ranges still arriving, and the keys the nodes that took over do not have yet.
    ///
    /// A source that answered while it was itself receiving is asked again with the others, which is how it comes to
    /// count toward the threshold once it has settled.
    pub fn fired(&mut self, now: Instant, replica: &Replica) {
        let mut asking = Vec::new();
        let mut arrived = Vec::new();
        for (index, held) in self.holds.iter_mut().enumerate() {
            if held.at > now {
                continue;
            }
            held.transfer
                .sources
                .retain(|node| self.present.contains(node));
            held.transfer
                .answered
                .retain(|node| held.transfer.sources.contains(node));
            held.transfer
                .settled
                .retain(|node| held.transfer.sources.contains(node));
            if held.transfer.pending().is_empty() {
                arrived.push(index);
                continue;
            }
            held.at = now + held.delay;
            held.delay = self.backoff.next(held.delay);
            for node in held.transfer.unsettled() {
                asking.push((
                    node,
                    Pull::PullRange {
                        actor: held.transfer.actor.clone(),
                        node: self.node.clone(),
                        transfer: held.transfer.id,
                        ranges: held.transfer.ranges.clone(),
                    },
                ));
            }
        }
        for index in arrived.into_iter().rev() {
            self.settled.push(self.holds.remove(index).step);
            self.swept = true;
        }
        for (node, pull) in asking {
            self.send(node, pull);
        }
        self.handovers(now, replica);
    }

    /// Send the keys each node is to take, again until it says it has them all.
    fn handovers(&mut self, now: Instant, replica: &Replica) {
        let mut sending = Vec::new();
        self.giving.retain(|(actor, to), giving| {
            let owed: Vec<String> = self
                .handing
                .iter()
                .filter(|((held, _), handing)| held == actor && handing.owes(to))
                .map(|((_, key), _)| key.clone())
                .collect();
            if owed.is_empty() || !self.present.contains(to) {
                return false;
            }
            if giving.at > now {
                return true;
            }
            giving.at = now + giving.delay;
            giving.delay = self.backoff.next(giving.delay);
            sending.push((actor.clone(), to.clone(), owed));
            true
        });
        for (actor, to, owed) in sending {
            let copies: Vec<Copy> = owed
                .iter()
                .flat_map(|key| replica.copies(&actor, key))
                .collect();
            let parts = packed(copies, self.limit);
            let last = parts.len() - 1;
            for (part, copies) in parts.into_iter().enumerate() {
                self.send(
                    to.clone(),
                    Pull::HandKeys {
                        actor: actor.clone(),
                        node: self.node.clone(),
                        final_part: part == last,
                        keys: copies,
                    },
                );
            }
        }
    }

    fn send(&mut self, to: NodeId, message: Pull) {
        self.sends.push(Outgoing {
            to,
            message: Message::Pull(message),
        });
    }
}

impl Transfers for Handoff {
    fn take(&mut self, step: &Step) {
        let kind = self.kinds.get(&step.actor).copied().unwrap_or(Kind {
            replicas: step.previous.nodes().len(),
            write: Write::Majority,
        });
        let count = kind.replicas.min(step.previous.nodes().len());
        let reads = quorums(kind.write, count).reads;
        let sources: BTreeSet<NodeId> = step
            .previous
            .nodes()
            .iter()
            .filter(|node| **node != self.node && self.present.contains(*node))
            .cloned()
            .collect();
        self.ids += 1;
        let transfer = Transfer {
            id: self.ids,
            actor: step.actor.clone(),
            previous: step.previous.clone(),
            count,
            wanted: reads.max((count + 1).saturating_sub(reads)),
            ranges: step.ranges.clone(),
            sources,
            answered: BTreeSet::new(),
            settled: BTreeSet::new(),
        };
        // Nothing to pull: no node of the previous ring is still here, so the step is through as soon as it is taken.
        if transfer.pending().is_empty() {
            self.settled.push(step.clone());
            self.swept = true;
            return;
        }
        self.holds.push(Hold {
            step: step.clone(),
            transfer,
            at: Instant::now(),
            delay: self.backoff.first,
        });
    }

    fn receiving(&self, step: &Step, key: &str) -> bool {
        self.holds
            .iter()
            .find(|held| held.step == *step)
            .is_some_and(|held| held.transfer.receiving(token(&step.actor, key)))
    }

    fn filled(&mut self) -> Vec<Step> {
        core::mem::take(&mut self.settled)
    }
}

/// Install the keys of one part, putting a key split over parts together first, and say which ones landed.
fn absorb(
    actor: &str,
    copies: Vec<Copy>,
    parts: &mut HashMap<(u64, NodeId), Copy>,
    at: &(u64, NodeId),
    replica: &mut Replica,
) -> Vec<String> {
    let mut part = parts.remove(at);
    let landed = installed(actor, copies, &mut part, replica);
    if let Some(part) = part {
        parts.insert(at.clone(), part);
    }
    landed
}

/// Install what is whole, keeping the key that is still missing pages in `part`.
fn installed(
    actor: &str,
    copies: Vec<Copy>,
    part: &mut Option<Copy>,
    replica: &mut Replica,
) -> Vec<String> {
    let mut landed = Vec::new();
    for copy in copies {
        let whole = match part.take() {
            Some(held) if held.key == copy.key => Copy {
                pages: joined(held.pages, copy.pages),
                ..copy
            },
            _ => copy,
        };
        if whole.final_part {
            landed.push(whole.key.clone());
            replica.install(actor, whole);
        } else {
            *part = Some(whole);
        }
    }
    landed
}

fn joined(held: Pages, arrived: Pages) -> Pages {
    let mut whole = held;
    whole.extend(arrived);
    whole
}

/// The copies grouped in order so that each group fits in a message.
fn packed(copies: Vec<Copy>, limit: usize) -> Vec<Vec<Copy>> {
    let mut parts: Vec<Vec<Copy>> = vec![Vec::new()];
    let mut size = 0;
    for copy in copies {
        let pages: usize = copy
            .pages
            .iter()
            .map(|(name, data)| cost(name, data.len()))
            .sum();
        let weight = cost(&copy.key, pages);
        let last = parts.last_mut().expect("there is always a part");
        if !last.is_empty() && size + weight > limit {
            parts.push(vec![copy]);
            size = weight;
        } else {
            last.push(copy);
            size += weight;
        }
    }
    parts
}
