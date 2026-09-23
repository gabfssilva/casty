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

use casty_core::backoff::Backoff;
use casty_core::handoff::messages::Pull;
use casty_core::handoff::ranges::{Transfer, overlap};
use casty_core::handoff::sweep::Handover;
use casty_core::node::NodeId;
use casty_core::placement::{pinned, token};
use casty_core::replication::messages::{Copy, Stamp, Write};
use casty_core::replication::owner::quorums;
use casty_core::replication::parts::packed;
use casty_core::replication::replica::{Arriving, Replica};
use tokio::time::Instant;

use crate::events::{Direction, Event};
use crate::node::Kind;
use crate::placement::{Step, Transfers};
use crate::replication::service::{Entity, Outgoing};
use crate::replication::wire::Message;

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
    /// The write the copy carried when this round began. A later one makes what they took the wrong copy.
    handed: Option<Stamp>,
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

#[derive(Debug)]
pub struct Handoff {
    node: NodeId,
    holds: Vec<Hold>,
    /// The steps whose ranges have all arrived, which the placement takes on its next pass.
    settled: Vec<Step>,
    /// The answer of a source being put back together, by the transfer and the source it came from.
    parts: HashMap<(u64, NodeId), Arriving>,
    handing: HashMap<Entity, Handing>,
    giving: HashMap<(String, NodeId), Giving>,
    /// The handover of a node being put back together, by the type and the node it comes from.
    taking: HashMap<(String, NodeId), Arriving>,
    present: BTreeSet<NodeId>,
    sends: Vec<Outgoing>,
    /// Keys that arrived since the last pass, which is when what this node keeps is decided again.
    swept: bool,
    /// The types this node was last reported to be filling ranges of, and to be handing keys of.
    coming: BTreeSet<String>,
    going: BTreeSet<String>,
    /// Whether a range or a handover may have started or ended since the handoffs were last reported.
    moved: bool,
    ids: u64,
    /// The last stream of keys this node sent, which is what tells one answer or one round of a handover from the
    /// ones before it where they arrive.
    streams: u64,
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
            present: BTreeSet::new(),
            sends: Vec::new(),
            swept: false,
            coming: BTreeSet::new(),
            going: BTreeSet::new(),
            moved: false,
            ids: 0,
            streams: 0,
            limit,
            backoff,
        }
    }

    /// The members a transfer can reach, which every decision here reads.
    ///
    /// A node the cluster gave up on is not one of them, even while the ring still places keys on it: nothing it is
    /// sent is answered, so asking it for a range or waiting for it to take one is waiting for the deadline.
    pub fn knows(&mut self, present: BTreeSet<NodeId>) {
        self.present = present;
        // The rest of a handover from a node the cluster gave up on never comes.
        self.taking
            .retain(|(_, node), _| self.present.contains(node));
        self.moved = true;
    }

    /// What is waiting to go out on the band of `replication`.
    pub fn take(&mut self) -> Vec<Outgoing> {
        core::mem::take(&mut self.sends)
    }

    /// The handoffs that started or ended since the last call, one per type and direction.
    ///
    /// Keys of a type are coming in while a range of it is still arriving, and going out while a node the cluster
    /// still has is yet to take one of them. Each call compares that with what the last one said, so a start is
    /// reported once and followed by one end.
    pub fn observed(&mut self) -> Vec<Event> {
        if !core::mem::take(&mut self.moved) {
            return Vec::new();
        }
        let coming: BTreeSet<&str> = self
            .holds
            .iter()
            .map(|held| held.transfer.actor.as_str())
            .collect();
        let going: BTreeSet<&str> = self
            .handing
            .iter()
            .filter(|(_, handing)| handing.owing(&self.present))
            .map(|((actor, _), _)| actor.as_str())
            .collect();
        let mut events = Vec::new();
        reconcile(&mut self.coming, &coming, Direction::In, &mut events);
        reconcile(&mut self.going, &going, Direction::Out, &mut events);
        events
    }

    /// Whether what this node keeps is to be decided again, which a range or a handover arriving asks for.
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
    ///
    /// A pinned key is in no range: no transfer carries it, and its node would otherwise not count its own answers
    /// while it fills a range of the type.
    #[must_use]
    pub fn arriving(&self, actor: &str, key: &str) -> bool {
        if pinned(key).is_some() {
            return false;
        }
        let point = token(actor, key);
        self.holds
            .iter()
            .any(|held| held.transfer.actor == actor && held.transfer.receiving(point))
    }

    /// What arrived on the band, whichever side of a transfer it belongs to.
    pub fn receive(&mut self, pull: Pull, replica: &mut Replica) {
        self.moved = true;
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
                // A pinned key is placed by the address it names and not by the ring, so it is in no range: its one
                // copy stays on its node.
                let keys: Vec<String> = replica
                    .keys(&actor)
                    .into_iter()
                    .filter(|key| {
                        let point = token(&actor, key);
                        pinned(key).is_none() && ranges.iter().any(|span| span.holds(point))
                    })
                    .collect();
                let copies: Vec<Copy> = keys
                    .iter()
                    .flat_map(|key| replica.copies(&actor, key))
                    .collect();
                let source = self.node.clone();
                self.stream(&node, copies, |stream, part, final_part, keys| {
                    Pull::RangeKeys {
                        actor: actor.clone(),
                        replica: source.clone(),
                        transfer,
                        stream,
                        part,
                        final_part,
                        receiving,
                        keys,
                    }
                });
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
    ///
    /// An answer a message of which went missing on the way leaves out keys the source keeps, whether or not it cut
    /// one of them, so it counts for nothing: the source is asked again, as one that has not answered.
    fn install(&mut self, answer: Pull, replica: &mut Replica) {
        let Pull::RangeKeys {
            actor,
            replica: from,
            transfer,
            stream,
            part,
            final_part,
            receiving,
            keys,
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
        let mut arriving = self.parts.remove(&at).unwrap_or_default();
        arriving.take(&actor, stream, part, keys, replica);
        if !final_part {
            self.parts.insert(at, arriving);
            return;
        }
        if !arriving.whole() {
            return;
        }
        let held = &mut self.holds[index];
        held.transfer.answered.insert(from.clone());
        if !receiving {
            held.transfer.settled.insert(from);
        }
        if held.transfer.pending().is_empty() {
            self.fill(index);
        } else if !receiving {
            // The threshold is per key, so this answer may have let keys through ahead of the step.
            self.swept = true;
        }
    }

    /// Take in one message of a handover, and tell the sender which keys it completed here.
    ///
    /// A key the sender does not hear of stays owed and goes out again, so a message that went missing costs a round
    /// and nothing else.
    fn take_over(&mut self, hand: Pull, replica: &mut Replica) {
        let Pull::HandKeys {
            actor,
            node,
            stream,
            part,
            final_part,
            keys,
        } = hand
        else {
            return;
        };
        let at = (actor.clone(), node.clone());
        let mut arriving = self.taking.remove(&at).unwrap_or_default();
        let landed = arriving.take(&actor, stream, part, keys, replica);
        if !final_part {
            self.taking.insert(at, arriving);
        }
        if landed.is_empty() {
            return;
        }
        // A key that came this way is a key this node did not keep a moment ago, and it may be one this node owns
        // and that carries the active mark. Nothing else asks for it, so the handover asks for the sweep itself.
        self.swept = true;
        self.send(
            node,
            Pull::TookKeys {
                actor,
                replica: self.node.clone(),
                keys: landed,
            },
        );
    }

    /// Drop the copy of every key that every node replicating it now has receiving.
    ///
    /// Only the copy they took: a write can land here after the key went out, while this node is still in the
    /// replica set of the owner that wrote it, and dropping then would drop a write nobody else has. That copy is
    /// handed away again instead, and the key goes only once what is kept is what they have.
    fn took(&mut self, actor: &str, from: &NodeId, keys: &[String], replica: &mut Replica) {
        let (mut done, mut again) = (Vec::new(), Vec::new());
        for key in keys {
            let entity = (actor.to_owned(), key.clone());
            let held = replica.accepted(actor, key).cloned();
            let Some(handing) = self.handing.get_mut(&entity) else {
                continue;
            };
            handing.took.insert(from.clone());
            if handing.owing(&self.present) {
                continue;
            }
            if held == handing.handed {
                done.push(entity);
            } else {
                handing.handed = held;
                handing.took.clear();
                again.push(entity);
            }
        }
        for entity in done {
            self.handing.remove(&entity);
            replica.drop(actor, &entity.1);
        }
        let at = Instant::now();
        for entity in again {
            let Some(handing) = self.handing.get(&entity) else {
                continue;
            };
            for node in handing.replicas.clone() {
                self.giving.insert(
                    (entity.0.clone(), node),
                    Giving {
                        at,
                        delay: self.backoff.first,
                    },
                );
            }
        }
    }

    /// Start handing away the keys this node stopped replicating, one sender per node that is to take them.
    pub fn give(&mut self, given: Vec<Handover>, replica: &Replica, now: Instant) {
        self.moved = true;
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
                let handed = replica.accepted(&entity.0, &entity.1).cloned();
                self.handing.insert(
                    entity,
                    Handing {
                        replicas,
                        took: BTreeSet::new(),
                        handed,
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
        self.moved = true;
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
            self.fill(index);
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
            let giver = self.node.clone();
            self.stream(&to, copies, |stream, part, final_part, keys| {
                Pull::HandKeys {
                    actor: actor.clone(),
                    node: giver.clone(),
                    stream,
                    part,
                    final_part,
                    keys,
                }
            });
        }
    }

    /// Let the step of the hold at `index` through, with whatever of an answer to it is still being put together.
    fn fill(&mut self, index: usize) {
        let held = self.holds.remove(index);
        self.parts
            .retain(|(transfer, _), _| *transfer != held.transfer.id);
        self.settled.push(held.step);
        self.swept = true;
    }

    /// Send `copies` to `to` as a stream this node has not sent before: messages that each fit in the limit, numbered
    /// from the first, the last one saying so.
    fn stream(
        &mut self,
        to: &NodeId,
        copies: Vec<Copy>,
        message: impl Fn(u64, u32, bool, Vec<Copy>) -> Pull,
    ) {
        self.streams += 1;
        let parts = packed(copies, self.limit);
        let last = parts.len() - 1;
        for (part, copies) in parts.into_iter().enumerate() {
            let pull = message(
                self.streams,
                u32::try_from(part).unwrap_or(u32::MAX),
                part == last,
                copies,
            );
            self.send(to.clone(), pull);
        }
    }

    fn send(&mut self, to: NodeId, message: Pull) {
        self.sends.push(Outgoing {
            to,
            message: Message::Pull(message),
        });
    }

    /// Start filling the ranges of `step`, a step of `kind` as this process has it. The placement drops the step once
    /// `filled` says so.
    pub fn begin(&mut self, step: &Step, kind: Option<&Kind>) {
        // No key of a pinned type is in a range, so its step has nothing to pull and nothing to report: it is through
        // as soon as it is taken, and the placement drops it on the pass that made it.
        if kind.is_some_and(|kind| kind.pinned) {
            self.settled.push(step.clone());
            return;
        }
        self.moved = true;
        let previous = step.previous.nodes().len();
        let count = kind.map_or(previous, |kind| kind.replicas.min(previous));
        let reads = quorums(kind.map_or(Write::Majority, |kind| kind.write), count).reads;
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
        // Nothing to pull: no node of the previous ring is still here, so the step is through as soon as it is receiving.
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
}

impl Transfers for Handoff {
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

/// Say which types started and which ended moving in `direction` since `reported`, and keep `now` in its place.
fn reconcile(
    reported: &mut BTreeSet<String>,
    now: &BTreeSet<&str>,
    direction: Direction,
    events: &mut Vec<Event>,
) {
    let started = now.iter().filter(|actor| !reported.contains(**actor));
    let ended = reported
        .iter()
        .filter(|actor| !now.contains(actor.as_str()));
    events.extend(started.map(|actor| Event::HandoffStarted {
        actor: (*actor).to_owned(),
        direction,
    }));
    events.extend(ended.map(|actor| Event::HandoffEnded {
        actor: actor.clone(),
        direction,
    }));
    *reported = now.iter().map(|actor| (*actor).to_owned()).collect();
}

#[cfg(test)]
mod tests {
    use casty_core::backoff::Backoff;
    use casty_core::handoff::messages::Pull;
    use casty_core::replication::messages::{ACTIVE, Copy, Epoch, Stamp, Write};
    use casty_core::replication::parts::append;
    use casty_core::replication::replica::Replica;
    use casty_core::rolls::Rolls;
    use casty_core::store::Pages;
    use casty_net::limits::Limits;
    use std::collections::BTreeSet;

    use casty_core::placement::{Range, Ring, pin};

    use super::{
        Direction, Duration, Event, Handoff, Handover, Instant, Kind, Message, NodeId, Outgoing,
        Step, Transfers,
    };
    use crate::node::ENVELOPE;
    use crate::placement::VNODES;
    use crate::replication::wire::{decode, encode};

    const ACTOR: &str = "tests.app:consumer";
    const KEY: &str = "partition-1";
    const LIMIT: usize = 1024;
    /// A limit that three small keys fill, so that a message lost takes whole keys with it and no part of one.
    const WIDE: usize = 4 * 1024;

    /// A state a few bytes long.
    fn small() -> Pages {
        Pages::from([("state".to_owned(), b"small".to_vec())])
    }

    /// Keep `count` keys of `pages` each, as `owner` wrote them, and give back their names in order.
    fn keep(replica: &mut Replica, owner: &NodeId, count: usize, pages: &Pages) -> Vec<String> {
        let keys: Vec<String> = (0..count).map(|index| format!("key-{index:06}")).collect();
        for key in &keys {
            replica.install(
                ACTOR,
                Copy {
                    key: key.clone(),
                    pages: pages.clone(),
                    ..at(owner, 1)
                },
            );
        }
        keys
    }

    /// The handover of `keys` to `to`, the one node that replicates them now.
    fn handovers(keys: &[String], to: &NodeId) -> Vec<Handover> {
        keys.iter()
            .map(|key| Handover {
                actor: ACTOR.to_owned(),
                key: key.clone(),
                replicas: vec![to.clone()],
            })
            .collect()
    }

    /// What a handoff sends, which is always a pull.
    fn pull(out: Outgoing) -> Pull {
        let Message::Pull(pull) = out.message else {
            panic!("a handoff sent something else than a pull");
        };
        pull
    }

    /// What `out` is once it crossed to the other node, which the transport refuses when it takes more than
    /// `message` bytes.
    fn crossed(out: &Outgoing, message: usize) -> Pull {
        let payload = encode(&out.message);
        assert!(
            payload.len() <= message,
            "a message of {} bytes, over the limit of {message}",
            payload.len()
        );
        match decode(&payload) {
            Ok(Message::Pull(pull)) => pull,
            other => panic!("a pull came back as {other:?}"),
        }
    }

    /// Fire `puller` at `now` and hand what it asks for to `source`, whose answer comes back as it was sent.
    fn answered(
        puller: &mut Handoff,
        filling: &Replica,
        source: &mut Handoff,
        kept: &mut Replica,
        now: Instant,
    ) -> Vec<Pull> {
        puller.fired(now, filling);
        let asked = puller.take();
        assert!(!asked.is_empty(), "the source was not asked");
        for out in asked {
            source.receive(pull(out), kept);
        }
        source.take().into_iter().map(pull).collect()
    }

    /// The key as one replica keeps it after the write `version`, whole and with the mark that says it was running.
    fn at(owner: &NodeId, version: u64) -> Copy {
        Copy {
            key: KEY.to_owned(),
            accepted: Some(Stamp {
                epoch: Epoch {
                    round: 1,
                    node: owner.clone(),
                },
                version,
            }),
            promised: None,
            pages: Pages::from([(ACTIVE.to_owned(), Vec::new())]),
            part: 0,
            final_part: true,
        }
    }

    /// A key larger than a message goes out over several parts of the handover, and lands only once all of it is in.
    #[test]
    fn a_key_larger_than_a_message_is_handed_over_in_parts_and_lands_whole() {
        let ids = Rolls::seeded(64).nodes(2);
        let limit: usize = 256;
        let state = Pages::from([
            (ACTIVE.to_owned(), Vec::new()),
            (
                "entries".to_owned(),
                (0..=u8::MAX).cycle().take(4 * limit).collect::<Vec<u8>>(),
            ),
        ]);
        let mut giver = Handoff::new(ids[0].clone(), limit, Backoff::default());
        let mut kept = Replica::new(ids[0].clone(), limit);
        kept.install(
            ACTOR,
            Copy {
                pages: state.clone(),
                ..at(&ids[0], 1)
            },
        );
        giver.knows(BTreeSet::from([ids[1].clone()]));
        let now = Instant::now();
        giver.give(
            vec![Handover {
                actor: ACTOR.to_owned(),
                key: KEY.to_owned(),
                replicas: vec![ids[1].clone()],
            }],
            &kept,
            now,
        );
        giver.fired(now, &kept);
        let sent = giver.take();
        assert!(
            sent.len() > 4,
            "a key four times a message went out in {} parts",
            sent.len()
        );

        let mut taker = Handoff::new(ids[1].clone(), limit, Backoff::default());
        let mut receiving = Replica::new(ids[1].clone(), limit);
        let last = sent.len() - 1;
        for (index, out) in sent.into_iter().enumerate() {
            let Message::Pull(pull) = out.message else {
                panic!("a handover travels as a pull");
            };
            assert!(
                receiving.accepted(ACTOR, KEY).is_none(),
                "the key landed before its part {index} of {last}"
            );
            taker.receive(pull, &mut receiving);
        }

        let mut rebuilt = Pages::new();
        for copy in receiving.copies(ACTOR, KEY) {
            append(&mut rebuilt, copy.pages);
        }
        assert_eq!(rebuilt, state);
        assert!(
            matches!(
                taker.take().as_slice(),
                [Outgoing {
                    message: Message::Pull(Pull::TookKeys { .. }),
                    ..
                }]
            ),
            "the key that landed was not reported as taken"
        );
    }

    /// Nothing else says a key arrived this way: a transfer asks for the sweep when its range lands, and a write
    /// with the mark asks for it through the replication. A handover that did not would leave the key marked
    /// active, owned here and run by nobody.
    #[test]
    fn a_handover_that_landed_asks_for_the_sweep() {
        let ids = Rolls::seeded(61).nodes(2);
        let mut handoff = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
        let mut replica = Replica::new(ids[0].clone(), LIMIT);
        assert!(!handoff.resweep(), "nothing arrived yet");

        handoff.receive(
            Pull::HandKeys {
                actor: ACTOR.to_owned(),
                node: ids[1].clone(),
                stream: 1,
                part: 0,
                final_part: true,
                keys: vec![at(&ids[1], 1)],
            },
            &mut replica,
        );

        assert!(replica.marked(ACTOR, KEY), "the key did not land");
        assert!(handoff.resweep(), "the key landed and nothing looked at it");
    }

    /// A write lands here after the copy went out and before the ones that take it have answered: the owner that
    /// wrote it still had this node in the replica set of the key. Dropping on their answer would drop that write.
    #[test]
    fn a_copy_that_moved_on_after_it_went_out_is_handed_again_instead_of_dropped() {
        let ids = Rolls::seeded(63).nodes(2);
        let mut handoff = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
        let mut replica = Replica::new(ids[0].clone(), LIMIT);
        replica.install(ACTOR, at(&ids[1], 1));
        handoff.knows(BTreeSet::from([ids[1].clone()]));
        let now = Instant::now();
        handoff.give(
            vec![Handover {
                actor: ACTOR.to_owned(),
                key: KEY.to_owned(),
                replicas: vec![ids[1].clone()],
            }],
            &replica,
            now,
        );
        handoff.fired(now, &replica);
        replica.install(ACTOR, at(&ids[1], 2));

        handoff.receive(
            Pull::TookKeys {
                actor: ACTOR.to_owned(),
                replica: ids[1].clone(),
                keys: vec![KEY.to_owned()],
            },
            &mut replica,
        );

        assert_eq!(
            replica.accepted(ACTOR, KEY).map(|stamp| stamp.version),
            Some(2),
            "the write that landed after the copy went out was dropped"
        );
        assert!(
            handoff.owing(),
            "the copy they do not have is owed to nobody"
        );
    }

    #[test]
    fn a_handover_of_nothing_asks_for_nothing() {
        let ids = Rolls::seeded(62).nodes(2);
        let mut handoff = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
        let mut replica = Replica::new(ids[0].clone(), LIMIT);

        handoff.receive(
            Pull::HandKeys {
                actor: ACTOR.to_owned(),
                node: ids[1].clone(),
                stream: 1,
                part: 0,
                final_part: true,
                keys: Vec::new(),
            },
            &mut replica,
        );

        assert!(!handoff.resweep());
    }

    #[test]
    fn a_handover_is_reported_once_as_it_starts_and_once_as_it_ends() {
        let ids = Rolls::seeded(65).nodes(2);
        let mut handoff = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
        let mut replica = Replica::new(ids[0].clone(), LIMIT);
        replica.install(ACTOR, at(&ids[1], 1));
        handoff.knows(BTreeSet::from([ids[1].clone()]));
        assert!(handoff.observed().is_empty(), "nothing is moving yet");

        handoff.give(
            vec![Handover {
                actor: ACTOR.to_owned(),
                key: KEY.to_owned(),
                replicas: vec![ids[1].clone()],
            }],
            &replica,
            Instant::now(),
        );
        assert_eq!(
            handoff.observed(),
            vec![Event::HandoffStarted {
                actor: ACTOR.to_owned(),
                direction: Direction::Out,
            }]
        );
        assert!(
            handoff.observed().is_empty(),
            "the start was reported twice"
        );

        handoff.receive(
            Pull::TookKeys {
                actor: ACTOR.to_owned(),
                replica: ids[1].clone(),
                keys: vec![KEY.to_owned()],
            },
            &mut replica,
        );
        assert_eq!(
            handoff.observed(),
            vec![Event::HandoffEnded {
                actor: ACTOR.to_owned(),
                direction: Direction::Out,
            }]
        );
    }

    #[test]
    fn a_range_is_reported_as_coming_in_until_its_sources_have_answered() {
        let ids = Rolls::seeded(66).nodes(2);
        let mut handoff = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
        let mut replica = Replica::new(ids[0].clone(), LIMIT);
        handoff.knows(BTreeSet::from([ids[1].clone()]));
        let previous = Ring::build(ids.iter().cloned(), VNODES);

        handoff.begin(
            &Step {
                actor: ACTOR.to_owned(),
                previous: previous.clone(),
                ring: previous,
                ranges: Vec::new(),
            },
            None,
        );
        assert_eq!(
            handoff.observed(),
            vec![Event::HandoffStarted {
                actor: ACTOR.to_owned(),
                direction: Direction::In,
            }]
        );

        handoff.receive(
            Pull::RangeKeys {
                actor: ACTOR.to_owned(),
                replica: ids[1].clone(),
                transfer: 1,
                stream: 1,
                part: 0,
                final_part: true,
                receiving: false,
                keys: Vec::new(),
            },
            &mut replica,
        );
        assert_eq!(
            handoff.observed(),
            vec![Event::HandoffEnded {
                actor: ACTOR.to_owned(),
                direction: Direction::In,
            }]
        );
    }

    /// An answer that lost a whole message of small keys has no key with a gap in it, and still leaves keys out: it is
    /// not counted, and the source is asked again until an answer arrives whole.
    #[test]
    fn a_range_answer_a_message_of_which_went_missing_is_not_counted_and_is_asked_again() {
        let ids = Rolls::seeded(68).nodes(2);
        let mut source = Handoff::new(ids[1].clone(), WIDE, Backoff::default());
        let mut kept = Replica::new(ids[1].clone(), WIDE);
        let keys = keep(&mut kept, &ids[1], 12, &small());
        let mut puller = Handoff::new(ids[0].clone(), WIDE, Backoff::default());
        let mut filling = Replica::new(ids[0].clone(), WIDE);
        puller.knows(BTreeSet::from([ids[1].clone()]));
        let previous = Ring::build(ids.iter().cloned(), VNODES);
        let step = Step {
            actor: ACTOR.to_owned(),
            previous: previous.clone(),
            ring: previous,
            // A range that ends where it starts is the whole circle.
            ranges: vec![Range {
                start: u64::MAX,
                end: u64::MAX,
            }],
        };
        puller.begin(&step, None);

        let asked = Instant::now();
        let mut answer = answered(&mut puller, &filling, &mut source, &mut kept, asked);
        assert!(
            answer.len() > 2,
            "the answer came in {} messages",
            answer.len()
        );
        let lost = answer.remove(1);
        assert!(
            matches!(&lost, Pull::RangeKeys { keys, .. } if keys.len() > 1),
            "the message lost is not one of several whole keys: {lost:?}"
        );
        for message in answer {
            puller.receive(message, &mut filling);
        }
        assert!(
            Transfers::filled(&mut puller).is_empty(),
            "an answer a message of which went missing was counted"
        );
        assert!(puller.arriving(ACTOR, &keys[0]));

        let later = asked + Duration::from_secs(1);
        let answer = answered(&mut puller, &filling, &mut source, &mut kept, later);
        for message in answer {
            puller.receive(message, &mut filling);
        }
        assert_eq!(Transfers::filled(&mut puller), vec![step]);
        let mut held = filling.keys(ACTOR);
        held.sort();
        assert_eq!(held, keys);
    }

    /// Each message of a handover is answered for the keys it completed, so one that went missing leaves only its own
    /// keys owed, and they go out again.
    #[test]
    fn a_handover_message_that_went_missing_leaves_only_its_keys_owed() {
        let ids = Rolls::seeded(69).nodes(2);
        let mut giver = Handoff::new(ids[0].clone(), WIDE, Backoff::default());
        let mut kept = Replica::new(ids[0].clone(), WIDE);
        let keys = keep(&mut kept, &ids[0], 12, &small());
        giver.knows(BTreeSet::from([ids[1].clone()]));
        let now = Instant::now();
        giver.give(handovers(&keys, &ids[1]), &kept, now);
        giver.fired(now, &kept);
        let mut sent: Vec<Pull> = giver.take().into_iter().map(pull).collect();
        assert!(
            sent.len() > 2,
            "the handover went out in {} messages",
            sent.len()
        );
        let Pull::HandKeys { keys: lost, .. } = sent.remove(1) else {
            panic!("a handover travels as keys handed");
        };
        let mut missing: Vec<String> = lost.into_iter().map(|copy| copy.key).collect();
        missing.sort();

        let mut taker = Handoff::new(ids[1].clone(), WIDE, Backoff::default());
        let mut receiving = Replica::new(ids[1].clone(), WIDE);
        for message in sent {
            taker.receive(message, &mut receiving);
        }
        for message in taker.take().into_iter().map(pull) {
            giver.receive(message, &mut kept);
        }
        let owed: Vec<String> = giver.owed().into_iter().map(|(_, key)| key).collect();
        assert_eq!(owed, missing);
        let mut held = kept.keys(ACTOR);
        held.sort();
        assert_eq!(
            held, missing,
            "what was taken is kept, or what was not is dropped"
        );

        giver.fired(now + Duration::from_secs(1), &kept);
        for message in giver.take().into_iter().map(pull) {
            taker.receive(message, &mut receiving);
        }
        for message in taker.take().into_iter().map(pull) {
            giver.receive(message, &mut kept);
        }
        assert!(!giver.owing(), "{:?} are still owed", giver.owed());
        assert!(kept.keys(ACTOR).is_empty());
        let mut held = receiving.keys(ACTOR);
        held.sort();
        assert_eq!(held, keys);
    }

    /// Under the default limits, a handover of half a million small keys goes out and is answered for in messages that
    /// each fit, although the list of its keys alone would not, and it ends with every key taken and dropped here.
    #[test]
    fn a_handover_of_half_a_million_small_keys_fits_in_messages_and_completes() {
        const KEYS: usize = 500_000;
        let message = Limits::default().message;
        let limit = message - ENVELOPE;
        let ids = Rolls::seeded(70).nodes(2);
        let mut giver = Handoff::new(ids[0].clone(), limit, Backoff::default());
        let mut kept = Replica::new(ids[0].clone(), limit);
        let keys = keep(&mut kept, &ids[0], KEYS, &Pages::new());
        let listed: usize = keys.iter().map(|key| key.len() + 1).sum();
        assert!(
            listed > message,
            "the names alone take {listed} bytes, which one message carries"
        );
        giver.knows(BTreeSet::from([ids[1].clone()]));
        let now = Instant::now();
        giver.give(handovers(&keys, &ids[1]), &kept, now);
        giver.fired(now, &kept);

        let mut taker = Handoff::new(ids[1].clone(), limit, Backoff::default());
        let mut receiving = Replica::new(ids[1].clone(), limit);
        let sent = giver.take();
        assert!(sent.len() > 1, "the handover went out in one message");
        for out in sent {
            taker.receive(crossed(&out, message), &mut receiving);
        }
        let answers = taker.take();
        assert!(
            answers.len() > 1,
            "the keys taken were listed in {} message",
            answers.len()
        );
        for out in &answers {
            giver.receive(crossed(out, message), &mut kept);
        }

        assert!(!giver.owing(), "{} keys are still owed", giver.owed().len());
        assert!(kept.keys(ACTOR).is_empty(), "keys that were taken are kept");
        assert_eq!(receiving.keys(ACTOR).len(), KEYS);
    }

    /// The whole circle of `ACTOR`, gained by the first of `ids` on the ring of all of them.
    fn whole(ids: &[NodeId]) -> Step {
        let previous = Ring::build(ids.iter().cloned(), VNODES);
        Step {
            actor: ACTOR.to_owned(),
            previous: previous.clone(),
            ring: previous,
            // A range that ends where it starts is the whole circle.
            ranges: vec![Range {
                start: u64::MAX,
                end: u64::MAX,
            }],
        }
    }

    /// A pinned key is placed by the address it names, so the node keeping it answers a pull of the range its token
    /// falls in without it: the node pulling would hold a copy that nothing reads.
    #[test]
    fn a_range_answer_leaves_out_the_keys_pinned_to_the_node_that_answers() {
        let ids = Rolls::seeded(71).nodes(2);
        let mut source = Handoff::new(ids[1].clone(), WIDE, Backoff::default());
        let mut kept = Replica::new(ids[1].clone(), WIDE);
        let keys = keep(&mut kept, &ids[1], 6, &small());
        for index in 0..6 {
            kept.install(
                ACTOR,
                Copy {
                    key: pin("10.0.0.5:7400", &format!("worker-{index}")),
                    pages: small(),
                    ..at(&ids[1], 1)
                },
            );
        }
        let mut puller = Handoff::new(ids[0].clone(), WIDE, Backoff::default());
        let mut filling = Replica::new(ids[0].clone(), WIDE);
        puller.knows(BTreeSet::from([ids[1].clone()]));
        let step = whole(&ids);
        puller.begin(&step, None);

        let answer = answered(
            &mut puller,
            &filling,
            &mut source,
            &mut kept,
            Instant::now(),
        );
        for message in answer {
            puller.receive(message, &mut filling);
        }

        assert_eq!(Transfers::filled(&mut puller), vec![step]);
        let mut held = filling.keys(ACTOR);
        held.sort();
        assert_eq!(held, keys, "a pinned key went out with the range");
    }

    /// A replica filling a range answers for its keys without being counted. A pinned key is in no range, so its node
    /// counts its own answers for it whatever range of the type it is filling.
    #[test]
    fn a_pinned_key_is_never_arriving() {
        let ids = Rolls::seeded(72).nodes(2);
        let mut handoff = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
        handoff.knows(BTreeSet::from([ids[1].clone()]));
        handoff.begin(&whole(&ids), None);

        assert!(
            handoff.arriving(ACTOR, KEY),
            "the whole circle is not arriving"
        );
        assert!(!handoff.arriving(ACTOR, &pin("10.0.0.5:7400", KEY)));
    }

    /// No key of a pinned type is in a range, so the step a change of the ring gives it is through as it is taken: no
    /// range of it is pulled, and no handoff of it is reported. The same step of a type placed by the ring is both.
    #[test]
    fn a_step_of_a_pinned_type_pulls_nothing_and_reports_nothing() {
        let ids = Rolls::seeded(73).nodes(2);
        let step = whole(&ids);
        let walked = |pinned: bool| {
            let mut handoff = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
            let replica = Replica::new(ids[0].clone(), LIMIT);
            let kind = Kind {
                actor: ACTOR.to_owned(),
                replicas: 1,
                write: Write::Majority,
                write_timeout: None,
                pinned,
                durable: None,
            };
            handoff.knows(BTreeSet::from([ids[1].clone()]));
            handoff.begin(&step, Some(&kind));
            handoff.fired(Instant::now(), &replica);
            (
                handoff.take().len(),
                handoff.observed(),
                Transfers::filled(&mut handoff),
            )
        };

        let (asked, reported, filled) = walked(false);
        assert_eq!(
            asked, 1,
            "a range of a type placed by the ring was not pulled"
        );
        assert_eq!(
            reported,
            vec![Event::HandoffStarted {
                actor: ACTOR.to_owned(),
                direction: Direction::In,
            }]
        );
        assert!(filled.is_empty(), "a range was through before it arrived");

        let (asked, reported, filled) = walked(true);
        assert_eq!(asked, 0, "a range of a pinned type was pulled");
        assert!(
            reported.is_empty(),
            "a handoff of a pinned type was reported: {reported:?}"
        );
        assert_eq!(filled, vec![step]);
    }

    /// A deleted key is handed over as its tombstone and never as a state: the node that takes it keeps no page and no
    /// mark, so nothing brings the key back there, and the older copy another node handed it first is fenced.
    #[test]
    fn a_handover_after_a_deletion_moves_the_tombstone_and_no_state() {
        let ids = Rolls::seeded(74).nodes(3);
        let tombstone = casty_core::replication::messages::tombstone();
        let mut giver = Handoff::new(ids[0].clone(), LIMIT, Backoff::default());
        let mut kept = Replica::new(ids[0].clone(), LIMIT);
        kept.install(
            ACTOR,
            Copy {
                pages: tombstone.clone(),
                ..at(&ids[0], 2)
            },
        );
        giver.knows(BTreeSet::from([ids[1].clone()]));
        let now = Instant::now();
        giver.give(
            vec![Handover {
                actor: ACTOR.to_owned(),
                key: KEY.to_owned(),
                replicas: vec![ids[1].clone()],
            }],
            &kept,
            now,
        );
        giver.fired(now, &kept);

        let mut taker = Handoff::new(ids[1].clone(), LIMIT, Backoff::default());
        let mut receiving = Replica::new(ids[1].clone(), LIMIT);
        let mut stale = small();
        stale.insert(ACTIVE.to_owned(), Vec::new());
        receiving.install(
            ACTOR,
            Copy {
                accepted: Some(Stamp {
                    epoch: Epoch {
                        round: 0,
                        node: ids[2].clone(),
                    },
                    version: 1,
                }),
                pages: stale,
                ..at(&ids[2], 1)
            },
        );
        for out in giver.take() {
            let Message::Pull(pull) = out.message else {
                panic!("a handover travels as a pull");
            };
            taker.receive(pull, &mut receiving);
        }

        assert!(receiving.deleted(ACTOR, KEY).is_some());
        assert!(
            !receiving.marked(ACTOR, KEY),
            "a deleted key would be brought back"
        );
        assert!(
            receiving
                .copies(ACTOR, KEY)
                .iter()
                .all(|copy| copy.pages == tombstone),
            "a state moved with the deleted key"
        );

        // It is taken like any other key, so the node that handed it over lets its own tombstone go.
        for out in taker.take() {
            if let Message::Pull(pull) = out.message {
                giver.receive(pull, &mut kept);
            }
        }
        assert!(kept.keys(ACTOR).is_empty());
        assert!(!giver.owing());
    }
}
