//! The overlay, the broadcast and the member table, driven by what arrives and by the clock.
//!
//! Nothing here does I/O: the caller hands it messages and instants, and takes what it must send. Every message
//! carries the record of its sender, because envelopes do not say where they come from.

use core::time::Duration;
use std::collections::{BTreeSet, HashMap};

use casty_core::membership::broadcast::{Broadcast, Broadcaster, Send as Pushed};
use casty_core::membership::table::{MemberTable, Record, Status, Transition};
use casty_core::membership::views::{Effect, Overlay, View, Views};
use casty_core::node::NodeId;
use casty_core::rolls::Rolls;
use casty_net::pool::Target;

use super::wire::{Body, Message, Sender};

/// The periods the membership of a cluster runs by.
#[derive(Debug, Clone, Copy)]
pub struct Timings {
    pub heartbeat: Duration,
    pub suspect_after: Duration,
    pub dead_after: Duration,
    pub remove_after: Option<Duration>,
    pub anti_entropy: Duration,
    pub graft_after: Duration,
    pub shuffle_every: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_secs(1),
            suspect_after: Duration::from_secs(5),
            dead_after: Duration::from_secs(10),
            remove_after: Some(Duration::from_secs(60)),
            anti_entropy: Duration::from_secs(10),
            graft_after: Duration::from_millis(500),
            shuffle_every: Duration::from_secs(30),
        }
    }
}

/// A member of the cluster as one node sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub node: NodeId,
    pub status: Status,
    pub types: BTreeSet<String>,
}

/// A message this node must put on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    pub to: Target,
    pub message: Message,
}

// The flags a node keeps about itself, each one named after the question it answers.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
pub struct Membership {
    node: NodeId,
    timings: Timings,
    table: MemberTable,
    views: Views,
    broadcast: Broadcaster,
    seeds: Vec<String>,
    seen: HashMap<NodeId, f64>,
    rolls: Rolls,
    leaving: bool,
    sends: Vec<Outgoing>,
    /// Whether this node is in a cluster, whether it was declared gone, and whether the members changed.
    pub joined: bool,
    pub removed: bool,
    pub changed: bool,
}

impl Membership {
    #[must_use]
    pub fn new(
        node: NodeId,
        types: BTreeSet<String>,
        seeds: Vec<String>,
        timings: Timings,
        overlay: Overlay,
    ) -> Self {
        let seeds = seeds
            .into_iter()
            .filter(|seed| Some(seed.as_str()) != node.address.as_deref())
            .collect::<Vec<_>>();
        let joined = seeds.is_empty();
        Self {
            table: MemberTable::new(
                node.clone(),
                types,
                timings.dead_after.as_secs_f64(),
                timings.remove_after.map(|after| after.as_secs_f64()),
            ),
            views: Views::new(node.clone(), overlay, Rolls::fresh()),
            broadcast: Broadcaster::new(node.clone(), timings.graft_after.as_secs_f64()),
            node,
            timings,
            seeds,
            seen: HashMap::new(),
            rolls: Rolls::fresh(),
            leaving: false,
            sends: Vec::new(),
            joined,
            removed: false,
            changed: false,
        }
    }

    /// The members this node knows, itself included and the ones that left excluded.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.table
            .records()
            .filter(|record| record.status != Status::Left)
            .map(|record| Member {
                node: record.node.clone(),
                status: record.status,
                types: record.types.clone(),
            })
            .collect()
    }

    /// Whether this node sees a majority of the members alive, itself included (`MemberTable::majority`).
    #[must_use]
    pub fn majority(&self) -> bool {
        self.table.majority()
    }

    /// What this node must put on the wire, taken out so that the caller sends it.
    pub fn take(&mut self) -> Vec<Outgoing> {
        core::mem::take(&mut self.sends)
    }

    /// The changes of status of the members since the last call, each one once and in the order the table made them.
    pub fn transitions(&mut self) -> Vec<Transition> {
        self.table.transitions()
    }

    /// Ask to enter the cluster, through every seed and through one member already known.
    ///
    /// Every seed, and not one of them, because two nodes that join each other and no seed settle into a second
    /// cluster that the first one never hears about.
    pub fn join(&mut self) {
        for address in self.seeds.clone() {
            self.send(Target::Seed(address), Body::View(View::Join));
        }
        let alive: Vec<NodeId> = self
            .table
            .records()
            .filter(|record| record.status == Status::Alive)
            .map(|record| record.node.clone())
            .collect();
        self.send_any(&alive, &Body::View(View::Join));
    }

    pub fn receive(&mut self, payload: &[u8], now: f64) {
        let Ok(message) = super::wire::decode(payload) else {
            return;
        };
        let sender = message.sender.node().clone();
        if let Some(record) = message.sender.record() {
            self.seen.insert(sender.clone(), now);
            self.learn(vec![record.clone()], None, now);
        }
        match message.body {
            Body::View(View::Join) => {
                if let Some(record) = message.sender.record().cloned() {
                    // Only `admit` replaces an older incarnation of the same address, and a join decides it.
                    let changed = self.table.admit(record, now);
                    self.publish(changed, None, now);
                    // The whole table, and not only what the views decide to send: a node that entered through a
                    // member whose active view was full would otherwise wait for the anti-entropy to learn the
                    // cluster, and until then it has no ring of its own.
                    let known = self.known();
                    self.send(Target::Node(sender.clone()), Body::Sync(known));
                }
                let effects = self.views.receive(&sender, View::Join);
                self.apply(effects, now);
            }
            Body::View(view) => {
                let effects = self.views.receive(&sender, view);
                self.apply(effects, now);
            }
            Body::Broadcast(broadcast) => {
                let heard = self.broadcast.receive(&sender, broadcast, now);
                if let Some(record) = heard.record.clone() {
                    self.learn(vec![record.clone()], Some(&record), now);
                }
                self.deliver(heard.sends);
            }
            Body::Sync(records) => {
                // A client is not a member: it reads the table and never writes to it.
                if message.sender.record().is_some() {
                    self.learn(records, None, now);
                }
                let known = self.known();
                self.send(Target::Node(sender), Body::SyncReply(known));
            }
            Body::SyncReply(records) => self.learn(records, None, now),
            // A logical link can be one-sided, and a ping from a stranger is how the sender learns it.
            Body::Ping => {
                let answer = if self.views.active().contains(&sender) {
                    Body::Ack
                } else {
                    Body::View(View::Disconnect)
                };
                self.send(Target::Node(sender), answer);
            }
            // An answer to a ping says nothing but that its sender is alive, which is noted above.
            Body::Ack => {}
        }
        self.settle();
    }

    /// Ping the neighbors and one member that no neighbor link watches, and suspect whoever went silent.
    ///
    /// Watching only the active view leaves a member this node has no link to alive for as long as nobody else
    /// reports it: the minority of a partition would keep counting the unreachable side as alive, and an alive
    /// majority is exactly what lets a node remove the dead.
    pub fn probe(&mut self, now: f64) {
        let silent = self.timings.suspect_after.as_secs_f64();
        let watched: Vec<NodeId> = self
            .views
            .active()
            .iter()
            .cloned()
            .chain(self.elsewhere())
            .collect();
        for node in watched {
            let since = *self.seen.entry(node.clone()).or_insert(now);
            if now - since >= silent {
                self.suspect(&node, now);
            } else {
                self.send(Target::Node(node), Body::Ping);
            }
        }
        if self.views.active().is_empty() {
            self.join();
        } else {
            let effects = self.views.promote();
            self.apply(effects, now);
        }
    }

    pub fn expire(&mut self, now: f64) {
        let changed = self.table.expire(now);
        self.publish(changed, None, now);
        self.settle();
    }

    pub fn graft(&mut self, now: f64) {
        let grafts = self.broadcast.tick(now);
        self.deliver(grafts);
    }

    pub fn shuffle(&mut self, now: f64) {
        let effects = self.views.shuffle();
        self.apply(effects, now);
    }

    /// Exchange the whole table with a random member.
    ///
    /// The target is any member that has not left, and not only an alive one: after a partition heals, the minority
    /// is left for the majority, which no longer talks to it, and only such a sync tells it so.
    pub fn anti_entropy(&mut self) {
        let others: Vec<NodeId> = self
            .table
            .records()
            .filter(|record| record.status != Status::Left)
            .map(|record| record.node.clone())
            .collect();
        let known = self.known();
        self.send_any(&others, &Body::Sync(known));
    }

    /// Tell the cluster of a type this node met, so that every member has it before a ring gives it keys.
    pub fn know(&mut self, types: &BTreeSet<String>, now: f64) {
        if let Some(record) = self.table.know(types, now) {
            self.publish(vec![record], None, now);
        }
    }

    /// Tell the cluster this node is on its way out.
    ///
    /// It stays in the ring and keeps replicating, so nothing it holds is lost, and stops being chosen as owner, so
    /// nothing new is routed to it.
    pub fn leave(&mut self, now: f64) {
        if self.leaving {
            return;
        }
        self.leaving = true;
        let record = self.table.leave(now);
        self.publish(vec![record], None, now);
    }

    /// Tell the cluster this node is gone, which also takes it out of the ring of every type it hosted.
    pub fn depart(&mut self, now: f64) {
        let record = self.table.depart(now);
        self.publish(vec![record], None, now);
    }

    /// The member with the oldest sign of life among those outside the active view, if there is one.
    fn elsewhere(&self) -> Option<NodeId> {
        self.table
            .records()
            .filter(|record| {
                matches!(record.status, Status::Alive | Status::Leaving)
                    && record.node != self.node
                    && !self.views.active().contains(&record.node)
            })
            .min_by(|left, right| {
                let at = |node: &NodeId| self.seen.get(node).copied().unwrap_or(0.0);
                at(&left.node).total_cmp(&at(&right.node))
            })
            .map(|record| record.node.clone())
    }

    fn suspect(&mut self, node: &NodeId, now: f64) {
        let record = self.table.record(node).cloned();
        self.seen.remove(node);
        if let Some(record) = record {
            self.learn(
                vec![Record {
                    status: Status::Suspect,
                    ..record
                }],
                None,
                now,
            );
        }
    }

    fn apply(&mut self, effects: Vec<Effect>, now: f64) {
        for effect in effects {
            match effect {
                Effect::Send { to, message } => self.send(Target::Node(to), Body::View(message)),
                Effect::NeighborUp(node) => {
                    self.seen.entry(node.clone()).or_insert(now);
                    self.broadcast.neighbor_up(node.clone());
                    let known = self.known();
                    self.send(Target::Node(node), Body::Sync(known));
                }
                Effect::NeighborDown(node) => {
                    self.seen.remove(&node);
                    self.broadcast.neighbor_down(&node);
                }
            }
        }
    }

    fn learn(&mut self, records: Vec<Record>, forwarded: Option<&Record>, now: f64) {
        let changed: Vec<Record> = records
            .into_iter()
            .filter_map(|observed| self.table.merge(observed, now))
            .collect();
        self.publish(changed, forwarded, now);
    }

    /// Take the records that changed the table to the other nodes, to the views and to the caller.
    ///
    /// The broadcast skips the record it is already forwarding, which the sender pushed in the first place.
    ///
    /// The views follow the table: a member that is alive is a standby neighbor, and one that is not is no neighbor
    /// at all. Without it the heartbeat would keep watching a member the cluster buried, and would never watch one
    /// it never had as a neighbor, which is how a partitioned node keeps counting the unreachable side as alive.
    fn publish(&mut self, records: Vec<Record>, forwarded: Option<&Record>, now: f64) {
        for record in records {
            if Some(&record) != forwarded {
                let sends = self.broadcast.broadcast(record.clone(), now);
                self.deliver(sends);
            }
            if record.node != self.node {
                if matches!(record.status, Status::Alive | Status::Leaving) {
                    self.views.discover([record.node.clone()]);
                } else {
                    self.seen.remove(&record.node);
                    let effects = self.views.remove(&record.node, false);
                    self.apply(effects, now);
                }
            }
            self.changed = true;
        }
    }

    fn deliver(&mut self, sends: Vec<Pushed<Broadcast>>) {
        for send in sends {
            self.send(Target::Node(send.to), Body::Broadcast(send.message));
        }
    }

    fn send_any(&mut self, targets: &[NodeId], body: &Body) {
        let choices: Vec<&NodeId> = targets.iter().filter(|node| **node != self.node).collect();
        if choices.is_empty() {
            return;
        }
        let chosen = (*self.rolls.pick(&choices)).clone();
        self.send(Target::Node(chosen), body.clone());
    }

    fn send(&mut self, to: Target, body: Body) {
        self.sends.push(Outgoing {
            to,
            message: Message {
                sender: Sender::Member(self.table.me().clone()),
                body,
            },
        });
    }

    /// Every record, the ones that left included: without the tombstones the other side would revive them.
    fn known(&self) -> Vec<Record> {
        self.table.records().cloned().collect()
    }

    fn settle(&mut self) {
        if self.table.me().status == Status::Left && !self.leaving {
            self.removed = true;
        } else if !self.joined && self.members().len() > 1 {
            self.joined = true;
        }
    }
}
