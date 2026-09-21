//! Plumtree over the active view.
//!
//! Events go as gossip to eager peers and as an announcement to lazy ones. A duplicate gossip prunes its sender to
//! lazy. An event announced and not received within `graft_after` grafts an announcer back to eager; the announcers
//! are asked in turn until the event arrives. Instants are monotonic seconds passed by the caller.

use std::collections::HashMap;

use super::table::Record;
use crate::node::NodeId;

/// An event forgotten a minute after it was seen.
const FORGET_AFTER: f64 = 60.0;

/// Which event this is: the node that broadcast it, and how many it had broadcast before.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventId {
    pub node: NodeId,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Broadcast {
    Gossip { id: EventId, record: Record },
    IHave(EventId),
    Graft(EventId),
    Prune,
}

/// A message the caller must send to `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Send<M> {
    pub to: NodeId,
    pub message: M,
}

/// The record to apply, on the first sighting of an event, and the messages to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub record: Option<Record>,
    pub sends: Vec<Send<Broadcast>>,
}

#[derive(Debug)]
struct Seen {
    gossip: Broadcast,
    at: f64,
}

#[derive(Debug)]
struct Missing {
    since: f64,
    deadline: f64,
    announcers: Vec<NodeId>,
}

#[derive(Debug)]
pub struct Broadcaster {
    node: NodeId,
    graft_after: f64,
    sequence: u64,
    /// Peers in the order they were learned, each eager or lazy. Small enough that a list is the right shape.
    peers: Vec<(NodeId, bool)>,
    seen: HashMap<EventId, Seen>,
    missing: Vec<(EventId, Missing)>,
}

impl Broadcaster {
    #[must_use]
    pub fn new(node: NodeId, graft_after: f64) -> Self {
        Self {
            node,
            graft_after,
            sequence: 0,
            peers: Vec::new(),
            seen: HashMap::new(),
            missing: Vec::new(),
        }
    }

    pub fn neighbor_up(&mut self, node: NodeId) {
        if !self.peers.iter().any(|(held, _)| *held == node) {
            self.peers.push((node, true));
        }
    }

    pub fn neighbor_down(&mut self, node: &NodeId) {
        self.peers.retain(|(held, _)| held != node);
        for (_, missing) in &mut self.missing {
            missing.announcers.retain(|held| held != node);
        }
    }

    pub fn broadcast(&mut self, record: Record, now: f64) -> Vec<Send<Broadcast>> {
        self.sequence += 1;
        let id = EventId {
            node: self.node.clone(),
            sequence: self.sequence,
        };
        let gossip = Broadcast::Gossip {
            id: id.clone(),
            record,
        };
        self.seen.insert(
            id,
            Seen {
                gossip: gossip.clone(),
                at: now,
            },
        );
        let node = self.node.clone();
        self.push(&gossip, &node)
    }

    pub fn receive(&mut self, sender: &NodeId, message: Broadcast, now: f64) -> Received {
        match message {
            Broadcast::Gossip { id, record } => {
                self.missing.retain(|(held, _)| *held != id);
                if self.seen.contains_key(&id) {
                    self.mark(sender, false);
                    return Received {
                        record: None,
                        sends: vec![Send {
                            to: sender.clone(),
                            message: Broadcast::Prune,
                        }],
                    };
                }
                let gossip = Broadcast::Gossip {
                    id: id.clone(),
                    record: record.clone(),
                };
                self.seen.insert(
                    id,
                    Seen {
                        gossip: gossip.clone(),
                        at: now,
                    },
                );
                self.mark(sender, true);
                Received {
                    record: Some(record),
                    sends: self.push(&gossip, sender),
                }
            }
            Broadcast::IHave(id) => {
                if !self.seen.contains_key(&id) {
                    match self.missing.iter_mut().find(|(held, _)| *held == id) {
                        Some((_, missing)) => {
                            if !missing.announcers.contains(sender) {
                                missing.announcers.push(sender.clone());
                            }
                        }
                        None => self.missing.push((
                            id,
                            Missing {
                                since: now,
                                deadline: now + self.graft_after,
                                announcers: vec![sender.clone()],
                            },
                        )),
                    }
                }
                Received {
                    record: None,
                    sends: Vec::new(),
                }
            }
            Broadcast::Graft(id) => {
                self.mark(sender, true);
                let sends = match self.seen.get(&id) {
                    None => Vec::new(),
                    Some(seen) => vec![Send {
                        to: sender.clone(),
                        message: seen.gossip.clone(),
                    }],
                };
                Received {
                    record: None,
                    sends,
                }
            }
            Broadcast::Prune => {
                self.mark(sender, false);
                Received {
                    record: None,
                    sends: Vec::new(),
                }
            }
        }
    }

    /// Send the grafts that are due, and forget events older than a minute.
    pub fn tick(&mut self, now: f64) -> Vec<Send<Broadcast>> {
        let mut grafts = Vec::new();
        let mut asked: Vec<NodeId> = Vec::new();
        self.missing.retain(|(_, missing)| {
            !missing.announcers.is_empty() && now - missing.since < FORGET_AFTER
        });
        for (event, missing) in &mut self.missing {
            if missing.deadline <= now {
                let announcer = missing.announcers.remove(0);
                missing.announcers.push(announcer.clone());
                missing.deadline = now + self.graft_after;
                asked.push(announcer.clone());
                grafts.push(Send {
                    to: announcer,
                    message: Broadcast::Graft(event.clone()),
                });
            }
        }
        for announcer in &asked {
            self.mark(announcer, true);
        }
        self.seen.retain(|_, seen| now - seen.at < FORGET_AFTER);
        grafts
    }

    fn push(&self, gossip: &Broadcast, exclude: &NodeId) -> Vec<Send<Broadcast>> {
        let Broadcast::Gossip { id, .. } = gossip else {
            unreachable!("only a gossip is pushed");
        };
        self.peers
            .iter()
            .filter(|(peer, _)| peer != exclude)
            .map(|(peer, eager)| Send {
                to: peer.clone(),
                message: if *eager {
                    gossip.clone()
                } else {
                    Broadcast::IHave(id.clone())
                },
            })
            .collect()
    }

    fn mark(&mut self, peer: &NodeId, eager: bool) {
        if let Some((_, held)) = self.peers.iter_mut().find(|(held, _)| held == peer) {
            *held = eager;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap, VecDeque};

    use super::super::table::{Record, Status};
    use super::{Broadcast, Broadcaster, EventId, Send};
    use crate::node::NodeId;
    use crate::rolls::Rolls;

    const GRAFT_AFTER: f64 = 0.5;

    fn types() -> BTreeSet<String> {
        BTreeSet::from(["account".to_owned()])
    }

    /// The broadcaster of every node, linked by a random graph that loses at most one gossip per event.
    ///
    /// Two losses can cut a node off from every announcement of an event, and only anti-entropy repairs that. The
    /// graph has no bridge, so a node that misses the lost gossip always has another neighbor that announces it.
    struct Network {
        ids: Vec<NodeId>,
        delivered: Vec<Vec<Record>>,
        lost: BTreeSet<EventId>,
        rolls: Rolls,
        broadcasters: Vec<Broadcaster>,
        links: HashMap<(usize, usize), VecDeque<Broadcast>>,
        now: f64,
    }

    impl Network {
        fn new(size: usize, mut rolls: Rolls) -> Self {
            let ids = rolls.nodes(size);
            let mut broadcasters: Vec<Broadcaster> = ids
                .iter()
                .map(|node| Broadcaster::new(node.clone(), GRAFT_AFTER))
                .collect();
            for (a, b) in bridgeless(size, &mut rolls) {
                broadcasters[a].neighbor_up(ids[b].clone());
                broadcasters[b].neighbor_up(ids[a].clone());
            }
            Self {
                delivered: vec![Vec::new(); size],
                ids,
                lost: BTreeSet::new(),
                rolls,
                broadcasters,
                links: HashMap::new(),
                now: 0.0,
            }
        }

        /// Broadcast every event, interleaved with the messages in flight, until nothing is left to deliver.
        fn run(&mut self, events: &[Record]) {
            let mut pending: Vec<Record> = events.to_vec();
            loop {
                let mut busy: Vec<(usize, usize)> = self
                    .links
                    .iter()
                    .filter(|(_, queue)| !queue.is_empty())
                    .map(|(link, _)| *link)
                    .collect();
                busy.sort_unstable();
                if !pending.is_empty() && (busy.is_empty() || self.rolls.chance(0.05)) {
                    let event = pending.pop().expect("something pending");
                    self.broadcast(&event);
                } else if busy.is_empty() {
                    if !self.graft() {
                        return;
                    }
                } else {
                    let link = *self.rolls.pick(&busy);
                    self.deliver(link);
                }
            }
        }

        fn broadcast(&mut self, event: &Record) {
            let origin = self.at(&event.node);
            let sends = self.broadcasters[origin].broadcast(event.clone(), self.now);
            self.send(origin, sends);
        }

        fn deliver(&mut self, link: (usize, usize)) {
            let (sender, receiver) = link;
            let message = self
                .links
                .get_mut(&link)
                .and_then(VecDeque::pop_front)
                .expect("a queued message");
            if let Broadcast::Gossip { id, .. } = &message
                && !self.lost.contains(id)
                && self.rolls.chance(0.2)
            {
                self.lost.insert(id.clone());
                return;
            }
            let from = self.ids[sender].clone();
            let heard = self.broadcasters[receiver].receive(&from, message, self.now);
            if let Some(record) = heard.record {
                self.delivered[receiver].push(record);
            }
            self.send(receiver, heard.sends);
        }

        /// Advance the clock past the graft deadline, send what is due, and say whether anything was sent.
        fn graft(&mut self) -> bool {
            self.now += GRAFT_AFTER;
            let grafts: Vec<Vec<Send<Broadcast>>> = self
                .broadcasters
                .iter_mut()
                .map(|broadcaster| broadcaster.tick(self.now))
                .collect();
            let mut anything = false;
            for (sender, sends) in grafts.into_iter().enumerate() {
                anything = anything || !sends.is_empty();
                self.send(sender, sends);
            }
            anything
        }

        fn send(&mut self, sender: usize, sends: Vec<Send<Broadcast>>) {
            for command in sends {
                let to = self.at(&command.to);
                self.links
                    .entry((sender, to))
                    .or_default()
                    .push_back(command.message);
            }
        }

        fn at(&self, node: &NodeId) -> usize {
            self.ids
                .iter()
                .position(|held| held == node)
                .expect("a node of this network")
        }
    }

    /// A random connected graph without bridges: a cycle through every node, plus random chords.
    fn bridgeless(size: usize, rolls: &mut Rolls) -> BTreeSet<(usize, usize)> {
        let cycle = rolls.shuffled(&(0..size).collect::<Vec<_>>());
        let mut edges: BTreeSet<(usize, usize)> = cycle
            .iter()
            .zip(cycle.iter().cycle().skip(1))
            .map(|(a, b)| (*a.min(b), *a.max(b)))
            .collect();
        while edges.len() < 2 * size {
            let (a, b) = (rolls.upto(size), rolls.upto(size));
            if a != b {
                edges.insert((a.min(b), a.max(b)));
            }
        }
        edges
    }

    #[test]
    fn every_event_reaches_every_node_exactly_once_though_gossip_is_lost() {
        let mut network = Network::new(30, Rolls::seeded(6));
        let events: Vec<Record> = (0..40)
            .map(|incarnation| Record {
                node: network.rolls.pick(&network.ids.clone()).clone(),
                incarnation,
                status: Status::Alive,
                types: types(),
            })
            .collect();

        network.run(&events);

        assert!(
            network.lost.len() > events.len() / 2,
            "only {} gossips were lost, so the graft was barely exercised",
            network.lost.len()
        );
        for (at, node) in network.ids.iter().enumerate() {
            let mut held = network.delivered[at].clone();
            let mut expected: Vec<Record> = events
                .iter()
                .filter(|event| event.node != *node)
                .cloned()
                .collect();
            let key = |record: &Record| (record.node.incarnation, record.incarnation);
            held.sort_by_key(key);
            expected.sort_by_key(key);
            assert_eq!(
                held, expected,
                "node {at} did not get every event exactly once"
            );
        }
    }

    #[test]
    fn a_duplicate_gossip_prunes_the_peer_that_sent_it() {
        let ids = Rolls::seeded(3).nodes(3);
        let mut broadcaster = Broadcaster::new(ids[0].clone(), GRAFT_AFTER);
        broadcaster.neighbor_up(ids[1].clone());
        broadcaster.neighbor_up(ids[2].clone());
        let event = Broadcast::Gossip {
            id: EventId {
                node: ids[1].clone(),
                sequence: 1,
            },
            record: Record {
                node: ids[1].clone(),
                incarnation: 0,
                status: Status::Alive,
                types: types(),
            },
        };

        let first = broadcaster.receive(&ids[1], event.clone(), 0.0);
        let again = broadcaster.receive(&ids[2], event, 0.0);

        assert!(
            first.record.is_some(),
            "the first sighting is the one that is applied"
        );
        assert_eq!(first.sends.len(), 1, "it is pushed to the other peer");
        assert!(again.record.is_none(), "the same event was applied twice");
        assert_eq!(
            again.sends,
            vec![Send {
                to: ids[2].clone(),
                message: Broadcast::Prune
            }]
        );
    }

    #[test]
    fn an_announcement_that_is_not_followed_by_the_event_grafts_its_announcer() {
        let ids = Rolls::seeded(4).nodes(2);
        let mut broadcaster = Broadcaster::new(ids[0].clone(), GRAFT_AFTER);
        broadcaster.neighbor_up(ids[1].clone());
        let id = EventId {
            node: ids[1].clone(),
            sequence: 9,
        };

        broadcaster.receive(&ids[1], Broadcast::IHave(id.clone()), 0.0);

        assert!(
            broadcaster.tick(0.1).is_empty(),
            "it grafted before the deadline"
        );
        assert_eq!(
            broadcaster.tick(0.6),
            vec![Send {
                to: ids[1].clone(),
                message: Broadcast::Graft(id)
            }]
        );
    }
}
