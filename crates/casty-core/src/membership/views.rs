//! `HyParView`: the active view of neighbors and the passive view of standby nodes.
//!
//! Links are logical, so both sides add each other: a neighbor request is accepted with a reply, and a seed answers
//! a join with an accepting reply, because the joining node dialed an address and does not know the seed's identity.

use crate::node::NodeId;
use crate::rolls::Rolls;

const SHUFFLE_ACTIVE: usize = 3;
const SHUFFLE_PASSIVE: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum View {
    Join,
    ForwardJoin {
        joiner: NodeId,
        ttl: u32,
    },
    /// Request to enter the receiver's active view. With `priority`, the sender has no neighbor and is not refused.
    Neighbor {
        priority: bool,
    },
    NeighborReply {
        accepted: bool,
    },
    Disconnect,
    Shuffle {
        origin: NodeId,
        sample: Vec<NodeId>,
        ttl: u32,
    },
    ShuffleReply {
        sample: Vec<NodeId>,
    },
}

/// What the caller must do after a message: send something, or note a change of the active view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Send { to: NodeId, message: View },
    NeighborUp(NodeId),
    NeighborDown(NodeId),
}

/// How large the views are and how far a walk goes.
#[derive(Debug, Clone, Copy)]
pub struct Overlay {
    pub active: usize,
    pub passive: usize,
    pub join_walk: u32,
    pub passive_walk: u32,
}

impl Default for Overlay {
    fn default() -> Self {
        Self {
            active: 5,
            passive: 30,
            join_walk: 6,
            passive_walk: 3,
        }
    }
}

#[derive(Debug)]
pub struct Views {
    node: NodeId,
    overlay: Overlay,
    rolls: Rolls,
    active: Vec<NodeId>,
    passive: Vec<NodeId>,
}

impl Views {
    #[must_use]
    pub fn new(node: NodeId, overlay: Overlay, rolls: Rolls) -> Self {
        Self {
            node,
            overlay,
            rolls,
            active: Vec::new(),
            passive: Vec::new(),
        }
    }

    #[must_use]
    pub fn active(&self) -> &[NodeId] {
        &self.active
    }

    pub fn receive(&mut self, sender: &NodeId, message: View) -> Vec<Effect> {
        match message {
            View::Join => self.joined(sender),
            View::ForwardJoin { joiner, ttl } => self.forwarded(sender, joiner, ttl),
            View::Neighbor { priority } => self.asked(sender, priority),
            View::NeighborReply { accepted } => {
                if accepted {
                    self.add_active(sender)
                } else {
                    Vec::new()
                }
            }
            View::Disconnect => self.remove(sender, true),
            View::Shuffle {
                origin,
                sample,
                ttl,
            } => self.shuffled(sender, origin, sample, ttl),
            View::ShuffleReply { sample } => {
                for node in &sample {
                    self.add_passive(node);
                }
                Vec::new()
            }
        }
    }

    /// A node that dialed a seed: it enters the view, hears that it was taken, and is walked to the others.
    fn joined(&mut self, sender: &NodeId) -> Vec<Effect> {
        let forwards: Vec<Effect> = self
            .active
            .iter()
            .filter(|node| *node != sender)
            .map(|node| Effect::Send {
                to: node.clone(),
                message: View::ForwardJoin {
                    joiner: sender.clone(),
                    ttl: self.overlay.join_walk,
                },
            })
            .collect();
        let mut effects = self.add_active(sender);
        effects.push(Effect::Send {
            to: sender.clone(),
            message: View::NeighborReply { accepted: true },
        });
        effects.extend(forwards);
        effects
    }

    /// One hop of the walk a join takes: at the end of it, the walker asks the joiner to be its neighbor.
    fn forwarded(&mut self, sender: &NodeId, joiner: NodeId, ttl: u32) -> Vec<Effect> {
        if joiner == self.node || self.active.contains(&joiner) {
            return Vec::new();
        }
        let others: Vec<NodeId> = self
            .active
            .iter()
            .filter(|node| **node != *sender && **node != joiner)
            .cloned()
            .collect();
        if ttl == 0 || others.is_empty() {
            return vec![Effect::Send {
                to: joiner,
                message: View::Neighbor { priority: false },
            }];
        }
        if ttl == self.overlay.passive_walk {
            self.add_passive(&joiner);
        }
        let next = self.rolls.pick(&others).clone();
        vec![Effect::Send {
            to: next,
            message: View::ForwardJoin {
                joiner,
                ttl: ttl - 1,
            },
        }]
    }

    fn asked(&mut self, sender: &NodeId, priority: bool) -> Vec<Effect> {
        if !priority && !self.active.contains(sender) && self.active.len() >= self.overlay.active {
            return vec![Effect::Send {
                to: sender.clone(),
                message: View::NeighborReply { accepted: false },
            }];
        }
        let mut effects = self.add_active(sender);
        effects.push(Effect::Send {
            to: sender.clone(),
            message: View::NeighborReply { accepted: true },
        });
        effects
    }

    /// One hop of a shuffle: at the end of the walk, the sample is answered with one of this node's own.
    fn shuffled(
        &mut self,
        sender: &NodeId,
        origin: NodeId,
        sample: Vec<NodeId>,
        ttl: u32,
    ) -> Vec<Effect> {
        let others: Vec<NodeId> = self
            .active
            .iter()
            .filter(|node| **node != *sender && **node != origin)
            .cloned()
            .collect();
        if ttl > 0 && !others.is_empty() {
            let next = self.rolls.pick(&others).clone();
            return vec![Effect::Send {
                to: next,
                message: View::Shuffle {
                    origin,
                    sample,
                    ttl: ttl - 1,
                },
            }];
        }
        let reply = View::ShuffleReply {
            sample: self.sample(&self.passive.clone(), sample.len()),
        };
        self.add_passive(&origin);
        for node in &sample {
            self.add_passive(node);
        }
        vec![Effect::Send {
            to: origin,
            message: reply,
        }]
    }

    /// Take `node` out of the views, keeping it in the passive view with `to_passive`, and fill the vacancy.
    pub fn remove(&mut self, node: &NodeId, to_passive: bool) -> Vec<Effect> {
        if !to_passive {
            self.passive.retain(|held| held != node);
        }
        if !self.active.contains(node) {
            return Vec::new();
        }
        self.active.retain(|held| held != node);
        if to_passive {
            self.add_passive(node);
        }
        let mut effects = vec![Effect::NeighborDown(node.clone())];
        effects.extend(self.promote());
        effects
    }

    /// Ask passive nodes to become neighbors, one for each vacancy of the active view.
    ///
    /// Without any neighbor, the requests have priority and are not refused.
    pub fn promote(&mut self) -> Vec<Effect> {
        let Some(vacancies) = self
            .overlay
            .active
            .checked_sub(self.active.len())
            .filter(|held| *held > 0)
        else {
            return Vec::new();
        };
        let priority = self.active.is_empty();
        self.sample(&self.passive.clone(), vacancies)
            .into_iter()
            .map(|node| Effect::Send {
                to: node,
                message: View::Neighbor { priority },
            })
            .collect()
    }

    /// Keep `nodes` as standby neighbors, which is where the members learned by gossip enter the views.
    pub fn discover(&mut self, nodes: impl IntoIterator<Item = NodeId>) {
        for node in nodes {
            self.add_passive(&node);
        }
    }

    /// Send a sample of both views on a walk of `passive_walk` hops, to refresh passive views.
    pub fn shuffle(&mut self) -> Vec<Effect> {
        if self.active.is_empty() {
            return Vec::new();
        }
        let mut sample = self.sample(&self.active.clone(), SHUFFLE_ACTIVE);
        sample.extend(self.sample(&self.passive.clone(), SHUFFLE_PASSIVE));
        let target = self.rolls.pick(&self.active.clone()).clone();
        vec![Effect::Send {
            to: target,
            message: View::Shuffle {
                origin: self.node.clone(),
                sample,
                ttl: self.overlay.passive_walk,
            },
        }]
    }

    fn add_active(&mut self, node: &NodeId) -> Vec<Effect> {
        if *node == self.node || self.active.contains(node) {
            return Vec::new();
        }
        let mut effects = Vec::new();
        if self.active.len() >= self.overlay.active {
            let evicted = self.rolls.pick(&self.active.clone()).clone();
            self.active.retain(|held| *held != evicted);
            self.add_passive(&evicted);
            effects.push(Effect::Send {
                to: evicted.clone(),
                message: View::Disconnect,
            });
            effects.push(Effect::NeighborDown(evicted));
        }
        self.passive.retain(|held| held != node);
        self.active.push(node.clone());
        effects.push(Effect::NeighborUp(node.clone()));
        effects
    }

    fn add_passive(&mut self, node: &NodeId) {
        if *node == self.node || self.active.contains(node) || self.passive.contains(node) {
            return;
        }
        if self.passive.len() >= self.overlay.passive {
            let dropped = self.rolls.pick(&self.passive.clone()).clone();
            self.passive.retain(|held| *held != dropped);
        }
        self.passive.push(node.clone());
    }

    fn sample(&mut self, nodes: &[NodeId], count: usize) -> Vec<NodeId> {
        let mut shuffled = self.rolls.shuffled(nodes);
        shuffled.truncate(count.min(nodes.len()));
        shuffled
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};

    use super::{Effect, Overlay, View, Views};
    use crate::node::NodeId;
    use crate::rolls::Rolls;

    /// Every node's views, with the messages between them delivered in the order they were sent.
    struct Overlayed {
        ids: Vec<NodeId>,
        views: Vec<Views>,
        pending: VecDeque<(usize, usize, View)>,
    }

    impl Overlayed {
        fn new(size: usize, overlay: Overlay, seed: u64) -> Self {
            let mut rolls = Rolls::seeded(seed);
            let ids = rolls.nodes(size);
            let views = ids
                .iter()
                .enumerate()
                .map(|(at, node)| {
                    Views::new(
                        node.clone(),
                        overlay,
                        #[allow(clippy::cast_possible_truncation)]
                        Rolls::seeded(seed.wrapping_add(at as u64 + 1)),
                    )
                })
                .collect();
            Self {
                ids,
                views,
                pending: VecDeque::new(),
            }
        }

        fn at(&self, node: &NodeId) -> usize {
            self.ids
                .iter()
                .position(|held| held == node)
                .expect("a node of this overlay")
        }

        fn apply(&mut self, from: usize, effects: Vec<Effect>) {
            for effect in effects {
                if let Effect::Send { to, message } = effect {
                    let to = self.at(&to);
                    self.pending.push_back((from, to, message));
                }
            }
        }

        /// Deliver everything in flight, up to a bound that a protocol that settles never reaches.
        fn settle(&mut self) {
            for _ in 0..200_000 {
                let Some((from, to, message)) = self.pending.pop_front() else {
                    return;
                };
                let sender = self.ids[from].clone();
                let effects = self.views[to].receive(&sender, message);
                self.apply(to, effects);
            }
            panic!("the overlay never settled");
        }

        /// Whether every node can be reached from the first one through the active views.
        fn connected(&self) -> bool {
            let mut seen = BTreeSet::from([0_usize]);
            let mut pending = vec![0_usize];
            while let Some(at) = pending.pop() {
                for neighbor in self.views[at].active() {
                    let next = self.at(neighbor);
                    if seen.insert(next) {
                        pending.push(next);
                    }
                }
            }
            seen.len() == self.ids.len()
        }
    }

    #[test]
    fn nodes_that_join_through_one_seed_end_up_in_one_overlay() {
        for seed in 1..=5_u64 {
            let overlay = Overlay::default();
            let mut world = Overlayed::new(24, overlay, seed);
            for joining in 1..world.ids.len() {
                let seeded = world.ids[0].clone();
                let effects = world.views[0].receive(&world.ids[joining].clone(), View::Join);
                world.apply(0, effects);
                let _ = seeded;
                world.settle();
            }

            for (at, views) in world.views.iter().enumerate() {
                assert!(
                    !views.active().is_empty(),
                    "seed {seed}: node {at} has no neighbor"
                );
                assert!(
                    views.active().len() <= overlay.active,
                    "seed {seed}: node {at} holds more than the active view allows"
                );
            }
            assert!(world.connected(), "seed {seed}: the overlay is in pieces");
        }
    }

    #[test]
    fn a_neighbor_that_goes_away_is_replaced_from_the_passive_view() {
        let overlay = Overlay {
            active: 2,
            ..Overlay::default()
        };
        let mut rolls = Rolls::seeded(11);
        let ids = rolls.nodes(6);
        let mut views = Views::new(ids[0].clone(), overlay, Rolls::seeded(12));
        views.discover(ids[1..].iter().cloned());
        let filled = views.promote();

        assert_eq!(filled.len(), 2, "it asked for one neighbor per vacancy");
        for effect in filled {
            let Effect::Send { to, .. } = effect else {
                panic!("a promotion is a request");
            };
            views.receive(&to, View::NeighborReply { accepted: true });
        }
        assert_eq!(views.active().len(), 2);

        let gone = views.active()[0].clone();
        let effects = views.remove(&gone, true);

        assert!(effects.contains(&Effect::NeighborDown(gone.clone())));
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                Effect::Send {
                    message: View::Neighbor { .. },
                    ..
                }
            )),
            "the vacancy was not filled"
        );
        assert!(!views.active().contains(&gone));
    }

    #[test]
    fn a_full_active_view_refuses_a_request_without_priority_and_takes_one_with_it() {
        let overlay = Overlay {
            active: 1,
            ..Overlay::default()
        };
        let mut rolls = Rolls::seeded(13);
        let ids = rolls.nodes(3);
        let mut views = Views::new(ids[0].clone(), overlay, Rolls::seeded(14));
        views.receive(&ids[1], View::Neighbor { priority: false });
        assert_eq!(views.active(), &ids[1..2]);

        let refused = views.receive(&ids[2], View::Neighbor { priority: false });
        assert_eq!(
            refused,
            vec![Effect::Send {
                to: ids[2].clone(),
                message: View::NeighborReply { accepted: false }
            }]
        );

        // With priority the request is never refused: the asker has no neighbor at all.
        let taken = views.receive(&ids[2], View::Neighbor { priority: true });
        assert!(taken.contains(&Effect::NeighborUp(ids[2].clone())));
        assert!(
            taken
                .iter()
                .any(|effect| matches!(effect, Effect::NeighborDown(_)))
        );
        assert_eq!(views.active(), &ids[2..3]);
    }
}
