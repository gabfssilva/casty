//! What a node does with what it keeps after the ring or the owner of a key moved. Nothing here talks to anyone.

use std::collections::BTreeSet;

use crate::node::NodeId;
use crate::placement::pinned;

/// A key this node keeps a copy of, and whether that copy carries the active mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kept {
    pub actor: String,
    pub key: String,
    pub active: bool,
}

/// A key this node stopped replicating, and the nodes that replicate it now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handover {
    pub actor: String,
    pub key: String,
    pub replicas: Vec<NodeId>,
}

/// What one node does with what it keeps: the keys to bring back, the ones to end, and the ones to give away.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Sweep {
    pub reattach: Vec<(String, String)>,
    pub end: Vec<(String, String)>,
    pub given: Vec<Handover>,
}

/// Where the keys of each actor type are, as the sweep reads it.
pub trait Placement {
    /// The nodes that keep `key`, the first being the one the ring gives it to.
    fn replicas(&self, actor: &str, key: &str) -> Vec<NodeId>;

    /// The first replica of `key` that is alive or suspect, or nothing when there is none.
    fn owner(&self, actor: &str, key: &str) -> Option<NodeId>;
}

/// Decide from what this node keeps, what runs on it and where the keys are now.
///
/// A key marked active whose owner is this node and that has no activation is brought back: whatever ended the
/// activation elsewhere, the mark is what says the key was running and nobody removed it. An activation whose owner
/// moved ends, because a write from it would be refused by the replicas anyway. A key whose replicas no longer
/// include this node is handed to the ones that have it now, and only then dropped here.
///
/// A pinned key is brought back and ended like any other, its owner being the node at the address it names while that
/// node is up, so a node that leaves ends it. It is never handed over: a process that replaced that node on its address
/// starts the key from `initial`, not from what another one kept.
pub fn sweep(
    kept: &[Kept],
    running: &BTreeSet<(String, String)>,
    node: &NodeId,
    where_: &impl Placement,
) -> Sweep {
    let reattach = kept
        .iter()
        .filter(|item| {
            item.active
                && !running.contains(&(item.actor.clone(), item.key.clone()))
                && where_.owner(&item.actor, &item.key).as_ref() == Some(node)
        })
        .map(|item| (item.actor.clone(), item.key.clone()))
        .collect();
    let end = running
        .iter()
        .filter(|(actor, key)| where_.owner(actor, key).as_ref() != Some(node))
        .cloned()
        .collect();
    let given = kept
        .iter()
        .filter(|item| pinned(&item.key).is_none())
        .filter_map(|item| {
            let replicas = where_.replicas(&item.actor, &item.key);
            if replicas.is_empty() || replicas.contains(node) {
                return None;
            }
            Some(Handover {
                actor: item.actor.clone(),
                key: item.key.clone(),
                replicas,
            })
        })
        .collect();
    Sweep {
        reattach,
        end,
        given,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{Handover, Kept, Placement, Sweep, sweep};
    use crate::node::NodeId;
    use crate::placement::pin;
    use crate::rolls::Rolls;

    /// Where each key is, said outright instead of derived from a ring.
    struct Placed(BTreeMap<(String, String), Vec<NodeId>>);

    impl Placement for Placed {
        fn replicas(&self, actor: &str, key: &str) -> Vec<NodeId> {
            self.0
                .get(&(actor.to_owned(), key.to_owned()))
                .cloned()
                .unwrap_or_default()
        }

        fn owner(&self, actor: &str, key: &str) -> Option<NodeId> {
            self.replicas(actor, key).first().cloned()
        }
    }

    /// `Placed` as it is while a node is leaving: still one of the replicas it was, and the owner of nothing.
    struct Leaving<'a>(&'a Placed, &'a NodeId);

    impl Placement for Leaving<'_> {
        fn replicas(&self, actor: &str, key: &str) -> Vec<NodeId> {
            self.0.replicas(actor, key)
        }

        fn owner(&self, actor: &str, key: &str) -> Option<NodeId> {
            self.replicas(actor, key)
                .into_iter()
                .find(|node| node != self.1)
        }
    }

    fn kept(key: &str, active: bool) -> Kept {
        Kept {
            actor: "account".to_owned(),
            key: key.to_owned(),
            active,
        }
    }

    fn at(key: &str) -> (String, String) {
        ("account".to_owned(), key.to_owned())
    }

    #[test]
    fn it_brings_back_what_is_marked_active_and_owned_here_without_an_activation() {
        let ids = Rolls::seeded(41).nodes(2);
        let placed = Placed(BTreeMap::from([
            (at("a"), vec![ids[0].clone(), ids[1].clone()]),
            (at("b"), vec![ids[0].clone()]),
            (at("c"), vec![ids[1].clone(), ids[0].clone()]),
        ]));

        let done = sweep(
            &[kept("a", true), kept("b", false), kept("c", true)],
            &BTreeSet::new(),
            &ids[0],
            &placed,
        );

        // `a` is marked and owned here; `b` carries no mark; `c` is owned by the other node.
        assert_eq!(done.reattach, vec![at("a")]);
        assert!(done.end.is_empty());
        assert!(done.given.is_empty());
    }

    #[test]
    fn it_ends_an_activation_whose_owner_moved() {
        let ids = Rolls::seeded(42).nodes(2);
        let placed = Placed(BTreeMap::from([
            (at("a"), vec![ids[1].clone(), ids[0].clone()]),
            (at("b"), vec![ids[0].clone()]),
        ]));

        let done = sweep(
            &[kept("a", true), kept("b", true)],
            &BTreeSet::from([at("a"), at("b")]),
            &ids[0],
            &placed,
        );

        assert_eq!(done.end, vec![at("a")]);
        // A key that is already running here is not brought back again.
        assert!(done.reattach.is_empty());
    }

    #[test]
    fn it_leaves_alone_a_key_that_runs_here_and_is_still_owned_here() {
        let ids = Rolls::seeded(44).nodes(2);
        let placed = Placed(BTreeMap::from([(
            at("a"),
            vec![ids[0].clone(), ids[1].clone()],
        )]));

        let done = sweep(
            &[kept("a", true)],
            &BTreeSet::from([at("a")]),
            &ids[0],
            &placed,
        );

        // Marked active and owned here, and already running: neither brought back a second time nor ended.
        assert_eq!(done, Sweep::default());
    }

    #[test]
    fn it_gives_away_what_it_no_longer_replicates_and_keeps_what_nobody_hosts() {
        let ids = Rolls::seeded(43).nodes(3);
        let placed = Placed(BTreeMap::from([
            (at("a"), vec![ids[1].clone(), ids[2].clone()]),
            (at("b"), vec![ids[0].clone()]),
            (at("c"), Vec::new()),
        ]));

        let done = sweep(
            &[kept("a", false), kept("b", false), kept("c", true)],
            &BTreeSet::new(),
            &ids[0],
            &placed,
        );

        assert_eq!(
            done.given,
            vec![Handover {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                replicas: vec![ids[1].clone(), ids[2].clone()],
            }]
        );
        // A type no member hosts is not handed to nobody: the copy stays here until someone has it.
        assert!(done.given.iter().all(|given| given.key != "c"));
    }

    #[test]
    fn a_pinned_key_comes_back_only_on_its_node_and_ends_there_when_the_node_leaves() {
        let ids = Rolls::seeded(45).nodes(2);
        let worker = pin("10.0.0.5:7400", "worker");
        let placed = Placed(BTreeMap::from([(at(&worker), vec![ids[0].clone()])]));

        let home = sweep(&[kept(&worker, true)], &BTreeSet::new(), &ids[0], &placed);
        assert_eq!(home.reattach, vec![at(&worker)]);
        // Any other node leaves a copy it holds alone: it does not own the key, and does not hand it over either.
        let elsewhere = sweep(&[kept(&worker, true)], &BTreeSet::new(), &ids[1], &placed);
        assert_eq!(elsewhere, Sweep::default());

        let leaving = sweep(
            &[kept(&worker, true)],
            &BTreeSet::from([at(&worker)]),
            &ids[0],
            &Leaving(&placed, &ids[0]),
        );
        assert_eq!(
            leaving,
            Sweep {
                end: vec![at(&worker)],
                ..Sweep::default()
            }
        );
    }

    #[test]
    fn a_pinned_key_is_never_handed_over() {
        let ids = Rolls::seeded(46).nodes(3);
        let worker = pin("10.0.0.5:7400", "worker");
        // Another process now advertises the address the key names: it starts the key over instead of taking this copy.
        let placed = Placed(BTreeMap::from([
            (at(&worker), vec![ids[1].clone()]),
            (at("a"), vec![ids[1].clone(), ids[2].clone()]),
        ]));

        let done = sweep(
            &[kept(&worker, true), kept("a", false)],
            &BTreeSet::new(),
            &ids[0],
            &placed,
        );

        assert_eq!(
            done,
            Sweep {
                given: vec![Handover {
                    actor: "account".to_owned(),
                    key: "a".to_owned(),
                    replicas: vec![ids[1].clone(), ids[2].clone()],
                }],
                ..Sweep::default()
            }
        );
    }
}
