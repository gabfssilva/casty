//! Where the keys of each actor type are, and the steps a node is still walking to get there.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casty_core::handoff::ranges::gained;
use casty_core::membership::table::Status;
use casty_core::node::NodeId;
use casty_core::placement::{Range, Ring, chain, pinned, token};

use crate::membership::service::Member;

/// Tokens per node on the ring of every type.
pub const VNODES: usize = 128;

/// A ring one member away from `previous`, and the token ranges it makes this node a replica of.
///
/// Only the ranges hold: a key outside them goes on to the ring the table asks for, because this node takes nothing
/// over by letting it through. A key inside them stays on `previous` until the replicas it had there answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub actor: String,
    pub previous: Ring,
    pub ring: Ring,
    pub ranges: Vec<Range>,
}

/// The ranges this node is filling from the replicas that had them.
pub trait Transfers {
    /// Whether `key` is in a range of `step` that is still arriving, so that this node holds it back.
    fn receiving(&self, step: &Step, key: &str) -> bool;

    /// The steps whose ranges have all arrived, taken out so that the placement drops them.
    fn filled(&mut self) -> Vec<Step>;
}

/// No transfer at all: the steps it is given are filled at once, which is what a node alone does.
#[derive(Debug, Default)]
pub struct Direct(Vec<Step>);

impl Transfers for Direct {
    fn receiving(&self, _step: &Step, _key: &str) -> bool {
        false
    }

    fn filled(&mut self) -> Vec<Step> {
        core::mem::take(&mut self.0)
    }
}

/// How many replicas each type asks for, as this process knows it.
///
/// A name the process does not have is a type of another version of the code. Routing does not need the count for
/// it: the node that receives the message answers that it is not the owner unless it is, and there the count is
/// known.
#[derive(Debug, Default)]
pub struct Counts(HashMap<String, Option<usize>>);

impl Counts {
    pub fn learn(&mut self, actor: &str, replicas: usize) {
        self.0.insert(actor.to_owned(), Some(replicas));
    }

    pub fn give_up(&mut self, actor: &str) {
        self.0.entry(actor.to_owned()).or_insert(None);
    }

    #[must_use]
    pub fn known(&self) -> BTreeSet<String> {
        self.0.keys().cloned().collect()
    }

    #[must_use]
    pub fn met(&self, actor: &str) -> bool {
        self.0.contains_key(actor)
    }

    /// Whether the replica count of `actor` is known, which a type this process gave up on has no answer for.
    #[must_use]
    pub fn counted(&self, actor: &str) -> bool {
        matches!(self.0.get(actor), Some(Some(_)))
    }

    /// How many replicas a key of `actor` has, never more than the nodes of the ring.
    #[must_use]
    pub fn of(&self, actor: &str, ring: &Ring) -> usize {
        match self.0.get(actor) {
            Some(Some(replicas)) => (*replicas).min(ring.nodes().len()),
            _ => ring.nodes().len(),
        }
    }
}

/// One ring for the whole cluster, built from the member table, and the steps each type is still walking.
///
/// The ring has every member that has not left, the dead ones included: a ring that shrank when a node died would
/// let each side of a partition compute replicas of its own and confirm writes there. Every member is in it for
/// every type, because every member runs the same code.
///
/// A table that changed more than one member is not answered from at once. The node walks the chain of rings between
/// the one it answered from and the one the table asks for, one member per step, and keeps the steps that make it a
/// replica of ranges it did not have. A key of such a range stays on the ring before that step until the replicas it
/// had there handed it over; every other key goes straight to the ring of the table.
///
/// Without a node of its own the steps are skipped, which is what a client does: it keeps no range and only needs to
/// know where to send.
#[derive(Debug)]
pub struct Placement {
    node: Option<NodeId>,
    members: BTreeMap<NodeId, Member>,
    ring: Option<Ring>,
    entered: bool,
    walked: HashMap<String, Ring>,
    steps: HashMap<String, Vec<Step>>,
}

impl Placement {
    #[must_use]
    pub fn new(node: Option<NodeId>) -> Self {
        Self {
            node,
            members: BTreeMap::new(),
            ring: None,
            entered: false,
            walked: HashMap::new(),
            steps: HashMap::new(),
        }
    }

    #[must_use]
    pub fn ring(&self) -> Option<&Ring> {
        self.ring.as_ref()
    }

    /// The nodes that keep `key`, the first being the one the ring gives it to.
    ///
    /// A pinned key is kept by the member advertised at the address it names and by no other: neither the ring nor a
    /// step being walked moves it. With no such member in the table, it is kept nowhere.
    #[must_use]
    pub fn replicas(
        &self,
        actor: &str,
        key: &str,
        counts: &Counts,
        transfers: &impl Transfers,
    ) -> Vec<NodeId> {
        if let Some(address) = pinned(key) {
            return self.advertised(address).into_iter().collect();
        }
        let Some(mut ring) = self.ring.as_ref() else {
            return Vec::new();
        };
        for step in self.steps.get(actor).into_iter().flatten() {
            if transfers.receiving(step, key) {
                ring = &step.previous;
                break;
            }
        }
        ring.replicas(token(actor, key), counts.of(actor, ring))
    }

    /// The first replica the table still has and counts as up.
    ///
    /// A key held on the ring before a step is answered from that ring, which may name a node the table has since
    /// dropped. Such a node keeps nothing and answers nothing, so it is no more an owner than a dead one.
    #[must_use]
    pub fn owner(
        &self,
        actor: &str,
        key: &str,
        counts: &Counts,
        transfers: &impl Transfers,
    ) -> Option<NodeId> {
        self.replicas(actor, key, counts, transfers)
            .into_iter()
            .find(|node| {
                self.members
                    .get(node)
                    .is_some_and(|member| up(member.status))
            })
    }

    /// The member advertised at `address`, the one most likely to answer when there are several.
    ///
    /// A node restarted on its address is another incarnation, and the older one can stay in the table beside it until
    /// the join that replaced it has reached every node. Two that are equally up are told apart by their identity,
    /// which every node orders the same way.
    fn advertised(&self, address: &str) -> Option<NodeId> {
        self.members
            .values()
            .filter(|member| member.node.address.as_deref() == Some(address))
            .min_by_key(|member| (!up(member.status), member.status))
            .map(|member| member.node.clone())
    }

    /// Take the member table: the ring when its nodes changed, and the steps each known type has to walk.
    ///
    /// Most changes to the table say nothing about which nodes are in the cluster, so a ring whose nodes did not
    /// change is kept as it is.
    ///
    /// What comes back is the steps this node gained ranges in, for its transfers to start filling. Each one holds its
    /// ranges until `settle` hears that it is filled.
    pub fn update(&mut self, members: &[Member], counts: &Counts) -> Vec<Step> {
        self.members = members
            .iter()
            .map(|member| (member.node.clone(), member.clone()))
            .collect();
        let nodes: BTreeSet<NodeId> = self.members.keys().cloned().collect();
        let same = self
            .ring
            .as_ref()
            .is_some_and(|ring| ring.nodes().iter().cloned().collect::<BTreeSet<_>>() == nodes);
        if !same {
            self.ring = if nodes.is_empty() {
                None
            } else {
                Some(Ring::build(nodes.iter().cloned(), VNODES))
            };
        }
        let mut taken = Vec::new();
        for name in counts.known() {
            taken.extend(self.advance(&name, counts));
        }
        self.entered = self.entered || nodes.len() > 1;
        taken
    }

    /// Put a type this node just met on the ring it answers from, as it is.
    ///
    /// A node that is already in the cluster was in the ring for every write of the type, whether it knew the type
    /// or not, so there is no range of it to fill: a write it missed is one the quorums already account for. What a
    /// change of the ring takes away from a node reaches the ones that replicate it now through the handover, which
    /// names the type as it arrives. The node that is entering is the other case, and `update` walks it in.
    pub fn learned(&mut self, actor: &str) {
        if let Some(ring) = &self.ring
            && self.entered
        {
            self.walked
                .entry(actor.to_owned())
                .or_insert_with(|| ring.clone());
        }
    }

    /// Drop the steps whose ranges have arrived, which is what lets their keys move on.
    pub fn settle(&mut self, transfers: &mut impl Transfers) {
        for step in transfers.filled() {
            if let Some(steps) = self.steps.get_mut(&step.actor) {
                steps.retain(|held| *held != step);
            }
        }
    }

    /// Keep the steps between the ring this node answered from and the one the table asks for.
    ///
    /// A step this node gains nothing in is walked through: it takes nothing over there, so nothing has to arrive
    /// first. The previous ring of a step is the one right before it, which is where its ranges are pulled from.
    fn advance(&mut self, actor: &str, counts: &Counts) -> Vec<Step> {
        let Some(target) = self.ring.clone() else {
            return Vec::new();
        };
        let steps = self.steps.entry(actor.to_owned()).or_default();
        let held = steps
            .last()
            .map(|step| step.ring.clone())
            .or_else(|| self.walked.get(actor).cloned());
        let Some(node) = self.node.clone() else {
            self.walked.insert(actor.to_owned(), target);
            return Vec::new();
        };
        // A base that holds nobody but this node is a node that was in no ring with anyone: it is the one entering,
        // and the ring without it is what came before.
        let mut base = match held {
            Some(base) if base.nodes().iter().any(|other| *other != node) => base,
            _ => {
                if target.nodes().contains(&node) {
                    target.applied(&node, VNODES)
                } else {
                    target.clone()
                }
            }
        };
        let mut taken = Vec::new();
        for ring in chain(&base, &target, VNODES) {
            let ranges = gained(&base, &ring, &node, counts.of(actor, &ring));
            if !ranges.is_empty() {
                let step = Step {
                    actor: actor.to_owned(),
                    previous: base.clone(),
                    ring: ring.clone(),
                    ranges,
                };
                taken.push(step);
            }
            base = ring;
        }
        self.steps
            .entry(actor.to_owned())
            .or_default()
            .extend(taken.iter().cloned());
        self.walked.insert(actor.to_owned(), base);
        taken
    }
}

/// Whether a member can own a key: it answers, and it is not on its way out.
fn up(status: Status) -> bool {
    matches!(status, Status::Alive | Status::Suspect)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use casty_core::membership::table::Status;
    use casty_core::node::NodeId;
    use casty_core::placement::{Ring, pin, token};
    use casty_core::rolls::Rolls;

    use super::{Counts, Direct, Placement, Step, Transfers, VNODES};
    use crate::membership::service::Member;

    const ACTOR: &str = "tests.app:account";
    const REPLICAS: usize = 3;

    fn member(node: &NodeId, status: Status) -> Member {
        Member {
            node: node.clone(),
            status,
            types: BTreeSet::from([ACTOR.to_owned()]),
        }
    }

    fn counts() -> Counts {
        let mut counts = Counts::default();
        counts.learn(ACTOR, REPLICAS);
        counts
    }

    fn alive(nodes: &[NodeId]) -> Vec<Member> {
        nodes
            .iter()
            .map(|node| member(node, Status::Alive))
            .collect()
    }

    /// `name` pinned to the address `node` advertises.
    fn pinned_to(node: &NodeId, name: &str) -> String {
        pin(
            node.address
                .as_deref()
                .expect("a member advertises an address"),
            name,
        )
    }

    fn keys() -> Vec<String> {
        (0..200).map(|index| format!("key-{index}")).collect()
    }

    /// Transfers that never finish, so that every step they are given keeps holding its ranges.
    #[derive(Debug, Default)]
    struct Waiting(Vec<Step>);

    impl Transfers for Waiting {
        fn receiving(&self, step: &Step, key: &str) -> bool {
            let point = token(&step.actor, key);
            self.0.contains(step) && step.ranges.iter().any(|span| span.holds(point))
        }

        fn filled(&mut self) -> Vec<Step> {
            Vec::new()
        }
    }

    #[test]
    fn a_settled_cluster_places_a_key_where_its_ring_does() {
        let ids = Rolls::seeded(51).nodes(5);
        let members: Vec<Member> = ids.iter().map(|node| member(node, Status::Alive)).collect();
        let mut placement = Placement::new(Some(ids[0].clone()));
        let mut transfers = Direct(placement.update(&members, &counts()));
        placement.settle(&mut transfers);
        let ring = Ring::build(ids.clone(), VNODES);

        for key in keys() {
            let expected = ring.replicas(token(ACTOR, &key), REPLICAS);
            assert_eq!(
                placement.replicas(ACTOR, &key, &counts(), &transfers),
                expected
            );
            assert_eq!(
                placement.owner(ACTOR, &key, &counts(), &transfers),
                expected.first().cloned()
            );
        }
    }

    #[test]
    fn an_owner_is_never_a_member_the_table_gave_up_on() {
        let ids = Rolls::seeded(52).nodes(4);
        let mut members: Vec<Member> = ids.iter().map(|node| member(node, Status::Alive)).collect();
        let mut placement = Placement::new(Some(ids[0].clone()));
        let transfers = Direct::default();
        placement.update(&members, &counts());
        let owned: Vec<String> = keys()
            .into_iter()
            .filter(|key| {
                placement.owner(ACTOR, key, &counts(), &transfers) == Some(ids[1].clone())
            })
            .collect();
        assert!(!owned.is_empty(), "the node owned nothing to begin with");

        members[1].status = Status::Dead;
        placement.update(&members, &counts());

        for key in owned {
            let answers = placement.owner(ACTOR, &key, &counts(), &transfers);
            assert_ne!(
                answers,
                Some(ids[1].clone()),
                "a dead node was still an owner"
            );
            // The ring did not change, so the key is still on the same replicas; the next one up answers for it.
            assert!(
                placement
                    .replicas(ACTOR, &key, &counts(), &transfers)
                    .contains(&ids[1])
            );
        }
    }

    #[test]
    fn a_node_that_enters_holds_the_keys_of_the_ranges_it_gained_on_the_ring_it_came_from() {
        let ids = Rolls::seeded(53).nodes(4);
        let entering = &ids[3];
        let before = Ring::build(ids[..3].iter().cloned(), VNODES);
        let after = Ring::build(ids.clone(), VNODES);
        let mut placement = Placement::new(Some(entering.clone()));
        let all: Vec<Member> = ids.iter().map(|node| member(node, Status::Alive)).collect();

        let transfers = Waiting(placement.update(&all, &counts()));

        assert_eq!(
            transfers.0.len(),
            1,
            "one member entered, so there is one step to walk"
        );
        let step = transfers.0[0].clone();
        let (mut held, mut through) = (0, 0);
        for key in keys() {
            let point = token(ACTOR, &key);
            let now = placement.replicas(ACTOR, &key, &counts(), &transfers);
            if step.ranges.iter().any(|span| span.holds(point)) {
                held += 1;
                // A key of a gained range stays where it was until the replicas that had it hand it over.
                assert_eq!(
                    now,
                    before.replicas(point, REPLICAS),
                    "key {key} did not wait"
                );
                assert!(!now.contains(entering));
            } else {
                through += 1;
                assert_eq!(
                    now,
                    after.replicas(point, REPLICAS),
                    "key {key} was held for nothing"
                );
            }
        }
        assert!(held > 0, "nothing was held back for the transfer");
        assert!(through > 0, "every key was held back");
    }

    #[test]
    fn a_client_answers_from_the_ring_of_the_table_without_waiting_for_anything() {
        let ids = Rolls::seeded(54).nodes(4);
        let members: Vec<Member> = ids.iter().map(|node| member(node, Status::Alive)).collect();
        let mut placement = Placement::new(None);
        let transfers = Waiting(placement.update(&members, &counts()));
        let ring = Ring::build(ids.clone(), VNODES);

        for key in keys() {
            assert_eq!(
                placement.replicas(ACTOR, &key, &counts(), &transfers),
                ring.replicas(token(ACTOR, &key), REPLICAS)
            );
        }
        assert!(transfers.0.is_empty(), "a client took a range to fill");
    }

    #[test]
    fn a_type_this_process_does_not_have_is_placed_on_the_whole_ring() {
        let ids = Rolls::seeded(55).nodes(4);
        let members: Vec<Member> = ids.iter().map(|node| member(node, Status::Alive)).collect();
        let mut placement = Placement::new(Some(ids[0].clone()));
        let transfers = Direct::default();
        let mut counts = Counts::default();
        counts.give_up("tests.deploy:audit");
        placement.update(&members, &counts);

        let replicas = placement.replicas("tests.deploy:audit", "a", &counts, &transfers);

        // Enough to route: the node that gets the message says it is not the owner unless it is.
        assert_eq!(replicas.len(), ids.len());
    }

    #[test]
    fn a_cluster_of_one_places_every_key_on_it() {
        let ids = Rolls::seeded(56).nodes(1);
        let mut placement = Placement::new(Some(ids[0].clone()));
        let transfers = Direct::default();
        placement.update(&[member(&ids[0], Status::Alive)], &counts());

        for key in keys() {
            assert_eq!(placement.replicas(ACTOR, &key, &counts(), &transfers), ids);
            assert_eq!(
                placement.owner(ACTOR, &key, &counts(), &transfers),
                Some(ids[0].clone())
            );
        }
    }

    #[test]
    fn a_pinned_key_is_kept_by_the_node_it_names_and_by_no_other() {
        let ids = Rolls::seeded(57).nodes(5);
        let members = alive(&ids);
        // Every member and a client read the same owner from the key alone.
        let views: Vec<Placement> = ids
            .iter()
            .cloned()
            .map(Some)
            .chain([None])
            .map(|node| {
                let mut placement = Placement::new(node);
                placement.update(&members, &counts());
                placement
            })
            .collect();

        for target in &ids {
            for name in ["worker", "gpu-0", "key-7"] {
                let key = pinned_to(target, name);
                for placement in &views {
                    assert_eq!(
                        placement.replicas(ACTOR, &key, &counts(), &Direct::default()),
                        vec![target.clone()]
                    );
                    assert_eq!(
                        placement.owner(ACTOR, &key, &counts(), &Direct::default()),
                        Some(target.clone())
                    );
                }
            }
        }
    }

    #[test]
    fn a_pinned_key_stays_on_its_node_while_members_enter_and_leave() {
        let ids = Rolls::seeded(58).nodes(7);
        let target = &ids[2];
        let key = pinned_to(target, "worker");
        let mut placement = Placement::new(Some(target.clone()));
        let mut transfers = Waiting::default();
        let tables = [
            ids[..5].to_vec(),
            ids.clone(),
            ids[2..].to_vec(),
            vec![ids[2].clone(), ids[5].clone()],
        ];

        for table in tables {
            transfers
                .0
                .extend(placement.update(&alive(&table), &counts()));

            assert_eq!(
                placement.replicas(ACTOR, &key, &counts(), &transfers),
                vec![target.clone()]
            );
            assert_eq!(
                placement.owner(ACTOR, &key, &counts(), &transfers),
                Some(target.clone())
            );
        }
        // The ring moved and the transfers never finished, so the key was answered for in the middle of every walk.
        assert!(
            !transfers.0.is_empty(),
            "no step was held back, so nothing was walked"
        );
    }

    #[test]
    fn a_pinned_key_has_no_owner_while_its_node_is_down_or_absent() {
        let ids = Rolls::seeded(59).nodes(4);
        let key = pinned_to(&ids[1], "worker");
        let mut placement = Placement::new(Some(ids[0].clone()));
        let transfers = Direct::default();

        for status in [Status::Dead, Status::Leaving] {
            let mut members = alive(&ids);
            members[1].status = status;
            placement.update(&members, &counts());
            // It is still the node that keeps the key: no other one takes it over.
            assert_eq!(
                placement.replicas(ACTOR, &key, &counts(), &transfers),
                vec![ids[1].clone()]
            );
            assert_eq!(placement.owner(ACTOR, &key, &counts(), &transfers), None);
        }

        let others: Vec<NodeId> = ids
            .iter()
            .filter(|node| **node != ids[1])
            .cloned()
            .collect();
        placement.update(&alive(&others), &counts());
        assert!(
            placement
                .replicas(ACTOR, &key, &counts(), &transfers)
                .is_empty()
        );
        assert_eq!(placement.owner(ACTOR, &key, &counts(), &transfers), None);
        // An address no member ever advertised is the same case.
        let nowhere = pin("10.9.9.9:7400", "worker");
        assert_eq!(
            placement.owner(ACTOR, &nowhere, &counts(), &transfers),
            None
        );
    }

    #[test]
    fn a_node_restarted_on_its_address_owns_the_keys_pinned_to_it() {
        let ids = Rolls::seeded(60).nodes(3);
        let restarted = NodeId::fresh(ids[1].address.clone());
        let key = pinned_to(&ids[1], "worker");
        let mut placement = Placement::new(Some(ids[0].clone()));
        let transfers = Direct::default();

        // The incarnation that went down stays in the table beside the new one until the join that replaced it is known.
        for status in [Status::Suspect, Status::Dead] {
            let mut members = alive(&ids);
            members[1].status = status;
            members.push(member(&restarted, Status::Alive));
            placement.update(&members, &counts());
            assert_eq!(
                placement.owner(ACTOR, &key, &counts(), &transfers),
                Some(restarted.clone())
            );
        }

        let now = [ids[0].clone(), restarted.clone(), ids[2].clone()];
        placement.update(&alive(&now), &counts());
        assert_eq!(
            placement.replicas(ACTOR, &key, &counts(), &transfers),
            vec![restarted.clone()]
        );
        assert_eq!(
            placement.owner(ACTOR, &key, &counts(), &transfers),
            Some(restarted)
        );
    }
}
