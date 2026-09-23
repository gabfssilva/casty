//! The consistent hashing ring of one actor type, and the pinned keys it does not place.
//!
//! Every node derives the same ring from the same members, so the hash and the order of the tokens are part of the
//! wire: a node of either implementation must place a key on the same replicas.

use blake2::digest::consts::U8;
use blake2::{Blake2b, Digest};

use crate::node::NodeId;

pub type Token = u64;

/// A slice of the circle, `start` exclusive and `end` inclusive, which may wrap around zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Range {
    pub start: Token,
    pub end: Token,
}

impl Range {
    #[must_use]
    pub fn holds(&self, token: Token) -> bool {
        // A range that ends where it starts is the whole circle: the range of a node that replicates every token.
        if self.start < self.end {
            self.start < token && token <= self.end
        } else {
            token > self.start || token <= self.end
        }
    }
}

/// `blake2b/8` of `"{actor}/{key}"`, the same hash on every node.
#[must_use]
pub fn token(actor: &str, key: &str) -> Token {
    digest(&format!("{actor}/{key}"))
}

/// The address a pinned key names, or nothing for a key the ring places.
///
/// A pinned key is `@{address}/{name}`, `address` being the advertised `host:port` of the node that runs it. The form
/// is part of the wire like the hash: every node, and every client, reads the owner of a pinned key from the key alone.
#[must_use]
pub fn pinned(key: &str) -> Option<&str> {
    let (address, _) = key.strip_prefix('@')?.split_once('/')?;
    let (host, port) = address.rsplit_once(':')?;
    let numeric = !port.is_empty() && port.bytes().all(|digit| digit.is_ascii_digit());
    (!host.is_empty() && numeric).then_some(address)
}

/// The key `name` pinned to the node advertised at `address`.
#[must_use]
pub fn pin(address: &str, name: &str) -> String {
    format!("@{address}/{name}")
}

/// Where the keys of one actor type live: `vnodes` tokens per node on a circle of `blake2b/8` digests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ring {
    nodes: Vec<NodeId>,
    tokens: Vec<Token>,
    owners: Vec<NodeId>,
}

impl Ring {
    #[must_use]
    pub fn build(nodes: impl IntoIterator<Item = NodeId>, vnodes: usize) -> Self {
        let mut members: Vec<NodeId> = nodes.into_iter().collect();
        members.sort_by(|left, right| order(left).cmp(&order(right)));
        members.dedup();
        let mut placed: Vec<(Token, usize)> = members
            .iter()
            .enumerate()
            .flat_map(|(at, node)| {
                tokens_of(node, vnodes)
                    .into_iter()
                    .map(move |point| (point, at))
            })
            .collect();
        placed.sort_unstable();
        let tokens = placed.iter().map(|(point, _)| *point).collect();
        let owners = placed.iter().map(|(_, at)| members[*at].clone()).collect();
        Self {
            nodes: members,
            tokens,
            owners,
        }
    }

    #[must_use]
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    /// The owner of the token and the next distinct physical nodes, at most `count`.
    #[must_use]
    pub fn replicas(&self, token: Token, count: usize) -> Vec<NodeId> {
        let wanted = count.min(self.nodes.len());
        let mut found: Vec<NodeId> = Vec::with_capacity(wanted);
        if self.tokens.is_empty() {
            return found;
        }
        let first = self.tokens.partition_point(|held| *held < token);
        for step in 0..self.tokens.len() {
            if found.len() == wanted {
                break;
            }
            let owner = &self.owners[(first + step) % self.tokens.len()];
            if !found.contains(owner) {
                found.push(owner.clone());
            }
        }
        found
    }

    /// The token ranges `node` is one of the `count` replicas of.
    #[must_use]
    pub fn ranges_of(&self, node: &NodeId, count: usize) -> Vec<Range> {
        let held: Vec<usize> = (0..self.tokens.len())
            .filter(|at| self.replicas(self.tokens[*at], count).contains(node))
            .collect();
        let mut runs: Vec<(usize, usize)> = Vec::new();
        for at in held {
            match runs.last_mut() {
                Some(last) if last.1 + 1 == at => last.1 = at,
                _ => runs.push((at, at)),
            }
        }
        if runs.len() > 1 && runs[0].0 == 0 && runs[runs.len() - 1].1 == self.tokens.len() - 1 {
            let first = runs.remove(0);
            let last = runs.pop().expect("more than one run");
            runs.insert(0, (last.0, first.1));
        }
        runs.into_iter()
            .map(|(start, end)| Range {
                start: self.tokens[(start + self.tokens.len() - 1) % self.tokens.len()],
                end: self.tokens[end],
            })
            .collect()
    }

    /// This ring with `node` taken out when it is a member, and put in otherwise: one step of a `chain`.
    ///
    /// Splicing the tokens of one node into the sorted ring costs a fraction of building it again, and a chain of a
    /// hundred steps runs while the node has membership to serve.
    #[must_use]
    pub fn applied(&self, node: &NodeId, vnodes: usize) -> Self {
        let joining = !self.nodes.contains(node);
        let placed = order(node);
        let mut tokens: Vec<Token> = Vec::with_capacity(self.tokens.len() + vnodes);
        let mut owners: Vec<NodeId> = Vec::with_capacity(self.owners.len() + vnodes);
        let mut at = 0;
        for point in tokens_of(node, vnodes) {
            let mut index = at + self.tokens[at..].partition_point(|held| *held < point);
            // Equal tokens keep the order of their members, so a spliced step and a ring built from scratch agree.
            while index < self.tokens.len()
                && self.tokens[index] == point
                && order(&self.owners[index]) < placed
            {
                index += 1;
            }
            tokens.extend_from_slice(&self.tokens[at..index]);
            owners.extend_from_slice(&self.owners[at..index]);
            if joining {
                tokens.push(point);
                owners.push(node.clone());
            }
            at = if joining { index } else { index + 1 };
        }
        tokens.extend_from_slice(&self.tokens[at..]);
        owners.extend_from_slice(&self.owners[at..]);
        let mut members: Vec<NodeId> = if joining {
            let mut held = self.nodes.clone();
            held.push(node.clone());
            held
        } else {
            self.nodes
                .iter()
                .filter(|held| *held != node)
                .cloned()
                .collect()
        };
        members.sort_by(|left, right| order(left).cmp(&order(right)));
        Self {
            nodes: members,
            tokens,
            owners,
        }
    }
}

/// The rings between two member sets, one member applied per step.
///
/// The pending members, the ones that entered and the ones that left, are ordered by incarnation, so that every node
/// derives the same sequence while learning the changes in different orders. The last ring is `target`, and the
/// sequence is empty when both sets are equal.
///
/// The ones that entered go first, so that no step of a replacement holds fewer members than the smaller of the two
/// ends. Interleaved by incarnation alone, a rolling deploy would take every key down to fewer replicas on the way,
/// and in the limit to one, before the new nodes arrived.
#[must_use]
pub fn chain(previous: &Ring, target: &Ring, vnodes: usize) -> Vec<Ring> {
    let mut pending: Vec<&NodeId> = previous
        .nodes
        .iter()
        .filter(|node| !target.nodes.contains(node))
        .chain(
            target
                .nodes
                .iter()
                .filter(|node| !previous.nodes.contains(node)),
        )
        .collect();
    pending.sort_by_key(|node| (previous.nodes.contains(node), order(node)));
    if pending.is_empty() {
        return Vec::new();
    }
    let mut ring = previous.clone();
    let mut steps = Vec::with_capacity(pending.len());
    for node in &pending[..pending.len() - 1] {
        ring = ring.applied(node, vnodes);
        steps.push(ring.clone());
    }
    steps.push(target.clone());
    steps
}

/// What orders the members of a ring, which every node has to agree on.
fn order(node: &NodeId) -> ([u8; 16], &str) {
    (node.incarnation, node.address.as_deref().unwrap_or(""))
}

/// The tokens of one node, sorted, which is what lets a step splice them into the ring in a single pass.
fn tokens_of(node: &NodeId, count: usize) -> Vec<Token> {
    let address = node.address.as_deref().unwrap_or("None");
    let incarnation = written(node.incarnation);
    let mut found: Vec<Token> = (0..count)
        .map(|index| digest(&format!("{address}/{incarnation}/{index}")))
        .collect();
    found.sort_unstable();
    found
}

/// The incarnation as the other implementation writes it into a token: the canonical form of a UUID.
fn written(incarnation: [u8; 16]) -> String {
    use core::fmt::Write;
    let hex = incarnation
        .iter()
        .fold(String::with_capacity(32), |mut written, byte| {
            let _ = write!(written, "{byte:02x}");
            written
        });
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn digest(value: &str) -> Token {
    let mut hasher = Blake2b::<U8>::new();
    hasher.update(value.as_bytes());
    let out = hasher.finalize();
    u64::from_be_bytes(out.into())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{Ring, Token, chain, order, pin, pinned, token};
    use crate::node::NodeId;
    use crate::rolls::Rolls;

    const VNODES: usize = 128;
    const REPLICAS: usize = 3;

    fn keys() -> Vec<Token> {
        (0..300)
            .map(|index| token("account", &format!("key-{index}")))
            .collect()
    }

    fn holders(ring: &Ring, key: Token) -> BTreeSet<[u8; 16]> {
        ring.replicas(key, REPLICAS)
            .into_iter()
            .map(|node| node.incarnation)
            .collect()
    }

    #[test]
    fn a_chain_changes_at_most_one_replica_of_a_key_per_step() {
        let keys = keys();
        for seed in 1..=8_u64 {
            let mut rolls = Rolls::seeded(seed);
            let count = rolls.between(3, 12);
            let members = rolls.nodes(count);
            let gone = rolls.between(1, members.len() - 1);
            let leaving: Vec<NodeId> = members[..gone].to_vec();
            let entering = rolls.between(1, 4);
            let joining = rolls.nodes(entering);
            let previous = Ring::build(members.clone(), VNODES);
            let target = Ring::build(
                members
                    .iter()
                    .filter(|node| !leaving.contains(node))
                    .cloned()
                    .chain(joining.iter().cloned()),
                VNODES,
            );
            let mut rings = vec![previous.clone()];
            rings.extend(chain(&previous, &target, VNODES));

            assert_eq!(
                rings.len(),
                1 + leaving.len() + joining.len(),
                "seed {seed}"
            );
            assert_eq!(
                rings[rings.len() - 1].nodes(),
                target.nodes(),
                "seed {seed}"
            );
            // No step is thinner than both ends: a replacement must not take keys down to fewer replicas on the way.
            let floor = previous.nodes().len().min(target.nodes().len());
            let thinnest = rings
                .iter()
                .map(|ring| ring.nodes().len())
                .min()
                .expect("a step");
            assert!(
                thinnest >= floor,
                "seed {seed}: a step of {thinnest} between {floor} and {floor}"
            );

            // A spliced step is the ring its members would have built from scratch.
            let spliced = &rings[rings.len() / 2];
            let rebuilt = Ring::build(spliced.nodes().to_vec(), VNODES);
            for key in &keys {
                assert_eq!(
                    spliced.replicas(*key, REPLICAS),
                    rebuilt.replicas(*key, REPLICAS),
                    "seed {seed}: the step of {} members is not their ring",
                    spliced.nodes().len()
                );
            }

            for pair in rings.windows(2) {
                for key in &keys {
                    let was = holders(&pair[0], *key);
                    let now = holders(&pair[1], *key);
                    assert_eq!(
                        now.len(),
                        REPLICAS.min(pair[1].nodes().len()),
                        "seed {seed}"
                    );
                    assert!(
                        was.difference(&now).count() <= 1,
                        "seed {seed}: lost more than one replica"
                    );
                    assert!(
                        now.difference(&was).count() <= 1,
                        "seed {seed}: gained more than one replica"
                    );
                }
            }
        }
    }

    #[test]
    fn the_ranges_of_a_node_are_the_keys_it_replicates() {
        let mut rolls = Rolls::seeded(0x5eed_5eed);
        let members = rolls.nodes(7);
        let ring = Ring::build(members.clone(), VNODES);
        let held = &members[3];
        let covered = ring.ranges_of(held, REPLICAS);

        assert!(!covered.is_empty());
        // The ranges do not touch: no range ends where another starts.
        let starts: BTreeSet<Token> = covered.iter().map(|span| span.start).collect();
        let ends: BTreeSet<Token> = covered.iter().map(|span| span.end).collect();
        assert!(covered.len() == 1 || starts.is_disjoint(&ends));
        for key in keys() {
            let inside = covered.iter().any(|span| span.holds(key));
            assert_eq!(
                inside,
                ring.replicas(key, REPLICAS).contains(held),
                "key {key}"
            );
        }
    }

    #[test]
    fn the_chain_is_the_same_however_the_changes_were_learned() {
        let mut rolls = Rolls::seeded(0x00c0_ffee);
        let members = rolls.nodes(6);
        let joining = rolls.nodes(3);
        let previous = Ring::build(members.clone(), VNODES);
        let target = Ring::build(
            members.iter().skip(2).cloned().chain(joining.clone()),
            VNODES,
        );

        let straight = chain(&previous, &target, VNODES);
        let shuffled = chain(
            &Ring::build(members.iter().rev().cloned(), VNODES),
            &Ring::build(
                joining
                    .iter()
                    .rev()
                    .cloned()
                    .chain(members.iter().skip(2).cloned()),
                VNODES,
            ),
            VNODES,
        );

        let written = |steps: &[Ring]| {
            steps
                .iter()
                .map(|step| {
                    step.nodes()
                        .iter()
                        .map(order)
                        .map(|(at, _)| at)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(written(&straight), written(&shuffled));
    }

    #[test]
    fn a_ring_with_one_node_gives_it_every_key_and_the_whole_circle() {
        let mut rolls = Rolls::seeded(1);
        let only = rolls.nodes(1);
        let ring = Ring::build(only.clone(), VNODES);

        for key in keys() {
            assert_eq!(ring.replicas(key, REPLICAS), only);
        }
        let covered = ring.ranges_of(&only[0], REPLICAS);
        assert_eq!(covered.len(), 1);
        assert!(covered[0].holds(0));
        assert!(covered[0].holds(Token::MAX));
        assert!(chain(&ring, &Ring::build(only, VNODES), VNODES).is_empty());
    }

    #[test]
    fn an_empty_ring_places_nothing() {
        let ring = Ring::build(Vec::new(), VNODES);
        assert!(ring.nodes().is_empty());
        assert!(ring.replicas(token("account", "a"), REPLICAS).is_empty());
    }

    #[test]
    fn a_pinned_key_names_the_address_it_was_pinned_to() {
        for address in ["10.0.0.5:7400", "worker-3.internal:7400", "[::1]:7400"] {
            // The name is whatever the caller chose, a slash or another pinned key included.
            for name in ["", "gpu", "a/b", "@10.0.0.6:7400/nested"] {
                assert_eq!(pinned(&pin(address, name)), Some(address));
            }
        }
    }

    #[test]
    fn a_key_that_names_no_address_is_placed_by_the_ring() {
        for key in [
            "acc-1",
            "",
            "@",
            "@handle",
            "@handle/x",
            "@10.0.0.5:7400",
            "@:7400/x",
            "@10.0.0.5:/x",
            "@10.0.0.5:74a0/x",
            "x@10.0.0.5:7400/y",
        ] {
            assert_eq!(pinned(key), None, "{key:?} was read as pinned");
        }
    }
}
