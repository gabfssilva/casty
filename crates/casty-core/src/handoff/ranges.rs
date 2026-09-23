//! The token ranges a node gained between two rings, and the transfer that fills them.

use std::collections::BTreeSet;

use crate::node::NodeId;
use crate::placement::{Range, Ring, Token};

/// Tokens are `blake2b/8`, so the circle is the 64 bit space. The arcs are counted in the next size up, because the
/// end of the circle is one past the largest token.
const SPACE: u128 = 1 << 64;

/// The token ranges `node` is one of the `count` replicas of in `current` and was not in `previous`.
#[must_use]
pub fn gained(previous: &Ring, current: &Ring, node: &NodeId, count: usize) -> Vec<Range> {
    let held = arcs(&current.ranges_of(node, count));
    let before = arcs(&previous.ranges_of(node, count));
    ranges(&without(&held, &before))
}

/// Whether a token falls in both.
#[must_use]
pub fn overlap(held: &[Range], others: &[Range]) -> bool {
    // Two ranges share a token exactly when one holds the end of the other: walking the circle on from a token of
    // both, the first of the two ends met is in both.
    held.iter().any(|mine| {
        others
            .iter()
            .any(|other| mine.holds(other.end) || other.holds(mine.end))
    })
}

/// The ranges one step of a ring gave this node, and the replicas of the previous ring that answer for them.
///
/// A key of a gained range is receiving until `wanted` of its replicas in `previous` have answered in full, with
/// `wanted` being `max(P, R - P + 1)` of the previous set: P answers cross every write that set confirmed, and
/// `R - P + 1` cross every quorum of promises, which is what keeps an owner it already fenced from writing here.
///
/// The threshold is counted against the replicas of each key and not of the range, because a range spans several
/// vnodes of the previous ring and its keys do not all have the same replicas there.
///
/// A source that is itself receiving one of the ranges answers all the same, with what it has: the answer ends its
/// part of the transfer, and only a settled one counts toward the threshold. Waiting for it to settle before taking
/// the answer at all is what would deadlock two nodes that entered together, each a source of the other.
#[derive(Debug, Clone)]
pub struct Transfer {
    pub id: u64,
    pub actor: String,
    pub previous: Ring,
    pub count: usize,
    pub wanted: usize,
    pub ranges: Vec<Range>,
    pub sources: BTreeSet<NodeId>,
    pub answered: BTreeSet<NodeId>,
    pub settled: BTreeSet<NodeId>,
}

impl Transfer {
    #[must_use]
    pub fn receiving(&self, token: Token) -> bool {
        if !self.ranges.iter().any(|span| span.holds(token)) {
            return false;
        }
        let replicas = self.previous.replicas(token, self.count);
        replicas
            .iter()
            .filter(|held| self.settled.contains(*held))
            .count()
            < self.wanted
    }

    /// The sources that still owe an answer. Every one has answered when nothing this node keeps is new.
    #[must_use]
    pub fn pending(&self) -> BTreeSet<NodeId> {
        self.sources.difference(&self.answered).cloned().collect()
    }

    /// The sources that do not count toward the threshold yet: asked again, they count once they have settled.
    #[must_use]
    pub fn unsettled(&self) -> BTreeSet<NodeId> {
        self.sources.difference(&self.settled).cloned().collect()
    }
}

/// The ranges as sorted half-open intervals of the circle, a wrapping one cut in two at zero.
fn arcs(held: &[Range]) -> Vec<(u128, u128)> {
    let mut found: Vec<(u128, u128)> = Vec::with_capacity(held.len() + 1);
    for span in held {
        let (start, end) = (u128::from(span.start), u128::from(span.end));
        match start.cmp(&end) {
            core::cmp::Ordering::Less => found.push((start + 1, end + 1)),
            core::cmp::Ordering::Greater => {
                found.push((0, end + 1));
                found.push((start + 1, SPACE));
            }
            // A range that ends where it starts is the whole circle.
            core::cmp::Ordering::Equal => found.push((0, SPACE)),
        }
    }
    found.sort_unstable();
    found
}

/// `held` minus `others`, both sorted and disjoint.
fn without(held: &[(u128, u128)], others: &[(u128, u128)]) -> Vec<(u128, u128)> {
    let mut left = Vec::new();
    for (start, end) in held.iter().copied() {
        let mut at = start;
        for (taken, until) in others.iter().copied() {
            if until <= at {
                continue;
            }
            if taken >= end {
                break;
            }
            if taken > at {
                left.push((at, taken));
            }
            at = until;
            if at >= end {
                break;
            }
        }
        if at < end {
            left.push((at, end));
        }
    }
    left
}

/// The intervals back as ranges, joining the one that ends at the top of the circle with the one at zero.
fn ranges(found: &[(u128, u128)]) -> Vec<Range> {
    if found.is_empty() {
        return Vec::new();
    }
    #[allow(clippy::cast_possible_truncation)]
    let point = |value: u128| (value - 1) as Token;
    if found == [(0, SPACE)] {
        return vec![Range {
            start: Token::MAX,
            end: Token::MAX,
        }];
    }
    if found.len() > 1 && found[0].0 == 0 && found[found.len() - 1].1 == SPACE {
        let first = found[0];
        let last = found[found.len() - 1];
        let mut held = vec![Range {
            start: point(last.0),
            end: point(first.1),
        }];
        held.extend(found[1..found.len() - 1].iter().map(|(start, end)| Range {
            start: point(*start),
            end: point(*end),
        }));
        return held;
    }
    found
        .iter()
        .map(|(start, end)| Range {
            start: point(*start),
            end: point(*end),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{gained, overlap};
    use crate::placement::{Range, Ring, token};
    use crate::rolls::Rolls;

    const VNODES: usize = 128;
    const REPLICAS: usize = 3;

    fn keys() -> Vec<u64> {
        (0..400)
            .map(|index| token("account", &format!("key-{index}")))
            .collect()
    }

    #[test]
    fn what_a_node_gained_is_what_it_replicates_now_and_did_not_before() {
        for seed in 1..=6_u64 {
            let mut rolls = Rolls::seeded(seed);
            let members = rolls.nodes(6);
            let joining = rolls.nodes(2);
            let previous = Ring::build(members.clone(), VNODES);
            let current = Ring::build(members.iter().cloned().chain(joining.clone()), VNODES);

            for node in members.iter().chain(joining.iter()) {
                let new = gained(&previous, &current, node, REPLICAS);
                for key in keys() {
                    let now = current.replicas(key, REPLICAS).contains(node);
                    let before = previous.replicas(key, REPLICAS).contains(node);
                    assert_eq!(
                        new.iter().any(|span| span.holds(key)),
                        now && !before,
                        "seed {seed}: key {key} of a node that {}",
                        if now { "holds it" } else { "does not" }
                    );
                }
            }
        }
    }

    #[test]
    fn a_node_that_was_already_a_replica_of_everything_gains_nothing() {
        let mut rolls = Rolls::seeded(7);
        let only = rolls.nodes(1);
        let ring = Ring::build(only.clone(), VNODES);

        assert!(gained(&ring, &ring, &only[0], REPLICAS).is_empty());
        // The whole circle against nothing is the whole circle.
        let empty = Ring::build(Vec::new(), VNODES);
        let whole = gained(&empty, &ring, &only[0], REPLICAS);
        assert_eq!(whole.len(), 1);
        for key in keys() {
            assert!(whole[0].holds(key));
        }
    }

    #[test]
    fn ranges_overlap_when_a_token_falls_in_both() {
        let a = [Range { start: 10, end: 20 }];
        let b = [Range { start: 15, end: 25 }];
        let far = [Range { start: 30, end: 40 }];
        let wrapping = [Range {
            start: u64::MAX - 5,
            end: 5,
        }];

        assert!(overlap(&a, &b));
        assert!(overlap(&b, &a));
        assert!(!overlap(&a, &far));
        assert!(overlap(&wrapping, &[Range { start: 0, end: 3 }]));
        assert!(!overlap(
            &wrapping,
            &[Range {
                start: 100,
                end: 200
            }]
        ));
        assert!(!overlap(&a, &[]));
    }
}
