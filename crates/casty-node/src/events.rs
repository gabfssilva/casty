//! What a node reports of itself: the members it saw change, the operations that did not go through, the keys it
//! moved, and what it lost on the way.
//!
//! The components decide these as they go and hand them out with what they send; the task of the node gives them to
//! the host. Nothing waits for anyone to listen, and a host with nobody listening lets them go.

use std::collections::BTreeMap;

use casty_core::membership::table::{Status, Transition};
use casty_core::node::NodeId;

use crate::replication::service::Failure;

/// Something a node saw or did that whoever watches it may want to hear of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A member took `status`: from `previous`, or from nothing when this node had not seen it before.
    MemberChanged {
        node: NodeId,
        status: Status,
        previous: Option<Status>,
    },
    /// An activation or a write of a key did not go through, and whoever waited for it was told why.
    WriteFailed {
        actor: String,
        key: String,
        operation: Operation,
        failure: Failure,
    },
    /// Keys of `actor` started moving: in, to fill the ranges a ring gave this node, or out, to the nodes that
    /// replicate the keys a ring took from it.
    HandoffStarted { actor: String, direction: Direction },
    /// Nothing of `actor` is moving that way any more.
    HandoffEnded { actor: String, direction: Direction },
    /// A leave stopped waiting for the nodes that replicate `keys` of `actor` now to take them, which ends the
    /// handoff out of `actor` with those keys not handed over.
    HandoffAbandoned { actor: String, keys: Vec<String> },
    /// A connection to a peer ended while this node was running.
    ConnectionLost { node: NodeId },
    /// A message nobody waits for ended on this node without reaching its key.
    MessageDropped {
        actor: String,
        key: String,
        reason: String,
    },
}

/// Which operation over the replicas of a key failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Taking the key over: the promises and the write that marks it active.
    Activate,
    /// A write of the state, or the one that lets the key go.
    Write,
}

impl Operation {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Activate => "activate",
            Self::Write => "write",
        }
    }
}

/// Which way keys move in a handoff, seen from the node that reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    In,
    Out,
}

impl Direction {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
        }
    }
}

impl From<Transition> for Event {
    fn from(moved: Transition) -> Self {
        Self::MemberChanged {
            node: moved.node,
            status: moved.status,
            previous: moved.previous,
        }
    }
}

/// What a leave went without, one event per type.
#[must_use]
pub fn abandoned(owed: &[(String, String)]) -> Vec<Event> {
    let mut by_type: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for (actor, key) in owed {
        by_type.entry(actor.as_str()).or_default().push(key.clone());
    }
    by_type
        .into_iter()
        .map(|(actor, keys)| Event::HandoffAbandoned {
            actor: actor.to_owned(),
            keys,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Event, abandoned};

    #[test]
    fn what_a_leave_went_without_is_said_once_per_type() {
        let owed = [
            ("tests.app:account".to_owned(), "a-1".to_owned()),
            ("tests.app:account".to_owned(), "a-2".to_owned()),
            ("tests.app:ledger".to_owned(), "l-1".to_owned()),
        ];

        assert_eq!(
            abandoned(&owed),
            vec![
                Event::HandoffAbandoned {
                    actor: "tests.app:account".to_owned(),
                    keys: vec!["a-1".to_owned(), "a-2".to_owned()],
                },
                Event::HandoffAbandoned {
                    actor: "tests.app:ledger".to_owned(),
                    keys: vec!["l-1".to_owned()],
                },
            ]
        );
    }
}
