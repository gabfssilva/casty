//! Taking a command to the owner of its key, and saying so when the owner it found disagrees.
//!
//! Nothing here does I/O either: it decides where a command goes, when one waits, and what answers the ones that will
//! not arrive.

use std::time::Duration;

use casty_core::backoff::Backoff;
use casty_core::mailbox::Command;
use casty_core::node::NodeId;
use casty_core::outcome::Outcome;
use casty_net::pool::Target as Destination;

use super::wire::{Cancel, Message, Routed};

/// How long a command that came back waits before it goes again. The two nodes see the table apart, and the change
/// that set them apart reaches the one behind within a few round trips.
const RETRY: Backoff = Backoff {
    first: Duration::from_millis(10),
    limit: Duration::from_secs(1),
    factor: 2.0,
};

/// Where a key is, as this node routes its messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Place {
    /// This node runs it.
    Here,
    /// Its messages wait on this node: the key is to run here once the range it is in has arrived, or this node has
    /// not joined the cluster and cannot tell where the key is yet.
    Holding,
    /// Another node runs it, or will once its range has arrived there.
    At(NodeId),
    /// No member hosts its type, or this node takes no part in the cluster.
    Nowhere,
}

impl Place {
    /// The node the messages of the key go to, `here` being this one.
    #[must_use]
    pub fn node(&self, here: &NodeId) -> Option<NodeId> {
        match self {
            Self::Here | Self::Holding => Some(here.clone()),
            Self::At(node) => Some(node.clone()),
            Self::Nowhere => None,
        }
    }
}

/// What the routing decided to do with a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Send it to the node that owns the key in this node's view.
    Send { to: Destination, message: Message },
    /// Hand it to the activation of its key here.
    Hand(Command),
    /// Keep it on this node, to go no sooner than `after`: until its key has arrived here, or, for one that came
    /// back, until the views of the nodes had time to meet.
    Wait { routed: Routed, after: Duration },
    /// Tell the activation of its key here, if there is one, that the caller of a request stopped waiting for it.
    Cancel(Cancel),
    /// Answer whoever waits, or drop the message with a word about why.
    Refuse {
        command: Command,
        outcome: Outcome,
        why: String,
    },
}

/// Where a command goes, given where its key is and whether this node still takes messages.
#[derive(Debug)]
pub struct Routing {
    node: NodeId,
    pub stopped: bool,
}

impl Routing {
    #[must_use]
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            stopped: false,
        }
    }

    /// One step of a command on its way: to the owner, to wait here, or to nobody.
    ///
    /// It is sent even when this node is the owner, through the transport as any other: a cancellation of the request
    /// takes that way too, and must not overtake it.
    #[must_use]
    pub fn route(&self, routed: Routed, place: Place) -> Decision {
        let owner = match place {
            Place::Nowhere => {
                let why = format!("no member hosts {}", routed.command.actor());
                return refuse(routed.command, Outcome::unreached, &why);
            }
            // What an ending activation hands back has nowhere else to go: this node is the owner and takes no more.
            Place::Here if self.stopped => {
                return refuse(
                    routed.command,
                    Outcome::unreached,
                    "the node is shutting down",
                );
            }
            Place::Holding => {
                return Decision::Wait {
                    routed,
                    after: Duration::ZERO,
                };
            }
            Place::Here => self.node.clone(),
            Place::At(owner) => owner,
        };
        Decision::Send {
            to: Destination::Node(owner),
            message: Message::Routed(routed),
        }
    }

    /// What arrived on the band of the component: a command to take, or one that came back.
    ///
    /// A command that came back waits for as long as the key takes to write, `timeout`, and not longer.
    #[must_use]
    pub fn receive(&self, message: Message, place: Place, timeout: Duration) -> Decision {
        match message {
            Message::Routed(routed) => match place {
                Place::Here => self.hand(routed.command),
                Place::Holding => Decision::Wait {
                    routed,
                    after: Duration::ZERO,
                },
                // Not the owner in this node's view, so it goes back to whoever routed it.
                Place::At(_) | Place::Nowhere => Decision::Send {
                    to: Destination::Node(routed.origin.clone()),
                    message: Message::WrongOwner(routed),
                },
            },
            Message::WrongOwner(routed) => self.again(routed, place, timeout),
            // Whoever owns the key now: the request runs on this node or it does not, and only the host knows which.
            Message::Cancel(cancel) => Decision::Cancel(cancel),
        }
    }

    /// Hand a command to an activation here, unless this node is on its way out.
    #[must_use]
    pub fn hand(&self, command: Command) -> Decision {
        if self.stopped {
            return refuse(command, Outcome::unreached, "the node is shutting down");
        }
        Decision::Hand(command)
    }

    /// Wait before sending once more, a little longer each time it comes back, until it has waited `timeout` in all.
    ///
    /// Sent again at once, it would go back and forth between two nodes that see the table apart for as long as they
    /// do. A node on its way out does not wait: it sends on what reaches it before the others have heard that it is
    /// leaving, and they send it back until they hear it, which is what ends the exchange.
    fn again(&self, routed: Routed, place: Place, timeout: Duration) -> Decision {
        if self.stopped {
            return self.route(routed, place);
        }
        if waited(routed.attempt, timeout) > timeout {
            return refuse(
                routed.command,
                Outcome::unreached,
                "the nodes did not agree on the owner of the key within the write timeout",
            );
        }
        Decision::Wait {
            after: delay(routed.attempt),
            routed: Routed {
                attempt: routed.attempt.saturating_add(1),
                ..routed
            },
        }
    }
}

/// How long a command waits after coming back from its `attempt`.
///
/// The attempt comes from the wire, so the delay is grown only until it stops growing.
fn delay(attempt: u32) -> Duration {
    let mut held = RETRY.first;
    for _ in 1..attempt {
        if held >= RETRY.limit {
            break;
        }
        held = RETRY.next(held);
    }
    held
}

/// How long a command has waited in all once it has come back from its `attempt`, counted until it passes `timeout`.
fn waited(attempt: u32, timeout: Duration) -> Duration {
    let mut held = RETRY.first;
    let mut total = Duration::ZERO;
    for _ in 0..attempt {
        total += held;
        if total > timeout {
            break;
        }
        held = RETRY.next(held);
    }
    total
}

/// Where the cancellation of a request goes: the node the messages of its key go to in this node's view, which is
/// where the request went, or where it went after the owner it first found sent it back. Nothing answers a
/// cancellation, so without an owner it goes nowhere.
///
/// It is sent even when this node is the owner: the request took that way too, and the cancellation must not overtake
/// it.
#[must_use]
pub fn cancelling(cancel: Cancel, owner: Option<NodeId>) -> Option<Decision> {
    owner.map(|owner| Decision::Send {
        to: Destination::Node(owner),
        message: Message::Cancel(cancel),
    })
}

/// Answer whoever waits for `command` with what `outcome` makes of its key, or drop it saying `why`.
#[must_use]
pub fn refuse(command: Command, outcome: fn(&str, &str) -> Outcome, why: &str) -> Decision {
    let held = outcome(command.actor(), command.key());
    Decision::Refuse {
        command,
        outcome: held,
        why: why.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use casty_core::chain::Chain;
    use casty_core::mailbox::{Command, Deliver};
    use casty_core::node::{NodeId, Target};
    use casty_core::rolls::Rolls;
    use casty_net::pool::Target as Destination;

    use super::{Decision, Place, Routing, cancelling, delay, waited};
    use crate::routing::wire::{Cancel, Message, Routed};

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn cancel(caller: &NodeId) -> Cancel {
        Cancel {
            actor: "tests.app:account".to_owned(),
            key: "acc-1".to_owned(),
            request: Target::Reply {
                node: caller.clone(),
                id: 9,
            },
        }
    }

    #[test]
    fn a_cancellation_goes_to_the_owner_of_its_key_and_nowhere_without_one() {
        let ids = Rolls::seeded(63).nodes(2);
        let held = cancel(&ids[0]);
        // The owner being this node changes nothing: the request went through the transport, and so does this.
        for owner in [&ids[1], &ids[0]] {
            assert_eq!(
                cancelling(held.clone(), Some(owner.clone())),
                Some(Decision::Send {
                    to: Destination::Node(owner.clone()),
                    message: Message::Cancel(held.clone()),
                })
            );
        }
        assert_eq!(cancelling(held, None), None);
    }

    #[test]
    fn a_cancellation_that_arrives_is_handed_here_whoever_owns_the_key() {
        let ids = Rolls::seeded(64).nodes(2);
        let mut routing = Routing::new(ids[0].clone());
        let held = cancel(&ids[1]);
        for place in [
            Place::Here,
            Place::Holding,
            Place::At(ids[1].clone()),
            Place::Nowhere,
        ] {
            assert_eq!(
                routing.receive(Message::Cancel(held.clone()), place, TIMEOUT),
                Decision::Cancel(held.clone())
            );
        }
        // A node on its way out still takes it: the body it is finishing may be the one asked to stop.
        routing.stopped = true;
        assert_eq!(
            routing.receive(Message::Cancel(held.clone()), Place::Here, TIMEOUT),
            Decision::Cancel(held)
        );
    }

    fn bounced(origin: &NodeId, attempt: u32) -> Routed {
        Routed {
            command: Command::Deliver(Deliver {
                actor: "tests.app:account".to_owned(),
                key: "acc-1".to_owned(),
                message: vec![1],
                reply: None,
                chain: Chain::default(),
            }),
            origin: origin.clone(),
            attempt,
        }
    }

    #[test]
    fn a_command_for_a_key_held_here_waits_and_one_for_a_key_elsewhere_goes_back() {
        let ids = Rolls::seeded(66).nodes(2);
        let routing = Routing::new(ids[0].clone());
        let routed = bounced(&ids[1], 1);

        for decision in [
            routing.route(routed.clone(), Place::Holding),
            routing.receive(Message::Routed(routed.clone()), Place::Holding, TIMEOUT),
        ] {
            assert_eq!(
                decision,
                Decision::Wait {
                    routed: routed.clone(),
                    after: Duration::ZERO,
                }
            );
        }
        assert_eq!(
            routing.receive(
                Message::Routed(routed.clone()),
                Place::At(ids[0].clone()),
                TIMEOUT
            ),
            Decision::Send {
                to: Destination::Node(ids[1].clone()),
                message: Message::WrongOwner(routed),
            }
        );
    }

    #[test]
    fn a_command_that_came_back_waits_longer_each_time_until_it_has_waited_the_timeout() {
        let ids = Rolls::seeded(67).nodes(2);
        let routing = Routing::new(ids[0].clone());
        let place = Place::At(ids[1].clone());
        assert_eq!(delay(1), Duration::from_millis(10));
        assert_eq!(delay(2), Duration::from_millis(20));

        let mut attempt = 1;
        loop {
            let decision = routing.receive(
                Message::WrongOwner(bounced(&ids[0], attempt)),
                place.clone(),
                TIMEOUT,
            );
            if waited(attempt, TIMEOUT) > TIMEOUT {
                assert!(matches!(decision, Decision::Refuse { .. }), "{decision:?}");
                break;
            }
            assert_eq!(
                decision,
                Decision::Wait {
                    routed: bounced(&ids[0], attempt + 1),
                    after: delay(attempt),
                }
            );
            attempt += 1;
        }
        // The delay stops growing at a second, so the timeout is reached in a bounded number of returns.
        assert!(attempt < 20, "{attempt}");
    }

    #[test]
    fn a_node_on_its_way_out_sends_on_what_comes_back_until_the_owner_takes_it() {
        let ids = Rolls::seeded(65).nodes(2);
        let mut routing = Routing::new(ids[0].clone());
        let routed = bounced(&ids[0], u32::MAX);
        let place = Place::At(ids[1].clone());
        assert!(matches!(
            routing.receive(Message::WrongOwner(routed.clone()), place.clone(), TIMEOUT),
            Decision::Refuse { .. }
        ));
        // The owner it sends to has not heard it is leaving and sends it back, as often as that takes.
        routing.stopped = true;
        assert_eq!(
            routing.receive(Message::WrongOwner(routed.clone()), place, TIMEOUT),
            Decision::Send {
                to: Destination::Node(ids[1].clone()),
                message: Message::Routed(routed),
            }
        );
    }
}
