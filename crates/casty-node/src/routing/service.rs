//! Taking a command to the owner of its key, and saying so when the owner it found disagrees.
//!
//! Nothing here does I/O either: it decides where a command goes and what answers the ones that will not arrive.

use casty_core::mailbox::Command;
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_net::pool::Target as Destination;

use super::wire::{Cancel, Message, Routed};

/// A second refusal ends a message: two nodes with different views of the table would otherwise pass it back and
/// forth.
const ATTEMPTS: u32 = 2;

/// What the routing decided to do with a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Send it to the node that owns the key in this node's view.
    Send { to: Destination, message: Message },
    /// Hand it to the activation of its key here.
    Hand(Command),
    /// Tell the activation of its key here, if there is one, that the caller of a request stopped waiting for it.
    Cancel(Cancel),
    /// Answer whoever waits, or drop the message with a word about why.
    Refuse {
        command: Command,
        outcome: Outcome,
        why: String,
    },
}

/// Where a command goes, given who owns its key and whether this node still takes messages.
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

    /// One step of a command on its way: to the owner, to an activation here, or to nobody.
    #[must_use]
    pub fn route(&self, routed: Routed, owner: Option<NodeId>) -> Decision {
        let command = routed.command.clone();
        let Some(owner) = owner else {
            return refuse(
                command,
                Outcome::unreached,
                &format!("no member hosts {}", routed.command.actor()),
            );
        };
        if self.stopped && owner == self.node {
            // What an ending activation hands back has nowhere else to go: this node is the owner and takes no more.
            return refuse(command, Outcome::unreached, "the node is shutting down");
        }
        Decision::Send {
            to: Destination::Node(owner),
            message: Message::Routed(routed),
        }
    }

    /// What arrived on the band of the component: a command to take, or one that came back.
    #[must_use]
    pub fn receive(&self, message: Message, owner: Option<NodeId>) -> Decision {
        match message {
            Message::Routed(routed) => {
                if owner.as_ref() == Some(&self.node) {
                    return self.hand(routed.command);
                }
                // Not the owner in this node's view, so it goes back to whoever routed it.
                Decision::Send {
                    to: Destination::Node(routed.origin.clone()),
                    message: Message::WrongOwner(routed),
                }
            }
            Message::WrongOwner(routed) => self.again(routed, owner),
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

    /// Send once more, through the view of now; a second refusal ends the message.
    ///
    /// A node on its way out does not count the refusals: it sends on what reaches it before the others have heard
    /// that it is leaving, and they send it back until they hear it, which is what ends the exchange.
    fn again(&self, routed: Routed, owner: Option<NodeId>) -> Decision {
        if self.stopped {
            return self.route(routed, owner);
        }
        if routed.attempt >= ATTEMPTS {
            return refuse(
                routed.command,
                Outcome::unreached,
                "the owner of the key changed twice while the message travelled",
            );
        }
        self.route(
            Routed {
                attempt: routed.attempt + 1,
                ..routed
            },
            owner,
        )
    }
}

/// Where the answer of a refused command goes, if anyone waits for it.
#[must_use]
pub fn waiting(command: &Command) -> Option<&Target> {
    command.reply()
}

/// Where the cancellation of a request goes: the owner of its key in this node's view, which is where the request
/// went, or where it went after the owner it first found sent it back. Nothing answers a cancellation, so without an
/// owner it goes nowhere.
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

fn refuse(command: Command, outcome: fn(&str, &str) -> Outcome, why: &str) -> Decision {
    let held = outcome(command.actor(), command.key());
    Decision::Refuse {
        command,
        outcome: held,
        why: why.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use casty_core::node::{NodeId, Target};
    use casty_core::rolls::Rolls;
    use casty_net::pool::Target as Destination;

    use casty_core::chain::Chain;
    use casty_core::mailbox::{Command, Deliver};

    use super::{ATTEMPTS, Decision, Routing, cancelling};
    use crate::routing::wire::{Cancel, Message, Routed};

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
        for owner in [Some(ids[0].clone()), Some(ids[1].clone()), None] {
            assert_eq!(
                routing.receive(Message::Cancel(held.clone()), owner),
                Decision::Cancel(held.clone())
            );
        }
        // A node on its way out still takes it: the body it is finishing may be the one asked to stop.
        routing.stopped = true;
        assert_eq!(
            routing.receive(Message::Cancel(held.clone()), Some(ids[0].clone())),
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
    fn a_node_on_its_way_out_sends_on_what_comes_back_until_the_owner_takes_it() {
        let ids = Rolls::seeded(65).nodes(2);
        let mut routing = Routing::new(ids[0].clone());
        let routed = bounced(&ids[0], ATTEMPTS);
        assert!(matches!(
            routing.receive(Message::WrongOwner(routed.clone()), Some(ids[1].clone())),
            Decision::Refuse { .. }
        ));
        // The owner it sends to has not heard it is leaving and sends it back, as often as that takes.
        routing.stopped = true;
        assert_eq!(
            routing.receive(Message::WrongOwner(routed.clone()), Some(ids[1].clone())),
            Decision::Send {
                to: Destination::Node(ids[1].clone()),
                message: Message::Routed(routed),
            }
        );
    }
}
