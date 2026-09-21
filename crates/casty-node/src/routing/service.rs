//! Taking a command to the owner of its key, and saying so when the owner it found disagrees.
//!
//! Nothing here does I/O either: it decides where a command goes and what answers the ones that will not arrive.

use casty_core::mailbox::Command;
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_net::pool::Target as Destination;

use super::wire::{Message, Routed};

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
    fn again(&self, routed: Routed, owner: Option<NodeId>) -> Decision {
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

fn refuse(command: Command, outcome: fn(&str, &str) -> Outcome, why: &str) -> Decision {
    let held = outcome(command.actor(), command.key());
    Decision::Refuse {
        command,
        outcome: held,
        why: why.to_owned(),
    }
}
