//! The member table as a client keeps it: asked for, never gossiped.
//!
//! It answers for `membership`, the component a member replies to. The `Sync` it sends carries only the `NodeId` of
//! the client, so the member that answers neither adds it to its table nor watches it. The table a client holds is
//! up to `every` old, and until the next one arrives a key whose owner changed costs a retry and a key whose owner
//! died waits.

use casty_core::membership::table::Status;
use casty_core::node::NodeId;
use casty_core::rolls::Rolls;
use casty_net::pool::Target;

use super::service::{Member, Outgoing};
use super::wire::{Body, Message, Sender, decode, encode};

#[derive(Debug)]
pub struct Directory {
    node: NodeId,
    seeds: Vec<String>,
    members: Vec<Member>,
    rolls: Rolls,
    /// Whether the last question was answered: it goes back to a seed when one was not.
    answered: bool,
    /// Whether the first table has arrived, which is what the start of a client waits for.
    pub joined: bool,
    pub changed: bool,
    sends: Vec<Outgoing>,
}

impl Directory {
    #[must_use]
    pub fn new(node: NodeId, seeds: Vec<String>) -> Self {
        Self {
            node,
            seeds,
            members: Vec::new(),
            rolls: Rolls::fresh(),
            answered: false,
            joined: false,
            changed: false,
            sends: Vec::new(),
        }
    }

    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.members.clone()
    }

    pub fn take(&mut self) -> Vec<Outgoing> {
        core::mem::take(&mut self.sends)
    }

    /// Ask a member for the whole table, or a seed when the last question went unanswered.
    pub fn ask(&mut self) {
        let alive: Vec<NodeId> = self
            .members
            .iter()
            .filter(|member| member.status == Status::Alive)
            .map(|member| member.node.clone())
            .collect();
        let to = if self.answered && !alive.is_empty() {
            Target::Node(self.rolls.pick(&alive).clone())
        } else {
            Target::Seed(self.rolls.pick(&self.seeds).clone())
        };
        self.answered = false;
        self.sends.push(Outgoing {
            to,
            message: Message {
                sender: Sender::Client(self.node.clone()),
                body: Body::Sync(Vec::new()),
            },
        });
    }

    /// The answer of a member, which is the only thing that ever arrives here.
    pub fn receive(&mut self, payload: &[u8]) {
        let Ok(message) = decode(payload) else {
            return;
        };
        let Body::SyncReply(records) = message.body else {
            return;
        };
        self.answered = true;
        // A `left` record is a tombstone, not a member.
        self.members = records
            .into_iter()
            .filter(|record| record.status != Status::Left)
            .map(|record| Member {
                node: record.node,
                status: record.status,
                types: record.types,
            })
            .collect();
        self.joined = true;
        self.changed = true;
    }

    /// The bytes of what is queued, which is what the transport sends.
    #[must_use]
    pub fn payload(message: &Message) -> Vec<u8> {
        encode(message)
    }
}
