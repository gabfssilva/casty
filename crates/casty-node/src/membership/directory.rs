//! The member table as a client keeps it: asked for, never gossiped.
//!
//! It answers for `membership`, the component a member replies to. The `Sync` it sends carries only the `NodeId` of
//! the client, so the member that answers neither adds it to its table nor watches it. The table a client holds is
//! up to `every` old, and until the next one arrives a key whose owner changed costs a retry and a key whose owner
//! died waits.

use std::collections::{HashMap, HashSet};

use casty_core::membership::table::{Status, Transition};
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
    moved: Vec<Transition>,
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
            moved: Vec::new(),
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

    /// The changes of status since the last call, from one table that arrived to the next.
    ///
    /// A member that went through several statuses between two tables is one change, from the first to the last.
    pub fn transitions(&mut self) -> Vec<Transition> {
        core::mem::take(&mut self.moved)
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
        let members: Vec<Member> = records
            .into_iter()
            .filter(|record| record.status != Status::Left)
            .map(|record| Member {
                node: record.node,
                status: record.status,
                types: record.types,
            })
            .collect();
        self.moved.extend(diff(&self.members, &members));
        self.members = members;
        self.joined = true;
        self.changed = true;
    }

    /// The bytes of what is queued, which is what the transport sends.
    #[must_use]
    pub fn payload(message: &Message) -> Vec<u8> {
        encode(message)
    }
}

/// The members whose status is not the same in `after` as in `before`, and the ones `after` no longer lists.
///
/// A table lists no member that left, so one that is gone from it left.
fn diff(before: &[Member], after: &[Member]) -> Vec<Transition> {
    let was: HashMap<&NodeId, Status> = before
        .iter()
        .map(|member| (&member.node, member.status))
        .collect();
    let listed: HashSet<&NodeId> = after.iter().map(|member| &member.node).collect();
    let changed = after
        .iter()
        .filter(|member| was.get(&member.node) != Some(&member.status))
        .map(|member| Transition {
            node: member.node.clone(),
            status: member.status,
            previous: was.get(&member.node).copied(),
        });
    let gone = before
        .iter()
        .filter(|member| !listed.contains(&member.node))
        .map(|member| Transition {
            node: member.node.clone(),
            status: Status::Left,
            previous: Some(member.status),
        });
    changed.chain(gone).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use casty_core::membership::table::{Status, Transition};
    use casty_core::node::NodeId;
    use casty_core::rolls::Rolls;

    use super::{Member, diff};

    fn member(node: &NodeId, status: Status) -> Member {
        Member {
            node: node.clone(),
            status,
            types: BTreeSet::new(),
        }
    }

    fn transition(node: &NodeId, status: Status, previous: Option<Status>) -> Transition {
        Transition {
            node: node.clone(),
            status,
            previous,
        }
    }

    #[test]
    fn a_member_in_the_table_for_the_first_time_has_no_previous_status() {
        let ids = Rolls::seeded(81).nodes(2);

        let seen = diff(
            &[member(&ids[0], Status::Alive)],
            &[
                member(&ids[0], Status::Alive),
                member(&ids[1], Status::Alive),
            ],
        );

        assert_eq!(seen, vec![transition(&ids[1], Status::Alive, None)]);
    }

    #[test]
    fn a_crash_is_suspect_then_dead_then_left_and_each_is_said_once() {
        let ids = Rolls::seeded(82).nodes(2);
        let tables = [
            vec![
                member(&ids[0], Status::Alive),
                member(&ids[1], Status::Alive),
            ],
            vec![
                member(&ids[0], Status::Alive),
                member(&ids[1], Status::Suspect),
            ],
            vec![
                member(&ids[0], Status::Alive),
                member(&ids[1], Status::Suspect),
            ],
            vec![
                member(&ids[0], Status::Alive),
                member(&ids[1], Status::Dead),
            ],
            vec![member(&ids[0], Status::Alive)],
        ];

        let seen: Vec<Transition> = tables
            .windows(2)
            .flat_map(|pair| diff(&pair[0], &pair[1]))
            .collect();

        assert_eq!(
            seen,
            vec![
                transition(&ids[1], Status::Suspect, Some(Status::Alive)),
                transition(&ids[1], Status::Dead, Some(Status::Suspect)),
                transition(&ids[1], Status::Left, Some(Status::Dead)),
            ]
        );
    }

    #[test]
    fn a_table_that_changed_nothing_but_the_types_says_nothing() {
        let ids = Rolls::seeded(83).nodes(1);
        let grown = Member {
            types: BTreeSet::from(["tests.app:account".to_owned()]),
            ..member(&ids[0], Status::Alive)
        };

        assert!(diff(&[member(&ids[0], Status::Alive)], &[grown]).is_empty());
    }
}
