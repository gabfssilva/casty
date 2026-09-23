//! The member table as a client keeps it: asked for, never gossiped.
//!
//! It answers for `membership`, the component a member replies to. The `Sync` it sends carries only the `NodeId` of
//! the client, so the member that answers neither adds it to its table nor watches it. The table a client holds is
//! up to `every` old, and until the next one arrives a key whose owner changed costs a retry and a key whose owner
//! died waits.
//!
//! A member the transport could not reach is dead to the client, whatever the tables say, until it answers a question
//! of its own: when it was the only node the client knew, nobody is left to say it died.

use std::collections::{HashMap, HashSet};

use casty_core::membership::table::{Status, Transition};
use casty_core::node::NodeId;
use casty_core::rolls::Rolls;
use casty_net::pool::Target;

use super::Members;
use super::service::{Member, Outgoing};
use super::wire::{Body, Message, Sender, decode, encode};

#[derive(Debug)]
pub struct Directory {
    node: NodeId,
    seeds: Vec<String>,
    members: Vec<Member>,
    /// Members the transport could not reach, dead here until one of them answers.
    unreached: HashSet<NodeId>,
    moved: Vec<Transition>,
    rolls: Rolls,
    /// Whether the last question was answered: it goes back to a seed when one was not.
    answered: bool,
    /// Whether the first table has arrived, which is what the start of a client waits for.
    joined: bool,
    changed: bool,
    sends: Vec<Outgoing>,
}

impl Directory {
    #[must_use]
    pub fn new(node: NodeId, seeds: Vec<String>) -> Self {
        Self {
            node,
            seeds,
            members: Vec::new(),
            unreached: HashSet::new(),
            moved: Vec::new(),
            rolls: Rolls::fresh(),
            answered: false,
            joined: false,
            changed: false,
            sends: Vec::new(),
        }
    }

    /// Ask a member for the whole table, or a seed when the last question went unanswered, and ask each member that
    /// was not reached whether it is there again.
    pub fn ask(&mut self) {
        let alive: Vec<NodeId> = self
            .members()
            .into_iter()
            .filter(|member| member.status == Status::Alive)
            .map(|member| member.node)
            .collect();
        let to = if self.answered && !alive.is_empty() {
            Target::Node(self.rolls.pick(&alive).clone())
        } else {
            Target::Seed(self.rolls.pick(&self.seeds).clone())
        };
        self.answered = false;
        self.question(to);
        let unreached: Vec<NodeId> = self.unreached.iter().cloned().collect();
        for node in unreached {
            self.question(Target::Node(node));
        }
    }

    fn question(&mut self, to: Target) {
        self.sends.push(Outgoing {
            to,
            message: Message {
                sender: Sender::Client(self.node.clone()),
                body: Body::Sync(Vec::new()),
            },
        });
    }

    /// The bytes of what is queued, which is what the transport sends.
    #[must_use]
    pub fn payload(message: &Message) -> Vec<u8> {
        encode(message)
    }
}

impl Members for Directory {
    /// The members of the last table, the ones the transport could not reach since then as dead.
    fn members(&self) -> Vec<Member> {
        self.members
            .iter()
            .map(|member| Member {
                status: if self.unreached.contains(&member.node) {
                    Status::Dead
                } else {
                    member.status
                },
                ..member.clone()
            })
            .collect()
    }

    fn take(&mut self) -> Vec<Outgoing> {
        core::mem::take(&mut self.sends)
    }

    /// The answer of a member, which is the only thing that ever arrives here.
    fn receive(&mut self, payload: &[u8], _: f64) {
        let Ok(message) = decode(payload) else {
            return;
        };
        let Body::SyncReply(records) = message.body else {
            return;
        };
        let before = self.members();
        self.answered = true;
        self.unreached.remove(message.sender.node());
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
        // A mark outlives only a member the table still lists up: one it lists dead, or no longer lists, is gone anyway.
        self.unreached.retain(|node| {
            members
                .iter()
                .any(|member| member.node == *node && member.status != Status::Dead)
        });
        self.members = members;
        self.moved.extend(diff(&before, &self.members()));
        self.joined = true;
        self.changed = true;
    }

    fn changed(&mut self) -> bool {
        core::mem::take(&mut self.changed)
    }

    fn mark(&mut self) {
        self.changed = true;
    }

    fn joined(&self) -> bool {
        self.joined
    }

    /// The changes of status since the last call, from one table that arrived to the next.
    ///
    /// A member that went through several statuses between two tables is one change, from the first to the last.
    fn transitions(&mut self) -> Vec<Transition> {
        core::mem::take(&mut self.moved)
    }

    /// A client is in no table: it asks a member for the one it holds.
    fn join(&mut self) {
        self.ask();
    }

    fn anti_entropy(&mut self) {
        self.ask();
    }

    /// No connection to `node` opened, or another incarnation answers at its address. A client has neither a failure
    /// detector nor the gossip of others to wait for, so the node is dead here.
    fn unreached(&mut self, node: &NodeId, _: f64) -> bool {
        if let Some(member) = self.members.iter().find(|member| member.node == *node)
            && member.status != Status::Dead
            && self.unreached.insert(node.clone())
        {
            self.moved.push(Transition {
                node: node.clone(),
                status: Status::Dead,
                previous: Some(member.status),
            });
            self.changed = true;
        }
        true
    }

    /// A connection to `node` ended: ask it for the table at once, which dials it again and says whether it is there.
    fn lost(&mut self, node: &NodeId) {
        if self.members.iter().any(|member| member.node == *node) {
            self.question(Target::Node(node.clone()));
        }
    }

    fn refresh(&mut self) {
        self.ask();
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

    use casty_core::membership::table::{Record, Status, Transition};
    use casty_core::node::NodeId;
    use casty_core::rolls::Rolls;
    use casty_net::pool::Target;

    use super::super::Members;
    use super::super::wire::{Body, Message, Sender, encode};
    use super::{Directory, Member, diff};

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

    fn record(node: &NodeId, status: Status) -> Record {
        Record {
            node: node.clone(),
            incarnation: 0,
            status,
            types: BTreeSet::new(),
        }
    }

    /// The table `from` answers a client with, listing every one of `ids` alive.
    fn table(from: &NodeId, ids: &[NodeId]) -> Vec<u8> {
        encode(&Message {
            sender: Sender::Member(record(from, Status::Alive)),
            body: Body::SyncReply(ids.iter().map(|id| record(id, Status::Alive)).collect()),
        })
    }

    fn statuses(directory: &Directory) -> Vec<Status> {
        directory
            .members()
            .iter()
            .map(|member| member.status)
            .collect()
    }

    #[test]
    fn a_member_the_transport_did_not_reach_is_dead_until_it_answers() {
        let ids = Rolls::seeded(84).nodes(3);
        let mut directory = Directory::new(ids[2].clone(), vec!["10.0.0.1:7400".to_owned()]);
        directory.receive(&table(&ids[0], &ids[..2]), 0.0);
        directory.transitions();

        directory.unreached(&ids[1], 0.0);

        assert_eq!(statuses(&directory), vec![Status::Alive, Status::Dead]);
        assert_eq!(
            directory.transitions(),
            vec![transition(&ids[1], Status::Dead, Some(Status::Alive))]
        );
        // Another member still lists it up, which says nothing about whether this client reaches it.
        directory.receive(&table(&ids[0], &ids[..2]), 0.0);
        assert_eq!(statuses(&directory), vec![Status::Alive, Status::Dead]);
        assert!(directory.transitions().is_empty());

        directory.receive(&table(&ids[1], &ids[..2]), 0.0);

        assert_eq!(statuses(&directory), vec![Status::Alive, Status::Alive]);
        assert_eq!(
            directory.transitions(),
            vec![transition(&ids[1], Status::Alive, Some(Status::Dead))]
        );
    }

    #[test]
    fn a_member_not_reached_is_asked_again_with_every_table_question() {
        let ids = Rolls::seeded(85).nodes(3);
        let mut directory = Directory::new(ids[2].clone(), vec!["10.0.0.1:7400".to_owned()]);
        directory.receive(&table(&ids[0], &ids[..2]), 0.0);
        directory.unreached(&ids[1], 0.0);
        directory.take();

        directory.ask();

        let asked: Vec<Target> = directory.take().into_iter().map(|out| out.to).collect();
        assert_eq!(
            asked,
            vec![Target::Node(ids[0].clone()), Target::Node(ids[1].clone())]
        );
    }

    #[test]
    fn a_lost_connection_asks_the_member_at_once() {
        let ids = Rolls::seeded(86).nodes(2);
        let mut directory = Directory::new(ids[1].clone(), vec!["10.0.0.1:7400".to_owned()]);
        directory.receive(&table(&ids[0], &ids[..1]), 0.0);

        directory.lost(&ids[0]);

        let asked: Vec<Target> = directory.take().into_iter().map(|out| out.to).collect();
        assert_eq!(asked, vec![Target::Node(ids[0].clone())]);
        assert_eq!(statuses(&directory), vec![Status::Alive]);
    }
}
