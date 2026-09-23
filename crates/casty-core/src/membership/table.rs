//! Members of the cluster as one node sees them, with the SWIM merge rules.
//!
//! Instants are monotonic seconds passed by the caller. Every method gives back the records that changed, and the
//! caller is what broadcasts them. The changes of status are kept, in the order they were made, until the caller
//! takes them.

use std::collections::{BTreeSet, HashMap};

use crate::node::NodeId;

/// How a member is seen. The order is the precedence of a merge: a later status wins at the same incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Status {
    Alive,
    Leaving,
    Suspect,
    Dead,
    Left,
}

impl Status {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Leaving => "leaving",
            Self::Suspect => "suspect",
            Self::Dead => "dead",
            Self::Left => "left",
        }
    }

    #[must_use]
    pub fn of(written: &str) -> Option<Self> {
        match written {
            "alive" => Some(Self::Alive),
            "leaving" => Some(Self::Leaving),
            "suspect" => Some(Self::Suspect),
            "dead" => Some(Self::Dead),
            "left" => Some(Self::Left),
            _ => None,
        }
    }
}

/// What a node knows about a member: its SWIM incarnation number, status and registered actor types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub node: NodeId,
    pub incarnation: u64,
    pub status: Status,
    pub types: BTreeSet<String>,
}

impl Record {
    /// What decides a merge: `left` beats any incarnation, then the higher incarnation, then the later status.
    fn rank(&self) -> (bool, u64, Status) {
        (self.status == Status::Left, self.incarnation, self.status)
    }
}

/// A member whose status changed in the table: to `status`, from `previous`, or from nothing when the table did not
/// hold it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub node: NodeId,
    pub status: Status,
    pub previous: Option<Status>,
}

/// A `left` record is forgotten a minute after it was written.
const TOMBSTONE: f64 = 60.0;

#[derive(Debug)]
pub struct MemberTable {
    node: NodeId,
    dead_after: f64,
    remove_after: Option<f64>,
    records: HashMap<NodeId, Record>,
    since: HashMap<NodeId, f64>,
    moved: Vec<Transition>,
}

impl MemberTable {
    #[must_use]
    pub fn new(
        node: NodeId,
        types: BTreeSet<String>,
        dead_after: f64,
        remove_after: Option<f64>,
    ) -> Self {
        let mine = Record {
            node: node.clone(),
            incarnation: 0,
            status: Status::Alive,
            types,
        };
        Self {
            records: HashMap::from([(node.clone(), mine)]),
            since: HashMap::from([(node.clone(), 0.0)]),
            node,
            dead_after,
            remove_after,
            moved: Vec::new(),
        }
    }

    #[must_use]
    pub fn me(&self) -> &Record {
        &self.records[&self.node]
    }

    /// Every known record, this node's and the `left` ones included.
    pub fn records(&self) -> impl Iterator<Item = &Record> {
        self.records.values()
    }

    #[must_use]
    pub fn record(&self, node: &NodeId) -> Option<&Record> {
        self.records.get(node)
    }

    /// The changes of status since the last call, in the order the table made them, each one once.
    ///
    /// A record that only raises the incarnation changes no status, and neither does a `left` record of a member the
    /// table does not hold: that is the tombstone of a departure it never saw, or already forgot.
    pub fn transitions(&mut self) -> Vec<Transition> {
        core::mem::take(&mut self.moved)
    }

    /// Apply an observation and give back the record that changed, if any.
    ///
    /// A suspicion of this node is refuted: its own record takes the next incarnation, and that record is what comes
    /// back, so that the cluster hears the refutation.
    pub fn merge(&mut self, record: Record, now: f64) -> Option<Record> {
        if let Some(current) = self.records.get(&record.node)
            && record.rank() <= current.rank()
        {
            return None;
        }
        let record = if record.node == self.node && record.status != Status::Left {
            if matches!(record.status, Status::Alive | Status::Leaving) {
                return None;
            }
            Record {
                incarnation: record.incarnation + 1,
                ..self.me().clone()
            }
        } else {
            record
        };
        self.set(record.clone(), now);
        Some(record)
    }

    /// Merge the record of a node that joined through this one, replacing other incarnations on its address.
    ///
    /// A port serves one process, so an older incarnation on the same address is no longer running and becomes
    /// `left` without waiting for failure detection.
    pub fn admit(&mut self, record: Record, now: f64) -> Vec<Record> {
        let replaced: Vec<Record> = self
            .records
            .values()
            .filter(|other| other.node.address == record.node.address && other.node != record.node)
            .map(|other| Record {
                status: Status::Left,
                ..other.clone()
            })
            .collect();
        core::iter::once(record)
            .chain(replaced)
            .filter_map(|observed| self.merge(observed, now))
            .collect()
    }

    /// Add `types` to the ones this node has, under the next incarnation so that the record replaces the old one.
    pub fn know(&mut self, types: &BTreeSet<String>, now: f64) -> Option<Record> {
        if types.is_subset(&self.me().types) {
            return None;
        }
        let record = Record {
            incarnation: self.me().incarnation + 1,
            types: self.me().types.union(types).cloned().collect(),
            ..self.me().clone()
        };
        self.set(record.clone(), now);
        Some(record)
    }

    /// Mark this node `leaving`, the first step of an orderly exit.
    pub fn leave(&mut self, now: f64) -> Record {
        self.mark(Status::Leaving, now)
    }

    /// Mark this node `left`, the last step of an orderly exit: terminal, and no ring keeps it any more.
    pub fn depart(&mut self, now: f64) -> Record {
        self.mark(Status::Left, now)
    }

    /// Apply the timeouts.
    ///
    /// `suspect` becomes `dead` after `dead_after`. `dead` becomes `left` after `remove_after`, only while this node
    /// sees a `majority`. `left` records are forgotten after a minute.
    pub fn expire(&mut self, now: f64) -> Vec<Record> {
        let majority = self.majority();
        let mut changed = Vec::new();
        let mut forgotten = Vec::new();
        for (node, record) in &self.records {
            let elapsed = now - self.since[node];
            match record.status {
                Status::Suspect if elapsed >= self.dead_after => changed.push(Record {
                    status: Status::Dead,
                    ..record.clone()
                }),
                Status::Dead
                    if majority && self.remove_after.is_some_and(|after| elapsed >= after) =>
                {
                    changed.push(Record {
                        status: Status::Left,
                        ..record.clone()
                    });
                }
                Status::Left if *node != self.node && elapsed >= TOMBSTONE => {
                    forgotten.push(node.clone());
                }
                _ => {}
            }
        }
        for node in forgotten {
            self.records.remove(&node);
            self.since.remove(&node);
        }
        for record in &changed {
            self.set(record.clone(), now);
        }
        changed
    }

    /// Whether this node sees more than half of the members that have not left as `alive`, itself included: two sides
    /// of a partition cannot both see that.
    #[must_use]
    pub fn majority(&self) -> bool {
        let left = self
            .records
            .values()
            .filter(|held| held.status == Status::Left)
            .count();
        let alive = self
            .records
            .values()
            .filter(|held| held.status == Status::Alive)
            .count();
        2 * alive > self.records.len() - left
    }

    fn mark(&mut self, status: Status, now: f64) -> Record {
        let record = Record {
            status,
            ..self.me().clone()
        };
        self.set(record.clone(), now);
        record
    }

    fn set(&mut self, record: Record, now: f64) {
        let previous = self.records.get(&record.node).map(|held| held.status);
        let moved = match previous {
            Some(previous) => previous != record.status,
            None => record.status != Status::Left,
        };
        if moved {
            self.moved.push(Transition {
                node: record.node.clone(),
                status: record.status,
                previous,
            });
        }
        self.since.insert(record.node.clone(), now);
        self.records.insert(record.node.clone(), record);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{MemberTable, Record, Status, Transition};
    use crate::node::NodeId;
    use crate::rolls::Rolls;

    fn types() -> BTreeSet<String> {
        BTreeSet::from(["account".to_owned()])
    }

    fn node(index: u8) -> NodeId {
        let mut incarnation = [0_u8; 16];
        incarnation[15] = index;
        NodeId {
            address: Some(format!("10.0.0.{index}:7400")),
            incarnation,
        }
    }

    fn table(me: &NodeId) -> MemberTable {
        MemberTable::new(me.clone(), types(), 0.5, Some(1.0))
    }

    fn seen(table: &MemberTable) -> BTreeMap<NodeId, (u64, Status)> {
        table
            .records()
            .map(|record| (record.node.clone(), (record.incarnation, record.status)))
            .collect()
    }

    fn heard(index: u8, incarnation: u64, status: Status) -> Record {
        Record {
            node: node(index),
            incarnation,
            status,
            types: types(),
        }
    }

    fn transition(index: u8, status: Status, previous: Option<Status>) -> Transition {
        Transition {
            node: node(index),
            status,
            previous,
        }
    }

    #[test]
    fn every_change_of_status_is_reported_once_in_the_order_the_table_made_it() {
        let me = node(0);
        let mut held = table(&me);
        for index in 1..=3 {
            held.merge(heard(index, 0, Status::Alive), 0.0);
        }
        held.merge(heard(1, 0, Status::Suspect), 0.0);
        held.merge(heard(1, 0, Status::Suspect), 0.1);

        // Nobody took them between the two changes of the first member, and both are there.
        assert_eq!(
            held.transitions(),
            vec![
                transition(1, Status::Alive, None),
                transition(2, Status::Alive, None),
                transition(3, Status::Alive, None),
                transition(1, Status::Suspect, Some(Status::Alive)),
            ]
        );
        assert!(
            held.transitions().is_empty(),
            "a transition was reported twice"
        );

        held.expire(0.6);
        held.expire(2.0);

        assert_eq!(
            held.transitions(),
            vec![
                transition(1, Status::Dead, Some(Status::Suspect)),
                transition(1, Status::Left, Some(Status::Dead)),
            ]
        );
    }

    #[test]
    fn a_new_incarnation_a_refutation_or_the_tombstone_of_a_stranger_changes_no_status() {
        let me = node(0);
        let mut held = table(&me);
        held.merge(heard(1, 0, Status::Alive), 0.0);
        assert_eq!(held.transitions().len(), 1);

        held.merge(heard(1, 1, Status::Alive), 0.0);
        held.merge(
            Record {
                node: me.clone(),
                incarnation: 3,
                status: Status::Suspect,
                types: types(),
            },
            0.0,
        );
        held.know(&BTreeSet::from(["order".to_owned()]), 0.0);
        held.merge(heard(2, 0, Status::Left), 0.0);

        assert!(held.transitions().is_empty());
    }

    #[test]
    fn this_node_going_away_is_leaving_then_left() {
        let me = node(0);
        let mut held = table(&me);

        held.leave(0.0);
        held.depart(0.1);

        assert_eq!(
            held.transitions(),
            vec![
                transition(0, Status::Leaving, Some(Status::Alive)),
                transition(0, Status::Left, Some(Status::Leaving)),
            ]
        );
    }

    #[test]
    fn the_same_observations_in_any_order_reach_the_same_table() {
        let mut rolls = Rolls::seeded(6);
        let me = node(0);
        let others: Vec<NodeId> = (1..=6).map(node).collect();
        let statuses = [
            Status::Alive,
            Status::Leaving,
            Status::Suspect,
            Status::Dead,
        ];
        let gone = &others[..2];
        let mut observations: Vec<Record> = Vec::new();
        for held in &others {
            for _ in 0..6 {
                observations.push(Record {
                    node: held.clone(),
                    incarnation: rolls.upto(4) as u64,
                    status: statuses[rolls.upto(statuses.len())],
                    types: types(),
                });
            }
        }
        for held in gone {
            observations.push(Record {
                node: held.clone(),
                incarnation: 1,
                status: Status::Left,
                types: types(),
            });
        }
        for _ in 0..4 {
            observations.push(Record {
                node: me.clone(),
                incarnation: rolls.upto(4) as u64,
                status: if rolls.upto(2) == 0 {
                    Status::Suspect
                } else {
                    Status::Dead
                },
                types: types(),
            });
        }

        let merged = |order: &[Record]| {
            let mut held = table(&me);
            for record in order {
                held.merge(record.clone(), 0.0);
            }
            held
        };
        let first = merged(&observations);
        for _ in 0..50 {
            let other = merged(&rolls.shuffled(&observations));
            assert_eq!(
                seen(&other),
                seen(&first),
                "the order of the gossip changed the table"
            );
        }

        // `left` is terminal, and this node refutes every suspicion of itself.
        for held in gone {
            assert_eq!(
                first.record(held).map(|record| record.status),
                Some(Status::Left)
            );
        }
        let mut observer = table(&others[5]);
        for record in observations.iter().chain(core::iter::once(first.me())) {
            observer.merge(record.clone(), 0.0);
        }
        assert_eq!(
            observer.record(&me).map(|record| record.status),
            Some(Status::Alive)
        );
    }

    #[test]
    fn a_suspicion_of_this_node_comes_back_as_a_refutation_under_a_higher_incarnation() {
        let me = node(0);
        let mut held = table(&me);

        let refuted = held.merge(
            Record {
                node: me.clone(),
                incarnation: 7,
                status: Status::Suspect,
                types: types(),
            },
            0.0,
        );

        let refuted = refuted.expect("a suspicion of this node is answered");
        assert_eq!(refuted.status, Status::Alive);
        assert_eq!(refuted.incarnation, 8);
        assert!(
            held.merge(
                Record {
                    node: me.clone(),
                    incarnation: 3,
                    status: Status::Alive,
                    types: types()
                },
                0.0
            )
            .is_none()
        );
    }

    #[test]
    fn a_node_that_joins_on_the_address_of_another_replaces_it() {
        let me = node(0);
        let mut held = table(&me);
        let old = node(1);
        let new = NodeId {
            address: old.address.clone(),
            incarnation: [9; 16],
        };
        held.merge(
            Record {
                node: old.clone(),
                incarnation: 0,
                status: Status::Alive,
                types: types(),
            },
            0.0,
        );

        let changed = held.admit(
            Record {
                node: new.clone(),
                incarnation: 0,
                status: Status::Alive,
                types: types(),
            },
            0.0,
        );

        assert_eq!(changed.len(), 2);
        assert_eq!(
            held.record(&new).map(|record| record.status),
            Some(Status::Alive)
        );
        assert_eq!(
            held.record(&old).map(|record| record.status),
            Some(Status::Left)
        );
    }

    #[test]
    fn a_suspect_becomes_dead_and_then_left_only_with_a_majority() {
        let me = node(0);
        let mut held = table(&me);
        for index in 1..=4 {
            held.merge(
                Record {
                    node: node(index),
                    incarnation: 0,
                    status: Status::Alive,
                    types: types(),
                },
                0.0,
            );
        }
        held.merge(
            Record {
                node: node(1),
                incarnation: 1,
                status: Status::Suspect,
                types: types(),
            },
            0.0,
        );

        assert!(held.expire(0.4).is_empty(), "it gave up before dead_after");
        let dead = held.expire(0.6);
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].status, Status::Dead);
        // Four of five alive: this side is the majority, so the dead one is removed.
        let gone = held.expire(2.0);
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].status, Status::Left);
    }

    #[test]
    fn a_minority_never_removes_what_it_sees_as_dead() {
        let me = node(0);
        let mut held = table(&me);
        for index in 1..=4 {
            held.merge(
                Record {
                    node: node(index),
                    incarnation: 1,
                    status: Status::Suspect,
                    types: types(),
                },
                0.0,
            );
        }

        let dead = held.expire(1.0);

        assert_eq!(dead.len(), 4);
        // One of five alive is not a majority, so nothing is taken out of the cluster on this side.
        assert!(held.expire(5.0).is_empty());
    }

    #[test]
    fn learning_a_type_raises_the_incarnation_so_the_record_replaces_the_old_one() {
        let me = node(0);
        let mut held = table(&me);
        let before = held.me().incarnation;

        let grown = held
            .know(&BTreeSet::from(["order".to_owned()]), 0.0)
            .expect("a new type");

        assert_eq!(grown.incarnation, before + 1);
        assert!(grown.types.contains("order") && grown.types.contains("account"));
        assert!(
            held.know(&types(), 0.0).is_none(),
            "a type it already had changed the record"
        );
    }
}
