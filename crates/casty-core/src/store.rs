//! The copies of the state of each key. Without a cluster there is one copy, on this node.
//!
//! A key is taken over, written, and deleted. A deleted key is activated again from `initial`, as one nothing ever
//! wrote.
//!
//! The state of a durable type is kept by the store of the system too, outside every process: one record per key, the
//! write it was saved from and the state of that write. The version of a record is the stamp of its write, so the
//! store keeps the later of two saves whatever order they reach it in, and a fenced owner never undoes a later one.

use core::time::Duration;
use std::collections::{BTreeMap, HashMap};

use crate::node::NodeId;
use crate::replication::messages::{Epoch, Stamp};
use crate::schema::msgpack::Malformed;
use crate::wire::{Reading, Writer};

/// The state a node took over for a key: its pages, whether no replica had the key before, and the lease of the
/// activation that took it.
///
/// The lease names one activation among those the node started for the key, and a write carries it: the node refuses
/// a write from an activation a later one replaced, whatever that one still holds in memory.
#[derive(Debug, Clone)]
pub struct Held {
    pub pages: Pages,
    pub created: bool,
    pub lease: u64,
}

/// The state of a key, split by the top level fields of its type so that a `save` writes only what changed.
pub type Pages = BTreeMap<String, Vec<u8>>;

/// When the writes of a durable type reach the store of the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durable {
    /// Every confirmed write, before it is confirmed to whoever made it.
    Write,
    /// The latest confirmed write at most this long after it, and the write a key lets go with or is deleted by as
    /// soon as it is confirmed. Nothing waits for the store.
    Every(Duration),
}

/// A write as the store keeps it: the stamp it was confirmed under, and its state, or nothing for a deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub stamp: Stamp,
    pub pages: Option<Pages>,
}

/// What a node asks of the store of its system about one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    /// The record the store keeps of the key, if it keeps one.
    Load,
    /// Keep this write in place of the record, unless the record is of a later one.
    Save(Stored),
    /// Forget the record, if it is of this write or of an earlier one: a deletion every replica has let go of.
    Drop(Stamp),
}

/// How many bytes a version takes.
pub const VERSION: usize = 32;

/// The version a store orders its records by: the round, the incarnation and the version of the stamp, big-endian,
/// which is the order `Stamp::before` compares them in. Comparing two versions as bytes compares their writes.
#[must_use]
pub fn version(stamp: &Stamp) -> [u8; VERSION] {
    let mut version = [0; VERSION];
    version[..8].copy_from_slice(&stamp.epoch.round.to_be_bytes());
    version[8..24].copy_from_slice(&stamp.epoch.node.incarnation);
    version[24..].copy_from_slice(&stamp.version.to_be_bytes());
    version
}

impl Stored {
    /// The version the store compares this record by.
    #[must_use]
    pub fn version(&self) -> [u8; VERSION] {
        version(&self.stamp)
    }

    /// The state as the store keeps it, which it never reads: the pages in msgpack, or nothing for a deletion.
    #[must_use]
    pub fn state(&self) -> Option<Vec<u8>> {
        self.pages.as_ref().map(encode)
    }

    /// A record as the store gave it back. Its stamp comes back without the address of its node, which nothing that
    /// orders writes reads.
    pub fn read(version: &[u8], state: Option<&[u8]>) -> Result<Self, Malformed> {
        Ok(Self {
            stamp: stamp(version)?,
            pages: state.map(decode).transpose()?,
        })
    }
}

fn stamp(version: &[u8]) -> Result<Stamp, Malformed> {
    if version.len() != VERSION {
        return Err(Malformed::Truncated);
    }
    let field = |from: usize, to: usize| version.get(from..to).ok_or(Malformed::Truncated);
    let round = <[u8; 8]>::try_from(field(0, 8)?).map_err(|_| Malformed::Truncated)?;
    let incarnation = <[u8; 16]>::try_from(field(8, 24)?).map_err(|_| Malformed::Truncated)?;
    let written = <[u8; 8]>::try_from(field(24, VERSION)?).map_err(|_| Malformed::Truncated)?;
    Ok(Stamp {
        epoch: Epoch {
            round: u64::from_be_bytes(round),
            node: NodeId {
                address: None,
                incarnation,
            },
        },
        version: u64::from_be_bytes(written),
    })
}

/// The pages of a state as a store keeps them: a `State` holding the list of its pages, so that a later version can
/// add fields this one steps over.
fn encode(pages: &Pages) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("State", 1);
    writer.name("pages");
    writer.pairs(pages.len());
    for (name, data) in pages {
        writer.pair();
        writer.text(name);
        writer.bytes(data);
    }
    writer.finish()
}

fn decode(state: &[u8]) -> Result<Pages, Malformed> {
    let mut reading = Reading::new(state);
    let (tag, fields) = reading.tagged()?;
    if tag != "State" {
        return Err(Malformed::Marker(0));
    }
    let mut pages = Pages::new();
    for _ in 0..fields {
        if reading.name()? != "pages" {
            reading.skip()?;
            continue;
        }
        let count = reading.items()?;
        for _ in 0..count {
            reading.items()?;
            let name = reading.text()?;
            pages.insert(name, reading.bytes()?);
        }
    }
    Ok(pages)
}

/// The node the stamps of a system running alone carry. It needs no incarnation of its own: a key taken from the store
/// is written in a round above the one it was saved in, so each start of the process orders after the one before.
const ALONE: NodeId = NodeId {
    address: None,
    incarnation: [0; 16],
};

/// The store of a node running alone: the pages of each key, and whether the key is active.
#[derive(Debug, Default)]
pub struct LocalStore {
    keys: HashMap<(String, String), Entry>,
}

#[derive(Debug)]
struct Entry {
    pages: Pages,
    active: bool,
    /// The round the key is written in: above the one the store of the system saved it in, when it came from there.
    round: u64,
    /// The last write of the key in that round.
    version: u64,
    /// Whether what the key holds is the tombstone of a deletion the store of the system may not have forgotten yet.
    deleted: bool,
    /// Whether the store of the system has not had the last write, and a save of it is planned.
    unsaved: bool,
}

impl Entry {
    fn new(pages: Pages, round: u64) -> Self {
        Self {
            pages,
            active: true,
            round,
            version: 0,
            deleted: false,
            unsaved: false,
        }
    }

    fn stamp(&self) -> Stamp {
        Stamp {
            epoch: Epoch {
                round: self.round,
                node: ALONE,
            },
            version: self.version,
        }
    }
}

impl LocalStore {
    /// Take `key` over, from `initial` when nothing has it yet. Nothing and no `initial` means the key does not exist.
    ///
    /// A key kept as the tombstone of its deletion starts from `initial` too, its writes going on above the deletion.
    pub fn activate(&mut self, actor: &str, key: &str, initial: Option<Pages>) -> Option<Held> {
        let at = (actor.to_owned(), key.to_owned());
        if let Some(entry) = self.keys.get_mut(&at) {
            if !entry.deleted {
                entry.active = true;
                return Some(Held {
                    pages: entry.pages.clone(),
                    created: false,
                    lease: 0,
                });
            }
            let pages = initial?;
            entry.pages = pages.clone();
            entry.active = true;
            entry.deleted = false;
            return Some(Held {
                pages,
                created: true,
                lease: 0,
            });
        }
        let pages = initial?;
        self.keys.insert(at, Entry::new(pages.clone(), 1));
        Some(Held {
            pages,
            created: true,
            lease: 0,
        })
    }

    /// Whether this node has `key`: its state, or the tombstone of its deletion. Every write of a key this node has was
    /// made here, so what it has is later than what the store of the system keeps.
    #[must_use]
    pub fn knows(&self, actor: &str, key: &str) -> bool {
        self.keys.contains_key(&(actor.to_owned(), key.to_owned()))
    }

    /// Take `key` over from the record the store of the system keeps, when this node does not have it: its state, or
    /// `initial` when the store keeps nothing or a deletion. The writes of the key from here on are in a round above
    /// the record's, so that the store keeps them.
    pub fn restore(
        &mut self,
        actor: &str,
        key: &str,
        stored: Option<Stored>,
        initial: Option<Pages>,
    ) -> Option<Held> {
        if self.knows(actor, key) {
            return self.activate(actor, key, initial);
        }
        let round = stored.as_ref().map_or(0, |record| record.stamp.epoch.round) + 1;
        let (pages, created) = match stored.and_then(|record| record.pages) {
            Some(pages) => (pages, false),
            None => (initial?, true),
        };
        self.keys.insert(
            (actor.to_owned(), key.to_owned()),
            Entry::new(pages.clone(), round),
        );
        Some(Held {
            pages,
            created,
            lease: 0,
        })
    }

    /// Write `key`, giving back the stamp of the write: the version the store of the system keeps it under.
    pub fn commit(&mut self, actor: &str, key: &str, pages: Pages, active: bool) -> Stamp {
        let entry = self
            .keys
            .entry((actor.to_owned(), key.to_owned()))
            .or_insert_with(|| Entry::new(Pages::new(), 1));
        entry.pages = pages;
        entry.active = active;
        entry.deleted = false;
        entry.version += 1;
        entry.stamp()
    }

    /// Forget `key` and its state.
    ///
    /// Nothing of it stays: the one copy there is cannot be outlived by an older one, which is what the tombstone a
    /// replica keeps for a deleted key fences.
    pub fn delete(&mut self, actor: &str, key: &str) {
        self.keys.remove(&(actor.to_owned(), key.to_owned()));
    }

    /// Delete `key` and keep its tombstone, giving back the stamp of the deletion.
    ///
    /// A durable key is deleted this way: the store of the system may keep its state until it is told to forget the
    /// deletion, and an activation meanwhile starts from `initial` without asking the store.
    pub fn tombstone(&mut self, actor: &str, key: &str) -> Stamp {
        let entry = self
            .keys
            .entry((actor.to_owned(), key.to_owned()))
            .or_insert_with(|| Entry::new(Pages::new(), 1));
        entry.pages = Pages::new();
        entry.active = false;
        entry.deleted = true;
        entry.version += 1;
        entry.stamp()
    }

    /// Forget the tombstone of the deletion `stamp`, which the store of the system has forgotten too. A key written again
    /// since keeps what it holds.
    pub fn purge(&mut self, actor: &str, key: &str, stamp: &Stamp) {
        let at = (actor.to_owned(), key.to_owned());
        if self
            .keys
            .get(&at)
            .is_some_and(|entry| entry.deleted && entry.stamp() == *stamp)
        {
            self.keys.remove(&at);
        }
    }

    /// Note that the store of the system has not had the last write of `key`. True when nothing was noted before, which
    /// is when a save of it has to be planned.
    pub fn unsaved(&mut self, actor: &str, key: &str) -> bool {
        self.keys
            .get_mut(&(actor.to_owned(), key.to_owned()))
            .is_some_and(|entry| !core::mem::replace(&mut entry.unsaved, true))
    }

    /// The last write of `key` as the store of the system keeps it, when a save of it is due, taking the note off.
    pub fn due(&mut self, actor: &str, key: &str) -> Option<Stored> {
        let entry = self.keys.get_mut(&(actor.to_owned(), key.to_owned()))?;
        if !core::mem::take(&mut entry.unsaved) {
            return None;
        }
        Some(Stored {
            stamp: entry.stamp(),
            pages: (!entry.deleted).then(|| entry.pages.clone()),
        })
    }

    /// The keys with the active mark, which a node brings back after it restarts.
    #[must_use]
    pub fn active(&self) -> Vec<(String, String)> {
        self.keys
            .iter()
            .filter(|(_, entry)| entry.active && !entry.deleted)
            .map(|(at, _)| at.clone())
            .collect()
    }

    /// Every key with a state here, and whether what it keeps is the tombstone of a deletion.
    #[must_use]
    pub fn kept(&self) -> Vec<(String, String, bool)> {
        self.keys
            .iter()
            .map(|((actor, key), entry)| (actor.clone(), key.clone(), entry.deleted))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalStore, Pages, Stored, VERSION, version};
    use crate::node::NodeId;
    use crate::replication::messages::{Epoch, Stamp};

    fn pages(value: u8) -> Pages {
        Pages::from([("balance".to_owned(), vec![value])])
    }

    fn stamp(round: u64, incarnation: u8, written: u64) -> Stamp {
        Stamp {
            epoch: Epoch {
                round,
                node: NodeId {
                    address: Some("127.0.0.1:7400".to_owned()),
                    incarnation: [incarnation; 16],
                },
            },
            version: written,
        }
    }

    /// A store compares versions as bytes, so two versions must compare the way their stamps do.
    #[test]
    fn versions_compare_as_bytes_the_way_their_stamps_do() {
        let stamps = [
            stamp(1, 9, 7),
            stamp(1, 9, 300),
            stamp(2, 0, 1),
            stamp(2, 3, 1),
            stamp(256, 1, 0),
            stamp(u64::MAX, 0, 0),
        ];

        for earlier in &stamps {
            for later in &stamps {
                assert_eq!(
                    version(earlier) < version(later),
                    earlier.before(later),
                    "{earlier:?} against {later:?}"
                );
            }
        }
        assert_eq!(version(&stamps[0]).len(), VERSION);
    }

    #[test]
    fn a_record_reads_back_as_it_was_saved_but_for_the_address() {
        let state = Pages::from([
            ("balance".to_owned(), vec![1, 2, 3]),
            ("@behavior".to_owned(), b"app:closed".to_vec()),
            ("empty".to_owned(), Vec::new()),
        ]);
        let saved = Stored {
            stamp: stamp(12, 4, 99),
            pages: Some(state.clone()),
        };

        let read = Stored::read(&saved.version(), saved.state().as_deref()).unwrap();

        assert_eq!(read.pages, Some(state));
        assert!(!read.stamp.before(&saved.stamp) && !saved.stamp.before(&read.stamp));
        assert_eq!(read.stamp.epoch.node.address, None);

        let deletion = Stored {
            stamp: stamp(13, 4, 1),
            pages: None,
        };
        assert_eq!(deletion.state(), None);
        let read = Stored::read(&deletion.version(), None).unwrap();
        assert_eq!(read.pages, None);
        assert!(Stored::read(&deletion.version()[1..], None).is_err());
        assert!(Stored::read(&deletion.version(), Some(b"not a state")).is_err());
    }

    /// After a restart the process knows nothing, and a key it takes from the store goes on above the round the store
    /// saved it in: every save it makes from there on is later than the record, so the store keeps it.
    #[test]
    fn a_key_taken_from_the_store_is_written_above_the_round_it_was_saved_in() {
        let mut store = LocalStore::default();
        assert!(!store.knows("a", "k"));
        let record = Stored {
            stamp: stamp(40, 7, 3),
            pages: Some(pages(9)),
        };

        let held = store
            .restore("a", "k", Some(record.clone()), Some(pages(0)))
            .unwrap();

        assert!(!held.created);
        assert_eq!(held.pages, pages(9));
        assert!(store.knows("a", "k"));
        let written = store.commit("a", "k", pages(10), true);
        assert!(record.stamp.before(&written));
        assert!(record.version() < super::version(&written));
    }

    #[test]
    fn a_key_the_store_keeps_nothing_or_a_deletion_of_starts_from_initial() {
        let mut store = LocalStore::default();
        assert!(store.restore("a", "gone", None, None).is_none());
        assert!(!store.knows("a", "gone"));

        let deletion = Stored {
            stamp: stamp(5, 1, 2),
            pages: None,
        };
        let held = store
            .restore("a", "k", Some(deletion.clone()), Some(pages(1)))
            .unwrap();
        assert!(held.created);
        assert_eq!(held.pages, pages(1));
        assert!(
            deletion
                .stamp
                .before(&store.commit("a", "k", pages(2), true))
        );
    }

    /// A durable key deleted alone keeps its tombstone, which an activation starts from `initial` on without asking the
    /// store, until the store has forgotten the deletion.
    #[test]
    fn a_durable_key_keeps_its_tombstone_until_the_store_forgot_the_deletion() {
        let mut store = LocalStore::default();
        store.activate("a", "k", Some(pages(0))).unwrap();
        let written = store.commit("a", "k", pages(7), true);

        let deletion = store.tombstone("a", "k");

        assert!(written.before(&deletion));
        assert!(store.knows("a", "k"));
        assert!(store.active().is_empty());
        assert_eq!(store.kept(), vec![("a".to_owned(), "k".to_owned(), true)]);
        assert!(store.activate("a", "k", None).is_none());
        store.purge("a", "k", &written);
        assert!(
            store.knows("a", "k"),
            "a stamp that is not the deletion purged it"
        );
        store.purge("a", "k", &deletion);
        assert!(store.kept().is_empty());

        // A key activated again before the purge goes on above its deletion, and the purge leaves it alone.
        let deletion = store.tombstone("a", "j");
        let again = store.activate("a", "j", Some(pages(3))).unwrap();
        assert!(again.created);
        assert!(deletion.before(&store.commit("a", "j", pages(4), true)));
        store.purge("a", "j", &deletion);
        assert_eq!(store.kept(), vec![("a".to_owned(), "j".to_owned(), false)]);
    }

    #[test]
    fn a_save_is_planned_once_and_is_due_with_the_last_write() {
        let mut store = LocalStore::default();
        assert!(!store.unsaved("a", "k"), "a key nothing wrote was noted");
        store.activate("a", "k", Some(pages(0))).unwrap();
        store.commit("a", "k", pages(1), true);
        assert!(store.unsaved("a", "k"));
        let last = store.commit("a", "k", pages(2), true);
        assert!(!store.unsaved("a", "k"), "a second save was planned");

        let due = store.due("a", "k").unwrap();

        assert_eq!(due.stamp, last);
        assert_eq!(due.pages, Some(pages(2)));
        assert!(store.due("a", "k").is_none());
        let deletion = store.tombstone("a", "k");
        assert!(store.unsaved("a", "k"));
        assert_eq!(
            store.due("a", "k"),
            Some(Stored {
                stamp: deletion,
                pages: None
            })
        );
    }

    #[test]
    fn a_key_nothing_created_does_not_exist() {
        let mut store = LocalStore::default();
        assert!(store.activate("a", "k", None).is_none());
        assert!(store.active().is_empty());
    }

    #[test]
    fn the_first_activation_creates_and_the_next_finds_what_was_written() {
        let mut store = LocalStore::default();
        let held = store.activate("a", "k", Some(pages(0))).unwrap();
        assert!(held.created);
        store.commit("a", "k", pages(7), true);
        let again = store.activate("a", "k", Some(pages(0))).unwrap();
        assert!(!again.created);
        assert_eq!(again.pages, pages(7));
        assert_eq!(store.active(), vec![("a".to_owned(), "k".to_owned())]);
    }

    #[test]
    fn a_key_whose_body_ended_keeps_its_state_without_the_mark() {
        let mut store = LocalStore::default();
        store.activate("a", "k", Some(pages(0))).unwrap();
        store.commit("a", "k", pages(3), false);
        assert!(store.active().is_empty());
        assert_eq!(store.activate("a", "k", None).unwrap().pages, pages(3));
    }

    #[test]
    fn a_deleted_key_keeps_nothing_and_starts_again_from_initial() {
        let mut store = LocalStore::default();
        store.activate("a", "k", Some(pages(0))).unwrap();
        store.commit("a", "k", pages(7), true);

        store.delete("a", "k");

        assert!(store.kept().is_empty());
        assert!(store.active().is_empty());
        assert!(store.activate("a", "k", None).is_none());
        let again = store.activate("a", "k", Some(pages(1))).unwrap();
        assert!(again.created);
        assert_eq!(again.pages, pages(1));
    }
}
