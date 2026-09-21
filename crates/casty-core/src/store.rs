//! The copies of the state of each key. Without a cluster there is one copy, on this node.

use std::collections::{BTreeMap, HashMap};

/// The state a node took over for a key: its pages, and whether no replica had the key before.
#[derive(Debug, Clone)]
pub struct Held {
    pub pages: Pages,
    pub created: bool,
}

/// The state of a key, split by the top level fields of its type so that a `save` writes only what changed.
pub type Pages = BTreeMap<String, Vec<u8>>;

/// The store of a node running alone: the pages of each key, and whether the key is active.
#[derive(Debug, Default)]
pub struct LocalStore {
    keys: HashMap<(String, String), Entry>,
}

#[derive(Debug)]
struct Entry {
    pages: Pages,
    active: bool,
}

impl LocalStore {
    /// Take `key` over, from `initial` when nothing has it yet. Nothing and no `initial` means the key does not exist.
    pub fn activate(&mut self, actor: &str, key: &str, initial: Option<Pages>) -> Option<Held> {
        let at = (actor.to_owned(), key.to_owned());
        if let Some(entry) = self.keys.get_mut(&at) {
            entry.active = true;
            return Some(Held {
                pages: entry.pages.clone(),
                created: false,
            });
        }
        let pages = initial?;
        self.keys.insert(
            at,
            Entry {
                pages: pages.clone(),
                active: true,
            },
        );
        Some(Held {
            pages,
            created: true,
        })
    }

    pub fn commit(&mut self, actor: &str, key: &str, pages: Pages, active: bool) {
        self.keys
            .insert((actor.to_owned(), key.to_owned()), Entry { pages, active });
    }

    /// The keys with the active mark, which a node brings back after it restarts.
    #[must_use]
    pub fn active(&self) -> Vec<(String, String)> {
        self.keys
            .iter()
            .filter(|(_, entry)| entry.active)
            .map(|(at, _)| at.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{LocalStore, Pages};

    fn pages(value: u8) -> Pages {
        Pages::from([("balance".to_owned(), vec![value])])
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
}
