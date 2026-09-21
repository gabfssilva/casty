//! The copies of every key that this node replicates, kept in memory.

use std::collections::HashMap;

use super::messages::{ACTIVE, Copy, Epoch, Reply, Request, Stamp};
use crate::node::NodeId;
use crate::store::Pages;

/// Bytes a page of `size` bytes takes out of the message limit.
#[must_use]
pub fn cost(name: &str, size: usize) -> usize {
    name.len() + size
}

/// Page names grouped in order so that each group fits in a message.
#[must_use]
pub fn chunks(sizes: &[(String, usize)], limit: usize) -> Vec<Vec<String>> {
    let mut grouped: Vec<Vec<String>> = vec![Vec::new()];
    let mut held = 0;
    for (name, length) in sizes {
        let last = grouped.last_mut().expect("a group is always open");
        if !last.is_empty() && held + cost(name, *length) > limit {
            grouped.push(vec![name.clone()]);
            held = cost(name, *length);
        } else {
            last.push(name.clone());
            held += cost(name, *length);
        }
    }
    grouped
}

#[derive(Debug, Default)]
struct Staged {
    stamp: Stamp,
    full: bool,
    parts: u32,
    pages: Pages,
    dropped: Vec<String>,
}

impl Staged {
    fn applied_to(&self, pages: &Pages) -> Pages {
        if self.full {
            return self.pages.clone();
        }
        let mut grown = pages.clone();
        for (name, data) in &self.pages {
            grown.insert(name.clone(), data.clone());
        }
        grown.retain(|name, _| !self.dropped.contains(name));
        grown
    }
}

impl Default for Stamp {
    fn default() -> Self {
        Self {
            epoch: Epoch {
                round: 0,
                node: NodeId {
                    address: None,
                    incarnation: [0; 16],
                },
            },
            version: 0,
        }
    }
}

#[derive(Debug, Default)]
struct Entry {
    promised: Option<Epoch>,
    accepted: Option<Stamp>,
    pages: Pages,
    staged: Option<Staged>,
}

#[derive(Debug)]
pub struct Replica {
    node: NodeId,
    limit: usize,
    entries: HashMap<(String, String), Entry>,
}

impl Replica {
    /// `limit` is the number of bytes of page names and data that fit in one message.
    #[must_use]
    pub fn new(node: NodeId, limit: usize) -> Self {
        Self {
            node,
            limit,
            entries: HashMap::new(),
        }
    }

    /// The epoch this replica promised for `key`, which is where an owner takes the round to beat.
    #[must_use]
    pub fn promised(&self, actor: &str, key: &str) -> Option<&Epoch> {
        self.entries
            .get(&(actor.to_owned(), key.to_owned()))
            .and_then(|entry| entry.promised.as_ref())
    }

    /// The keys of `actor` this replica keeps, which is where a range read starts.
    #[must_use]
    pub fn keys(&self, actor: &str) -> Vec<String> {
        self.entries
            .keys()
            .filter(|(held, _)| held == actor)
            .map(|(_, key)| key.clone())
            .collect()
    }

    /// Whether the copy kept here carries the active mark, which is what tells a sweep to bring the key back.
    #[must_use]
    pub fn marked(&self, actor: &str, key: &str) -> bool {
        self.entries
            .get(&(actor.to_owned(), key.to_owned()))
            .is_some_and(|entry| entry.pages.contains_key(ACTIVE))
    }

    /// Forget a key this node stopped replicating, once every node that replicates it now has taken it.
    pub fn drop(&mut self, actor: &str, key: &str) {
        self.entries.remove(&(actor.to_owned(), key.to_owned()));
    }

    /// The key as it stands here, in as many parts as the message limit takes.
    #[must_use]
    pub fn copies(&self, actor: &str, key: &str) -> Vec<Copy> {
        let Some(entry) = self.entries.get(&(actor.to_owned(), key.to_owned())) else {
            return Vec::new();
        };
        let sizes: Vec<(String, usize)> = entry
            .pages
            .iter()
            .map(|(name, data)| (name.clone(), data.len()))
            .collect();
        let parts = chunks(&sizes, self.limit);
        let last = parts.len() - 1;
        parts
            .into_iter()
            .enumerate()
            .map(|(part, names)| Copy {
                key: key.to_owned(),
                accepted: entry.accepted.clone(),
                promised: entry.promised.clone(),
                pages: names
                    .into_iter()
                    .filter_map(|name| entry.pages.get(&name).map(|data| (name, data.clone())))
                    .collect(),
                final_part: part == last,
            })
            .collect()
    }

    /// Take a key from another replica: keep the later write and the later promise.
    ///
    /// Both, and not only the state: a state without the promise it was written under would let an owner that the
    /// replicas of the previous ring already fenced write here again.
    pub fn install(&mut self, actor: &str, copy: Copy) {
        let entry = self
            .entries
            .entry((actor.to_owned(), copy.key.clone()))
            .or_default();
        if let Some(accepted) = &copy.accepted
            && entry
                .accepted
                .as_ref()
                .is_none_or(|held| held.before(accepted))
        {
            entry.accepted = Some(accepted.clone());
            entry.pages = copy.pages;
            entry.staged = None;
        }
        if let Some(promised) = &copy.promised
            && entry
                .promised
                .as_ref()
                .is_none_or(|held| held.before(promised))
        {
            entry.promised = Some(promised.clone());
        }
    }

    /// Apply `request` and give back the reply for its owner, if any.
    ///
    /// `receiving` tells that this node has not yet received the range of the key, so its replies do not count for
    /// quorums.
    pub fn receive(&mut self, request: Request, receiving: bool) -> Option<Reply> {
        let at = (request.actor().to_owned(), request.key().to_owned());
        self.entries.entry(at).or_default();
        match request {
            Request::Prepare { actor, key, epoch } => self.prepare(actor, key, epoch, receiving),
            Request::Accept {
                actor,
                key,
                stamp,
                base,
                part,
                final_part,
                pages,
                dropped,
            } => self.accept(
                actor,
                key,
                stamp,
                base.as_ref(),
                part,
                final_part,
                pages,
                dropped,
                receiving,
            ),
            Request::FetchPages {
                actor,
                key,
                epoch,
                names,
            } => self.fetch(actor, key, epoch, names),
        }
    }

    /// Promise a term, unless a later one was already promised. The answer carries what this replica holds.
    fn prepare(
        &mut self,
        actor: String,
        key: String,
        epoch: Epoch,
        receiving: bool,
    ) -> Option<Reply> {
        let entry = self.entries.get_mut(&(actor.clone(), key.clone()))?;
        if let Some(promised) = &entry.promised
            && !promised.before(&epoch)
        {
            return Some(Reply::Rejected {
                actor,
                key,
                replica: self.node.clone(),
                promised: promised.clone(),
            });
        }
        entry.promised = Some(epoch.clone());
        let sizes: Vec<(String, usize)> = entry
            .pages
            .iter()
            .map(|(name, data)| (name.clone(), data.len()))
            .collect();
        let held: usize = sizes.iter().map(|(name, size)| cost(name, *size)).sum();
        Some(Reply::Promise {
            actor,
            key,
            epoch,
            replica: self.node.clone(),
            accepted: entry.accepted.clone(),
            sizes,
            pages: if held <= self.limit {
                entry.pages.clone()
            } else {
                Pages::new()
            },
            receiving,
        })
    }

    /// Take one part of a write. Only the last part of a whole and ordered write is published.
    #[allow(clippy::too_many_arguments)]
    fn accept(
        &mut self,
        actor: String,
        key: String,
        stamp: Stamp,
        base: Option<&Stamp>,
        part: u32,
        final_part: bool,
        pages: Pages,
        dropped: Vec<String>,
        receiving: bool,
    ) -> Option<Reply> {
        let entry = self.entries.get_mut(&(actor.clone(), key.clone()))?;
        if let Some(promised) = &entry.promised
            && stamp.epoch.before(promised)
        {
            return Some(Reply::Rejected {
                actor,
                key,
                replica: self.node.clone(),
                promised: promised.clone(),
            });
        }
        entry.promised = Some(stamp.epoch.clone());
        if part == 0 {
            if base.is_some() && base != entry.accepted.as_ref() {
                entry.staged = None;
                return Some(Reply::NeedFull {
                    actor,
                    key,
                    stamp,
                    replica: self.node.clone(),
                });
            }
            entry.staged = Some(Staged {
                stamp: stamp.clone(),
                full: base.is_none(),
                ..Staged::default()
            });
        }
        let staged = entry.staged.as_mut()?;
        if staged.stamp != stamp || staged.parts != part {
            entry.staged = None;
            return None;
        }
        for (name, data) in pages {
            staged.pages.insert(name, data);
        }
        staged.dropped.extend(dropped);
        staged.parts += 1;
        if !final_part {
            return None;
        }
        let staged = entry.staged.take().expect("the staged write of this part");
        entry.pages = staged.applied_to(&entry.pages);
        entry.accepted = Some(stamp.clone());
        Some(Reply::Accepted {
            actor,
            key,
            stamp,
            replica: self.node.clone(),
            receiving,
        })
    }

    fn fetch(
        &mut self,
        actor: String,
        key: String,
        epoch: Epoch,
        names: Vec<String>,
    ) -> Option<Reply> {
        let entry = self.entries.get(&(actor.clone(), key.clone()))?;
        let found = names
            .into_iter()
            .filter_map(|name| entry.pages.get(&name).map(|data| (name, data.clone())))
            .collect();
        Some(Reply::Pages {
            actor,
            key,
            epoch,
            accepted: entry.accepted.clone(),
            pages: found,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::messages::{ACTIVE, Copy, Epoch, Reply, Request, Stamp};
    use super::{Replica, chunks};
    use crate::node::NodeId;
    use crate::rolls::Rolls;
    use crate::store::Pages;

    fn nodes(count: usize) -> Vec<NodeId> {
        Rolls::seeded(21).nodes(count)
    }

    fn epoch(node: &NodeId, round: u64) -> Epoch {
        Epoch {
            round,
            node: node.clone(),
        }
    }

    fn stamp(node: &NodeId, round: u64, version: u64) -> Stamp {
        Stamp {
            epoch: epoch(node, round),
            version,
        }
    }

    fn pages(entries: &[(&str, &[u8])]) -> Pages {
        entries
            .iter()
            .map(|(name, data)| ((*name).to_owned(), (*data).to_vec()))
            .collect()
    }

    fn whole(actor: &str, key: &str, stamp: Stamp, pages: Pages) -> Request {
        Request::Accept {
            actor: actor.to_owned(),
            key: key.to_owned(),
            stamp,
            base: None,
            part: 0,
            final_part: true,
            pages,
            dropped: Vec::new(),
        }
    }

    #[test]
    fn a_prepare_of_an_older_term_is_refused_and_the_promise_stands() {
        let held = nodes(3);
        let mut replica = Replica::new(held[0].clone(), 1024);

        let promised = replica.receive(
            Request::Prepare {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                epoch: epoch(&held[1], 2),
            },
            false,
        );

        assert!(matches!(promised, Some(Reply::Promise { .. })));
        assert_eq!(replica.promised("account", "a"), Some(&epoch(&held[1], 2)));
        let refused = replica.receive(
            Request::Prepare {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                epoch: epoch(&held[2], 1),
            },
            false,
        );
        let Some(Reply::Rejected { promised, .. }) = refused else {
            panic!("an older term was promised: {refused:?}");
        };
        assert_eq!(promised, epoch(&held[1], 2));
        assert_eq!(replica.promised("account", "a"), Some(&epoch(&held[1], 2)));
    }

    #[test]
    fn a_write_of_a_fenced_owner_is_refused() {
        let held = nodes(3);
        let mut replica = Replica::new(held[0].clone(), 1024);
        replica.receive(
            Request::Prepare {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                epoch: epoch(&held[1], 5),
            },
            false,
        );

        let refused = replica.receive(
            whole(
                "account",
                "a",
                stamp(&held[2], 4, 1),
                pages(&[("balance", b"1")]),
            ),
            false,
        );

        assert!(
            matches!(refused, Some(Reply::Rejected { .. })),
            "{refused:?}"
        );
        // The state was not touched by the write that was refused.
        assert!(!replica.marked("account", "a"));
        assert!(replica.copies("account", "a")[0].pages.is_empty());
    }

    #[test]
    fn a_write_is_published_only_on_its_last_part_and_only_in_order() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 1024);
        let written = stamp(&held[1], 1, 1);

        let first = replica.receive(
            Request::Accept {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                stamp: written.clone(),
                base: None,
                part: 0,
                final_part: false,
                pages: pages(&[("balance", b"1")]),
                dropped: Vec::new(),
            },
            false,
        );
        assert!(first.is_none(), "a part that is not the last was published");
        assert!(replica.copies("account", "a")[0].pages.is_empty());

        // A part out of order drops what was staged, and nothing is published.
        let skipped = replica.receive(
            Request::Accept {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                stamp: written.clone(),
                base: None,
                part: 5,
                final_part: true,
                pages: pages(&[("tags", b"x")]),
                dropped: Vec::new(),
            },
            false,
        );
        assert!(skipped.is_none());
        assert!(replica.copies("account", "a")[0].accepted.is_none());

        let done = replica.receive(
            whole("account", "a", written.clone(), pages(&[("balance", b"7")])),
            false,
        );
        let Some(Reply::Accepted {
            stamp: accepted, ..
        }) = done
        else {
            panic!("the whole write was not accepted: {done:?}");
        };
        assert_eq!(accepted, written);
        assert_eq!(
            replica.copies("account", "a")[0].pages,
            pages(&[("balance", b"7")])
        );
    }

    #[test]
    fn a_delta_against_a_write_this_replica_does_not_have_asks_for_the_whole_state() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 1024);
        let first = stamp(&held[1], 1, 1);
        replica.receive(
            whole("account", "a", first.clone(), pages(&[("balance", b"1")])),
            false,
        );

        let refused = replica.receive(
            Request::Accept {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                stamp: stamp(&held[1], 1, 3),
                base: Some(stamp(&held[1], 1, 2)),
                part: 0,
                final_part: true,
                pages: pages(&[("balance", b"2")]),
                dropped: Vec::new(),
            },
            false,
        );

        assert!(
            matches!(refused, Some(Reply::NeedFull { .. })),
            "{refused:?}"
        );
        assert_eq!(replica.copies("account", "a")[0].accepted, Some(first));
    }

    #[test]
    fn a_delta_on_the_write_it_holds_changes_only_the_pages_it_names() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 1024);
        let first = stamp(&held[1], 1, 1);
        replica.receive(
            whole(
                "account",
                "a",
                first.clone(),
                pages(&[("balance", b"1"), ("tags", b"x"), (ACTIVE, b"")]),
            ),
            false,
        );

        let next = stamp(&held[1], 1, 2);
        replica.receive(
            Request::Accept {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                stamp: next.clone(),
                base: Some(first),
                part: 0,
                final_part: true,
                pages: pages(&[("balance", b"9")]),
                dropped: vec!["tags".to_owned()],
            },
            false,
        );

        let copy = &replica.copies("account", "a")[0];
        assert_eq!(copy.accepted, Some(next));
        assert_eq!(copy.pages, pages(&[("balance", b"9"), (ACTIVE, b"")]));
        assert!(
            replica.marked("account", "a"),
            "the active mark was lost by a delta"
        );
    }

    #[test]
    fn installing_a_copy_keeps_the_later_write_and_the_later_promise() {
        let held = nodes(3);
        let mut replica = Replica::new(held[0].clone(), 1024);
        replica.install(
            "account",
            Copy {
                key: "a".to_owned(),
                accepted: Some(stamp(&held[1], 2, 1)),
                promised: Some(epoch(&held[1], 2)),
                pages: pages(&[("balance", b"5")]),
                final_part: true,
            },
        );

        // An older write is ignored, and the newer promise that came with it is kept.
        replica.install(
            "account",
            Copy {
                key: "a".to_owned(),
                accepted: Some(stamp(&held[1], 1, 9)),
                promised: Some(epoch(&held[2], 3)),
                pages: pages(&[("balance", b"0")]),
                final_part: true,
            },
        );

        let copy = &replica.copies("account", "a")[0];
        assert_eq!(copy.accepted, Some(stamp(&held[1], 2, 1)));
        assert_eq!(copy.pages, pages(&[("balance", b"5")]));
        assert_eq!(replica.promised("account", "a"), Some(&epoch(&held[2], 3)));
    }

    #[test]
    fn a_key_larger_than_a_message_crosses_in_parts_that_add_up_to_it() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 32);
        let written = pages(&[("one", &[1; 20]), ("two", &[2; 20]), ("three", &[3; 20])]);
        replica.receive(
            whole("account", "a", stamp(&held[1], 1, 1), written.clone()),
            false,
        );

        let parts = replica.copies("account", "a");

        assert!(
            parts.len() > 1,
            "a key larger than a message crossed in one part"
        );
        assert!(
            parts
                .iter()
                .take(parts.len() - 1)
                .all(|part| !part.final_part)
        );
        assert!(parts[parts.len() - 1].final_part);
        let mut rebuilt = Pages::new();
        for part in parts {
            rebuilt.extend(part.pages);
        }
        assert_eq!(rebuilt, written);
    }

    #[test]
    fn a_group_of_pages_never_goes_past_the_message_limit_on_its_own() {
        let sizes = vec![
            ("a".to_owned(), 10),
            ("b".to_owned(), 10),
            ("c".to_owned(), 100),
            ("d".to_owned(), 1),
        ];

        let grouped = chunks(&sizes, 30);

        assert_eq!(
            grouped,
            vec![
                vec!["a".to_owned(), "b".to_owned()],
                vec!["c".to_owned()],
                vec!["d".to_owned()]
            ]
        );
        // A page that does not fit on its own still travels, alone.
        assert_eq!(
            chunks(&[("big".to_owned(), 1_000)], 10),
            vec![vec!["big".to_owned()]]
        );
        assert_eq!(chunks(&[], 10), vec![Vec::<String>::new()]);
    }
}
