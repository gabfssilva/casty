//! The copies of every key that this node replicates, kept in memory.

use std::collections::HashMap;

use super::messages::{ACTIVE, Copy, DELETED, Epoch, Reply, Request, Stamp, tombstone};
use super::parts::{append, cost, cut, plan, sizes, split};
use crate::node::NodeId;
use crate::store::Pages;

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

impl Entry {
    /// The deletion this entry holds the tombstone of, when that is what it holds.
    fn deletion(&self) -> Option<&Stamp> {
        self.accepted
            .as_ref()
            .filter(|_| self.pages.contains_key(DELETED))
    }
}

/// A key deleted, the tombstone kept here in place of its state, and the deletion that wrote it.
pub type Laid = (String, String, Stamp);

/// The copies of the keys this node replicates.
///
/// A deleted key keeps its tombstone here, with no page but the mark, until every other replica of it has said it
/// keeps nothing older (`Bury`); only then is it forgotten, and with it the promise it held. A tombstone moves between
/// nodes like any copy, so a replica that takes one fences the older copies the others still hand it.
#[derive(Debug)]
pub struct Replica {
    node: NodeId,
    limit: usize,
    entries: HashMap<(String, String), Entry>,
    laid: Vec<Laid>,
}

impl Replica {
    /// `limit` is the number of bytes of page names and data that fit in one message.
    #[must_use]
    pub fn new(node: NodeId, limit: usize) -> Self {
        Self {
            node,
            limit,
            entries: HashMap::new(),
            laid: Vec::new(),
        }
    }

    /// The epoch this replica promised for `key`, which is where an owner takes the round to beat.
    #[must_use]
    pub fn promised(&self, actor: &str, key: &str) -> Option<&Epoch> {
        self.entries
            .get(&(actor.to_owned(), key.to_owned()))
            .and_then(|entry| entry.promised.as_ref())
    }

    /// The write this replica last accepted for `key`, which names the copy it keeps.
    #[must_use]
    pub fn accepted(&self, actor: &str, key: &str) -> Option<&Stamp> {
        self.entries
            .get(&(actor.to_owned(), key.to_owned()))
            .and_then(|entry| entry.accepted.as_ref())
    }

    /// The deletion `key` was last written with, when what this replica keeps of it is the tombstone.
    #[must_use]
    pub fn deleted(&self, actor: &str, key: &str) -> Option<&Stamp> {
        self.entries
            .get(&(actor.to_owned(), key.to_owned()))
            .and_then(Entry::deletion)
    }

    /// Every key kept here, and whether what it keeps is a tombstone, for whoever looks at what a node holds.
    #[must_use]
    pub fn kept(&self) -> Vec<(String, String, bool)> {
        self.entries
            .iter()
            .map(|((actor, key), entry)| (actor.clone(), key.clone(), entry.deletion().is_some()))
            .collect()
    }

    /// The tombstones laid here since the last call: by a deletion, by a copy that carried one, or by a burial.
    pub fn laid(&mut self) -> Vec<Laid> {
        core::mem::take(&mut self.laid)
    }

    /// Forget the tombstone of the deletion `stamp`, which every other replica of the key has answered for. What a
    /// key holds that is not that tombstone, a write that came after it, stays.
    pub fn purge(&mut self, actor: &str, key: &str, stamp: &Stamp) {
        let at = (actor.to_owned(), key.to_owned());
        if self
            .entries
            .get(&at)
            .and_then(Entry::deletion)
            .is_some_and(|held| held == stamp)
        {
            self.entries.remove(&at);
        }
    }

    /// The keys of `actor` this replica keeps, tombstones included, which is where a range read starts.
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
        let parts = split(&entry.pages, self.limit);
        let last = parts.len() - 1;
        parts
            .into_iter()
            .enumerate()
            .map(|(part, pages)| Copy {
                key: key.to_owned(),
                accepted: entry.accepted.clone(),
                promised: entry.promised.clone(),
                pages,
                #[allow(clippy::cast_possible_truncation)]
                part: part as u32,
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
            // A tombstone that came this way lingers here as one laid by the deletion itself.
            if entry.pages.contains_key(DELETED) {
                self.laid
                    .push((actor.to_owned(), copy.key.clone(), accepted.clone()));
            }
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
        // A burial is the one request that must not leave an entry behind: a key with nothing here stays that way.
        if !matches!(request, Request::Bury { .. }) {
            let at = (request.actor().to_owned(), request.key().to_owned());
            self.entries.entry(at).or_default();
        }
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
                part,
            } => self.fetch(actor, key, epoch, &names, part),
            Request::Bury {
                actor, key, stamp, ..
            } => Some(self.bury(actor, key, stamp, receiving)),
        }
    }

    /// Answer for the copy of `key` kept here against the deletion `stamp`, taking its tombstone in place of an older
    /// write.
    ///
    /// The answer is the same whether this replica held that write or had nothing: either way, nothing here is older
    /// than the deletion any more, which is what the one asking waits to hear from every replica before it forgets
    /// its tombstone.
    fn bury(&mut self, actor: String, key: String, stamp: Stamp, receiving: bool) -> Reply {
        if let Some(entry) = self.entries.get_mut(&(actor.clone(), key.clone()))
            && entry
                .accepted
                .as_ref()
                .is_some_and(|held| held.before(&stamp))
        {
            entry.accepted = Some(stamp.clone());
            entry.pages = tombstone();
            entry.staged = None;
            if entry
                .promised
                .as_ref()
                .is_none_or(|promised| promised.before(&stamp.epoch))
            {
                entry.promised = Some(stamp.epoch.clone());
            }
            self.laid.push((actor.clone(), key.clone(), stamp.clone()));
        }
        Reply::Buried {
            actor,
            key,
            stamp,
            replica: self.node.clone(),
            receiving,
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
        let indexed = sizes(&entry.pages);
        let held: usize = indexed.iter().map(|(name, size)| cost(name, *size)).sum();
        Some(Reply::Promise {
            actor,
            key,
            epoch,
            replica: self.node.clone(),
            accepted: entry.accepted.clone(),
            sizes: indexed,
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
        append(&mut staged.pages, pages);
        staged.dropped.extend(dropped);
        staged.parts += 1;
        if !final_part {
            return None;
        }
        let staged = entry.staged.take().expect("the staged write of this part");
        entry.pages = staged.applied_to(&entry.pages);
        entry.accepted = Some(stamp.clone());
        if entry.pages.contains_key(DELETED) {
            self.laid.push((actor.clone(), key.clone(), stamp.clone()));
        }
        Some(Reply::Accepted {
            actor,
            key,
            stamp,
            replica: self.node.clone(),
            receiving,
        })
    }

    /// Part `part` of the pages `names`, cut the same way on every call, so that the parts add up to the pages for as
    /// long as the accepted write they come from stays the same.
    fn fetch(
        &self,
        actor: String,
        key: String,
        epoch: Epoch,
        names: &[String],
        part: u32,
    ) -> Option<Reply> {
        let entry = self.entries.get(&(actor.clone(), key.clone()))?;
        let wanted: Vec<(String, usize)> = names
            .iter()
            .filter_map(|name| entry.pages.get(name).map(|data| (name.clone(), data.len())))
            .collect();
        let parts = plan(&wanted, self.limit);
        let at = usize::try_from(part).unwrap_or(usize::MAX);
        Some(Reply::Pages {
            actor,
            key,
            epoch,
            accepted: entry.accepted.clone(),
            part,
            final_part: at >= parts.len() - 1,
            pages: parts
                .get(at)
                .map_or_else(Pages::new, |pieces| cut(&entry.pages, pieces)),
        })
    }
}

/// Keys arriving from another node in the numbered messages of a stream, put back together before anything of them
/// is installed.
///
/// The messages of a stream come in order, and the parts of a key one right after the other. A key some part of which
/// did not come is never installed, and a stream some message of which did not come is broken: what the sender meant
/// to hand over is not all here, even when every key that came is whole. A message of another stream starts over
/// from it, because nothing of the one before goes on in it.
#[derive(Debug, Default)]
pub struct Arriving {
    stream: Option<u64>,
    next: u32,
    held: Option<Copy>,
    broken: bool,
}

impl Arriving {
    /// Take message `part` of the stream `stream`, installing each key whose last part is in, and say which keys
    /// landed.
    pub fn take(
        &mut self,
        actor: &str,
        stream: u64,
        part: u32,
        copies: Vec<Copy>,
        replica: &mut Replica,
    ) -> Vec<String> {
        if self.stream != Some(stream) {
            *self = Self {
                stream: Some(stream),
                ..Self::default()
            };
        }
        if part != self.next {
            // The key a missing message went on with cannot be joined to what follows it.
            self.broken = true;
            self.held = None;
        }
        self.next = part.saturating_add(1);
        let mut landed = Vec::new();
        for copy in copies {
            let joined = match self.held.take() {
                Some(held) if held.key == copy.key && held.part + 1 == copy.part => {
                    let mut pages = held.pages;
                    append(&mut pages, copy.pages);
                    Some(Copy { pages, ..copy })
                }
                held => {
                    // What was held misses the rest of it, and a part past the first goes on from one not here.
                    self.broken |= held.is_some() || copy.part != 0;
                    (copy.part == 0).then_some(copy)
                }
            };
            match joined {
                Some(whole) if whole.final_part => {
                    landed.push(whole.key.clone());
                    replica.install(actor, whole);
                }
                started => self.held = started,
            }
        }
        landed
    }

    /// Whether every message of the stream and every part of its keys came, which is when what came is all the sender
    /// meant.
    #[must_use]
    pub fn whole(&self) -> bool {
        !self.broken && self.held.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::super::messages::{ACTIVE, Copy, Epoch, Reply, Request, Stamp, tombstone};
    use super::super::parts::{append, cost, split};
    use super::{Arriving, Replica};
    use crate::node::NodeId;
    use crate::rolls::Rolls;
    use crate::store::Pages;

    fn nodes(count: usize) -> Vec<NodeId> {
        Rolls::seeded(21).nodes(count)
    }

    /// `size` bytes that differ from one another, so that a piece out of place shows.
    fn long(size: usize) -> Vec<u8> {
        (0..=u8::MAX).cycle().take(size).collect()
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
                part: 0,
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
                part: 0,
                final_part: true,
            },
        );

        let copy = &replica.copies("account", "a")[0];
        assert_eq!(copy.accepted, Some(stamp(&held[1], 2, 1)));
        assert_eq!(copy.pages, pages(&[("balance", b"5")]));
        assert_eq!(replica.promised("account", "a"), Some(&epoch(&held[2], 3)));
    }

    #[test]
    fn a_key_larger_than_a_message_crosses_in_numbered_parts_that_add_up_to_it() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 64);
        let written = pages(&[
            ("entries", long(300).as_slice()),
            ("owner", b"ana"),
            (ACTIVE, b""),
        ]);
        replica.receive(
            whole("account", "a", stamp(&held[1], 1, 1), written.clone()),
            false,
        );

        let parts = replica.copies("account", "a");

        assert!(
            parts.len() > 300 / 64,
            "{} parts for a page of 300 bytes",
            parts.len()
        );
        let mut rebuilt = Pages::new();
        for (at, part) in parts.iter().enumerate() {
            assert_eq!(usize::try_from(part.part), Ok(at));
            assert_eq!(part.final_part, at == parts.len() - 1);
            let carried: usize = part
                .pages
                .iter()
                .map(|(name, data)| cost(name, data.len()))
                .sum();
            assert!(carried <= 64, "a part carries {carried} bytes");
            append(&mut rebuilt, part.pages.clone());
        }
        assert_eq!(rebuilt, written);
    }

    #[test]
    fn a_page_written_in_parts_is_exposed_only_once_its_last_part_is_in() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 64);
        let written = pages(&[("entries", long(300).as_slice()), (ACTIVE, b"")]);
        let parts = split(&written, 64);
        let last = parts.len() - 1;
        let requests: Vec<Request> = parts
            .into_iter()
            .enumerate()
            .map(|(part, pages)| Request::Accept {
                actor: "account".to_owned(),
                key: "a".to_owned(),
                stamp: stamp(&held[1], 1, 1),
                base: None,
                part: u32::try_from(part).expect("a few parts"),
                final_part: part == last,
                pages,
                dropped: Vec::new(),
            })
            .collect();
        assert!(
            requests.len() > 1,
            "a page larger than a message was written in one part"
        );

        let mut replies = Vec::new();
        for request in requests {
            assert!(
                replica.accepted("account", "a").is_none(),
                "a write was published before its last part"
            );
            assert!(!replica.marked("account", "a"));
            replies.extend(replica.receive(request, false));
        }

        assert!(
            matches!(replies.as_slice(), [Reply::Accepted { .. }]),
            "{replies:?}"
        );
        let mut rebuilt = Pages::new();
        for copy in replica.copies("account", "a") {
            append(&mut rebuilt, copy.pages);
        }
        assert_eq!(rebuilt, written);
    }

    #[test]
    fn a_fetch_answers_in_parts_that_add_up_to_the_pages_it_names() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 64);
        let mut written = pages(&[
            ("entries", long(300).as_slice()),
            ("owner", b"ana"),
            ("tags", b"x"),
        ]);
        replica.receive(
            whole("account", "a", stamp(&held[1], 1, 1), written.clone()),
            false,
        );
        let names = vec!["entries".to_owned(), "owner".to_owned()];

        let mut fetched = Pages::new();
        let mut part = 0;
        loop {
            let reply = replica.receive(
                Request::FetchPages {
                    actor: "account".to_owned(),
                    key: "a".to_owned(),
                    epoch: epoch(&held[1], 2),
                    names: names.clone(),
                    part,
                },
                false,
            );
            let Some(Reply::Pages {
                part: answered,
                final_part,
                pages,
                ..
            }) = reply
            else {
                panic!("a fetch was not answered with pages: {reply:?}");
            };
            assert_eq!(answered, part);
            let carried: usize = pages
                .iter()
                .map(|(name, data)| cost(name, data.len()))
                .sum();
            assert!(carried <= 64, "a part carries {carried} bytes");
            append(&mut fetched, pages);
            if final_part {
                break;
            }
            part += 1;
            assert!(part < 100, "the parts never ended");
        }

        written.remove("tags");
        assert!(part > 0, "a page larger than a message came in one part");
        assert_eq!(fetched, written);
    }

    #[test]
    fn a_key_a_part_of_which_went_missing_is_never_installed() {
        let held = nodes(2);
        let mut source = Replica::new(held[0].clone(), 64);
        let mut target = Replica::new(held[1].clone(), 64);
        let small = pages(&[("owner", b"ana")]);
        source.receive(
            whole(
                "account",
                "a",
                stamp(&held[0], 1, 1),
                pages(&[("entries", long(300).as_slice())]),
            ),
            false,
        );
        source.receive(
            whole("account", "b", stamp(&held[0], 1, 1), small.clone()),
            false,
        );
        let mut parts = source.copies("account", "a");
        assert!(parts.len() > 2, "{} parts", parts.len());
        parts.remove(1);
        parts.extend(source.copies("account", "b"));

        let mut arriving = Arriving::default();
        let landed = arriving.take("account", 1, 0, parts, &mut target);

        assert_eq!(landed, vec!["b".to_owned()]);
        assert!(
            target.accepted("account", "a").is_none(),
            "a key with a part missing was installed"
        );
        assert!(!arriving.whole());
        assert_eq!(target.copies("account", "b")[0].pages, small);
    }

    /// A message of small keys that went missing leaves no key with a gap, and the stream is broken all the same.
    #[test]
    fn a_stream_a_message_of_which_went_missing_is_broken_though_every_key_that_came_is_whole() {
        let held = nodes(2);
        let mut source = Replica::new(held[0].clone(), 64);
        let mut target = Replica::new(held[1].clone(), 64);
        for key in ["a", "b", "c"] {
            source.receive(
                whole(
                    "account",
                    key,
                    stamp(&held[0], 1, 1),
                    pages(&[("owner", b"ana")]),
                ),
                false,
            );
        }

        let mut arriving = Arriving::default();
        arriving.take("account", 1, 0, source.copies("account", "a"), &mut target);
        arriving.take("account", 1, 2, source.copies("account", "c"), &mut target);

        assert!(
            !arriving.whole(),
            "a stream with a message missing is whole"
        );
        assert!(target.accepted("account", "c").is_some());
    }

    /// Another stream starts over, so neither a key cut in the one before nor the message it lost carries into it.
    #[test]
    fn a_message_of_another_stream_starts_over_from_it() {
        let held = nodes(2);
        let mut source = Replica::new(held[0].clone(), 64);
        let mut target = Replica::new(held[1].clone(), 64);
        let large = pages(&[("entries", long(300).as_slice())]);
        source.receive(
            whole("account", "a", stamp(&held[0], 1, 1), large.clone()),
            false,
        );
        source.receive(
            whole(
                "account",
                "b",
                stamp(&held[0], 1, 1),
                pages(&[("owner", b"ana")]),
            ),
            false,
        );
        let parts = source.copies("account", "a");
        assert!(parts.len() > 2, "{} parts", parts.len());

        // The first stream ends after the first part of `a`, and its last message never comes.
        let mut arriving = Arriving::default();
        arriving.take("account", 1, 0, vec![parts[0].clone()], &mut target);
        // The next one lost its first message, and goes on with the part of `a` that would have come next in the first.
        arriving.take("account", 2, 1, vec![parts[1].clone()], &mut target);
        assert!(!arriving.whole());
        assert!(target.accepted("account", "a").is_none());

        // A stream that comes whole from its first message is whole, whatever came before it.
        let mut messages = parts;
        messages.extend(source.copies("account", "b"));
        let mut landed = Vec::new();
        for (part, copy) in messages.into_iter().enumerate() {
            let part = u32::try_from(part).expect("a few parts");
            landed.extend(arriving.take("account", 3, part, vec![copy], &mut target));
        }
        assert_eq!(landed, vec!["a".to_owned(), "b".to_owned()]);
        assert!(arriving.whole());
        let mut rebuilt = Pages::new();
        for copy in target.copies("account", "a") {
            append(&mut rebuilt, copy.pages);
        }
        assert_eq!(rebuilt, large);
    }

    fn bury(key: &str, stamp: Stamp, node: &NodeId) -> Request {
        Request::Bury {
            actor: "account".to_owned(),
            key: key.to_owned(),
            stamp,
            node: node.clone(),
        }
    }

    #[test]
    fn a_deletion_leaves_a_tombstone_that_lingers_and_fences_older_copies() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 1024);
        replica.receive(
            whole(
                "account",
                "a",
                stamp(&held[1], 1, 1),
                pages(&[("balance", b"1"), (ACTIVE, b"")]),
            ),
            false,
        );
        assert!(replica.laid().is_empty());
        let deletion = stamp(&held[1], 1, 2);

        let reply = replica.receive(whole("account", "a", deletion.clone(), tombstone()), false);

        assert!(matches!(reply, Some(Reply::Accepted { .. })), "{reply:?}");
        assert_eq!(replica.deleted("account", "a"), Some(&deletion));
        assert!(!replica.marked("account", "a"));
        assert_eq!(replica.copies("account", "a")[0].pages, tombstone());
        assert_eq!(
            replica.laid(),
            vec![("account".to_owned(), "a".to_owned(), deletion.clone())]
        );
        assert!(replica.laid().is_empty(), "a tombstone was laid twice");

        // An older copy another node still hands over does not bring the state back.
        replica.install(
            "account",
            Copy {
                key: "a".to_owned(),
                accepted: Some(stamp(&held[1], 1, 1)),
                promised: None,
                pages: pages(&[("balance", b"1")]),
                part: 0,
                final_part: true,
            },
        );
        assert_eq!(replica.deleted("account", "a"), Some(&deletion));
        assert_eq!(replica.copies("account", "a")[0].pages, tombstone());
    }

    #[test]
    fn a_burial_takes_the_tombstone_in_place_of_an_older_copy_and_leaves_nothing_where_there_was_nothing()
     {
        let held = nodes(3);
        let deletion = stamp(&held[1], 2, 1);
        let mut stale = Replica::new(held[0].clone(), 1024);
        stale.receive(
            whole(
                "account",
                "a",
                stamp(&held[1], 1, 4),
                pages(&[("balance", b"7"), (ACTIVE, b"")]),
            ),
            false,
        );

        let answered = stale.receive(bury("a", deletion.clone(), &held[2]), false);

        let Some(Reply::Buried {
            stamp: buried,
            receiving,
            ..
        }) = answered
        else {
            panic!("a burial was not answered: {answered:?}");
        };
        assert_eq!(buried, deletion);
        assert!(!receiving);
        assert_eq!(stale.deleted("account", "a"), Some(&deletion));
        assert!(!stale.marked("account", "a"));
        assert_eq!(stale.promised("account", "a"), Some(&epoch(&held[1], 2)));
        assert_eq!(stale.laid().len(), 1);

        // A replica that keeps nothing of the key answers the same, and still keeps nothing.
        let mut empty = Replica::new(held[0].clone(), 1024);
        let answered = empty.receive(bury("a", deletion.clone(), &held[2]), true);
        assert!(
            matches!(
                answered,
                Some(Reply::Buried {
                    receiving: true,
                    ..
                })
            ),
            "{answered:?}"
        );
        assert!(empty.keys("account").is_empty());
        assert!(empty.laid().is_empty());

        // A key written again after the deletion keeps what it holds.
        let mut later = Replica::new(held[0].clone(), 1024);
        let rewritten = pages(&[("balance", b"0"), (ACTIVE, b"")]);
        later.receive(
            whole("account", "a", stamp(&held[1], 3, 1), rewritten.clone()),
            false,
        );
        later.receive(bury("a", deletion, &held[2]), false);
        assert_eq!(later.copies("account", "a")[0].pages, rewritten);
        assert!(later.laid().is_empty());
    }

    #[test]
    fn only_the_tombstone_a_purge_names_is_forgotten() {
        let held = nodes(2);
        let mut replica = Replica::new(held[0].clone(), 1024);
        let deletion = stamp(&held[1], 1, 2);
        replica.receive(whole("account", "a", deletion.clone(), tombstone()), false);

        replica.purge("account", "a", &stamp(&held[1], 1, 1));
        assert_eq!(replica.deleted("account", "a"), Some(&deletion));

        replica.purge("account", "a", &deletion);
        assert!(replica.keys("account").is_empty());
        assert!(replica.kept().is_empty());

        // What a key holds that is not a tombstone is never purged.
        let written = stamp(&held[1], 1, 1);
        replica.receive(
            whole("account", "b", written.clone(), pages(&[("balance", b"1")])),
            false,
        );
        replica.purge("account", "b", &written);
        assert_eq!(replica.keys("account"), vec!["b".to_owned()]);
    }
}
