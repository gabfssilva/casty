//! An index: a key with the values listed under it, which is what a set, a dict and a multimap are kept in.
//!
//! A shard of an index is a directory and the segments it counts, each segment a key of its own that lists the keys
//! whose hash it holds, so that a write rewrites one segment and not the shard. The shard grows by linear hashing: when
//! a write finds its segment full, the directory splits the next segment in order, moving the keys of half its hashes
//! to a new segment after the last. The directory counts a split only once it is done, and a segment refuses a key it
//! does not hold, so a caller that has not seen a split is told to ask again and no key is ever listed twice.
//!
//! A dict lists each key once, under the generation of its entry. `List` never lowers it and `Unlist` drops only a
//! listing that is not newer than the one it names, so a message of an earlier generation that arrives late changes
//! nothing. `List` answers whether the key is listed under the generation it names, which it is not when it was
//! listed under a newer one.

use blake2::digest::consts::U8;
use blake2::{Blake2b, Digest};
use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{
    Asking, Given, Native, Turn, count, named, nil, number, number_in, optional_bytes, truth,
};

const SEGMENTS: &str = "segments";
const ENTRIES: &str = "entries";
const MODULUS: &str = "modulus";
const ID: &str = "id";
const VERSION: &str = "version";

/// The keys a segment lists before a write to it asks for a split. A write rewrites its segment, so this is what
/// bounds its cost.
const KEYS: usize = 512;

/// The bits of the hash that tell the segments of a shard apart, which is as far as a shard splits.
const BITS: u32 = 32;

/// What a key holds, in the order the entries were written.
pub type Entries = Vec<(Vec<u8>, Vec<Vec<u8>>)>;

/// The directory of a shard: how many segments it has, which is all a caller needs to find the one a key is in.
#[derive(Debug)]
pub struct Table;

impl Native for Table {
    fn initial(&self) -> Pages {
        counted(1)
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let (message, answered) = match given {
            Given::Message(message) => (*message, false),
            Given::Answered { message, .. } => (*message, true),
            Given::Alarm => return Turn::default(),
        };
        let Ok(read) = read(message) else {
            return Turn::default();
        };
        let segments = number_in(held, SEGMENTS, 0).max(1);
        let mut turn = Turn::default();
        let answer = match named(&read.tag) {
            "Segments" => number(segments),
            "Grow" => {
                let (Some(seen), Some(split), Some(into)) = (read.seen, read.split, read.into)
                else {
                    return Turn::default();
                };
                match doubled(segments) {
                    // Only the split the caller counted from is made, so callers that found segments full at once
                    // make one between them.
                    Some(modulus) if seen == segments => {
                        if !answered {
                            turn.ask = Some(Asking {
                                to: split,
                                message: Box::new(move |reply| splitting(reply, &into, modulus)),
                            });
                            return turn;
                        }
                        turn.save = Some(counted(segments + 1));
                        number(segments + 1)
                    }
                    _ => number(segments),
                }
            }
            _ => return Turn::default(),
        };
        turn.replies = vec![(read.reply, answer)];
        turn
    }
}

/// A segment of a shard: the keys whose hash leaves `id` over `modulus`, with the values listed under each.
#[derive(Debug)]
pub struct Segment;

impl Native for Segment {
    fn initial(&self) -> Pages {
        Bucket::first().pages()
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let (message, answered) = match given {
            Given::Message(message) => (*message, false),
            Given::Answered { message, .. } => (*message, true),
            Given::Alarm => return Turn::default(),
        };
        let Ok(read) = read(message) else {
            return Turn::default();
        };
        let before = Bucket::of(held);
        let mut bucket = before.clone();
        let adopting = named(&read.tag) == "Adopt";
        let mut turn = Turn::default();
        let answer = match named(&read.tag) {
            "Split" => {
                let (Some(into), Some(modulus)) = (read.into, read.modulus) else {
                    return Turn::default();
                };
                // Asked again after a split whose count the directory did not save, the segment is split already.
                if bucket.modulus < modulus {
                    let (kept, moved) = bucket.parted(modulus);
                    if !answered {
                        let (id, version) = (bucket.id + modulus / 2, bucket.version);
                        turn.ask = Some(Asking {
                            to: into,
                            message: Box::new(move |reply| {
                                adoption(reply, &moved, modulus, id, version)
                            }),
                        });
                        return turn;
                    }
                    bucket.entries = kept;
                    bucket.modulus = modulus;
                }
                nil()
            }
            "Adopt" => {
                let (Some(entries), Some(modulus), Some(id), Some(version)) =
                    (read.entries, read.modulus, read.id, read.version)
                else {
                    return Turn::default();
                };
                // An attempt at the split that did not finish adopted an older version of the segment the keys came
                // from, and its adoption may arrive after the one that did: it never replaces it.
                if (modulus, version) > (bucket.modulus, bucket.version) {
                    bucket = Bucket {
                        entries,
                        modulus,
                        id,
                        version,
                    };
                }
                nil()
            }
            tag => {
                let Some(answer) =
                    bucket.answer(tag, read.key, read.value, read.generation, read.position)
                else {
                    return Turn::default();
                };
                answer
            }
        };
        if bucket != before {
            // An adoption keeps the version of the segment the keys came from; every other write raises it.
            if !adopting {
                bucket.version = bucket.version.saturating_add(1);
            }
            turn.save = Some(bucket.pages());
        }
        turn.replies = vec![(read.reply, answer)];
        turn
    }
}

/// What a segment holds: its keys, the hashes it holds them by, and how many times it was written.
#[derive(Debug, Clone, PartialEq)]
struct Bucket {
    entries: Entries,
    modulus: i64,
    id: i64,
    version: i64,
}

impl Bucket {
    /// The first segment of a shard, which holds every hash until it splits.
    fn first() -> Self {
        Self {
            entries: Entries::new(),
            modulus: 1,
            id: 0,
            version: 0,
        }
    }

    fn of(pages: &Pages) -> Self {
        Self {
            entries: pages
                .get(ENTRIES)
                .and_then(|page| read_entries(&mut Reading::new(page)).ok())
                .unwrap_or_default(),
            modulus: number_in(pages, MODULUS, 0).max(1),
            id: number_in(pages, ID, 0),
            version: number_in(pages, VERSION, 0),
        }
    }

    fn pages(&self) -> Pages {
        Pages::from([
            (ENTRIES.to_owned(), written(&self.entries)),
            (MODULUS.to_owned(), number(self.modulus)),
            (ID.to_owned(), number(self.id)),
            (VERSION.to_owned(), number(self.version)),
        ])
    }

    /// The answer to a message about keys, or nothing for a message a segment does not take.
    ///
    /// A key the segment does not hold is refused before anything is read or written, with None for an answer: the
    /// caller has not seen a split, and asks the directory where the key went. A scan is refused the same way at a
    /// position whose hash the segment does not hold.
    fn answer(
        &mut self,
        tag: &str,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        generation: Option<i64>,
        position: Option<i64>,
    ) -> Option<Vec<u8>> {
        if let Some(key) = &key
            && !hashes(key, self.modulus, self.id)
        {
            return Some(nil());
        }
        Some(match (tag, key) {
            ("Add", Some(key)) => {
                let value = value?;
                let listed = at(&mut self.entries, &key);
                let changed = !listed.contains(&value);
                if changed {
                    listed.push(value);
                }
                wrote(changed, self.full())
            }
            ("Get", Some(key)) => listed(&self.entries, &key),
            ("Remove", Some(key)) => {
                number(count(remove(&mut self.entries, &key, value.as_deref())))
            }
            ("List", Some(key)) => {
                let generation = generation?;
                let listed = at(&mut self.entries, &key);
                let newer = !listed.is_empty() && listed_under(listed) > generation;
                if !newer {
                    *listed = vec![number(generation)];
                }
                wrote(!newer, self.full())
            }
            ("Unlist", Some(key)) => {
                let generation = generation?;
                let found = self
                    .entries
                    .iter()
                    .position(|(held, values)| *held == key && listed_under(values) <= generation);
                if let Some(found) = found {
                    self.entries.remove(found);
                }
                truth(found.is_some())
            }
            ("Size", None) => walked(self.modulus, |writer| writer.int(self.values())),
            ("Scan", None) => {
                if holds(position?, self.modulus, self.id) {
                    walked(self.modulus, |writer| write_entries(writer, &self.entries))
                } else {
                    nil()
                }
            }
            ("Clear", None) => {
                let dropped = self.values();
                self.entries.clear();
                walked(self.modulus, |writer| writer.int(dropped))
            }
            _ => return None,
        })
    }

    /// The entries that stay once the hashes are told apart over `modulus`, and those that go to the new segment.
    fn parted(&self, modulus: i64) -> (Entries, Entries) {
        self.entries
            .iter()
            .cloned()
            .partition(|(key, _)| hashes(key, modulus, self.id))
    }

    /// Whether a write should ask the directory for a split.
    fn full(&self) -> bool {
        self.entries.len() > KEYS
    }

    /// How many values are listed, which is the size of a set or of a multimap.
    fn values(&self) -> i64 {
        self.entries
            .iter()
            .map(|(_, values)| count(values.len()))
            .sum()
    }
}

/// The hash that places a key among the segments of its shard: the high half of the `blake2b/8` digest, read big
/// endian, that the facade also chooses the shard by.
fn spread(key: &[u8]) -> u64 {
    let mut hasher = Blake2b::<U8>::new();
    hasher.update(key);
    u64::from_be_bytes(hasher.finalize().into()) >> BITS
}

/// Whether `key` hashes to `id` over `modulus`.
fn hashes(key: &[u8], modulus: i64, id: i64) -> bool {
    i64::try_from(spread(key)).is_ok_and(|spread| holds(spread, modulus, id))
}

/// Whether the hash `spread` is `id` over `modulus`.
fn holds(spread: i64, modulus: i64, id: i64) -> bool {
    spread % modulus.max(1) == id
}

/// The modulus the next split of a shard of `segments` takes its segment to, while the hash has bits left to split by.
fn doubled(segments: i64) -> Option<i64> {
    let level = segments.checked_ilog2()?;
    (level < BITS).then_some(2_i64 << level)
}

fn counted(segments: i64) -> Pages {
    Pages::from([(SEGMENTS.to_owned(), number(segments))])
}

/// The values under `key`, listed there if they were not before.
fn at<'a>(entries: &'a mut Entries, key: &[u8]) -> &'a mut Vec<Vec<u8>> {
    let at = entries
        .iter()
        .position(|(held, _)| held == key)
        .unwrap_or_else(|| {
            entries.push((key.to_vec(), Vec::new()));
            entries.len() - 1
        });
    &mut entries[at].1
}

/// Take `value` out from under `key`, or the whole key when there is no value to name.
fn remove(entries: &mut Entries, key: &[u8], value: Option<&[u8]>) -> usize {
    let Some(at) = entries.iter().position(|(held, _)| held == key) else {
        return 0;
    };
    let before = entries[at].1.len();
    match value {
        None => entries[at].1.clear(),
        Some(value) => entries[at].1.retain(|held| held != value),
    }
    let removed = before - entries[at].1.len();
    if entries[at].1.is_empty() {
        entries.remove(at);
    }
    removed
}

/// The generation a dict key is listed under, which is the one value the index keeps for it. A value that is not a
/// generation counts as the oldest.
fn listed_under(values: &[Vec<u8>]) -> i64 {
    values
        .iter()
        .filter_map(|value| Reading::new(value).int().ok())
        .max()
        .unwrap_or(0)
}

/// Everything the messages of a directory and of its segments carry between them.
struct Held {
    tag: String,
    reply: Target,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    generation: Option<i64>,
    seen: Option<i64>,
    split: Option<Target>,
    into: Option<Target>,
    modulus: Option<i64>,
    id: Option<i64>,
    version: Option<i64>,
    entries: Option<Entries>,
    position: Option<i64>,
}

fn read(message: &[u8]) -> Result<Held> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut key = None;
    let mut value = None;
    let mut generation = None;
    let mut seen = None;
    let mut split = None;
    let mut into = None;
    let mut modulus = None;
    let mut id = None;
    let mut version = None;
    let mut entries = None;
    let mut position = None;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "key" => key = Some(reading.bytes()?),
            "value" => value = optional_bytes(&mut reading)?,
            "generation" => generation = Some(reading.int()?),
            "seen" => seen = Some(reading.int()?),
            "split" => split = Some(reading.target()?),
            "into" => into = Some(reading.target()?),
            "modulus" => modulus = Some(reading.int()?),
            "id" => id = Some(reading.int()?),
            "version" => version = Some(reading.int()?),
            "entries" => entries = Some(read_entries(&mut reading)?),
            "position" => position = Some(reading.int()?),
            _ => reading.skip()?,
        }
    }
    Ok(Held {
        tag,
        reply: reply.ok_or(Malformed::Truncated)?,
        key,
        value,
        generation,
        seen,
        split,
        into,
        modulus,
        id,
        version,
        entries,
        position,
    })
}

/// `table_segment.Split` of a segment into `into`, telling the hashes apart over `modulus`.
fn splitting(reply: &Target, into: &Target, modulus: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("table_segment.Split", 3);
    writer.name("reply_to");
    writer.target(reply);
    writer.name("into");
    writer.target(into);
    writer.name("modulus");
    writer.int(modulus);
    writer.finish()
}

/// `table_segment.Adopt` of the entries a split moves, by the segment `id` they move to.
fn adoption(reply: &Target, entries: &Entries, modulus: i64, id: i64, version: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("table_segment.Adopt", 5);
    writer.name("reply_to");
    writer.target(reply);
    writer.name("entries");
    write_entries(&mut writer, entries);
    writer.name("modulus");
    writer.int(modulus);
    writer.name("id");
    writer.int(id);
    writer.name("version");
    writer.int(version);
    writer.finish()
}

fn read_entries(reading: &mut Reading<'_>) -> Result<Entries> {
    let count = reading.items()?;
    let mut entries = Entries::with_capacity(count);
    for _ in 0..count {
        reading.items()?;
        let key = reading.bytes()?;
        let held = reading.items()?;
        let values = (0..held).map(|_| reading.bytes()).collect::<Result<_>>()?;
        entries.push((key, values));
    }
    Ok(entries)
}

/// The entries as a mapping, which the schema writes as a list of pairs.
fn write_entries(writer: &mut Writer, entries: &Entries) {
    writer.items(entries.len());
    for (key, values) in entries {
        writer.items(2);
        writer.bytes(key);
        writer.items(values.len());
        for value in values {
            writer.bytes(value);
        }
    }
}

fn written(entries: &Entries) -> Vec<u8> {
    let mut writer = Writer::new();
    write_entries(&mut writer, entries);
    writer.finish()
}

/// `tuple[bool, bool]`: whether a write changed the listing, and whether its segment now asks for a split.
fn wrote(changed: bool, full: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.items(2);
    writer.bool(changed);
    writer.bool(full);
    writer.finish()
}

/// `tuple[int, ...]`: the modulus of a segment, which tells a walk the hashes it answered for, then the answer.
fn walked(modulus: i64, then: impl FnOnce(&mut Writer)) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.items(2);
    writer.int(modulus);
    then(&mut writer);
    writer.finish()
}

fn listed(entries: &Entries, key: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new();
    let values = entries
        .iter()
        .find(|(held, _)| held == key)
        .map_or(&[][..], |(_, values)| values.as_slice());
    writer.items(values.len());
    for value in values {
        writer.bytes(value);
    }
    writer.finish()
}

#[cfg(test)]
mod tests {
    use casty_core::node::Target;
    use casty_core::store::Pages;
    use casty_core::wire::{Reading, Result, Writer};

    use super::{Bucket, Entries, KEYS, Segment, Table, adoption, read_entries, spread};
    use crate::collections::{Given, Native, native};

    fn at(key: &str) -> Target {
        Target::Entity {
            actor: "casty.collections:table_segment.actor".to_owned(),
            key: key.to_owned(),
        }
    }

    /// A message of `tag` with a reply target and the `fields` that `write` adds after it.
    fn message(tag: &str, fields: usize, write: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.tagged(tag, fields + 1);
        writer.name("reply_to");
        writer.target(&at("reply"));
        write(&mut writer);
        writer.finish()
    }

    fn bare(tag: &str) -> Vec<u8> {
        message(tag, 0, |_| {})
    }

    /// `table_segment.<tag>` of `key` under `generation`.
    fn listing(tag: &str, key: &[u8], generation: i64) -> Vec<u8> {
        message(tag, 2, |writer| {
            writer.name("key");
            writer.bytes(key);
            writer.name("generation");
            writer.int(generation);
        })
    }

    fn add(key: &[u8]) -> Vec<u8> {
        message("table_segment.Add", 2, |writer| {
            writer.name("key");
            writer.bytes(key);
            writer.name("value");
            writer.bytes(key);
        })
    }

    fn scan(position: i64) -> Vec<u8> {
        message("table_segment.Scan", 1, |writer| {
            writer.name("position");
            writer.int(position);
        })
    }

    fn split(modulus: i64) -> Vec<u8> {
        message("table_segment.Split", 2, |writer| {
            writer.name("into");
            writer.target(&at("0.1"));
            writer.name("modulus");
            writer.int(modulus);
        })
    }

    fn grow(seen: i64) -> Vec<u8> {
        message("table.Grow", 3, |writer| {
            writer.name("seen");
            writer.int(seen);
            writer.name("split");
            writer.target(&at("0.0"));
            writer.name("into");
            writer.target(&at("0.1"));
        })
    }

    /// Run `message` on the key whose state is `pages` as the activation does, answering at once what it asks, and
    /// keep what it saved. Gives back its answer, and the message it asked with when it asked.
    fn step(body: &dyn Native, pages: &mut Pages, message: &[u8]) -> (Vec<u8>, Option<Vec<u8>>) {
        let mut turn = body.step(pages, &Given::Message(message), 0.0);
        let asked = turn.ask.take().map(|ask| (ask.message)(&at("asking")));
        if asked.is_some() {
            turn = body.step(
                pages,
                &Given::Answered {
                    message,
                    answer: &[],
                },
                0.0,
            );
        }
        if let Some(saved) = turn.save {
            *pages = saved;
        }
        let answer = turn
            .replies
            .pop()
            .map(|(_, answer)| answer)
            .unwrap_or_default();
        (answer, asked)
    }

    /// Run `message` on a segment whose state is `pages`, keeping what it saved, and give back its answer.
    fn on(pages: &mut Pages, message: &[u8]) -> Vec<u8> {
        step(&Segment, pages, message).0
    }

    /// Whether a write changed the listing, and whether it asks for a split.
    fn wrote(answer: &[u8]) -> Result<(bool, bool)> {
        let mut reading = Reading::new(answer);
        reading.items()?;
        Ok((reading.bool()?, reading.bool()?))
    }

    fn added(pages: &mut Pages, key: &[u8]) -> Result<(bool, bool)> {
        wrote(&on(pages, &add(key)))
    }

    fn list(pages: &mut Pages, generation: i64) -> Result<bool> {
        let answer = on(pages, &listing("table_segment.List", b"key", generation));
        Ok(wrote(&answer)?.0)
    }

    fn unlist(pages: &mut Pages, generation: i64) -> Result<bool> {
        let answer = on(pages, &listing("table_segment.Unlist", b"key", generation));
        Reading::new(&answer).bool()
    }

    /// Whether the answer to `message` is None, which is how a segment refuses a key it does not hold.
    fn refused(pages: &mut Pages, message: &[u8]) -> Result<bool> {
        Reading::new(&on(pages, message)).nil()
    }

    /// What a walk hears from a segment when it sends `tag`: the modulus, and the count answered with it.
    fn walked(pages: &mut Pages, tag: &str) -> Result<(i64, i64)> {
        let answer = on(pages, &bare(tag));
        let mut reading = Reading::new(&answer);
        reading.items()?;
        Ok((reading.int()?, reading.int()?))
    }

    /// Adopt `entries` as segment 1 at modulus 2, from a segment at `version`.
    fn adopt(pages: &mut Pages, entries: &Entries, version: i64) {
        on(pages, &adoption(&at("reply"), entries, 2, 1, version));
    }

    /// The first `count` keys whose hash leaves `id` over `modulus`.
    fn keys(modulus: u64, id: u64, count: usize) -> Vec<Vec<u8>> {
        (0_u32..)
            .map(|at| at.to_be_bytes().to_vec())
            .filter(|key| spread(key) % modulus == id)
            .take(count)
            .collect()
    }

    fn segment(modulus: i64, id: i64) -> Pages {
        Bucket {
            entries: Entries::new(),
            modulus,
            id,
            version: 0,
        }
        .pages()
    }

    /// The modulus a `Split` names.
    fn modulus_of(message: &[u8]) -> Result<i64> {
        let mut reading = Reading::new(message);
        let (_, fields) = reading.tagged()?;
        let mut modulus = 0;
        for _ in 0..fields {
            match reading.name()? {
                "modulus" => modulus = reading.int()?,
                _ => reading.skip()?,
            }
        }
        Ok(modulus)
    }

    #[test]
    fn a_late_unlisting_leaves_the_newer_listing_in_place() -> Result<()> {
        let mut held = Segment.initial();
        assert!(list(&mut held, 1)?);
        assert!(list(&mut held, 2)?);
        assert!(!unlist(&mut held, 1)?);
        assert_eq!(Bucket::of(&held).entries.len(), 1);
        assert!(unlist(&mut held, 2)?);
        assert!(Bucket::of(&held).entries.is_empty());
        Ok(())
    }

    #[test]
    fn a_late_listing_does_not_take_the_generation_back() -> Result<()> {
        let mut held = Segment.initial();
        assert!(list(&mut held, 2)?);
        assert!(
            list(&mut held, 2)?,
            "a key listed under the same generation is listed under it"
        );
        assert!(!list(&mut held, 1)?);
        assert!(!unlist(&mut held, 1)?);
        assert!(unlist(&mut held, 2)?);
        assert!(!unlist(&mut held, 2)?);
        assert!(Bucket::of(&held).entries.is_empty());
        Ok(())
    }

    #[test]
    fn a_segment_refuses_a_key_whose_hash_it_does_not_hold() -> Result<()> {
        let mut held = segment(2, 1);
        let before = held.clone();
        let theirs = &keys(2, 0, 1)[0];
        let ours = &keys(2, 1, 1)[0];
        let get = listing("table_segment.Get", theirs, 0);
        assert!(refused(&mut held, &add(theirs))?);
        assert!(refused(&mut held, &get)?);
        assert_eq!(held, before);
        assert_eq!(added(&mut held, ours)?, (true, false));
        assert_eq!(Bucket::of(&held).entries.len(), 1);
        Ok(())
    }

    #[test]
    fn a_write_past_the_bound_asks_for_a_split() -> Result<()> {
        let mut held = Segment.initial();
        let all = keys(1, 0, KEYS + 1);
        for key in &all[..KEYS] {
            assert_eq!(added(&mut held, key)?, (true, false));
        }
        assert_eq!(added(&mut held, &all[KEYS])?, (true, true));
        assert_eq!(added(&mut held, &all[0])?, (false, true));
        Ok(())
    }

    #[test]
    fn a_split_moves_the_keys_of_the_new_segment_there_and_keeps_the_others() -> Result<()> {
        let mut source = Segment.initial();
        let all = keys(1, 0, 600);
        for key in &all {
            added(&mut source, key)?;
        }
        let (done, asked) = step(&Segment, &mut source, &split(2));
        assert!(Reading::new(&done).nil()?);
        let mut image = Segment.initial();
        on(&mut image, &asked.unwrap_or_default());
        let (kept, moved) = (Bucket::of(&source), Bucket::of(&image));
        assert_eq!((kept.modulus, kept.id), (2, 0));
        assert_eq!((moved.modulus, moved.id), (2, 1));
        assert!(
            kept.entries
                .iter()
                .all(|(key, _)| spread(key).is_multiple_of(2))
        );
        assert!(moved.entries.iter().all(|(key, _)| spread(key) % 2 == 1));
        assert_eq!(kept.entries.len() + moved.entries.len(), all.len());
        // Asked again, after a split whose count the directory did not save, the segment moves nothing more.
        let (_, again) = step(&Segment, &mut source, &split(2));
        assert!(again.is_none());
        assert_eq!(Bucket::of(&source), kept);
        Ok(())
    }

    #[test]
    fn a_late_adoption_never_replaces_the_one_that_finished() {
        let first = vec![(b"first".to_vec(), vec![b"first".to_vec()])];
        let late = vec![(b"late".to_vec(), vec![b"late".to_vec()])];
        let mut image = Segment.initial();
        adopt(&mut image, &first, 5);
        adopt(&mut image, &late, 3);
        adopt(&mut image, &late, 5);
        assert_eq!(Bucket::of(&image).entries, first);
        // A later attempt, of a segment written since, is the one the split finishes with.
        adopt(&mut image, &late, 7);
        assert_eq!(Bucket::of(&image).entries, late);
        assert_eq!(Bucket::of(&image).version, 7);
    }

    #[test]
    fn a_walk_hears_the_modulus_of_each_segment_it_visits() -> Result<()> {
        let mut held = segment(4, 3);
        for key in keys(4, 3, 3) {
            added(&mut held, &key)?;
        }
        assert_eq!(walked(&mut held, "table_segment.Size")?, (4, 3));
        assert_eq!(walked(&mut held, "table_segment.Clear")?, (4, 3));
        assert_eq!(walked(&mut held, "table_segment.Size")?, (4, 0));
        Ok(())
    }

    #[test]
    fn a_scan_is_answered_only_from_a_position_whose_hash_the_segment_holds() -> Result<()> {
        let mut held = segment(4, 3);
        let ours = keys(4, 3, 3);
        for key in &ours {
            added(&mut held, key)?;
        }
        let before = held.clone();
        assert!(refused(&mut held, &scan(1))?);
        assert!(refused(&mut held, &scan(4))?);
        let answer = on(&mut held, &scan(7));
        let mut reading = Reading::new(&answer);
        reading.items()?;
        assert_eq!(reading.int()?, 4);
        let listed = read_entries(&mut reading)?;
        assert_eq!(
            listed.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            ours
        );
        assert_eq!(held, before);
        Ok(())
    }

    #[test]
    fn the_directory_makes_only_the_split_it_was_asked_from() -> Result<()> {
        let mut held = Table.initial();
        let (count, asked) = step(&Table, &mut held, &grow(1));
        assert_eq!(Reading::new(&count).int()?, 2);
        assert_eq!(modulus_of(&asked.unwrap_or_default())?, 2);
        let (count, asked) = step(&Table, &mut held, &grow(1));
        assert_eq!(Reading::new(&count).int()?, 2);
        assert!(asked.is_none());
        let (count, _) = step(&Table, &mut held, &bare("table.Segments"));
        assert_eq!(Reading::new(&count).int()?, 2);
        let (_, asked) = step(&Table, &mut held, &grow(2));
        assert_eq!(modulus_of(&asked.unwrap_or_default())?, 4);
        Ok(())
    }

    #[test]
    fn the_hash_is_the_one_the_facade_places_keys_by() {
        // `int.from_bytes(blake2b(key, digest_size=8).digest()) >> 32` in Python.
        assert_eq!(spread(b""), 3_836_125_271);
        assert_eq!(spread(b"key"), 3_464_285_357);
    }

    #[test]
    fn a_segment_has_a_body_under_every_configuration() {
        assert!(native("casty.collections:table_segment_3_majority").is_some());
        assert!(native("casty.collections:table_segment.actor").is_some());
    }
}
