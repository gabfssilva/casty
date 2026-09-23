//! A queue: items taken in the order they were offered, kept in segments under keys of their own.
//!
//! The index of a queue names two segments: the head, which polls take from, and the tail, which offers go to. A
//! segment takes offers until it is full and is then sealed for good, so an offer that reaches it late is refused and
//! asks the index where the tail went. Nothing is ever added behind an item that a later segment holds, which is what
//! keeps the order across segments, whichever node owns each of them.
//!
//! A segment sealed and drained is deleted when its activation ends, so a queue keeps no key for the items that went
//! through it. What made it refuse is then gone with it, and a facade that fell behind can still name it: a segment
//! nothing wrote asks the index before it takes an offer or hands out items. Every segment below the tail was sealed,
//! so one found there is one that was deleted, and it answers as the sealed and empty segment it was.

use casty_core::node::Target;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Asking, Given, Native, Turn, count, fields, nil, number, number_in, truth};

const HEAD: &str = "head";
const TAIL: &str = "tail";
const ITEMS: &str = "items";
const TAKEN: &str = "taken";
const SEALED: &str = "sealed";

/// The items a segment takes before it is sealed.
const COUNT: usize = 1024;

/// The bytes of items a segment takes before it is sealed, unless one item alone is larger. An offer writes the items
/// of its segment, so this is what bounds its cost.
const BYTES: usize = 64 * 1024;

/// The index of a queue: the first segment that may hold items, and the segment offers go to.
#[derive(Debug)]
pub struct Queue;

impl Native for Queue {
    fn initial(&self) -> Pages {
        index(0, 0)
    }

    fn turn(&self, pages: &Pages, given: &Given<'_>, _: f64) -> Option<Turn> {
        let (tag, reply, read) = read(given.message()?)?;
        if tag != "Advance" {
            return None;
        }
        let (head, tail) = (number_in(pages, HEAD, 0), number_in(pages, TAIL, 0));
        // Both only move forward, and the tail never stays behind the head: a segment the head passed is sealed.
        let next_head = head.max(read.head.unwrap_or(head));
        let next_tail = tail.max(read.tail.unwrap_or(tail)).max(next_head);
        let mut turn = Turn::default();
        if (next_head, next_tail) != (head, tail) {
            turn.save = Some(index(next_head, next_tail));
        }
        turn.replies = vec![(reply, span(next_head, next_tail))];
        Some(turn)
    }
}

/// A run of the items of a queue, which takes offers until it is sealed and never again after.
#[derive(Debug)]
pub struct Segment;

impl Native for Segment {
    /// No page at all, which tells a segment nothing wrote, new or deleted, from one that took offers and was emptied.
    fn initial(&self) -> Pages {
        Pages::new()
    }

    /// A segment sealed and drained holds nothing and takes nothing, and one nothing wrote holds nothing either.
    fn disposable(&self, held: &Pages) -> bool {
        let run = Run::of(held);
        held.is_empty() || (run.sealed && run.waiting().is_empty())
    }

    fn turn(&self, held: &Pages, given: &Given<'_>, _: f64) -> Option<Turn> {
        let (tag, reply, read) = read(given.message()?)?;
        // Only the messages that take or hand out items need to know which segment nothing wrote this is.
        let opening = held.is_empty() && matches!(tag, "Offer" | "Take" | "Peek");
        if opening {
            let Some(answer) = given.answer() else {
                return Some(Turn {
                    ask: Some(Asking {
                        to: read.index?,
                        message: Box::new(where_is),
                    }),
                    ..Turn::default()
                });
            };
            let (_, tail) = ends(answer).ok()?;
            if read.at? < tail {
                // Deleted once it was sealed and drained: it answers as it did then, and goes again.
                let answer = if tag == "Offer" {
                    truth(false)
                } else {
                    listed(&[], true)
                };
                return Some(Turn {
                    delete: true,
                    replies: vec![(reply, answer)],
                    ..Turn::default()
                });
            }
        }
        let before = Run::of(held);
        let mut run = before.clone();
        let answer = match tag {
            "Offer" => truth(run.offer(read.value?)),
            "Take" => {
                // A negative limit is refused by the facade, and here it simply takes nothing.
                let taken = run.take(usize::try_from(read.limit.unwrap_or(0)).unwrap_or(0));
                listed(&taken, run.sealed)
            }
            "Peek" => {
                let waiting = run.waiting();
                listed(&waiting[..waiting.len().min(1)], run.sealed)
            }
            "Size" => number(count(run.waiting().len())),
            "Clear" => {
                run.clear();
                nil()
            }
            _ => return None,
        };
        let mut turn = Turn::default();
        // The tail the index named is written even when nothing in it changed, so that it is asked about only once.
        if run != before || opening {
            turn.save = Some(run.pages());
        }
        turn.replies = vec![(reply, answer)];
        Some(turn)
    }
}

/// What a segment holds: the items offered to it, how many of those were taken, and whether it takes more.
#[derive(Debug, Clone, Default, PartialEq)]
struct Run {
    items: Vec<Vec<u8>>,
    taken: usize,
    sealed: bool,
}

impl Run {
    fn of(pages: &Pages) -> Self {
        Self {
            items: pages
                .get(ITEMS)
                .and_then(|page| read_items(page).ok())
                .unwrap_or_default(),
            taken: usize::try_from(number_in(pages, TAKEN, 0)).unwrap_or(0),
            sealed: pages
                .get(SEALED)
                .and_then(|page| Reading::new(page).bool().ok())
                .unwrap_or(false),
        }
    }

    fn pages(&self) -> Pages {
        let mut items = Writer::new();
        items.items(self.items.len());
        for item in &self.items {
            items.bytes(item);
        }
        Pages::from([
            (ITEMS.to_owned(), items.finish()),
            (TAKEN.to_owned(), number(count(self.taken))),
            (SEALED.to_owned(), truth(self.sealed)),
        ])
    }

    /// The items not taken yet, in the order they were offered.
    fn waiting(&self) -> &[Vec<u8>] {
        &self.items[self.taken.min(self.items.len())..]
    }

    /// Append `item`, unless the segment is sealed. An item that would take a segment that holds something past
    /// `BYTES` seals it instead and goes to the next one, so a segment is larger than that only with one item alone.
    fn offer(&mut self, item: Vec<u8>) -> bool {
        if self.sealed {
            return false;
        }
        let bytes = self.items.iter().map(Vec::len).sum::<usize>() + item.len();
        if !self.items.is_empty() && bytes > BYTES {
            self.sealed = true;
            return false;
        }
        self.items.push(item);
        self.sealed = self.items.len() >= COUNT || bytes >= BYTES;
        true
    }

    /// Up to `limit` items, from the first one not taken.
    ///
    /// What was taken stays in the page until it is half of it, so that most takes write only the count. A segment
    /// emptied this way starts over, which keeps one that consumers keep up with from ever filling.
    fn take(&mut self, limit: usize) -> Vec<Vec<u8>> {
        let start = self.taken.min(self.items.len());
        let end = start.saturating_add(limit).min(self.items.len());
        let given = self.items[start..end].to_vec();
        self.taken = end;
        if self.taken * 2 >= self.items.len() {
            self.items = self.items.split_off(self.taken);
            self.taken = 0;
        }
        given
    }

    /// Drop every item. An open segment goes on taking offers, and a sealed one stays empty for good.
    fn clear(&mut self) {
        self.items.clear();
        self.taken = 0;
    }
}

/// Everything the messages of the index and of a segment carry between them, besides who they answer.
///
/// `index` and `at` are the index of the queue and the number of the segment a message is for, which a segment
/// nothing wrote asks the index about.
#[derive(Default)]
struct Held {
    value: Option<Vec<u8>>,
    limit: Option<i64>,
    head: Option<i64>,
    tail: Option<i64>,
    index: Option<Target>,
    at: Option<i64>,
}

fn read(message: &[u8]) -> Option<(&str, Target, Held)> {
    let mut held = Held::default();
    let (tag, reply) = fields(message, |name, reading| {
        match name {
            "value" => held.value = Some(reading.bytes()?),
            "limit" => held.limit = Some(reading.int()?),
            "head" => held.head = Some(reading.int()?),
            "tail" => held.tail = Some(reading.int()?),
            "index" => held.index = Some(reading.target()?),
            "at" => held.at = Some(reading.int()?),
            _ => reading.skip()?,
        }
        Ok(())
    })?;
    Some((tag, reply, held))
}

/// `queue.Advance` that moves nothing, which answers where the head and the tail are.
fn where_is(reply: &Target) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Advance", 3);
    writer.name("reply_to");
    writer.target(reply);
    writer.name("head");
    writer.int(0);
    writer.name("tail");
    writer.int(0);
    writer.finish()
}

/// The head and the tail an `Advance` answered.
fn ends(answer: &[u8]) -> Result<(i64, i64)> {
    let mut reading = Reading::new(answer);
    reading.items()?;
    Ok((reading.int()?, reading.int()?))
}

fn read_items(page: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut reading = Reading::new(page);
    let count = reading.items()?;
    (0..count).map(|_| reading.bytes()).collect()
}

fn index(head: i64, tail: i64) -> Pages {
    Pages::from([
        (HEAD.to_owned(), number(head)),
        (TAIL.to_owned(), number(tail)),
    ])
}

/// `tuple[int, int]`: where the head and the tail are.
fn span(head: i64, tail: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.items(2);
    writer.int(head);
    writer.int(tail);
    writer.finish()
}

/// `tuple[tuple[bytes, ...], bool]`: the items taken or looked at, and whether the segment is sealed.
fn listed(items: &[Vec<u8>], sealed: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.items(2);
    writer.items(items.len());
    for item in items {
        writer.bytes(item);
    }
    writer.bool(sealed);
    writer.finish()
}

#[cfg(test)]
mod tests {
    use casty_core::node::Target;
    use casty_core::store::Pages;
    use casty_core::wire::{Reading, Result, Writer};

    use super::{BYTES, COUNT, Queue, Run, Segment, ends, span};
    use crate::collections::{Given, Native, native};

    /// A segment the index named as the tail and that took nothing yet, which is where most tests start from.
    fn opened() -> Pages {
        Run::default().pages()
    }

    /// A message of `tag` with a reply target and the `fields` that `write` adds after it.
    fn message(tag: &str, fields: usize, write: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.tagged(tag, fields + 1);
        writer.name("reply_to");
        writer.target(&Target::Entity {
            actor: "test".to_owned(),
            key: "reply".to_owned(),
        });
        write(&mut writer);
        writer.finish()
    }

    fn offer(item: &[u8]) -> Vec<u8> {
        message("queue_segment.Offer", 1, |writer| {
            writer.name("value");
            writer.bytes(item);
        })
    }

    fn take(limit: i64) -> Vec<u8> {
        message("queue_segment.Take", 1, |writer| {
            writer.name("limit");
            writer.int(limit);
        })
    }

    fn bare(tag: &str) -> Vec<u8> {
        message(tag, 0, |_| {})
    }

    fn advance(head: i64, tail: i64) -> Vec<u8> {
        message("queue.Advance", 2, |writer| {
            writer.name("head");
            writer.int(head);
            writer.name("tail");
            writer.int(tail);
        })
    }

    /// Run `message` on the key whose state is `pages`, keep what it saved, and give back its answer.
    fn step(body: &dyn Native, pages: &mut Pages, message: &[u8]) -> Vec<u8> {
        let mut turn = body.step(pages, &Given::Message(message), 0.0);
        if let Some(saved) = turn.save {
            *pages = saved;
        }
        turn.replies
            .pop()
            .map(|(_, answer)| answer)
            .unwrap_or_default()
    }

    fn accepted(answer: &[u8]) -> Result<bool> {
        Reading::new(answer).bool()
    }

    fn listed(answer: &[u8]) -> Result<(Vec<Vec<u8>>, bool)> {
        let mut reading = Reading::new(answer);
        reading.items()?;
        let count = reading.items()?;
        let items = (0..count)
            .map(|_| reading.bytes())
            .collect::<Result<Vec<_>>>()?;
        Ok((items, reading.bool()?))
    }

    #[test]
    fn a_segment_seals_at_its_count_and_refuses_every_offer_after() -> Result<()> {
        let mut pages = opened();
        for item in 0..COUNT {
            assert!(accepted(&step(
                &Segment,
                &mut pages,
                &offer(&item.to_be_bytes())
            ))?);
        }
        assert!(!accepted(&step(&Segment, &mut pages, &offer(b"late")))?);
        let (items, sealed) = listed(&step(&Segment, &mut pages, &take(i64::MAX)))?;
        assert!(sealed);
        let offered: Vec<Vec<u8>> = (0..COUNT).map(|item| item.to_be_bytes().to_vec()).collect();
        assert_eq!(items, offered);
        assert_eq!(
            listed(&step(&Segment, &mut pages, &take(1)))?,
            (Vec::new(), true)
        );
        Ok(())
    }

    #[test]
    fn an_item_that_would_pass_the_byte_bound_seals_the_segment_and_goes_alone_to_the_next()
    -> Result<()> {
        let large = vec![0_u8; BYTES];
        let mut first = opened();
        assert!(accepted(&step(&Segment, &mut first, &offer(b"small")))?);
        assert!(!accepted(&step(&Segment, &mut first, &offer(&large)))?);
        let mut second = opened();
        assert!(accepted(&step(&Segment, &mut second, &offer(&large)))?);
        assert!(!accepted(&step(&Segment, &mut second, &offer(b"small")))?);
        assert_eq!(
            listed(&step(&Segment, &mut first, &take(10)))?,
            (vec![b"small".to_vec()], true)
        );
        Ok(())
    }

    #[test]
    fn taking_keeps_the_order_and_an_emptied_open_segment_starts_over() -> Result<()> {
        let mut pages = opened();
        for item in [b"a", b"b", b"c"] {
            assert!(accepted(&step(&Segment, &mut pages, &offer(item)))?);
        }
        assert_eq!(
            listed(&step(&Segment, &mut pages, &bare("queue_segment.Peek")))?,
            (vec![b"a".to_vec()], false)
        );
        assert_eq!(
            listed(&step(&Segment, &mut pages, &take(2)))?,
            (vec![b"a".to_vec(), b"b".to_vec()], false)
        );
        assert_eq!(
            Reading::new(&step(&Segment, &mut pages, &bare("queue_segment.Size"))).int()?,
            1
        );
        assert_eq!(
            listed(&step(&Segment, &mut pages, &take(5)))?,
            (vec![b"c".to_vec()], false)
        );
        assert_eq!(pages, opened());
        Ok(())
    }

    #[test]
    fn a_cleared_segment_that_was_sealed_stays_sealed() -> Result<()> {
        let mut pages = opened();
        for item in 0..COUNT {
            step(&Segment, &mut pages, &offer(&item.to_be_bytes()));
        }
        step(&Segment, &mut pages, &bare("queue_segment.Clear"));
        assert_eq!(
            listed(&step(&Segment, &mut pages, &take(1)))?,
            (Vec::new(), true)
        );
        assert!(!accepted(&step(&Segment, &mut pages, &offer(b"late")))?);
        Ok(())
    }

    #[test]
    fn the_index_only_moves_forward_and_never_leaves_the_tail_behind_the_head() -> Result<()> {
        let mut pages = Queue.initial();
        assert_eq!(ends(&step(&Queue, &mut pages, &advance(0, 1)))?, (0, 1));
        assert_eq!(ends(&step(&Queue, &mut pages, &advance(0, 0)))?, (0, 1));
        assert_eq!(ends(&step(&Queue, &mut pages, &advance(3, 0)))?, (3, 3));
        assert_eq!(ends(&step(&Queue, &mut pages, &advance(2, 2)))?, (3, 3));
        Ok(())
    }

    #[test]
    fn the_index_and_the_segments_have_a_body_under_every_configuration() {
        assert!(native("casty.collections:queue_3_majority").is_some());
        assert!(native("casty.collections:queue_segment_3_majority").is_some());
        assert!(native("casty.collections:queue_segment.actor").is_some());
    }

    fn index() -> Target {
        Target::Entity {
            actor: "casty.collections:queue.actor".to_owned(),
            key: "queue".to_owned(),
        }
    }

    /// `tag` for the segment `at` of the queue whose index is `index()`, with the `fields` that `write` adds.
    fn placed(tag: &str, at: i64, fields: usize, write: impl FnOnce(&mut Writer)) -> Vec<u8> {
        message(tag, fields + 2, |writer| {
            write(writer);
            writer.name("index");
            writer.target(&index());
            writer.name("at");
            writer.int(at);
        })
    }

    /// Run `message` on a segment, answering what it asks the index with a queue whose tail is `tail`. Gives back its
    /// answer and whether it deleted the key; a deletion leaves it where a key nothing wrote starts.
    fn asked(pages: &mut Pages, message: &[u8], tail: i64) -> Result<(Vec<u8>, bool)> {
        let mut turn = Segment.step(pages, &Given::Message(message), 0.0);
        if let Some(ask) = turn.ask.take() {
            assert_eq!(ask.to, index());
            let (tag, _) = Reading::new(&(ask.message)(&index())).tagged()?;
            assert!(tag.ends_with("Advance"), "{tag}");
            turn = Segment.step(
                pages,
                &Given::Answered {
                    message,
                    answer: &span(0, tail),
                },
                0.0,
            );
        }
        if turn.delete {
            *pages = Segment.initial();
        } else if let Some(saved) = turn.save {
            *pages = saved;
        }
        let answer = turn
            .replies
            .pop()
            .map(|(_, answer)| answer)
            .unwrap_or_default();
        Ok((answer, turn.delete))
    }

    #[test]
    fn a_segment_nothing_wrote_takes_offers_once_the_index_names_it_the_tail() -> Result<()> {
        let mut pages = Segment.initial();
        let offered = placed("queue_segment.Offer", 4, 1, |writer| {
            writer.name("value");
            writer.bytes(b"one");
        });
        let (answer, deleted) = asked(&mut pages, &offered, 4)?;
        assert!(accepted(&answer)?);
        assert!(!deleted);
        // It holds pages now, so the next message is not asked about.
        let turn = Segment.step(&pages, &Given::Message(&offered), 0.0);
        assert!(turn.ask.is_none());
        Ok(())
    }

    #[test]
    fn a_tail_nothing_wrote_answers_empty_and_open_and_is_asked_about_once() -> Result<()> {
        let mut pages = Segment.initial();
        let peeked = placed("queue_segment.Peek", 0, 0, |_| {});
        let (answer, deleted) = asked(&mut pages, &peeked, 0)?;
        assert_eq!(listed(&answer)?, (Vec::new(), false));
        assert!(!deleted);
        assert_eq!(pages, opened());
        Ok(())
    }

    /// A facade that fell behind still names a segment that was sealed, drained and deleted. Found below the tail,
    /// it answers as the sealed and empty segment it was, and deletes the key its activation made again.
    #[test]
    fn a_deleted_segment_below_the_tail_answers_sealed_and_empty_and_goes_again() -> Result<()> {
        let mut pages = Segment.initial();
        let offered = placed("queue_segment.Offer", 1, 1, |writer| {
            writer.name("value");
            writer.bytes(b"late");
        });
        let (answer, deleted) = asked(&mut pages, &offered, 3)?;
        assert!(
            !accepted(&answer)?,
            "an item went into a segment nobody polls"
        );
        assert!(deleted);

        let taken = placed("queue_segment.Take", 1, 1, |writer| {
            writer.name("limit");
            writer.int(10);
        });
        let (answer, deleted) = asked(&mut pages, &taken, 3)?;
        assert_eq!(listed(&answer)?, (Vec::new(), true));
        assert!(deleted);
        assert_eq!(pages, Segment.initial());
        Ok(())
    }

    #[test]
    fn a_sealed_drained_segment_is_disposable_and_the_open_tail_is_not() -> Result<()> {
        assert!(Segment.disposable(&Segment.initial()));
        let mut pages = opened();
        step(&Segment, &mut pages, &offer(b"a"));
        step(&Segment, &mut pages, &take(1));
        assert!(!Segment.disposable(&pages), "the emptied tail would go");
        for item in 0..COUNT {
            step(&Segment, &mut pages, &offer(&item.to_be_bytes()));
        }
        assert!(
            !Segment.disposable(&pages),
            "a sealed segment with items would go"
        );
        let (items, sealed) = listed(&step(&Segment, &mut pages, &take(i64::MAX)))?;
        assert_eq!(items.len(), COUNT);
        assert!(sealed);
        assert!(Segment.disposable(&pages));
        Ok(())
    }
}
