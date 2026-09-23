//! One entry of a dict: the value under a key, and the generation its index lists the key under.
//!
//! The entry never asks the index; the facade does, in an order that keeps every saved value listed. A put that finds
//! no value answers the generation to list the key under, and saves the value only when it is sent again naming that
//! generation, once the listing is in place. A removal clears the value and answers the generation it had, which the
//! facade then unlists. A put after it lists under a newer one, so an unlisting that arrives late, or whose answer was
//! lost, can only drop a listing no value relies on, never the newer one a put made since.
//!
//! A put that listed its key and stopped before the value was saved leaves a listing under the generation after the
//! entry's. `Retire` raises the entry to it before the facade unlists it, so that the next put lists under a newer one
//! still.
//!
//! A removal deletes the entry, and an entry that ends its activation with no value is deleted then: nothing of the
//! key stays on the replicas. What the generation fenced is kept by the clock instead. An entry starts at the wall
//! clock in microseconds when its activation creates it, so the next life of a key lists under a generation newer
//! than any the deleted one had, and an unlisting or a put of the old life that arrives late is as harmless as one
//! of the same life. Where the clock of a node that owned the key ran ahead, the index refuses the listing of the
//! next life as older than the one it holds, and the facade retires that one before it puts again, so that the next
//! life lists under a newer generation still.

use std::time::{SystemTime, UNIX_EPOCH};

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Given, Native, Turn, named, number, number_in, optional, optional_in, truth};

const VALUE: &str = "value";
const GENERATION: &str = "generation";

#[derive(Debug)]
pub struct Entry;

impl Native for Entry {
    fn initial(&self) -> Pages {
        saved(None, started())
    }

    /// An entry with no value holds only its generation, which the clock stands in for once it is gone.
    fn disposable(&self, held: &Pages) -> bool {
        value(held).is_none()
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let Given::Message(message) = given else {
            return Turn::default();
        };
        let Ok(read) = self::read(message) else {
            return Turn::default();
        };
        let value = value(held);
        let generation = generation(held);
        let next = generation.saturating_add(1);
        let mut turn = Turn::default();
        let answer = match named(&read.tag) {
            "Put" => {
                let (Some(put), Some(listed)) = (read.value, read.listed) else {
                    return Turn::default();
                };
                if value.is_some() {
                    turn.save = Some(saved(Some(&put), generation));
                    number(0)
                } else if listed == next {
                    turn.save = Some(saved(Some(&put), next));
                    number(0)
                } else {
                    number(next)
                }
            }
            "Get" => optional(value.as_deref()),
            "Contains" => truth(value.is_some()),
            "Remove" => {
                turn.delete = value.is_some();
                removal(value.is_some(), generation)
            }
            "Retire" => {
                let Some(listed) = read.listed else {
                    return Turn::default();
                };
                if value.is_some() {
                    number(0)
                } else if listed > generation {
                    turn.save = Some(saved(None, listed));
                    number(listed)
                } else {
                    number(generation)
                }
            }
            _ => return Turn::default(),
        };
        turn.replies = vec![(read.reply, answer)];
        turn
    }
}

/// Everything the five messages carry between them.
struct Held {
    tag: String,
    reply: Target,
    value: Option<Vec<u8>>,
    listed: Option<i64>,
}

fn read(message: &[u8]) -> Result<Held> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut value = None;
    let mut listed = None;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "value" => value = Some(reading.bytes()?),
            "listed" => listed = Some(reading.int()?),
            _ => reading.skip()?,
        }
    }
    Ok(Held {
        tag,
        reply: reply.ok_or(Malformed::Truncated)?,
        value,
        listed,
    })
}

/// `tuple[bool, int]`: whether a value was removed, and the generation to unlist the key under, 0 when none was listed.
fn removal(removed: bool, generation: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.items(2);
    writer.bool(removed);
    writer.int(generation);
    writer.finish()
}

fn value(held: &Pages) -> Option<Vec<u8>> {
    optional_in(held, VALUE)
}

fn generation(held: &Pages) -> i64 {
    number_in(held, GENERATION, 0)
}

/// The wall clock in microseconds, which is the generation an entry starts from.
fn started() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
        })
}

fn saved(value: Option<&[u8]>, generation: i64) -> Pages {
    Pages::from([
        (VALUE.to_owned(), optional(value)),
        (GENERATION.to_owned(), number(generation)),
    ])
}

#[cfg(test)]
mod tests {
    use casty_core::node::Target;
    use casty_core::store::Pages;
    use casty_core::wire::{Reading, Result, Writer};

    use super::{Entry, generation, saved, started, value};
    use crate::collections::{Given, Native};

    /// An entry whose life started at generation 0, so that the generations a test expects read as small numbers.
    fn fresh() -> Pages {
        saved(None, 0)
    }

    /// `entry.<tag>` with the value and the listed generation, each message taking the ones it reads.
    fn message(tag: &str, value: &[u8], listed: i64) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.tagged(tag, 3);
        writer.name("reply_to");
        writer.target(&Target::Entity {
            actor: "casty.collections:table_segment.actor".to_owned(),
            key: "caller".to_owned(),
        });
        writer.name("value");
        writer.bytes(value);
        writer.name("listed");
        writer.int(listed);
        writer.finish()
    }

    /// One message, keeping what it saved, and its answer. A deletion leaves the entry where the activation goes on
    /// from, which is `initial`.
    fn run(held: &mut Pages, message: &[u8]) -> Vec<u8> {
        let turn = Entry.step(held, &Given::Message(message), 0.0);
        if turn.delete {
            *held = Entry.initial();
        } else if let Some(pages) = turn.save {
            *held = pages;
        }
        turn.replies
            .into_iter()
            .next()
            .map(|(_, answer)| answer)
            .unwrap_or_default()
    }

    /// What `Put` answers: 0 once the value is saved, or the generation to list the key under first.
    fn put(held: &mut Pages, value: &[u8], listed: i64) -> Result<i64> {
        Reading::new(&run(held, &message("entry.Put", value, listed))).int()
    }

    /// What `Remove` answers: whether it removed a value, and the generation to unlist the key under.
    fn remove(held: &mut Pages) -> Result<(bool, i64)> {
        let answer = run(held, &message("entry.Remove", b"", 0));
        let mut reading = Reading::new(&answer);
        reading.items()?;
        Ok((reading.bool()?, reading.int()?))
    }

    /// What `Retire` answers: the generation to unlist the key under, or 0 when there is nothing to unlist.
    fn retire(held: &mut Pages, listed: i64) -> Result<i64> {
        Reading::new(&run(held, &message("entry.Retire", b"", listed))).int()
    }

    #[test]
    fn a_new_key_is_saved_only_when_sent_again_under_the_generation_it_listed() -> Result<()> {
        let mut held = fresh();
        assert_eq!(put(&mut held, b"one", 0)?, 1);
        assert_eq!(value(&held), None);
        assert_eq!(put(&mut held, b"one", 1)?, 0);
        assert_eq!(value(&held).as_deref(), Some(&b"one"[..]));
        assert_eq!(generation(&held), 1);
        // An overwrite is listed already.
        assert_eq!(put(&mut held, b"two", 0)?, 0);
        assert_eq!(value(&held).as_deref(), Some(&b"two"[..]));
        assert_eq!(generation(&held), 1);
        Ok(())
    }

    #[test]
    fn a_removal_deletes_the_entry_and_answers_the_generation_to_unlist() -> Result<()> {
        let mut held = fresh();
        put(&mut held, b"one", 0)?;
        put(&mut held, b"one", 1)?;
        let turn = Entry.step(
            &held,
            &Given::Message(&message("entry.Remove", b"", 0)),
            0.0,
        );
        assert!(turn.delete, "a removed entry kept its state");
        assert!(turn.save.is_none());
        assert_eq!(remove(&mut held)?, (true, 1));
        assert_eq!(value(&held), None);
        // Answered again, under the generation of the life that starts after the deletion, which is newer and drops
        // the listing a removal that stopped before unlisting left behind.
        let (removed, again) = remove(&mut held)?;
        assert!(!removed);
        assert!(again > 1, "{again}");
        Ok(())
    }

    #[test]
    fn a_put_after_a_removal_lists_the_key_under_a_newer_generation() -> Result<()> {
        let mut held = fresh();
        put(&mut held, b"one", 0)?;
        put(&mut held, b"one", 1)?;
        remove(&mut held)?;
        // The unlisting of the removal is under 1, and may reach the index after this listing.
        let listed = put(&mut held, b"two", 1)?;
        assert!(listed > 1, "{listed}");
        assert_eq!(value(&held), None);
        assert_eq!(put(&mut held, b"two", listed)?, 0);
        assert_eq!(generation(&held), listed);
        Ok(())
    }

    /// The generation a deleted entry had is gone with it, and the clock is what the next life starts from: an
    /// unlisting or a put of the old life that arrives late names a generation older than any the new one lists under.
    #[test]
    fn an_entry_starts_from_the_clock_so_that_its_next_life_lists_under_a_newer_generation()
    -> Result<()> {
        let before = started();
        let mut held = Entry.initial();
        assert!(generation(&held) >= before);
        assert_eq!(value(&held), None);
        let listed = put(&mut held, b"one", 0)?;
        assert_eq!(listed, generation(&held) + 1);
        // A put that listed under a generation of an old life and comes back late is sent to list again.
        assert_eq!(put(&mut held, b"one", 1)?, listed);
        assert_eq!(put(&mut held, b"one", listed)?, 0);
        Ok(())
    }

    #[test]
    fn an_entry_without_a_value_is_disposable_and_one_with_a_value_is_not() -> Result<()> {
        let mut held = fresh();
        assert!(Entry.disposable(&held));
        put(&mut held, b"one", 0)?;
        put(&mut held, b"one", 1)?;
        assert!(!Entry.disposable(&held));
        // A retired listing leaves the entry with a generation and no value, which the clock stands in for.
        let mut retired = fresh();
        retire(&mut retired, 3)?;
        assert!(Entry.disposable(&retired));
        Ok(())
    }

    #[test]
    fn a_listing_whose_value_was_never_saved_is_retired_under_its_own_generation() -> Result<()> {
        let mut held = fresh();
        // A put that listed the key under 1 and stopped before sending the value again.
        assert_eq!(put(&mut held, b"lost", 0)?, 1);
        assert_eq!(retire(&mut held, 1)?, 1);
        assert_eq!(generation(&held), 1);
        assert_eq!(value(&held), None);
        // The next put lists under a generation the unlisting of the retired one cannot drop.
        assert_eq!(put(&mut held, b"kept", 0)?, 2);
        Ok(())
    }

    #[test]
    fn a_retirement_leaves_a_saved_value_listed() -> Result<()> {
        let mut held = fresh();
        put(&mut held, b"one", 0)?;
        put(&mut held, b"one", 1)?;
        assert_eq!(retire(&mut held, 1)?, 0);
        assert_eq!(value(&held).as_deref(), Some(&b"one"[..]));
        Ok(())
    }

    #[test]
    fn a_key_never_listed_has_nothing_to_unlist() -> Result<()> {
        let mut held = fresh();
        assert_eq!(remove(&mut held)?, (false, 0));
        assert_eq!(retire(&mut held, 0)?, 0);
        assert_eq!(held, fresh());
        Ok(())
    }
}
