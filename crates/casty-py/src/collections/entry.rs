//! One entry of a dict: the value under a key, and the index it is listed in.

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Asking, Given, Native, Turn, named};

const VALUE: &str = "value";
const INDEXED: &str = "indexed";

#[derive(Debug)]
pub struct Entry;

impl Native for Entry {
    fn initial(&self) -> Pages {
        Pages::from([
            (VALUE.to_owned(), optional(None)),
            (INDEXED.to_owned(), truth(false)),
        ])
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let message = match given {
            Given::Message(message) | Given::Answered { message, .. } => *message,
            Given::Alarm => return Turn::default(),
        };
        let Ok(read) = self::read(message) else {
            return Turn::default();
        };
        let value = value(held);
        let indexed = indexed(held);
        let mut turn = Turn::default();
        let answer = match named(&read.tag) {
            "Put" => {
                let (Some(key), Some(put)) = (read.key, read.value) else {
                    return Turn::default();
                };
                // Register first: a failed value commit may leave an empty entry, never an unlisted saved value.
                if !indexed && matches!(given, Given::Message(_)) {
                    let Some(index) = read.index else {
                        return Turn::default();
                    };
                    turn.ask = Some(Asking {
                        to: index,
                        message: Box::new(move |reply| listing(reply, &key)),
                    });
                    return turn;
                }
                turn.save = Some(saved(Some(&put), true));
                nil()
            }
            "Get" => optional(value.as_deref()),
            "Contains" => truth(value.is_some()),
            "Remove" => {
                let present = value.is_some();
                if present {
                    turn.save = Some(saved(None, indexed));
                }
                truth(present)
            }
            _ => return Turn::default(),
        };
        turn.replies = vec![(read.reply, answer)];
        turn
    }
}

/// Everything the four messages carry between them.
struct Held {
    tag: String,
    reply: Target,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    index: Option<Target>,
}

fn read(message: &[u8]) -> Result<Held> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut key = None;
    let mut value = None;
    let mut index = None;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "key" => key = Some(reading.bytes()?),
            "value" => value = Some(reading.bytes()?),
            "index" => index = Some(reading.target()?),
            _ => reading.skip()?,
        }
    }
    Ok(Held {
        tag,
        reply: reply.ok_or(Malformed::Truncated)?,
        key,
        value,
        index,
    })
}

/// `table.Add(reply_to, key, b"")`: the key listed in the index, with no value of its own.
fn listing(reply: &Target, key: &[u8]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Add", 3);
    writer.name("reply_to");
    writer.target(reply);
    writer.name("key");
    writer.bytes(key);
    writer.name("value");
    writer.bytes(&[]);
    writer.finish()
}

fn value(held: &Pages) -> Option<Vec<u8>> {
    let page = held.get(VALUE)?;
    let mut reading = Reading::new(page);
    if reading.nil().ok()? {
        return None;
    }
    reading.bytes().ok()
}

fn indexed(held: &Pages) -> bool {
    held.get(INDEXED)
        .and_then(|page| Reading::new(page).bool().ok())
        .unwrap_or(false)
}

fn saved(value: Option<&[u8]>, indexed: bool) -> Pages {
    Pages::from([
        (VALUE.to_owned(), optional(value)),
        (INDEXED.to_owned(), truth(indexed)),
    ])
}

fn optional(value: Option<&[u8]>) -> Vec<u8> {
    let mut writer = Writer::new();
    match value {
        Some(value) => writer.bytes(value),
        None => writer.nil(),
    }
    writer.finish()
}

fn truth(value: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.bool(value);
    writer.finish()
}

fn nil() -> Vec<u8> {
    optional(None)
}
