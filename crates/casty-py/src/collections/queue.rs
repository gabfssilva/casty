//! A queue: items taken in the order they were offered.

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Given, Native, Turn, WHOLE, named};

#[derive(Debug)]
pub struct Queue;

impl Native for Queue {
    fn initial(&self) -> Pages {
        Pages::from([(WHOLE.to_owned(), written(&[]))])
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let Given::Message(message) = given else {
            return Turn::default();
        };
        let Ok(read) = read(message) else {
            return Turn::default();
        };
        let mut items = items(held);
        let mut turn = Turn::default();
        let answer = match named(&read.tag) {
            "Offer" => {
                let Some(item) = read.item else {
                    return Turn::default();
                };
                items.push(item);
                turn.save = Some(saved(&items));
                nil()
            }
            "Poll" => {
                if items.is_empty() {
                    nil()
                } else {
                    let first = items.remove(0);
                    turn.save = Some(saved(&items));
                    optional(Some(&first))
                }
            }
            "Peek" => optional(items.first().map(Vec::as_slice)),
            "Drain" => {
                // A negative limit is refused by the facade, and here it simply takes nothing.
                let limit = usize::try_from(read.count.unwrap_or(0)).unwrap_or(0);
                let taken: Vec<Vec<u8>> = items.drain(..limit.min(items.len())).collect();
                if !taken.is_empty() {
                    turn.save = Some(saved(&items));
                }
                written(&taken)
            }
            "Size" => number(i64::try_from(items.len()).unwrap_or(i64::MAX)),
            "Clear" => {
                turn.save = Some(saved(&[]));
                nil()
            }
            _ => return Turn::default(),
        };
        turn.replies = vec![(read.reply, answer)];
        turn
    }
}

/// Everything the six messages carry between them.
struct Held {
    tag: String,
    reply: Target,
    item: Option<Vec<u8>>,
    count: Option<i64>,
}

fn read(message: &[u8]) -> Result<Held> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut item = None;
    let mut count = None;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "value" => item = Some(reading.bytes()?),
            "limit" => count = Some(reading.int()?),
            _ => reading.skip()?,
        }
    }
    Ok(Held {
        tag,
        reply: reply.ok_or(Malformed::Truncated)?,
        item,
        count,
    })
}

fn items(held: &Pages) -> Vec<Vec<u8>> {
    let Some(page) = held.get(WHOLE) else {
        return Vec::new();
    };
    read_items(page).unwrap_or_default()
}

fn read_items(page: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut reading = Reading::new(page);
    let count = reading.items()?;
    (0..count).map(|_| reading.bytes()).collect()
}

fn saved(items: &[Vec<u8>]) -> Pages {
    Pages::from([(WHOLE.to_owned(), written(items))])
}

fn written(items: &[Vec<u8>]) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.items(items.len());
    for item in items {
        writer.bytes(item);
    }
    writer.finish()
}

fn optional(item: Option<&[u8]>) -> Vec<u8> {
    let mut writer = Writer::new();
    match item {
        Some(item) => writer.bytes(item),
        None => writer.nil(),
    }
    writer.finish()
}

fn number(value: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.int(value);
    writer.finish()
}

fn nil() -> Vec<u8> {
    let mut writer = Writer::new();
    writer.nil();
    writer.finish()
}
