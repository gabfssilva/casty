//! An index: a key with the values listed under it, which is what a set, a dict and a multimap are kept in.

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Given, Native, Turn, named};

/// The only field of the state, which is the page it lives in.
const ENTRIES: &str = "entries";

/// What a key holds, in the order the entries were written.
pub type Entries = Vec<(Vec<u8>, Vec<Vec<u8>>)>;

#[derive(Debug)]
pub struct Table;

impl Native for Table {
    fn initial(&self) -> Pages {
        Pages::from([(ENTRIES.to_owned(), written(&Entries::new()))])
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let Given::Message(message) = given else {
            return Turn::default();
        };
        let Ok(read) = read(message) else {
            return Turn::default();
        };
        let mut entries = entries(held);
        let mut turn = Turn::default();
        let answer = match named(&read.tag) {
            "Add" => {
                let (Some(key), Some(value)) = (read.key, read.value) else {
                    return Turn::default();
                };
                let listed = at(&mut entries, &key);
                let changed = !listed.contains(&value);
                if changed {
                    listed.push(value);
                    turn.save = Some(saved(&entries));
                }
                truth(changed)
            }
            "Get" => {
                let Some(key) = read.key else {
                    return Turn::default();
                };
                listed(&entries, &key)
            }
            "Remove" => {
                let Some(key) = read.key else {
                    return Turn::default();
                };
                let removed = remove(&mut entries, &key, read.value.as_deref());
                if removed > 0 {
                    turn.save = Some(saved(&entries));
                }
                number(count(removed))
            }
            "Size" => number(entries.iter().map(|(_, values)| count(values.len())).sum()),
            "Clear" => {
                turn.save = Some(saved(&Entries::new()));
                nil()
            }
            "Items" => written(&entries),
            _ => return Turn::default(),
        };
        turn.replies = vec![(read.reply, answer)];
        turn
    }
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

/// A count as the schema writes one, which is a signed number.
fn count(held: usize) -> i64 {
    i64::try_from(held).unwrap_or(i64::MAX)
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

/// Everything the six messages carry between them.
struct Held {
    tag: String,
    reply: Target,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

fn read(message: &[u8]) -> Result<Held> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut key = None;
    let mut value = None;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "key" => key = Some(reading.bytes()?),
            "value" => {
                value = if reading.nil()? {
                    None
                } else {
                    Some(reading.bytes()?)
                };
            }
            _ => reading.skip()?,
        }
    }
    Ok(Held {
        tag,
        reply: reply.ok_or(Malformed::Truncated)?,
        key,
        value,
    })
}

fn entries(held: &Pages) -> Entries {
    let Some(page) = held.get(ENTRIES) else {
        return Entries::new();
    };
    read_entries(page).unwrap_or_default()
}

fn read_entries(page: &[u8]) -> Result<Entries> {
    let mut reading = Reading::new(page);
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

fn saved(entries: &Entries) -> Pages {
    Pages::from([(ENTRIES.to_owned(), written(entries))])
}

/// The entries as a mapping, which the schema writes as a list of pairs.
fn written(entries: &Entries) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.pairs(entries.len());
    for (key, values) in entries {
        writer.pair();
        writer.bytes(key);
        writer.items(values.len());
        for value in values {
            writer.bytes(value);
        }
    }
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

fn number(value: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.int(value);
    writer.finish()
}

fn truth(value: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.bool(value);
    writer.finish()
}

fn nil() -> Vec<u8> {
    let mut writer = Writer::new();
    writer.nil();
    writer.finish()
}
