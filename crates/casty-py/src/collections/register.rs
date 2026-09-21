//! A register: one value, replaced whole, with a compare-and-set that decides in one message.

use casty_core::node::Target;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Given, Native, Turn, named};

/// The only field of the state, which is the page it lives in.
const VALUE: &str = "value";

#[derive(Debug)]
pub struct Register;

impl Native for Register {
    fn initial(&self) -> Pages {
        Pages::from([(VALUE.to_owned(), nil())])
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let Given::Message(message) = given else {
            return Turn::default();
        };
        let Ok(read) = read(message) else {
            return Turn::default();
        };
        let value = value(held);
        let mut turn = Turn::default();
        let answer = match named(&read.tag) {
            "Get" => optional(value.as_deref()),
            "Put" => {
                turn.save = Some(saved(read.value.as_deref()));
                nil()
            }
            "CompareAndSet" => {
                let same = value.as_deref() == read.expected.as_deref();
                if same {
                    turn.save = Some(saved(read.value.as_deref()));
                }
                truth(same)
            }
            "GetAndSet" => {
                turn.save = Some(saved(read.value.as_deref()));
                optional(value.as_deref())
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
    value: Option<Vec<u8>>,
    expected: Option<Vec<u8>>,
}

fn read(message: &[u8]) -> Result<Held> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut value = None;
    let mut expected = None;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "value" => value = optional_bytes(&mut reading)?,
            "expected" => expected = optional_bytes(&mut reading)?,
            _ => reading.skip()?,
        }
    }
    Ok(Held {
        tag,
        reply: reply.ok_or(casty_core::schema::msgpack::Malformed::Truncated)?,
        value,
        expected,
    })
}

fn optional_bytes(reading: &mut Reading<'_>) -> Result<Option<Vec<u8>>> {
    if reading.nil()? {
        return Ok(None);
    }
    Ok(Some(reading.bytes()?))
}

fn value(held: &Pages) -> Option<Vec<u8>> {
    let page = held.get(VALUE)?;
    let mut reading = Reading::new(page);
    optional_bytes(&mut reading).ok().flatten()
}

fn saved(value: Option<&[u8]>) -> Pages {
    Pages::from([(VALUE.to_owned(), optional(value))])
}

fn optional(value: Option<&[u8]>) -> Vec<u8> {
    let mut writer = Writer::new();
    match value {
        Some(value) => writer.bytes(value),
        None => writer.nil(),
    }
    writer.finish()
}

fn nil() -> Vec<u8> {
    optional(None)
}

fn truth(value: bool) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.bool(value);
    writer.finish()
}
