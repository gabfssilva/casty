//! A counter: a number that goes up and down, and reads back.

use casty_core::store::Pages;
use casty_core::wire::{Reading, Writer};

use super::{Given, Native, Turn, WHOLE, named};

#[derive(Debug)]
pub struct Counter;

impl Native for Counter {
    fn initial(&self) -> Pages {
        Pages::from([(WHOLE.to_owned(), written(0))])
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let Given::Message(message) = given else {
            return Turn::default();
        };
        let count = count(held);
        let Ok((tag, reply, delta)) = read(message) else {
            return Turn::default();
        };
        let mut turn = Turn::default();
        let answer = match named(&tag) {
            "Add" => {
                turn.save = Some(Pages::from([(WHOLE.to_owned(), written(count + delta))]));
                nothing()
            }
            "Reset" => {
                turn.save = Some(Pages::from([(WHOLE.to_owned(), written(0))]));
                nothing()
            }
            "Get" => written(count),
            _ => return Turn::default(),
        };
        turn.replies = vec![(reply, answer)];
        turn
    }
}

/// The message, as the three of them are one shape: a tag, who asked, and the only number any of them carries.
fn read(message: &[u8]) -> casty_core::wire::Result<(String, casty_core::node::Target, i64)> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut delta = 0;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "delta" => delta = reading.int()?,
            _ => reading.skip()?,
        }
    }
    Ok((
        tag,
        reply.ok_or(casty_core::schema::msgpack::Malformed::Truncated)?,
        delta,
    ))
}

fn count(held: &Pages) -> i64 {
    held.get(WHOLE)
        .and_then(|page| Reading::new(page).int().ok())
        .unwrap_or(0)
}

fn written(count: i64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.int(count);
    writer.finish()
}

fn nothing() -> Vec<u8> {
    let mut writer = Writer::new();
    writer.nil();
    writer.finish()
}
