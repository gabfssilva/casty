//! A counter: a number that goes up and down, and reads back.

use casty_core::store::Pages;
use casty_core::wire::Reading;

use super::{Given, Native, Turn, WHOLE, named, nil, number, number_in};

#[derive(Debug)]
pub struct Counter;

impl Native for Counter {
    fn initial(&self) -> Pages {
        Pages::from([(WHOLE.to_owned(), number(0))])
    }

    fn step(&self, held: &Pages, given: &Given<'_>, _: f64) -> Turn {
        let Given::Message(message) = given else {
            return Turn::default();
        };
        let count = number_in(held, WHOLE, 0);
        let Ok((tag, reply, delta)) = read(message) else {
            return Turn::default();
        };
        let mut turn = Turn::default();
        let answer = match named(&tag) {
            "Add" => {
                turn.save = Some(Pages::from([(WHOLE.to_owned(), number(count + delta))]));
                nil()
            }
            "Reset" => {
                turn.save = Some(Pages::from([(WHOLE.to_owned(), number(0))]));
                nil()
            }
            "Get" => number(count),
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
