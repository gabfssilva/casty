//! A counter: a number that goes up and down, and reads back.

use casty_core::store::Pages;

use super::{Given, Native, Turn, WHOLE, fields, nil, number, number_in};

#[derive(Debug)]
pub struct Counter;

impl Native for Counter {
    fn initial(&self) -> Pages {
        Pages::from([(WHOLE.to_owned(), number(0))])
    }

    fn turn(&self, held: &Pages, given: &Given<'_>, _: f64) -> Option<Turn> {
        let mut delta = 0;
        let (tag, reply) = fields(given.message()?, |name, reading| {
            match name {
                "delta" => delta = reading.int()?,
                _ => reading.skip()?,
            }
            Ok(())
        })?;
        let count = number_in(held, WHOLE, 0);
        let mut turn = Turn::default();
        let answer = match tag {
            "Add" => {
                turn.save = Some(Pages::from([(WHOLE.to_owned(), number(count + delta))]));
                nil()
            }
            "Reset" => {
                turn.save = Some(Pages::from([(WHOLE.to_owned(), number(0))]));
                nil()
            }
            "Get" => number(count),
            _ => return None,
        };
        turn.replies = vec![(reply, answer)];
        Some(turn)
    }
}
