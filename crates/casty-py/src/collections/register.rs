//! A register: one value, replaced whole, with a compare-and-set that decides in one message.

use casty_core::store::Pages;

use super::{Given, Native, Turn, fields, nil, optional, optional_bytes, optional_in, truth};

/// The only field of the state, which is the page it lives in.
const VALUE: &str = "value";

#[derive(Debug)]
pub struct Register;

impl Native for Register {
    fn initial(&self) -> Pages {
        Pages::from([(VALUE.to_owned(), nil())])
    }

    fn turn(&self, held: &Pages, given: &Given<'_>, _: f64) -> Option<Turn> {
        let (mut value, mut expected) = (None, None);
        let (tag, reply) = fields(given.message()?, |name, reading| {
            match name {
                "value" => value = optional_bytes(reading)?,
                "expected" => expected = optional_bytes(reading)?,
                _ => reading.skip()?,
            }
            Ok(())
        })?;
        let current = optional_in(held, VALUE);
        let mut turn = Turn::default();
        let answer = match tag {
            "Get" => optional(current.as_deref()),
            "Put" => {
                turn.save = Some(saved(value.as_deref()));
                nil()
            }
            "CompareAndSet" => {
                let same = current.as_deref() == expected.as_deref();
                if same {
                    turn.save = Some(saved(value.as_deref()));
                }
                truth(same)
            }
            "GetAndSet" => {
                turn.save = Some(saved(value.as_deref()));
                optional(current.as_deref())
            }
            _ => return None,
        };
        turn.replies = vec![(reply, answer)];
        Some(turn)
    }
}

fn saved(value: Option<&[u8]>) -> Pages {
    Pages::from([(VALUE.to_owned(), optional(value))])
}
