//! The messages of the `actors` and `replies` components.
//!
//! A command travels to the owner of its key and comes back when the owner it found disagrees. The answer of a
//! request travels on its own band, addressed by the id the caller took.

use casty_core::mailbox::{Command, Deliver, Start};
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_core::schema::msgpack::Malformed;
use casty_core::wire::{Reading, Result, Writer};

/// A command on its way to the owner of its key: the node that routed it, and which attempt this is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Routed {
    pub command: Command,
    pub origin: NodeId,
    pub attempt: u32,
}

/// What travels between the `actors` components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Routed(Routed),
    /// The answer of a node that is not the owner of the key in its own view, with the command it did not take.
    WrongOwner(Routed),
}

/// The answer of a request, addressed by the id the caller took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub id: i64,
    pub outcome: Outcome,
}

#[must_use]
pub fn encode(message: &Message) -> Vec<u8> {
    let mut writer = Writer::new();
    let (tag, routed) = match message {
        Message::Routed(routed) => ("Routed", routed),
        Message::WrongOwner(routed) => ("WrongOwner", routed),
    };
    match message {
        Message::Routed(_) => {
            writer.tagged(tag, 3);
            routed_fields(&mut writer, routed);
        }
        Message::WrongOwner(_) => {
            writer.tagged(tag, 1);
            writer.name("routed");
            writer.fields(3);
            routed_fields(&mut writer, routed);
        }
    }
    writer.finish()
}

pub fn decode(payload: &[u8]) -> Result<Message> {
    let mut reading = Reading::new(payload);
    let (tag, fields) = reading.tagged()?;
    match tag.as_str() {
        "Routed" => Ok(Message::Routed(read_routed_fields(&mut reading, fields)?)),
        "WrongOwner" => {
            let mut held = None;
            for _ in 0..fields {
                match reading.name()? {
                    "routed" => {
                        let inner = reading.fields()?;
                        held = Some(read_routed_fields(&mut reading, inner)?);
                    }
                    _ => reading.skip()?,
                }
            }
            Ok(Message::WrongOwner(held.ok_or(Malformed::Truncated)?))
        }
        _ => Err(Malformed::Marker(0)),
    }
}

#[must_use]
pub fn encode_answer(answer: &Answer) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Reply", 2);
    writer.name("id");
    writer.int(answer.id);
    writer.name("outcome");
    outcome(&mut writer, &answer.outcome);
    writer.finish()
}

pub fn decode_answer(payload: &[u8]) -> Result<Answer> {
    let mut reading = Reading::new(payload);
    let (tag, fields) = reading.tagged()?;
    if tag != "Reply" {
        return Err(Malformed::Marker(0));
    }
    let mut id = None;
    let mut held = None;
    for _ in 0..fields {
        match reading.name()? {
            "id" => id = Some(reading.int()?),
            "outcome" => held = Some(read_outcome(&mut reading)?),
            _ => reading.skip()?,
        }
    }
    Ok(Answer {
        id: id.ok_or(Malformed::Truncated)?,
        outcome: held.ok_or(Malformed::Truncated)?,
    })
}

fn routed_fields(writer: &mut Writer, routed: &Routed) {
    writer.name("command");
    command(writer, &routed.command);
    writer.name("origin");
    writer.node(&routed.origin);
    writer.name("attempt");
    writer.unsigned(u64::from(routed.attempt));
}

fn command(writer: &mut Writer, held: &Command) {
    match held {
        Command::Deliver(deliver) => {
            writer.tagged("Deliver", 4);
            writer.name("actor");
            writer.text(&deliver.actor);
            writer.name("key");
            writer.text(&deliver.key);
            writer.name("message");
            writer.bytes(&deliver.message);
            writer.name("reply");
            match &deliver.reply {
                // `ReplyTarget | None` is a union, so the target travels under its tag.
                Some(target) => tagged_reply(writer, target),
                None => writer.nil(),
            }
        }
        Command::Start(start) => {
            writer.tagged("Start", 3);
            writer.name("actor");
            writer.text(&start.actor);
            writer.name("key");
            writer.text(&start.key);
            writer.name("state");
            match &start.state {
                Some(state) => writer.bytes(state),
                None => writer.nil(),
            }
        }
    }
}

/// Whoever waits, on a node, for the answer of the request numbered `id`: an alternative of a union, so tagged.
fn tagged_reply(writer: &mut Writer, target: &Target) {
    writer.tagged("ReplyTarget", 2);
    let Target::Reply { node, id } = target else {
        unreachable!("a command answers a request, never an entity");
    };
    writer.name("node");
    writer.node(node);
    writer.name("id");
    writer.int(*id);
}

fn outcome(writer: &mut Writer, held: &Outcome) {
    match held {
        Outcome::Value(data) => {
            writer.tagged("Value", 1);
            writer.name("data");
            writer.bytes(data);
        }
        Outcome::Failed {
            actor,
            key,
            error,
            message,
        } => {
            writer.tagged("Failed", 4);
            writer.name("actor");
            writer.text(actor);
            writer.name("key");
            writer.text(key);
            writer.name("error");
            writer.text(error);
            writer.name("message");
            writer.text(message);
        }
        Outcome::Missing { actor, key } => named(writer, "Missing", actor, key),
        Outcome::Full { actor, key } => named(writer, "Full", actor, key),
        Outcome::Unreached { actor, key } => named(writer, "Unreached", actor, key),
        Outcome::Unknown { actor, key } => named(writer, "Unknown", actor, key),
    }
}

fn named(writer: &mut Writer, tag: &str, actor: &str, key: &str) {
    writer.tagged(tag, 2);
    writer.name("actor");
    writer.text(actor);
    writer.name("key");
    writer.text(key);
}

fn read_routed_fields(reading: &mut Reading<'_>, fields: usize) -> Result<Routed> {
    let mut held = None;
    let mut origin = None;
    let mut attempt = None;
    for _ in 0..fields {
        match reading.name()? {
            "command" => held = Some(read_command(reading)?),
            "origin" => origin = Some(reading.node()?),
            "attempt" => {
                attempt =
                    Some(u32::try_from(reading.unsigned()?).map_err(|_| Malformed::Truncated)?);
            }
            _ => reading.skip()?,
        }
    }
    Ok(Routed {
        command: held.ok_or(Malformed::Truncated)?,
        origin: origin.ok_or(Malformed::Truncated)?,
        attempt: attempt.ok_or(Malformed::Truncated)?,
    })
}

fn read_command(reading: &mut Reading<'_>) -> Result<Command> {
    let (tag, fields) = reading.tagged()?;
    let mut actor = None;
    let mut key = None;
    let mut message = None;
    let mut state = None;
    let mut target = None;
    for _ in 0..fields {
        match reading.name()? {
            "actor" => actor = Some(reading.text()?),
            "key" => key = Some(reading.text()?),
            "message" => message = Some(reading.bytes()?),
            "state" => {
                if !reading.nil()? {
                    state = Some(reading.bytes()?);
                }
            }
            "reply" => target = read_reply(reading)?,
            _ => reading.skip()?,
        }
    }
    let actor = actor.ok_or(Malformed::Truncated)?;
    let key = key.ok_or(Malformed::Truncated)?;
    match tag.as_str() {
        "Deliver" => Ok(Command::Deliver(Deliver {
            actor,
            key,
            message: message.ok_or(Malformed::Truncated)?,
            reply: target,
        })),
        "Start" => Ok(Command::Start(Start { actor, key, state })),
        _ => Err(Malformed::Marker(0)),
    }
}

fn read_reply(reading: &mut Reading<'_>) -> Result<Option<Target>> {
    if reading.nil()? {
        return Ok(None);
    }
    // The target is an alternative of a union, so it carries its tag.
    let fields = reading.tagged()?.1;
    let mut node = None;
    let mut id = None;
    for _ in 0..fields {
        match reading.name()? {
            "node" => node = Some(reading.node()?),
            "id" => id = Some(reading.int()?),
            _ => reading.skip()?,
        }
    }
    Ok(Some(Target::Reply {
        node: node.ok_or(Malformed::Truncated)?,
        id: id.ok_or(Malformed::Truncated)?,
    }))
}

fn read_outcome(reading: &mut Reading<'_>) -> Result<Outcome> {
    let (tag, fields) = reading.tagged()?;
    let mut actor = String::new();
    let mut key = String::new();
    let mut error = String::new();
    let mut message = String::new();
    let mut data = Vec::new();
    for _ in 0..fields {
        match reading.name()? {
            "actor" => actor = reading.text()?,
            "key" => key = reading.text()?,
            "error" => error = reading.text()?,
            "message" => message = reading.text()?,
            "data" => data = reading.bytes()?,
            _ => reading.skip()?,
        }
    }
    Ok(match tag.as_str() {
        "Value" => Outcome::Value(data),
        "Failed" => Outcome::Failed {
            actor,
            key,
            error,
            message,
        },
        "Missing" => Outcome::Missing { actor, key },
        "Full" => Outcome::Full { actor, key },
        "Unreached" => Outcome::Unreached { actor, key },
        "Unknown" => Outcome::Unknown { actor, key },
        _ => return Err(Malformed::Marker(0)),
    })
}

#[cfg(test)]
mod tests {
    use casty_core::mailbox::{Command, Deliver, Start};
    use casty_core::node::Target;
    use casty_core::outcome::Outcome;
    use casty_core::rolls::Rolls;

    use super::{Answer, Message, Routed, decode, decode_answer, encode, encode_answer};

    #[test]
    fn every_command_reads_back_as_it_was_written() {
        let ids = Rolls::seeded(61).nodes(2);
        let reply = Target::Reply {
            node: ids[1].clone(),
            id: 42,
        };
        let asked = Routed {
            command: Command::Deliver(Deliver {
                actor: "tests.app:account".to_owned(),
                key: "acc-1".to_owned(),
                message: vec![1, 2, 3],
                reply: Some(reply.clone()),
            }),
            origin: ids[0].clone(),
            attempt: 1,
        };
        let told = Routed {
            command: Command::Deliver(Deliver {
                actor: "tests.app:account".to_owned(),
                key: "acc-1".to_owned(),
                message: Vec::new(),
                reply: None,
            }),
            origin: ids[0].clone(),
            attempt: 2,
        };
        let started = Routed {
            command: Command::Start(Start {
                actor: "tests.app:order".to_owned(),
                key: "o-1".to_owned(),
                state: Some(vec![7]),
            }),
            origin: ids[0].clone(),
            attempt: 1,
        };

        for message in [
            Message::Routed(asked.clone()),
            Message::Routed(told),
            Message::Routed(started),
            Message::WrongOwner(asked),
        ] {
            assert_eq!(decode(&encode(&message)), Ok(message));
        }
    }

    #[test]
    fn every_answer_reads_back_as_it_was_written() {
        let outcomes = [
            Outcome::Value(vec![5, 6]),
            Outcome::Failed {
                actor: "a".to_owned(),
                key: "k".to_owned(),
                error: "RuntimeError".to_owned(),
                message: "boom".to_owned(),
            },
            Outcome::missing("a", "k"),
            Outcome::full("a", "k"),
            Outcome::unreached("a", "k"),
            Outcome::unknown("a", "k"),
        ];
        for outcome in outcomes {
            let answer = Answer { id: 7, outcome };
            assert_eq!(decode_answer(&encode_answer(&answer)), Ok(answer));
        }
    }

    #[test]
    fn a_payload_that_is_not_one_of_these_messages_is_refused() {
        assert!(decode(&[0xc0]).is_err());
        assert!(decode_answer(&[]).is_err());
        assert!(
            decode(&encode_answer(&Answer {
                id: 1,
                outcome: Outcome::missing("a", "k")
            }))
            .is_err()
        );
    }
}
