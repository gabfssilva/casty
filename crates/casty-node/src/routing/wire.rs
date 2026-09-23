//! The messages of the `actors` and `replies` components.
//!
//! A command travels to the owner of its key and comes back when the owner it found disagrees. The answer of a
//! request travels on its own band, addressed by the id the caller took. The cancellation of a request travels like
//! the request, to the owner of its key, and names it by that same address.

use casty_core::chain::{Chain, Link};
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

/// A caller that stopped waiting for the answer of a request it sent to `(actor, key)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cancel {
    pub actor: String,
    pub key: String,
    /// Where the answer of the request was to go, which is how the node running it knows it.
    pub request: Target,
}

/// What travels between the `actors` components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Routed(Routed),
    /// The answer of a node that is not the owner of the key in its own view, with the command it did not take.
    WrongOwner(Routed),
    Cancel(Cancel),
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
    match message {
        Message::Routed(routed) => {
            writer.tagged("Routed", 3);
            routed_fields(&mut writer, routed);
        }
        Message::WrongOwner(routed) => {
            writer.tagged("WrongOwner", 1);
            writer.name("routed");
            writer.fields(3);
            routed_fields(&mut writer, routed);
        }
        Message::Cancel(cancel) => {
            writer.tagged("Cancel", 3);
            writer.name("actor");
            writer.text(&cancel.actor);
            writer.name("key");
            writer.text(&cancel.key);
            writer.name("request");
            writer.target(&cancel.request);
        }
    }
    writer.finish()
}

pub fn decode(payload: &[u8]) -> Result<Message> {
    let mut reading = Reading::new(payload);
    let (tag, fields) = reading.tagged()?;
    match tag.as_str() {
        "Routed" => Ok(Message::Routed(read_routed_fields(&mut reading, fields)?)),
        "Cancel" => Ok(Message::Cancel(read_cancel_fields(&mut reading, fields)?)),
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

/// How many bytes `command`, routed from `origin`, takes on the wire in its largest form: sent back by an owner that
/// disagrees, on whatever attempt. The payload is counted, not copied: the rest is written around an empty one.
#[must_use]
pub fn routed_size(command: &Command, origin: &NodeId) -> usize {
    let (hollow, payload) = match command {
        Command::Deliver(deliver) => (
            Command::Deliver(Deliver {
                actor: deliver.actor.clone(),
                key: deliver.key.clone(),
                message: Vec::new(),
                reply: deliver.reply.clone(),
                chain: deliver.chain.clone(),
            }),
            deliver.message.len(),
        ),
        Command::Start(start) => (
            Command::Start(Start {
                actor: start.actor.clone(),
                key: start.key.clone(),
                state: start.state.as_ref().map(|_| Vec::new()),
            }),
            start.state.as_ref().map_or(0, Vec::len),
        ),
    };
    let around = encode(&Message::WrongOwner(Routed {
        command: hollow,
        origin: origin.clone(),
        attempt: u32::MAX,
    }));
    around.len() + grown(payload)
}

/// How many bytes the answer `outcome` to the request `id` takes on the wire. A value is counted, not copied.
#[must_use]
pub fn answer_size(id: i64, outcome: &Outcome) -> usize {
    let Outcome::Value(data) = outcome else {
        return encode_answer(&Answer {
            id,
            outcome: outcome.clone(),
        })
        .len();
    };
    let around = encode_answer(&Answer {
        id,
        outcome: Outcome::Value(Vec::new()),
    });
    around.len() + grown(data.len())
}

/// What `len` bytes add to a msgpack `bin` written empty: themselves, and the longer length that says how many.
fn grown(len: usize) -> usize {
    let length = match len {
        0..=0xff => 1,
        0x100..=0xffff => 2,
        _ => 4,
    };
    len + length - 1
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
            // A message that keeps nobody waiting, every `tell` and every `ask` from outside a body, carries no chain.
            let chained = !deliver.chain.is_empty();
            writer.tagged("Deliver", if chained { 5 } else { 4 });
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
            if chained {
                writer.name("chain");
                chain(writer, &deliver.chain);
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

fn chain(writer: &mut Writer, held: &Chain) {
    writer.items(held.links().len());
    for link in held.links() {
        writer.fields(3);
        writer.name("actor");
        writer.text(&link.actor);
        writer.name("key");
        writer.text(&link.key);
        writer.name("hold");
        writer.unsigned(link.hold);
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
        Outcome::TooLarge(why) => {
            writer.tagged("TooLarge", 1);
            writer.name("message");
            writer.text(why);
        }
        Outcome::Cycle(cycle) => {
            writer.tagged("Cycle", 1);
            writer.name("message");
            writer.text(cycle);
        }
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

fn read_cancel_fields(reading: &mut Reading<'_>, fields: usize) -> Result<Cancel> {
    let mut actor = None;
    let mut key = None;
    let mut request = None;
    for _ in 0..fields {
        match reading.name()? {
            "actor" => actor = Some(reading.text()?),
            "key" => key = Some(reading.text()?),
            "request" => request = Some(reading.target()?),
            _ => reading.skip()?,
        }
    }
    Ok(Cancel {
        actor: actor.ok_or(Malformed::Truncated)?,
        key: key.ok_or(Malformed::Truncated)?,
        request: request.ok_or(Malformed::Truncated)?,
    })
}

fn read_command(reading: &mut Reading<'_>) -> Result<Command> {
    let (tag, fields) = reading.tagged()?;
    let mut actor = None;
    let mut key = None;
    let mut message = None;
    let mut state = None;
    let mut target = None;
    let mut chain = Chain::default();
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
            "chain" => chain = read_chain(reading)?,
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
            chain,
        })),
        "Start" => Ok(Command::Start(Start { actor, key, state })),
        _ => Err(Malformed::Marker(0)),
    }
}

/// The callers of an `ask`, cut to the bound of a chain whatever the sender wrote.
fn read_chain(reading: &mut Reading<'_>) -> Result<Chain> {
    let count = reading.items()?;
    let mut links = Vec::with_capacity(count.min(casty_core::chain::BOUND));
    for _ in 0..count {
        let fields = reading.fields()?;
        let mut actor = None;
        let mut key = None;
        let mut hold = None;
        for _ in 0..fields {
            match reading.name()? {
                "actor" => actor = Some(reading.text()?),
                "key" => key = Some(reading.text()?),
                "hold" => hold = Some(reading.unsigned()?),
                _ => reading.skip()?,
            }
        }
        links.push(Link {
            actor: actor.ok_or(Malformed::Truncated)?,
            key: key.ok_or(Malformed::Truncated)?,
            hold: hold.ok_or(Malformed::Truncated)?,
        });
    }
    Ok(Chain::of(links))
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
        "TooLarge" => Outcome::TooLarge(message),
        "Cycle" => Outcome::Cycle(message),
        _ => return Err(Malformed::Marker(0)),
    })
}

#[cfg(test)]
mod tests {
    use casty_core::chain::{BOUND, Chain, Link};
    use casty_core::mailbox::{Command, Deliver, Start};
    use casty_core::node::{NodeId, Target};
    use casty_core::outcome::Outcome;
    use casty_core::rolls::Rolls;
    use casty_core::schema::msgpack::Malformed;
    use casty_core::wire::Writer;

    use super::{
        Answer, Cancel, Message, Routed, answer_size, decode, decode_answer, encode, encode_answer,
        routed_size,
    };

    /// A chain of `hops` callers, each a key of its own.
    fn chained(hops: u64) -> Chain {
        (0..hops).fold(Chain::default(), |chain, hop| {
            chain.then(Link {
                actor: "tests.app:hop".to_owned(),
                key: format!("h-{hop:04}"),
                hold: u64::MAX - hop,
            })
        })
    }

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
                chain: Chain::default(),
            }),
            origin: ids[0].clone(),
            attempt: 1,
        };
        let asked_from_a_body = Routed {
            command: Command::Deliver(Deliver {
                actor: "tests.app:account".to_owned(),
                key: "acc-1".to_owned(),
                message: vec![1, 2, 3],
                reply: Some(reply.clone()),
                chain: chained(3),
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
                chain: Chain::default(),
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
            Message::Routed(asked_from_a_body.clone()),
            Message::Routed(told),
            Message::Routed(started),
            Message::WrongOwner(asked),
            Message::WrongOwner(asked_from_a_body),
        ] {
            assert_eq!(decode(&encode(&message)), Ok(message));
        }
    }

    #[test]
    fn a_chain_costs_nothing_when_empty_and_stays_within_its_bound_however_deep() {
        let ids = Rolls::seeded(63).nodes(1);
        let routed = |chain: Chain| {
            encode(&Message::Routed(Routed {
                command: Command::Deliver(Deliver {
                    actor: "tests.app:account".to_owned(),
                    key: "acc-1".to_owned(),
                    message: vec![1, 2, 3],
                    reply: None,
                    chain,
                }),
                origin: ids[0].clone(),
                attempt: 1,
            }))
            .len()
        };
        let bound = u64::try_from(BOUND).unwrap();
        let full = routed(chained(bound));
        assert!(routed(chained(1)) > routed(Chain::default()));
        assert_eq!(routed(chained(bound * 10)), full);

        // A sender that writes more than the bound is read as its most recent callers only.
        let mut writer = Writer::new();
        writer.tagged("Routed", 3);
        writer.name("command");
        writer.tagged("Deliver", 5);
        writer.name("actor");
        writer.text("a");
        writer.name("key");
        writer.text("k");
        writer.name("message");
        writer.bytes(&[]);
        writer.name("reply");
        writer.nil();
        writer.name("chain");
        writer.items(BOUND + 2);
        for hop in 0..BOUND + 2 {
            writer.fields(3);
            writer.name("actor");
            writer.text("a");
            writer.name("key");
            writer.text(&hop.to_string());
            writer.name("hold");
            writer.unsigned(7);
        }
        writer.name("origin");
        writer.node(&ids[0]);
        writer.name("attempt");
        writer.unsigned(1);
        let Ok(Message::Routed(Routed {
            command: Command::Deliver(deliver),
            ..
        })) = decode(&writer.finish())
        else {
            panic!("a chain longer than the bound is still a message");
        };
        assert_eq!(deliver.chain.links().len(), BOUND);
        assert_eq!(deliver.chain.links()[0].key, "2");
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
            Outcome::TooLarge("the answer is too large".to_owned()),
            Outcome::Cycle("a/k -> b/k -> a/k".to_owned()),
        ];
        for outcome in outcomes {
            let answer = Answer { id: 7, outcome };
            assert_eq!(decode_answer(&encode_answer(&answer)), Ok(answer));
        }
    }

    #[test]
    fn a_size_is_the_length_of_what_is_written() {
        let ids = Rolls::seeded(65).nodes(2);
        let origins = [
            ids[0].clone(),
            NodeId {
                address: None,
                incarnation: [3; 16],
            },
        ];
        let reply = Target::Reply {
            node: ids[1].clone(),
            id: 42,
        };
        // Every length a msgpack `bin` writes differently, on both sides of each boundary.
        for len in [0, 1, 0xff, 0x100, 0xffff, 0x1_0000, 0x10_0000] {
            let payload = vec![7; len];
            let commands = [
                Command::Deliver(Deliver {
                    actor: "tests.app:account".to_owned(),
                    key: "acc-1".to_owned(),
                    message: payload.clone(),
                    reply: Some(reply.clone()),
                    chain: Chain::default(),
                }),
                Command::Deliver(Deliver {
                    actor: "tests.app:account".to_owned(),
                    key: "acc-1".to_owned(),
                    message: payload.clone(),
                    reply: Some(reply.clone()),
                    chain: chained(u64::try_from(BOUND).unwrap()),
                }),
                Command::Deliver(Deliver {
                    actor: "tests.app:account".to_owned(),
                    key: "acc-1".to_owned(),
                    message: payload.clone(),
                    reply: None,
                    chain: Chain::default(),
                }),
                Command::Start(Start {
                    actor: "tests.app:order".to_owned(),
                    key: "o-1".to_owned(),
                    state: Some(payload.clone()),
                }),
                Command::Start(Start {
                    actor: "tests.app:order".to_owned(),
                    key: "o-1".to_owned(),
                    state: None,
                }),
            ];
            for command in commands {
                for origin in &origins {
                    let size = routed_size(&command, origin);
                    let largest = encode(&Message::WrongOwner(Routed {
                        command: command.clone(),
                        origin: origin.clone(),
                        attempt: u32::MAX,
                    }));
                    assert_eq!(size, largest.len(), "{len} bytes");
                    for attempt in [1, 2] {
                        let routed = Routed {
                            command: command.clone(),
                            origin: origin.clone(),
                            attempt,
                        };
                        assert!(encode(&Message::Routed(routed.clone())).len() <= size);
                        assert!(encode(&Message::WrongOwner(routed)).len() <= size);
                    }
                }
            }
            let outcomes = [
                Outcome::Value(payload),
                Outcome::Failed {
                    actor: "a".to_owned(),
                    key: "k".to_owned(),
                    error: "RuntimeError".to_owned(),
                    message: "x".repeat(len),
                },
                Outcome::unreached("a", "k"),
            ];
            for outcome in outcomes {
                let answer = Answer { id: 42, outcome };
                assert_eq!(
                    answer_size(answer.id, &answer.outcome),
                    encode_answer(&answer).len(),
                    "{len} bytes"
                );
            }
        }
    }

    #[test]
    fn a_cancellation_reads_back_as_it_was_written() {
        let ids = Rolls::seeded(62).nodes(1);
        let callers = [
            ids[0].clone(),
            // A client has no address.
            NodeId {
                address: None,
                incarnation: [3; 16],
            },
        ];
        for node in callers {
            let cancel = Message::Cancel(Cancel {
                actor: "tests.app:account".to_owned(),
                key: "acc-1".to_owned(),
                request: Target::Reply { node, id: 42 },
            });
            assert_eq!(decode(&encode(&cancel)), Ok(cancel));
        }
    }

    #[test]
    fn a_message_of_a_kind_this_node_does_not_know_is_refused() {
        let mut unknown = Writer::new();
        unknown.tagged("Abort", 1);
        unknown.name("id");
        unknown.int(7);
        assert_eq!(decode(&unknown.finish()), Err(Malformed::Marker(0)));

        let mut unaddressed = Writer::new();
        unaddressed.tagged("Cancel", 2);
        unaddressed.name("actor");
        unaddressed.text("a");
        unaddressed.name("key");
        unaddressed.text("k");
        assert_eq!(decode(&unaddressed.finish()), Err(Malformed::Truncated));
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
