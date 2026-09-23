//! A semaphore: leases over a capacity, granted in order, renewed while they are held, and expired when they are not.
//!
//! A request names the lease it asks for, or has one named for it, so that sending it again keeps its place in line
//! and, once it is granted, hears the same grant. What a request hears is a message, `Acquired` or `Denied`, so that an
//! actor can have it told to its own ref and go on reading its mailbox meanwhile. A release answers no one.

use std::hash::{BuildHasher, RandomState};

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Given, Native, Turn, count, number, number_in, told, truth};

const CAPACITY: &str = "capacity";
const NEXT_TOKEN: &str = "next_token";
const HELD: &str = "held";
const PENDING: &str = "pending";

#[derive(Debug)]
pub struct Semaphore;

impl Native for Semaphore {
    /// A semaphore of no capacity. A key of this type starts from the `initial` its ref is obtained with, which is
    /// where its capacity comes from, so no activation starts from this.
    fn initial(&self) -> Pages {
        saved(&State {
            capacity: 0,
            next_token: 1,
            held: Vec::new(),
            pending: Vec::new(),
        })
    }

    fn timed(&self) -> bool {
        true
    }

    fn turn(&self, pages: &Pages, given: &Given<'_>, at: f64) -> Option<Turn> {
        let before = state(pages);
        let mut state = before.clone();
        let mut turn = Turn::default();
        lapse(&mut state, at, &mut turn.replies);
        // What answers once the leases of this turn have been handed out.
        let mut after: Option<(Target, Answer)> = None;
        if let Some(message) = given.message() {
            let (tag, reply, read) = read(message)?;
            match tag {
                "Acquire" => {
                    let waiting = Pending {
                        id: read.lease_id.unwrap_or_else(named),
                        reply: reply?,
                        count: read.n?,
                        ttl: read.ttl?,
                        until: read.wait.map(|wait| at + wait),
                    };
                    acquire(&mut state, waiting, &mut turn.replies);
                }
                "Release" => {
                    let id = read.lease_id?;
                    state.held.retain(|held| held.id != id);
                    state.pending.retain(|waiting| waiting.id != id);
                }
                "Renew" => {
                    let (id, ttl) = (read.lease_id?, read.ttl?);
                    let renewed = match state.held.iter_mut().find(|held| held.id == id) {
                        Some(held) => {
                            held.expires = at + ttl;
                            true
                        }
                        None => false,
                    };
                    after = Some((reply?, Answer::Renewed(renewed)));
                }
                "Get" => after = Some((reply?, Answer::Status)),
                _ => return None,
            }
        }
        grant(&mut state, at, &mut turn.replies);
        // A request that would not wait, and was not granted.
        lapse(&mut state, at, &mut turn.replies);
        if state != before {
            turn.save = Some(saved(&state));
        }
        turn.alarm = deadline(&state);
        if let Some((reply, answer)) = after {
            turn.replies.push((
                reply,
                match answer {
                    Answer::Renewed(renewed) => truth(renewed),
                    Answer::Status => status(&state),
                },
            ));
        }
        Some(turn)
    }
}

/// What a message answers once the leases of its turn have been handed out.
enum Answer {
    Renewed(bool),
    Status,
}

#[derive(Debug, Clone, PartialEq)]
struct State {
    capacity: i64,
    next_token: i64,
    held: Vec<Lease>,
    pending: Vec<Pending>,
}

#[derive(Debug, Clone, PartialEq)]
struct Lease {
    id: String,
    token: i64,
    count: i64,
    expires: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct Pending {
    id: String,
    reply: Target,
    count: i64,
    ttl: f64,
    /// When it stops waiting, or never.
    until: Option<f64>,
}

/// Grant `waiting` again when its lease is held, deny it when no capacity could ever grant it, and put it in line
/// otherwise: where it was, when it waits already.
fn acquire(state: &mut State, waiting: Pending, replies: &mut Vec<(Target, Vec<u8>)>) {
    if let Some(held) = state.held.iter().find(|held| held.id == waiting.id) {
        replies.push((waiting.reply, acquired(held)));
        return;
    }
    if !(1..=state.capacity).contains(&waiting.count) {
        state.pending.retain(|held| held.id != waiting.id);
        replies.push((waiting.reply, denied(&waiting.id)));
        return;
    }
    match state.pending.iter().position(|held| held.id == waiting.id) {
        Some(place) => state.pending[place] = waiting,
        None => state.pending.push(waiting),
    }
}

/// Hand the capacity out in order, to as many of those waiting as it reaches.
fn grant(state: &mut State, at: f64, replies: &mut Vec<(Target, Vec<u8>)>) {
    while let Some(waiting) = state.pending.first() {
        if available(state) < waiting.count {
            return;
        }
        let waiting = state.pending.remove(0);
        let lease = Lease {
            id: waiting.id,
            token: state.next_token,
            count: waiting.count,
            expires: at + waiting.ttl,
        };
        state.next_token += 1;
        replies.push((waiting.reply, acquired(&lease)));
        state.held.push(lease);
    }
}

/// Take back the leases that ran out, and deny whoever waited past its deadline.
fn lapse(state: &mut State, at: f64, replies: &mut Vec<(Target, Vec<u8>)>) {
    state.held.retain(|held| held.expires > at);
    state.pending.retain(|waiting| {
        let waits = waiting.until.is_none_or(|until| until > at);
        if !waits {
            replies.push((waiting.reply.clone(), denied(&waiting.id)));
        }
        waits
    });
}

fn available(state: &State) -> i64 {
    state.capacity - state.held.iter().map(|held| held.count).sum::<i64>()
}

/// The earliest moment a lease runs out or someone stops waiting.
fn deadline(state: &State) -> Option<f64> {
    state
        .held
        .iter()
        .map(|held| held.expires)
        .chain(state.pending.iter().filter_map(|waiting| waiting.until))
        .reduce(f64::min)
}

/// The id of a lease whose request named none: random, so that it does not meet one a sender chose.
fn named() -> String {
    let random = RandomState::new();
    format!(
        "{:016x}{:016x}",
        random.hash_one(0_u8),
        random.hash_one(1_u8)
    )
}

/// Everything the four messages carry between them, besides who they answer.
#[derive(Default)]
struct Carried {
    lease_id: Option<String>,
    n: Option<i64>,
    ttl: Option<f64>,
    wait: Option<f64>,
}

fn read(message: &[u8]) -> Option<(&str, Option<Target>, Carried)> {
    let mut carried = Carried::default();
    let (tag, reply) = told(message, |name, reading| {
        match name {
            "lease_id" => {
                carried.lease_id = if reading.nil()? {
                    None
                } else {
                    Some(reading.text()?)
                };
            }
            "n" => carried.n = Some(reading.int()?),
            "ttl" => carried.ttl = Some(reading.float()?),
            "wait" => {
                carried.wait = if reading.nil()? {
                    None
                } else {
                    Some(reading.float()?)
                };
            }
            _ => reading.skip()?,
        }
        Ok(())
    })?;
    Some((tag, reply, carried))
}

/// `Acquired`, which travels under its tag: it is an alternative of the union a request is answered with.
fn acquired(lease: &Lease) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Acquired", 2);
    writer.name("lease_id");
    writer.text(&lease.id);
    writer.name("token");
    writer.int(lease.token);
    writer.finish()
}

fn denied(id: &str) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Denied", 1);
    writer.name("lease_id");
    writer.text(id);
    writer.finish()
}

fn status(state: &State) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.tagged("Status", 3);
    writer.name("capacity");
    writer.int(state.capacity);
    writer.name("available");
    writer.int(available(state));
    writer.name("waiting");
    writer.int(count(state.pending.len()));
    writer.finish()
}

fn state(pages: &Pages) -> State {
    State {
        capacity: number_in(pages, CAPACITY, 0),
        next_token: number_in(pages, NEXT_TOKEN, 1),
        held: pages
            .get(HELD)
            .and_then(|page| read_held(page).ok())
            .unwrap_or_default(),
        pending: pages
            .get(PENDING)
            .and_then(|page| read_pending(page).ok())
            .unwrap_or_default(),
    }
}

fn read_held(page: &[u8]) -> Result<Vec<Lease>> {
    let mut reading = Reading::new(page);
    let count = reading.items()?;
    let mut leases = Vec::with_capacity(count);
    for _ in 0..count {
        let fields = reading.fields()?;
        let mut lease = Lease {
            id: String::new(),
            token: 0,
            count: 0,
            expires: 0.0,
        };
        for _ in 0..fields {
            match reading.name()? {
                "lease_id" => lease.id = reading.text()?,
                "token" => lease.token = reading.int()?,
                "count" => lease.count = reading.int()?,
                "expires" => lease.expires = reading.float()?,
                _ => reading.skip()?,
            }
        }
        leases.push(lease);
    }
    Ok(leases)
}

fn read_pending(page: &[u8]) -> Result<Vec<Pending>> {
    let mut reading = Reading::new(page);
    let count = reading.items()?;
    let mut waiting = Vec::with_capacity(count);
    for _ in 0..count {
        let fields = reading.fields()?;
        let mut id = None;
        let mut reply = None;
        let mut held = Pending {
            id: String::new(),
            reply: Target::Entity {
                actor: String::new(),
                key: String::new(),
            },
            count: 0,
            ttl: 0.0,
            until: None,
        };
        for _ in 0..fields {
            match reading.name()? {
                "lease_id" => id = Some(reading.text()?),
                "reply_to" => reply = Some(reading.target()?),
                "count" => held.count = reading.int()?,
                "ttl" => held.ttl = reading.float()?,
                "until" => {
                    held.until = if reading.nil()? {
                        None
                    } else {
                        Some(reading.float()?)
                    };
                }
                _ => reading.skip()?,
            }
        }
        held.id = id.ok_or(Malformed::Truncated)?;
        held.reply = reply.ok_or(Malformed::Truncated)?;
        waiting.push(held);
    }
    Ok(waiting)
}

fn saved(state: &State) -> Pages {
    let mut held = Writer::new();
    held.items(state.held.len());
    for lease in &state.held {
        held.fields(4);
        held.name("lease_id");
        held.text(&lease.id);
        held.name("token");
        held.int(lease.token);
        held.name("count");
        held.int(lease.count);
        held.name("expires");
        held.float(lease.expires);
    }
    let mut pending = Writer::new();
    pending.items(state.pending.len());
    for waiting in &state.pending {
        pending.fields(5);
        pending.name("lease_id");
        pending.text(&waiting.id);
        pending.name("reply_to");
        pending.target(&waiting.reply);
        pending.name("count");
        pending.int(waiting.count);
        pending.name("ttl");
        pending.float(waiting.ttl);
        pending.name("until");
        match waiting.until {
            Some(until) => pending.float(until),
            None => pending.nil(),
        }
    }
    Pages::from([
        (CAPACITY.to_owned(), number(state.capacity)),
        (NEXT_TOKEN.to_owned(), number(state.next_token)),
        (HELD.to_owned(), held.finish()),
        (PENDING.to_owned(), pending.finish()),
    ])
}

#[cfg(test)]
mod tests {
    use casty_core::node::Target;
    use casty_core::store::Pages;
    use casty_core::wire::{Reading, Result, Writer};

    use super::{Semaphore, State, saved};
    use crate::collections::{Given, Native, native};

    /// What a semaphore answered, read back.
    #[derive(Debug, PartialEq)]
    enum Heard {
        Acquired(String, i64),
        Denied(String),
        Status(i64, i64, i64),
    }

    fn created(capacity: i64) -> Pages {
        saved(&State {
            capacity,
            next_token: 1,
            held: Vec::new(),
            pending: Vec::new(),
        })
    }

    fn asker(name: &str) -> Target {
        Target::Entity {
            actor: "test".to_owned(),
            key: name.to_owned(),
        }
    }

    fn acquire(by: &str, n: i64, ttl: f64, wait: Option<f64>, lease: Option<&str>) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.tagged("semaphore.Acquire", 5);
        writer.name("reply_to");
        writer.target(&asker(by));
        writer.name("n");
        writer.int(n);
        writer.name("ttl");
        writer.float(ttl);
        writer.name("wait");
        match wait {
            Some(wait) => writer.float(wait),
            None => writer.nil(),
        }
        writer.name("lease_id");
        match lease {
            Some(lease) => writer.text(lease),
            None => writer.nil(),
        }
        writer.finish()
    }

    fn release(lease: &str) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.tagged("semaphore.Release", 1);
        writer.name("lease_id");
        writer.text(lease);
        writer.finish()
    }

    fn renew(lease: &str, ttl: f64) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.tagged("semaphore.Renew", 3);
        writer.name("reply_to");
        writer.target(&asker("renewer"));
        writer.name("lease_id");
        writer.text(lease);
        writer.name("ttl");
        writer.float(ttl);
        writer.finish()
    }

    fn get() -> Vec<u8> {
        let mut writer = Writer::new();
        writer.tagged("semaphore.Get", 1);
        writer.name("reply_to");
        writer.target(&asker("getter"));
        writer.finish()
    }

    /// Run `given` at `at` on the semaphore whose state is `pages`, keep what it saved, and give back what it told
    /// whom.
    fn turned(pages: &mut Pages, given: &Given<'_>, at: f64) -> Vec<(Target, Vec<u8>)> {
        let turn = Semaphore.step(pages, given, at);
        if let Some(saved) = turn.save {
            *pages = saved;
        }
        turn.replies
    }

    /// `message` at `at`, with what it answered read back by the key of whom it went to.
    fn step(pages: &mut Pages, message: &[u8], at: f64) -> Result<Vec<(String, Heard)>> {
        heard(turned(pages, &Given::Message(message), at))
    }

    fn heard(replies: Vec<(Target, Vec<u8>)>) -> Result<Vec<(String, Heard)>> {
        replies
            .into_iter()
            .map(|(target, answer)| {
                let Target::Entity { key, .. } = target else {
                    panic!("a reply to an ask in a test that asks none");
                };
                let mut reading = Reading::new(&answer);
                let (tag, fields) = reading.tagged()?;
                let mut lease = String::new();
                let mut numbers = Vec::new();
                for _ in 0..fields {
                    if reading.name()? == "lease_id" {
                        lease = reading.text()?;
                    } else {
                        numbers.push(reading.int()?);
                    }
                }
                let answer = match tag.as_str() {
                    "Acquired" => Heard::Acquired(lease, numbers[0]),
                    "Denied" => Heard::Denied(lease),
                    _ => Heard::Status(numbers[0], numbers[1], numbers[2]),
                };
                Ok((key, answer))
            })
            .collect()
    }

    fn granted(lease: &str, token: i64, to: &str) -> (String, Heard) {
        (to.to_owned(), Heard::Acquired(lease.to_owned(), token))
    }

    fn status(pages: &mut Pages, at: f64) -> Result<Heard> {
        let mut answers = step(pages, &get(), at)?;
        Ok(answers.remove(0).1)
    }

    #[test]
    fn a_request_that_fits_is_granted_at_once_and_one_that_does_not_waits_for_a_release()
    -> Result<()> {
        let mut pages = created(2);
        assert_eq!(
            step(&mut pages, &acquire("a", 2, 30.0, None, Some("a")), 0.0)?,
            [granted("a", 1, "a")]
        );
        assert_eq!(
            step(&mut pages, &acquire("b", 1, 30.0, None, Some("b")), 0.0)?,
            []
        );
        assert_eq!(status(&mut pages, 0.0)?, Heard::Status(2, 0, 1));
        assert_eq!(
            step(&mut pages, &release("a"), 1.0)?,
            [granted("b", 2, "b")]
        );
        assert_eq!(status(&mut pages, 1.0)?, Heard::Status(2, 1, 0));
        Ok(())
    }

    #[test]
    fn waiters_are_granted_in_the_order_they_came_even_when_a_later_one_would_fit() -> Result<()> {
        let mut pages = created(2);
        step(&mut pages, &acquire("a", 1, 30.0, None, Some("a")), 0.0)?;
        step(&mut pages, &acquire("b", 2, 30.0, None, Some("b")), 0.0)?;
        assert_eq!(
            step(&mut pages, &acquire("c", 1, 30.0, None, Some("c")), 0.0)?,
            []
        );
        assert_eq!(
            step(&mut pages, &release("a"), 0.0)?,
            [granted("b", 2, "b")]
        );
        assert_eq!(
            step(&mut pages, &release("b"), 0.0)?,
            [granted("c", 3, "c")]
        );
        Ok(())
    }

    #[test]
    fn a_request_that_would_not_wait_or_could_never_fit_is_denied_at_once() -> Result<()> {
        let mut pages = created(1);
        step(&mut pages, &acquire("a", 1, 30.0, None, Some("a")), 0.0)?;
        assert_eq!(
            step(
                &mut pages,
                &acquire("b", 1, 30.0, Some(0.0), Some("b")),
                0.0
            )?,
            [("b".to_owned(), Heard::Denied("b".to_owned()))]
        );
        assert_eq!(
            step(&mut pages, &acquire("c", 2, 30.0, None, Some("c")), 0.0)?,
            [("c".to_owned(), Heard::Denied("c".to_owned()))]
        );
        assert_eq!(
            step(&mut pages, &acquire("d", 0, 30.0, None, Some("d")), 0.0)?,
            [("d".to_owned(), Heard::Denied("d".to_owned()))]
        );
        assert_eq!(status(&mut pages, 0.0)?, Heard::Status(1, 0, 0));
        Ok(())
    }

    #[test]
    fn a_wait_that_runs_out_is_denied_and_leaves_the_line() -> Result<()> {
        let mut pages = created(1);
        step(&mut pages, &acquire("a", 1, 30.0, None, Some("a")), 0.0)?;
        let turn = Semaphore.step(
            &pages,
            &Given::Message(&acquire("b", 1, 30.0, Some(5.0), Some("b"))),
            0.0,
        );
        assert_eq!(turn.alarm, Some(5.0));
        if let Some(saved) = turn.save {
            pages = saved;
        }
        assert_eq!(
            heard(turned(&mut pages, &Given::Alarm, 5.0))?,
            [("b".to_owned(), Heard::Denied("b".to_owned()))]
        );
        assert_eq!(step(&mut pages, &release("a"), 6.0)?, []);
        assert_eq!(status(&mut pages, 6.0)?, Heard::Status(1, 1, 0));
        Ok(())
    }

    #[test]
    fn a_lease_runs_out_at_its_ttl_unless_it_is_renewed() -> Result<()> {
        let mut pages = created(1);
        step(&mut pages, &acquire("a", 1, 10.0, None, Some("a")), 0.0)?;
        let renewed = turned(&mut pages, &Given::Message(&renew("a", 10.0)), 5.0);
        assert!(Reading::new(&renewed[0].1).bool()?);
        assert_eq!(status(&mut pages, 12.0)?, Heard::Status(1, 0, 0));
        assert_eq!(status(&mut pages, 15.0)?, Heard::Status(1, 1, 0));
        let late = turned(&mut pages, &Given::Message(&renew("a", 10.0)), 16.0);
        assert!(!Reading::new(&late[0].1).bool()?);
        Ok(())
    }

    #[test]
    fn a_request_sent_again_keeps_its_place_and_hears_its_grant_again() -> Result<()> {
        let mut pages = created(1);
        step(&mut pages, &acquire("x", 1, 30.0, None, Some("x")), 0.0)?;
        step(&mut pages, &acquire("b", 1, 30.0, None, Some("b")), 0.0)?;
        step(&mut pages, &acquire("c", 1, 30.0, None, Some("c")), 0.0)?;
        assert_eq!(
            step(
                &mut pages,
                &acquire("b-again", 1, 30.0, None, Some("b")),
                1.0
            )?,
            []
        );
        assert_eq!(
            step(&mut pages, &release("x"), 2.0)?,
            [granted("b", 2, "b-again")]
        );
        assert_eq!(
            step(
                &mut pages,
                &acquire("b-lost", 1, 30.0, None, Some("b")),
                3.0
            )?,
            [granted("b", 2, "b-lost")]
        );
        assert_eq!(status(&mut pages, 3.0)?, Heard::Status(1, 0, 1));
        Ok(())
    }

    #[test]
    fn a_request_that_names_no_lease_is_given_one_it_is_released_by() -> Result<()> {
        let mut pages = created(1);
        let answers = step(&mut pages, &acquire("a", 1, 30.0, None, None), 0.0)?;
        let [(_, Heard::Acquired(lease, 1))] = answers.as_slice() else {
            panic!("{answers:?}");
        };
        assert_eq!(lease.len(), 32);
        step(&mut pages, &release(lease), 0.0)?;
        assert_eq!(status(&mut pages, 0.0)?, Heard::Status(1, 1, 0));
        Ok(())
    }

    #[test]
    fn a_release_withdraws_a_request_still_waiting() -> Result<()> {
        let mut pages = created(1);
        step(&mut pages, &acquire("a", 1, 30.0, None, Some("a")), 0.0)?;
        step(&mut pages, &acquire("b", 1, 30.0, None, Some("b")), 0.0)?;
        assert_eq!(step(&mut pages, &release("b"), 0.0)?, []);
        assert_eq!(step(&mut pages, &release("a"), 0.0)?, []);
        assert_eq!(status(&mut pages, 0.0)?, Heard::Status(1, 1, 0));
        Ok(())
    }

    #[test]
    fn the_semaphore_has_a_body_under_every_configuration() {
        assert!(native("casty.collections:semaphore.actor").is_some());
        assert!(native("casty.collections:semaphore_3_majority").is_some());
    }
}
