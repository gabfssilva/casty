//! A semaphore: leases over a capacity, granted in order, renewed while they are held, and expired when they are not.

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Given, Native, Turn, named, nil, number, number_in, truth, uuid};

const CAPACITY: &str = "capacity";
const NEXT_TOKEN: &str = "next_token";
const HELD: &str = "held";
const PENDING: &str = "pending";

#[derive(Debug)]
pub struct Semaphore;

impl Native for Semaphore {
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

    #[allow(clippy::too_many_lines)]
    fn step(&self, pages: &Pages, given: &Given<'_>, at: f64) -> Turn {
        let before = state(pages);
        let mut turn = Turn::default();
        let mut state = expired(&before, at, &mut turn.replies);
        let message = match given {
            Given::Message(message) | Given::Answered { message, .. } => Some(*message),
            Given::Alarm => None,
        };
        // What answers after the leases have been handed out, which is every message but a request.
        let mut after: Option<(Target, Answer)> = None;
        if let Some(message) = message {
            let Ok(read) = self::read(message) else {
                return Turn::default();
            };
            match named(&read.tag) {
                "Request" => {
                    let (Some(id), Some(count), Some(ttl), Some(capacity)) =
                        (read.id, read.count, read.ttl, read.capacity)
                    else {
                        return Turn::default();
                    };
                    state.capacity = capacity;
                    if let Some(held) = state.held.iter().find(|held| held.id == id) {
                        turn.replies.push((read.reply, token(Some(held.token))));
                    } else if let Some(until) = read.until {
                        if until <= at {
                            turn.replies.push((read.reply, token(None)));
                        } else {
                            let waiting = Pending {
                                id,
                                reply: read.reply,
                                count,
                                ttl,
                                until,
                            };
                            match state.pending.iter().position(|held| held.id == id) {
                                Some(at) => state.pending[at] = waiting,
                                None => state.pending.push(waiting),
                            }
                        }
                    } else if state.pending.is_empty() && available(&state) >= count {
                        let held = Lease {
                            id,
                            token: state.next_token,
                            count,
                            expires: at + ttl,
                        };
                        state.next_token += 1;
                        turn.replies.push((read.reply, token(Some(held.token))));
                        state.held.push(held);
                    } else {
                        turn.replies.push((read.reply, token(None)));
                    }
                }
                "Cancel" => {
                    let Some(id) = read.id else {
                        return Turn::default();
                    };
                    state.held.retain(|held| held.id != id);
                    state.pending.retain(|waiting| waiting.id != id);
                    after = Some((read.reply, Answer::Nothing));
                }
                "Release" => {
                    let Some(wanted) = read.token else {
                        return Turn::default();
                    };
                    let before = state.held.len();
                    state.held.retain(|held| held.token != wanted);
                    after = Some((read.reply, Answer::Truth(state.held.len() != before)));
                }
                "Renew" => {
                    let (Some(wanted), Some(ttl)) = (read.token, read.ttl) else {
                        return Turn::default();
                    };
                    let mut changed = false;
                    for held in &mut state.held {
                        if held.token == wanted {
                            held.expires = at + ttl;
                            changed = true;
                        }
                    }
                    after = Some((read.reply, Answer::Truth(changed)));
                }
                "Available" => {
                    let Some(capacity) = read.capacity else {
                        return Turn::default();
                    };
                    state.capacity = capacity;
                    after = Some((read.reply, Answer::Available));
                }
                _ => return Turn::default(),
            }
        }
        grant(&mut state, at, &mut turn.replies);
        if state != before {
            turn.save = Some(saved(&state));
        }
        turn.alarm = deadline(&state);
        if let Some((reply, answer)) = after {
            turn.replies.push((
                reply,
                match answer {
                    Answer::Nothing => nil(),
                    Answer::Truth(value) => truth(value),
                    Answer::Available => number(available(&state)),
                },
            ));
        }
        turn
    }
}

/// What a message answers once the leases of this turn have been handed out.
enum Answer {
    Nothing,
    Truth(bool),
    Available,
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
    id: [u8; 16],
    token: i64,
    count: i64,
    expires: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct Pending {
    id: [u8; 16],
    reply: Target,
    count: i64,
    ttl: f64,
    until: f64,
}

/// Drop the leases that ran out and tell whoever waited past its deadline that it was not granted.
fn expired(state: &State, at: f64, replies: &mut Vec<(Target, Vec<u8>)>) -> State {
    for waiting in &state.pending {
        if waiting.until <= at {
            replies.push((waiting.reply.clone(), token(None)));
        }
    }
    State {
        capacity: state.capacity,
        next_token: state.next_token,
        held: state
            .held
            .iter()
            .filter(|held| held.expires > at)
            .cloned()
            .collect(),
        pending: state
            .pending
            .iter()
            .filter(|waiting| waiting.until > at)
            .cloned()
            .collect(),
    }
}

/// Hand the capacity out in order, to as many of those waiting as it reaches.
fn grant(state: &mut State, at: f64, replies: &mut Vec<(Target, Vec<u8>)>) {
    while let Some(waiting) = state.pending.first() {
        if available(state) < waiting.count {
            return;
        }
        let waiting = state.pending.remove(0);
        let held = Lease {
            id: waiting.id,
            token: state.next_token,
            count: waiting.count,
            expires: at + waiting.ttl,
        };
        state.next_token += 1;
        replies.push((waiting.reply, token(Some(held.token))));
        state.held.push(held);
    }
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
        .chain(state.pending.iter().map(|waiting| waiting.until))
        .fold(None, |held: Option<f64>, at| {
            Some(held.map_or(at, |held| held.min(at)))
        })
}

/// Everything the five messages carry between them.
struct Held {
    tag: String,
    reply: Target,
    id: Option<[u8; 16]>,
    token: Option<i64>,
    count: Option<i64>,
    ttl: Option<f64>,
    capacity: Option<i64>,
    until: Option<f64>,
}

fn read(message: &[u8]) -> Result<Held> {
    let mut reading = Reading::new(message);
    let (tag, fields) = reading.tagged()?;
    let mut reply = None;
    let mut id = None;
    let mut token = None;
    let mut count = None;
    let mut ttl = None;
    let mut capacity = None;
    let mut until = None;
    for _ in 0..fields {
        match reading.name()? {
            "reply_to" => reply = Some(reading.target()?),
            "id" => id = Some(uuid(&mut reading)?),
            "token" => token = Some(reading.int()?),
            "count" => count = Some(reading.int()?),
            "ttl" => ttl = Some(reading.float()?),
            "capacity" => capacity = Some(reading.int()?),
            "until" => {
                until = if reading.nil()? {
                    None
                } else {
                    Some(reading.float()?)
                };
            }
            _ => reading.skip()?,
        }
    }
    Ok(Held {
        tag,
        reply: reply.ok_or(Malformed::Truncated)?,
        id,
        token,
        count,
        ttl,
        capacity,
        until,
    })
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
            id: [0; 16],
            token: 0,
            count: 0,
            expires: 0.0,
        };
        for _ in 0..fields {
            match reading.name()? {
                "id" => lease.id = uuid(&mut reading)?,
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
            id: [0; 16],
            reply: Target::Entity {
                actor: String::new(),
                key: String::new(),
            },
            count: 0,
            ttl: 0.0,
            until: 0.0,
        };
        for _ in 0..fields {
            match reading.name()? {
                "id" => id = Some(uuid(&mut reading)?),
                "reply_to" => reply = Some(reading.target()?),
                "count" => held.count = reading.int()?,
                "ttl" => held.ttl = reading.float()?,
                "until" => held.until = reading.float()?,
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
        held.name("id");
        held.bytes(&lease.id);
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
        pending.name("id");
        pending.bytes(&waiting.id);
        pending.name("reply_to");
        pending.target(&waiting.reply);
        pending.name("count");
        pending.int(waiting.count);
        pending.name("ttl");
        pending.float(waiting.ttl);
        pending.name("until");
        pending.float(waiting.until);
    }
    Pages::from([
        (CAPACITY.to_owned(), number(state.capacity)),
        (NEXT_TOKEN.to_owned(), number(state.next_token)),
        (HELD.to_owned(), held.finish()),
        (PENDING.to_owned(), pending.finish()),
    ])
}

/// `int | None`, which is what a request answers with: the token, or nothing when it was not granted.
fn token(value: Option<i64>) -> Vec<u8> {
    let mut writer = Writer::new();
    match value {
        Some(value) => writer.int(value),
        None => writer.nil(),
    }
    writer.finish()
}
