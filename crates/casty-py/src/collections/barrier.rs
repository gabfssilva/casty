//! A barrier: parties that arrive and are all released together, or time out where they wait.

use casty_core::node::Target;
use casty_core::schema::msgpack::Malformed;
use casty_core::store::Pages;
use casty_core::wire::{Reading, Result, Writer};

use super::{Given, Native, Turn, count, fields, number, number_in, truth, uuid};

const GENERATION: &str = "generation";
const PENDING: &str = "pending";
const COMPLETED: &str = "completed";

/// How long a generation that was released is remembered, so that a party asking again hears it went through.
const REMEMBERED: f64 = 30.0;

#[derive(Debug)]
pub struct Barrier;

impl Native for Barrier {
    fn initial(&self) -> Pages {
        saved(&State::default())
    }

    fn timed(&self) -> bool {
        true
    }

    fn turn(&self, held: &Pages, given: &Given<'_>, at: f64) -> Option<Turn> {
        let before = state(held);
        let mut turn = Turn::default();
        // Whoever was waiting past its deadline hears that the barrier did not release it.
        let mut state = State {
            generation: before.generation,
            pending: Vec::new(),
            completed: remembered(&before, at),
        };
        for party in &before.pending {
            if party.until <= at {
                turn.replies.push((party.reply.clone(), truth(false)));
            } else {
                state.pending.push(party.clone());
            }
        }
        let mut waiting = None;
        if let Some(message) = given.message() {
            let (mut id, mut parties, mut until) = (None, None, None);
            let (tag, reply) = fields(message, |name, reading| {
                match name {
                    "id" => id = Some(uuid(reading)?),
                    "parties" => {
                        parties = Some(
                            usize::try_from(reading.int()?).map_err(|_| Malformed::Truncated)?,
                        );
                    }
                    "until" => until = Some(reading.float()?),
                    _ => reading.skip()?,
                }
                Ok(())
            })?;
            match tag {
                "Arrive" => {
                    let (id, parties, until) = (id?, parties?, until?);
                    if state.completed.iter().any(|done| done.id == id) {
                        turn.replies.push((reply, truth(true)));
                    } else if until <= at {
                        turn.replies.push((reply, truth(false)));
                    } else {
                        let party = Party { id, reply, until };
                        match state.pending.iter().position(|held| held.id == id) {
                            Some(at) => state.pending[at] = party,
                            None => state.pending.push(party),
                        }
                        if state.pending.len() == parties {
                            for party in &state.pending {
                                turn.replies.push((party.reply.clone(), truth(true)));
                            }
                            state = State {
                                generation: state.generation + 1,
                                completed: state
                                    .completed
                                    .iter()
                                    .cloned()
                                    .chain(state.pending.iter().map(|party| Completed {
                                        id: party.id,
                                        until: party.until.max(at + REMEMBERED),
                                    }))
                                    .collect(),
                                pending: Vec::new(),
                            };
                        }
                    }
                }
                "Cancel" => {
                    let id = id?;
                    let done = state.completed.iter().any(|held| held.id == id);
                    turn.replies.push((reply, truth(done)));
                    state.pending.retain(|party| party.id != id);
                }
                "Waiting" => waiting = Some(reply),
                _ => return None,
            }
        }
        if state != before {
            turn.save = Some(saved(&state));
        }
        turn.alarm = deadline(&state);
        if let Some(waiting) = waiting {
            turn.replies
                .push((waiting, number(count(state.pending.len()))));
        }
        Some(turn)
    }
}

/// The parties that are still waiting, and the generations still remembered.
#[derive(Debug, Default, Clone, PartialEq)]
struct State {
    generation: i64,
    pending: Vec<Party>,
    completed: Vec<Completed>,
}

#[derive(Debug, Clone, PartialEq)]
struct Party {
    id: [u8; 16],
    reply: Target,
    until: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct Completed {
    id: [u8; 16],
    until: f64,
}

/// The generations that are still remembered at `at`.
fn remembered(state: &State, at: f64) -> Vec<Completed> {
    state
        .completed
        .iter()
        .filter(|done| done.until > at)
        .cloned()
        .collect()
}

/// The earliest moment anything here stops waiting.
fn deadline(state: &State) -> Option<f64> {
    state
        .pending
        .iter()
        .map(|party| party.until)
        .chain(state.completed.iter().map(|done| done.until))
        .fold(None, |held: Option<f64>, until| {
            Some(held.map_or(until, |held| held.min(until)))
        })
}

fn state(held: &Pages) -> State {
    State {
        generation: number_in(held, GENERATION, 0),
        pending: held
            .get(PENDING)
            .and_then(|page| read_pending(page).ok())
            .unwrap_or_default(),
        completed: held
            .get(COMPLETED)
            .and_then(|page| read_completed(page).ok())
            .unwrap_or_default(),
    }
}

fn read_pending(page: &[u8]) -> Result<Vec<Party>> {
    let mut reading = Reading::new(page);
    let count = reading.items()?;
    let mut parties = Vec::with_capacity(count);
    for _ in 0..count {
        let fields = reading.fields()?;
        let mut id = None;
        let mut reply = None;
        let mut until = 0.0;
        for _ in 0..fields {
            match reading.name()? {
                "id" => id = Some(uuid(&mut reading)?),
                "reply_to" => reply = Some(reading.target()?),
                "until" => until = reading.float()?,
                _ => reading.skip()?,
            }
        }
        parties.push(Party {
            id: id.ok_or(Malformed::Truncated)?,
            reply: reply.ok_or(Malformed::Truncated)?,
            until,
        });
    }
    Ok(parties)
}

fn read_completed(page: &[u8]) -> Result<Vec<Completed>> {
    let mut reading = Reading::new(page);
    let count = reading.items()?;
    let mut done = Vec::with_capacity(count);
    for _ in 0..count {
        let fields = reading.fields()?;
        let mut id = None;
        let mut until = 0.0;
        for _ in 0..fields {
            match reading.name()? {
                "id" => id = Some(uuid(&mut reading)?),
                "until" => until = reading.float()?,
                _ => reading.skip()?,
            }
        }
        done.push(Completed {
            id: id.ok_or(Malformed::Truncated)?,
            until,
        });
    }
    Ok(done)
}

fn saved(state: &State) -> Pages {
    let mut pending = Writer::new();
    pending.items(state.pending.len());
    for party in &state.pending {
        pending.fields(3);
        pending.name("id");
        pending.bytes(&party.id);
        pending.name("reply_to");
        pending.target(&party.reply);
        pending.name("until");
        pending.float(party.until);
    }
    let mut completed = Writer::new();
    completed.items(state.completed.len());
    for done in &state.completed {
        completed.fields(2);
        completed.name("id");
        completed.bytes(&done.id);
        completed.name("until");
        completed.float(done.until);
    }
    Pages::from([
        (GENERATION.to_owned(), number(state.generation)),
        (PENDING.to_owned(), pending.finish()),
        (COMPLETED.to_owned(), completed.finish()),
    ])
}
