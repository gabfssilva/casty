//! The answers of `ask`, by id.
//!
//! A request keeps an id only while it waits: a timeout or a cancellation drops whatever arrives later, and so does
//! a second answer to the same id. Either of them also tells the key the request went to that nobody waits for it.

use std::collections::HashMap;

use casty_core::mailbox::{Command, Deliver, OnFull};
use casty_core::outcome::Outcome;
use pyo3::prelude::*;

use crate::lock::Locked;
use crate::schema::Schema;

#[derive(Debug)]
pub struct Waiting {
    pub future: Py<PyAny>,
    /// What the answer is read with. A native body asks for the effect and not for the value, and has none.
    pub schema: Option<Py<Schema>>,
    /// The timer that ends the request at its `timeout`, cancelled when the answer arrives first.
    pub deadline: Option<Py<PyAny>>,
    /// The actor and key the request was sent to, which is where its cancellation goes.
    pub to: Option<(String, String)>,
    /// What was sent, kept for a type whose full mailbox holds its callers: the owner calls the request back with
    /// `Full` once there is room, and this goes again.
    pub again: Option<Deliver>,
}

/// Whether a request to `actor` keeps what it sends, to send it again when the owner calls it back.
///
/// The caller reads it from its own copy of the type, as it does the deadline: every member runs the same code.
pub fn waits(py: Python<'_>, node: &super::Node, actor: &str) -> bool {
    node.resolve(py, actor)
        .is_some_and(|definition| definition.settings.on_full == OnFull::Wait)
}

/// How long a request to `target` waits for its answer, in seconds.
///
/// It is the `ask_timeout` of the type the target names, or the system's when the type sets none. The timer is the
/// caller's, and the type is read from the ref, not from the owner: every member runs the same code, so the
/// definition found here is the one the owner runs, and nothing has to be routed before the deadline is known.
pub fn timeout(py: Python<'_>, node: &super::Node, target: &casty_core::node::Target) -> f64 {
    let within = match target {
        casty_core::node::Target::Entity { actor, .. } => node
            .resolve(py, actor)
            .map_or(node.settings.ask_timeout, |definition| {
                definition.settings.over(&node.settings).ask_timeout
            }),
        casty_core::node::Target::Reply { .. } => node.settings.ask_timeout,
    };
    within.as_secs_f64()
}

#[derive(Debug, Default)]
pub struct Replies {
    next: i64,
    waiting: HashMap<i64, Waiting>,
}

impl Replies {
    /// Take the next id, which is what the answer comes back addressed to.
    pub fn take(&mut self) -> i64 {
        let id = self.next;
        self.next += 1;
        id
    }

    pub fn wait(&mut self, id: i64, waiting: Waiting) {
        self.waiting.insert(id, waiting);
    }

    pub fn forget(&mut self, id: i64) -> Option<Waiting> {
        self.waiting.remove(&id)
    }

    /// How many requests wait for their answer.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.waiting.len()
    }

    /// Every request still waiting, taken out: once the system has stopped, none of them will be answered.
    pub fn abandon(&mut self) -> Vec<Waiting> {
        self.waiting.drain().map(|(_, waiting)| waiting).collect()
    }

    /// Note the key the request `id` was sent to, if it still waits, and keep what was sent when it may go `again`.
    pub fn sent(&mut self, id: i64, deliver: &Deliver, again: bool) {
        if let Some(waiting) = self.waiting.get_mut(&id) {
            waiting.to = Some((deliver.actor.clone(), deliver.key.clone()));
            waiting.again = again.then(|| deliver.clone());
        }
    }
}

/// End a request nobody waits for any more: its timer goes, and the key it was sent to hears that the answer is not
/// wanted, so that the body working on it can stop.
pub fn cancel(
    py: Python<'_>,
    id: i64,
    waiting: &Waiting,
    node: &std::sync::Arc<super::Node>,
) -> PyResult<()> {
    if let Some(deadline) = &waiting.deadline {
        deadline.bind(py).call_method0("cancel")?;
    }
    match &waiting.to {
        Some((actor, key)) => node.cancel(py, actor, key, id),
        None => Ok(()),
    }
}

/// Fail a request its system stopped under with `Unavailable`: the key may have processed it, but no answer reaches a
/// system that has stopped.
pub fn forsake(py: Python<'_>, waiting: &Waiting) -> PyResult<()> {
    let future = waiting.future.bind(py);
    if future.call_method0("done")?.is_truthy()? {
        return Ok(());
    }
    if let Some(deadline) = &waiting.deadline {
        deadline.bind(py).call_method0("cancel")?;
    }
    let why = match &waiting.to {
        Some((actor, key)) => format!("the system stopped before {actor}/{key} answered"),
        None => "the system stopped before the answer arrived".to_owned(),
    };
    future.call_method1("set_exception", (crate::errors::Unavailable::new_err(why),))?;
    Ok(())
}

/// Turn `outcome` into what the one waiting sees: the decoded value, or the error that says why there is none.
///
/// A `Full` for a request that kept what it sent is its owner calling it back once there is room: it is sent again,
/// and goes on waiting under the deadline it already had.
pub fn settle(
    py: Python<'_>,
    id: i64,
    waiting: Waiting,
    outcome: &Outcome,
    node: &std::sync::Arc<super::Node>,
) -> PyResult<()> {
    if waiting.future.bind(py).call_method0("done")?.is_truthy()? {
        return Ok(());
    }
    if matches!(outcome, Outcome::Full { .. })
        && let Some(again) = waiting.again.clone()
    {
        node.replies.locked().wait(id, waiting);
        return node.hand(py, Command::Deliver(again));
    }
    let future = waiting.future.bind(py);
    if let Some(deadline) = &waiting.deadline {
        deadline.bind(py).call_method0("cancel")?;
    }
    match answered(py, outcome) {
        Ok(data) => {
            // A native body reads the answer itself, as the bytes it travelled as.
            let Some(schema) = &waiting.schema else {
                future.call_method1("set_result", (pyo3::types::PyBytes::new(py, data),))?;
                return Ok(());
            };
            let schema = schema.bind(py);
            let sent = schema.get().tree().sent();
            match Schema::read(schema, sent, data, Some(node)) {
                Ok(value) => future.call_method1("set_result", (value,))?,
                Err(failure) => future.call_method1("set_exception", (PyErr::from(failure),))?,
            };
        }
        Err(raised) => {
            future.call_method1("set_exception", (raised,))?;
        }
    }
    Ok(())
}

/// The bytes of the answer an outcome carries, or the exception it becomes when it carries none.
fn answered<'a>(py: Python<'_>, outcome: &'a Outcome) -> Result<&'a [u8], PyErr> {
    Err(match outcome {
        Outcome::Value(data) => return Ok(data),
        Outcome::Failed {
            actor,
            key,
            error,
            message,
        } => crate::errors::failed(py, actor, key, error, message),
        Outcome::Missing { actor, key } => {
            crate::errors::NotStarted::new_err(format!("{actor}/{key} was not started"))
        }
        Outcome::Full { actor, key } => {
            crate::errors::MailboxFull::new_err(format!("the mailbox of {actor}/{key} is full"))
        }
        Outcome::Unreached { actor, key } => crate::errors::Unavailable::new_err(format!(
            "{actor}/{key} did not process the message"
        )),
        Outcome::Unknown { actor, key } => crate::errors::UnknownActor::new_err(format!(
            "the owner of {actor}/{key} does not have the type {actor}"
        )),
        Outcome::TooLarge(why) => crate::errors::MessageTooLarge::new_err(why.clone()),
        Outcome::Cycle(cycle) => crate::errors::ReentrancyError::new_err(format!(
            "ask cycle {cycle}: the first key waits for this answer and would never read it"
        )),
    })
}
