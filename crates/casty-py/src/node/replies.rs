//! The answers of `ask`, by id.
//!
//! A request keeps an id only while it waits: a timeout or a cancellation drops whatever arrives later, and so does
//! a second answer to the same id.

use std::collections::HashMap;

use casty_core::outcome::Outcome;
use pyo3::prelude::*;

use crate::schema::Schema;

#[derive(Debug)]
pub struct Waiting {
    pub future: Py<PyAny>,
    /// What the answer is read with. A native body asks for the effect and not for the value, and has none.
    pub schema: Option<Py<Schema>>,
    /// The timer that ends the request at `ask_timeout`, cancelled when the answer arrives first.
    pub deadline: Option<Py<PyAny>>,
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
}

/// Turn `outcome` into what the one waiting sees: the decoded value, or the error that says why there is none.
pub fn settle(
    py: Python<'_>,
    waiting: &Waiting,
    outcome: &Outcome,
    node: &std::sync::Arc<super::Node>,
) -> PyResult<()> {
    let future = waiting.future.bind(py);
    if future.call_method0("done")?.is_truthy()? {
        return Ok(());
    }
    if let Some(deadline) = &waiting.deadline {
        deadline.bind(py).call_method0("cancel")?;
    }
    match outcome {
        Outcome::Value(data) => {
            let Some(schema) = &waiting.schema else {
                future.call_method1("set_result", (py.None(),))?;
                return Ok(());
            };
            let schema = schema.bind(py);
            let sent = schema.get().tree().sent();
            match Schema::read(schema, sent, data, Some(node)) {
                Ok(value) => future.call_method1("set_result", (value,))?,
                Err(failure) => future.call_method1("set_exception", (PyErr::from(failure),))?,
            };
        }
        other => {
            future.call_method1("set_exception", (raised(py, other),))?;
        }
    }
    Ok(())
}

/// The exception an outcome that is not a value becomes.
#[must_use]
pub fn raised(py: Python<'_>, outcome: &Outcome) -> PyErr {
    match outcome {
        Outcome::Value(_) => unreachable!("a value is not an error"),
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
    }
}
