//! The store of a system: how a call reaches the Python object that keeps the state of the durable types, and how a
//! system running alone keeps its durable keys there.
//!
//! A store is async and lives on the event loop, where the clients of the database it talks to live. A call is made on
//! the loop and never waited for: its coroutine runs as a task bounded by the write timeout of the operation, and what
//! it answered is handed back by a callback, to the node of a cluster through its channel, or to the key of a system
//! running alone.
//!
//! Alone, the process is the only writer of its keys. The store is read the first time a key is activated in the
//! process, and what the process holds after that is newer. A deleted key keeps its tombstone here, in place of the
//! state the store may still keep, until the store has forgotten the deletion, `leave_timeout` after it.

use core::time::Duration;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use casty_core::replication::messages::Stamp;
use casty_core::store::{Durable, Pages, Storage, Stored, version};
use casty_node::events::{Event, Operation};
use casty_node::node::Node as Cluster;
use casty_node::replication::service::{Failure, StoreAnswer, Storing};
use pyo3::exceptions::{PyTimeoutError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use super::callback;
use super::cluster::Taken;
use super::observe::Observed;
use super::{Node, Op};

/// What a durable type asks of the store of a system running alone: when it keeps the writes, and how long a call to
/// it may take, in seconds.
#[derive(Debug, Clone, Copy)]
pub struct Durability {
    durable: Durable,
    within: f64,
}

/// What is done with the answer of the store, on the loop.
type Then = Box<dyn FnOnce(Python<'_>, StoreAnswer) -> PyResult<()> + Send>;

/// Why a system without a store takes no durable type.
pub const NO_STORE: &str =
    "this system has no store, which a durable type needs: pass store= to its ActorSystem";

/// The store a system was built with, which has the three methods of `casty.Store`, or nothing.
pub fn storing(store: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Py<PyAny>>> {
    let Some(store) = store.filter(|store| !store.is_none()) else {
        return Ok(None);
    };
    for method in ["load", "save", "drop"] {
        if !store.getattr(method).is_ok_and(|found| found.is_callable()) {
            return Err(PyTypeError::new_err(format!(
                "store is {store:?}, which has no {method} method to call"
            )));
        }
    }
    Ok(Some(store.clone().unbind()))
}

/// Carry `request` of the node of a cluster out on `store`, on the loop: the node hears what the store answered
/// through its channel.
pub fn hand(
    py: Python<'_>,
    store: &Bound<'_, PyAny>,
    node: &Cluster,
    request: &Storing,
) -> PyResult<()> {
    let (id, cluster) = (request.id, node.clone());
    perform(
        py,
        store,
        &request.actor,
        &request.key,
        &request.storage,
        request.within.as_secs_f64(),
        Box::new(move |_, kept| {
            cluster.from_store(id, kept);
            Ok(())
        }),
    )
}

/// Call `store` for `storage` of `(actor, key)`, on the loop this runs on, and hand what it answers to `then`.
///
/// Nothing waits here. A store that raises, that answers `load` with something that is not a record, or that takes
/// longer than `within` seconds, answers a failure.
fn perform(
    py: Python<'_>,
    store: &Bound<'_, PyAny>,
    actor: &str,
    key: &str,
    storage: &Storage,
    within: f64,
    then: Then,
) -> PyResult<()> {
    let called = match storage {
        Storage::Load => store.call_method1("load", (actor, key)),
        Storage::Save(stored) => {
            let state = stored.state().map(|state| PyBytes::new(py, &state));
            store.call_method1(
                "save",
                (actor, key, PyBytes::new(py, &stored.version()), state),
            )
        }
        Storage::Drop(stamp) => {
            store.call_method1("drop", (actor, key, PyBytes::new(py, &version(stamp))))
        }
    };
    let asyncio = py.import("asyncio")?;
    let task = called
        .and_then(|coroutine| asyncio.call_method1("wait_for", (coroutine, within)))
        .and_then(|bounded| asyncio.call_method1("ensure_future", (bounded,)));
    let task = match task {
        Ok(task) => task,
        Err(failed) => return then(py, Err(described(py, &failed, within))),
    };
    let loading = matches!(storage, Storage::Load);
    callback::when_done(&task, move |py, task| {
        let kept = match task.call_method0("result") {
            Ok(answered) if loading => record(&answered),
            Ok(_) => Ok(None),
            Err(failed) => Err(described(py, &failed, within)),
        };
        then(py, kept)
    })
}

/// The record `load` answered: `None`, or the version and the state, which is `None` for a deletion.
fn record(answered: &Bound<'_, PyAny>) -> StoreAnswer {
    if answered.is_none() {
        return Ok(None);
    }
    let shape = || {
        format!(
            "load answered {answered}, which is neither None nor a (version: bytes, state: bytes | None) pair"
        )
    };
    let pair = answered.cast::<PyTuple>().map_err(|_| shape())?;
    if pair.len() != 2 {
        return Err(shape());
    }
    let written = pair.get_item(0).map_err(|_| shape())?;
    let written = written
        .cast::<PyBytes>()
        .map_err(|_| shape())?
        .as_bytes()
        .to_vec();
    let state = pair.get_item(1).map_err(|_| shape())?;
    let state = if state.is_none() {
        None
    } else {
        Some(
            state
                .cast::<PyBytes>()
                .map_err(|_| shape())?
                .as_bytes()
                .to_vec(),
        )
    };
    Stored::read(&written, state.as_deref())
        .map(Some)
        .map_err(|malformed| {
            format!("load answered a record this version of casty cannot read: {malformed}")
        })
}

/// What a call to the store that failed says of why.
fn described(py: Python<'_>, failed: &PyErr, within: f64) -> String {
    if failed.is_instance_of::<PyTimeoutError>(py) {
        return format!("it did not answer within {within} s");
    }
    let name = failed
        .get_type(py)
        .getattr("__name__")
        .and_then(|name| name.extract::<String>())
        .unwrap_or_else(|_| "an exception".to_owned());
    let message = failed
        .value(py)
        .str()
        .and_then(|message| message.extract::<String>())
        .unwrap_or_default();
    format!("{name}: {message}")
}

/// The exception a call to the store that failed raises in whatever waits for it.
fn unavailable(py: Python<'_>, why: String) -> Bound<'_, PyAny> {
    crate::errors::Unavailable::new_err(why)
        .into_value(py)
        .into_bound(py)
        .into_any()
}

impl Node {
    /// The durability of the type `actor` names, as this process defines it: nothing for a type the store does not
    /// keep.
    #[must_use]
    pub fn durability(&self, py: Python<'_>, actor: &str) -> Option<Durability> {
        let settings = self.resolve(py, actor)?.settings;
        Some(Durability {
            durable: settings.durable?,
            within: settings.over(&self.settings).write_timeout.as_secs_f64(),
        })
    }

    /// The store this system was built with, which the bridge of a cluster calls.
    #[must_use]
    pub fn storage(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.storage.as_ref().map(|store| store.clone_ref(py))
    }

    /// Call the store of this system, which answers `then` with the failure that says there is none when it has none.
    fn stow(
        &self,
        py: Python<'_>,
        actor: &str,
        key: &str,
        storage: &Storage,
        within: f64,
        then: Then,
    ) -> PyResult<()> {
        match &self.storage {
            Some(store) => perform(py, store.bind(py), actor, key, storage, within, then),
            None => then(py, Err(NO_STORE.to_owned())),
        }
    }

    /// Report to the observer what the store did not do.
    fn reported(&self, py: Python<'_>, actor: &str, key: &str, operation: Operation, why: String) {
        self.observe(py, || {
            Observed::Cluster(Event::WriteFailed {
                actor: actor.to_owned(),
                key: key.to_owned(),
                operation,
                failure: Failure::Unavailable(why),
            })
        });
    }

    /// Carry `op` out on a durable key alone. A write goes to the store as its type says, and a deletion keeps its
    /// tombstone here until the store has forgotten it.
    pub fn persist_stored(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        op: Op,
        durability: Durability,
        answer: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let (stored, now) = match op {
            Op::Activate { initial } => {
                return self.take_stored(py, actor, key, initial, durability, answer);
            }
            Op::Commit { pages, active, .. } => {
                let stamp = self.store().commit(actor, key, pages.clone(), active);
                self.saved.fetch_add(1, Ordering::Relaxed);
                let stored = Stored {
                    stamp,
                    pages: Some(pages),
                };
                (stored, !active)
            }
            Op::Delete { .. } => {
                let stamp = self.store().tombstone(actor, key);
                self.saved.fetch_add(1, Ordering::Relaxed);
                self.forgetting(py, actor, key, stamp.clone(), durability.within)?;
                (Stored { stamp, pages: None }, true)
            }
        };
        self.keep(py, actor, key, stored, durability, now, answer)
    }

    /// Take a durable key over alone: from this process when it has the key, from the store when it does not.
    fn take_stored(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        initial: Option<Pages>,
        durability: Durability,
        taken: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if self.store().knows(actor, key) {
            let held = self.store().activate(actor, key, initial);
            taken.call_method1("set_result", (Bound::new(py, Taken::of(held))?,))?;
            return Ok(());
        }
        let node = Arc::clone(self);
        let (owner, name) = (actor.to_owned(), key.to_owned());
        let answer = taken.clone().unbind();
        let then: Then = Box::new(move |py, kept| {
            let answer = answer.bind(py);
            if answer.call_method0("done")?.is_truthy()? {
                return Ok(());
            }
            match kept {
                Ok(stored) => {
                    let held = node.store().restore(&owner, &name, stored, initial);
                    answer.call_method1("set_result", (Bound::new(py, Taken::of(held))?,))?;
                }
                Err(why) => {
                    let why = format!("{owner}/{name}: the store could not be read: {why}");
                    node.reported(py, &owner, &name, Operation::Activate, why.clone());
                    answer.call_method1("set_exception", (unavailable(py, why),))?;
                }
            }
            Ok(())
        });
        self.stow(py, actor, key, &Storage::Load, durability.within, then)
    }

    /// Hand a write to the store as its type says. A type that saves every write has `written` resolved once the
    /// store kept it. One that saves on a schedule has it resolved at once, and the write saved at once too when it is
    /// the last of the activation or a deletion (`now`), or once its period is over otherwise.
    #[allow(clippy::too_many_arguments)]
    fn keep(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        stored: Stored,
        durability: Durability,
        now: bool,
        written: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let Durable::Every(period) = durability.durable else {
            let node = Arc::clone(self);
            let (owner, name) = (actor.to_owned(), key.to_owned());
            let answer = written.clone().unbind();
            let then: Then = Box::new(move |py, kept| {
                let answer = answer.bind(py);
                if answer.call_method0("done")?.is_truthy()? {
                    return Ok(());
                }
                match kept {
                    Ok(_) => answer.call_method1("set_result", (py.None(),)).map(|_| ()),
                    Err(why) => {
                        let why =
                            format!("{owner}/{name}: the store did not keep the write: {why}");
                        node.reported(py, &owner, &name, Operation::Write, why.clone());
                        answer
                            .call_method1("set_exception", (unavailable(py, why),))
                            .map(|_| ())
                    }
                }
            });
            return self.stow(
                py,
                actor,
                key,
                &Storage::Save(stored),
                durability.within,
                then,
            );
        };
        if now {
            // What was planned is older than this write, which goes at once.
            let _ = self.store().due(actor, key);
            self.flush(py, actor, key, stored, period, durability.within)?;
        } else if self.store().unsaved(actor, key) {
            self.plan(py, actor, key, period, durability.within)?;
        }
        written.call_method1("set_result", (py.None(),))?;
        Ok(())
    }

    /// Save `stored` now. A save that fails is reported and planned again one period later, with the last write then.
    fn flush(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        stored: Stored,
        period: Duration,
        within: f64,
    ) -> PyResult<()> {
        let node = Arc::clone(self);
        let (owner, name) = (actor.to_owned(), key.to_owned());
        let then: Then = Box::new(move |py, kept| {
            let Err(why) = kept else {
                return Ok(());
            };
            let why = format!("{owner}/{name}: the store did not keep a write: {why}");
            node.reported(py, &owner, &name, Operation::Write, why);
            if node.store().unsaved(&owner, &name) {
                node.plan(py, &owner, &name, period, within)?;
            }
            Ok(())
        });
        self.stow(py, actor, key, &Storage::Save(stored), within, then)
    }

    /// Save the last write of the key once `period` is over.
    fn plan(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        period: Duration,
        within: f64,
    ) -> PyResult<()> {
        let node = Arc::clone(self);
        let (actor, key) = (actor.to_owned(), key.to_owned());
        self.later(py, period.as_secs_f64(), move |py| {
            let Some(stored) = node.store().due(&actor, &key) else {
                return Ok(());
            };
            node.flush(py, &actor, &key, stored, period, within)
        })?;
        Ok(())
    }

    /// Ask the store to forget the deletion `stamp` `leave_timeout` from now: the time a save older than it may still
    /// be on its way to the store, which the drop has to come after.
    ///
    /// The tombstone kept here goes once the store has forgotten the deletion, and the drop is asked again
    /// `leave_timeout` later when it has not.
    fn forgetting(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        stamp: Stamp,
        within: f64,
    ) -> PyResult<()> {
        if self.storage.is_none() {
            return Ok(());
        }
        let node = Arc::clone(self);
        let (actor, key) = (actor.to_owned(), key.to_owned());
        self.later(py, self.settings.leave_timeout.as_secs_f64(), move |py| {
            let dropped = Storage::Drop(stamp.clone());
            let held = Arc::clone(&node);
            let (owner, name) = (actor.clone(), key.clone());
            let then: Then = Box::new(move |py, kept| {
                match kept {
                    Ok(_) => held.store().purge(&owner, &name, &stamp),
                    Err(why) => {
                        let why =
                            format!("{owner}/{name}: the store did not forget the deletion: {why}");
                        held.reported(py, &owner, &name, Operation::Write, why);
                        held.forgetting(py, &owner, &name, stamp, within)?;
                    }
                }
                Ok(())
            });
            node.stow(py, &actor, &key, &dropped, within, then)
        })?;
        Ok(())
    }
}
