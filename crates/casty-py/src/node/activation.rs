//! A key active on this node: its mailbox, its state and the tasks of its body.
//!
//! The body runs again from the start after it fails, after `become`, after it returns leaving messages behind, and
//! after the caller of the message it is on cancels it. Only the last saved state survives, so a restart never sees
//! the local variables of the previous run. One run of the body reads the mailbox at a time, known by the message it
//! took last.

use core::time::Duration;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use casty_core::chain::{Chain, Link};
use casty_core::mailbox::{Command, Deliver, Mailbox, Put, Withdrawn};
use casty_core::node::Target;
use casty_core::outcome::Outcome;
use casty_core::schedule::{self, SCHEDULES};
use casty_core::store::Pages;
use pyo3::prelude::*;

use crate::collections::{Given, Native, Turn};

use super::callback;
use super::cluster::{Fencing, Taken};
use super::context::{Context, Schedule, ended};
use super::observe::Observed;
use super::{Node, Op};
use crate::actor::Definition;
use crate::awaited::Awaited;
use crate::lock::Locked;
use crate::schema::Schema;

/// The reserved page with the name of the behavior that reads the state.
const BEHAVIOR: &str = "@behavior";

// The flags of the state machine, each one named after the thing it decides.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct State {
    /// The type the key runs now: the one it started as, unless it became another.
    behavior: Arc<Definition>,
    mailbox: Mailbox,
    /// The state the first `Start` to arrive offered, which the key takes if no replica has one.
    offered: Option<Vec<u8>>,
    exists: bool,
    fenced: bool,
    draining: bool,
    /// Whether the node let the key go on purpose: its body ends at its next read, as at `idle_after`.
    releasing: bool,
    /// Who waits for this activation to be over: the releases of its key.
    ends: Vec<Py<PyAny>>,
    switching: bool,
    pages: Pages,
    /// The activation the node took the key for, which every write of this one carries.
    lease: u64,
    value: Option<Py<PyAny>>,
    /// The run of the body on the loop. A native body has none.
    run: Option<Run>,
    /// The id of the last run started.
    started: u64,
    /// The writes of the body on the loop that have not landed. A run that ends meanwhile waits for them.
    writes: usize,
    finished: bool,
    /// The message a native body is on.
    current: Option<Deliver>,
    /// The stretch a native body is on, numbered anew for every message it takes.
    hold: u64,
    /// The idle timer of a native body with nothing to do, and when it goes off.
    idle: Option<Py<PyAny>>,
    deadline: Option<f64>,
    /// What a native body is still working on, and the answer it is waiting for.
    awaiting: Option<(Vec<u8>, i64)>,
    /// What a native body answers once the write of the message it is on has landed.
    pending: Vec<(Target, Vec<u8>)>,
    /// The state of a native body that is on its way to the replicas, which it takes only once it is there.
    writing: Option<Pages>,
    /// Whether that write is the deletion of the key.
    deleting: bool,
    /// Whether the last write that landed deleted the key, which leaves nothing for the deactivation to write.
    deleted: bool,
    /// The timer of a native body that asked for a deadline.
    alarm: Option<Py<PyAny>>,
    /// Whether a native body has nothing to do, so that a message arriving takes its loop up again.
    resting: bool,
    /// The schedules of the key, in the order they were made. Every write of the body carries them in `@schedules`.
    schedules: Vec<Scheduled>,
    /// The number the next schedule is known by.
    scheduled: u64,
    /// The pages of the last write sent, which a write of the schedules alone carries so as not to undo a write still
    /// in flight.
    sent: Pages,
}

impl State {
    fn ending(&self) -> bool {
        self.fenced || self.draining
    }

    fn run(&mut self, id: u64) -> Option<&mut Run> {
        self.run.as_mut().filter(|run| run.id == id)
    }

    /// Whether the run of the body has a task that has not ended.
    fn running(&self) -> bool {
        self.run.as_ref().is_some_and(|run| run.task.is_some())
    }

    /// The behavior the state names: the type the key started as, unless it became another.
    fn named(&self, entry: &str) -> String {
        match self.pages.get(BEHAVIOR) {
            None => entry.to_owned(),
            Some(page) => String::from_utf8_lossy(page).into_owned(),
        }
    }

    /// The page of the schedules of the key. A key without schedules has none.
    fn schedules_page(&self) -> Option<Vec<u8>> {
        if self.schedules.is_empty() {
            return None;
        }
        let all: Vec<schedule::Schedule> = self
            .schedules
            .iter()
            .map(|scheduled| scheduled.schedule.clone())
            .collect();
        Some(schedule::encode(&all))
    }

    /// The timers of the schedules, taken out to be cancelled.
    fn unarmed(&mut self) -> Vec<Py<PyAny>> {
        self.schedules
            .iter_mut()
            .filter_map(|scheduled| scheduled.timer.take())
            .collect()
    }
}

/// A schedule of the key, known by its number in this activation: another of its name takes its place under another
/// number, so that what was waiting on the one it replaced does not reach it.
#[derive(Debug)]
struct Scheduled {
    id: u64,
    schedule: schedule::Schedule,
    /// The message, as the body gave it or as the page reads back.
    message: Py<PyAny>,
    /// The timer that fires it, armed once the replicas have it and again after each time it goes off.
    timer: Option<Py<PyAny>>,
}

/// One run of the body on the loop: its task, and the message it took last.
///
/// A run is known by its id, which a restart replaces, so the context of an earlier run reads nothing.
#[derive(Debug, Default)]
struct Run {
    id: u64,
    task: Option<Py<PyAny>>,
    /// The message the run took last, until it reads the next one or ends.
    current: Option<Deliver>,
    /// Whether the run told the caller of `current` its answer: nobody waits on it any more, so its cancellation
    /// changes nothing and the asks of the run keep none of its callers waiting.
    answered: bool,
    /// The stretch the run is on, from one read to the next, numbered by the node. An `ask` of the run carries it,
    /// and a message that comes back down the same chain finds the run still on it unless it has read since.
    hold: u64,
    /// The task that read `current`, or the task of the run until it has read: the one the runs of the node know it by.
    reader: Option<Py<PyAny>>,
    /// Whether it has read, which tells a body that returned leaving messages behind from one that never reads.
    read: bool,
    /// Whether the caller of `current` cancelled it, which ends the run and starts it again for the next message.
    cancelled: bool,
    failures: u32,
    waiting: Option<Py<PyAny>>,
    idle: Option<Py<PyAny>>,
    /// When the read being waited on stops waiting, which a message arriving later does not undo.
    deadline: Option<f64>,
    item: Option<Py<PyAny>>,
    /// How the run ended while a write was in flight, which it acts on once the write has landed.
    settling: Option<Exit>,
}

/// How the task of a run ended.
#[derive(Debug)]
enum Exit {
    /// It returned, or the activation cancelled it.
    Returned,
    Raised(Py<PyAny>),
}

/// Cancel what a run that is over left pending: the next item of the source it merged, and the timer of its read. Its
/// reader reads for nothing any more.
fn forsaken(py: Python<'_>, node: &Node, run: Run) -> PyResult<()> {
    if let Some(reader) = run.reader {
        node.runs.leave(reader.bind(py));
    }
    for pending in [run.item, run.idle].into_iter().flatten() {
        pending.bind(py).call_method0("cancel")?;
    }
    Ok(())
}

#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Activation {
    node: Arc<Node>,
    entry: String,
    key: String,
    /// When the key became active here, by the wall clock, which a listing of the node reads.
    since: SystemTime,
    inner: Mutex<State>,
}

impl Activation {
    pub fn new(
        py: Python<'_>,
        node: &Arc<Node>,
        behavior: &Arc<Definition>,
        entry: &str,
        key: &str,
    ) -> PyResult<Py<Self>> {
        let settings = behavior.settings;
        let activation = Self {
            node: node.clone(),
            entry: entry.to_owned(),
            key: key.to_owned(),
            since: SystemTime::now(),
            inner: Mutex::new(State {
                behavior: Arc::clone(behavior),
                mailbox: Mailbox::new(settings.mailbox, settings.on_full),
                offered: None,
                exists: false,
                fenced: false,
                draining: false,
                releasing: false,
                ends: Vec::new(),
                switching: false,
                pages: Pages::new(),
                lease: 0,
                value: None,
                run: None,
                started: 0,
                writes: 0,
                finished: false,
                current: None,
                hold: 0,
                idle: None,
                deadline: None,
                awaiting: None,
                pending: Vec::new(),
                writing: None,
                deleting: false,
                deleted: false,
                alarm: None,
                resting: false,
                schedules: Vec::new(),
                scheduled: 0,
                sent: Pages::new(),
            }),
        };
        Ok(Bound::new(py, activation)?.unbind())
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn entry(&self) -> &str {
        &self.entry
    }

    #[must_use]
    pub fn since(&self) -> SystemTime {
        self.since
    }

    /// How many messages wait in the mailbox.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.held().mailbox.queued()
    }

    #[must_use]
    pub fn messages(&self, py: Python<'_>) -> Py<Schema> {
        self.held().behavior.messages.clone_ref(py)
    }

    pub fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state = self.held();
        match &state.value {
            Some(value) => Ok(value.clone_ref(py)),
            None if state.deleted => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "the state of {}/{} was deleted, and its type has no default to go on from",
                self.entry, self.key
            ))),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the state is read before the body starts",
            )),
        }
    }

    fn held(&self) -> MutexGuard<'_, State> {
        self.inner.locked()
    }

    /// What this activation goes by: the settings of the behavior it runs, over the system's.
    fn settings(&self) -> super::Settings {
        self.held().behavior.settings.over(&self.node.settings)
    }

    /// What the body hears when it writes after `become`.
    fn became(&self) -> PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(format!(
            "{}/{} became another behavior, which owns the state now",
            self.entry, self.key
        ))
    }

    /// Take the key over on the next turn of the loop, after whatever brought it here has been queued.
    pub fn begin(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let activation = slf.clone().unbind();
        callback::soon(&slf.get().node.running(py)?, move |py| {
            Self::taken(activation.bind(py), py)
        })
    }

    /// Run `step` of this activation with `future`, once it is done.
    fn then(
        slf: &Bound<'_, Self>,
        future: &Bound<'_, PyAny>,
        step: impl FnOnce(&Bound<'_, Self>, Python<'_>, &Bound<'_, PyAny>) -> PyResult<()>
        + Send
        + 'static,
    ) -> PyResult<()> {
        let activation = slf.clone().unbind();
        callback::when_done(future, move |py, done| step(activation.bind(py), py, done))
    }

    /// Run `step` of this activation `delay` seconds from now, giving back the timer that cancels it.
    fn timer<'py>(
        slf: &Bound<'py, Self>,
        delay: f64,
        step: impl FnOnce(&Bound<'_, Self>, Python<'_>) -> PyResult<()> + Send + 'static,
    ) -> PyResult<Bound<'py, PyAny>> {
        let activation = slf.clone().unbind();
        slf.get()
            .node
            .later(slf.py(), delay, move |py| step(activation.bind(py), py))
    }

    /// Ask the replicas for the key, and go on with what they hold.
    fn taken(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let this = slf.get();
        let initial = Self::initial(slf, py)?;
        let taken = this
            .node
            .persist(py, &this.entry, &this.key, Op::Activate { initial })?;
        Self::then(slf, &taken, Self::held_by)
    }

    /// What the replicas answered when this node asked for the key.
    ///
    /// A sweep activates a key nobody asked for, and it does so exactly while the cluster is changing, which is when
    /// the replicas are least likely to answer. Letting that reach the loop would take the node down with it.
    fn held_by(slf: &Bound<'_, Self>, py: Python<'_>, taken: &Bound<'_, PyAny>) -> PyResult<()> {
        let answered = match taken.call_method0("result") {
            Ok(answered) => answered,
            Err(failure) => {
                let why = failure.value(py).str()?.extract::<String>()?;
                Self::abandon(slf, py, &why)?;
                return Self::finish(slf, py);
            }
        };
        let held = answered.cast::<Taken>()?.get().held();
        let Some(held) = held else {
            slf.get().node.vacate(slf);
            Self::refuse_waiting(slf, py, Outcome::missing, "the key was not started")?;
            return Self::finish(slf, py);
        };
        {
            let mut state = slf.get().held();
            state.sent = held.pages.clone();
            state.pages = held.pages;
            state.lease = held.lease;
            state.exists = true;
            state.offered = None;
        }
        let behavior = Arc::clone(&slf.get().held().behavior);
        if behavior.native.is_none() {
            Self::taken_up(slf, py, &behavior)?;
        }
        Self::resume(slf, py)
    }

    /// The pages a key that nothing has starts from: what a `Start` offered, or the default of the type.
    fn initial(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Option<Pages>> {
        let (definition, pending) = {
            let state = slf.get().held();
            (Arc::clone(&state.behavior), state.offered.clone())
        };
        // A native body says what its keys start from, without a value of the interpreter in between, when its type
        // has a default; one without, as any other type, starts from the `initial` of a ref or not at all.
        if let (None, Some(native), Some(_)) = (&pending, &definition.native, &definition.initial) {
            return Ok(Some(native.initial()));
        }
        let state = match (pending, &definition.initial) {
            (Some(written), _) => {
                let schema = definition.state.bind(py);
                Schema::read(
                    schema,
                    schema.get().tree().sent(),
                    &written,
                    Some(&slf.get().node),
                )?
            }
            (None, Some(default)) => default.bind(py).clone(),
            (None, None) => return Ok(None),
        };
        Ok(Some(Self::pages_of(slf, py, &definition, &state)?))
    }

    fn pages_of(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        definition: &Definition,
        state: &Bound<'_, PyAny>,
    ) -> PyResult<Pages> {
        let mut pages = Schema::write_pages(definition.state.bind(py), state)?;
        if definition.name != slf.get().entry {
            pages.insert(BEHAVIOR.to_owned(), definition.name.clone().into_bytes());
        }
        if let Some(page) = slf.get().held().schedules_page() {
            pages.insert(SCHEDULES.to_owned(), page);
        }
        Ok(pages)
    }

    fn resume(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        {
            let mut state = slf.get().held();
            state.exists = true;
            if state.ending() {
                drop(state);
                return Self::wound_down(slf, py);
            }
        }
        let (behavior, named, pages) = {
            let state = slf.get().held();
            (
                Arc::clone(&state.behavior),
                state.named(&slf.get().entry),
                state.pages.clone(),
            )
        };
        if named != behavior.name {
            return Self::successor(slf, py, &named);
        }
        let without: Pages = pages
            .into_iter()
            .filter(|(name, _)| name != BEHAVIOR)
            .collect();
        let schema = behavior.state.bind(py);
        let value = Schema::read_pages(schema, &without, Some(&slf.get().node))?;
        slf.get().held().value = Some(value.unbind());
        Self::attempt(slf, py)
    }

    /// The activation of the behavior the state names, which takes the place of this one.
    fn successor(slf: &Bound<'_, Self>, py: Python<'_>, named: &str) -> PyResult<()> {
        let Some(behavior) = slf.get().node.resolve(py, named) else {
            let entry = &slf.get().entry;
            let key = &slf.get().key;
            slf.get().node.observe(py, || Observed::Failed {
                actor: entry.clone(),
                key: key.clone(),
                error: crate::errors::UnknownActor::new_err(format!(
                    "{entry}/{key} became {named}, which this node does not have"
                ))
                .into_value(py)
                .into_any(),
            });
            Self::abandon(slf, py, &format!("this node does not have {named}"))?;
            return Self::finish(slf, py);
        };
        slf.get().held().behavior = behavior;
        Self::resume(slf, py)
    }

    /// Start the body of the behavior the key runs: its native loop, or a run of it on the loop.
    ///
    /// A key let go before its body started, or while it became another behavior, lets go with what it holds instead.
    fn attempt(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let (behavior, releasing, ended) = {
            let mut state = slf.get().held();
            state.switching = false;
            (
                Arc::clone(&state.behavior),
                state.releasing,
                state.run.take(),
            )
        };
        if let Some(run) = ended {
            forsaken(py, &slf.get().node, run)?;
        }
        if releasing {
            return Self::deactivate(slf, py);
        }
        if let Some(native) = behavior.native.clone() {
            return Self::stepping(slf, py, &native);
        }
        Self::start(slf, py)
    }

    /// Start a run of the body in place of the one there is, whose failures it keeps.
    fn start(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let hold = slf.get().node.runs.hold();
        let (id, behavior, replaced) = {
            let mut state = slf.get().held();
            if state.finished {
                return Ok(());
            }
            state.started += 1;
            let id = state.started;
            let replaced = state.run.take();
            state.run = Some(Run {
                id,
                hold,
                failures: replaced.as_ref().map_or(0, |old| old.failures),
                ..Run::default()
            });
            (id, Arc::clone(&state.behavior), replaced)
        };
        if let Some(replaced) = replaced {
            forsaken(py, &slf.get().node, replaced)?;
        }
        let context = Bound::new(
            py,
            Context::new(slf.clone().unbind(), slf.get().node.clone(), id),
        )?;
        let body = behavior.body.bind(py).call1((context,))?;
        let task = slf
            .get()
            .node
            .running(py)?
            .call_method1("create_task", (body,))?;
        Self::then(slf, &task, move |slf, py, task| {
            Self::done(slf, py, id, task)
        })?;
        let found = match slf.get().held().run(id) {
            Some(run) => {
                run.task = Some(task.clone().unbind());
                run.reader = Some(task.clone().unbind());
                true
            }
            None => false,
        };
        // Until it reads, the run is known by its own task: what the body awaits before its first read holds up the
        // key as much as what it awaits on a message.
        if found {
            slf.get().node.runs.enter(&task, slf, id);
        }
        Ok(())
    }

    fn done(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        id: u64,
        task: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        // A body the activation cancelled is not a body that failed: `become`, `release` and a cancelled `ask` end it
        // that way.
        let exit = if task.call_method0("cancelled")?.is_truthy()? {
            Exit::Returned
        } else {
            let raised = task.call_method0("exception")?;
            if raised.is_none() {
                Exit::Returned
            } else {
                Exit::Raised(raised.unbind())
            }
        };
        let (reader, exit) = {
            let mut state = slf.get().held();
            let waits = !state.ending() && state.writes > 0;
            let Some(run) = state.run(id) else {
                return Ok(());
            };
            run.task = None;
            // Until the write lands, the state the run leaves is not known: one started now would read a state the
            // write is about to replace, and a release written now could land before it.
            let exit = if waits {
                run.settling = Some(exit);
                None
            } else {
                Some(exit)
            };
            (run.reader.take(), exit)
        };
        if let Some(reader) = reader {
            slf.get().node.runs.leave(reader.bind(py));
        }
        match exit {
            Some(exit) => Self::after(slf, py, id, exit),
            None => Ok(()),
        }
    }

    /// What a run that ended does next, with no write of the body in flight: wait out its failure, take the next
    /// message, hand the key to another behavior, or let the key go.
    fn after(slf: &Bound<'_, Self>, py: Python<'_>, id: u64, exit: Exit) -> PyResult<()> {
        if slf.get().held().ending() {
            return Self::wound_down(slf, py);
        }
        if let Exit::Raised(failure) = exit {
            let delay = Self::failed(slf, py, id, failure.bind(py))?;
            // A key being let go does not start a body that failed again: it ends where it failed.
            if !slf.get().held().releasing {
                Self::timer(slf, delay, move |slf, py| Self::retry(slf, py, id))?;
                return Ok(());
            }
        }
        let entry = &slf.get().entry;
        let (switching, moved, again, gone) = {
            let mut state = slf.get().held();
            let moved = state.named(entry) != state.behavior.name;
            let switching = state.switching;
            let empty = state.mailbox.empty();
            let releasing = state.releasing;
            let Some(run) = state.run(id) else {
                return Ok(());
            };
            run.current = None;
            let again = !releasing && (run.cancelled || (run.read && !empty));
            let gone = if switching || again {
                None
            } else {
                state.run.take()
            };
            (switching, moved, again, gone)
        };
        if let Some(gone) = gone {
            forsaken(py, &slf.get().node, gone)?;
        }
        if switching && moved {
            let named = slf.get().held().named(entry);
            return Self::successor(slf, py, &named);
        }
        if switching {
            return Self::attempt(slf, py);
        }
        if again {
            return Self::start(slf, py);
        }
        Self::deactivate(slf, py)
    }

    /// The backoff of a run that failed is over: it starts again, unless the activation is ending meanwhile, or the key
    /// is being let go, which ends the run there.
    fn retry(slf: &Bound<'_, Self>, py: Python<'_>, id: u64) -> PyResult<()> {
        let (ending, releasing) = {
            let state = slf.get().held();
            (state.ending(), state.releasing)
        };
        if ending {
            return Self::wound_down(slf, py);
        }
        if releasing {
            return Self::after(slf, py, id, Exit::Returned);
        }
        Self::start(slf, py)
    }

    /// The last write of the body in flight landed: a run that ended meanwhile goes on.
    fn settled(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let ended = slf
            .get()
            .held()
            .run
            .as_mut()
            .and_then(|run| run.settling.take().map(|exit| (run.id, exit)));
        match ended {
            Some((id, exit)) => Self::after(slf, py, id, exit),
            None => Ok(()),
        }
    }

    /// End an activation that is ending, once its body is no longer running. Only the first call does it.
    fn wound_down(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        {
            let state = slf.get().held();
            if state.finished || state.running() {
                return Ok(());
            }
        }
        let relinquished = Self::relinquish(slf, py);
        Self::finish(slf, py)?;
        relinquished
    }

    /// Tell the `ask` the run `id` was on what ended it, and answer how long the run waits before it starts again.
    fn failed(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        id: u64,
        failure: &Bound<'_, PyAny>,
    ) -> PyResult<f64> {
        let backoff = slf.get().settings().backoff;
        let (current, delay) = {
            let mut state = slf.get().held();
            let Some(run) = state.run(id) else {
                return Ok(backoff.first.as_secs_f64());
            };
            run.failures = run.failures.saturating_add(1);
            let grown = backoff.first.as_secs_f64()
                * backoff
                    .factor
                    .powi(i32::try_from(run.failures - 1).unwrap_or(i32::MAX));
            // A request the body answered before it raised has its answer.
            let answered = run.answered;
            (
                run.current.take().filter(|_| !answered),
                grown.min(backoff.limit.as_secs_f64()),
            )
        };
        Self::ended_by(slf, py, current, failure)?;
        Ok(delay)
    }

    /// `failure` ended the message `current`: the observer hears of it, and so does whoever asked, as a message the
    /// key did not process when it is `Unavailable` and as one that failed there otherwise.
    fn ended_by(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        current: Option<Deliver>,
        failure: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let this = slf.get();
        this.node.observe(py, || Observed::Failed {
            actor: this.entry.clone(),
            key: this.key.clone(),
            error: failure.clone().unbind(),
        });
        let Some(reply) = current.and_then(|current| current.reply) else {
            return Ok(());
        };
        let error: String = failure.get_type().getattr("__name__")?.extract()?;
        let message: String = failure.str()?.extract()?;
        let outcome = if error == "Unavailable" {
            Outcome::unreached(&this.entry, &this.key)
        } else {
            Outcome::Failed {
                actor: this.entry.clone(),
                key: this.key.clone(),
                error,
                message,
            }
        };
        this.node.answer(py, &reply, &outcome)
    }

    /// Remove the activation and the active mark, so that nothing brings the key back.
    ///
    /// The activation leaves the table before the write, so that a message arriving later starts a new one. The mark
    /// stays when the write does not go through: fenced, it is not this node's to remove, and the owner that took
    /// the key wrote over it; unavailable, the key keeps the mark and is taken over again from it.
    ///
    /// A key let go on purpose keeps its place in the table until its last write is out instead: what reaches it
    /// meanwhile queues behind what was there, and goes on once a new activation would no longer race that write.
    fn deactivate(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let this = slf.get();
        let releasing = this.held().releasing;
        if !releasing {
            this.node.vacate(slf);
            Self::refuse_waiting(
                slf,
                py,
                Outcome::unreached,
                "the body ended without reading it",
            )?;
        }
        let (pages, deleted, native, lease) = {
            let state = slf.get().held();
            (
                state.pages.clone(),
                state.deleted,
                state.behavior.native.clone(),
                state.lease,
            )
        };
        // A key whose state was deleted keeps nothing, not even the mark, so there is nothing left to write.
        if deleted {
            this.node.forgo(&this.entry, &this.key, lease);
            return Self::over(slf, py);
        }
        // A native key that holds nothing a new activation would miss goes instead of staying: a read of a key nothing
        // wrote leaves no key behind.
        let last = if native.is_some_and(|native| native.disposable(&pages)) {
            Op::Delete {
                lease,
                active: false,
            }
        } else {
            Op::Commit {
                lease,
                pages,
                active: false,
            }
        };
        let released = this.node.persist(py, &this.entry, &this.key, last)?;
        Self::then(slf, &released, Self::released)
    }

    /// The release write landed, or did not: either way this activation is over.
    ///
    /// A write that did not go through was reported where it failed. Its result is read only so that asyncio does not
    /// log the exception as never retrieved.
    fn released(slf: &Bound<'_, Self>, py: Python<'_>, written: &Bound<'_, PyAny>) -> PyResult<()> {
        let _ = written.call_method0("result");
        Self::over(slf, py)
    }

    /// The last write of the activation is out: it is over. A key let go on purpose leaves the table only now, and
    /// what reached it goes on, in the order it came, to the activation that starts it again.
    fn over(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let releasing = {
            let state = slf.get().held();
            state.releasing && !state.finished
        };
        if releasing {
            slf.get().node.vacate(slf);
            Self::pass_on(slf, py)?;
        }
        Self::finish(slf, py)
    }

    /// Give the key up: what was in flight is refused and what is queued goes back to routing.
    fn relinquish(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let draining = slf.get().held().draining;
        let why = if draining {
            "the node is shutting down"
        } else {
            "the key moved to another node"
        };
        slf.get().node.vacate(slf);
        let (in_hand, pending) = {
            let mut state = slf.get().held();
            let mut in_hand: Vec<Deliver> = Vec::new();
            let mut pending: Vec<Py<PyAny>> = Vec::new();
            if let Some(run) = state.run.as_mut() {
                let answered = run.answered;
                in_hand.extend(run.current.take().filter(|_| !answered));
                pending.extend(run.item.take());
                pending.extend(run.idle.take());
            }
            pending.extend(state.unarmed());
            in_hand.extend(state.current.take());
            (in_hand, pending)
        };
        for pending in pending {
            pending.bind(py).call_method0("cancel")?;
        }
        for current in &in_hand {
            Self::answer(slf, py, current, Outcome::unreached, why)?;
        }
        Self::pass_on(slf, py)
    }

    /// Hand what is queued back to routing, which sends it to whoever runs the key now, and call back the callers held
    /// for room.
    ///
    /// A queued `tell` that routing refuses, larger than a message between two nodes, is dropped and reported, and
    /// the rest of the queue still goes.
    fn pass_on(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let this = slf.get();
        let (queued, held) = this.held().mailbox.drain();
        for deliver in queued {
            if let Err(refused) = this.node.hand(py, Command::Deliver(deliver)) {
                let reason = refused.value(py).to_string();
                this.node.dropped(py, &this.entry, &this.key, &reason);
            }
        }
        Self::call(slf, py, &held)
    }

    /// Drop the activation without holding the key, telling whoever waits that nothing was done.
    fn abandon(slf: &Bound<'_, Self>, py: Python<'_>, why: &str) -> PyResult<()> {
        slf.get().node.vacate(slf);
        Self::refuse_waiting(slf, py, Outcome::unreached, why)
    }

    fn refuse_waiting(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        outcome: fn(&str, &str) -> Outcome,
        why: &str,
    ) -> PyResult<()> {
        let (queued, held) = slf.get().held().mailbox.drain();
        for deliver in &queued {
            Self::answer(slf, py, deliver, outcome, why)?;
        }
        // The held callers never reached this activation: they send again, to whichever takes the key next.
        Self::call(slf, py, &held)
    }

    /// The next message of the mailbox, calling back the callers held for the room its take makes.
    ///
    /// Every take of a running activation goes through here. A caller on this node sends again before its call back
    /// returns, so when there was nothing to take, what the callers sent is taken instead.
    fn next(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Option<Deliver>> {
        loop {
            let (deliver, called) = slf.get().held().mailbox.take();
            let sent = deliver.is_none() && !called.is_empty();
            Self::call(slf, py, &called)?;
            if !sent {
                return Ok(deliver);
            }
        }
    }

    /// Tell callers held for room that there is some, which they answer by sending their message again.
    fn call(slf: &Bound<'_, Self>, py: Python<'_>, called: &[Target]) -> PyResult<()> {
        if called.is_empty() {
            return Ok(());
        }
        let this = slf.get();
        let room = Outcome::full(&this.entry, &this.key);
        for caller in called {
            this.node.answer(py, caller, &room)?;
        }
        Ok(())
    }

    fn answer(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        deliver: &Deliver,
        outcome: fn(&str, &str) -> Outcome,
        why: &str,
    ) -> PyResult<()> {
        match &deliver.reply {
            None => {
                slf.get()
                    .node
                    .dropped(py, &deliver.actor, &deliver.key, why);
                Ok(())
            }
            Some(reply) => slf
                .get()
                .node
                .answer(py, reply, &outcome(&deliver.actor, &deliver.key)),
        }
    }

    fn finish(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let (ends, timers) = {
            let mut state = slf.get().held();
            if state.finished {
                return Ok(());
            }
            state.finished = true;
            let mut timers: Vec<Py<PyAny>> = state
                .idle
                .take()
                .into_iter()
                .chain(state.alarm.take())
                .collect();
            timers.extend(state.unarmed());
            (core::mem::take(&mut state.ends), timers)
        };
        // A collection's idle or deadline timer left armed would step a body that is over, and a schedule would tell a
        // key that is no longer active here.
        for timer in timers {
            timer.bind(py).call_method0("cancel")?;
        }
        let ended = slf.get().node.ended(py);
        for end in ends {
            let end = end.bind(py);
            if !end.call_method0("done")?.is_truthy()? {
                end.call_method1("set_result", (true,))?;
            }
        }
        ended
    }

    // --- what reaches the activation from outside ---

    pub fn put(slf: &Bound<'_, Self>, py: Python<'_>, command: Command) -> PyResult<()> {
        match command {
            Command::Start(start) => {
                let mut state = slf.get().held();
                if !state.exists && state.offered.is_none() {
                    state.offered = start.state;
                }
                Ok(())
            }
            Command::Deliver(deliver) => {
                let cycle = slf.get().closed(&deliver.chain);
                // What a body asks its own key reaches it in the step that asks, before that step could hand the
                // answer over to `to_self`: it waits in the mailbox, in its place, until the step is over.
                let undecided = match (&cycle, &deliver.reply) {
                    (Some(_), Some(reply)) if Self::reader(slf, py).is_some() => {
                        Some((reply.clone(), deliver.chain.clone()))
                    }
                    _ => None,
                };
                // Before the mailbox: a message no run would ever read fails at once, whether there is room or not.
                if let (Some(cycle), None) = (cycle, &undecided) {
                    return match &deliver.reply {
                        Some(reply) => slf.get().node.answer(py, reply, &Outcome::Cycle(cycle)),
                        None => Ok(()),
                    };
                }
                let put = slf.get().held().mailbox.put(deliver);
                match put {
                    Put::Queued => Self::wake(slf, py)?,
                    Put::Refused(deliver) => {
                        Self::answer(slf, py, &deliver, Outcome::full, "the mailbox is full")?;
                    }
                    Put::Held => {}
                }
                match undecided {
                    Some((reply, chain)) => Self::decide(slf, py, reply, chain),
                    None => Ok(()),
                }
            }
        }
    }

    /// Once the step on the loop is over, refuse the request `reply` if the run still waits for its answer down
    /// `chain`, unless the run read it meanwhile.
    fn decide(slf: &Bound<'_, Self>, py: Python<'_>, reply: Target, chain: Chain) -> PyResult<()> {
        let activation = slf.clone().unbind();
        callback::soon(&slf.get().node.running(py)?, move |py| {
            let slf = activation.bind(py);
            let Some(cycle) = slf.get().closed(&chain) else {
                return Ok(());
            };
            let withdrawn = slf.get().held().mailbox.withdraw(&reply);
            match withdrawn {
                Withdrawn::Queued(called) => Self::call(slf, py, &called)?,
                Withdrawn::Held => {}
                Withdrawn::Absent => return Ok(()),
            }
            slf.get().node.answer(py, &reply, &Outcome::Cycle(cycle))
        })
    }

    /// End the activation because the key is not this node's any more.
    pub fn release(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let (task, native, waiting) = {
            let mut state = slf.get().held();
            state.fenced = true;
            let task = state
                .run
                .as_ref()
                .and_then(|run| run.task.as_ref().map(|task| task.clone_ref(py)));
            (
                task,
                state.behavior.native.is_some(),
                state.awaiting.is_some() || state.writing.is_some(),
            )
        };
        if let Some(task) = task {
            task.bind(py).call_method0("cancel")?;
            return Ok(());
        }
        // A native body has no task to cancel: its loop is what ends it, and this takes the loop up. One that is
        // waiting for a write or for an answer ends on the callback of what it is waiting for. So does the run of the
        // body on the loop when it has no task, waiting out a backoff or a write.
        if native && !waiting {
            return Self::again(slf, py);
        }
        Ok(())
    }

    /// End the activation after the messages it is processing: this node is shutting down.
    pub fn drain(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        slf.get().held().draining = true;
        Self::wake(slf, py)
    }

    /// Let the key go on purpose, as if it idled out now, and resolve `over` with `True` once the activation is over.
    ///
    /// Nothing is cut short. The body ends at its next read, after the message it is on, and a run that ends with a
    /// write in flight waits for it to land, as any run does; a run waiting out a failure ends when the wait is over. A collection ends after the message it is on, and one with a deadline pending once it has none,
    /// since that is when it could idle out. The key then lets go with its last write, and what reached it meanwhile
    /// goes on once that write is out.
    pub fn retire(slf: &Bound<'_, Self>, py: Python<'_>, over: &Bound<'_, PyAny>) -> PyResult<()> {
        let (read, timers, resting) = {
            let mut state = slf.get().held();
            state.ends.push(over.clone().unbind());
            if state.releasing || state.ending() {
                return Ok(());
            }
            state.releasing = true;
            let mut read = None;
            let mut timers: Vec<Py<PyAny>> = Vec::new();
            if let Some(run) = state.run.as_mut() {
                run.deadline = None;
                read = run.waiting.take();
                timers.extend(run.idle.take());
            }
            let resting = state.behavior.native.is_some() && state.resting && state.alarm.is_none();
            if resting {
                state.resting = false;
                state.deadline = None;
                timers.extend(state.idle.take());
            }
            (read, timers, resting)
        };
        for timer in timers {
            timer.bind(py).call_method0("cancel")?;
        }
        if let Some(read) = read {
            let read = read.bind(py);
            if !read.call_method0("done")?.is_truthy()? {
                read.call_method1("set_exception", (ended(),))?;
            }
        }
        if resting {
            return Self::again(slf, py);
        }
        Ok(())
    }

    /// The caller of `request` stopped waiting for its answer. Answers whether the request was found here.
    ///
    /// Queued, the request goes, and the place it leaves goes to a caller held for room; held for room, its caller
    /// goes. In the hands of the body on the loop, its run is cancelled at the `await` it is on and starts again for
    /// the next message, from the last state written, unless it has told the request its answer: then the
    /// caller stopped waiting for an answer already on its way, and the run goes on with what it does after it. A
    /// native body finishes it: its keys are ordered by the messages it takes one at a time, and an answer nobody waits
    /// for is dropped by the caller's node. A request not found was answered already, or has not arrived.
    pub fn cancelled(slf: &Bound<'_, Self>, py: Python<'_>, request: &Target) -> PyResult<bool> {
        let withdrawn = slf.get().held().mailbox.withdraw(request);
        match withdrawn {
            Withdrawn::Queued(called) => return Self::call(slf, py, &called).map(|()| true),
            Withdrawn::Held => return Ok(true),
            Withdrawn::Absent => {}
        }
        let asked = |deliver: &Option<Deliver>| {
            deliver
                .as_ref()
                .is_some_and(|deliver| deliver.reply.as_ref() == Some(request))
        };
        let task = {
            let mut state = slf.get().held();
            if asked(&state.current) {
                return Ok(true);
            }
            let Some(run) = state.run.as_mut().filter(|run| asked(&run.current)) else {
                return Ok(false);
            };
            if run.answered {
                return Ok(true);
            }
            run.current = None;
            run.cancelled = true;
            run.task.as_ref().map(|task| task.clone_ref(py))
        };
        if let Some(task) = task {
            task.bind(py).call_method0("cancel")?;
        }
        Ok(true)
    }

    /// The bodies an `ask` of the run `id` keeps waiting: the callers still waiting on the message it is on, and the
    /// run itself, which holds the key until it reads again.
    #[must_use]
    pub fn chain(&self, id: u64) -> Chain {
        let state = self.held();
        let Some(run) = state.run.as_ref().filter(|run| run.id == id) else {
            return Chain::default();
        };
        let link = Link {
            actor: self.entry.clone(),
            key: self.key.clone(),
            hold: run.hold,
        };
        match &run.current {
            Some(current) if !run.answered => current.chain.then(link),
            _ => Chain::default().then(link),
        }
    }

    /// The run `id` waits for none of what it asked since it last read: the task that reads for it handed that over to
    /// `to_self`. A new stretch begins, so an `ask` that comes back down one of those chains is queued, not refused.
    pub fn handed(slf: &Bound<'_, Self>, py: Python<'_>, id: u64) {
        if Self::reader(slf, py) != Some(id) {
            return;
        }
        let hold = slf.get().node.runs.hold();
        if let Some(run) = slf.get().held().run(id) {
            run.hold = hold;
        }
    }

    /// The run of this activation the task running now reads for, if it reads for one.
    fn reader(slf: &Bound<'_, Self>, py: Python<'_>) -> Option<u64> {
        let (activation, id) = slf.get().node.runs.running(py)?;
        activation.bind(py).is(slf).then_some(id)
    }

    /// The run `id` told `target` its answer. When that is the request it is on, the request is answered.
    pub fn told(&self, id: u64, target: &Target) {
        if let Some(run) = self.held().run(id)
            && run
                .current
                .as_ref()
                .is_some_and(|current| current.reply.as_ref() == Some(target))
        {
            run.answered = true;
        }
    }

    /// The cycle a message down `chain` closes here, when the body is waiting, down that chain, for its answer.
    ///
    /// A run waits from its read until the next, except while it is reading; a native body waits while an answer is
    /// out, holding its mailbox.
    fn closed(&self, chain: &Chain) -> Option<String> {
        if chain.is_empty() {
            return None;
        }
        let state = self.held();
        let holding = if state.behavior.native.is_some() {
            state.awaiting.as_ref().map(|_| state.hold)
        } else {
            state
                .run
                .as_ref()
                .filter(|run| run.task.is_some() && run.waiting.is_none())
                .map(|run| run.hold)
        };
        chain.closed(&self.entry, &self.key, holding)
    }

    /// Hand the waiting read what arrived, or end it because nothing else will.
    ///
    /// A read whose deadline has passed ends even though a message is there: the body stops reading at `idle_after`,
    /// and what arrived after that waits in the mailbox for the run that reads next.
    fn wake(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        // A native body is not parked on a future: it is resting on the idle timer, and this takes it up again.
        if slf.get().held().behavior.native.is_some() {
            return Self::stirred(slf, py);
        }
        let reading = slf
            .get()
            .held()
            .run
            .as_ref()
            .filter(|run| run.waiting.is_some())
            .map(|run| (run.id, run.deadline));
        match reading {
            Some((id, deadline)) => Self::woken(slf, py, id, deadline),
            None => Ok(()),
        }
    }

    /// Wake the read the run `id` waits on, unless there is nothing to hand it and it goes on waiting.
    fn woken(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        id: u64,
        deadline: Option<f64>,
    ) -> PyResult<()> {
        let expired = match deadline {
            None => false,
            Some(deadline) => slf.get().node.now(py)? >= deadline,
        };
        let (waiting, draining) = {
            let mut state = slf.get().held();
            let draining = state.draining;
            let Some(waiting) = state.run(id).and_then(|run| run.waiting.take()) else {
                return Ok(());
            };
            (waiting, draining)
        };
        // A read the body gave up on, with a timeout of its own, takes nothing: what it took would be lost.
        if waiting.bind(py).call_method0("done")?.is_truthy()? {
            let idle = slf.get().held().run(id).and_then(|run| {
                run.deadline = None;
                run.idle.take()
            });
            if let Some(idle) = idle {
                idle.bind(py).call_method0("cancel")?;
            }
            return Ok(());
        }
        // The read leaves the run before the take: a caller called back from inside it sends again at once, and that
        // message is to be queued, not handed to this read in place of the one taken.
        let deliver = if expired {
            None
        } else {
            match Self::next(slf, py) {
                Ok(deliver) => deliver,
                Err(failure) => {
                    if let Some(run) = slf.get().held().run(id) {
                        run.waiting = Some(waiting);
                    }
                    return Err(failure);
                }
            }
        };
        let idle = {
            let mut state = slf.get().held();
            let Some(run) = state.run(id) else {
                return Ok(());
            };
            if deliver.is_none() && !draining && !expired {
                run.waiting = Some(waiting);
                return Ok(());
            }
            run.deadline = None;
            if let Some(deliver) = &deliver {
                run.current = Some(deliver.clone());
            }
            run.idle.take()
        };
        if let Some(idle) = idle {
            idle.bind(py).call_method0("cancel")?;
        }
        let waiting = waiting.bind(py);
        let Some(deliver) = deliver else {
            waiting.call_method1("set_exception", (ended(),))?;
            return Ok(());
        };
        match Self::opened(slf, py, &deliver) {
            Ok(msg) => waiting.call_method1("set_result", (msg,))?,
            Err(error) => waiting.call_method1("set_exception", (error,))?,
        };
        Ok(())
    }

    fn opened<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        deliver: &Deliver,
    ) -> PyResult<Bound<'py, PyAny>> {
        let behavior = Arc::clone(&slf.get().held().behavior);
        let schema = behavior.messages.bind(py);
        Ok(Schema::read(
            schema,
            schema.get().tree().sent(),
            &deliver.message,
            Some(&slf.get().node),
        )?)
    }

    /// One read of `inbox` or of `merge` by the run `id`: the next message, or what ends the reading.
    ///
    /// Reading is what tells the message the run took before is done with, and starts a new stretch of the run. The
    /// task that reads is the one the run is known by from then on: it holds what it reads. A context of a run that has
    /// ended, kept by something its body left behind, reads nothing.
    pub fn read<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        id: u64,
        source: Option<&Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let reading = slf.get().node.runs.current(py);
        let hold = slf.get().node.runs.hold();
        let (switching, task, moved) = {
            let mut state = slf.get().held();
            let switching = state.switching;
            let Some(run) = state.run(id) else {
                return Err(ended());
            };
            run.read = true;
            run.cancelled = false;
            run.answered = false;
            run.hold = hold;
            if run.current.take().is_some() {
                run.failures = 0;
            }
            let moved = reading
                .filter(|reading| {
                    !run.reader
                        .as_ref()
                        .is_some_and(|held| held.bind(py).is(reading))
                })
                .map(|reading| {
                    let left = run.reader.replace(reading.clone().unbind());
                    (reading, left)
                });
            (
                switching,
                run.task.as_ref().map(|task| task.clone_ref(py)),
                moved,
            )
        };
        if let Some((reading, left)) = moved {
            if let Some(left) = left {
                slf.get().node.runs.leave(left.bind(py));
            }
            slf.get().node.runs.enter(&reading, slf, id);
        }
        if switching {
            if let Some(task) = task {
                task.bind(py).call_method0("cancel")?;
            }
            // The cancellation lands on this await, which is where the body ends and the next behavior takes over.
            return slf.get().node.future(py);
        }
        let ending = {
            let state = slf.get().held();
            state.draining || state.releasing
        };
        if ending {
            return Err(ended());
        }
        let deliver = Self::next(slf, py)?;
        if let Some(deliver) = deliver {
            if let Some(run) = slf.get().held().run(id) {
                run.current = Some(deliver.clone());
            }
            let msg = Self::opened(slf, py, &deliver)?;
            let answer = slf.get().node.future(py)?;
            answer.call_method1("set_result", (msg,))?;
            return Ok(answer);
        }
        if let Some(source) = source {
            return Self::merging(slf, py, id, source);
        }
        let answer = slf.get().node.future(py)?;
        let (scheduled, given_up) = {
            let mut state = slf.get().held();
            let scheduled = !state.schedules.is_empty();
            let given_up = state.run(id).and_then(|run| {
                run.waiting = Some(answer.clone().unbind());
                run.deadline = None;
                run.idle.take()
            });
            (scheduled, given_up)
        };
        // The timer of a read the body gave up on would end this one early.
        if let Some(given_up) = given_up {
            given_up.bind(py).call_method0("cancel")?;
        }
        // Schedules go off only while the key is active, so a key that has some does not idle out.
        if !scheduled {
            Self::idling(slf, py, id)?;
        }
        Ok(answer)
    }

    /// End the read the run `id` waits on once `idle_after` passes without a message.
    fn idling(slf: &Bound<'_, Self>, py: Python<'_>, id: u64) -> PyResult<()> {
        let idle_after = slf.get().settings().idle_after.as_secs_f64();
        let timer = Self::timer(slf, idle_after, move |slf, py| Self::idle(slf, py, id))?;
        let deadline = slf.get().node.now(py)? + idle_after;
        if let Some(run) = slf.get().held().run(id) {
            run.deadline = Some(deadline);
            run.idle = Some(timer.unbind());
        }
        Ok(())
    }

    /// A read that races the mailbox with the source the body brought. Idleness does not end it.
    fn merging<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        id: u64,
        source: &Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pending = slf
            .get()
            .held()
            .run(id)
            .and_then(|run| run.item.as_ref().map(|task| task.clone_ref(py)));
        if let Some(pending) = pending {
            let pending = pending.bind(py);
            if pending.call_method0("done")?.is_truthy()? {
                if let Some(run) = slf.get().held().run(id) {
                    run.item = None;
                }
                return Self::delivered(slf, py, pending);
            }
        } else {
            let coroutine = source.bind(py).call_method0("__anext__")?;
            let task = slf
                .get()
                .node
                .running(py)?
                .call_method1("create_task", (coroutine,))?;
            Self::then(slf, &task, move |slf, py, task| {
                Self::item(slf, py, id, task)
            })?;
            if let Some(run) = slf.get().held().run(id) {
                run.item = Some(task.unbind());
            }
        }
        let answer = slf.get().node.future(py)?;
        if let Some(run) = slf.get().held().run(id) {
            run.waiting = Some(answer.clone().unbind());
        }
        Ok(answer)
    }

    /// The item a finished `anext` holds, as the answer of one read.
    fn delivered<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        task: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let answer = slf.get().node.future(py)?;
        match task.call_method0("result") {
            Ok(value) => {
                answer.call_method1("set_result", (value,))?;
            }
            Err(error) => {
                answer.call_method1("set_exception", (error,))?;
            }
        }
        Ok(answer)
    }

    /// The source produced an item while a read of the run `id` was waiting for one.
    fn item(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        id: u64,
        task: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let waiting = {
            let mut state = slf.get().held();
            let Some(run) = state.run(id) else {
                return Ok(());
            };
            let Some(waiting) = run.waiting.take() else {
                return Ok(());
            };
            run.item = None;
            waiting
        };
        let waiting = waiting.bind(py);
        if waiting.call_method0("done")?.is_truthy()? {
            return Ok(());
        }
        match task.call_method0("result") {
            Ok(value) => {
                waiting.call_method1("set_result", (value,))?;
            }
            Err(error) => {
                waiting.call_method1("set_exception", (error,))?;
            }
        }
        Ok(())
    }

    /// Nothing arrived for `idle_after`, so the reading of `inbox` by the run `id` ends and its body returns.
    fn idle(slf: &Bound<'_, Self>, py: Python<'_>, id: u64) -> PyResult<()> {
        let waiting = {
            let mut state = slf.get().held();
            let Some(run) = state.run(id) else {
                return Ok(());
            };
            run.idle = None;
            run.deadline = None;
            run.waiting.take()
        };
        let Some(waiting) = waiting else {
            return Ok(());
        };
        let waiting = waiting.bind(py);
        if !waiting.call_method0("done")?.is_truthy()? {
            waiting.call_method1("set_exception", (ended(),))?;
        }
        Ok(())
    }

    /// Store `state` and return once it is written. Without a cluster, that is at once.
    pub fn save<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let written = Self::stored(slf, py, value, false)?;
        Ok(Bound::new(py, Awaited::of(written))?.into_any())
    }

    /// Store what `change` makes of the state, and give it back once it is written.
    ///
    /// `change` answers the new state, or an awaitable of it. An awaitable runs as a task of its own, which is
    /// cancelled with whoever waits for the update.
    pub fn update<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        change: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let current = slf.get().value(py)?;
        let changed = change.call1((current,))?;
        let awaitable: bool = py
            .import("inspect")?
            .call_method1("isawaitable", (&changed,))?
            .extract()?;
        if !awaitable {
            let written = Self::stored(slf, py, &changed, true)?;
            return Ok(Bound::new(py, Awaited::of(written))?.into_any());
        }
        let changing = py
            .import("asyncio")?
            .call_method1("ensure_future", (changed,))?;
        let updated = slf.get().node.future(py)?;
        let waiting = updated.clone().unbind();
        Self::then(slf, &changing, move |slf, py, changing| {
            Self::changed(slf, py, changing, waiting.bind(py))
        })?;
        // The task computing the new state has nobody left to compute it for once the update is cancelled.
        let computing = changing.unbind();
        callback::when_done(&updated, move |py, updated| {
            if updated.call_method0("cancelled")?.is_truthy()? {
                computing.bind(py).call_method0("cancel")?;
            }
            Ok(())
        })?;
        Ok(Bound::new(py, Awaited::of(updated))?.into_any())
    }

    /// The new state an `update` waited for, on its way to being written. `updated`, which the body waits on,
    /// resolves to it once it is written.
    fn changed<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        changing: &Bound<'py, PyAny>,
        updated: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        if changing.call_method0("cancelled")?.is_truthy()? {
            return relay(changing, updated);
        }
        // Nobody waits for the update: the run that asked for it was cancelled, and a write now would land under the
        // run that took its place. Reading the outcome keeps asyncio from logging it as never retrieved.
        if updated.call_method0("done")?.is_truthy()? {
            let _ = changing.call_method0("exception");
            return Ok(());
        }
        let stored = changing
            .call_method0("result")
            .and_then(|value| Self::stored(slf, py, &value, true));
        match stored {
            Ok(written) => {
                let to = updated.clone().unbind();
                callback::when_done(&written, move |py, written| relay(written, to.bind(py)))?;
            }
            Err(failure) => {
                if !updated.call_method0("done")?.is_truthy()? {
                    updated.call_method1("set_exception", (failure,))?;
                }
            }
        }
        Ok(())
    }

    /// The future of the write of `value`, which resolves to it when `yields` and to nothing otherwise.
    fn stored<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
        yields: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        if slf.get().held().switching {
            return Err(slf.get().became());
        }
        let behavior = Arc::clone(&slf.get().held().behavior);
        let pages = Self::pages_of(slf, py, &behavior, value)?;
        let wrote = Wrote {
            pages,
            value: Some(value.clone().unbind()),
            switching: false,
            yields,
            deleted: false,
        };
        Self::write(slf, py, wrote)
    }

    /// Hand the key to another actor type, which runs it from the next read on.
    pub fn become_another<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        actor: &Bound<'py, PyAny>,
        definition: &Arc<Definition>,
        value: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        slf.get().node.learn(actor, definition);
        let mut pages = match value {
            None => slf
                .get()
                .held()
                .pages
                .iter()
                .filter(|(name, _)| name.as_str() != BEHAVIOR && name.as_str() != SCHEDULES)
                .map(|(name, page)| (name.clone(), page.clone()))
                .collect::<Pages>(),
            Some(value) => Schema::write_pages(definition.state.bind(py), value)?,
        };
        if definition.name != slf.get().entry {
            pages.insert(BEHAVIOR.to_owned(), definition.name.clone().into_bytes());
        }
        // The behavior takes the same messages, so the schedules of the key go on under it.
        if let Some(page) = slf.get().held().schedules_page() {
            pages.insert(SCHEDULES.to_owned(), page);
        }
        let wrote = Wrote {
            pages,
            value: None,
            switching: true,
            yields: false,
            deleted: false,
        };
        let written = Self::write(slf, py, wrote)?;
        Ok(Bound::new(py, Awaited::of(written))?.into_any())
    }

    /// Write the state and give back what the body waits on until the replicas have it.
    ///
    /// The state the activation holds changes when the write lands, not before: a body that goes on from a write
    /// that did not happen would be running on a state no replica has.
    fn write<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        wrote: Wrote,
    ) -> PyResult<Bound<'py, PyAny>> {
        let this = slf.get();
        let written = this.node.future(py)?;
        let op = this.writing(&wrote.pages, wrote.deleted);
        this.held().sent = wrote.pages.clone();
        let landing = this.node.persist(py, &this.entry, &this.key, op)?;
        let waiting = written.clone().unbind();
        Self::then(slf, &landing, move |slf, py, landing| {
            wrote.landed(slf, py, waiting.bind(py), landing)
        })?;
        this.held().writes += 1;
        Ok(written)
    }

    /// The write of `pages` under the lease of this activation, or the deletion of the key, which stays active.
    fn writing(&self, pages: &Pages, deleting: bool) -> Op {
        let lease = self.held().lease;
        if deleting {
            Op::Delete {
                lease,
                active: true,
            }
        } else {
            Op::Commit {
                lease,
                pages: pages.clone(),
                active: true,
            }
        }
    }

    /// Delete the state of the key on the replicas, and return once the deletion is written.
    ///
    /// The body goes on from the default of its type, which is what the key starts from when it is activated again;
    /// a type without one has no state to read until the next `set`. A body that ends without writing again leaves
    /// nothing of the key behind.
    pub fn delete<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if slf.get().held().switching {
            return Err(slf.get().became());
        }
        Self::cleared(slf, py)?;
        let behavior = Arc::clone(&slf.get().held().behavior);
        let (pages, value) = match &behavior.initial {
            Some(default) => (
                Self::pages_of(slf, py, &behavior, default.bind(py))?,
                Some(default.clone_ref(py)),
            ),
            None => (Pages::new(), None),
        };
        let wrote = Wrote {
            pages,
            value,
            switching: false,
            yields: false,
            deleted: true,
        };
        let written = Self::write(slf, py, wrote)?;
        Ok(Bound::new(py, Awaited::of(written))?.into_any())
    }

    // --- schedules ---

    /// Schedule `message` for the key under `name`, `delay` from now and then every `every`, in place of the schedule
    /// of that name, and give back what resolves to the schedule once the replicas have it: it goes off from then on,
    /// while the key is active.
    pub fn schedule<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        name: String,
        delay: Duration,
        every: Option<Duration>,
        message: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let behavior = Arc::clone(&slf.get().held().behavior);
        let messages = behavior.messages.bind(py);
        let schedule = schedule::Schedule {
            name: name.clone(),
            message: Schema::write(messages, messages.get().tree().sent(), message)?,
            due: wall().saturating_add(i64::try_from(delay.as_micros()).unwrap_or(i64::MAX)),
            every: every.map(|every| u64::try_from(every.as_micros()).unwrap_or(u64::MAX)),
        };
        let (id, replaced) = {
            let mut state = slf.get().held();
            let id = state.scheduled;
            state.scheduled += 1;
            let at = state
                .schedules
                .iter()
                .position(|scheduled| scheduled.schedule.name == name);
            let replaced = at.and_then(|at| state.schedules.remove(at).timer);
            state.schedules.push(Scheduled {
                id,
                schedule,
                message: message.clone().unbind(),
                timer: None,
            });
            (id, replaced)
        };
        if let Some(replaced) = replaced {
            replaced.bind(py).call_method0("cancel")?;
        }
        let made = Bound::new(
            py,
            Schedule::new(
                slf.clone().unbind(),
                id,
                name,
                message.clone().unbind(),
                every,
            ),
        )?
        .unbind();
        let landing = Self::rescheduled(slf, py)?;
        let answer = slf.get().node.future(py)?;
        let waiting = answer.clone().unbind();
        Self::then(slf, &landing, move |slf, py, landing| {
            let answer = waiting.bind(py);
            let landed = landing.call_method0("result");
            if let Err(failure) = &landed {
                // What the replicas did not confirm does not go off: the next write leaves it out.
                slf.get()
                    .held()
                    .schedules
                    .retain(|scheduled| scheduled.id != id);
                Self::unscheduled(slf, py)?;
                if !answer.call_method0("done")?.is_truthy()? {
                    answer.call_method1("set_exception", (failure.clone_ref(py),))?;
                }
                return Ok(());
            }
            Self::arm(slf, py, id)?;
            if !answer.call_method0("done")?.is_truthy()? {
                answer.call_method1("set_result", (made,))?;
            }
            Ok(())
        })?;
        Ok(Bound::new(py, Awaited::of(answer))?.into_any())
    }

    /// Cancel the schedule `id`, and give back what resolves once the replicas no longer have it. A schedule that is
    /// over, or that another of its name replaced, has nothing left to cancel.
    pub fn unschedule<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        id: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let removed = {
            let mut state = slf.get().held();
            if state.finished {
                let Self { entry, key, .. } = slf.get();
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "the activation of {entry}/{key} is over, and its schedules with it"
                )));
            }
            let at = state
                .schedules
                .iter()
                .position(|scheduled| scheduled.id == id);
            at.map(|at| state.schedules.remove(at))
        };
        let Some(removed) = removed else {
            let over = slf.get().node.future(py)?;
            over.call_method1("set_result", (py.None(),))?;
            return Ok(Bound::new(py, Awaited::of(over))?.into_any());
        };
        if let Some(timer) = removed.timer {
            timer.bind(py).call_method0("cancel")?;
        }
        let written = Self::rescheduled(slf, py)?;
        Self::unscheduled(slf, py)?;
        Ok(Bound::new(py, Awaited::of(written))?.into_any())
    }

    /// The schedules of the key, as the body lists them.
    pub fn schedules(slf: &Bound<'_, Self>, py: Python<'_>) -> Vec<Schedule> {
        slf.get()
            .held()
            .schedules
            .iter()
            .map(|scheduled| {
                Schedule::new(
                    slf.clone().unbind(),
                    scheduled.id,
                    scheduled.schedule.name.clone(),
                    scheduled.message.clone_ref(py),
                    scheduled.schedule.every.map(Duration::from_micros),
                )
            })
            .collect()
    }

    /// When the schedule `id` goes off next, in microseconds since the Unix epoch, while it is not over.
    #[must_use]
    pub fn due(&self, id: u64) -> Option<i64> {
        self.held()
            .schedules
            .iter()
            .find(|scheduled| scheduled.id == id)
            .map(|scheduled| scheduled.schedule.due)
    }

    /// Take up the schedules of the state the replicas answered, and arm them: one whose time passed while no node ran
    /// the key goes off at once. A schedule this type no longer reads as a message is dropped, and reported. Every
    /// behavior of the key takes the same messages, so the type the key started as reads them.
    fn taken_up(slf: &Bound<'_, Self>, py: Python<'_>, behavior: &Definition) -> PyResult<()> {
        let this = slf.get();
        let Some(page) = this.held().pages.get(SCHEDULES).cloned() else {
            return Ok(());
        };
        let schedules = match schedule::decode(&page) {
            Ok(schedules) => schedules,
            Err(malformed) => {
                let why = format!("the schedules of the key do not read: {malformed}");
                this.node.dropped(py, &this.entry, &this.key, &why);
                return Ok(());
            }
        };
        let messages = behavior.messages.bind(py);
        for schedule in schedules {
            let read = Schema::read(
                messages,
                messages.get().tree().sent(),
                &schedule.message,
                Some(&this.node),
            );
            let message = match read {
                Ok(message) => message,
                Err(failure) => {
                    let failure = PyErr::from(failure);
                    let why = format!(
                        "the schedule {} of the key does not read as a message of its type: {}",
                        schedule.name,
                        failure.value(py)
                    );
                    this.node.dropped(py, &this.entry, &this.key, &why);
                    continue;
                }
            };
            let id = {
                let mut state = this.held();
                let id = state.scheduled;
                state.scheduled += 1;
                state.schedules.push(Scheduled {
                    id,
                    schedule,
                    message: message.unbind(),
                    timer: None,
                });
                id
            };
            Self::arm(slf, py, id)?;
        }
        Ok(())
    }

    /// Arm the timer of the schedule `id` for when it is due.
    fn arm(slf: &Bound<'_, Self>, py: Python<'_>, id: u64) -> PyResult<()> {
        let due = {
            let state = slf.get().held();
            if state.finished {
                return Ok(());
            }
            let found = state.schedules.iter().find(|scheduled| scheduled.id == id);
            found.map(|scheduled| scheduled.schedule.due)
        };
        let Some(due) = due else {
            return Ok(());
        };
        let wait = Duration::from_micros(u64::try_from(due.saturating_sub(wall())).unwrap_or(0));
        let timer = Self::timer(slf, wait.as_secs_f64(), move |slf, py| {
            Self::fire(slf, py, id)
        })?;
        let replaced = {
            let mut state = slf.get().held();
            let found = state
                .schedules
                .iter_mut()
                .find(|scheduled| scheduled.id == id);
            found.and_then(|scheduled| scheduled.timer.replace(timer.unbind()))
        };
        if let Some(replaced) = replaced {
            replaced.bind(py).call_method0("cancel")?;
        }
        Ok(())
    }

    /// The schedule `id` is due: the key is told its message, and the schedule goes off again at its next time, or is
    /// over. The replicas hear which, so that the node that takes the key over next does not send it again.
    fn fire(slf: &Bound<'_, Self>, py: Python<'_>, id: u64) -> PyResult<()> {
        let now = wall();
        let fired = {
            let mut state = slf.get().held();
            if state.ending() || state.releasing {
                return Ok(());
            }
            let Some(at) = state
                .schedules
                .iter()
                .position(|scheduled| scheduled.id == id)
            else {
                return Ok(());
            };
            let scheduled = &mut state.schedules[at];
            scheduled.timer = None;
            let message = scheduled.schedule.message.clone();
            let next = scheduled.schedule.after(now);
            if let Some(next) = next {
                scheduled.schedule.due = next;
            } else {
                state.schedules.remove(at);
            }
            (message, next.is_some())
        };
        let (message, again) = fired;
        let this = slf.get();
        let told = this.node.hand(
            py,
            Command::Deliver(Deliver {
                actor: this.entry.clone(),
                key: this.key.clone(),
                message,
                reply: None,
                chain: Chain::default(),
            }),
        );
        if let Err(refused) = told {
            let why = refused.value(py).to_string();
            this.node.dropped(py, &this.entry, &this.key, &why);
        }
        if again {
            Self::arm(slf, py, id)?;
        }
        // Nobody waits for this write: a write that fails was reported where it failed, and the next one carries the
        // schedules as they are then.
        let written = Self::rescheduled(slf, py)?;
        callback::when_done(&written, |_, written| {
            let _ = written.call_method0("exception");
            Ok(())
        })?;
        if again {
            return Ok(());
        }
        Self::unscheduled(slf, py)
    }

    /// Write the schedules as they are now, beside the state of the last write sent.
    fn rescheduled<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pages = {
            let state = slf.get().held();
            let mut pages = state.sent.clone();
            pages.remove(SCHEDULES);
            if let Some(page) = state.schedules_page() {
                pages.insert(SCHEDULES.to_owned(), page);
            }
            pages
        };
        let wrote = Wrote {
            pages,
            value: None,
            switching: false,
            yields: false,
            deleted: false,
        };
        Self::write(slf, py, wrote)
    }

    /// The key may have no schedules left, and then a read waiting with nothing to end it idles out from now.
    fn unscheduled(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let reading = {
            let state = slf.get().held();
            if !state.schedules.is_empty() {
                return Ok(());
            }
            state
                .run
                .as_ref()
                .filter(|run| run.waiting.is_some() && run.idle.is_none() && run.item.is_none())
                .map(|run| run.id)
        };
        match reading {
            Some(id) => Self::idling(slf, py, id),
            None => Ok(()),
        }
    }

    /// Cancel every schedule of the key.
    fn cleared(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let timers = {
            let mut state = slf.get().held();
            let timers = state.unarmed();
            state.schedules.clear();
            timers
        };
        for timer in timers {
            timer.bind(py).call_method0("cancel")?;
        }
        Ok(())
    }
}

/// The wall clock in microseconds since the Unix epoch, which is what a schedule is written in, since it travels
/// between nodes.
fn wall() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX)
        })
}

/// A write of the body on its way to the replicas, and what it changes here once they have it.
#[derive(Debug)]
struct Wrote {
    pages: Pages,
    value: Option<Py<PyAny>>,
    switching: bool,
    /// Whether the write resolves to the value it stored, which is what an `update` gives back.
    yields: bool,
    /// Whether the write is the deletion of the key, after which the body goes on from `value`, or from no value at
    /// all when its type has no default.
    deleted: bool,
}

impl Wrote {
    /// The write landed, or did not, and `written`, which the body waits on, hears which. Once no write of the body is
    /// in flight, a run that ended meanwhile goes on.
    fn landed(
        self,
        activation: &Bound<'_, Activation>,
        py: Python<'_>,
        written: &Bound<'_, PyAny>,
        commit: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let landed = self.taken_in(activation, py, written, commit);
        let settled = {
            let mut state = activation.get().held();
            state.writes = state.writes.saturating_sub(1);
            state.writes == 0
        };
        if settled {
            Activation::settled(activation, py)?;
        }
        landed
    }

    /// Take in what the write did: the state it stored, or the failure whoever waits on it hears.
    fn taken_in(
        self,
        activation: &Bound<'_, Activation>,
        py: Python<'_>,
        written: &Bound<'_, PyAny>,
        commit: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if let Err(failure) = commit.call_method0("result") {
            // The key moved to another owner, so the body ends where its write was refused instead of going on.
            if failure.is_instance_of::<Fencing>(py) {
                return Activation::release(activation, py);
            }
            if !written.call_method0("done")?.is_truthy()? {
                written.call_method1("set_exception", (failure,))?;
            }
            return Ok(());
        }
        let result = match (&self.value, self.yields) {
            (Some(value), true) => value.clone_ref(py),
            _ => py.None(),
        };
        {
            let mut state = activation.get().held();
            state.pages = self.pages;
            if self.deleted {
                state.value = self.value;
            } else if let Some(value) = self.value {
                state.value = Some(value);
            }
            state.deleted = self.deleted;
            state.switching = state.switching || self.switching;
        }
        if !written.call_method0("done")?.is_truthy()? {
            written.call_method1("set_result", (result,))?;
        }
        Ok(())
    }
}

/// The end of one future carried to another that nobody has ended yet.
fn relay(from: &Bound<'_, PyAny>, to: &Bound<'_, PyAny>) -> PyResult<()> {
    if to.call_method0("done")?.is_truthy()? {
        return Ok(());
    }
    if from.call_method0("cancelled")?.is_truthy()? {
        to.call_method0("cancel")?;
        return Ok(());
    }
    match from.call_method0("result") {
        Ok(value) => to.call_method1("set_result", (value,))?,
        Err(failure) => to.call_method1("set_exception", (failure,))?,
    };
    Ok(())
}

/// The body of a collection, which runs here instead of on the loop.
impl Activation {
    /// Take messages until there are none, writing and answering as the body says.
    ///
    /// Every suspension point of the body is a return from here: the write of a message, the answer of something it
    /// asked for, a deadline it set, and the idle timeout of a key with nothing to do. Each of them comes back to
    /// this through a callback of the loop.
    fn stepping(slf: &Bound<'_, Self>, py: Python<'_>, native: &Arc<dyn Native>) -> PyResult<()> {
        loop {
            let (ending, releasing) = {
                let state = slf.get().held();
                (state.ending(), state.releasing && state.alarm.is_none())
            };
            if ending {
                return Self::wound_down(slf, py);
            }
            // Let go, a key lets go between two messages, unless a deadline of its own keeps it busy.
            if releasing {
                return Self::deactivate(slf, py);
            }
            slf.get().held().resting = false;
            let deliver = Self::next(slf, py)?;
            let Some(deliver) = deliver else {
                return Self::resting(slf, py, native);
            };
            let hold = slf.get().node.runs.hold();
            {
                let mut state = slf.get().held();
                state.current = Some(deliver.clone());
                state.hold = hold;
            }
            let turn = {
                let pages = slf.get().held().pages.clone();
                native.step(
                    &pages,
                    &Given::Message(&deliver.message),
                    slf.get().node.clock(py)?,
                )
            };
            if !Self::turned(slf, py, turn, Some(deliver.message))? {
                return Ok(());
            }
        }
    }

    /// Carry out a turn. `false` says the body is waiting for something and this run of it is over.
    fn turned(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        turn: Turn,
        message: Option<Vec<u8>>,
    ) -> PyResult<bool> {
        Self::alarmed(slf, py, turn.alarm)?;
        if let Some(ask) = turn.ask {
            let Some(message) = message else {
                return Ok(true);
            };
            Self::asking(slf, py, &ask, message)?;
            return Ok(false);
        }
        let deleting = turn.delete;
        let pages = if deleting {
            // The body goes on from where a key nothing wrote starts.
            let native = slf.get().held().behavior.native.clone();
            native.map_or_else(Pages::new, |native| native.initial())
        } else {
            let Some(pages) = turn.save else {
                Self::answer_all(slf, py, turn.replies)?;
                slf.get().held().current = None;
                return Ok(true);
            };
            pages
        };
        // The state changes when the write lands, not before: a body that went on from a write that did not happen
        // would be running on a state no replica has.
        let this = slf.get();
        let landing = this
            .node
            .persist(py, &this.entry, &this.key, this.writing(&pages, deleting));
        let written = match landing {
            Ok(written) => written,
            // A write that is refused before it starts ends the message the same way one that fails does.
            Err(failure) => {
                Self::refused(slf, py, &failure)?;
                return Ok(true);
            }
        };
        {
            let mut state = slf.get().held();
            state.pending = turn.replies;
            state.writing = Some(pages);
            state.deleting = deleting;
        }
        Self::then(slf, &written, Self::stepped)?;
        Ok(false)
    }

    /// The write of the message the body is on has landed, or it has not.
    fn stepped(slf: &Bound<'_, Self>, py: Python<'_>, written: &Bound<'_, PyAny>) -> PyResult<()> {
        let (replies, pages, deleting) = {
            let mut state = slf.get().held();
            (
                core::mem::take(&mut state.pending),
                state.writing.take(),
                core::mem::take(&mut state.deleting),
            )
        };
        let Err(failure) = written.call_method0("result") else {
            {
                let mut state = slf.get().held();
                if let Some(pages) = pages {
                    state.pages = pages;
                }
                state.deleted = deleting;
            }
            Self::answer_all(slf, py, replies)?;
            slf.get().held().current = None;
            return Self::again(slf, py);
        };
        // The key moved to another owner, so this activation ends where its write was refused.
        if failure.is_instance_of::<Fencing>(py) {
            return Self::release(slf, py);
        }
        Self::refused(slf, py, &failure)?;
        Self::again(slf, py)
    }

    /// A write or an ask of a native body did not go through: whoever asked hears what ended it.
    fn refused(slf: &Bound<'_, Self>, py: Python<'_>, failure: &PyErr) -> PyResult<()> {
        let current = slf.get().held().current.take();
        Self::ended_by(slf, py, current, failure.value(py).as_any())
    }

    /// Ask another key, and come back to the same message with its answer.
    fn asking(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        ask: &crate::collections::Asking,
        message: Vec<u8>,
    ) -> PyResult<()> {
        let node = &slf.get().node;
        let answer = node.future(py)?;
        let id = node.awaited(&answer);
        let reply = Target::Reply {
            node: node.id(),
            id,
        };
        super::armed(py, node, id, super::replies::timeout(py, node, &ask.to))?;
        Self::then(slf, &answer, Self::answered)?;
        // The body holds its mailbox until the answer arrives, so it waits down the chain of the message it is on.
        let chain = {
            let mut state = slf.get().held();
            state.awaiting = Some((message, id));
            let link = Link {
                actor: slf.get().entry.clone(),
                key: slf.get().key.clone(),
                hold: state.hold,
            };
            match &state.current {
                Some(current) => current.chain.then(link),
                None => Chain::default().then(link),
            }
        };
        let Target::Entity { actor, key } = &ask.to else {
            return Ok(());
        };
        node.hand(
            py,
            Command::Deliver(Deliver {
                actor: actor.clone(),
                key: key.clone(),
                message: (ask.message)(&reply),
                reply: Some(reply),
                chain,
            }),
        )
    }

    /// What the body asked for has answered, so the message it is on goes on.
    fn answered(slf: &Bound<'_, Self>, py: Python<'_>, answer: &Bound<'_, PyAny>) -> PyResult<()> {
        let awaiting = slf.get().held().awaiting.take();
        let Some((message, _)) = awaiting else {
            return Ok(());
        };
        let Some(native) = slf.get().held().behavior.native.clone() else {
            return Ok(());
        };
        let held = match answer.call_method0("result") {
            Ok(held) => held,
            Err(failure) => {
                // Nothing was written, so the key is where it was and whoever asked hears that it did not happen.
                Self::refused(slf, py, &failure)?;
                return Self::again(slf, py);
            }
        };
        // A native ask waits with no schema, so its answer comes back as the bytes it travelled as.
        let answered: Vec<u8> = held.extract().unwrap_or_default();
        let turn = {
            let pages = slf.get().held().pages.clone();
            native.step(
                &pages,
                &Given::Answered {
                    message: &message,
                    answer: &answered,
                },
                slf.get().node.clock(py)?,
            )
        };
        if Self::turned(slf, py, turn, Some(message))? {
            Self::again(slf, py)?;
        }
        Ok(())
    }

    /// The deadline a native body asked for has passed, with no message in the meantime.
    fn rang(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        {
            let mut state = slf.get().held();
            state.alarm = None;
            state.resting = false;
        }
        let Some(native) = slf.get().held().behavior.native.clone() else {
            return Ok(());
        };
        let turn = {
            let pages = slf.get().held().pages.clone();
            native.step(&pages, &Given::Alarm, slf.get().node.clock(py)?)
        };
        if Self::turned(slf, py, turn, None)? {
            Self::again(slf, py)?;
        }
        Ok(())
    }

    /// Nothing to take: wait for the next message, for the deadline, or for the key to idle out.
    fn resting(slf: &Bound<'_, Self>, py: Python<'_>, native: &Arc<dyn Native>) -> PyResult<()> {
        if slf.get().held().draining {
            return Self::deactivate(slf, py);
        }
        slf.get().held().resting = true;
        // A body with a deadline of its own waits on that instead: the key is busy as long as it has one.
        if native.timed() && slf.get().held().alarm.is_some() {
            return Ok(());
        }
        let idle_after = slf.get().settings().idle_after.as_secs_f64();
        let timer = Self::timer(slf, idle_after, Self::sleeping)?;
        let mut state = slf.get().held();
        state.idle = Some(timer.unbind());
        state.deadline = Some(slf.get().node.now(py)? + idle_after);
        Ok(())
    }

    /// The key has had nothing to do for `idle_after`: it lets go, and a message brings it back.
    fn sleeping(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        {
            let mut state = slf.get().held();
            state.idle = None;
            state.deadline = None;
            state.resting = false;
            if !state.mailbox.empty() {
                // A message arrived while the timer was going off, and it is read instead.
                drop(state);
                return Self::again(slf, py);
            }
        }
        Self::deactivate(slf, py)
    }

    /// A message arrived while a native body had nothing to do. It reads again unless it is busy already.
    fn stirred(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        {
            let mut state = slf.get().held();
            // Working on a message, waiting for an answer or for a write: the loop comes back on its own. Not
            // resting either means the body has not started yet, and the message is there when it does.
            if !state.resting || state.current.is_some() || state.awaiting.is_some() {
                return Ok(());
            }
            state.resting = false;
            state.deadline = None;
            let idle = state.idle.take();
            drop(state);
            if let Some(idle) = idle {
                idle.bind(py).call_method0("cancel")?;
            }
        }
        Self::again(slf, py)
    }

    /// Take the reading up again, from wherever the body left off.
    fn again(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let Some(native) = slf.get().held().behavior.native.clone() else {
            return Ok(());
        };
        Self::stepping(slf, py, &native)
    }

    /// Set, replace or drop the deadline the body asked for.
    fn alarmed(slf: &Bound<'_, Self>, py: Python<'_>, at: Option<f64>) -> PyResult<()> {
        let held = slf.get().held().alarm.take();
        if let Some(held) = held {
            held.bind(py).call_method0("cancel")?;
        }
        let Some(at) = at else {
            return Ok(());
        };
        let delay = (at - slf.get().node.clock(py)?).max(0.0);
        let timer = Self::timer(slf, delay, Self::rang)?;
        slf.get().held().alarm = Some(timer.unbind());
        Ok(())
    }

    /// Tell each answer of a native body to whom it goes, as `Ref.tell` does: the caller of an `ask` settles, and an
    /// entity reads it from its mailbox, as a message nobody waits on.
    fn answer_all(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        replies: Vec<(Target, Vec<u8>)>,
    ) -> PyResult<()> {
        let node = &slf.get().node;
        for (target, answer) in replies {
            match target {
                Target::Entity { actor, key } => node.hand(
                    py,
                    Command::Deliver(Deliver {
                        actor,
                        key,
                        message: answer,
                        reply: None,
                        chain: Chain::default(),
                    }),
                )?,
                reply @ Target::Reply { .. } => {
                    node.answer(py, &reply, &Outcome::Value(answer))?;
                }
            }
        }
        Ok(())
    }
}
