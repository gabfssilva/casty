//! A key active on this node: its mailbox, its state and the tasks of its body.
//!
//! The body runs again from the start after it fails, after `become`, after it returns leaving messages behind, and
//! after the caller of the message it is on cancels it. Only the last saved state survives, so a restart never sees
//! the local variables of the previous run. A type with `concurrency=n` has up to n runs of its body reading the same
//! mailbox, each known by the message it took last; its state is read-only, so no run writes under another.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use casty_core::chain::{Chain, Link};
use casty_core::mailbox::{Command, Deliver, Mailbox, Put, Withdrawn};
use casty_core::node::Target;
use casty_core::outcome::Outcome;
use casty_core::store::Pages;
use pyo3::prelude::*;

use crate::collections::{Given, Native, Turn};

use super::Node;
use super::cluster::{Fencing, Taken};
use super::context::{Context, ended};
use super::observe::Observed;
use crate::actor::Behavior;
use crate::awaited::Awaited;
use crate::lock::Locked;
use crate::schema::Schema;

/// The reserved page with the name of the behavior that reads the state.
const BEHAVIOR: &str = "@behavior";

// The flags of the state machine, each one named after the thing it decides.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct State {
    behavior: Behavior,
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
    /// Whether the body on the loop runs, so that a message finding every run of it busy can start another.
    live: bool,
    pages: Pages,
    /// The activation the node took the key for, which every write of this one carries.
    lease: u64,
    value: Option<Py<PyAny>>,
    /// The runs of the body on the loop, at most `concurrency` of them. A native body has none.
    runs: Vec<Run>,
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
}

impl State {
    fn ending(&self) -> bool {
        self.fenced || self.draining
    }

    fn run(&mut self, id: u64) -> Option<&mut Run> {
        self.runs.iter_mut().find(|run| run.id == id)
    }

    /// Whether any run of the body has a task that has not ended.
    fn running(&self) -> bool {
        self.runs.iter().any(|run| run.task.is_some())
    }

    /// The behavior the state names: the type the key started as, unless it became another.
    fn named(&self, entry: &str) -> String {
        match self.pages.get(BEHAVIOR) {
            None => entry.to_owned(),
            Some(page) => String::from_utf8_lossy(page).into_owned(),
        }
    }
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
        behavior: &Behavior,
        entry: &str,
        key: &str,
    ) -> PyResult<Py<Self>> {
        let settings = behavior.definition().settings;
        let activation = Self {
            node: node.clone(),
            entry: entry.to_owned(),
            key: key.to_owned(),
            since: SystemTime::now(),
            inner: Mutex::new(State {
                behavior: behavior.clone_ref(py),
                mailbox: Mailbox::new(settings.mailbox, settings.on_full),
                offered: None,
                exists: false,
                fenced: false,
                draining: false,
                releasing: false,
                ends: Vec::new(),
                switching: false,
                live: false,
                pages: Pages::new(),
                lease: 0,
                value: None,
                runs: Vec::new(),
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
        self.held().behavior.definition().messages.clone_ref(py)
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
        self.held()
            .behavior
            .definition()
            .settings
            .over(&self.node.settings)
    }

    /// Take the key over on the next turn of the loop, after whatever brought it here has been queued.
    pub fn begin(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let call = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Begin,
            },
        )?;
        slf.get()
            .node
            .running(py)?
            .call_method1("call_soon", (call,))?;
        Ok(())
    }

    /// Ask the replicas for the key, and go on with what they hold.
    fn taken(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let initial = Self::initial(slf, py)?;
        let taken = slf.get().node.activate(py, &entry, &key, initial)?;
        let then = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Held,
            },
        )?;
        taken.call_method1("add_done_callback", (then,))?;
        Ok(())
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
            state.pages = held.pages;
            state.lease = held.lease;
            state.exists = true;
            state.offered = None;
        }
        Self::resume(slf, py)
    }

    /// The pages a key that nothing has starts from: what a `Start` offered, or the default of the type.
    fn initial(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Option<Pages>> {
        let (behavior, pending) = {
            let state = slf.get().held();
            (state.behavior.clone_ref(py), state.offered.clone())
        };
        let definition = behavior.definition();
        // A native body says what its keys start from, without a value of the interpreter in between.
        if let (None, Some(native)) = (&pending, &definition.native) {
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
        Ok(Some(Self::pages_of(slf, py, &behavior, &state)?))
    }

    fn pages_of(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        behavior: &Behavior,
        state: &Bound<'_, PyAny>,
    ) -> PyResult<Pages> {
        let definition = behavior.definition();
        let mut pages = Schema::write_pages(definition.state.bind(py), state)?;
        if definition.name != slf.get().entry {
            pages.insert(BEHAVIOR.to_owned(), definition.name.clone().into_bytes());
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
        let entry = slf.get().entry.clone();
        let (behavior, named, pages) = {
            let state = slf.get().held();
            (
                state.behavior.clone_ref(py),
                state.named(&entry),
                state.pages.clone(),
            )
        };
        if named != behavior.definition().name {
            return Self::successor(slf, py, &named);
        }
        let without: Pages = pages
            .into_iter()
            .filter(|(name, _)| name != BEHAVIOR)
            .collect();
        let schema = behavior.definition().state.bind(py);
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

    /// Start the body of the behavior the key runs: its native loop, or `concurrency` runs of it on the loop.
    ///
    /// A key let go before its body started, or while it became another behavior, lets go with what it holds instead.
    fn attempt(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let (behavior, releasing, ended) = {
            let mut state = slf.get().held();
            state.switching = false;
            (
                state.behavior.clone_ref(py),
                state.releasing,
                core::mem::take(&mut state.runs),
            )
        };
        for run in ended {
            forsaken(py, &slf.get().node, run)?;
        }
        if releasing {
            return Self::deactivate(slf, py);
        }
        if let Some(native) = behavior.definition().native.clone() {
            return Self::stepping(slf, py, &native);
        }
        slf.get().held().live = true;
        for _ in 0..behavior.definition().settings.concurrency {
            Self::start(slf, py, None)?;
        }
        Ok(())
    }

    /// Start a run of the body, in place of the run `replacing` when there is one, whose failures it keeps.
    fn start(slf: &Bound<'_, Self>, py: Python<'_>, replacing: Option<u64>) -> PyResult<()> {
        let hold = slf.get().node.runs.hold();
        let (id, behavior, replaced) = {
            let mut state = slf.get().held();
            if state.finished {
                return Ok(());
            }
            state.started += 1;
            let id = state.started;
            let replaced = replacing
                .and_then(|old| state.runs.iter().position(|run| run.id == old))
                .map(|at| state.runs.remove(at));
            state.runs.push(Run {
                id,
                hold,
                failures: replaced.as_ref().map_or(0, |old| old.failures),
                ..Run::default()
            });
            (id, state.behavior.clone_ref(py), replaced)
        };
        if let Some(replaced) = replaced {
            forsaken(py, &slf.get().node, replaced)?;
        }
        let context = Bound::new(
            py,
            Context::new(slf.clone().unbind(), slf.get().node.clone(), id),
        )?;
        let body = behavior.definition().body.bind(py).call1((context,))?;
        let task = slf
            .get()
            .node
            .running(py)?
            .call_method1("create_task", (body,))?;
        let done = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Done(id),
            },
        )?;
        task.call_method1("add_done_callback", (done,))?;
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
    /// message, hand the key to another behavior, or let the key go when it was the last run.
    fn after(slf: &Bound<'_, Self>, py: Python<'_>, id: u64, exit: Exit) -> PyResult<()> {
        if slf.get().held().ending() {
            return Self::wound_down(slf, py);
        }
        if let Exit::Raised(failure) = exit {
            let delay = Self::failed(slf, py, id, failure.bind(py))?;
            // A key being let go does not start a body that failed again: it ends where it failed.
            if !slf.get().held().releasing {
                let retry = Bound::new(
                    py,
                    Step {
                        activation: slf.clone().unbind(),
                        step: Which::Retry(id),
                    },
                )?;
                slf.get().node.later(py, delay, retry.into_any())?;
                return Ok(());
            }
        }
        let entry = slf.get().entry.clone();
        let (switching, moved, again, gone, alone) = {
            let mut state = slf.get().held();
            let moved = state.named(&entry) != state.behavior.definition().name;
            let switching = state.switching;
            let empty = state.mailbox.empty();
            let releasing = state.releasing;
            let Some(run) = state.run(id) else {
                return Ok(());
            };
            run.current = None;
            let again = !releasing && (run.cancelled || (run.read && !empty));
            let mut gone = None;
            if !switching && !again {
                let at = state.runs.iter().position(|run| run.id == id);
                gone = at.map(|at| state.runs.remove(at));
            }
            (switching, moved, again, gone, state.runs.is_empty())
        };
        if let Some(gone) = gone {
            forsaken(py, &slf.get().node, gone)?;
        }
        if switching && moved {
            let named = slf.get().held().named(&entry);
            return Self::successor(slf, py, &named);
        }
        if switching {
            return Self::attempt(slf, py);
        }
        if again {
            return Self::start(slf, py, Some(id));
        }
        if alone {
            return Self::deactivate(slf, py);
        }
        Ok(())
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
        Self::start(slf, py, Some(id))
    }

    /// The last write of the body in flight landed: the runs that ended meanwhile go on.
    fn settled(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let ended: Vec<(u64, Exit)> = slf
            .get()
            .held()
            .runs
            .iter_mut()
            .filter_map(|run| run.settling.take().map(|exit| (run.id, exit)))
            .collect();
        for (id, exit) in ended {
            Self::after(slf, py, id, exit)?;
        }
        Ok(())
    }

    /// End an activation that is ending, once no run of its body is left running. Only the first call does it.
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
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
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
        slf.get().node.observe(py, || Observed::Failed {
            actor: entry.clone(),
            key: key.clone(),
            error: failure.clone().unbind(),
        });
        if let Some(current) = current
            && let Some(reply) = &current.reply
        {
            let error: String = failure.get_type().getattr("__name__")?.extract()?;
            let message: String = failure.str()?.extract()?;
            let outcome = if error == "Unavailable" {
                Outcome::unreached(&entry, &key)
            } else {
                Outcome::Failed {
                    actor: entry.clone(),
                    key: key.clone(),
                    error,
                    message,
                }
            };
            slf.get().node.answer(py, reply, &outcome)?;
        }
        Ok(delay)
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
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let releasing = {
            let mut state = slf.get().held();
            state.live = false;
            state.releasing
        };
        if !releasing {
            slf.get().node.vacate(slf);
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
                state.behavior.definition().native.clone(),
                state.lease,
            )
        };
        // A key whose state was deleted keeps nothing, not even the mark, so there is nothing left to write.
        if deleted {
            slf.get().node.forgo(&entry, &key, lease);
            return Self::over(slf, py);
        }
        // A native key that holds nothing a new activation would miss goes instead of staying: a read of a key nothing
        // wrote leaves no key behind.
        let released = if native.is_some_and(|native| native.disposable(&pages)) {
            slf.get().node.delete(py, &entry, &key, lease, false)?
        } else {
            slf.get()
                .node
                .commit(py, &entry, &key, lease, pages, false)?
        };
        let then = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Released,
            },
        )?;
        released.call_method1("add_done_callback", (then,))?;
        Ok(())
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
            for run in &mut state.runs {
                let answered = run.answered;
                in_hand.extend(run.current.take().filter(|_| !answered));
                pending.extend(run.item.take());
                pending.extend(run.idle.take());
            }
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
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let (queued, held) = slf.get().held().mailbox.drain();
        for deliver in queued {
            if let Err(refused) = slf.get().node.hand(py, Command::Deliver(deliver)) {
                let reason = refused.value(py).to_string();
                slf.get().node.dropped(py, &entry, &key, &reason);
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
        let room = Outcome::full(&slf.get().entry, &slf.get().key);
        for caller in called {
            slf.get().node.answer(py, caller, &room)?;
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
            let timers: Vec<Py<PyAny>> = state
                .idle
                .take()
                .into_iter()
                .chain(state.alarm.take())
                .collect();
            (core::mem::take(&mut state.ends), timers)
        };
        // A collection's idle or deadline timer left armed would step a body that is over.
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
                // Before the mailbox: a message no run would ever read fails at once, whether there is room or not.
                if let Some(cycle) = slf.get().closed(&deliver) {
                    return match &deliver.reply {
                        Some(reply) => slf.get().node.answer(py, reply, &Outcome::Cycle(cycle)),
                        None => Ok(()),
                    };
                }
                let put = slf.get().held().mailbox.put(deliver);
                match put {
                    Put::Queued => Self::wake(slf, py),
                    Put::Refused(deliver) => {
                        Self::answer(slf, py, &deliver, Outcome::full, "the mailbox is full")
                    }
                    Put::Held => Ok(()),
                }
            }
        }
    }

    /// End the activation because the key is not this node's any more.
    pub fn release(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let (tasks, native, waiting) = {
            let mut state = slf.get().held();
            state.fenced = true;
            let tasks: Vec<Py<PyAny>> = state
                .runs
                .iter()
                .filter_map(|run| run.task.as_ref().map(|task| task.clone_ref(py)))
                .collect();
            (
                tasks,
                state.behavior.definition().native.is_some(),
                state.awaiting.is_some() || state.writing.is_some(),
            )
        };
        if !tasks.is_empty() {
            for task in tasks {
                task.bind(py).call_method0("cancel")?;
            }
            return Ok(());
        }
        // A native body has no task to cancel: its loop is what ends it, and this takes the loop up. One that is
        // waiting for a write or for an answer ends on the callback of what it is waiting for. So does a run of the
        // body on the loop that has no task, waiting out a backoff or a write.
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
    /// Nothing is cut short. Each run of the body ends at its next read, after the message it is on, and a run that
    /// ends with a write in flight waits for it to land, as any run does; a run waiting out a failure ends when the
    /// wait is over. A collection ends after the message it is on, and one with a deadline pending once it has none,
    /// since that is when it could idle out. The key then lets go with its last write, and what reached it meanwhile
    /// goes on once that write is out.
    pub fn retire(slf: &Bound<'_, Self>, py: Python<'_>, over: &Bound<'_, PyAny>) -> PyResult<()> {
        let (reads, timers, resting) = {
            let mut state = slf.get().held();
            state.ends.push(over.clone().unbind());
            if state.releasing || state.ending() {
                return Ok(());
            }
            state.releasing = true;
            let mut reads: Vec<Py<PyAny>> = Vec::new();
            let mut timers: Vec<Py<PyAny>> = Vec::new();
            for run in &mut state.runs {
                run.deadline = None;
                reads.extend(run.waiting.take());
                timers.extend(run.idle.take());
            }
            let resting = state.behavior.definition().native.is_some()
                && state.resting
                && state.alarm.is_none();
            if resting {
                state.resting = false;
                state.deadline = None;
                timers.extend(state.idle.take());
            }
            (reads, timers, resting)
        };
        for timer in timers {
            timer.bind(py).call_method0("cancel")?;
        }
        for read in reads {
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
    /// goes. In the hands of a run of the body on the loop, that run is cancelled at the `await` it is on and starts
    /// again for the next message, from the last state written, unless it has told the request its answer: then the
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
            let Some(run) = state.runs.iter_mut().find(|run| asked(&run.current)) else {
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
        let Some(run) = state.runs.iter().find(|run| run.id == id) else {
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

    /// The cycle `deliver` closes here, when every run the body may have is waiting, down its chain, for its answer.
    ///
    /// A run waits from its read until the next, except while it is reading; a native body waits while an answer is
    /// out, holding its mailbox.
    fn closed(&self, deliver: &Deliver) -> Option<String> {
        if deliver.chain.is_empty() {
            return None;
        }
        let state = self.held();
        let definition = state.behavior.definition();
        let (holding, concurrency): (Vec<u64>, usize) = if definition.native.is_some() {
            (state.awaiting.iter().map(|_| state.hold).collect(), 1)
        } else {
            (
                state
                    .runs
                    .iter()
                    .filter(|run| run.task.is_some() && run.waiting.is_none())
                    .map(|run| run.hold)
                    .collect(),
                definition.settings.concurrency,
            )
        };
        deliver
            .chain
            .closed(&self.entry, &self.key, &holding, concurrency)
    }

    /// Hand the waiting reads what arrived, or end them because nothing else will. With no read waiting, a body that
    /// runs fewer runs than its `concurrency` starts one more for what arrived.
    ///
    /// A read whose deadline has passed ends even though a message is there: the body stops reading at `idle_after`,
    /// and what arrived after that waits in the mailbox for the run that reads next.
    fn wake(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        // A native body is not parked on a future: it is resting on the idle timer, and this takes it up again.
        if slf.get().held().behavior.definition().native.is_some() {
            return Self::stirred(slf, py);
        }
        loop {
            let first = slf
                .get()
                .held()
                .runs
                .iter()
                .find(|run| run.waiting.is_some())
                .map(|run| (run.id, run.deadline));
            let Some((id, deadline)) = first else {
                return Self::widen(slf, py);
            };
            if !Self::woken(slf, py, id, deadline)? {
                return Ok(());
            }
        }
    }

    /// Wake the read the run `id` waits on. `false` says it goes on waiting: there is nothing to hand it.
    fn woken(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        id: u64,
        deadline: Option<f64>,
    ) -> PyResult<bool> {
        let expired = match deadline {
            None => false,
            Some(deadline) => slf.get().node.now(py)? >= deadline,
        };
        let (waiting, draining) = {
            let mut state = slf.get().held();
            let draining = state.draining;
            let Some(waiting) = state.run(id).and_then(|run| run.waiting.take()) else {
                return Ok(true);
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
            return Ok(true);
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
                return Ok(true);
            };
            if deliver.is_none() && !draining && !expired {
                run.waiting = Some(waiting);
                return Ok(false);
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
            return Ok(true);
        };
        match Self::opened(slf, py, &deliver) {
            Ok(msg) => waiting.call_method1("set_result", (msg,))?,
            Err(error) => waiting.call_method1("set_exception", (error,))?,
        };
        Ok(true)
    }

    /// Start one more run of the body for a message that finds every run busy, up to its `concurrency`.
    fn widen(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let wanted = {
            let state = slf.get().held();
            state.live
                && !state.ending()
                && !state.releasing
                && !state.mailbox.empty()
                && state.runs.len() < state.behavior.definition().settings.concurrency
        };
        if wanted {
            return Self::start(slf, py, None);
        }
        Ok(())
    }

    fn opened<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        deliver: &Deliver,
    ) -> PyResult<Bound<'py, PyAny>> {
        let behavior = slf.get().held().behavior.clone_ref(py);
        let schema = behavior.definition().messages.bind(py);
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
        let idle = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Idle(id),
            },
        )?;
        let idle_after = slf.get().settings().idle_after.as_secs_f64();
        let timer = slf.get().node.later(py, idle_after, idle.into_any())?;
        let deadline = slf.get().node.now(py)? + idle_after;
        // The timer of a read the body gave up on would end this one early.
        let replaced = slf.get().held().run(id).and_then(|run| {
            run.waiting = Some(answer.clone().unbind());
            run.deadline = Some(deadline);
            run.idle.replace(timer.unbind())
        });
        if let Some(replaced) = replaced {
            replaced.bind(py).call_method0("cancel")?;
        }
        Ok(answer)
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
            let ready = Bound::new(
                py,
                Step {
                    activation: slf.clone().unbind(),
                    step: Which::Item(id),
                },
            )?;
            task.call_method1("add_done_callback", (ready,))?;
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
        Self::writable(slf)?;
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
        Self::writable(slf)?;
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
        let then = Bound::new(
            py,
            Changed {
                activation: slf.clone().unbind(),
                updated: updated.clone().unbind(),
            },
        )?;
        changing.call_method1("add_done_callback", (then,))?;
        let abandoned = Bound::new(
            py,
            Abandoned {
                changing: changing.unbind(),
            },
        )?;
        updated.call_method1("add_done_callback", (abandoned,))?;
        Ok(Bound::new(py, Awaited::of(updated))?.into_any())
    }

    /// The future of the write of `value`, which resolves to it when `yields` and to nothing otherwise.
    fn stored<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
        yields: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        if slf.get().held().switching {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{entry}/{key} became another behavior, which owns the state now"
            )));
        }
        let behavior = slf.get().held().behavior.clone_ref(py);
        let pages = Self::pages_of(slf, py, &behavior, value)?;
        Self::write(slf, py, pages, Some(value.clone().unbind()), false, yields)
    }

    /// Hand the key to another actor type, which runs it from the next read on.
    pub fn become_another<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        behavior: &Behavior,
        value: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::writable(slf)?;
        let entry = slf.get().entry.clone();
        slf.get().node.learn(py, behavior);
        let definition = behavior.definition();
        let mut pages = match value {
            None => slf
                .get()
                .held()
                .pages
                .iter()
                .filter(|(name, _)| name.as_str() != BEHAVIOR)
                .map(|(name, page)| (name.clone(), page.clone()))
                .collect::<Pages>(),
            Some(value) => Schema::write_pages(definition.state.bind(py), value)?,
        };
        if definition.name != entry {
            pages.insert(BEHAVIOR.to_owned(), definition.name.clone().into_bytes());
        }
        let written = Self::write(slf, py, pages, None, true, false)?;
        Ok(Bound::new(py, Awaited::of(written))?.into_any())
    }

    /// Write the state and give back what the body waits on until the replicas have it.
    ///
    /// The state the activation holds changes when the write lands, not before: a body that goes on from a write
    /// that did not happen would be running on a state no replica has.
    fn write<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        pages: Pages,
        value: Option<Py<PyAny>>,
        switching: bool,
        yields: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let written = slf.get().node.future(py)?;
        let lease = slf.get().held().lease;
        let commit = slf
            .get()
            .node
            .commit(py, &entry, &key, lease, pages.clone(), true)?;
        let then = Bound::new(
            py,
            Wrote {
                activation: slf.clone().unbind(),
                written: written.clone().unbind(),
                pages: Mutex::new(Some(pages)),
                value: Mutex::new(value),
                switching,
                yields,
                deleted: false,
            },
        )?;
        commit.call_method1("add_done_callback", (then,))?;
        slf.get().held().writes += 1;
        Ok(written)
    }

    /// Delete the state of the key on the replicas, and return once the deletion is written.
    ///
    /// The body goes on from the default of its type, which is what the key starts from when it is activated again;
    /// a type without one has no state to read until the next `set`. A body that ends without writing again leaves
    /// nothing of the key behind.
    pub fn delete<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Self::writable(slf)?;
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        if slf.get().held().switching {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{entry}/{key} became another behavior, which owns the state now"
            )));
        }
        let behavior = slf.get().held().behavior.clone_ref(py);
        let (pages, value) = match &behavior.definition().initial {
            Some(default) => (
                Self::pages_of(slf, py, &behavior, default.bind(py))?,
                Some(default.clone_ref(py)),
            ),
            None => (Pages::new(), None),
        };
        let written = slf.get().node.future(py)?;
        let lease = slf.get().held().lease;
        let deletion = slf.get().node.delete(py, &entry, &key, lease, true)?;
        let then = Bound::new(
            py,
            Wrote {
                activation: slf.clone().unbind(),
                written: written.clone().unbind(),
                pages: Mutex::new(Some(pages)),
                value: Mutex::new(value),
                switching: false,
                yields: false,
                deleted: true,
            },
        )?;
        deletion.call_method1("add_done_callback", (then,))?;
        slf.get().held().writes += 1;
        Ok(Bound::new(py, Awaited::of(written))?.into_any())
    }

    /// Refuse a write of the state from a type whose body handles several messages at once: two runs writing it is the
    /// race the mailbox of one key exists to prevent.
    fn writable(slf: &Bound<'_, Self>) -> PyResult<()> {
        let concurrency = slf.get().held().behavior.definition().settings.concurrency;
        if concurrency > 1 {
            let entry = &slf.get().entry;
            let key = &slf.get().key;
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "{entry}/{key} handles {concurrency} messages at once (concurrency={concurrency}), so its state is \
                 read-only"
            )));
        }
        Ok(())
    }
}

/// Which step of an activation a callback of the loop runs, and for which run of its body.
#[derive(Debug, Clone, Copy)]
enum Which {
    Begin,
    Held,
    Released,
    Done(u64),
    Retry(u64),
    Idle(u64),
    Item(u64),
    Stepped,
    Answered,
    Ring,
    Sleeping,
}

/// One step of an activation, as something the event loop can call.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Step {
    activation: Py<Activation>,
    step: Which,
}

#[pymethods]
impl Step {
    #[pyo3(signature = (*args))]
    fn __call__(&self, py: Python<'_>, args: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<()> {
        let activation = self.activation.bind(py);
        match self.step {
            Which::Begin => Activation::taken(activation, py),
            Which::Held => Activation::held_by(activation, py, &args.get_item(0)?),
            Which::Released => Activation::released(activation, py, &args.get_item(0)?),
            Which::Done(run) => Activation::done(activation, py, run, &args.get_item(0)?),
            Which::Retry(run) => Activation::retry(activation, py, run),
            Which::Idle(run) => Activation::idle(activation, py, run),
            Which::Item(run) => Activation::item(activation, py, run, &args.get_item(0)?),
            Which::Stepped => Activation::stepped(activation, py, &args.get_item(0)?),
            Which::Answered => Activation::answered(activation, py, &args.get_item(0)?),
            Which::Ring => Activation::rang(activation, py),
            Which::Sleeping => Activation::sleeping(activation, py),
        }
    }
}

/// A write on its way to the replicas, and what it changes here once they have it.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Wrote {
    activation: Py<Activation>,
    /// What the body is waiting on, which only the write landing resolves.
    written: Py<PyAny>,
    pages: Mutex<Option<Pages>>,
    value: Mutex<Option<Py<PyAny>>>,
    switching: bool,
    /// Whether the write resolves to the value it stored, which is what an `update` gives back.
    yields: bool,
    /// Whether the write is the deletion of the key, after which the body goes on from `value`, or from no value at
    /// all when its type has no default.
    deleted: bool,
}

#[pymethods]
impl Wrote {
    fn __call__(&self, py: Python<'_>, commit: &Bound<'_, PyAny>) -> PyResult<()> {
        let activation = self.activation.bind(py);
        let landed = self.landed(py, activation, commit);
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
}

impl Wrote {
    /// Take in what the write did: the state it stored, or the failure whoever waits on it hears.
    fn landed(
        &self,
        py: Python<'_>,
        activation: &Bound<'_, Activation>,
        commit: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let written = self.written.bind(py);
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
        let stored = self.value.locked().take();
        let result = match (&stored, self.yields) {
            (Some(value), true) => value.clone_ref(py),
            _ => py.None(),
        };
        {
            let mut state = activation.get().held();
            if let Some(pages) = self.pages.locked().take() {
                state.pages = pages;
            }
            if self.deleted {
                state.value = stored;
            } else if let Some(value) = stored {
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

/// The new state an `update` waited for, on its way to being written.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Changed {
    activation: Py<Activation>,
    /// What the body is waiting on, which resolves to the state once it is written.
    updated: Py<PyAny>,
}

#[pymethods]
impl Changed {
    fn __call__(&self, py: Python<'_>, changing: &Bound<'_, PyAny>) -> PyResult<()> {
        let updated = self.updated.bind(py);
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
            .and_then(|value| Activation::stored(self.activation.bind(py), py, &value, true));
        match stored {
            Ok(written) => {
                let then = Bound::new(
                    py,
                    Relayed {
                        to: self.updated.clone_ref(py),
                    },
                )?;
                written.call_method1("add_done_callback", (then,))?;
            }
            Err(failure) => {
                if !updated.call_method0("done")?.is_truthy()? {
                    updated.call_method1("set_exception", (failure,))?;
                }
            }
        }
        Ok(())
    }
}

/// A write whose end is the end of the `update` that asked for it.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Relayed {
    to: Py<PyAny>,
}

#[pymethods]
impl Relayed {
    fn __call__(&self, py: Python<'_>, written: &Bound<'_, PyAny>) -> PyResult<()> {
        relay(written, self.to.bind(py))
    }
}

/// The task computing a new state, which has nobody left to compute it for once the `update` is cancelled.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Abandoned {
    changing: Py<PyAny>,
}

#[pymethods]
impl Abandoned {
    fn __call__(&self, py: Python<'_>, updated: &Bound<'_, PyAny>) -> PyResult<()> {
        if updated.call_method0("cancelled")?.is_truthy()? {
            self.changing.bind(py).call_method0("cancel")?;
        }
        Ok(())
    }
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
            let native = slf.get().held().behavior.definition().native.clone();
            native.map_or_else(Pages::new, |native| native.initial())
        } else {
            let Some(pages) = turn.save else {
                Self::answer_all(slf, py, turn.replies)?;
                slf.get().held().current = None;
                return Ok(true);
            };
            pages
        };
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        // The state changes when the write lands, not before: a body that went on from a write that did not happen
        // would be running on a state no replica has.
        let lease = slf.get().held().lease;
        let landing = if deleting {
            slf.get().node.delete(py, &entry, &key, lease, true)
        } else {
            slf.get()
                .node
                .commit(py, &entry, &key, lease, pages.clone(), true)
        };
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
        let then = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Stepped,
            },
        )?;
        written.call_method1("add_done_callback", (then,))?;
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
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let error: String = failure.get_type(py).getattr("__name__")?.extract()?;
        let message: String = failure.value(py).str()?.extract()?;
        slf.get().node.observe(py, || Observed::Failed {
            actor: entry.clone(),
            key: key.clone(),
            error: failure.value(py).clone().into_any().unbind(),
        });
        let Some(current) = slf.get().held().current.take() else {
            return Ok(());
        };
        let Some(reply) = &current.reply else {
            return Ok(());
        };
        let outcome = if error == "Unavailable" {
            Outcome::unreached(&entry, &key)
        } else {
            Outcome::Failed {
                actor: entry,
                key,
                error,
                message,
            }
        };
        slf.get().node.answer(py, reply, &outcome)
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
        let then = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Answered,
            },
        )?;
        answer.call_method1("add_done_callback", (then,))?;
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
        let behavior = slf.get().held().behavior.clone_ref(py);
        let Some(native) = behavior.definition().native.clone() else {
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
        let behavior = slf.get().held().behavior.clone_ref(py);
        let Some(native) = behavior.definition().native.clone() else {
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
        let idle = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Sleeping,
            },
        )?;
        let idle_after = slf.get().settings().idle_after.as_secs_f64();
        let timer = slf.get().node.later(py, idle_after, idle.into_any())?;
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
        let behavior = slf.get().held().behavior.clone_ref(py);
        let Some(native) = behavior.definition().native.clone() else {
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
        let ring = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Ring,
            },
        )?;
        let delay = (at - slf.get().node.clock(py)?).max(0.0);
        let timer = slf.get().node.later(py, delay, ring.into_any())?;
        slf.get().held().alarm = Some(timer.unbind());
        Ok(())
    }

    fn answer_all(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        replies: Vec<(Target, Vec<u8>)>,
    ) -> PyResult<()> {
        for (target, answer) in replies {
            slf.get()
                .node
                .answer(py, &target, &Outcome::Value(answer))?;
        }
        Ok(())
    }
}
