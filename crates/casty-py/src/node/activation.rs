//! A key active on this node: its mailbox, its state and the task of its body.
//!
//! The body runs again from the start after it fails, after `become`, and after it returns leaving messages behind.
//! Only the last saved state survives, so a restart never sees the local variables of the previous run.

use std::sync::{Arc, Mutex, MutexGuard};

use casty_core::mailbox::{Command, Deliver, Mailbox};
use casty_core::node::Target;
use casty_core::outcome::Outcome;
use casty_core::store::Pages;
use pyo3::prelude::*;

use crate::collections::{Given, Native, Turn};

use super::cluster::{Fencing, Taken};
use super::context::{Context, ended};
use super::{Node, dropped, log};
use crate::actor::Behavior;
use crate::awaited::Awaited;
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
    switching: bool,
    read: bool,
    failures: u32,
    current: Option<Deliver>,
    pages: Pages,
    value: Option<Py<PyAny>>,
    task: Option<Py<PyAny>>,
    waiting: Option<Py<PyAny>>,
    idle: Option<Py<PyAny>>,
    /// When the read being waited on stops waiting, which a message arriving later does not undo.
    deadline: Option<f64>,
    item: Option<Py<PyAny>>,
    finished: bool,
    /// What a native body is still working on, and the answer it is waiting for.
    awaiting: Option<(Vec<u8>, i64)>,
    /// What a native body answers once the write of the message it is on has landed.
    pending: Vec<(Target, Vec<u8>)>,
    /// The state of a native body that is on its way to the replicas, which it takes only once it is there.
    writing: Option<Pages>,
    /// The timer of a native body that asked for a deadline.
    alarm: Option<Py<PyAny>>,
    /// Whether a native body has nothing to do, so that a message arriving takes its loop up again.
    resting: bool,
}

impl State {
    fn ending(&self) -> bool {
        self.fenced || self.draining
    }

    /// The behavior the state names: the type the key started as, unless it became another.
    fn named(&self, entry: &str) -> String {
        match self.pages.get(BEHAVIOR) {
            None => entry.to_owned(),
            Some(page) => String::from_utf8_lossy(page).into_owned(),
        }
    }
}

#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Activation {
    node: Arc<Node>,
    entry: String,
    key: String,
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
        let capacity = behavior.definition().mailbox;
        let activation = Self {
            node: node.clone(),
            entry: entry.to_owned(),
            key: key.to_owned(),
            inner: Mutex::new(State {
                behavior: behavior.clone_ref(py),
                mailbox: Mailbox::new(capacity),
                offered: None,
                exists: false,
                fenced: false,
                draining: false,
                switching: false,
                read: false,
                failures: 0,
                current: None,
                pages: Pages::new(),
                value: None,
                task: None,
                waiting: None,
                idle: None,
                deadline: None,
                item: None,
                finished: false,
                awaiting: None,
                pending: Vec::new(),
                writing: None,
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
    pub fn messages(&self, py: Python<'_>) -> Py<Schema> {
        self.held().behavior.definition().messages.clone_ref(py)
    }

    pub fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.held().value {
            Some(value) => Ok(value.clone_ref(py)),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the state is read before the body starts",
            )),
        }
    }

    fn held(&self) -> MutexGuard<'_, State> {
        self.inner
            .lock()
            .expect("the activation lock is never poisoned")
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
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let answered = match taken.call_method0("result") {
            Ok(answered) => answered,
            Err(failure) => {
                let why = failure.value(py).str()?.extract::<String>()?;
                let _ = log(py, format!("{entry}/{key} was not taken over: {why}"));
                Self::abandon(slf, py, &why)?;
                return Self::finish(slf, py);
            }
        };
        let held = answered.cast::<Taken>()?.get().held();
        let Some(held) = held else {
            slf.get().node.remove(&entry, &key);
            Self::refuse_waiting(slf, py, Outcome::missing, "the key was not started")?;
            return Self::finish(slf, py);
        };
        {
            let mut state = slf.get().held();
            state.pages = held.pages;
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
                Self::relinquish(slf, py)?;
                return Self::finish(slf, py);
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
            let _ = log(
                py,
                format!("{entry}/{key} became {named}, which this node does not have"),
            );
            Self::abandon(slf, py, &format!("this node does not have {named}"))?;
            return Self::finish(slf, py);
        };
        slf.get().held().behavior = behavior;
        Self::resume(slf, py)
    }

    fn attempt(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let behavior = {
            let mut state = slf.get().held();
            state.switching = false;
            state.read = false;
            state.behavior.clone_ref(py)
        };
        if let Some(native) = behavior.definition().native.clone() {
            return Self::stepping(slf, py, &native);
        }
        let context = Bound::new(
            py,
            Context::new(slf.clone().unbind(), slf.get().node.clone()),
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
                step: Which::Done,
            },
        )?;
        task.call_method1("add_done_callback", (done,))?;
        slf.get().held().task = Some(task.unbind());
        Ok(())
    }

    fn done(slf: &Bound<'_, Self>, py: Python<'_>, task: &Bound<'_, PyAny>) -> PyResult<()> {
        slf.get().held().task = None;
        // A body the activation cancelled is not a body that failed: `become` and `release` end it that way.
        let failure = if task.call_method0("cancelled")?.is_truthy()? {
            None
        } else {
            let raised = task.call_method0("exception")?;
            if raised.is_none() { None } else { Some(raised) }
        };
        if slf.get().held().ending() {
            Self::relinquish(slf, py)?;
            return Self::finish(slf, py);
        }
        if let Some(failure) = failure {
            let delay = Self::failed(slf, py, &failure)?;
            let retry = Bound::new(
                py,
                Step {
                    activation: slf.clone().unbind(),
                    step: Which::Retry,
                },
            )?;
            slf.get().node.later(py, delay, retry.into_any())?;
            return Ok(());
        }
        let entry = slf.get().entry.clone();
        let (switching, moved, again) = {
            let mut state = slf.get().held();
            state.current = None;
            let moved = state.named(&entry) != state.behavior.definition().name;
            (state.switching, moved, state.read && !state.mailbox.empty())
        };
        if switching && moved {
            let named = slf.get().held().named(&entry);
            return Self::successor(slf, py, &named);
        }
        if switching || again {
            return Self::attempt(slf, py);
        }
        Self::deactivate(slf, py)
    }

    /// Restart the body from the saved state, telling the `ask` that was in flight what ended it.
    fn failed(slf: &Bound<'_, Self>, py: Python<'_>, failure: &Bound<'_, PyAny>) -> PyResult<f64> {
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let (current, delay) = {
            let mut state = slf.get().held();
            state.failures += 1;
            let settings = slf.get().node.settings;
            let grown = settings.backoff_first
                * settings
                    .backoff_factor
                    .powi(i32::try_from(state.failures - 1).unwrap_or(i32::MAX));
            (state.current.take(), grown.min(settings.backoff_limit))
        };
        let _ = log(
            py,
            format!("{entry}/{key} failed and restarts from the saved state: {failure}"),
        );
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
    fn deactivate(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        slf.get().node.remove(&entry, &key);
        Self::refuse_waiting(
            slf,
            py,
            Outcome::unreached,
            "the body ended without reading it",
        )?;
        let pages = slf.get().held().pages.clone();
        let released = slf.get().node.commit(py, &entry, &key, pages, false)?;
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
    fn released(slf: &Bound<'_, Self>, py: Python<'_>, written: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Err(failure) = written.call_method0("result")
            && !failure.is_instance_of::<Fencing>(py)
        {
            let entry = slf.get().entry.clone();
            let key = slf.get().key.clone();
            let why = failure.value(py).str()?.extract::<String>()?;
            let _ = log(py, format!("{entry}/{key} kept the active mark: {why}"));
        }
        Self::finish(slf, py)
    }

    /// Give the key up: what was in flight is refused and what is queued goes back to routing.
    fn relinquish(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        let draining = slf.get().held().draining;
        let why = if draining {
            "the node is shutting down"
        } else {
            "the key moved to another node"
        };
        slf.get().node.remove(&entry, &key);
        let current = slf.get().held().current.take();
        if let Some(current) = current {
            Self::answer(slf, py, &current, Outcome::unreached, why)?;
        }
        loop {
            let waiting = slf.get().held().mailbox.take();
            let Some(deliver) = waiting else { break };
            slf.get().node.hand(py, Command::Deliver(deliver))?;
        }
        Ok(())
    }

    /// Drop the activation without holding the key, telling whoever waits that nothing was done.
    fn abandon(slf: &Bound<'_, Self>, py: Python<'_>, why: &str) -> PyResult<()> {
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        slf.get().node.remove(&entry, &key);
        Self::refuse_waiting(slf, py, Outcome::unreached, why)
    }

    fn refuse_waiting(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        outcome: fn(&str, &str) -> Outcome,
        why: &str,
    ) -> PyResult<()> {
        loop {
            let waiting = slf.get().held().mailbox.take();
            let Some(deliver) = waiting else {
                return Ok(());
            };
            Self::answer(slf, py, &deliver, outcome, why)?;
        }
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
                dropped(py, &deliver.actor, &deliver.key, why);
                Ok(())
            }
            Some(reply) => slf
                .get()
                .node
                .answer(py, reply, &outcome(&deliver.actor, &deliver.key)),
        }
    }

    fn finish(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        {
            let mut state = slf.get().held();
            if state.finished {
                return Ok(());
            }
            state.finished = true;
        }
        slf.get().node.ended(py)
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
                let queued = slf.get().held().mailbox.put(deliver.clone());
                if !queued {
                    return Self::answer(slf, py, &deliver, Outcome::full, "the mailbox is full");
                }
                Self::wake(slf, py)
            }
        }
    }

    /// End the activation because the key is not this node's any more.
    pub fn release(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let (task, native, waiting) = {
            let mut state = slf.get().held();
            state.fenced = true;
            (
                state.task.as_ref().map(|task| task.clone_ref(py)),
                state.behavior.definition().native.is_some(),
                state.awaiting.is_some() || state.writing.is_some(),
            )
        };
        if let Some(task) = task {
            return task.bind(py).call_method0("cancel").map(|_| ());
        }
        // A native body has no task to cancel: its loop is what ends it, and this takes the loop up. One that is
        // waiting for a write or for an answer ends on the callback of what it is waiting for.
        if native && !waiting {
            return Self::again(slf, py);
        }
        Ok(())
    }

    /// End the activation after the message it is processing: this node is shutting down.
    pub fn drain(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        slf.get().held().draining = true;
        Self::wake(slf, py)
    }

    /// Hand a waiting read the message that just arrived, or end it because nothing else will.
    ///
    /// A read whose deadline has passed ends even though a message is there: the body stops reading at `idle_after`,
    /// and what arrived after that waits in the mailbox for the body that runs next.
    fn wake(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        // A native body is not parked on a future: it is resting on the idle timer, and this takes it up again.
        if slf.get().held().behavior.definition().native.is_some() {
            return Self::stirred(slf, py);
        }
        let expired = match slf.get().held().deadline {
            None => false,
            Some(deadline) => slf.get().node.now(py)? >= deadline,
        };
        let (waiting, idle, deliver, draining) = {
            let mut state = slf.get().held();
            let Some(waiting) = state.waiting.take() else {
                return Ok(());
            };
            let draining = state.draining;
            let deliver = if expired { None } else { state.mailbox.take() };
            if deliver.is_none() && !draining && !expired {
                state.waiting = Some(waiting);
                return Ok(());
            }
            let idle = state.idle.take();
            state.deadline = None;
            if let Some(deliver) = &deliver {
                state.current = Some(deliver.clone());
            }
            (waiting, idle, deliver, draining)
        };
        if let Some(idle) = idle {
            idle.bind(py).call_method0("cancel")?;
        }
        let waiting = waiting.bind(py);
        if waiting.call_method0("done")?.is_truthy()? {
            return Ok(());
        }
        let _ = draining;
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
        let behavior = slf.get().held().behavior.clone_ref(py);
        let schema = behavior.definition().messages.bind(py);
        Ok(Schema::read(
            schema,
            schema.get().tree().sent(),
            &deliver.message,
            Some(&slf.get().node),
        )?)
    }

    /// One read of `inbox` or of `merge`: the next message, or what ends the reading.
    pub fn read<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        source: Option<&Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let switching = {
            let mut state = slf.get().held();
            state.read = true;
            if state.current.is_some() {
                state.current = None;
                state.failures = 0;
            }
            state.switching
        };
        if switching {
            let task = slf
                .get()
                .held()
                .task
                .as_ref()
                .map(|task| task.clone_ref(py));
            if let Some(task) = task {
                task.bind(py).call_method0("cancel")?;
            }
            // The cancellation lands on this await, which is where the body ends and the next behavior takes over.
            return slf.get().node.future(py);
        }
        if slf.get().held().draining {
            return Err(ended());
        }
        let deliver = slf.get().held().mailbox.take();
        if let Some(deliver) = deliver {
            slf.get().held().current = Some(deliver.clone());
            let msg = Self::opened(slf, py, &deliver)?;
            let answer = slf.get().node.future(py)?;
            answer.call_method1("set_result", (msg,))?;
            return Ok(answer);
        }
        if let Some(source) = source {
            return Self::merging(slf, py, source);
        }
        let answer = slf.get().node.future(py)?;
        let idle = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Idle,
            },
        )?;
        let idle_after = slf.get().node.settings.idle_after;
        let timer = slf.get().node.later(py, idle_after, idle.into_any())?;
        let deadline = slf.get().node.now(py)? + idle_after;
        {
            let mut state = slf.get().held();
            state.waiting = Some(answer.clone().unbind());
            state.idle = Some(timer.unbind());
            state.deadline = Some(deadline);
        }
        Ok(answer)
    }

    /// A read that races the mailbox with the source the body brought. Idleness does not end it.
    fn merging<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        source: &Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pending = slf
            .get()
            .held()
            .item
            .as_ref()
            .map(|task| task.clone_ref(py));
        if let Some(pending) = pending {
            let pending = pending.bind(py);
            if pending.call_method0("done")?.is_truthy()? {
                slf.get().held().item = None;
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
                    step: Which::Item,
                },
            )?;
            task.call_method1("add_done_callback", (ready,))?;
            slf.get().held().item = Some(task.unbind());
        }
        let answer = slf.get().node.future(py)?;
        slf.get().held().waiting = Some(answer.clone().unbind());
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

    /// The source produced an item while a read was waiting for one.
    fn item(slf: &Bound<'_, Self>, py: Python<'_>, task: &Bound<'_, PyAny>) -> PyResult<()> {
        let waiting = slf.get().held().waiting.take();
        let Some(waiting) = waiting else {
            return Ok(());
        };
        slf.get().held().item = None;
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

    /// Nothing arrived for `idle_after`, so the reading of `inbox` ends and the body returns.
    fn idle(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let waiting = slf.get().held().waiting.take();
        {
            let mut state = slf.get().held();
            state.idle = None;
            state.deadline = None;
        }
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
        let commit = slf
            .get()
            .node
            .commit(py, &entry, &key, pages.clone(), true)?;
        let then = Bound::new(
            py,
            Wrote {
                activation: slf.clone().unbind(),
                written: written.clone().unbind(),
                pages: Mutex::new(Some(pages)),
                value: Mutex::new(value),
                switching,
                yields,
            },
        )?;
        commit.call_method1("add_done_callback", (then,))?;
        Ok(written)
    }
}

/// Which step of an activation a callback of the loop runs.
#[derive(Debug, Clone, Copy)]
enum Which {
    Begin,
    Held,
    Released,
    Done,
    Retry,
    Idle,
    Item,
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
            Which::Done => Activation::done(activation, py, &args.get_item(0)?),
            Which::Retry => Activation::attempt(activation, py),
            Which::Idle => Activation::idle(activation, py),
            Which::Item => Activation::item(activation, py, &args.get_item(0)?),
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
}

#[pymethods]
impl Wrote {
    fn __call__(&self, py: Python<'_>, commit: &Bound<'_, PyAny>) -> PyResult<()> {
        let activation = self.activation.bind(py);
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
        let stored = self
            .value
            .lock()
            .expect("the write lock is never poisoned")
            .take();
        let result = match (&stored, self.yields) {
            (Some(value), true) => value.clone_ref(py),
            _ => py.None(),
        };
        {
            let mut state = activation.get().held();
            if let Some(pages) = self
                .pages
                .lock()
                .expect("the write lock is never poisoned")
                .take()
            {
                state.pages = pages;
            }
            if let Some(value) = stored {
                state.value = Some(value);
            }
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
            if slf.get().held().ending() {
                Self::relinquish(slf, py)?;
                return Self::finish(slf, py);
            }
            slf.get().held().resting = false;
            let deliver = slf.get().held().mailbox.take();
            let Some(deliver) = deliver else {
                return Self::resting(slf, py, native);
            };
            slf.get().held().current = Some(deliver.clone());
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
        let Some(pages) = turn.save else {
            Self::answer_all(slf, py, turn.replies)?;
            slf.get().held().current = None;
            return Ok(true);
        };
        let entry = slf.get().entry.clone();
        let key = slf.get().key.clone();
        // The state changes when the write lands, not before: a body that went on from a write that did not happen
        // would be running on a state no replica has.
        let written = match slf.get().node.commit(py, &entry, &key, pages.clone(), true) {
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
        let (replies, pages) = {
            let mut state = slf.get().held();
            (core::mem::take(&mut state.pending), state.writing.take())
        };
        let Err(failure) = written.call_method0("result") else {
            if let Some(pages) = pages {
                slf.get().held().pages = pages;
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
        let _ = log(
            py,
            format!("{entry}/{key} did not take the message: {failure}"),
        );
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
        super::armed(py, node, id, &answer, node.settings.ask_timeout)?;
        let then = Bound::new(
            py,
            Step {
                activation: slf.clone().unbind(),
                step: Which::Answered,
            },
        )?;
        answer.call_method1("add_done_callback", (then,))?;
        slf.get().held().awaiting = Some((message, id));
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
        let turn = {
            let pages = slf.get().held().pages.clone();
            let _ = &held;
            native.step(
                &pages,
                &Given::Answered {
                    message: &message,
                    answer: &[],
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
        let idle_after = slf.get().node.settings.idle_after;
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
