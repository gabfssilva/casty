//! What the body of an activation receives, and the iterator it reads its messages from.

use core::time::Duration;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use super::Node;
use super::activation::Activation;
use crate::actor::{Definition, period};
use crate::refs::Ref;

/// The context of one run of the body of an activation: its key, its state, and the messages that reach it.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Context {
    activation: Py<Activation>,
    node: Arc<Node>,
    /// The run of the body this context was given to, which is who its reads take messages for.
    run: u64,
}

impl Context {
    #[must_use]
    pub fn new(activation: Py<Activation>, node: Arc<Node>, run: u64) -> Self {
        Self {
            activation,
            node,
            run,
        }
    }
}

#[pymethods]
impl Context {
    /// `Context[S]` is `Context[S, Never]`: a body that takes no message at all.
    #[classmethod]
    fn __class_getitem__<'py>(
        class: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut args = crate::generic::subscript(item);
        if args.len() == 1 {
            args.push(class.py().import("typing")?.getattr("Never")?);
        }
        crate::generic::aliased(class, &args)
    }

    #[getter]
    fn key(&self) -> String {
        self.activation.get().key().to_owned()
    }

    /// The state of the key: what was saved last, and the way to save another.
    #[getter]
    fn state(&self, py: Python<'_>) -> State {
        State {
            activation: self.activation.clone_ref(py),
        }
    }

    /// Messages in arrival order. Ends after `idle_after` without messages: the type's, or the system's.
    #[getter]
    fn inbox(&self, py: Python<'_>) -> Inbox {
        Inbox {
            activation: self.activation.clone_ref(py),
            run: self.run,
            source: None,
        }
    }

    /// Reference to this entity, to hand to other actors.
    #[getter]
    #[pyo3(name = "self")]
    fn itself(&self, py: Python<'_>) -> Ref {
        let activation = self.activation.bind(py).get();
        Ref::entity_of(
            activation.messages(py),
            activation.entry().to_owned(),
            activation.key().to_owned(),
            Some(self.node.clone()),
        )
    }

    /// The system of the node where this activation runs.
    #[getter]
    fn system(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.node.system(py)
    }

    /// Hand the key to another actor type, which runs it from the next read of `inbox` or `merge` on.
    #[pyo3(name = "become", signature = (behavior, state = None, /))]
    fn become_<'py>(
        &self,
        py: Python<'py>,
        behavior: &Bound<'py, PyAny>,
        state: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let definition = Definition::of(behavior)?;
        Activation::become_another(self.activation.bind(py), py, behavior, &definition, state)
    }

    /// Await `work` beside the body, and tell this entity what it gives, through `mapper` when there is one, or what
    /// `failed` makes of what it raised.
    #[pyo3(signature = (work, mapper = None, /, *, failed = None))]
    fn to_self(
        &self,
        py: Python<'_>,
        work: &Bound<'_, PyAny>,
        mapper: Option<Py<PyAny>>,
        failed: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let activation = self.activation.bind(py);
        Activation::handed(activation, py, self.run);
        let itself = Bound::new(py, self.itself(py))?.unbind();
        let actor = activation.get().entry().to_owned();
        let key = activation.get().key().to_owned();
        let node = self.node.clone();
        self.node.pipe(py, work, move |py, done| {
            let message = match (done, mapper, failed) {
                (Ok(value), Some(mapper), _) => mapper.bind(py).call1((value,)),
                (Ok(value), None, _) => Ok(value),
                (Err(error), _, Some(failed)) => failed.bind(py).call1((error,)),
                (Err(error), _, None) => {
                    node.dropped(py, &actor, &key, &raised(&error)?);
                    return Ok(());
                }
            };
            let told = message.and_then(|message| itself.bind(py).call_method1("tell", (message,)));
            if let Err(error) = told {
                node.dropped(py, &actor, &key, &error.value(py).to_string());
            }
            Ok(())
        })
    }

    /// Tell this entity `message` `delay` from now, and then every `interval`, while the key is active; once, without
    /// `interval`. It takes the place of the schedule named `name`, and resolves to it once the replicas have it.
    #[pyo3(signature = (name, delay, interval, message, /))]
    fn schedule<'py>(
        &self,
        py: Python<'py>,
        name: String,
        delay: &Bound<'py, PyAny>,
        interval: &Bound<'py, PyAny>,
        message: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let delay = period("delay", delay)?;
        let every = if interval.is_none() {
            None
        } else {
            let every = period("interval", interval)?;
            if every.is_zero() {
                return Err(PyValueError::new_err(format!(
                    "interval is {interval}, which is not positive"
                )));
            }
            Some(every)
        };
        Activation::schedule(self.activation.bind(py), py, name, delay, every, message)
    }

    /// The schedules of the key that are not over, by name, in the order they were made.
    #[getter]
    fn schedules<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let all = PyDict::new(py);
        for schedule in Activation::schedules(self.activation.bind(py), py) {
            let name = schedule.name.clone();
            all.set_item(name, Bound::new(py, schedule)?)?;
        }
        Ok(all)
    }

    /// Messages and items of `source` in arrival order, until `source` ends.
    fn merge(&self, py: Python<'_>, source: &Bound<'_, PyAny>) -> Inbox {
        let items = source
            .call_method0("__aiter__")
            .unwrap_or_else(|_| source.clone());
        Inbox {
            activation: self.activation.clone_ref(py),
            run: self.run,
            source: Some(items.unbind()),
        }
    }
}

/// The state of one activation, as the body reads and writes it.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct State {
    activation: Py<Activation>,
}

#[pymethods]
impl State {
    #[classmethod]
    fn __class_getitem__<'py>(
        class: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::generic::alias(class, item)
    }

    /// The last saved state.
    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.activation.bind(py).get().value(py)
    }

    /// Store `state` on the replicas and return once the type's write level confirms it.
    fn set<'py>(&self, py: Python<'py>, state: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        Activation::save(self.activation.bind(py), py, state)
    }

    /// Store what `change` makes of the last saved state, and answer it once it is confirmed.
    fn update<'py>(
        &self,
        py: Python<'py>,
        change: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        Activation::update(self.activation.bind(py), py, change)
    }

    /// Delete the state from the replicas and return once the type's write level confirms it.
    fn delete<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Activation::delete(self.activation.bind(py), py)
    }
}

/// A message a key tells itself at a time, once or on an interval, which its replicas keep.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Schedule {
    activation: Py<Activation>,
    /// Which of the schedules of that name this is: the one made last takes the place of the others.
    id: u64,
    name: String,
    message: Py<PyAny>,
    interval: Option<Duration>,
}

impl Schedule {
    #[must_use]
    pub fn new(
        activation: Py<Activation>,
        id: u64,
        name: String,
        message: Py<PyAny>,
        interval: Option<Duration>,
    ) -> Self {
        Self {
            activation,
            id,
            name,
            message,
            interval,
        }
    }
}

#[pymethods]
impl Schedule {
    #[classmethod]
    fn __class_getitem__<'py>(
        class: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::generic::alias(class, item)
    }

    /// What the key calls it: no two of its schedules share a name.
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    /// The message it tells the key.
    #[getter]
    fn message(&self, py: Python<'_>) -> Py<PyAny> {
        self.message.clone_ref(py)
    }

    /// How long after each time it goes off it goes off again. `None` for one that goes off once.
    #[getter]
    fn interval(&self) -> Option<Duration> {
        self.interval
    }

    /// When it goes off next, or `None` once it is over or another of its name took its place.
    #[getter]
    fn due<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let Some(due) = self.activation.get().due(self.id) else {
            return Ok(None);
        };
        let datetime = py.import("datetime")?;
        let utc = datetime.getattr("timezone")?.getattr("utc")?;
        let epoch = datetime
            .getattr("datetime")?
            .call_method1("fromtimestamp", (0, utc))?;
        let since = datetime.getattr("timedelta")?.call1((0, 0, due))?;
        epoch.add(since).map(Some)
    }

    /// Stop it, and return once the replicas no longer have it.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Activation::unschedule(self.activation.bind(py), py, self.id)
    }
}

/// The messages of an activation, on their own or merged with a source the body brought.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Inbox {
    activation: Py<Activation>,
    run: u64,
    source: Option<Py<PyAny>>,
}

#[pymethods]
impl Inbox {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Activation::read(self.activation.bind(py), py, self.run, self.source.as_ref())
    }
}

/// Why work handed to `to_self` without `failed` sent nothing: the class of what it raised, and its text.
fn raised(error: &Bound<'_, PyAny>) -> PyResult<String> {
    let class: String = error.get_type().getattr("__name__")?.extract()?;
    let text: String = error.str()?.extract()?;
    Ok(if text.is_empty() {
        format!("the work handed to to_self raised {class}")
    } else {
        format!("the work handed to to_self raised {class}: {text}")
    })
}

/// What a read ends with when nothing else will arrive.
#[must_use]
pub fn ended() -> PyErr {
    PyStopAsyncIteration::new_err(())
}

#[must_use]
pub fn stopped() -> PyErr {
    PyRuntimeError::new_err("the system has stopped")
}
