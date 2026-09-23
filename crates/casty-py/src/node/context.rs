//! What the body of an activation receives, and the iterator it reads its messages from.

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use pyo3::types::PyType;

use super::Node;
use super::activation::Activation;
use crate::actor::Definition;
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

/// What a read ends with when nothing else will arrive.
#[must_use]
pub fn ended() -> PyErr {
    PyStopAsyncIteration::new_err(())
}

#[must_use]
pub fn stopped() -> PyErr {
    PyRuntimeError::new_err("the system has stopped")
}
