//! What the core gives back where the API says `async def`.
//!
//! A future is awaitable, but `asyncio.create_task` takes coroutines only, and a caller is free to start an `ask`
//! that way. This is a coroutine in the eyes of asyncio: it delegates every step to the future it stands for.

use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::types::{PyModule, PyTuple};

use crate::lock::Locked;
use crate::node::Node;

/// One awaited value, as the coroutine protocol asyncio drives.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Awaited {
    future: Py<PyAny>,
    steps: Mutex<Option<Py<PyAny>>>,
    /// The request this is the answer of, and the node that waits for it: nothing else waits for the future, so an
    /// awaiter that gives up cancels it and ends the request.
    request: Option<(Arc<Node>, i64)>,
}

impl Awaited {
    #[must_use]
    pub fn of(future: Bound<'_, PyAny>) -> Self {
        Self {
            future: future.unbind(),
            steps: Mutex::new(None),
            request: None,
        }
    }

    /// The answer of the request `id` of `node`, which only this waits for. Giving up on it is what tells the key the
    /// request went to that nobody waits any more, also when the caller is cancelled before its first step.
    #[must_use]
    pub fn answer(future: Bound<'_, PyAny>, node: &Arc<Node>, id: i64) -> Self {
        Self {
            request: Some((node.clone(), id)),
            ..Self::of(future)
        }
    }

    /// A task that is cancelled cancels the future it waits on before it throws in here, so a done future does not mean
    /// an answered request: the request itself says, and one that was answered is no longer waited for.
    fn abandon(&self, py: Python<'_>) -> PyResult<()> {
        let Some((node, id)) = &self.request else {
            return Ok(());
        };
        self.future.bind(py).call_method0("cancel")?;
        crate::node::abandoned(py, node, *id)
    }

    /// The generator of the future, made on the first step and driven from then on.
    fn steps<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut held = self.steps.locked();
        if let Some(steps) = &*held {
            return Ok(steps.bind(py).clone());
        }
        let steps = self.future.bind(py).call_method0("__await__")?;
        *held = Some(steps.clone().unbind());
        Ok(steps)
    }

    fn started(&self) -> bool {
        self.steps.locked().is_some()
    }
}

#[pymethods]
impl Awaited {
    /// Itself rather than the iterator of the future: an `await` that is cancelled throws into what `__await__` gave
    /// it, and only here does that end the request.
    fn __await__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __iter__(slf: Bound<'_, Self>) -> Bound<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.send(py, None)
    }

    #[pyo3(signature = (value = None))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        value: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let first = !self.started();
        let steps = self.steps(py)?;
        if first {
            return steps.call_method0("__next__");
        }
        steps.call_method1(
            "send",
            (value.cloned().unwrap_or_else(|| py.None().into_bound(py)),),
        )
    }

    #[pyo3(signature = (*raised))]
    fn throw<'py>(
        &self,
        py: Python<'py>,
        raised: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.abandon(py)?;
        self.steps(py)?.call_method1("throw", raised)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if !self.started() {
            self.abandon(py)?;
            self.future.bind(py).call_method0("cancel")?;
            return Ok(());
        }
        self.abandon(py)?;
        self.steps(py)?.call_method0("close")?;
        Ok(())
    }
}

/// Tell asyncio that this is a coroutine, so that `create_task` takes it.
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();
    py.import("collections.abc")?
        .getattr("Coroutine")?
        .call_method1("register", (py.get_type::<Awaited>(),))?;
    Ok(())
}
