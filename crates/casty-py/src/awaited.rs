//! What the core gives back where the API says `async def`.
//!
//! A future is awaitable, but `asyncio.create_task` takes coroutines only, and a caller is free to start an `ask`
//! that way. This is a coroutine in the eyes of asyncio: it delegates every step to the future it stands for.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyModule, PyTuple};

/// One awaited value, as the coroutine protocol asyncio drives.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Awaited {
    future: Py<PyAny>,
    steps: Mutex<Option<Py<PyAny>>>,
}

impl Awaited {
    #[must_use]
    pub fn of(future: Bound<'_, PyAny>) -> Self {
        Self {
            future: future.unbind(),
            steps: Mutex::new(None),
        }
    }

    /// The generator of the future, made on the first step and driven from then on.
    fn steps<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut held = self
            .steps
            .lock()
            .expect("the coroutine lock is never poisoned");
        if let Some(steps) = &*held {
            return Ok(steps.bind(py).clone());
        }
        let steps = self.future.bind(py).call_method0("__await__")?;
        *held = Some(steps.clone().unbind());
        Ok(steps)
    }

    fn started(&self) -> bool {
        self.steps
            .lock()
            .expect("the coroutine lock is never poisoned")
            .is_some()
    }
}

#[pymethods]
impl Awaited {
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.steps(py)
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.steps(py)
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
        self.steps(py)?.call_method1("throw", raised)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        if !self.started() {
            self.future.bind(py).call_method0("cancel")?;
            return Ok(());
        }
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
