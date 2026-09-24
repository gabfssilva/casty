//! Rust closures as the callables the event loop takes: `call_soon`, `call_later` and `add_done_callback`.
//!
//! Each closure is handed to one of those once, and the loop calls it once. It moves out what it captured when it
//! runs, so a second call would find nothing and do nothing.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::lock::Locked;

type Call = Box<dyn FnOnce(Python<'_>, &Bound<'_, PyTuple>) -> PyResult<()> + Send>;

/// A closure the loop calls, with the arguments it passes.
#[pyclass(frozen, module = "casty._casty")]
pub struct Callback(Mutex<Option<Call>>);

impl core::fmt::Debug for Callback {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("Callback").finish_non_exhaustive()
    }
}

impl Callback {
    fn new(
        py: Python<'_>,
        call: impl FnOnce(Python<'_>, &Bound<'_, PyTuple>) -> PyResult<()> + Send + 'static,
    ) -> PyResult<Bound<'_, Self>> {
        Bound::new(py, Self(Mutex::new(Some(Box::new(call)))))
    }

    /// A closure the loop calls with no argument.
    fn bare(
        py: Python<'_>,
        call: impl FnOnce(Python<'_>) -> PyResult<()> + Send + 'static,
    ) -> PyResult<Bound<'_, Self>> {
        Self::new(py, move |py, _| call(py))
    }
}

#[pymethods]
impl Callback {
    #[pyo3(signature = (*args))]
    fn __call__(&self, py: Python<'_>, args: &Bound<'_, PyTuple>) -> PyResult<()> {
        let Some(call) = self.0.locked().take() else {
            return Ok(());
        };
        call(py, args)
    }
}

/// Call `call` on `running_loop`, after the step this is part of.
pub fn soon(
    running_loop: &Bound<'_, PyAny>,
    call: impl FnOnce(Python<'_>) -> PyResult<()> + Send + 'static,
) -> PyResult<()> {
    let callback = Callback::bare(running_loop.py(), call)?;
    running_loop.call_method1("call_soon", (callback,))?;
    Ok(())
}

/// Call `call` on `running_loop` `delay` seconds from now, giving back the timer that cancels it.
pub fn later<'py>(
    running_loop: &Bound<'py, PyAny>,
    delay: f64,
    call: impl FnOnce(Python<'_>) -> PyResult<()> + Send + 'static,
) -> PyResult<Bound<'py, PyAny>> {
    let callback = Callback::bare(running_loop.py(), call)?;
    running_loop.call_method1("call_later", (delay, callback))
}

/// Call `call` with `future` once it is done.
pub fn when_done(
    future: &Bound<'_, PyAny>,
    call: impl FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<()> + Send + 'static,
) -> PyResult<()> {
    let callback = Callback::new(future.py(), move |py, args| call(py, &args.get_item(0)?))?;
    future.call_method1("add_done_callback", (callback,))?;
    Ok(())
}
