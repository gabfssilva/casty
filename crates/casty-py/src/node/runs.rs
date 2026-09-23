//! Which run of a body the code running now works for.
//!
//! `Ref.ask` and `Ref.tell` are not methods of the context, so nothing they are handed says that they are called from
//! inside a body. The task does: a run is known here by the task that read its last message, or by its own task until
//! it has read one. That is the task that holds the message, and what it awaits holds up the key. A task the body
//! starts beside it is not known here: what it asks keeps nobody waiting as far as the chain says, because the run may
//! be reading its next message meanwhile.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use super::activation::Activation;
use crate::lock::Locked;

/// The task that reads for one run of a body, kept alive while it is here so that its address names nothing else.
#[derive(Debug)]
struct Reader {
    task: Py<PyAny>,
    activation: Py<Activation>,
    run: u64,
}

pub struct Runs {
    readers: Mutex<HashMap<usize, Reader>>,
    current_task: PyOnceLock<Py<PyAny>>,
    /// The number of the next stretch of a body, from one read to the next. It starts anywhere, so that a stretch of
    /// one node is not taken for a stretch of another that runs the key after it.
    holds: AtomicU64,
}

impl core::fmt::Debug for Runs {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Runs")
            .field("readers", &self.readers)
            .field("holds", &self.holds)
            .finish_non_exhaustive()
    }
}

impl Default for Runs {
    fn default() -> Self {
        Self {
            readers: Mutex::new(HashMap::new()),
            current_task: PyOnceLock::new(),
            holds: AtomicU64::new(RandomState::new().hash_one(0_u8)),
        }
    }
}

impl Runs {
    /// A number for a new stretch of a body.
    #[must_use]
    pub fn hold(&self) -> u64 {
        self.holds.fetch_add(1, Ordering::Relaxed)
    }

    /// The task running now, if there is one: code called from a callback of the loop, or from another thread, has
    /// none.
    #[must_use]
    pub fn current<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyAny>> {
        let current_task = self
            .current_task
            .get_or_try_init(py, || {
                Ok::<_, PyErr>(py.import("asyncio")?.getattr("current_task")?.unbind())
            })
            .ok()?;
        current_task
            .bind(py)
            .call0()
            .ok()
            .filter(|task| !task.is_none())
    }

    /// `task` reads for the run `run` of `activation` from now on.
    pub fn enter(&self, task: &Bound<'_, PyAny>, activation: &Bound<'_, Activation>, run: u64) {
        let reader = Reader {
            task: task.clone().unbind(),
            activation: activation.clone().unbind(),
            run,
        };
        let replaced = self.held().insert(task.as_ptr() as usize, reader);
        // Released without the lock: letting go of an object can run a finalizer, which can reach back in here.
        drop(replaced);
    }

    /// `task` reads for no run any more.
    pub fn leave(&self, task: &Bound<'_, PyAny>) {
        let left = {
            let mut readers = self.held();
            let at = task.as_ptr() as usize;
            let same = readers
                .get(&at)
                .is_some_and(|reader| reader.task.bind(task.py()).is(task));
            if same { readers.remove(&at) } else { None }
        };
        drop(left);
    }

    /// The run the task running now reads for.
    #[must_use]
    pub fn running(&self, py: Python<'_>) -> Option<(Py<Activation>, u64)> {
        if self.held().is_empty() {
            return None;
        }
        let task = self.current(py)?;
        let readers = self.held();
        let reader = readers.get(&(task.as_ptr() as usize))?;
        reader
            .task
            .bind(py)
            .is(&task)
            .then(|| (reader.activation.clone_ref(py), reader.run))
    }

    fn held(&self) -> std::sync::MutexGuard<'_, HashMap<usize, Reader>> {
        self.readers.locked()
    }
}
