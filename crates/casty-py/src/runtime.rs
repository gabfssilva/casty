//! The threads the transport of a system runs on, which systems of one process may share.
//!
//! A system given no `Runtime` makes its own, a thread per core, and it ends with the system. Given one, the system
//! runs its transport there beside the others, and what ends with it is only what is its own: the node, its listener
//! and its connections. The runtime goes when its last holder lets it go.

use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// A tokio runtime, given up without waiting when its last holder lets it go.
#[derive(Debug)]
pub struct Threads {
    runtime: Option<tokio::runtime::Runtime>,
    handle: tokio::runtime::Handle,
}

impl Threads {
    /// A runtime of `workers` threads, or of one per core. Always of several threads: a dial that asks the loop where
    /// an address goes waits in place (`block_in_place`), which a runtime of one thread refuses.
    pub fn start(workers: Option<usize>) -> PyResult<Arc<Self>> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        if let Some(workers) = workers {
            builder.worker_threads(workers);
        }
        let runtime = builder.build().map_err(|failed| {
            PyRuntimeError::new_err(format!("the transport did not start: {failed}"))
        })?;
        Ok(Arc::new(Self {
            handle: runtime.handle().clone(),
            runtime: Some(runtime),
        }))
    }

    #[must_use]
    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
}

impl Drop for Threads {
    // The last holder can be a task on one of these threads, where a runtime dropped panics and one given up does not,
    // and what runs on them may be waiting for the loop that is letting them go.
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// Threads for the transport of the systems and clients given it.
///
/// Without one, each `ActorSystem` in a cluster and each `Client` starts a thread per core of its own. Given the same
/// `Runtime`, they share its threads, and a system that ends stops only its node, its listener and its connections;
/// the threads go when nothing holds the runtime any more.
///
/// Parameters
/// ----------
/// threads
///     How many threads carry the transport. Zero raises `ValueError`.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Runtime {
    threads: Arc<Threads>,
}

impl Runtime {
    #[must_use]
    pub fn threads(&self) -> Arc<Threads> {
        Arc::clone(&self.threads)
    }
}

#[pymethods]
impl Runtime {
    #[new]
    #[pyo3(signature = (*, threads))]
    fn new(threads: usize) -> PyResult<Self> {
        if threads == 0 {
            return Err(PyValueError::new_err(
                "threads is 0, and the transport needs at least one",
            ));
        }
        Ok(Self {
            threads: Threads::start(Some(threads))?,
        })
    }

    /// How many tasks are alive on these threads, for a test that looks at what a system left behind.
    fn _tasks(&self) -> usize {
        self.threads.handle.metrics().num_alive_tasks()
    }
}
