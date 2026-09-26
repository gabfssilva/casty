//! `casty.stores.SQL`: the store of the durable types over a SQL database, which the node of a cluster reaches without
//! the loop.
//!
//! The store keeps its connections on a thread of its own, from `__aenter__` to `__aexit__`. To Python it is a
//! `casty.Store`: `load`, `save` and `drop` run on that thread and answer on the loop the store was entered on, through
//! an inbox, and that is how a system running alone calls it. The node of a cluster calls it from its task instead
//! (`carry`) and hears the answer on its channel, so no call of a cluster reaches the loop or the interpreter.
//!
//! A call holds the thread until it is done: the store can be left while calls are under way, and they end first.

use core::future::Future;
use core::time::Duration;
use std::sync::{Arc, Mutex};

use casty_core::store::{Storage, Stored, version};
use casty_node::replication::service::StoreAnswer;
use casty_store::{Error, Record};
use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

use crate::awaited::Awaited;
use crate::lock::Locked;
use crate::node::inbox::Inbox;
use crate::node::storage::{late, unreadable};
use crate::runtime::Threads;

/// Why a store that was not entered, or was left, does nothing.
const NOT_OPEN: &str = "the store is not open: use it inside async with";

/// A store while it is open: its connections, the thread they live on, and the loop it answers Python on.
#[derive(Debug)]
struct Open {
    sql: Arc<casty_store::Sql>,
    threads: Arc<Threads>,
    inbox: Inbox,
    running: Py<PyAny>,
}

/// `casty.Store` over a SQL database: PostgreSQL, MySQL, MariaDB, SQLite, and the databases that speak the protocol of
/// one of them.
///
/// The records are the rows of one table, `casty_records`, which the store makes when the database has none. A row is
/// found by a digest of its actor and its key, so no collation, limit on the length of a key or reserved word of the
/// database changes which row a key has. A save keeps its record only when its version is greater than the version of
/// the record kept, compared as bytes.
///
/// The store is used inside `async with`, which connects on entering and closes the connections on leaving, once the
/// calls under way are done. The calls run on a thread of the store, and each answers on the event loop the store was
/// entered on. The nodes of a cluster call it from their own threads, without the event loop.
///
/// Entering raises `ValueError` for a URL that names no database the store knows, and `ConnectionError` when the
/// database cannot be reached or refuses the table. A call that fails raises `Unavailable`, and one made outside
/// `async with` raises `RuntimeError`.
///
/// Parameters
/// ----------
/// url
///     The database, by the scheme of its driver: `postgres://user:password@host/database`,
///     `mysql://user:password@host/database`, or `sqlite://path?mode=rwc` for a file made when it does not exist.
///     Not `sqlite::memory:`: each connection of the store would open a database of its own.
#[pyclass(frozen, module = "casty._casty", name = "SQL")]
pub struct Sql {
    url: String,
    open: Mutex<Option<Open>>,
}

impl core::fmt::Debug for Sql {
    // Without the URL, which may carry a password.
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("Sql").finish_non_exhaustive()
    }
}

impl Sql {
    /// Carry `storage` of `(actor, key)` out for the node of a cluster, from its task: `then` takes what the database
    /// answered within `within`, or the failure that says why it did not, on the thread of the store.
    pub fn carry(
        &self,
        actor: String,
        key: String,
        storage: Storage,
        within: Duration,
        then: impl FnOnce(StoreAnswer) + Send + 'static,
    ) {
        let open = self
            .open
            .locked()
            .as_ref()
            .map(|open| (Arc::clone(&open.sql), Arc::clone(&open.threads)));
        let Some((sql, threads)) = open else {
            then(Err(NOT_OPEN.to_owned()));
            return;
        };
        let handle = threads.handle().clone();
        handle.spawn(async move {
            let carried = tokio::time::timeout(within, carried(&sql, &actor, &key, storage)).await;
            then(carried.unwrap_or_else(|_| Err(late(within.as_secs_f64()))));
            drop(threads);
        });
    }

    /// Run `operation` on the thread of the store, and resolve the future this returns with what `answered` makes of
    /// its result, on the loop.
    fn call<'py, T, Fut>(
        &self,
        py: Python<'py>,
        operation: impl FnOnce(Arc<casty_store::Sql>) -> Fut,
        answered: fn(Python<'_>, T) -> PyResult<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, Error>> + Send + 'static,
    {
        let (sql, threads, inbox, running) = {
            let open = self.open.locked();
            let open = open
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err(NOT_OPEN))?;
            (
                Arc::clone(&open.sql),
                Arc::clone(&open.threads),
                open.inbox.clone(),
                open.running.clone_ref(py),
            )
        };
        let current = py.import("asyncio")?.call_method0("get_running_loop")?;
        if !current.is(running.bind(py)) {
            return Err(PyRuntimeError::new_err(
                "the store answers on the event loop it was entered on, and this is another one",
            ));
        }
        let future = current.call_method0("create_future")?;
        let answer = future.clone().unbind();
        let operation = operation(sql);
        let handle = threads.handle().clone();
        handle.spawn(async move {
            let done = operation.await;
            inbox.send(move |py| {
                let answer = answer.bind(py);
                if answer.call_method0("done")?.is_truthy()? {
                    return Ok(());
                }
                match done {
                    Ok(value) => answer.call_method1("set_result", (answered(py, value)?,))?,
                    Err(failed) => answer.call_method1(
                        "set_exception",
                        (crate::errors::Unavailable::new_err(failed.to_string()).into_value(py),),
                    )?,
                };
                Ok(())
            });
            drop(threads);
        });
        Ok(Bound::new(py, Awaited::of(future))?.into_any())
    }
}

#[pymethods]
impl Sql {
    #[new]
    #[pyo3(signature = (url, /))]
    fn new(url: String) -> Self {
        Self {
            url,
            open: Mutex::new(None),
        }
    }

    fn __aenter__<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if slf.get().open.locked().is_some() {
            return Err(PyRuntimeError::new_err("the store is already open"));
        }
        let running = py.import("asyncio")?.call_method0("get_running_loop")?;
        let entered = running.call_method0("create_future")?;
        let inbox = Inbox::open(py, &running)?;
        let threads = Threads::start(Some(1))?;
        let url = slf.get().url.clone();
        let (store, answer, running) = (
            slf.clone().unbind(),
            entered.clone().unbind(),
            running.unbind(),
        );
        let held = Arc::clone(&threads);
        threads.handle().spawn(async move {
            let opened = casty_store::Sql::open(&url).await;
            let reply = inbox.clone();
            reply.send(move |py| {
                let (store, answer) = (store.bind(py), answer.bind(py));
                let sql = match opened {
                    Ok(sql) => sql,
                    Err(failed) => {
                        answer.call_method1("set_exception", (unopened(py, &failed),))?;
                        return Ok(());
                    }
                };
                if answer.call_method0("done")?.is_truthy()? {
                    // Whoever entered gave up waiting, and nothing will leave the store.
                    held.handle().clone().spawn(async move {
                        sql.close().await;
                        drop(held);
                    });
                    return Ok(());
                }
                *store.get().open.locked() = Some(Open {
                    sql: Arc::new(sql),
                    threads: held,
                    inbox,
                    running,
                });
                answer.call_method1("set_result", (store,))?;
                Ok(())
            });
        });
        Ok(Bound::new(py, Awaited::of(entered))?.into_any())
    }

    #[pyo3(signature = (*_exc))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let left = py
            .import("asyncio")?
            .call_method0("get_running_loop")?
            .call_method0("create_future")?;
        let Some(Open {
            sql,
            threads,
            inbox,
            running: _,
        }) = self.open.locked().take()
        else {
            left.call_method1("set_result", (py.None(),))?;
            return Ok(Bound::new(py, Awaited::of(left))?.into_any());
        };
        let answer = left.clone().unbind();
        let handle = threads.handle().clone();
        handle.spawn(async move {
            sql.close().await;
            inbox.send(move |py| {
                let answer = answer.bind(py);
                if !answer.call_method0("done")?.is_truthy()? {
                    answer.call_method1("set_result", (py.None(),))?;
                }
                Ok(())
            });
            drop(threads);
        });
        Ok(Bound::new(py, Awaited::of(left))?.into_any())
    }

    /// The record of `(actor, key)` as `(version, state)`, or `None` when there is none.
    #[pyo3(signature = (actor, key, /))]
    fn load<'py>(
        &self,
        py: Python<'py>,
        actor: String,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.call(
            py,
            move |sql| async move { sql.load(&actor, &key).await },
            record,
        )
    }

    /// Keep `(version, state)` as the record of `(actor, key)`, unless its record has a greater version.
    #[pyo3(signature = (actor, key, version, state, /))]
    fn save<'py>(
        &self,
        py: Python<'py>,
        actor: String,
        key: String,
        version: &Bound<'py, PyBytes>,
        state: Option<&Bound<'py, PyBytes>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (version, state) = (
            version.as_bytes().to_vec(),
            state.map(|state| state.as_bytes().to_vec()),
        );
        self.call(
            py,
            move |sql| async move { sql.save(&actor, &key, &version, state.as_deref()).await },
            |py, ()| Ok(py.None().into_bound(py)),
        )
    }

    /// Forget the record of `(actor, key)` if its version is not greater than `version`.
    #[pyo3(signature = (actor, key, version, /))]
    fn drop<'py>(
        &self,
        py: Python<'py>,
        actor: String,
        key: String,
        version: &Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let version = version.as_bytes().to_vec();
        self.call(
            py,
            move |sql| async move { sql.forget(&actor, &key, &version).await },
            |py, ()| Ok(py.None().into_bound(py)),
        )
    }
}

/// What the database answered to `storage` of `(actor, key)`, as a node takes it.
async fn carried(sql: &casty_store::Sql, actor: &str, key: &str, storage: Storage) -> StoreAnswer {
    let kept = match storage {
        Storage::Load => sql.load(actor, key).await,
        Storage::Save(stored) => {
            let (written, state) = (stored.version(), stored.state());
            let saved = sql.save(actor, key, &written, state.as_deref()).await;
            return saved.map(|()| None).map_err(|failed| failed.to_string());
        }
        Storage::Drop(stamp) => {
            let forgotten = sql.forget(actor, key, &version(&stamp)).await;
            return forgotten
                .map(|()| None)
                .map_err(|failed| failed.to_string());
        }
    };
    let record = kept.map_err(|failed| failed.to_string())?;
    record
        .map(|(written, state)| Stored::read(&written, state.as_deref()).map_err(unreadable))
        .transpose()
}

/// A record as `casty.Store.load` answers it.
fn record(py: Python<'_>, record: Option<Record>) -> PyResult<Bound<'_, PyAny>> {
    let Some((version, state)) = record else {
        return Ok(py.None().into_bound(py));
    };
    let state = match state {
        Some(state) => PyBytes::new(py, &state).into_any(),
        None => py.None().into_bound(py),
    };
    Ok(PyTuple::new(py, [PyBytes::new(py, &version).into_any(), state])?.into_any())
}

/// The exception of a store that could not open its database: its URL named no database this knows, or the
/// database could not be reached or refused the table.
fn unopened<'py>(py: Python<'py>, failed: &Error) -> Bound<'py, PyAny> {
    let raised = match failed {
        Error::Configuration(_) => PyValueError::new_err(failed.to_string()),
        _ => PyConnectionError::new_err(format!("the store could not open its database: {failed}")),
    };
    raised.into_value(py).into_bound(py).into_any()
}
