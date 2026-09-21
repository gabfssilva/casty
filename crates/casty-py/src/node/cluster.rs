//! The bridge between the node of a cluster and the event loop this process runs its bodies on.
//!
//! The components run on the threads of the transport and never touch an interpreter. What they decide reaches the
//! loop as a callback, and what the loop decides reaches them through the channel of the node. Nothing here waits
//! for the other side: a call from the node task hands the work over and returns, because the loop is what would
//! answer it, and a call from the loop leaves a request on the channel.

use std::collections::HashMap;
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use casty_core::mailbox::{Backoff, Command};
use casty_core::membership::views::Overlay;
use casty_core::node::NodeId;
use casty_core::outcome::Outcome;
use casty_core::store::{Held, Pages};
use casty_net::compress::Name;
use casty_net::limits::Limits;
use casty_net::pool::AddressMap;
use casty_net::tls::Tls;
pub use casty_node::membership::runner::Cluster as Settings;
use casty_node::membership::service::{Member, Timings};
use casty_node::node::{Host, Kind, Node as Cluster, Running};
use casty_node::replication::service::Failure;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use super::Node;

pyo3::create_exception!(
    _casty,
    Fencing,
    PyException,
    "Another owner took the key. It never reaches the body: the activation ends on it."
);

/// A node of a cluster, with the runtime its threads belong to.
#[derive(Debug)]
pub struct Joined {
    /// The threads of the transport. They are let go of without waiting: what runs on them may be waiting for the
    /// loop, and the loop is what lets them go.
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    /// How work is put on those threads, which outlives taking the runtime out to end it.
    threads: tokio::runtime::Handle,
    running: Mutex<Option<Running>>,
    /// The node of the cluster, from the moment it has joined one.
    node: OnceLock<Cluster>,
    /// The loop the bodies run on, which is the only thread that touches an interpreter.
    running_loop: Py<PyAny>,
    addresses: Option<Arc<Mapping>>,
}

impl Joined {
    /// Join the cluster and give back the node, or the reason a seed would not have it.
    /// Start joining the cluster. `entered` is resolved with `system` once the node is in, or with the refusal.
    ///
    /// Joining talks to the seeds and the loop is what answers them, in a test that puts a proxy in between and in
    /// any process where a seed is another system of its own. So nothing waits here: the join runs on the threads of
    /// the transport and comes back to the loop when it is done.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        py: Python<'_>,
        node: &Arc<Node>,
        mut settings: Settings,
        map: Option<&Bound<'_, PyAny>>,
        types: Vec<Kind>,
        running_loop: Py<PyAny>,
        entered: Py<PyAny>,
        system: Py<PyAny>,
        member: bool,
    ) -> PyResult<Arc<Self>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|failed| {
                PyRuntimeError::new_err(format!("the transport did not start: {failed}"))
            })?;
        let host: Arc<dyn Host> = Arc::new(Bridge {
            node: Arc::downgrade(node),
            running_loop: running_loop.clone_ref(py),
        });
        let addresses = match map {
            None => None,
            Some(map) => {
                let mapping = Arc::new(Mapping::new(py, map, &running_loop));
                // The seeds are the first thing dialed, and they are asked for here, where the interpreter is.
                mapping.learn(py, &settings.seeds)?;
                let held = Arc::clone(&mapping);
                let dialing: AddressMap = Arc::new(move |address: &str| held.dialed(address));
                settings.address_map = Some(dialing);
                Some(mapping)
            }
        };
        let joined = Arc::new(Self {
            node: OnceLock::new(),
            running: Mutex::new(None),
            threads: runtime.handle().clone(),
            runtime: Mutex::new(Some(runtime)),
            running_loop: running_loop.clone_ref(py),
            addresses,
        });
        let held = Arc::clone(&joined);
        let node = Arc::clone(node);
        joined.threads.spawn(async move {
            let started = if member {
                Running::start(settings, host, types).await
            } else {
                Running::client(settings, host, types).await
            };
            Python::attach(|py| {
                let call = Bound::new(
                    py,
                    Entering {
                        started: Mutex::new(Some(started)),
                        joined: held,
                        node,
                        entered,
                        system,
                    },
                );
                match call {
                    Ok(call) => {
                        let _ = running_loop
                            .bind(py)
                            .call_method1("call_soon_threadsafe", (call,));
                    }
                    Err(failed) => failed.restore(py),
                }
            });
        });
        Ok(joined)
    }

    /// Whether this node is in the cluster: until then there is nothing to route through.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.node.get().is_some()
    }

    /// Let the threads of the transport go without waiting for them.
    ///
    /// They are given up rather than joined: a dial of theirs may be waiting for the loop, and this runs on the loop,
    /// so waiting here is waiting for something that is waiting for this.
    pub fn shutdown(&self) {
        if let Some(runtime) = self
            .runtime
            .lock()
            .expect("the cluster lock is never poisoned")
            .take()
        {
            runtime.shutdown_background();
        }
    }

    /// Ask for the address of every member here, on the loop, so that a dial finds it without waiting.
    pub fn learn(&self, py: Python<'_>, members: &[Member]) -> PyResult<()> {
        let Some(addresses) = &self.addresses else {
            return Ok(());
        };
        let seen: Vec<String> = members
            .iter()
            .filter_map(|member| member.node.address.clone())
            .collect();
        addresses.learn(py, &seen)
    }

    #[must_use]
    pub fn id(&self) -> &NodeId {
        self.node().id()
    }

    #[must_use]
    pub fn node(&self) -> &Cluster {
        self.node.get().expect("the node has joined a cluster")
    }

    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.node().members()
    }

    /// Take the key over, resolving `answer` on the loop with what the replicas hold.
    pub fn activate(
        &self,
        py: Python<'_>,
        actor: &str,
        key: &str,
        initial: Option<Pages>,
        answer: Py<PyAny>,
    ) {
        let node = self.node().clone();
        let running_loop = self.running_loop.clone_ref(py);
        let (actor, key) = (actor.to_owned(), key.to_owned());
        self.threads.spawn(async move {
            let held = node.activate(&actor, &key, initial).await;
            settle(&running_loop, Landed::Taken(held), answer);
        });
    }

    /// Write the state of the key, resolving `answer` on the loop once the replicas confirmed it.
    pub fn commit(
        &self,
        py: Python<'_>,
        actor: &str,
        key: &str,
        pages: Pages,
        active: bool,
        answer: Py<PyAny>,
    ) {
        let node = self.node().clone();
        let running_loop = self.running_loop.clone_ref(py);
        let (actor, key) = (actor.to_owned(), key.to_owned());
        self.threads.spawn(async move {
            let written = node.commit(&actor, &key, pages, active).await;
            settle(&running_loop, Landed::Written(written), answer);
        });
    }

    /// Say goodbye, give away what this node keeps, and stop the transport. `gone` is resolved once it is over.
    ///
    /// `abort` is the process going away: its sockets close where they are and nothing is handed over, which is what
    /// the other nodes see as a machine that disappeared.
    ///
    /// Like the join, this talks to the other nodes and the loop is what answers them, so it runs on the threads of
    /// the transport and comes back when it is done.
    pub fn leave(joined: &Arc<Self>, py: Python<'_>, node: &Arc<Node>, abort: bool, then: Ending) {
        let Some(running) = joined
            .running
            .lock()
            .expect("the cluster lock is never poisoned")
            .take()
        else {
            Departed::of(joined, node, then).settle(py);
            return;
        };
        let running_loop = joined.running_loop.clone_ref(py);
        let node = Arc::clone(node);
        let joined = Arc::clone(joined);
        joined.threads.clone().spawn(async move {
            let owed = if abort {
                running.crash().await;
                Vec::new()
            } else {
                running.leave().await
            };
            Python::attach(|py| {
                let call = Bound::new(
                    py,
                    Departed {
                        owed: Mutex::new(Some(owed)),
                        joined,
                        node,
                        then,
                    },
                );
                match call {
                    Ok(call) => {
                        let _ = running_loop
                            .bind(py)
                            .call_method1("call_soon_threadsafe", (call,));
                    }
                    Err(failed) => failed.restore(py),
                }
            });
        });
    }
}

/// The end of a join, as something the event loop can call.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Entering {
    started: Mutex<Option<std::io::Result<Running>>>,
    joined: Arc<Joined>,
    node: Arc<Node>,
    entered: Py<PyAny>,
    system: Py<PyAny>,
}

#[pymethods]
impl Entering {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let Some(started) = self
            .started
            .lock()
            .expect("the join lock is never poisoned")
            .take()
        else {
            return Ok(());
        };
        let entered = self.entered.bind(py);
        if entered.call_method0("done")?.is_truthy()? {
            return Ok(());
        }
        let running = match started {
            Ok(running) => running,
            Err(refused) => {
                let refusal = crate::errors::Refused::new_err(refused.to_string());
                entered.call_method1("set_exception", (refusal,))?;
                return Ok(());
            }
        };
        let _ = self.joined.node.set(running.node.clone());
        *self
            .joined
            .running
            .lock()
            .expect("the cluster lock is never poisoned") = Some(running);
        self.node.entered(py, &self.joined);
        entered.call_method1("set_result", (self.system.bind(py),))?;
        Ok(())
    }
}

/// The end of a leave, as something the event loop can call.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Departed {
    owed: Mutex<Option<Vec<(String, String)>>>,
    /// The cluster this ends. An identity that was removed has already been replaced by another one here.
    joined: Arc<Joined>,
    node: Arc<Node>,
    /// What happens once the transport of this identity is gone.
    then: Ending,
}

/// What a node does after it has let a cluster go.
#[derive(Debug)]
pub enum Ending {
    /// The system is out: a ref of it reaches nothing from here on, and `__aexit__` returns.
    Gone(Py<PyAny>),
    /// The cluster declared this identity left: the node joins again as a new one, on the address it had.
    Again,
}

impl Departed {
    fn of(joined: &Arc<Joined>, node: &Arc<Node>, then: Ending) -> Self {
        Self {
            owed: Mutex::new(None),
            joined: Arc::clone(joined),
            node: Arc::clone(node),
            then,
        }
    }

    /// Let the transport go, and then do what the node was waiting for it to be gone to do.
    ///
    /// Joining again waits for exactly this: the listener of the identity that was removed holds the address until
    /// its transport is gone, and the new identity binds the same one.
    fn settle(&self, py: Python<'_>) {
        self.joined.shutdown();
        self.node.let_go(&self.joined);
        match &self.then {
            Ending::Again => {
                if let Err(failed) = self.node.rejoin(py) {
                    failed.restore(py);
                }
            }
            Ending::Gone(gone) => {
                self.node.stop_taking();
                let gone = gone.bind(py);
                if let Ok(done) = gone.call_method0("done")
                    && !done.is_truthy().unwrap_or(true)
                {
                    let _ = gone.call_method1("set_result", (py.None(),));
                }
            }
        }
    }
}

#[pymethods]
impl Departed {
    fn __call__(&self, py: Python<'_>) {
        let owed = self
            .owed
            .lock()
            .expect("the leave lock is never poisoned")
            .take();
        for (actor, key) in owed.into_iter().flatten() {
            let _ = super::log(py, format!("left without handing over {actor}/{key}"));
        }
        self.settle(py);
    }
}

/// What a store answered, on its way from a thread of the transport back to the loop.
#[derive(Debug)]
enum Landed {
    Taken(Result<Option<Held>, Failure>),
    Written(Result<(), Failure>),
}

/// Hand the answer to the loop, which resolves the future the body is waiting on.
fn settle(running_loop: &Py<PyAny>, landed: Landed, answer: Py<PyAny>) {
    Python::attach(|py| {
        let call = match Bound::new(
            py,
            Settling {
                landed: Mutex::new(Some(landed)),
                answer,
            },
        ) {
            Ok(call) => call,
            Err(failed) => {
                failed.restore(py);
                return;
            }
        };
        // The loop may already be closed, which is what a system that stopped under an operation looks like.
        let _ = running_loop
            .bind(py)
            .call_method1("call_soon_threadsafe", (call,));
    });
}

/// The answer of a store, as something the event loop can call.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Settling {
    landed: Mutex<Option<Landed>>,
    answer: Py<PyAny>,
}

#[pymethods]
impl Settling {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let Some(landed) = self
            .landed
            .lock()
            .expect("the answer lock is never poisoned")
            .take()
        else {
            return Ok(());
        };
        let answer = self.answer.bind(py);
        if answer.call_method0("done")?.is_truthy()? {
            return Ok(());
        }
        match landed {
            Landed::Taken(Ok(held)) => {
                let taken = Bound::new(py, Taken::of(held))?;
                answer.call_method1("set_result", (taken,))?;
            }
            Landed::Written(Ok(())) => {
                answer.call_method1("set_result", (py.None(),))?;
            }
            Landed::Taken(Err(failure)) | Landed::Written(Err(failure)) => {
                answer.call_method1("set_exception", (raised(py, &failure),))?;
            }
        }
        Ok(())
    }
}

/// What the replicas hold for a key, or nothing when no replica has a state for it.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Taken(Mutex<Option<Held>>);

impl Taken {
    #[must_use]
    pub fn of(held: Option<Held>) -> Self {
        Self(Mutex::new(held))
    }

    /// What was taken over, read once by the activation that asked for it.
    #[must_use]
    pub fn held(&self) -> Option<Held> {
        self.0
            .lock()
            .expect("the answer lock is never poisoned")
            .take()
    }
}

/// The exception an operation that did not go through raises in whatever was waiting for it.
///
/// `Fencing` is not one of them: the key moved to another owner, so the activation ends instead of the body hearing
/// about it. What waits for such a write is cancelled with the body.
fn raised<'py>(py: Python<'py>, failure: &Failure) -> Bound<'py, PyAny> {
    let held = match failure {
        Failure::Unavailable(why) => crate::errors::Unavailable::new_err(why.clone()),
        Failure::Fencing(why) => Fencing::new_err(why.clone()),
        Failure::TooLarge(why) => PyValueError::new_err(why.clone()),
    };
    held.into_value(py).into_bound(py).into_any()
}

/// What the node of a cluster calls when something reaches this process.
struct Bridge {
    node: Weak<Node>,
    running_loop: Py<PyAny>,
}

impl core::fmt::Debug for Bridge {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("Bridge").finish_non_exhaustive()
    }
}

impl Bridge {
    /// Queue `work` on the loop. It is dropped if the loop is gone, which is a process on its way out.
    fn hand_over(&self, work: Arriving) {
        Python::attach(|py| {
            let Some(node) = self.node.upgrade() else {
                return;
            };
            let call = match Bound::new(
                py,
                Arrived {
                    work: Mutex::new(Some(work)),
                    node,
                },
            ) {
                Ok(call) => call,
                Err(failed) => {
                    failed.restore(py);
                    return;
                }
            };
            let _ = self
                .running_loop
                .bind(py)
                .call_method1("call_soon_threadsafe", (call,));
        });
    }
}

impl Host for Bridge {
    fn hand(&self, _: &Cluster, command: Command) {
        self.hand_over(Arriving::Hand(command));
    }

    fn settle(&self, id: i64, outcome: Outcome) {
        self.hand_over(Arriving::Settle { id, outcome });
    }

    fn members(&self, members: Vec<Member>) {
        self.hand_over(Arriving::Members(members));
    }

    fn meet(&self, _: &Cluster, actor: &str) {
        self.hand_over(Arriving::Meet(actor.to_owned()));
    }

    fn attach(&self, _: &Cluster, actor: &str, key: &str) {
        self.hand_over(Arriving::Attach {
            actor: actor.to_owned(),
            key: key.to_owned(),
        });
    }

    fn release(&self, actor: &str, key: &str) {
        self.hand_over(Arriving::Release {
            actor: actor.to_owned(),
            key: key.to_owned(),
        });
    }

    fn removed(&self) {
        self.hand_over(Arriving::Removed);
    }
}

/// What the node of a cluster asked of this process.
#[derive(Debug)]
enum Arriving {
    Hand(Command),
    Settle { id: i64, outcome: Outcome },
    Members(Vec<Member>),
    Meet(String),
    Attach { actor: String, key: String },
    Release { actor: String, key: String },
    Removed,
}

/// One thing the cluster asked of this process, as something the event loop can call.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Arrived {
    work: Mutex<Option<Arriving>>,
    node: Arc<Node>,
}

#[pymethods]
impl Arrived {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let Some(work) = self
            .work
            .lock()
            .expect("the work lock is never poisoned")
            .take()
        else {
            return Ok(());
        };
        match work {
            Arriving::Hand(command) => self.node.take(py, command),
            Arriving::Settle { id, outcome } => self.node.settle(py, id, &outcome),
            Arriving::Members(members) => {
                self.node.seen(py, members);
                Ok(())
            }
            Arriving::Meet(actor) => {
                self.node.met(py, &actor);
                Ok(())
            }
            Arriving::Attach { actor, key } => self.node.attach(py, &actor, &key).map(|_| ()),
            Arriving::Release { actor, key } => self.node.relinquish(py, &actor, &key),
            Arriving::Removed => self.node.removed(py),
        }
    }
}

/// Turns an advertised address into the one to dial, asking the loop when it has not seen it before.
///
/// The map is a Python callable, so it runs where the interpreter is: the loop. A dial that does not know an address
/// yet waits for the loop to answer, which it can, because the node the dial belongs to is not what would answer it.
#[derive(Debug)]
struct Mapping {
    map: Py<PyAny>,
    running_loop: Py<PyAny>,
    known: Mutex<HashMap<String, String>>,
}

impl Mapping {
    fn new(py: Python<'_>, map: &Bound<'_, PyAny>, running_loop: &Py<PyAny>) -> Self {
        Self {
            map: map.clone().unbind(),
            running_loop: running_loop.clone_ref(py),
            known: Mutex::new(HashMap::new()),
        }
    }

    /// Ask for these addresses here, which is where the interpreter already is.
    fn learn(&self, py: Python<'_>, addresses: &[String]) -> PyResult<()> {
        for address in addresses {
            if self.held(address).is_some() {
                continue;
            }
            let dialed: String = self.map.bind(py).call1((address.clone(),))?.extract()?;
            self.took(address, dialed);
        }
        Ok(())
    }

    fn held(&self, address: &str) -> Option<String> {
        self.known
            .lock()
            .expect("the address lock is never poisoned")
            .get(address)
            .cloned()
    }

    fn took(&self, address: &str, dialed: String) {
        self.known
            .lock()
            .expect("the address lock is never poisoned")
            .insert(address.to_owned(), dialed);
    }

    /// The address to dial, asked of the loop when this is the first time it comes up.
    ///
    /// Waiting here does not hold anything of the node up: a dial has a task of its own, and the worker it runs on
    /// is given back to the runtime while it waits. An answer that never comes leaves the address as it was, which
    /// is a node dialed directly instead of through whatever the map would have put in between.
    fn dialed(&self, address: &str) -> String {
        if let Some(found) = self.held(address) {
            return found;
        }
        let (answer, answered) = channel();
        let asked = Python::attach(|py| -> PyResult<()> {
            let call = Bound::new(
                py,
                Asking {
                    map: self.map.clone_ref(py),
                    address: address.to_owned(),
                    answer: Mutex::new(Some(answer)),
                },
            )?;
            self.running_loop
                .bind(py)
                .call_method1("call_soon_threadsafe", (call,))?;
            Ok(())
        });
        if asked.is_err() {
            return address.to_owned();
        }
        let dialed = tokio::task::block_in_place(|| answered.recv_timeout(ASKING))
            .unwrap_or_else(|_| address.to_owned());
        self.took(address, dialed.clone());
        dialed
    }
}

/// One address on its way to the map, as something the event loop can call.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Asking {
    map: Py<PyAny>,
    address: String,
    answer: Mutex<Option<Sender<String>>>,
}

#[pymethods]
impl Asking {
    fn __call__(&self, py: Python<'_>) {
        let Some(answer) = self
            .answer
            .lock()
            .expect("the address lock is never poisoned")
            .take()
        else {
            return;
        };
        let dialed = self
            .map
            .bind(py)
            .call1((self.address.clone(),))
            .and_then(|dialed| dialed.extract::<String>())
            .unwrap_or_else(|_| self.address.clone());
        let _ = answer.send(dialed);
    }
}

/// How long a dial waits for the loop to say where the address goes.
const ASKING: Duration = Duration::from_secs(5);

/// Read the `Cluster` of the public API into what the node takes.
pub fn settings(
    cluster: &Bound<'_, PyAny>,
    write_timeout: f64,
    leave_timeout: f64,
    backoff: Backoff,
) -> PyResult<Settings> {
    let overlay = cluster.getattr("overlay")?;
    Ok(Settings {
        bind: cluster.getattr("bind")?.extract()?,
        advertise: cluster.getattr("advertise")?.extract()?,
        seeds: cluster.getattr("seeds")?.extract()?,
        name: cluster.getattr("name")?.extract()?,
        timings: Timings {
            heartbeat: span(&cluster.getattr("heartbeat")?)?,
            suspect_after: span(&cluster.getattr("suspect_after")?)?,
            dead_after: span(&cluster.getattr("dead_after")?)?,
            remove_after: match cluster.getattr("remove_after")? {
                held if held.is_none() => None,
                held => Some(span(&held)?),
            },
            anti_entropy: span(&cluster.getattr("anti_entropy")?)?,
            graft_after: span(&overlay.getattr("graft_after")?)?,
            shuffle_every: span(&overlay.getattr("shuffle_every")?)?,
        },
        overlay: Overlay {
            active: overlay.getattr("active")?.extract()?,
            passive: overlay.getattr("passive")?.extract()?,
            join_walk: overlay.getattr("join_walk")?.extract()?,
            passive_walk: overlay.getattr("passive_walk")?.extract()?,
        },
        tls: tls(&cluster.getattr("tls")?)?,
        compression: compressed(&cluster.getattr("compression")?)?,
        address_map: None,
        limits: Limits::default(),
        write_timeout: core::time::Duration::from_secs_f64(write_timeout),
        leave_timeout: core::time::Duration::from_secs_f64(leave_timeout),
        backoff,
    })
}

/// What a client joins with: seeds and the table it asks for, with none of what a member gossips.
pub fn dialling(
    seeds: Vec<String>,
    name: String,
    tls_of: &Bound<'_, PyAny>,
    compression: &Bound<'_, PyAny>,
    sync_every: core::time::Duration,
) -> PyResult<Settings> {
    Ok(Settings {
        seeds,
        name,
        tls: tls(tls_of)?,
        compression: compressed(compression)?,
        // A client asks for the whole table on this period, which is the only clock it has.
        timings: Timings {
            anti_entropy: sync_every,
            ..Timings::default()
        },
        ..Settings::at("")
    })
}

/// The certificates of a connection, if the cluster has any.
fn tls(held: &Bound<'_, PyAny>) -> PyResult<Option<Tls>> {
    if held.is_none() {
        return Ok(None);
    }
    Ok(Some(Tls {
        cert: held.getattr("cert")?.extract()?,
        key: held.getattr("key")?.extract()?,
        ca: held.getattr("ca")?.extract()?,
        require_client_cert: held.getattr("require_client_cert")?.extract()?,
    }))
}

/// The compressors offered, in the order they are preferred. Nothing means every one this build has.
fn compressed(compression: &Bound<'_, PyAny>) -> PyResult<Option<Vec<Name>>> {
    let codecs: Option<Vec<String>> = compression.getattr("codecs")?.extract()?;
    codecs
        .map(|codecs| {
            codecs
                .iter()
                .map(|name| {
                    Name::of(name).ok_or_else(|| {
                        PyValueError::new_err(format!(
                            "compression codec {name:?} is not one of zstd, lz4, zlib"
                        ))
                    })
                })
                .collect()
        })
        .transpose()
}

/// A `timedelta` as a duration.
fn span(held: &Bound<'_, PyAny>) -> PyResult<core::time::Duration> {
    let seconds: f64 = held.call_method0("total_seconds")?.extract()?;
    Ok(core::time::Duration::from_secs_f64(seconds))
}
