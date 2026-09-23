//! The bridge between the node of a cluster and the event loop this process runs its bodies on.
//!
//! The components run on the threads of the transport and never touch an interpreter. What they decide reaches the
//! loop as a callback, and what the loop decides reaches them through the channel of the node. Nothing here waits
//! for the other side: a call from the node task hands the work over and returns, because the loop is what would
//! answer it, and a call from the loop leaves a request on the channel.

use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use casty_core::mailbox::Command;
use casty_core::membership::views::Overlay;
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_core::store::Held;
use casty_net::compress::Name;
use casty_net::limits::Limits;
use casty_net::pool::AddressMap;
use casty_net::tls::Tls;
use casty_node::events::Event;
use casty_node::membership::service::{Member, Timings};
pub use casty_node::node::Cluster as Settings;
use casty_node::node::{Host, Kind, Node as Cluster, Placed, Running};
use casty_node::replication::service::{Failure, Storing};
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

use super::callback;
use super::observe::{self, Observed};
use super::{Node, Op};
use crate::actor::period;
use crate::lock::Locked;
use crate::runtime::Threads;

pyo3::create_exception!(
    _casty,
    Fencing,
    PyException,
    "Another owner took the key. It never reaches the body: the activation ends on it."
);

/// A node of a cluster, with the runtime its threads belong to.
#[derive(Debug)]
pub struct Joined {
    /// The threads of the transport: its own, or a runtime it shares with other systems. Its own are let go of without
    /// waiting, since what runs on them may be waiting for the loop, and the loop is what lets them go.
    runtime: Mutex<Option<Arc<Threads>>>,
    /// How work is put on those threads, which outlives letting the runtime go.
    threads: tokio::runtime::Handle,
    running: Mutex<Option<Running>>,
    /// The node of the cluster, from the moment it has joined one.
    node: OnceLock<Cluster>,
    /// The loop the bodies run on, which is the only thread that touches an interpreter.
    running_loop: Py<PyAny>,
    addresses: Option<Arc<Mapping>>,
}

impl Joined {
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
        shared: Option<Arc<Threads>>,
    ) -> PyResult<Arc<Self>> {
        let runtime = match shared {
            Some(shared) => shared,
            None => Threads::start(None)?,
        };
        let host: Arc<dyn Host> = Arc::new(Bridge {
            node: Arc::downgrade(node),
            running_loop: running_loop.clone_ref(py),
            observer: node.observer(py),
            store: node.storage(py),
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
            callback::on_loop(&running_loop, move |py| {
                entering(py, &held, &node, started, entered.bind(py), system.bind(py))
            });
        });
        Ok(joined)
    }

    /// The cluster this is, once the node has joined it: until then there is nothing to route through.
    #[must_use]
    pub fn entered(self: &Arc<Self>) -> Option<Entered> {
        self.node.get().map(|node| Entered {
            joined: Arc::clone(self),
            node: node.clone(),
        })
    }

    /// Let the threads of the transport go: a runtime of its own ends here, without waiting for them, and a shared one
    /// goes on for the systems that still hold it.
    ///
    /// They are given up rather than joined: a dial of theirs may be waiting for the loop, and this runs on the loop,
    /// so waiting here is waiting for something that is waiting for this.
    pub fn shutdown(&self) {
        drop(self.runtime.locked().take());
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

    /// Say goodbye, give away what this node keeps, and stop the transport. `gone` is resolved once it is over.
    ///
    /// `abort` is the process going away: its sockets close where they are and nothing is handed over, which is what
    /// the other nodes see as a machine that disappeared.
    ///
    /// Like the join, this talks to the other nodes and the loop is what answers them, so it runs on the threads of
    /// the transport and comes back when it is done.
    pub fn leave(joined: &Arc<Self>, py: Python<'_>, node: &Arc<Node>, abort: bool, then: Ending) {
        let Some(running) = joined.running.locked().take() else {
            left(py, joined, node, &then);
            return;
        };
        let running_loop = joined.running_loop.clone_ref(py);
        let node = Arc::clone(node);
        let joined = Arc::clone(joined);
        joined.threads.clone().spawn(async move {
            if abort {
                running.crash().await;
            } else {
                // What it went without, the node has already reported to the observer.
                running.leave().await;
            }
            callback::on_loop(&running_loop, move |py| {
                left(py, &joined, &node, &then);
                Ok(())
            });
        });
    }
}

impl Drop for Joined {
    // A cluster the loop never let go of ends where its last holder does, which can be a task on one of its own
    // threads: a runtime dropped there panics, one given up does not.
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A cluster this node is in: the node it joined as, with the transport it runs on.
#[derive(Debug, Clone)]
pub struct Entered {
    joined: Arc<Joined>,
    node: Cluster,
}

impl Entered {
    #[must_use]
    pub fn node(&self) -> &Cluster {
        &self.node
    }

    #[must_use]
    pub fn id(&self) -> &NodeId {
        self.node.id()
    }

    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        self.node.members()
    }

    /// Ask for the address of every member here, on the loop, so that a dial finds it without waiting.
    pub fn learn(&self, py: Python<'_>, members: &[Member]) -> PyResult<()> {
        self.joined.learn(py, members)
    }

    /// Carry `op` out on the replicas of the key, resolving `answer` on the loop once they have answered: with what
    /// they hold when it takes the key over, with nothing once they confirmed a write.
    pub fn persist(&self, py: Python<'_>, actor: &str, key: &str, op: Op, answer: Py<PyAny>) {
        let (node, actor, key) = (self.node.clone(), actor.to_owned(), key.to_owned());
        self.dispatch(py, answer, async move {
            match op {
                Op::Activate { initial } => {
                    Landed::Taken(node.activate(&actor, &key, initial).await)
                }
                Op::Commit {
                    lease,
                    pages,
                    active,
                } => Landed::Written(node.commit(&actor, &key, lease, pages, active).await),
                Op::Delete { lease, active } => {
                    Landed::Written(node.delete(&actor, &key, lease, active).await)
                }
            }
        });
    }

    /// Resolve `answer` on the loop with where `(actor, key)` is as this node sees it, as a `casty.Placement`.
    pub fn placed(&self, py: Python<'_>, actor: &str, key: &str, answer: Py<PyAny>) {
        let (node, actor, key) = (self.node.clone(), actor.to_owned(), key.to_owned());
        self.dispatch(py, answer, async move {
            Landed::Placed(node.placed(&actor, &key).await)
        });
    }

    /// Resolve `answer` on the loop with every key the replica of this node keeps, as `(actor, key, deleted)`.
    pub fn stored(&self, py: Python<'_>, answer: Py<PyAny>) {
        let node = self.node.clone();
        self.dispatch(
            py,
            answer,
            async move { Landed::Stored(node.stored().await) },
        );
    }

    /// Run `operation` on the threads of the transport, and resolve `answer` on the loop with what it lands.
    ///
    /// The loop may already be closed when it lands, which is what a system that stopped under an operation looks
    /// like: nobody is left to resolve it for.
    fn dispatch(
        &self,
        py: Python<'_>,
        answer: Py<PyAny>,
        operation: impl Future<Output = Landed> + Send + 'static,
    ) {
        let running_loop = self.joined.running_loop.clone_ref(py);
        self.joined.threads.spawn(async move {
            let landed = operation.await;
            callback::on_loop(&running_loop, move |py| landed.resolve(py, answer.bind(py)));
        });
    }
}

/// The end of a join, on the loop: `entered` is resolved with `system` once the node is in, or with the refusal.
fn entering(
    py: Python<'_>,
    joined: &Arc<Joined>,
    node: &Arc<Node>,
    started: std::io::Result<Running>,
    entered: &Bound<'_, PyAny>,
    system: &Bound<'_, PyAny>,
) -> PyResult<()> {
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
    let cluster = Entered {
        joined: Arc::clone(joined),
        node: running.node.clone(),
    };
    let _ = joined.node.set(running.node.clone());
    *joined.running.locked() = Some(running);
    node.entered(py, &cluster);
    entered.call_method1("set_result", (system,))?;
    Ok(())
}

/// What a node does after it has let a cluster go.
#[derive(Debug)]
pub enum Ending {
    /// The system is out: a ref of it reaches nothing from here on, and `__aexit__` returns.
    Gone(Py<PyAny>),
    /// The cluster declared this identity left: the node joins again as a new one, on the address it had.
    Again,
}

/// The end of a leave: let the transport of `joined` go, and then do what the node was waiting for it to be gone to do.
///
/// `joined` is the cluster this ends, which the node has already replaced by another one when its identity was
/// removed. Joining again waits for exactly this: the listener of the identity that was removed holds the address
/// until its transport is gone, and the new identity binds the same one.
fn left(py: Python<'_>, joined: &Arc<Joined>, node: &Arc<Node>, then: &Ending) {
    joined.shutdown();
    node.let_go(joined);
    match then {
        Ending::Again => {
            if let Err(failed) = node.rejoin(py) {
                failed.restore(py);
            }
        }
        Ending::Gone(gone) => {
            node.stop_taking();
            let gone = gone.bind(py);
            if let Ok(done) = gone.call_method0("done")
                && !done.is_truthy().unwrap_or(true)
            {
                let _ = gone.call_method1("set_result", (py.None(),));
            }
        }
    }
}

/// What a store answered, on its way from a thread of the transport back to the loop.
#[derive(Debug)]
enum Landed {
    Taken(Result<Option<Held>, Failure>),
    Written(Result<(), Failure>),
    Stored(Vec<(String, String, bool)>),
    Placed(Placed),
}

impl Landed {
    /// Resolve `answer` with what landed, on the loop.
    fn resolve(self, py: Python<'_>, answer: &Bound<'_, PyAny>) -> PyResult<()> {
        if answer.call_method0("done")?.is_truthy()? {
            return Ok(());
        }
        match self {
            Self::Taken(Ok(held)) => {
                let taken = Bound::new(py, Taken::of(held))?;
                answer.call_method1("set_result", (taken,))?;
            }
            Self::Written(Ok(())) => {
                answer.call_method1("set_result", (py.None(),))?;
            }
            Self::Stored(stored) => {
                answer.call_method1("set_result", (stored,))?;
            }
            Self::Placed(placed) => {
                answer.call_method1("set_result", (super::located(py, &placed)?,))?;
            }
            Self::Taken(Err(failure)) | Self::Written(Err(failure)) => {
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
        self.0.locked().take()
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
        Failure::TooLarge(why) => crate::errors::MessageTooLarge::new_err(why.clone()),
    };
    held.into_value(py).into_bound(py).into_any()
}

/// What the node of a cluster calls when something reaches this process.
struct Bridge {
    node: Weak<Node>,
    running_loop: Py<PyAny>,
    /// What the system reports to.
    observer: Py<PyAny>,
    /// What keeps the state of the durable types, when the system was built with a store.
    store: Option<Py<PyAny>>,
}

impl core::fmt::Debug for Bridge {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("Bridge").finish_non_exhaustive()
    }
}

impl Bridge {
    /// Queue `work` on the loop. It is dropped if the loop or the interpreter is gone, which is a process on its way
    /// out.
    fn hand_over(&self, work: Arriving) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        callback::on_loop(&self.running_loop, move |py| work.arrive(py, &node));
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

    fn cancel(&self, actor: &str, key: &str, request: &Target) {
        self.hand_over(Arriving::Cancel {
            actor: actor.to_owned(),
            key: key.to_owned(),
            request: request.clone(),
        });
    }

    fn removed(&self) {
        self.hand_over(Arriving::Removed);
    }

    fn observe(&self, event: Event) {
        let event = Observed::Cluster(event);
        // Decided before the interpreter is reached: what the observer does not take costs the node task nothing.
        if !self
            .node
            .upgrade()
            .is_some_and(|node| node.takes(event.kind()))
        {
            return;
        }
        let _ = Python::try_attach(|py| {
            observe::deliver(py, self.running_loop.bind(py), &self.observer, event, true);
        });
    }

    fn store(&self, node: &Cluster, request: Storing) {
        let Some(store) = &self.store else {
            node.from_store(request.id, Err(super::storage::NO_STORE.to_owned()));
            return;
        };
        let id = request.id;
        let handed = Python::try_attach(|py| {
            super::storage::hand(py, self.running_loop.bind(py), store, node, request);
        });
        if handed.is_none() {
            node.from_store(id, Err("the interpreter is shutting down".to_owned()));
        }
    }
}

/// What the node of a cluster asked of this process.
#[derive(Debug)]
enum Arriving {
    Hand(Command),
    Settle {
        id: i64,
        outcome: Outcome,
    },
    Members(Vec<Member>),
    Meet(String),
    Attach {
        actor: String,
        key: String,
    },
    Release {
        actor: String,
        key: String,
    },
    Cancel {
        actor: String,
        key: String,
        request: Target,
    },
    Removed,
}

impl Arriving {
    /// Do what the cluster asked of `node`, on the loop.
    fn arrive(self, py: Python<'_>, node: &Arc<Node>) -> PyResult<()> {
        match self {
            Self::Hand(command) => node.take(py, command),
            Self::Settle { id, outcome } => node.settle(py, id, &outcome),
            Self::Members(members) => {
                node.seen(py, members);
                Ok(())
            }
            Self::Meet(actor) => {
                node.met(py, &actor);
                Ok(())
            }
            // A leaving node takes no key back, whatever asked for it before it started leaving.
            Self::Attach { .. } if node.draining() => Ok(()),
            Self::Attach { actor, key } => node.attach(py, &actor, &key).map(|_| ()),
            Self::Release { actor, key } => node.relinquish(py, &actor, &key),
            Self::Cancel {
                actor,
                key,
                request,
            } => node.cancelled(py, &actor, &key, &request),
            Self::Removed => node.removed(py),
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
        self.known.locked().get(address).cloned()
    }

    fn took(&self, address: &str, dialed: String) {
        self.known.locked().insert(address.to_owned(), dialed);
    }

    /// The address to dial, asked of the loop when this is the first time it comes up.
    ///
    /// Waiting here does not hold anything of the node up: a dial has a task of its own, and the worker it runs on
    /// is given back to the runtime while it waits. An answer that never comes leaves the address as it was, which
    /// is a node dialed directly instead of through whatever the map would have put in between.
    fn dialed(self: &Arc<Self>, address: &str) -> String {
        if let Some(found) = self.held(address) {
            return found;
        }
        let (answer, answered) = channel();
        let (mapping, asked) = (Arc::clone(self), address.to_owned());
        let handed = callback::on_loop(&self.running_loop, move |py| {
            let dialed = mapping
                .map
                .bind(py)
                .call1((asked.clone(),))
                .and_then(|dialed| dialed.extract::<String>())
                .unwrap_or(asked);
            let _ = answer.send(dialed);
            Ok(())
        });
        if !handed {
            return address.to_owned();
        }
        let dialed = tokio::task::block_in_place(|| answered.recv_timeout(ASKING))
            .unwrap_or_else(|_| address.to_owned());
        self.took(address, dialed.clone());
        dialed
    }
}

/// How long a dial waits for the loop to say where the address goes.
const ASKING: Duration = Duration::from_secs(5);

/// Read the `Cluster` of the public API into what the node takes, with the timings of the system it belongs to.
pub fn settings(cluster: &Bound<'_, PyAny>, system: &super::Settings) -> PyResult<Settings> {
    let overlay = cluster.getattr("overlay")?;
    let compression = cluster.getattr("compression")?;
    Ok(Settings {
        bind: cluster.getattr("bind")?.extract()?,
        advertise: cluster.getattr("advertise")?.extract()?,
        seeds: cluster.getattr("seeds")?.extract()?,
        name: cluster.getattr("name")?.extract()?,
        timings: Timings {
            heartbeat: every("heartbeat", &cluster.getattr("heartbeat")?)?,
            suspect_after: period("suspect_after", &cluster.getattr("suspect_after")?)?,
            dead_after: period("dead_after", &cluster.getattr("dead_after")?)?,
            remove_after: match cluster.getattr("remove_after")? {
                held if held.is_none() => None,
                held => Some(period("remove_after", &held)?),
            },
            anti_entropy: every("anti_entropy", &cluster.getattr("anti_entropy")?)?,
            graft_after: every("overlay.graft_after", &overlay.getattr("graft_after")?)?,
            shuffle_every: every("overlay.shuffle_every", &overlay.getattr("shuffle_every")?)?,
        },
        overlay: Overlay {
            active: overlay.getattr("active")?.extract()?,
            passive: overlay.getattr("passive")?.extract()?,
            join_walk: overlay.getattr("join_walk")?.extract()?,
            passive_walk: overlay.getattr("passive_walk")?.extract()?,
        },
        tls: tls(&cluster.getattr("tls")?)?,
        compression: compressed(&compression)?,
        min_compressed: compression.getattr("min_bytes")?.extract()?,
        address_map: None,
        limits: limits(&cluster.getattr("limits")?)?,
        write_timeout: system.write_timeout,
        leave_timeout: system.leave_timeout,
        backoff: system.backoff,
    })
}

/// What a client joins with: seeds and the table it asks for, with none of what a member gossips.
pub fn dialling(
    seeds: Vec<String>,
    name: String,
    tls_of: &Bound<'_, PyAny>,
    compression: &Bound<'_, PyAny>,
    limits_of: &Bound<'_, PyAny>,
    sync_every: core::time::Duration,
) -> PyResult<Settings> {
    Ok(Settings {
        seeds,
        name,
        tls: tls(tls_of)?,
        compression: compressed(compression)?,
        min_compressed: compression.getattr("min_bytes")?.extract()?,
        limits: limits(limits_of)?,
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

/// The sizes of a connection, as the `Limits` of the public API has them. The periods stay those of the transport.
fn limits(held: &Bound<'_, PyAny>) -> PyResult<Limits> {
    Ok(Limits {
        frame: held.getattr("frame")?.extract()?,
        message: held.getattr("message")?.extract()?,
        window: held.getattr("window")?.extract()?,
        ..Limits::default()
    })
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

/// The period the transport repeats something on, which cannot be zero: it would never wait between two rounds.
pub fn every(name: &str, held: &Bound<'_, PyAny>) -> PyResult<Duration> {
    let every = period(name, held)?;
    if every.is_zero() {
        return Err(PyValueError::new_err(format!(
            "{name} is {held}, and a period of zero would never wait between two rounds"
        )));
    }
    Ok(every)
}
