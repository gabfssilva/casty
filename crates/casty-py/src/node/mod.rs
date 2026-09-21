//! A node: the keys it hosts, the answers it is waiting for, and the settings they share.

pub mod activation;
pub mod catalog;
pub mod cluster;
pub mod context;
pub mod replies;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use casty_core::mailbox::{Backoff, Start};
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_core::store::{LocalStore, Pages};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple, PyType};

use crate::actor::Behavior;
use crate::awaited::Awaited;
use crate::refs::Ref;
use crate::schema::Schema;
use activation::Activation;
use catalog::Catalog;
use cluster::{Ending, Joined, Taken};
use replies::{Replies, Waiting};

/// The schema of the answer of each `ask` builder, held by the builder it was read from.
type Answers = HashMap<usize, (Py<PyAny>, Py<Schema>)>;

/// The periods a system runs by, in seconds.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub idle_after: f64,
    pub ask_timeout: f64,
    pub write_timeout: f64,
    pub leave_timeout: f64,
    pub backoff_first: f64,
    pub backoff_limit: f64,
    pub backoff_factor: f64,
}

/// Everything a node owns. Nothing of it is global to the process: a second system is a second one of these.
#[derive(Debug)]
pub struct Node {
    pub settings: Settings,
    /// Where this node is, which the cluster replaces by the identity it joined under.
    id: Mutex<NodeId>,
    /// The cluster this node is in, from the moment the system enters. Without one it runs alone.
    joined: Mutex<Option<Arc<Joined>>>,
    /// The network this node joins with, kept so that it can join again under another identity.
    network: Mutex<Option<Py<PyAny>>>,
    /// What the cluster last said its members are, which is what `system.members` answers.
    members: Mutex<Vec<casty_node::membership::service::Member>>,
    /// What watches the pages this node writes, when a test asked to see them.
    writes: Mutex<Option<Py<Writes>>>,
    running: Mutex<Option<Py<PyAny>>>,
    system: Mutex<Option<Py<PyAny>>>,
    answers: Mutex<Answers>,
    catalog: Mutex<Catalog>,
    store: Mutex<LocalStore>,
    replies: Mutex<Replies>,
    activations: Mutex<HashMap<(String, String), Py<Activation>>>,
    leaving: Mutex<Option<Py<PyAny>>>,
    /// Whether this node takes no new message: it is on its way out and finishing what it is on.
    draining: AtomicBool,
    stopped: AtomicBool,
}

impl Node {
    fn new(settings: Settings, id: NodeId) -> Self {
        Self {
            settings,
            id: Mutex::new(id),
            joined: Mutex::new(None),
            network: Mutex::new(None),
            members: Mutex::new(Vec::new()),
            writes: Mutex::new(None),
            running: Mutex::new(None),
            system: Mutex::new(None),
            answers: Mutex::new(HashMap::new()),
            catalog: Mutex::new(Catalog::default()),
            store: Mutex::new(LocalStore::default()),
            replies: Mutex::new(Replies::default()),
            activations: Mutex::new(HashMap::new()),
            leaving: Mutex::new(None),
            draining: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }
    }

    /// Whether a ref of this node still reaches anything. It stops only once the system is out of the cluster.
    #[must_use]
    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Whether this node takes new messages. It stops taking them at the start of the exit, while bodies finish.
    #[must_use]
    pub fn draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Whether a message that reaches this node is refused because it is leaving.
    ///
    /// In a cluster it never is. The node of the cluster refuses it there, in the same step that takes this node out
    /// of the choice of owner, so a message that arrives after that is sent on to whoever owns the key now instead
    /// of being answered with a failure. What is already queued here goes the same way when the activation ends.
    #[must_use]
    fn refusing(&self) -> bool {
        self.draining() && self.cluster().is_none()
    }

    /// Nothing reaches this node any more, which is what a ref of it raises on.
    pub fn stop_taking(&self) {
        self.draining.store(true, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Where this node is. Alone it is an incarnation with no address; in a cluster it is the identity it joined under.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
            .lock()
            .expect("the node lock is never poisoned")
            .clone()
    }

    /// The cluster this node is in, if it is in one.
    #[must_use]
    pub fn cluster(&self) -> Option<Arc<Joined>> {
        self.joined
            .lock()
            .expect("the node lock is never poisoned")
            .clone()
            .filter(|joined| joined.ready())
    }

    /// The members of the cluster as this node last saw them. Alone, it is the only one.
    #[must_use]
    pub fn members(&self) -> Vec<casty_node::membership::service::Member> {
        self.members
            .lock()
            .expect("the node lock is never poisoned")
            .clone()
    }

    /// Take the members the cluster reports, which is what `system.members` reads.
    ///
    /// Their addresses are asked of the map here, where the interpreter is, so that a dial does not have to wait.
    pub fn seen(&self, py: Python<'_>, members: Vec<casty_node::membership::service::Member>) {
        if let Some(cluster) = self.cluster() {
            let _ = cluster.learn(py, &members);
        }
        *self
            .members
            .lock()
            .expect("the node lock is never poisoned") = members;
    }

    /// A type the cluster named: importing it is what tells the node whether this process has it.
    pub fn met(self: &Arc<Self>, py: Python<'_>, actor: &str) {
        let _ = self.resolve(py, actor);
    }

    /// Start joining the cluster `settings` names. `entered` is resolved with `system` once the node is in.
    pub fn join(
        self: &Arc<Self>,
        py: Python<'_>,
        network: &Bound<'_, PyAny>,
        running_loop: Py<PyAny>,
        entered: &Bound<'_, PyAny>,
        system: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let backoff = Backoff {
            first: core::time::Duration::from_secs_f64(self.settings.backoff_first),
            limit: core::time::Duration::from_secs_f64(self.settings.backoff_limit),
            factor: self.settings.backoff_factor,
        };
        let map = network.getattr("address_map")?;
        let map = if map.is_none() { None } else { Some(map) };
        let settings = cluster::settings(
            network,
            self.settings.write_timeout,
            self.settings.leave_timeout,
            backoff,
        )?;
        *self
            .network
            .lock()
            .expect("the node lock is never poisoned") = Some(network.clone().unbind());
        self.enter(
            py,
            settings,
            map.as_ref(),
            running_loop,
            entered,
            system,
            true,
        )
    }

    /// Start reaching a cluster, as one of its nodes or as a client of it.
    #[allow(clippy::too_many_arguments)]
    pub fn enter(
        self: &Arc<Self>,
        py: Python<'_>,
        settings: cluster::Settings,
        map: Option<&Bound<'_, PyAny>>,
        running_loop: Py<PyAny>,
        entered: &Bound<'_, PyAny>,
        system: &Bound<'_, PyAny>,
        member: bool,
    ) -> PyResult<()> {
        let types = self
            .catalog
            .lock()
            .expect("the node lock is never poisoned")
            .kinds();
        let joined = Joined::start(
            py,
            self,
            settings,
            map,
            types,
            running_loop,
            entered.clone().unbind(),
            system.clone().unbind(),
            member,
        )?;
        *self.joined.lock().expect("the node lock is never poisoned") = Some(joined);
        Ok(())
    }

    /// The cluster declared this node left: it forgets what it knew and joins again as a new node.
    ///
    /// Two of five are not a majority, so a minority that was removed while it was cut off comes back this way once
    /// the partition heals, and under identities the others have never seen.
    pub fn removed(self: &Arc<Self>, py: Python<'_>) -> PyResult<()> {
        if self.draining() {
            return Ok(());
        }
        let activations: Vec<Py<Activation>> = self
            .activations
            .lock()
            .expect("the node lock is never poisoned")
            .drain()
            .map(|(_, activation)| activation)
            .collect();
        for activation in activations {
            let _ = Activation::release(activation.bind(py), py);
        }
        // The identity that was removed goes without a word: the cluster already buried it.
        let shed = self
            .joined
            .lock()
            .expect("the node lock is never poisoned")
            .take();
        match shed {
            Some(shed) => Joined::leave(&shed, py, self, true, Ending::Again),
            None => self.rejoin(py)?,
        }
        Ok(())
    }

    /// Join again as a new node, on the address the identity that was removed had.
    pub fn rejoin(self: &Arc<Self>, py: Python<'_>) -> PyResult<()> {
        let network = self
            .network
            .lock()
            .expect("the node lock is never poisoned")
            .as_ref()
            .map(|network| network.clone_ref(py));
        let (Some(network), Ok(system), Ok(running)) = (network, self.system(py), self.running(py))
        else {
            return Ok(());
        };
        let entered = self.future(py)?;
        self.join(
            py,
            network.bind(py),
            running.unbind(),
            &entered,
            system.bind(py),
        )
    }

    /// The node is in the cluster: take the identity it joined under and what it sees.
    pub fn entered(&self, py: Python<'_>, joined: &Arc<Joined>) {
        *self.id.lock().expect("the node lock is never poisoned") = joined.id().clone();
        let members = joined.members();
        let _ = joined.learn(py, &members);
        *self
            .members
            .lock()
            .expect("the node lock is never poisoned") = members;
    }

    /// Say goodbye to the cluster and stop the transport, resolving `gone` once it is over.
    ///
    /// The cluster is let go of only when the leave has finished: what it holds is the runtime the leave runs on.
    pub fn departed(self: &Arc<Self>, py: Python<'_>, abort: bool, gone: &Bound<'_, PyAny>) {
        let joined = self
            .joined
            .lock()
            .expect("the node lock is never poisoned")
            .clone();
        if let Some(joined) = joined {
            Joined::leave(
                &joined,
                py,
                self,
                abort,
                Ending::Gone(gone.clone().unbind()),
            );
            return;
        }
        self.stop_taking();
        if !gone
            .call_method0("done")
            .is_ok_and(|done| done.is_truthy().unwrap_or(true))
        {
            let _ = gone.call_method1("set_result", (py.None(),));
        }
    }

    /// Let go of `joined`, unless this node has already taken another one in its place.
    pub fn let_go(&self, joined: &Arc<Joined>) {
        let mut held = self.joined.lock().expect("the node lock is never poisoned");
        if held.as_ref().is_some_and(|held| Arc::ptr_eq(held, joined)) {
            *held = None;
        }
    }

    /// End the activation of a key whose owner moved: a write from it would be refused by the replicas anyway.
    pub fn relinquish(self: &Arc<Self>, py: Python<'_>, actor: &str, key: &str) -> PyResult<()> {
        let held = self
            .activations
            .lock()
            .expect("the node lock is never poisoned")
            .get(&(actor.to_owned(), key.to_owned()))
            .map(|activation| activation.clone_ref(py));
        match held {
            Some(activation) => Activation::release(activation.bind(py), py),
            None => Ok(()),
        }
    }

    /// The loop this node runs on, which is the one that owns every future it hands out.
    pub fn running<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &*self
            .running
            .lock()
            .expect("the node lock is never poisoned")
        {
            Some(running) => Ok(running.bind(py).clone()),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the system has not started",
            )),
        }
    }

    /// The loop of a system that is running. A system that has not started, or has exited, has none.
    pub fn started<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.stopped() {
            return Err(context::stopped());
        }
        self.running(py)
    }

    /// The system this node belongs to, which is what a body reaches through `ctx.system`.
    pub fn system(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &*self.system.lock().expect("the node lock is never poisoned") {
            Some(system) => Ok(system.clone_ref(py)),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the system has not started",
            )),
        }
    }

    /// The schema of the answer of `ask(build, ...)`, from the annotation of the first parameter of `build`.
    pub fn answers(&self, py: Python<'_>, build: &Bound<'_, PyAny>) -> PyResult<Py<Schema>> {
        let at = build.as_ptr() as usize;
        {
            let answers = self
                .answers
                .lock()
                .expect("the node lock is never poisoned");
            if let Some((held, schema)) = answers.get(&at)
                && held.bind(py).is(build)
            {
                return Ok(schema.clone_ref(py));
            }
        }
        let schema = crate::schema::reply_schema(py, build)?;
        let schema = Bound::new(py, schema)?.unbind();
        self.answers
            .lock()
            .expect("the node lock is never poisoned")
            .insert(at, (build.clone().unbind(), schema.clone_ref(py)));
        Ok(schema)
    }

    pub fn future<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.running(py)?.call_method0("create_future")
    }

    /// What the loop calls now, which is what a deadline is measured against.
    pub fn now(&self, py: Python<'_>) -> PyResult<f64> {
        self.running(py)?.call_method0("time")?.extract()
    }

    /// The wall clock, which is what a deadline that travels between nodes is written in.
    #[allow(clippy::unused_self)]
    pub fn clock(&self, py: Python<'_>) -> PyResult<f64> {
        py.import("time")?.call_method0("time")?.extract()
    }

    pub fn later<'py>(
        &self,
        py: Python<'py>,
        delay: f64,
        call: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.running(py)?.call_method1("call_later", (delay, call))
    }

    pub fn learn(&self, py: Python<'_>, behavior: &Behavior) {
        self.catalog
            .lock()
            .expect("the node lock is never poisoned")
            .learn(behavior, py);
        if let Some(cluster) = self.cluster() {
            let definition = behavior.definition();
            cluster.node().learn(casty_node::node::Kind {
                actor: definition.name.clone(),
                replicas: definition.replicas,
                write: definition.write,
            });
        }
    }

    /// The type called `name`, imported when this system has not met it yet.
    #[must_use]
    pub fn resolve(&self, py: Python<'_>, name: &str) -> Option<Behavior> {
        {
            let catalog = self
                .catalog
                .lock()
                .expect("the node lock is never poisoned");
            if let Some(known) = catalog.known(name) {
                return Some(known.clone_ref(py));
            }
            if catalog.gave_up(name) {
                return None;
            }
        }
        // The import runs without the lock: it is arbitrary Python, and it can reach back into this node.
        let found = catalog::imported(py, name);
        let mut catalog = self
            .catalog
            .lock()
            .expect("the node lock is never poisoned");
        let Some(behavior) = found else {
            catalog.give_up(name);
            drop(catalog);
            if let Some(cluster) = self.cluster() {
                cluster.node().gave_up(name);
            }
            return None;
        };
        catalog.learn(&behavior, py);
        drop(catalog);
        // A type met by importing it is one this node hosts, so the cluster and the rings hear of it too.
        if let Some(cluster) = self.cluster() {
            let definition = behavior.definition();
            cluster.node().learn(casty_node::node::Kind {
                actor: definition.name.clone(),
                replicas: definition.replicas,
                write: definition.write,
            });
        }
        Some(behavior)
    }

    pub fn store(&self) -> std::sync::MutexGuard<'_, LocalStore> {
        self.store.lock().expect("the node lock is never poisoned")
    }

    /// Send `command` to the key it names, wherever that key is.
    ///
    /// In a cluster the node decides where it goes and hands it back here if this is the node that owns the key.
    pub fn hand(
        self: &Arc<Self>,
        py: Python<'_>,
        command: casty_core::mailbox::Command,
    ) -> PyResult<()> {
        let Some(cluster) = self.cluster() else {
            return self.take(py, command);
        };
        match command {
            casty_core::mailbox::Command::Deliver(deliver) => {
                cluster.node().deliver(
                    &deliver.actor,
                    &deliver.key,
                    deliver.message,
                    deliver.reply,
                );
            }
            casty_core::mailbox::Command::Start(start) => {
                cluster.node().start(&start.actor, &start.key, start.state);
            }
        }
        Ok(())
    }

    /// Send `command` to the activation of its key here, starting one when the key has none.
    pub fn take(
        self: &Arc<Self>,
        py: Python<'_>,
        command: casty_core::mailbox::Command,
    ) -> PyResult<()> {
        if self.refusing() {
            return self.refuse(
                py,
                &command,
                "the node is shutting down",
                Outcome::unreached,
            );
        }
        let Some(activation) = self.attach(py, command.actor(), command.key())? else {
            return self.refuse(
                py,
                &command,
                "this node does not have the type",
                Outcome::unknown,
            );
        };
        Activation::put(activation.bind(py), py, command)
    }

    /// The activation of `key`, started if it has none. Nothing when this node does not have the type.
    pub fn attach(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
    ) -> PyResult<Option<Py<Activation>>> {
        let at = (actor.to_owned(), key.to_owned());
        {
            let activations = self
                .activations
                .lock()
                .expect("the node lock is never poisoned");
            if let Some(found) = activations.get(&at) {
                return Ok(Some(found.clone_ref(py)));
            }
        }
        let Some(behavior) = self.resolve(py, actor) else {
            return Ok(None);
        };
        let started = Activation::new(py, self, &behavior, actor, key)?;
        let held = {
            let mut activations = self
                .activations
                .lock()
                .expect("the node lock is never poisoned");
            activations
                .entry(at)
                .or_insert_with(|| started.clone_ref(py))
                .clone_ref(py)
        };
        if held.is(&started) {
            Activation::begin(held.bind(py), py)?;
        }
        Ok(Some(held))
    }

    pub fn remove(&self, actor: &str, key: &str) -> Option<Py<Activation>> {
        self.activations
            .lock()
            .expect("the node lock is never poisoned")
            .remove(&(actor.to_owned(), key.to_owned()))
    }

    /// Answer the request `target` is waiting for, wherever it waits.
    pub fn answer(
        self: &Arc<Self>,
        py: Python<'_>,
        target: &Target,
        outcome: &Outcome,
    ) -> PyResult<()> {
        let Target::Reply { node, id } = target else {
            return Ok(());
        };
        if let Some(cluster) = self.cluster()
            && node != cluster.id()
        {
            cluster.node().answer(target.clone(), outcome.clone());
            return Ok(());
        }
        self.settle(py, *id, outcome)
    }

    /// Answer the request numbered `id`, which something on this node is waiting for.
    pub fn settle(self: &Arc<Self>, py: Python<'_>, id: i64, outcome: &Outcome) -> PyResult<()> {
        let waiting = self
            .replies
            .lock()
            .expect("the node lock is never poisoned")
            .forget(id);
        match waiting {
            Some(waiting) => replies::settle(py, &waiting, outcome, self),
            None => Ok(()),
        }
    }

    /// Take the key over, resolving the future it gives back with what the replicas hold.
    ///
    /// Alone, that is the local store and the answer is there at once; in a cluster it is an operation on the
    /// replicas, and the future resolves when they have answered.
    pub fn activate<'py>(
        self: &Arc<Self>,
        py: Python<'py>,
        actor: &str,
        key: &str,
        initial: Option<Pages>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let taken = self.future(py)?;
        if let Some(cluster) = self.cluster() {
            cluster.activate(py, actor, key, initial, taken.clone().unbind());
        } else {
            let held = self.store().activate(actor, key, initial);
            taken.call_method1("set_result", (Bound::new(py, Taken::of(held))?,))?;
        }
        Ok(taken)
    }

    /// Write the state of the key, resolving the future it gives back once it is written.
    pub fn commit<'py>(
        self: &Arc<Self>,
        py: Python<'py>,
        actor: &str,
        key: &str,
        pages: Pages,
        active: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let watching = self
            .writes
            .lock()
            .expect("the node lock is never poisoned")
            .as_ref()
            .map(|writes| writes.clone_ref(py));
        if let Some(watching) = watching {
            watching.bind(py).get().saw(py, &pages)?;
        }
        let written = self.future(py)?;
        if let Some(cluster) = self.cluster() {
            cluster.commit(py, actor, key, pages, active, written.clone().unbind());
        } else {
            self.store().commit(actor, key, pages, active);
            written.call_method1("set_result", (py.None(),))?;
        }
        Ok(written)
    }

    /// Start waiting for an answer, decoded with `schema`, and give back the id it comes addressed to.
    #[must_use]
    pub fn request(&self, py: Python<'_>, schema: &Py<Schema>, future: &Bound<'_, PyAny>) -> i64 {
        self.waited(future, Some(schema.clone_ref(py)))
    }

    /// Start waiting for an answer whose value nobody reads, which is what a native body asks for.
    #[must_use]
    pub fn awaited(&self, future: &Bound<'_, PyAny>) -> i64 {
        self.waited(future, None)
    }

    fn waited(&self, future: &Bound<'_, PyAny>, schema: Option<Py<Schema>>) -> i64 {
        let mut replies = self
            .replies
            .lock()
            .expect("the node lock is never poisoned");
        let id = replies.take();
        replies.wait(
            id,
            Waiting {
                future: future.clone().unbind(),
                schema,
                deadline: None,
            },
        );
        id
    }

    pub fn deadline(&self, py: Python<'_>, id: i64, timer: &Bound<'_, PyAny>) {
        let mut replies = self
            .replies
            .lock()
            .expect("the node lock is never poisoned");
        if let Some(waiting) = replies.forget(id) {
            replies.wait(
                id,
                Waiting {
                    deadline: Some(timer.clone().unbind()),
                    ..waiting
                },
            );
        }
        let _ = py;
    }

    pub fn forget(&self, id: i64) -> Option<Waiting> {
        self.replies
            .lock()
            .expect("the node lock is never poisoned")
            .forget(id)
    }

    fn refuse(
        self: &Arc<Self>,
        py: Python<'_>,
        command: &casty_core::mailbox::Command,
        why: &str,
        outcome: fn(&str, &str) -> Outcome,
    ) -> PyResult<()> {
        match command.reply() {
            None => {
                dropped(py, command.actor(), command.key(), why);
                Ok(())
            }
            Some(target) => self.answer(py, target, &outcome(command.actor(), command.key())),
        }
    }

    /// Take no more: a ref of this node raises from here on, and no activation goes further.
    fn stop(self: &Arc<Self>, py: Python<'_>) {
        self.stop_taking();
        let running: Vec<Py<Activation>> = self
            .activations
            .lock()
            .expect("the node lock is never poisoned")
            .values()
            .map(|activation| activation.clone_ref(py))
            .collect();
        for activation in running {
            let _ = Activation::release(activation.bind(py), py);
        }
    }

    /// End every activation after the message it is on: this node is shutting down.
    fn leave<'py>(self: &Arc<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Only new messages are refused here. A body finishing the one it is on still answers it, and the reply of
        // its `ask` goes out before the system closes.
        self.draining.store(true, Ordering::SeqCst);
        let waiting = self.future(py)?;
        let running: Vec<Py<Activation>> = self
            .activations
            .lock()
            .expect("the node lock is never poisoned")
            .values()
            .map(|activation| activation.clone_ref(py))
            .collect();
        if running.is_empty() {
            waiting.call_method1("set_result", (py.None(),))?;
            return Ok(waiting);
        }
        *self
            .leaving
            .lock()
            .expect("the node lock is never poisoned") = Some(waiting.clone().unbind());
        for activation in running {
            Activation::drain(activation.bind(py), py)?;
        }
        // The deadline is the deadline: a body that never ends does not keep the process alive.
        let abandon = Bound::new(
            py,
            Abandoning {
                node: Arc::clone(self),
            },
        )?;
        self.later(py, self.settings.leave_timeout, abandon.into_any())?;
        Ok(waiting)
    }

    /// Give up on what `drain` did not finish: every activation left gives its key up where it is.
    pub fn abandon(self: &Arc<Self>, py: Python<'_>) {
        let running: Vec<Py<Activation>> = self
            .activations
            .lock()
            .expect("the node lock is never poisoned")
            .values()
            .map(|activation| activation.clone_ref(py))
            .collect();
        for activation in running {
            let _ = Activation::release(activation.bind(py), py);
        }
    }

    /// Called by an activation that ended: the last one out closes the shutdown.
    pub fn ended(&self, py: Python<'_>) -> PyResult<()> {
        let empty = self
            .activations
            .lock()
            .expect("the node lock is never poisoned")
            .is_empty();
        if !empty {
            return Ok(());
        }
        let waiting = self
            .leaving
            .lock()
            .expect("the node lock is never poisoned")
            .take();
        if let Some(waiting) = waiting {
            let waiting = waiting.bind(py);
            if !waiting.call_method0("done")?.is_truthy()? {
                waiting.call_method1("set_result", (py.None(),))?;
            }
        }
        Ok(())
    }
}

/// The member table as a node or a client of the cluster answers it.
///
/// A node running alone is the only member there is, and it lists itself with the types this process holds.
fn listed<'py>(node: &Arc<Node>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    node.started(py)?;
    let listed = py.import("casty")?.getattr("Member")?;
    let frozen = py.import("builtins")?.getattr("frozenset")?;
    let seen = node.members();
    if seen.is_empty() && node.cluster().is_none() {
        let types: Vec<String> = node
            .catalog
            .lock()
            .expect("the node lock is never poisoned")
            .kinds()
            .into_iter()
            .map(|kind| kind.actor)
            .collect();
        let alone = listed.call1((identity(py, &node.id())?, "alive", frozen.call1((types,))?))?;
        return Ok(PyTuple::new(py, [alone])?.into_any());
    }
    let mut members: Vec<Bound<'py, PyAny>> = Vec::with_capacity(seen.len());
    for member in seen {
        let types: Vec<String> = member.types.into_iter().collect();
        members.push(listed.call1((
            identity(py, &member.node)?,
            member.status.name(),
            frozen.call1((types,))?,
        ))?);
    }
    Ok(PyTuple::new(py, members)?.into_any())
}

/// What a collection stores: the value in the canonical order its schema was compiled in.
fn encoded<'py>(
    py: Python<'py>,
    schema: &Bound<'py, Schema>,
    value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let written = Schema::write(schema, schema.get().tree().sent(), value)?;
    Ok(PyBytes::new(py, &written))
}

/// What a collection stored, with every ref in it bound to the node that can reach what it points at.
fn decoded<'py>(
    node: &Arc<Node>,
    schema: &Bound<'py, Schema>,
    data: &[u8],
) -> PyResult<Bound<'py, PyAny>> {
    Ok(Schema::read(
        schema,
        schema.get().tree().sent(),
        data,
        Some(node),
    )?)
}

/// A message nobody is waiting for, which ends here.
pub fn dropped(py: Python<'_>, actor: &str, key: &str, why: &str) {
    let _ = log(py, format!("dropped a message to {actor}/{key}: {why}"));
}

pub fn log(py: Python<'_>, message: String) -> PyResult<()> {
    py.import("logging")?
        .call_method1("getLogger", ("casty",))?
        .call_method1("warning", ("%s", message))?;
    Ok(())
}

/// A node. It hosts every actor type it meets: the ones this process uses, and the ones the cluster tells it of.
#[pyclass(frozen, module = "casty._casty", subclass)]
#[derive(Debug)]
pub struct ActorSystem {
    node: Arc<Node>,
    /// The network of this node, as the constructor took it. Without one it runs alone.
    cluster: Option<Py<PyAny>>,
}

#[pymethods]
impl ActorSystem {
    #[new]
    // The settings of a system, which is what the constructor takes.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *_extra,
        cluster = None,
        codec = "msgpack",
        idle_after = None,
        backoff = None,
        ask_timeout = None,
        write_timeout = None,
        leave_timeout = None,
    ))]
    fn new(
        py: Python<'_>,
        // A subclass of its own may take arguments: what `object.__new__` ignores when `__init__` is overridden.
        _extra: &Bound<'_, pyo3::types::PyTuple>,
        cluster: Option<&Bound<'_, PyAny>>,
        codec: &str,
        idle_after: Option<&Bound<'_, PyAny>>,
        backoff: Option<&Bound<'_, PyAny>>,
        ask_timeout: Option<&Bound<'_, PyAny>>,
        write_timeout: Option<&Bound<'_, PyAny>>,
        leave_timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if codec != "msgpack" {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "codec is {codec:?}, and the only format is 'msgpack'"
            )));
        }
        let (first, limit, factor) = match backoff {
            None => (0.1, 10.0, 2.0),
            Some(backoff) => (
                seconds(&backoff.getattr("first")?)?,
                seconds(&backoff.getattr("limit")?)?,
                backoff.getattr("factor")?.extract()?,
            ),
        };
        let settings = Settings {
            idle_after: optional(idle_after, 60.0)?,
            ask_timeout: optional(ask_timeout, 10.0)?,
            write_timeout: optional(write_timeout, 5.0)?,
            leave_timeout: optional(leave_timeout, 30.0)?,
            backoff_first: first,
            backoff_limit: limit,
            backoff_factor: factor,
        };
        let id = NodeId {
            address: None,
            incarnation: incarnation(py)?,
        };
        Ok(Self {
            node: Arc::new(Node::new(settings, id)),
            cluster: match cluster {
                Some(cluster) if !cluster.is_none() => Some(cluster.clone().unbind()),
                _ => None,
            },
        })
    }

    /// Everything a system is built from is read by `__new__`; this is here so that a subclass can call it.
    #[pyo3(signature = (*_args, **_kwargs))]
    #[allow(clippy::unused_self)]
    fn __init__(&self, _args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) {}

    fn __aenter__<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let node = &slf.get().node;
        let running = py.import("asyncio")?.call_method0("get_running_loop")?;
        *node
            .running
            .lock()
            .expect("the node lock is never poisoned") = Some(running.clone().unbind());
        *node.system.lock().expect("the node lock is never poisoned") =
            Some(slf.clone().into_any().unbind());
        let entered = node.future(py)?;
        match &slf.get().cluster {
            // The join is answered by the loop, so it runs on the transport and comes back when it is done.
            Some(cluster) => {
                node.join(
                    py,
                    cluster.bind(py),
                    running.unbind(),
                    &entered,
                    slf.as_any(),
                )?;
            }
            None => {
                entered.call_method1("set_result", (slf.clone(),))?;
            }
        }
        Ok(Bound::new(py, Awaited::of(entered))?.into_any())
    }

    /// Leaving by an exception is the process going away: nothing is drained and the transport aborts.
    #[pyo3(signature = (*exc))]
    fn __aexit__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        exc: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let crashed = exc.iter().any(|part| !part.is_none());
        let node = slf.get().node.clone();
        let gone = node.future(py)?;
        if crashed {
            node.stop(py);
            node.departed(py, true, &gone);
            return Ok(Bound::new(py, Awaited::of(gone))?.into_any());
        }
        // The cluster hears this first: while the bodies finish what they are on, nothing new is routed here.
        if let Some(cluster) = node.cluster() {
            cluster.node().leaving();
        }
        let left = node.leave(py)?;
        let departing = Bound::new(
            py,
            Departing {
                node,
                gone: gone.clone().unbind(),
            },
        )?;
        left.call_method1("add_done_callback", (departing,))?;
        Ok(Bound::new(py, Awaited::of(gone))?.into_any())
    }

    /// The members of the cluster as this node sees them, itself included.
    #[getter]
    fn members<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        listed(&self.node, py)
    }

    /// Meet `actor` before the cluster names it, so this node runs it and not what the import would bring.
    ///
    /// A test hook, not part of the API: it is how a node of another deploy is built inside one process.
    #[pyo3(name = "_learn")]
    fn learned(&self, py: Python<'_>, actor: &Bound<'_, PyAny>) -> PyResult<()> {
        self.node.learn(py, &Behavior::of(actor)?);
        Ok(())
    }

    /// `annotation` compiled in the canonical order a stored value is compared in.
    #[pyo3(name = "_schema")]
    #[allow(clippy::unused_self)]
    fn compiled(&self, py: Python<'_>, annotation: &Bound<'_, PyAny>) -> PyResult<Schema> {
        Ok(Schema::of(py, annotation, true)?)
    }

    /// The type called `name`, imported from where it lives. Nothing when this process does not have it.
    ///
    /// A test hook, not part of the API: it is how a test sees what a node finds when a name reaches it.
    #[pyo3(name = "_resolve")]
    fn resolved(&self, py: Python<'_>, name: &str) -> Option<Py<PyAny>> {
        self.node
            .resolve(py, name)
            .map(|behavior| behavior.held(py))
    }

    /// Watch the pages this node writes from here on.
    ///
    /// A test hook, not part of the API.
    #[pyo3(name = "_writes")]
    fn watching(&self, py: Python<'_>) -> PyResult<Py<Writes>> {
        let writes = Bound::new(
            py,
            Writes {
                seen: PyList::empty(py).unbind(),
                refuse: Mutex::new(None),
            },
        )?
        .unbind();
        *self
            .node
            .writes
            .lock()
            .expect("the node lock is never poisoned") = Some(writes.clone_ref(py));
        Ok(writes)
    }

    /// `value` as the bytes a collection stores.
    #[pyo3(name = "_encode")]
    #[allow(clippy::unused_self)]
    fn encoded<'py>(
        &self,
        py: Python<'py>,
        schema: &Bound<'py, Schema>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        encoded(py, schema, value)
    }

    /// The value `data` holds, with every ref in it bound to this system.
    #[pyo3(name = "_decode")]
    fn decoded<'py>(
        &self,
        schema: &Bound<'py, Schema>,
        data: &[u8],
    ) -> PyResult<Bound<'py, PyAny>> {
        decoded(&self.node, schema, data)
    }

    /// Reference to the entity `(actor, key)`, wherever it is placed. It creates the key if it has to, and activates it.
    #[pyo3(name = "ref", signature = (actor, key, /, *, initial = None))]
    fn reference(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        actor: &Bound<'_, PyAny>,
        key: &str,
        initial: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Ref> {
        Self::referenced(&slf.get().node, py, actor, key, initial)
    }

    #[getter]
    fn node(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.node.started(py)?;
        identity(py, &self.node.id())
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        class: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::generic::alias(class, &crate::generic::subscript(item))
    }
}

impl ActorSystem {
    #[must_use]
    pub fn node_of(&self) -> &Arc<Node> {
        &self.node
    }

    /// Reference to the entity `(actor, key)`, which is the same from a node of the cluster and from a client.
    ///
    /// Obtaining it asks the owner to create the key, if it does not exist, and to activate it. Nobody waits for
    /// that: what goes wrong with it shows in the first `ask`.
    fn referenced(
        node: &Arc<Node>,
        py: Python<'_>,
        actor: &Bound<'_, PyAny>,
        key: &str,
        initial: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Ref> {
        node.started(py)?;
        let behavior = Behavior::of(actor)?;
        node.learn(py, &behavior);
        let definition = behavior.definition();
        let state = definition.state.bind(py);
        let written: Option<Vec<u8>> = match (initial, &definition.initial) {
            (Some(initial), _) => Some(state.call_method1("dump", (initial,))?.extract()?),
            (None, Some(_)) => None,
            // A type without a default starts from nothing, which only a state that can be `None` allows.
            (None, None) => match state.call_method1("dump", (py.None(),)) {
                Ok(nothing) => Some(nothing.extract()?),
                Err(_) => {
                    return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                        "{} has no default initial state, and its state cannot be None: pass initial=",
                        definition.name
                    )));
                }
            },
        };
        node.hand(
            py,
            casty_core::mailbox::Command::Start(Start {
                actor: definition.name.clone(),
                key: key.to_owned(),
                state: written,
            }),
        )?;
        Ok(Ref::entity_of(
            definition.messages.clone_ref(py),
            definition.name.clone(),
            key.to_owned(),
            Some(node.clone()),
        ))
    }
}

/// End the request at `within`, so that nothing waits for an answer that is not coming.
pub fn armed(
    py: Python<'_>,
    node: &Arc<Node>,
    id: i64,
    answer: &Bound<'_, PyAny>,
    within: f64,
) -> PyResult<()> {
    let expire = Bound::new(
        py,
        Expire {
            node: node.clone(),
            id,
        },
    )?;
    let timer = node.later(py, within, expire.into_any())?;
    node.deadline(py, id, &timer);
    let _ = answer;
    Ok(())
}

/// What a request that nothing answered by its deadline ends with.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Expire {
    node: Arc<Node>,
    id: i64,
}

#[pymethods]
impl Expire {
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let Some(waiting) = self.node.forget(self.id) else {
            return Ok(());
        };
        let future = waiting.future.bind(py);
        if !future.call_method0("done")?.is_truthy()? {
            let timeout =
                pyo3::exceptions::PyTimeoutError::new_err("the answer did not arrive in time");
            future.call_method1("set_exception", (timeout,))?;
        }
        Ok(())
    }
}

/// `NodeId` as the facade still declares it, built from what the core holds.
/// A `NodeId` as the core holds it, read from the value the facade hands around.
pub fn read_identity(value: &Bound<'_, PyAny>) -> PyResult<NodeId> {
    let incarnation: Vec<u8> = value.getattr("incarnation")?.getattr("bytes")?.extract()?;
    Ok(NodeId {
        address: value.getattr("address")?.extract()?,
        incarnation: <[u8; 16]>::try_from(incarnation.as_slice()).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err("an incarnation is sixteen bytes")
        })?,
    })
}

/// The nodes that keep `key` among `nodes`, the first being the one the ring gives it to.
///
/// A test hook, not part of the API: it is how a test says which node to take away.
#[pyfunction]
pub fn replicas(
    py: Python<'_>,
    actor: &str,
    key: &str,
    nodes: &Bound<'_, PyAny>,
    count: usize,
) -> PyResult<Vec<Py<PyAny>>> {
    let held: PyResult<Vec<NodeId>> = nodes
        .try_iter()?
        .map(|node| read_identity(&node?))
        .collect();
    let ring = casty_core::placement::Ring::build(held?, casty_node::placement::VNODES);
    ring.replicas(casty_core::placement::token(actor, key), count)
        .iter()
        .map(|node| identity(py, node))
        .collect()
}

pub fn identity(py: Python<'_>, id: &NodeId) -> PyResult<Py<PyAny>> {
    let class = py.import("casty")?.getattr("NodeId")?;
    let uuid = py.import("uuid")?.getattr("UUID")?;
    let named = PyDict::new(py);
    named.set_item("bytes", PyBytes::new(py, &id.incarnation))?;
    let incarnation = uuid.call((), Some(&named))?;
    Ok(class.call1((id.address.clone(), incarnation))?.unbind())
}

fn incarnation(py: Python<'_>) -> PyResult<[u8; 16]> {
    let raw: Vec<u8> = py
        .import("uuid")?
        .call_method0("uuid4")?
        .getattr("bytes")?
        .extract()?;
    raw.try_into()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err("a uuid is sixteen bytes"))
}

fn optional(value: Option<&Bound<'_, PyAny>>, default: f64) -> PyResult<f64> {
    match value {
        None => Ok(default),
        Some(value) => seconds(value),
    }
}

fn seconds(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    value.call_method0("total_seconds")?.extract()
}

/// What leaves the cluster once every activation of this node has ended.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Departing {
    node: Arc<Node>,
    gone: Py<PyAny>,
}

#[pymethods]
impl Departing {
    #[pyo3(signature = (*_args))]
    fn __call__(&self, py: Python<'_>, _args: &Bound<'_, PyTuple>) {
        self.node.departed(py, false, self.gone.bind(py));
    }
}

/// What ends the activations that did not finish before the deadline of the exit.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
struct Abandoning {
    node: Arc<Node>,
}

#[pymethods]
impl Abandoning {
    fn __call__(&self, py: Python<'_>) {
        self.node.abandon(py);
    }
}

/// Sends messages to the actors of a cluster, without hosting keys or joining the membership.
///
/// `__aenter__` returns after the first member table is received from a seed.
#[pyclass(frozen, module = "casty._casty", subclass)]
#[derive(Debug)]
pub struct Client {
    node: Arc<Node>,
    settings: cluster::Settings,
    map: Option<Py<PyAny>>,
}

#[pymethods]
impl Client {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *_extra,
        seeds,
        name = "casty",
        codec = "msgpack",
        tls = None,
        compression = None,
        address_map = None,
        ask_timeout = None,
        sync_every = None,
    ))]
    fn new(
        py: Python<'_>,
        _extra: &Bound<'_, PyTuple>,
        seeds: Vec<String>,
        name: &str,
        codec: &str,
        tls: Option<&Bound<'_, PyAny>>,
        compression: Option<&Bound<'_, PyAny>>,
        address_map: Option<&Bound<'_, PyAny>>,
        ask_timeout: Option<&Bound<'_, PyAny>>,
        sync_every: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if codec != "msgpack" {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "codec is {codec:?}, and the only format is 'msgpack'"
            )));
        }
        if seeds.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "seeds is empty, and a client reaches a cluster only through a seed",
            ));
        }
        let checking = py.import("casty")?.getattr("address")?;
        for seed in &seeds {
            checking.call1(("seeds", seed.clone()))?;
        }
        let none = py.None().into_bound(py);
        let compression = match compression {
            Some(compression) if !compression.is_none() => compression.clone(),
            _ => py.import("casty")?.call_method0("Compression")?,
        };
        let sync = optional(sync_every, 5.0)?;
        let settings = cluster::dialling(
            seeds,
            name.to_owned(),
            tls.unwrap_or(&none),
            &compression,
            core::time::Duration::from_secs_f64(sync),
        )?;
        // A client has no activation to idle out and no replica to write to: nothing of it waits for these.
        let held = Settings {
            idle_after: 0.0,
            ask_timeout: optional(ask_timeout, 10.0)?,
            write_timeout: 0.0,
            leave_timeout: 0.0,
            backoff_first: 0.1,
            backoff_limit: 10.0,
            backoff_factor: 2.0,
        };
        Ok(Self {
            node: Arc::new(Node::new(
                held,
                NodeId {
                    address: None,
                    incarnation: incarnation(py)?,
                },
            )),
            settings,
            map: match address_map {
                Some(map) if !map.is_none() => Some(map.clone().unbind()),
                _ => None,
            },
        })
    }

    /// Everything a client is built from is read by `__new__`; this is here so that a subclass can call it.
    #[pyo3(signature = (*_args, **_kwargs))]
    #[allow(clippy::unused_self)]
    fn __init__(&self, _args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) {}

    fn __aenter__<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let held = slf.get();
        let node = &held.node;
        let running = py.import("asyncio")?.call_method0("get_running_loop")?;
        *node
            .running
            .lock()
            .expect("the node lock is never poisoned") = Some(running.clone().unbind());
        *node.system.lock().expect("the node lock is never poisoned") =
            Some(slf.clone().into_any().unbind());
        let entered = node.future(py)?;
        let map = held.map.as_ref().map(|map| map.bind(py));
        node.enter(
            py,
            held.settings.clone(),
            map,
            running.unbind(),
            &entered,
            slf.as_any(),
            false,
        )?;
        Ok(Bound::new(py, Awaited::of(entered))?.into_any())
    }

    #[pyo3(signature = (*_exc))]
    fn __aexit__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        _exc: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let node = slf.get().node.clone();
        let gone = node.future(py)?;
        node.stop(py);
        // A client is in no table and keeps nothing, so there is nothing to say and nothing to hand over.
        node.departed(py, true, &gone);
        Ok(Bound::new(py, Awaited::of(gone))?.into_any())
    }

    /// Reference to the entity `(actor, key)`, wherever it is placed. It creates the key if it has to, and activates it.
    #[pyo3(name = "ref", signature = (actor, key, /, *, initial = None))]
    fn reference(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        actor: &Bound<'_, PyAny>,
        key: &str,
        initial: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Ref> {
        ActorSystem::referenced(&slf.get().node, py, actor, key, initial)
    }

    /// `annotation` compiled in the canonical order a stored value is compared in.
    #[pyo3(name = "_schema")]
    #[allow(clippy::unused_self)]
    fn compiled(&self, py: Python<'_>, annotation: &Bound<'_, PyAny>) -> PyResult<Schema> {
        Ok(Schema::of(py, annotation, true)?)
    }

    /// The type called `name`, imported from where it lives. Nothing when this process does not have it.
    ///
    /// A test hook, not part of the API: it is how a test sees what a node finds when a name reaches it.
    #[pyo3(name = "_resolve")]
    fn resolved(&self, py: Python<'_>, name: &str) -> Option<Py<PyAny>> {
        self.node
            .resolve(py, name)
            .map(|behavior| behavior.held(py))
    }

    /// `value` as the bytes a collection stores.
    #[pyo3(name = "_encode")]
    #[allow(clippy::unused_self)]
    fn encoded<'py>(
        &self,
        py: Python<'py>,
        schema: &Bound<'py, Schema>,
        value: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        encoded(py, schema, value)
    }

    /// The value `data` holds, with every ref in it bound to this client.
    #[pyo3(name = "_decode")]
    fn decoded<'py>(
        &self,
        schema: &Bound<'py, Schema>,
        data: &[u8],
    ) -> PyResult<Bound<'py, PyAny>> {
        decoded(&self.node, schema, data)
    }

    /// The members of the cluster as this client last heard of them.
    #[getter]
    fn members<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        listed(&self.node, py)
    }

    #[getter]
    fn node(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.node.started(py)?;
        identity(py, &self.node.id())
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        class: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        crate::generic::alias(class, &crate::generic::subscript(item))
    }
}

/// The pages this node writes, for a test that has to see them.
///
/// A test hook, not part of the API: the only way to watch a write from outside used to be a codec of one's own,
/// which is an extension point the library does not otherwise need.
#[pyclass(frozen, module = "casty._casty")]
#[derive(Debug)]
pub struct Writes {
    seen: Py<PyList>,
    refuse: Mutex<Option<Vec<u8>>>,
}

impl Writes {
    /// Note the pages of a write, and refuse the one a test asked to fail.
    fn saw(&self, py: Python<'_>, pages: &Pages) -> PyResult<()> {
        let refuse = self
            .refuse
            .lock()
            .expect("the writes lock is never poisoned")
            .clone();
        // A page that holds a stored value holds it as bytes, and that is what a test names.
        for page in pages.values() {
            if refuse.is_some() && casty_core::wire::Reading::new(page).bytes().ok() == refuse {
                *self
                    .refuse
                    .lock()
                    .expect("the writes lock is never poisoned") = None;
                return Err(pyo3::exceptions::PyValueError::new_err("value save failed"));
            }
        }
        let seen = self.seen.bind(py);
        for page in pages.values() {
            seen.append(PyBytes::new(py, page))?;
        }
        Ok(())
    }
}

#[pymethods]
impl Writes {
    /// Every page written since this started watching, in the order they were written.
    #[getter]
    fn payloads(&self, py: Python<'_>) -> Py<PyList> {
        self.seen.clone_ref(py)
    }

    /// A page whose write raises instead of happening, once.
    #[getter]
    fn fail_on(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
        self.refuse
            .lock()
            .expect("the writes lock is never poisoned")
            .as_ref()
            .map(|page| PyBytes::new(py, page).unbind())
    }

    #[setter]
    fn set_fail_on(&self, page: Option<Vec<u8>>) {
        *self
            .refuse
            .lock()
            .expect("the writes lock is never poisoned") = page;
    }
}
