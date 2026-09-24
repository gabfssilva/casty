//! A node: the keys it hosts, the answers it is waiting for, and the settings they share.

pub mod activation;
pub mod callback;
pub mod catalog;
pub mod cluster;
pub mod context;
pub mod inbox;
pub mod observe;
pub mod replies;
pub mod runs;
pub mod stats;
pub mod storage;

use core::time::Duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use casty_core::backoff::Backoff;
use casty_core::chain::Chain;
use casty_core::mailbox::Start;
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_core::store::{LocalStore, Pages};
use casty_net::endpoint::TooLarge;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple, PyType};

use crate::actor::Definition;
use crate::awaited::Awaited;
use crate::lock::Locked;
use crate::refs::Ref;
use crate::runtime::{Runtime, Threads};
use crate::schema::Schema;
use activation::Activation;
use catalog::Catalog;
use cluster::{Ending, Entered, Joined, Taken};
use observe::{Kind, Observed, Wanted};
use replies::{Replies, Waiting};

/// The schema of the answer of each `ask` builder, held by the builder it was read from.
type Answers = HashMap<usize, (Py<PyAny>, Py<Schema>)>;

/// The periods a system runs by.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub idle_after: Duration,
    pub ask_timeout: Duration,
    pub write_timeout: Duration,
    pub leave_timeout: Duration,
    pub backoff: Backoff,
}

/// What a node does to the state of a key. The replicas carry it out in a cluster; alone, the store of the system does
/// for a durable type, and the memory of the process for any other.
#[derive(Debug)]
pub enum Op {
    /// Take the key over, which starts from `initial` when nothing holds it.
    Activate { initial: Option<Pages> },
    /// Write the state of the key. Without `active` the key lets go with it, which is the last write of its
    /// activation.
    Commit {
        lease: u64,
        pages: Pages,
        active: bool,
    },
    /// Delete the state of the key. Without `active` the key lets go with it.
    Delete { lease: u64, active: bool },
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
    /// The store the system was built with, which keeps the state of the durable types outside the process.
    storage: Option<Py<PyAny>>,
    replies: Mutex<Replies>,
    /// Requests whose cancellation arrived before them, until their deadline in loop time: the request is dropped if
    /// it arrives after all.
    forestalled: Mutex<HashMap<Target, f64>>,
    activations: Mutex<HashMap<(String, String), Py<Activation>>>,
    /// Which run of a body each task reads for, which is how an `ask` or an answer knows the body it comes from.
    runs: runs::Runs,
    leaving: Mutex<Option<Py<PyAny>>>,
    /// Whether this node takes no new message: it is on its way out and finishing what it is on.
    draining: AtomicBool,
    stopped: AtomicBool,
    /// What the system reports to: the observer it was built with, or the one that logs.
    observer: Py<PyAny>,
    /// The kinds of event the observer takes, which nothing of another kind is built for.
    wanted: Wanted,
    /// The writes of the store of a node running alone, which are confirmed as they are made. In a cluster the
    /// replication counts them.
    saved: AtomicU64,
    /// The runtime the transport of this node shares with other systems, when it was given one.
    threads: Option<Arc<Threads>>,
}

impl Node {
    fn new(
        settings: Settings,
        id: NodeId,
        observer: Py<PyAny>,
        storage: Option<Py<PyAny>>,
        threads: Option<Arc<Threads>>,
    ) -> Self {
        Self {
            settings,
            observer,
            storage,
            threads,
            wanted: Wanted::default(),
            saved: AtomicU64::new(0),
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
            forestalled: Mutex::new(HashMap::new()),
            runs: runs::Runs::default(),
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

    /// What the system reports to.
    #[must_use]
    pub fn observer(&self, py: Python<'_>) -> Py<PyAny> {
        self.observer.clone_ref(py)
    }

    /// Report what `event` makes to the observer, on the loop and after the step this is part of.
    ///
    /// `event` is called only when there is a loop to report on: a system that has not started, or has exited, has
    /// nobody to tell. What it makes goes no further when the observer does not take its kind.
    pub fn observe(&self, py: Python<'_>, event: impl FnOnce() -> Observed) {
        let Ok(running) = self.running(py) else {
            return;
        };
        let event = event();
        if self.takes(event.kind()) {
            observe::deliver(py, &running, &self.observer, event);
        }
    }

    /// Whether the observer takes events of `kind`.
    #[must_use]
    pub fn takes(&self, kind: Kind) -> bool {
        self.wanted.takes(kind)
    }

    /// Ask the observer which kinds of event it takes, which is all this node reports to it from now on.
    fn subscribe(&self, py: Python<'_>) -> PyResult<()> {
        self.wanted.ask(self.observer.bind(py))
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
    /// of being answered with a failure. What is already queued here goes the same way when the activation ends, and
    /// so does what was on its way to the loop when the node started leaving.
    #[must_use]
    fn refusing(&self) -> bool {
        self.draining() && self.cluster().is_none()
    }

    /// Nothing reaches this node any more, which is what a ref of it raises on.
    pub fn stop_taking(&self) {
        self.draining.store(true, Ordering::SeqCst);
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Fail every `ask` still waiting for its answer: the system has stopped, and no answer reaches it any more.
    pub fn forsake(&self, py: Python<'_>) {
        let abandoned = self.replies.locked().abandon();
        for waiting in &abandoned {
            let _ = replies::forsake(py, waiting);
        }
    }

    /// Where this node is. Alone it is an incarnation with no address; in a cluster it is the identity it joined under.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id.locked().clone()
    }

    /// The cluster this node is in, if it is in one.
    #[must_use]
    pub fn cluster(&self) -> Option<Entered> {
        self.joined.locked().as_ref().and_then(Joined::entered)
    }

    /// The members of the cluster as this node last saw them. Alone, it is the only one.
    #[must_use]
    pub fn members(&self) -> Vec<casty_node::membership::service::Member> {
        self.members.locked().clone()
    }

    /// Take the members the cluster reports, which is what `system.members` reads.
    pub fn seen(&self, members: Vec<casty_node::membership::service::Member>) {
        *self.members.locked() = members;
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
        running_loop: &Bound<'_, PyAny>,
        entered: &Bound<'_, PyAny>,
        system: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let map = network.getattr("address_map")?;
        let map = if map.is_none() { None } else { Some(map) };
        let settings = cluster::settings(network, &self.settings)?;
        *self.network.locked() = Some(network.clone().unbind());
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
        running_loop: &Bound<'_, PyAny>,
        entered: &Bound<'_, PyAny>,
        system: &Bound<'_, PyAny>,
        member: bool,
    ) -> PyResult<()> {
        let types = self.catalog.locked().kinds();
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
            self.threads.clone(),
        )?;
        *self.joined.locked() = Some(joined);
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
        let activations: Vec<((String, String), Py<Activation>)> =
            self.activations.locked().drain().collect();
        for ((actor, key), activation) in activations {
            let _ = Activation::release(activation.bind(py), py);
            self.observe(py, || Observed::Ended { actor, key });
        }
        // The identity that was removed goes without a word: the cluster already buried it.
        let shed = self.joined.locked().take();
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
            .locked()
            .as_ref()
            .map(|network| network.clone_ref(py));
        let (Some(network), Ok(system), Ok(running)) = (network, self.system(py), self.running(py))
        else {
            return Ok(());
        };
        let entered = self.future(py)?;
        self.join(py, network.bind(py), &running, &entered, system.bind(py))
    }

    /// What `__aenter__` gives back to await: the entry, undone if its caller stops waiting for it or the cluster
    /// refuses it, so that the system is as it was before, and its address, its connections and its threads are let go.
    fn entry<'py>(
        self: &Arc<Self>,
        py: Python<'py>,
        entered: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let node = Arc::clone(self);
        callback::when_done(&entered, move |_, entered| {
            if entered.call_method0("cancelled")?.is_truthy()?
                || !entered.call_method0("exception")?.is_none()
            {
                node.withdraw();
            }
            Ok(())
        })?;
        Ok(Bound::new(py, Awaited::of(entered))?.into_any())
    }

    fn withdraw(&self) {
        let joined = self.joined.locked().take();
        if let Some(joined) = joined {
            joined.shutdown();
        }
        *self.running.locked() = None;
        *self.system.locked() = None;
    }

    /// The node is in the cluster: take the identity it joined under and what it sees.
    pub fn entered(&self, cluster: &Entered) {
        *self.id.locked() = cluster.id().clone();
        *self.members.locked() = cluster.members();
    }

    /// Say goodbye to the cluster and stop the transport, resolving `gone` once it is over.
    ///
    /// The cluster is let go of only when the leave has finished: what it holds is the runtime the leave runs on.
    pub fn departed(self: &Arc<Self>, py: Python<'_>, abort: bool, gone: &Bound<'_, PyAny>) {
        let joined = self.joined.locked().clone();
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
        self.forsake(py);
        if !gone
            .call_method0("done")
            .is_ok_and(|done| done.is_truthy().unwrap_or(true))
        {
            let _ = gone.call_method1("set_result", (py.None(),));
        }
    }

    /// Let go of `joined`, unless this node has already taken another one in its place.
    pub fn let_go(&self, joined: &Arc<Joined>) {
        let mut held = self.joined.locked();
        if held.as_ref().is_some_and(|held| Arc::ptr_eq(held, joined)) {
            *held = None;
        }
    }

    /// End the activation of a key whose owner moved: a write from it would be refused by the replicas anyway.
    ///
    /// A node shutting down moved every key it runs, and ends each one after the message it is on instead.
    pub fn relinquish(self: &Arc<Self>, py: Python<'_>, actor: &str, key: &str) -> PyResult<()> {
        match self.activation(py, actor, key) {
            Some(activation) if self.draining() => Activation::drain(activation.bind(py), py),
            Some(activation) => Activation::release(activation.bind(py), py),
            None => Ok(()),
        }
    }

    /// The loop this node runs on, which is the one that owns every future it hands out.
    pub fn running<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match &*self.running.locked() {
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
        match &*self.system.locked() {
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
            let answers = self.answers.locked();
            if let Some((held, schema)) = answers.get(&at)
                && held.bind(py).is(build)
            {
                return Ok(schema.clone_ref(py));
            }
        }
        let schema = crate::schema::reply_schema(py, build)?;
        let schema = Bound::new(py, schema)?.unbind();
        self.answers
            .locked()
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

    /// Call `call` on the loop `delay` seconds from now, giving back the timer that cancels it.
    pub fn later<'py>(
        &self,
        py: Python<'py>,
        delay: f64,
        call: impl FnOnce(Python<'_>) -> PyResult<()> + Send + 'static,
    ) -> PyResult<Bound<'py, PyAny>> {
        callback::later(&self.running(py)?, delay, call)
    }

    /// Meet the type `actor` defines, which is the one this system runs under its name unless it met another first.
    pub fn learn(&self, actor: &Bound<'_, PyAny>, definition: &Arc<Definition>) {
        self.catalog.locked().learn(actor, definition);
        if let Some(cluster) = self.cluster() {
            cluster.node().learn(definition.kind());
        }
    }

    /// The type called `name`, imported when this system has not met it yet.
    #[must_use]
    pub fn resolve(&self, py: Python<'_>, name: &str) -> Option<Arc<Definition>> {
        {
            let catalog = self.catalog.locked();
            if let Some(known) = catalog.known(name) {
                return Some(Arc::clone(known));
            }
            if catalog.gave_up(name) {
                return None;
            }
        }
        // The import runs without the lock: it is arbitrary Python, and it can reach back into this node.
        let found = catalog::imported(py, name);
        let mut catalog = self.catalog.locked();
        let Some((actor, definition)) = found else {
            catalog.give_up(name);
            drop(catalog);
            if let Some(cluster) = self.cluster() {
                cluster.node().gave_up(name);
            }
            return None;
        };
        catalog.learn(&actor, &definition);
        drop(catalog);
        // A type met by importing it is one this node hosts, so the cluster and the rings hear of it too.
        if let Some(cluster) = self.cluster() {
            cluster.node().learn(definition.kind());
        }
        Some(definition)
    }

    pub fn store(&self) -> std::sync::MutexGuard<'_, LocalStore> {
        self.store.locked()
    }

    /// Send `command` to the key it names, wherever that key is.
    ///
    /// In a cluster the node decides where it goes and hands it back here if this is the node that owns the key. A
    /// command larger than one message between two nodes goes nowhere, wherever the key is: whoever waits for its
    /// answer is answered with the refusal at once, and a command nobody waits for raises it.
    pub fn hand(
        self: &Arc<Self>,
        py: Python<'_>,
        command: casty_core::mailbox::Command,
    ) -> PyResult<()> {
        if let casty_core::mailbox::Command::Deliver(deliver) = &command {
            self.sent(py, deliver);
        }
        let Some(cluster) = self.cluster() else {
            return self.take(py, command);
        };
        let reply = command.reply().cloned();
        let sent = match command {
            casty_core::mailbox::Command::Deliver(deliver) => cluster.node().deliver(
                &deliver.actor,
                &deliver.key,
                deliver.message,
                deliver.reply,
                deliver.chain,
            ),
            casty_core::mailbox::Command::Start(start) => {
                cluster.node().start(&start.actor, &start.key, start.state)
            }
        };
        let Err(TooLarge(why)) = sent else {
            return Ok(());
        };
        match reply {
            Some(target) => self.answer(py, &target, &Outcome::TooLarge(why)),
            None => Err(crate::errors::MessageTooLarge::new_err(why)),
        }
    }

    /// Note the key an `ask` of this node went to, which is where its cancellation goes, and keep what it sent when
    /// the owner may call it back to send it again.
    fn sent(&self, py: Python<'_>, deliver: &casty_core::mailbox::Deliver) {
        let Some(Target::Reply { node, id }) = &deliver.reply else {
            return;
        };
        if *node == self.id() {
            let again = replies::waits(py, self, &deliver.actor);
            self.replies.locked().sent(*id, deliver, again);
        }
    }

    /// The bodies an `ask` made now keeps waiting: the chain of the message the run of the task running now is on,
    /// and that run. Outside a body, or in a task the body started beside it, nobody.
    #[must_use]
    pub fn chain(&self, py: Python<'_>) -> Chain {
        match self.runs.running(py) {
            Some((activation, run)) => activation.get().chain(run),
            None => Chain::default(),
        }
    }

    /// Something on this node told `target` its answer. From the run holding the request `target` waits on, that
    /// request is answered: nobody waits on it any more, and a cancellation of it changes nothing.
    pub fn told(&self, py: Python<'_>, target: &Target) {
        if let Some((activation, run)) = self.runs.running(py) {
            activation.get().told(run, target);
        }
    }

    /// Tell the key `(actor, key)` that nobody waits for the answer of the request `id` of this node any more.
    pub fn cancel(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        id: i64,
    ) -> PyResult<()> {
        let request = Target::Reply {
            node: self.id(),
            id,
        };
        match self.cluster() {
            Some(cluster) => {
                cluster.node().cancel(actor, key, request);
                Ok(())
            }
            None => self.cancelled(py, actor, key, &request),
        }
    }

    /// The caller of `request`, sent to `(actor, key)`, stopped waiting for its answer: it timed out or was cancelled.
    ///
    /// This is where a cancellation lands on the node that runs the request, from the cluster or from a caller on the
    /// same node. It reaches the activation of the key here and never starts one. The activation matches `request`
    /// against the reply target of the messages its runs are on, of those it queues and of the callers it holds.
    ///
    /// A request it does not have is remembered until its deadline and dropped if it arrives: a request an owner sent
    /// back went again from the caller, and its cancellation, which took the direct way, can arrive first. One that
    /// was answered already is remembered for nothing, and forgotten at the deadline.
    pub fn cancelled(
        self: &Arc<Self>,
        py: Python<'_>,
        actor: &str,
        key: &str,
        request: &Target,
    ) -> PyResult<()> {
        let found = match self.activation(py, actor, key) {
            Some(activation) => Activation::cancelled(activation.bind(py), py, request)?,
            None => false,
        };
        if found {
            return Ok(());
        }
        let now = self.now(py)?;
        let within = replies::timeout(
            py,
            self,
            &Target::Entity {
                actor: actor.to_owned(),
                key: key.to_owned(),
            },
        );
        let mut forestalled = self.forestalled.locked();
        forestalled.retain(|_, until| *until > now);
        forestalled.insert(request.clone(), now + within);
        Ok(())
    }

    /// Whether the caller of `request` cancelled it before it arrived here, so that nobody waits for it.
    fn forestalls(&self, py: Python<'_>, request: &Target) -> bool {
        let until = {
            let mut forestalled = self.forestalled.locked();
            if forestalled.is_empty() {
                return false;
            }
            forestalled.remove(request)
        };
        until.is_some_and(|until| self.now(py).is_ok_and(|now| now < until))
    }

    /// Send `command` to the activation of its key here, starting one when the key has none.
    pub fn take(
        self: &Arc<Self>,
        py: Python<'_>,
        command: casty_core::mailbox::Command,
    ) -> PyResult<()> {
        if let Some(request) = command.reply()
            && self.forestalls(py, request)
        {
            return Ok(());
        }
        if self.refusing() {
            return self.refuse(
                py,
                &command,
                "the node is shutting down",
                Outcome::unreached,
            );
        }
        // Handed over before the node started leaving, it reaches the loop after the activation of its key ended:
        // starting another one here would take the key back from the node that owns it now.
        if self.draining() && !self.holds(command.actor(), command.key()) {
            let (actor, key) = (command.actor().to_owned(), command.key().to_owned());
            if let Err(refused) = self.hand(py, command) {
                self.dropped(py, &actor, &key, &refused.value(py).to_string());
            }
            return Ok(());
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

    /// The activation of `key` here, if it has one.
    fn activation(&self, py: Python<'_>, actor: &str, key: &str) -> Option<Py<Activation>> {
        self.activations
            .locked()
            .get(&(actor.to_owned(), key.to_owned()))
            .map(|activation| activation.clone_ref(py))
    }

    /// Whether `key` has an activation here.
    fn holds(&self, actor: &str, key: &str) -> bool {
        self.activations
            .locked()
            .contains_key(&(actor.to_owned(), key.to_owned()))
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
            let activations = self.activations.locked();
            if let Some(found) = activations.get(&at) {
                return Ok(Some(found.clone_ref(py)));
            }
        }
        let Some(definition) = self.resolve(py, actor) else {
            return Ok(None);
        };
        let started = Activation::new(py, self, &definition, actor, key)?;
        let held = {
            let mut activations = self.activations.locked();
            activations
                .entry(at)
                .or_insert_with(|| started.clone_ref(py))
                .clone_ref(py)
        };
        if held.is(&started) {
            if let Some(cluster) = self.cluster() {
                cluster.node().attached(actor, key);
            }
            self.observe(py, || Observed::Started {
                actor: actor.to_owned(),
                key: key.to_owned(),
            });
            Activation::begin(held.bind(py), py)?;
        }
        Ok(Some(held))
    }

    /// Take the activation of `key` out of the table, which is where a message that arrives next starts a new one.
    pub fn remove(&self, py: Python<'_>, actor: &str, key: &str) -> Option<Py<Activation>> {
        let removed = self
            .activations
            .locked()
            .remove(&(actor.to_owned(), key.to_owned()));
        if removed.is_some() {
            if let Some(cluster) = self.cluster() {
                cluster.node().detached(actor, key);
            }
            self.observe(py, || Observed::Ended {
                actor: actor.to_owned(),
                key: key.to_owned(),
            });
        }
        removed
    }

    /// Take `activation` out of the table, unless another activation of its key has taken its place there.
    pub fn vacate(&self, activation: &Bound<'_, Activation>) {
        let (actor, key) = (activation.get().entry(), activation.get().key());
        let held = self
            .activations
            .locked()
            .get(&(actor.to_owned(), key.to_owned()))
            .is_some_and(|held| held.is(activation));
        if held {
            self.remove(activation.py(), actor, key);
        }
    }

    /// Let the key go on purpose, as if its activation here idled out now. The future it gives back resolves with
    /// whether there was one, once it is over.
    pub fn retire<'py>(
        &self,
        py: Python<'py>,
        actor: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let over = self.future(py)?;
        match self.activation(py, actor, key) {
            Some(activation) => Activation::retire(activation.bind(py), py, &over)?,
            None => {
                over.call_method1("set_result", (false,))?;
            }
        }
        Ok(over)
    }

    /// Every activation this node holds now, which is what a reading of the node goes through: each one answers for
    /// its type, its key and its mailbox.
    #[must_use]
    pub fn census(&self, py: Python<'_>) -> Vec<Py<Activation>> {
        self.activations
            .locked()
            .values()
            .map(|activation| activation.clone_ref(py))
            .collect()
    }

    /// Answer the request `target` is waiting for, wherever it waits.
    ///
    /// In a cluster an answer larger than one message between two nodes is replaced by the refusal that says so, for a
    /// caller on this node too, so that an answer does not arrive or fail by where the key landed.
    pub fn answer(
        self: &Arc<Self>,
        py: Python<'_>,
        target: &Target,
        outcome: &Outcome,
    ) -> PyResult<()> {
        let Target::Reply { node, id } = target else {
            return Ok(());
        };
        let Some(cluster) = self.cluster() else {
            return self.settle(py, *id, outcome);
        };
        if node != cluster.id() {
            cluster.node().answer(target.clone(), outcome.clone());
            return Ok(());
        }
        match cluster.node().oversized(target, outcome) {
            Some(refused) => self.settle(py, *id, &refused),
            None => self.settle(py, *id, outcome),
        }
    }

    /// Answer the request numbered `id`, which something on this node is waiting for.
    pub fn settle(self: &Arc<Self>, py: Python<'_>, id: i64, outcome: &Outcome) -> PyResult<()> {
        let waiting = self.replies.locked().forget(id);
        match waiting {
            Some(waiting) => replies::settle(py, id, waiting, outcome, self),
            None => Ok(()),
        }
    }

    /// Carry `op` out on the key, resolving the future it gives back once it is done: with what the replicas hold when
    /// it takes the key over, with nothing once a write is written.
    ///
    /// Alone, the memory of the process answers at once; the store of a durable type and the replicas of a cluster
    /// answer when they have it, and a node of a cluster that is out of it answers `Unavailable`.
    pub fn persist<'py>(
        self: &Arc<Self>,
        py: Python<'py>,
        actor: &str,
        key: &str,
        op: Op,
    ) -> PyResult<Bound<'py, PyAny>> {
        if let Op::Commit { pages, .. } = &op {
            let watching = self
                .writes
                .locked()
                .as_ref()
                .map(|writes| writes.clone_ref(py));
            if let Some(watching) = watching {
                watching.bind(py).get().saw(py, pages)?;
            }
        }
        let answer = self.future(py)?;
        if let Some(cluster) = self.cluster() {
            cluster.persist(actor, key, op, answer.clone().unbind());
        } else if self.network.locked().is_some() {
            // Before it has entered, or between a removal and the join after it, a key has no replicas to be taken
            // from, and what the memory of the process kept would never reach them.
            let outside = crate::errors::Unavailable::new_err(format!(
                "{actor}/{key}: this node is not in its cluster"
            ));
            answer.call_method1("set_exception", (outside.into_value(py),))?;
        } else if let Some(durability) = self.durability(py, actor) {
            self.persist_stored(py, actor, key, op, durability, &answer)?;
        } else {
            let landed = match op {
                Op::Activate { initial } => {
                    let held = self.store().activate(actor, key, initial);
                    Bound::new(py, Taken::of(held))?.into_any()
                }
                Op::Commit { pages, active, .. } => {
                    self.store().commit(actor, key, pages, active);
                    self.saved.fetch_add(1, Ordering::Relaxed);
                    py.None().into_bound(py)
                }
                Op::Delete { .. } => {
                    self.store().delete(actor, key);
                    self.saved.fetch_add(1, Ordering::Relaxed);
                    py.None().into_bound(py)
                }
            };
            answer.call_method1("set_result", (landed,))?;
        }
        Ok(answer)
    }

    /// Let go of a key whose activation ended without a last write, so that the cluster drops what its owner kept.
    ///
    /// It goes out before anything a later activation of the key asks for, which is what keeps it from dropping the
    /// owner that one builds.
    pub fn forgo(&self, actor: &str, key: &str, lease: u64) {
        if let Some(cluster) = self.cluster() {
            cluster.node().release(actor, key, lease);
        }
    }

    /// Start waiting for an answer, decoded with `schema`, and give back the id it comes addressed to.
    #[must_use]
    pub fn request(&self, py: Python<'_>, schema: &Py<Schema>, future: &Bound<'_, PyAny>) -> i64 {
        self.waited(future, Some(schema.clone_ref(py)))
    }

    /// Start waiting for an answer that no schema reads, which is what a native body asks for: it reads the bytes.
    #[must_use]
    pub fn awaited(&self, future: &Bound<'_, PyAny>) -> i64 {
        self.waited(future, None)
    }

    fn waited(&self, future: &Bound<'_, PyAny>, schema: Option<Py<Schema>>) -> i64 {
        let mut replies = self.replies.locked();
        let id = replies.take();
        replies.wait(
            id,
            Waiting {
                future: future.clone().unbind(),
                schema,
                deadline: None,
                to: None,
                again: None,
            },
        );
        id
    }

    pub fn deadline(&self, id: i64, timer: &Bound<'_, PyAny>) {
        let mut replies = self.replies.locked();
        if let Some(waiting) = replies.forget(id) {
            replies.wait(
                id,
                Waiting {
                    deadline: Some(timer.clone().unbind()),
                    ..waiting
                },
            );
        }
    }

    pub fn forget(&self, id: i64) -> Option<Waiting> {
        self.replies.locked().forget(id)
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
                self.dropped(py, command.actor(), command.key(), why);
                Ok(())
            }
            Some(target) => self.answer(py, target, &outcome(command.actor(), command.key())),
        }
    }

    /// A message nobody is waiting for, which ends here.
    pub fn dropped(&self, py: Python<'_>, actor: &str, key: &str, why: &str) {
        self.observe(py, || Observed::Dropped {
            actor: actor.to_owned(),
            key: key.to_owned(),
            reason: why.to_owned(),
        });
    }

    /// Take no more: a ref of this node raises from here on, and no activation goes further.
    fn stop(self: &Arc<Self>, py: Python<'_>) {
        self.stop_taking();
        self.abandon(py);
    }

    /// End every activation after the message it is on: this node is shutting down.
    fn leave<'py>(self: &Arc<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Only new messages are refused here. A body finishing the one it is on still answers it, and the reply of
        // its `ask` goes out before the system closes.
        self.draining.store(true, Ordering::SeqCst);
        let waiting = self.future(py)?;
        let running = self.census(py);
        if running.is_empty() {
            waiting.call_method1("set_result", (py.None(),))?;
            return Ok(waiting);
        }
        *self.leaving.locked() = Some(waiting.clone().unbind());
        for activation in running {
            Activation::drain(activation.bind(py), py)?;
        }
        // The deadline is the deadline: a body that never ends does not keep the process alive.
        let node = Arc::clone(self);
        self.later(py, self.settings.leave_timeout.as_secs_f64(), move |py| {
            node.abandon(py);
            Ok(())
        })?;
        Ok(waiting)
    }

    /// Give up on what `drain` did not finish: every activation left gives its key up where it is.
    pub fn abandon(self: &Arc<Self>, py: Python<'_>) {
        for activation in self.census(py) {
            let _ = Activation::release(activation.bind(py), py);
        }
    }

    /// Called by an activation that ended: the last one out closes the shutdown.
    pub fn ended(&self, py: Python<'_>) -> PyResult<()> {
        let empty = self.activations.locked().is_empty();
        if !empty {
            return Ok(());
        }
        let waiting = self.leaving.locked().take();
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
            .locked()
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

/// The methods of `$class`, which is `ActorSystem` or `Client`, and those the two share over the node each holds.
///
/// pyo3 takes one `#[pymethods]` block per class, so what is the class's own goes in here as well.
macro_rules! system_methods {
    ($class:ident { $($own:tt)* }) => {
        #[pymethods]
        impl $class {
            /// The members of the cluster as this sees them: a node lists itself among them, and a client the members
            /// it last heard of.
            #[getter]
            fn members<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                listed(&self.node, py)
            }

            /// What this counts now: the activations of a node by type and the writes of its keys, the answers it
            /// waits for, and its connections and what they carried.
            fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                stats::snapshot(&self.node, py)
            }

            /// Where the key a ref of `actor` goes to is, as this sees it: the owner a message to it is sent to, and
            /// the nodes that keep it.
            #[pyo3(signature = (actor, key, /, *, at = None))]
            fn placement<'py>(
                &self,
                py: Python<'py>,
                actor: &Bound<'py, PyAny>,
                key: &str,
                at: Option<&Bound<'py, PyAny>>,
            ) -> PyResult<Bound<'py, PyAny>> {
                whereabouts(&self.node, py, actor, key, at)
            }

            /// Reference to the entity `(actor, key)`, wherever it is placed, or on the node `at` names for a pinned
            /// type. It creates the key if it has to, and activates it.
            #[pyo3(name = "ref", signature = (actor, key, /, *, initial = None, at = None))]
            fn reference(
                &self,
                py: Python<'_>,
                actor: &Bound<'_, PyAny>,
                key: &str,
                initial: Option<&Bound<'_, PyAny>>,
                at: Option<&Bound<'_, PyAny>>,
            ) -> PyResult<Ref> {
                referenced(&self.node, py, actor, key, initial, at)
            }

            #[getter]
            fn node(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                self.node.started(py)?;
                identity(py, &self.node.id())
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
                self.node.resolve(py, name)?;
                self.node.catalog.locked().object(py, name)
            }

            /// `value` as the bytes a collection stores: the value in the canonical order its schema was compiled in.
            #[pyo3(name = "_encode")]
            #[allow(clippy::unused_self)]
            fn encoded<'py>(
                &self,
                py: Python<'py>,
                schema: &Bound<'py, Schema>,
                value: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyBytes>> {
                let written = Schema::write(schema, schema.get().tree().sent(), value)?;
                Ok(PyBytes::new(py, &written))
            }

            /// The value `data` holds, with every ref in it bound to the node of this system or client.
            #[pyo3(name = "_decode")]
            fn decoded<'py>(
                &self,
                schema: &Bound<'py, Schema>,
                data: &[u8],
            ) -> PyResult<Bound<'py, PyAny>> {
                Ok(Schema::read(schema, schema.get().tree().sent(), data, Some(&self.node))?)
            }

            #[classmethod]
            fn __class_getitem__<'py>(
                class: &Bound<'py, PyType>,
                item: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                crate::generic::alias(class, item)
            }

            $($own)*
        }
    };
}

/// A node. It hosts every actor type it meets: the ones this process uses, and the ones the cluster tells it of.
#[pyclass(frozen, module = "casty._casty", subclass)]
#[derive(Debug)]
pub struct ActorSystem {
    node: Arc<Node>,
    /// The network of this node, as the constructor took it. Without one it runs alone.
    cluster: Option<Py<PyAny>>,
}

system_methods!(ActorSystem {
    #[new]
    // The settings of a system, which is what the constructor takes.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *_extra,
        cluster = None,
        idle_after = None,
        backoff = None,
        ask_timeout = None,
        write_timeout = None,
        leave_timeout = None,
        observer = None,
        store = None,
        runtime = None,
    ))]
    fn new(
        py: Python<'_>,
        // A subclass of its own may take arguments: what `object.__new__` ignores when `__init__` is overridden.
        _extra: &Bound<'_, pyo3::types::PyTuple>,
        cluster: Option<&Bound<'_, PyAny>>,
        idle_after: Option<&Bound<'_, PyAny>>,
        backoff: Option<&Bound<'_, PyAny>>,
        ask_timeout: Option<&Bound<'_, PyAny>>,
        write_timeout: Option<&Bound<'_, PyAny>>,
        leave_timeout: Option<&Bound<'_, PyAny>>,
        observer: Option<&Bound<'_, PyAny>>,
        store: Option<&Bound<'_, PyAny>>,
        runtime: Option<&Bound<'_, Runtime>>,
    ) -> PyResult<Self> {
        let settings = Settings {
            idle_after: timing("idle_after", idle_after, Duration::from_secs(60))?,
            ask_timeout: timing("ask_timeout", ask_timeout, Duration::from_secs(10))?,
            write_timeout: timing("write_timeout", write_timeout, Duration::from_secs(5))?,
            leave_timeout: timing("leave_timeout", leave_timeout, Duration::from_secs(30))?,
            backoff: backoff.map_or(Ok(Backoff::default()), crate::actor::backed_off)?,
        };
        let id = NodeId {
            address: None,
            incarnation: incarnation(py)?,
        };
        Ok(Self {
            node: Arc::new(Node::new(
                settings,
                id,
                observing(py, observer)?,
                storage::storing(store)?,
                runtime.map(|runtime| runtime.get().threads()),
            )),
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
        node.subscribe(py)?;
        *node.running.locked() = Some(running.clone().unbind());
        *node.system.locked() = Some(slf.clone().into_any().unbind());
        let entered = node.future(py)?;
        match &slf.get().cluster {
            // The join is answered by the loop, so it runs on the transport and comes back when it is done.
            Some(cluster) => {
                node.join(
                    py,
                    cluster.bind(py),
                    &running,
                    &entered,
                    slf.as_any(),
                )?;
            }
            None => {
                entered.call_method1("set_result", (slf.clone(),))?;
            }
        }
        node.entry(py, entered)
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
        // It leaves the cluster once every activation of this node has ended.
        let left = node.leave(py)?;
        let then = gone.clone().unbind();
        callback::when_done(&left, move |py, _| {
            node.departed(py, false, then.bind(py));
            Ok(())
        })?;
        Ok(Bound::new(py, Awaited::of(gone))?.into_any())
    }

    /// The keys active on this node now, each with its type, when it became active and what waits in its mailbox.
    fn activations<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        stats::listing(&self.node, py)
    }

    /// Let the key go on purpose, as if its activation here idled out now, answering whether there was one once it
    /// is over.
    #[pyo3(signature = (actor, key, /))]
    fn release<'py>(
        &self,
        py: Python<'py>,
        actor: &Bound<'py, PyAny>,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.node.started(py)?;
        let definition = Definition::of(actor)?;
        let over = self.node.retire(py, &definition.name, key)?;
        Ok(Bound::new(py, Awaited::of(over))?.into_any())
    }

    /// Meet `actor` before the cluster names it, so this node runs it and not what the import would bring.
    ///
    /// A test hook, not part of the API: it is how a node of another deploy is built inside one process.
    #[pyo3(name = "_learn")]
    fn learned(&self, actor: &Bound<'_, PyAny>) -> PyResult<()> {
        self.node.learn(actor, &Definition::of(actor)?);
        Ok(())
    }

    /// Every key whose state this node keeps, as `(actor, key, deleted)`: `deleted` says what it keeps is the
    /// tombstone of a deletion, which a replica holds until every other replica of the key has answered for it.
    ///
    /// A test hook, not part of the API.
    #[pyo3(name = "_stored")]
    fn stored<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stored = self.node.future(py)?;
        if let Some(cluster) = self.node.cluster() {
            cluster.stored(stored.clone().unbind());
        } else {
            let keys = self.node.store().kept();
            stored.call_method1("set_result", (keys,))?;
        }
        Ok(Bound::new(py, Awaited::of(stored))?.into_any())
    }

    /// How many messages wait in the mailbox of `(actor, key)` on this node. Nothing when it has no activation here.
    ///
    /// A test hook, not part of the API: it is how a test sees that a mailbox holds no more than its bound.
    #[pyo3(name = "_queued")]
    fn queued(
        &self,
        py: Python<'_>,
        actor: &Bound<'_, PyAny>,
        key: &str,
    ) -> PyResult<Option<usize>> {
        let definition = Definition::of(actor)?;
        let held = self.node.activation(py, &definition.name, key);
        Ok(held.map(|activation| activation.bind(py).get().queued()))
    }

    /// The bodies an `ask` made now would name as waiting for its answer, `actor/key` each, the one asking last.
    ///
    /// A test hook, not part of the API: it is how a test sees what the chain of an `ask` carries.
    #[pyo3(name = "_chain")]
    fn chained(&self, py: Python<'_>) -> Vec<String> {
        self.node
            .chain(py)
            .links()
            .iter()
            .map(|link| format!("{}/{}", link.actor, link.key))
            .collect()
    }

    /// Cancel the request answered at `reply_to`, as if the cancellation of its caller reached `(actor, key)` now.
    ///
    /// A test hook, not part of the API: it is how a test makes a cancellation arrive after the body answered, which on
    /// one node the answer always overtakes.
    #[pyo3(name = "_cancel")]
    fn cancelling(
        &self,
        py: Python<'_>,
        actor: &Bound<'_, PyAny>,
        key: &str,
        reply_to: &Bound<'_, Ref>,
    ) -> PyResult<()> {
        let definition = Definition::of(actor)?;
        self.node
            .cancelled(py, &definition.name, key, reply_to.get().target())
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
        *self.node.writes.locked() = Some(writes.clone_ref(py));
        Ok(writes)
    }
});

impl ActorSystem {
    #[must_use]
    pub fn node_of(&self) -> &Arc<Node> {
        &self.node
    }
}

/// Reference to the entity `(actor, key)`, which is the same from a node of the cluster and from a client.
///
/// Obtaining it asks the owner to create the key, if it does not exist, and to activate it. Nobody waits for that:
/// what goes wrong with it shows in the first `ask`.
fn referenced(
    node: &Arc<Node>,
    py: Python<'_>,
    actor: &Bound<'_, PyAny>,
    key: &str,
    initial: Option<&Bound<'_, PyAny>>,
    at: Option<&Bound<'_, PyAny>>,
) -> PyResult<Ref> {
    node.started(py)?;
    let definition = Definition::of(actor)?;
    let key = placed(py, &definition, key, at)?;
    node.learn(actor, &definition);
    let state = definition.state.bind(py);
    let sent = state.get().tree().sent();
    let written: Option<Vec<u8>> = match (initial, &definition.initial) {
        (Some(initial), _) => Some(Schema::write(state, sent, initial)?),
        (None, Some(_)) => None,
        // A type without a default starts from nothing, which only a state that can be `None` allows.
        (None, None) => match Schema::write(state, sent, &py.None().into_bound(py)) {
            Ok(nothing) => Some(nothing),
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
            key: key.clone(),
            state: written,
        }),
    )?;
    Ok(Ref::entity_of(
        definition.messages.clone_ref(py),
        definition.name.clone(),
        key,
        Some(node.clone()),
    ))
}

/// The key a ref of `definition` goes to: `key` itself, which the ring places, or `key` pinned to the node `at` names.
///
/// A key of a pinned type always names its node, and a key of any other type never has the form of one: placement
/// reads the form alone, so either would be sent where the type does not belong.
fn placed(
    py: Python<'_>,
    definition: &Definition,
    key: &str,
    at: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let name = &definition.name;
    match (definition.settings.pinned, at) {
        (true, Some(at)) => {
            let address = advertised(py, at)?;
            let pinned = casty_core::placement::pin(&address, key);
            if casty_core::placement::pinned(&pinned) != Some(address.as_str()) {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "at is {address:?}, which is not the host:port a node advertises"
                )));
            }
            Ok(pinned)
        }
        (true, None) => Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{name} is pinned, so its ref names the node it runs on: pass at="
        ))),
        (false, Some(_)) => Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{name} is placed by the ring, and only a type declared with pinned=True takes at="
        ))),
        (false, None) if casty_core::placement::pinned(key).is_some() => {
            Err(pyo3::exceptions::PyValueError::new_err(format!(
                "{name}: the key {key:?} has the form @host:port/name, which only the keys of a pinned type have"
            )))
        }
        (false, None) => Ok(key.to_owned()),
    }
}

/// The advertised address `at` names: that of a `Member`, of a `NodeId`, or the `host:port` itself.
fn advertised(py: Python<'_>, at: &Bound<'_, PyAny>) -> PyResult<String> {
    let casty = py.import("casty")?;
    let address: Option<String> = if at.is_instance(&casty.getattr("Member")?)? {
        at.getattr("node")?.getattr("address")?.extract()?
    } else if at.is_instance(&casty.getattr("NodeId")?)? {
        at.getattr("address")?.extract()?
    } else if let Ok(address) = at.extract::<String>() {
        Some(address)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "at is {at}, which is not a Member, a NodeId or a host:port"
        )));
    };
    address.ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "at is {at}, which has no address: only a node of a cluster runs keys"
        ))
    })
}

/// Where the key a ref of `actor` would go to is, as `node` sees it, as the future of a `casty.Placement`.
///
/// The key is the one `ref` makes of `key` and `at`, and the node meets the type first, as `ref` does: the number of
/// replicas of a key is the one its type declares. Alone, the node is the owner and the only replica of every key.
fn whereabouts<'py>(
    node: &Arc<Node>,
    py: Python<'py>,
    actor: &Bound<'py, PyAny>,
    key: &str,
    at: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    node.started(py)?;
    let definition = Definition::of(actor)?;
    let key = placed(py, &definition, key, at)?;
    node.learn(actor, &definition);
    let answer = node.future(py)?;
    if let Some(cluster) = node.cluster() {
        cluster.placed(&definition.name, &key, answer.clone().unbind());
    } else {
        let alone = node.id();
        let placement = casty_node::node::Placed {
            owner: Some(alone.clone()),
            replicas: vec![alone],
        };
        answer.call_method1("set_result", (located(py, &placement)?,))?;
    }
    Ok(Bound::new(py, Awaited::of(answer))?.into_any())
}

/// A placement as the `Placement` of `casty`.
pub fn located<'py>(
    py: Python<'py>,
    placed: &casty_node::node::Placed,
) -> PyResult<Bound<'py, PyAny>> {
    let owner = placed
        .owner
        .as_ref()
        .map(|node| identity(py, node))
        .transpose()?;
    let replicas = placed
        .replicas
        .iter()
        .map(|node| identity(py, node))
        .collect::<PyResult<Vec<_>>>()?;
    py.import("casty")?
        .getattr("Placement")?
        .call1((owner, PyTuple::new(py, replicas)?))
}

/// End the request at `within`, so that nothing waits for an answer that is not coming.
///
/// A request that nothing answered by then ends with a timeout, and the key it went to hears so.
pub fn armed(py: Python<'_>, node: &Arc<Node>, id: i64, within: f64) -> PyResult<()> {
    let held = Arc::clone(node);
    let timer = node.later(py, within, move |py| {
        let Some(waiting) = held.forget(id) else {
            return Ok(());
        };
        let future = waiting.future.bind(py);
        if !future.call_method0("done")?.is_truthy()? {
            let timeout =
                pyo3::exceptions::PyTimeoutError::new_err("the answer did not arrive in time");
            future.call_method1("set_exception", (timeout,))?;
        }
        replies::cancel(py, id, &waiting, &held)
    })?;
    node.deadline(id, &timer);
    Ok(())
}

/// End the request `id`, which whoever waited for gave up on. If it still waits, nothing answered it, and the key it
/// went to hears that nobody waits for it any more.
pub fn abandoned(py: Python<'_>, node: &Arc<Node>, id: i64) -> PyResult<()> {
    match node.forget(id) {
        Some(waiting) => replies::cancel(py, id, &waiting, node),
        None => Ok(()),
    }
}

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

/// `NodeId` as the facade declares it, built from what the core holds.
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

/// A timing a constructor took, or `default` when it took none.
fn timing(name: &str, value: Option<&Bound<'_, PyAny>>, default: Duration) -> PyResult<Duration> {
    value.map_or(Ok(default), |value| crate::actor::period(name, value))
}

/// The observer a constructor took, which has to be something to call. Without one, the system logs what it reports.
fn observing(py: Python<'_>, observer: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
    match observer {
        Some(observer) if !observer.is_none() => {
            if !observer.is_callable() {
                return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                    "observer is {observer:?}, which cannot be called with an event"
                )));
            }
            Ok(observer.clone().unbind())
        }
        _ => Ok(py
            .import("casty")?
            .getattr("LoggingObserver")?
            .call0()?
            .unbind()),
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

system_methods!(Client {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *_extra,
        seeds,
        name = "casty",
        tls = None,
        compression = None,
        address_map = None,
        limits = None,
        ask_timeout = None,
        sync_every = None,
        observer = None,
        runtime = None,
    ))]
    fn new(
        py: Python<'_>,
        _extra: &Bound<'_, PyTuple>,
        seeds: Vec<String>,
        name: &str,
        tls: Option<&Bound<'_, PyAny>>,
        compression: Option<&Bound<'_, PyAny>>,
        address_map: Option<&Bound<'_, PyAny>>,
        limits: Option<&Bound<'_, PyAny>>,
        ask_timeout: Option<&Bound<'_, PyAny>>,
        sync_every: Option<&Bound<'_, PyAny>>,
        observer: Option<&Bound<'_, PyAny>>,
        runtime: Option<&Bound<'_, Runtime>>,
    ) -> PyResult<Self> {
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
        let limits = match limits {
            Some(limits) if !limits.is_none() => limits.clone(),
            _ => py.import("casty")?.call_method0("Limits")?,
        };
        let sync = match sync_every {
            Some(sync_every) => cluster::every("sync_every", sync_every)?,
            None => Duration::from_secs(5),
        };
        let settings = cluster::dialling(
            seeds,
            name.to_owned(),
            tls.unwrap_or(&none),
            &compression,
            &limits,
            sync,
        )?;
        // A client has no activation to idle out and no replica to write to: nothing of it waits for these.
        let held = Settings {
            idle_after: Duration::ZERO,
            ask_timeout: timing("ask_timeout", ask_timeout, Duration::from_secs(10))?,
            write_timeout: Duration::ZERO,
            leave_timeout: Duration::ZERO,
            backoff: Backoff::default(),
        };
        Ok(Self {
            node: Arc::new(Node::new(
                held,
                NodeId {
                    address: None,
                    incarnation: incarnation(py)?,
                },
                observing(py, observer)?,
                None,
                runtime.map(|runtime| runtime.get().threads()),
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
        node.subscribe(py)?;
        *node.running.locked() = Some(running.clone().unbind());
        *node.system.locked() = Some(slf.clone().into_any().unbind());
        let entered = node.future(py)?;
        let map = held.map.as_ref().map(|map| map.bind(py));
        node.enter(
            py,
            held.settings.clone(),
            map,
            &running,
            &entered,
            slf.as_any(),
            false,
        )?;
        node.entry(py, entered)
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
});

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
        let refuse = self.refuse.locked().clone();
        // A page that holds a stored value holds it as bytes, and that is what a test names.
        for page in pages.values() {
            if refuse.is_some() && casty_core::wire::Reading::new(page).bytes().ok() == refuse {
                *self.refuse.locked() = None;
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
            .locked()
            .as_ref()
            .map(|page| PyBytes::new(py, page).unbind())
    }

    #[setter]
    fn set_fail_on(&self, page: Option<Vec<u8>>) {
        *self.refuse.locked() = page;
    }
}
