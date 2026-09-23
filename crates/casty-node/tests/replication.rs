//! The state of a key surviving the node that wrote it, across a cluster of nodes of this core.
//!
//! No host runs here: the node is driven through `activate` and `commit` directly, which is what an activation does
//! once the body side of it exists.

mod common;

use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use casty_core::mailbox::Command;
use casty_core::outcome::Outcome;
use casty_core::replication::messages::Write;
use casty_core::store::{Durable, Held, Pages, Storage, Stored, version};
use casty_net::limits::Limits;
use casty_node::node::{Cluster, Host, Kind, Node, Running};
use casty_node::replication::service::{Failure, Storing};

use common::{ACTOR, Idle, WITHIN, cluster, kind};

const MIB: usize = 1024 * 1024;

/// A host with no bodies whose store every node given it shares, the way the nodes of a cluster share a database: one
/// record per key, kept as a store must.
#[derive(Debug, Default)]
struct Records {
    kept: Mutex<HashMap<(String, String), Stored>>,
    /// The state page of every save asked, in the order the nodes asked them.
    saved: Mutex<Vec<Option<Vec<u8>>>>,
}

impl Records {
    /// The state page the store keeps for `key`.
    fn state(&self, key: &str) -> Option<Vec<u8>> {
        self.kept
            .lock()
            .unwrap()
            .get(&(ACTOR.to_owned(), key.to_owned()))
            .and_then(|record| record.pages.as_ref())
            .and_then(|pages| state(pages).map(<[u8]>::to_vec))
    }

    fn saved(&self) -> Vec<Option<Vec<u8>>> {
        self.saved.lock().unwrap().clone()
    }
}

impl Host for Records {
    fn hand(&self, _: &Node, _: Command) {}
    fn settle(&self, _: i64, _: Outcome) {}

    fn store(&self, node: &Node, request: Storing) {
        let entity = (request.actor, request.key);
        let mut kept = self.kept.lock().unwrap();
        let answer = match request.storage {
            Storage::Load => Ok(kept.get(&entity).cloned()),
            Storage::Save(stored) => {
                self.saved.lock().unwrap().push(
                    stored
                        .pages
                        .as_ref()
                        .and_then(|pages| state(pages).map(<[u8]>::to_vec)),
                );
                if kept
                    .get(&entity)
                    .is_none_or(|record| record.version() <= stored.version())
                {
                    kept.insert(entity, stored);
                }
                Ok(None)
            }
            Storage::Drop(stamp) => {
                if kept
                    .get(&entity)
                    .is_some_and(|record| record.version() <= version(&stamp))
                {
                    kept.remove(&entity);
                }
                Ok(None)
            }
        };
        drop(kept);
        node.from_store(request.id, answer);
    }
}

struct World {
    nodes: Vec<Running>,
    kind: Kind,
    timeout: Duration,
    limits: Limits,
    seed: Option<String>,
    host: Arc<dyn Host>,
    /// The lease each node was given for each key it activated, which its writes carry.
    leases: Mutex<HashMap<(usize, String), u64>>,
}

impl World {
    async fn start(size: usize, replicas: usize, write: Write) -> Self {
        Self::within(size, replicas, write, Duration::from_secs(5)).await
    }

    async fn within(size: usize, replicas: usize, write: Write, timeout: Duration) -> Self {
        Self::built(size, replicas, write, timeout, Limits::default()).await
    }

    /// Nodes whose messages carry at most `limits.message` bytes, so that a state can be larger than one.
    async fn limited(size: usize, replicas: usize, write: Write, limits: Limits) -> Self {
        Self::built(size, replicas, write, Duration::from_secs(5), limits).await
    }

    async fn built(
        size: usize,
        replicas: usize,
        write: Write,
        timeout: Duration,
        limits: Limits,
    ) -> Self {
        Self::of(size, kind(replicas, write), timeout, limits, Arc::new(Idle)).await
    }

    /// Nodes of a type whose every write `store` keeps, which each node is given as the store of its cluster.
    async fn stored(size: usize, replicas: usize, store: &Arc<Records>) -> Self {
        let kind = Kind {
            durable: Some(Durable::Write),
            ..kind(replicas, Write::Majority)
        };
        let host: Arc<dyn Host> = store.clone();
        Self::of(size, kind, Duration::from_secs(5), Limits::default(), host).await
    }

    async fn of(
        size: usize,
        kind: Kind,
        timeout: Duration,
        limits: Limits,
        host: Arc<dyn Host>,
    ) -> Self {
        let mut world = Self {
            leases: Mutex::new(HashMap::new()),
            nodes: Vec::new(),
            kind,
            timeout,
            limits,
            seed: None,
            host,
        };
        for _ in 0..size {
            world.join().await;
        }
        world.converged(size).await;
        world
    }

    /// Bring one more node in through the seed, and wait until every node sees it.
    async fn join(&mut self) {
        let seeds: Vec<String> = self.seed.clone().into_iter().collect();
        let cluster = Cluster {
            write_timeout: self.timeout,
            limits: self.limits,
            ..cluster(&seeds)
        };
        let node = Running::start(cluster, Arc::clone(&self.host), vec![self.kind.clone()])
            .await
            .expect("it joined");
        self.seed = self.seed.clone().or_else(|| node.node.id().address.clone());
        self.nodes.push(node);
    }

    async fn converged(&self, count: usize) {
        common::converged(&self.nodes, count, self.kind.replicas).await;
    }

    fn at(&self, index: usize) -> &Node {
        &self.nodes[index].node
    }

    fn lease(&self, index: usize, key: &str) -> u64 {
        self.leases
            .lock()
            .unwrap()
            .get(&(index, key.to_owned()))
            .copied()
            .unwrap_or_default()
    }

    /// Node `index` takes `key` over, as an activation of it does, keeping the lease for the writes that follow.
    async fn activate(
        &self,
        index: usize,
        key: &str,
        initial: Option<Pages>,
    ) -> Result<Option<Held>, Failure> {
        let held = self.at(index).activate(ACTOR, key, initial).await;
        if let Ok(Some(held)) = &held {
            self.leases
                .lock()
                .unwrap()
                .insert((index, key.to_owned()), held.lease);
        }
        held
    }

    async fn commit(
        &self,
        index: usize,
        key: &str,
        pages: Pages,
        active: bool,
    ) -> Result<(), Failure> {
        let lease = self.lease(index, key);
        self.at(index)
            .commit(ACTOR, key, lease, pages, active)
            .await
    }

    async fn delete(&self, index: usize, key: &str, active: bool) -> Result<(), Failure> {
        let lease = self.lease(index, key);
        self.at(index).delete(ACTOR, key, lease, active).await
    }

    fn release(&self, index: usize, key: &str) {
        self.at(index).release(ACTOR, key, self.lease(index, key));
    }

    /// Take a node away without a word, which is what a machine going away looks like to the others.
    async fn crash(&mut self, index: usize) {
        self.nodes.remove(index).crash().await;
    }

    /// Take a node out the way it is meant to go: it gives away what it keeps first.
    async fn depart(&mut self, index: usize) -> Vec<(String, String)> {
        self.nodes.remove(index).leave().await
    }

    async fn stop(self) {
        for node in self.nodes {
            node.leave().await;
        }
    }
}

fn pages(state: &[u8]) -> Pages {
    Pages::from([("state".to_owned(), state.to_vec())])
}

/// What a key holds, without the mark a write carries for the replicas.
fn state(pages: &Pages) -> Option<&[u8]> {
    pages.get("state").map(Vec::as_slice)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_written_state_is_found_by_the_next_owner() {
    let world = World::start(3, 3, Write::Majority).await;
    let held = world
        .activate(0, "acc-1", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    assert!(held.created, "the key was not there before");
    assert_eq!(state(&held.pages), Some(&b"one"[..]));

    world
        .commit(0, "acc-1", pages(b"two"), true)
        .await
        .expect("the replicas confirmed the write");
    world.release(0, "acc-1");

    // Another node takes the key over and reads what the first one wrote, not the state it would have started from.
    let taken = world
        .activate(1, "acc-1", Some(pages(b"other")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    assert!(!taken.created, "the key was already there");
    assert_eq!(state(&taken.pages), Some(&b"two"[..]));
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_no_replica_holds_and_that_has_no_initial_state_is_missing() {
    let world = World::start(3, 3, Write::Majority).await;
    let held = world
        .activate(2, "nowhere", None)
        .await
        .expect("it asked the replicas");
    assert!(held.is_none(), "no replica has a state for the key");
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_owner_a_later_one_replaced_cannot_write_again() {
    let world = World::start(3, 3, Write::Majority).await;
    world
        .activate(0, "acc-2", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .activate(1, "acc-2", None)
        .await
        .expect("it took the key over")
        .expect("it has a state");

    let refused = world
        .commit(0, "acc-2", pages(b"three"), true)
        .await
        .expect_err("the key moved to another owner");
    assert!(
        matches!(refused, Failure::Fencing(_)),
        "a fenced write ended in {refused:?}"
    );
    world.stop().await;
}

/// An activation waits for every replica the table has not buried, and decides without the ones that went quiet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_activation_settles_with_the_replicas_that_are_alive() {
    let mut world = World::start(3, 3, Write::Majority).await;
    world
        .activate(0, "acc-3", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .commit(0, "acc-3", pages(b"two"), true)
        .await
        .expect("the replicas confirmed the write");
    world.release(0, "acc-3");
    // Gone, and not yet buried: the third replica is still one the activation would wait for.
    world.crash(2).await;

    let taken = world
        .activate(1, "acc-3", None)
        .await
        .expect("it took the key over")
        .expect("it has a state");
    assert_eq!(state(&taken.pages), Some(&b"two"[..]));
    world.stop().await;
}

/// A write no quorum can confirm has no grace: it waits for the replicas until the call gives up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_no_quorum_can_confirm_ends_unavailable() {
    let mut world = World::within(3, 3, Write::All, Duration::from_millis(500)).await;
    world
        .activate(0, "acc-5", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world.crash(2).await;

    let refused = world
        .commit(0, "acc-5", pages(b"two"), true)
        .await
        .expect_err("one replica of the three is gone");
    assert!(
        matches!(refused, Failure::Unavailable(_)),
        "a write nobody could confirm ended in {refused:?}"
    );
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_lets_the_key_go_without_losing_its_state() {
    let world = World::start(3, 3, Write::Majority).await;
    world
        .activate(0, "acc-4", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .commit(0, "acc-4", pages(b"kept"), false)
        .await
        .expect("the replicas confirmed the release");

    let taken = world
        .activate(2, "acc-4", None)
        .await
        .expect("it took the key")
        .expect("the state outlived the activation");
    assert_eq!(state(&taken.pages), Some(&b"kept"[..]));
    world.stop().await;
}

/// A node leaving while another is dead does not wait for the dead one to take what it hands over.
///
/// The node that leaves cannot bury the dead one: leaving costs it its own vote, and burying takes a majority of
/// the members that are alive. So the dead node stays in the table, and in the replicas of every key this one gives
/// away. Counting it there is waiting out the whole handover budget for an answer that cannot come.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_leaves_does_not_wait_for_a_dead_replica_to_take_its_keys() {
    let mut world = World::start(3, 3, Write::Majority).await;
    world
        .activate(0, "acc-6", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world.release(0, "acc-6");
    world.crash(2).await;

    let began = std::time::Instant::now();
    let owed = world.depart(0).await;
    let took = began.elapsed();
    assert!(owed.is_empty(), "it left without handing over {owed:?}");
    assert!(
        took < WITHIN / 3,
        "the leave waited {took:?} for a node that is gone"
    );
    world.stop().await;
}

/// With one replica per key, a key has nowhere else to be: what the node that leaves keeps is what it hands over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_keys_of_a_node_that_leaves_are_taken_by_the_ones_that_replicate_them_now() {
    const KEYS: usize = 24;
    let mut world = World::start(2, 1, Write::All).await;
    for index in 0..KEYS {
        let key = format!("acc-{index}");
        world
            .activate(0, &key, Some(pages(b"start")))
            .await
            .expect("it took the key")
            .expect("it has a state");
        world
            .commit(0, &key, pages(key.as_bytes()), false)
            .await
            .expect("the replicas confirmed the write");
        world.release(0, &key);
    }
    // A third node takes ranges over from both, and then the first one goes, handing away what it still keeps.
    world.join().await;
    world.converged(3).await;
    let owed = world.depart(0).await;
    assert!(owed.is_empty(), "it left without handing over {owed:?}");
    world.converged(2).await;

    for index in 0..KEYS {
        let key = format!("acc-{index}");
        let taken = world
            .activate(0, &key, None)
            .await
            .expect("it took the key")
            .unwrap_or_else(|| panic!("{key} was lost with the node that held it"));
        assert_eq!(state(&taken.pages), Some(key.as_bytes()));
        world.release(0, &key);
    }
    world.stop().await;
}

/// Messages of 320 KiB, which leave 256 KiB of each for the state it carries.
fn narrow() -> Limits {
    Limits {
        message: 320 * 1024,
        ..Limits::default()
    }
}

/// `size` bytes that differ from one another from `from` on, so that a piece out of place shows.
fn long(size: usize, from: usize) -> Vec<u8> {
    (0..=u8::MAX).cycle().skip(from).take(size).collect()
}

/// A page four times what a message carries crosses the transport in parts, and never as one message it would drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_larger_than_a_message_is_written_in_parts_and_read_back_whole() {
    let world = World::limited(3, 3, Write::Majority, narrow()).await;
    let first = long(MIB, 0);
    let held = world
        .activate(0, "large-1", Some(pages(&first)))
        .await
        .expect("it took the key")
        .expect("it has a state");
    assert!(
        state(&held.pages) == Some(first.as_slice()),
        "the activation answered with another state"
    );

    let grown = long(MIB + 1_000, 7);
    world
        .commit(0, "large-1", pages(&grown), true)
        .await
        .expect("the replicas confirmed the write");
    world.release(0, "large-1");

    // No promise can carry the state, so the next owner fetches it in parts.
    let taken = world
        .activate(1, "large-1", None)
        .await
        .expect("it took the key over")
        .expect("it has a state");
    assert!(
        state(&taken.pages) == Some(grown.as_slice()),
        "the next owner read another state"
    );
    world.stop().await;
}

/// The ranges a new node pulls and the keys a node that leaves hands over carry a page larger than a message too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_larger_than_a_message_moves_whole_with_ranges_and_handovers() {
    const KEYS: usize = 6;
    let mut world = World::limited(2, 1, Write::All, narrow()).await;
    for index in 0..KEYS {
        let key = format!("large-{index}");
        world
            .activate(0, &key, Some(pages(b"start")))
            .await
            .expect("it took the key")
            .expect("it has a state");
        world
            .commit(0, &key, pages(&long(MIB, index)), false)
            .await
            .expect("the replicas confirmed the write");
        world.release(0, &key);
    }
    world.join().await;
    world.converged(3).await;
    let owed = world.depart(0).await;
    assert!(owed.is_empty(), "it left without handing over {owed:?}");
    world.converged(2).await;

    for index in 0..KEYS {
        let key = format!("large-{index}");
        let taken = world
            .activate(0, &key, None)
            .await
            .expect("it took the key")
            .unwrap_or_else(|| panic!("{key} was lost on the way"));
        assert!(
            state(&taken.pages) == Some(long(MIB, index).as_slice()),
            "{key} came back different"
        );
        world.release(0, &key);
    }
    world.stop().await;
}

/// Messages of 96 KiB, which leave 32 KiB of each for the keys a range or a handover carries.
fn tight() -> Limits {
    Limits {
        message: 96 * 1024,
        ..Limits::default()
    }
}

/// A node that leaves hands over more keys than one message can name. Each message it sends is answered for the keys
/// it completed, so no answer lists them all, and the leave does not wait out its budget for one the transport
/// refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handover_of_more_keys_than_one_message_can_name_completes() {
    const KEYS: usize = 60;
    // Names of 8 KiB: the ones the node that leaves keeps take several messages to list.
    let name = |index: usize| format!("{index:02}-{}", "k".repeat(8 * 1024));
    let mut world = World::limited(2, 1, Write::All, tight()).await;
    for index in 0..KEYS {
        let key = name(index);
        world
            .activate(0, &key, Some(pages(b"start")))
            .await
            .expect("it took the key")
            .expect("it has a state");
        world
            .commit(0, &key, pages(index.to_string().as_bytes()), false)
            .await
            .expect("the replicas confirmed the write");
        world.release(0, &key);
    }

    let owed = world.depart(0).await;
    assert!(
        owed.is_empty(),
        "it left without handing over {} keys",
        owed.len()
    );
    world.converged(1).await;

    for index in 0..KEYS {
        let key = name(index);
        let taken = world
            .activate(0, &key, None)
            .await
            .expect("it took the key")
            .unwrap_or_else(|| panic!("key {index} was lost with the node that held it"));
        assert_eq!(state(&taken.pages), Some(index.to_string().as_bytes()));
        world.release(0, &key);
    }
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_key_keeps_no_state_anywhere_and_starts_again_from_its_initial_state() {
    let world = World::start(3, 3, Write::Majority).await;
    world
        .activate(0, "acc-9", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .commit(0, "acc-9", pages(b"two"), true)
        .await
        .expect("the replicas confirmed the write");

    world
        .delete(0, "acc-9", false)
        .await
        .expect("the replicas confirmed the deletion");

    // What a node keeps of the key is its tombstone at most, until every replica has answered for it.
    for index in 0..3 {
        let stored = world.at(index).stored().await;
        assert!(
            stored
                .iter()
                .all(|(_, key, deleted)| key != "acc-9" || *deleted),
            "node {index} kept the state of a deleted key: {stored:?}"
        );
    }
    let missing = world
        .activate(1, "acc-9", None)
        .await
        .expect("it asked the replicas");
    assert!(missing.is_none(), "a deleted key came back");
    let taken = world
        .activate(1, "acc-9", Some(pages(b"fresh")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    assert!(taken.created, "the key was deleted, and nothing had it");
    assert_eq!(state(&taken.pages), Some(&b"fresh"[..]));
    world.stop().await;
}

/// The criterion of 14.2: every node of the cluster goes, a new cluster starts on the same store, and a key no replica
/// holds comes up from the store and not from the state its activation brings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_that_lost_every_node_takes_its_keys_back_from_the_store() {
    let store = Arc::new(Records::default());
    let mut world = World::stored(3, 3, &store).await;
    world
        .activate(0, "durable-1", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .commit(0, "durable-1", pages(b"two"), true)
        .await
        .expect("the replicas and the store confirmed the write");
    assert_eq!(store.state("durable-1").as_deref(), Some(&b"two"[..]));
    for _ in 0..3 {
        world.crash(0).await;
    }

    let world = World::stored(3, 3, &store).await;
    let taken = world
        .activate(1, "durable-1", Some(pages(b"initial")))
        .await
        .expect("it took the key")
        .expect("it has a state");

    assert!(!taken.created, "the key came up as one nothing wrote");
    assert_eq!(state(&taken.pages), Some(&b"two"[..]));
    // Its writes go on above what the cluster before saved, so the store keeps them.
    world
        .commit(1, "durable-1", pages(b"three"), true)
        .await
        .expect("the replicas and the store confirmed the write");
    assert_eq!(store.state("durable-1").as_deref(), Some(&b"three"[..]));
    world.stop().await;
}

/// The criterion of 14.2: a write the replicas refused, because another node took the key, never reaches the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fenced_owner_s_write_never_reaches_the_store() {
    let store = Arc::new(Records::default());
    let world = World::stored(3, 3, &store).await;
    world
        .activate(0, "durable-2", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .commit(0, "durable-2", pages(b"two"), true)
        .await
        .expect("the replicas and the store confirmed the write");
    world
        .activate(1, "durable-2", None)
        .await
        .expect("it took the key over")
        .expect("it has a state");

    let refused = world
        .commit(0, "durable-2", pages(b"three"), true)
        .await
        .expect_err("the key moved to another owner");

    assert!(
        matches!(refused, Failure::Fencing(_)),
        "a fenced write ended in {refused:?}"
    );
    assert!(
        !store.saved().contains(&Some(b"three".to_vec())),
        "the store was asked to keep a write the replicas refused"
    );
    assert_eq!(store.state("durable-2").as_deref(), Some(&b"two"[..]));
    world.stop().await;
}

/// The criterion of 14.2: keys handed to another node, and written there, are read back after a restart of everything
/// as that node left them, and not as the node that held them before saved them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_handed_to_another_node_are_not_read_back_from_what_the_old_one_saved() {
    const KEYS: usize = 8;
    let key = |index: usize| format!("handed-{index}");
    let store = Arc::new(Records::default());
    let mut world = World::stored(2, 1, &store).await;
    for index in 0..KEYS {
        world
            .activate(0, &key(index), Some(pages(b"start")))
            .await
            .expect("it took the key")
            .expect("it has a state");
        world
            .commit(0, &key(index), pages(b"before"), false)
            .await
            .expect("the replicas and the store confirmed the write");
        world.release(0, &key(index));
    }

    // The first node goes, handing what it keeps to the other one, which writes every key again.
    let owed = world.depart(0).await;
    assert!(owed.is_empty(), "it left without handing over {owed:?}");
    world.converged(1).await;
    for index in 0..KEYS {
        world
            .activate(0, &key(index), None)
            .await
            .expect("it took the key")
            .expect("the key was handed over");
        world
            .commit(0, &key(index), pages(b"after"), false)
            .await
            .expect("the replicas and the store confirmed the write");
        world.release(0, &key(index));
    }

    // Everything goes, and a new cluster reads each key back from the store.
    world.crash(0).await;
    let world = World::stored(2, 1, &store).await;
    for index in 0..KEYS {
        let taken = world
            .activate(index % 2, &key(index), None)
            .await
            .expect("it took the key")
            .expect("the store kept the key");
        assert_eq!(
            state(&taken.pages),
            Some(&b"after"[..]),
            "{} came back as another node saved it",
            key(index)
        );
        world.release(index % 2, &key(index));
    }
    world.stop().await;
}
