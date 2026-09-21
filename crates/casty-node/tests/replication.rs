//! The state of a key surviving the node that wrote it, across a cluster of nodes of this core.
//!
//! No host runs here: the node is driven through `activate` and `commit` directly, which is what an activation does
//! once the body side of it exists.

use core::time::Duration;
use std::sync::Arc;

use casty_core::mailbox::Command;
use casty_core::outcome::Outcome;
use casty_core::replication::messages::Write;
use casty_core::store::Pages;
use casty_node::membership::runner::Cluster;
use casty_node::membership::service::Timings;
use casty_node::node::{Host, Kind, Node, Running};
use casty_node::replication::service::Failure;

const ACTOR: &str = "tests.app:account";
const WITHIN: Duration = Duration::from_secs(30);

fn quick() -> Timings {
    Timings {
        heartbeat: Duration::from_millis(50),
        suspect_after: Duration::from_millis(500),
        dead_after: Duration::from_millis(500),
        remove_after: Some(Duration::from_secs(1)),
        anti_entropy: Duration::from_millis(250),
        graft_after: Duration::from_millis(100),
        shuffle_every: Duration::from_millis(500),
    }
}

/// A host with no bodies: nothing in these tests routes a message.
#[derive(Debug)]
struct Idle;

impl Host for Idle {
    fn hand(&self, _: &Node, _: Command) {}
    fn settle(&self, _: i64, _: Outcome) {}
}

struct World {
    nodes: Vec<Running>,
    kind: Kind,
    timeout: Duration,
    seed: Option<String>,
}

impl World {
    async fn start(size: usize, replicas: usize, write: Write) -> Self {
        Self::within(size, replicas, write, Duration::from_secs(5)).await
    }

    async fn within(size: usize, replicas: usize, write: Write, timeout: Duration) -> Self {
        let kind = Kind {
            actor: ACTOR.to_owned(),
            replicas,
            write,
        };
        let mut world = Self {
            nodes: Vec::new(),
            kind,
            timeout,
            seed: None,
        };
        for _ in 0..size {
            world.join().await;
        }
        world.converged(size).await;
        world
    }

    /// Bring one more node in through the seed, and wait until every node sees it.
    async fn join(&mut self) {
        let cluster = Cluster {
            seeds: self.seed.clone().into_iter().collect(),
            timings: quick(),
            write_timeout: self.timeout,
            ..Cluster::at("127.0.0.1:0")
        };
        let node = Running::start(cluster, Arc::new(Idle), vec![self.kind.clone()])
            .await
            .expect("it joined");
        self.seed = self.seed.clone().or_else(|| node.node.id().address.clone());
        self.nodes.push(node);
    }

    async fn converged(&mut self, count: usize) {
        let waiting = tokio::time::timeout(WITHIN, async {
            for node in &mut self.nodes {
                node.until(|members| members.len() == count).await;
            }
        })
        .await;
        assert!(waiting.is_ok(), "they never agreed on {count} members");
    }

    fn at(&self, index: usize) -> &Node {
        &self.nodes[index].node
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
        .at(0)
        .activate(ACTOR, "acc-1", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    assert!(held.created, "the key was not there before");
    assert_eq!(state(&held.pages), Some(&b"one"[..]));

    world
        .at(0)
        .commit(ACTOR, "acc-1", pages(b"two"), true)
        .await
        .expect("the replicas confirmed the write");
    world.at(0).release(ACTOR, "acc-1");

    // Another node takes the key over and reads what the first one wrote, not the state it would have started from.
    let taken = world
        .at(1)
        .activate(ACTOR, "acc-1", Some(pages(b"other")))
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
        .at(2)
        .activate(ACTOR, "nowhere", None)
        .await
        .expect("it asked the replicas");
    assert!(held.is_none(), "no replica has a state for the key");
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_owner_a_later_one_replaced_cannot_write_again() {
    let world = World::start(3, 3, Write::Majority).await;
    world
        .at(0)
        .activate(ACTOR, "acc-2", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .at(1)
        .activate(ACTOR, "acc-2", None)
        .await
        .expect("it took the key over")
        .expect("it has a state");

    let refused = world
        .at(0)
        .commit(ACTOR, "acc-2", pages(b"three"), true)
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
        .at(0)
        .activate(ACTOR, "acc-3", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .at(0)
        .commit(ACTOR, "acc-3", pages(b"two"), true)
        .await
        .expect("the replicas confirmed the write");
    world.at(0).release(ACTOR, "acc-3");
    // Gone, and not yet buried: the third replica is still one the activation would wait for.
    world.crash(2).await;

    let taken = world
        .at(1)
        .activate(ACTOR, "acc-3", None)
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
        .at(0)
        .activate(ACTOR, "acc-5", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world.crash(2).await;

    let refused = world
        .at(0)
        .commit(ACTOR, "acc-5", pages(b"two"), true)
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
        .at(0)
        .activate(ACTOR, "acc-4", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world
        .at(0)
        .commit(ACTOR, "acc-4", pages(b"kept"), false)
        .await
        .expect("the replicas confirmed the release");

    let taken = world
        .at(2)
        .activate(ACTOR, "acc-4", None)
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
        .at(0)
        .activate(ACTOR, "acc-6", Some(pages(b"one")))
        .await
        .expect("it took the key")
        .expect("it has a state");
    world.at(0).release(ACTOR, "acc-6");
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
            .at(0)
            .activate(ACTOR, &key, Some(pages(b"start")))
            .await
            .expect("it took the key")
            .expect("it has a state");
        world
            .at(0)
            .commit(ACTOR, &key, pages(key.as_bytes()), false)
            .await
            .expect("the replicas confirmed the write");
        world.at(0).release(ACTOR, &key);
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
            .at(0)
            .activate(ACTOR, &key, None)
            .await
            .expect("it took the key")
            .unwrap_or_else(|| panic!("{key} was lost with the node that held it"));
        assert_eq!(state(&taken.pages), Some(key.as_bytes()));
        world.at(0).release(ACTOR, &key);
    }
    world.stop().await;
}
