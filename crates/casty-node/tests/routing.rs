//! A message finding the owner of its key across a cluster of nodes of this core.
//!
//! The host here is a stub: it keeps a counter per key instead of running a body, which is enough to say that the
//! message reached exactly one node and that the answer came back to the one that asked.

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use casty_core::mailbox::Command;
use casty_core::node::NodeId;
use casty_core::outcome::Outcome;
use casty_core::replication::messages::Write;
use casty_node::membership::runner::Cluster;
use casty_node::membership::service::Timings;
use casty_node::node::{Host, Kind, Node, Running};
use tokio::sync::oneshot;

const ACTOR: &str = "tests.app:account";
const REPLICAS: usize = 3;
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

fn cluster(seeds: &[String]) -> Cluster {
    Cluster {
        seeds: seeds.to_vec(),
        timings: quick(),
        ..Cluster::at("127.0.0.1:0")
    }
}

/// A host that answers every message with the number of messages that key has taken here.
#[derive(Debug, Default)]
struct Counting {
    taken: Mutex<BTreeMap<String, u64>>,
    waiting: Mutex<BTreeMap<i64, oneshot::Sender<Outcome>>>,
}

impl Counting {
    /// Wait for the answer of the request `id`, which the node settles when it arrives.
    fn expect(&self, id: i64) -> oneshot::Receiver<Outcome> {
        let (answer, waiting) = oneshot::channel();
        self.waiting.lock().expect("a live lock").insert(id, answer);
        waiting
    }
}

impl Host for Counting {
    fn hand(&self, node: &Node, command: Command) {
        let Command::Deliver(deliver) = command else {
            return;
        };
        let count = {
            let mut taken = self.taken.lock().expect("a live lock");
            let count = taken.entry(deliver.key.clone()).or_default();
            *count += 1;
            *count
        };
        if let Some(reply) = deliver.reply {
            node.answer(reply, Outcome::Value(count.to_be_bytes().to_vec()));
        }
    }

    fn settle(&self, id: i64, outcome: Outcome) {
        if let Some(answer) = self.waiting.lock().expect("a live lock").remove(&id) {
            let _ = answer.send(outcome);
        }
    }
}

struct World {
    nodes: Vec<Running>,
    hosts: Vec<Arc<Counting>>,
}

impl World {
    async fn start(size: usize) -> Self {
        let types = vec![Kind {
            actor: ACTOR.to_owned(),
            replicas: REPLICAS,
            write: Write::Majority,
        }];
        let mut nodes = Vec::new();
        let mut hosts = Vec::new();
        let mut seed = None;
        for _ in 0..size {
            let host = Arc::new(Counting::default());
            let seeds: Vec<String> = seed.clone().into_iter().collect();
            let node = Running::start(cluster(&seeds), host.clone(), types.clone())
                .await
                .expect("it joined");
            seed = seed.or_else(|| node.node.id().address.clone());
            nodes.push(node);
            hosts.push(host);
        }
        Self { nodes, hosts }
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

    /// Ask `key` from the node `at`, and wait for the answer.
    async fn ask(&self, at: usize, key: &str) -> Outcome {
        let node = &self.nodes[at].node;
        let id = node.take();
        let waiting = self.hosts[at].expect(id);
        node.deliver(ACTOR, key, b"hello".to_vec(), Some(node.waiting(id)));
        tokio::time::timeout(WITHIN, waiting)
            .await
            .expect("the answer never came")
            .expect("the answer was dropped")
    }

    fn taken(&self) -> BTreeMap<String, u64> {
        let mut total: BTreeMap<String, u64> = BTreeMap::new();
        for host in &self.hosts {
            for (key, count) in host.taken.lock().expect("a live lock").iter() {
                *total.entry(key.clone()).or_default() += count;
            }
        }
        total
    }

    fn holders(&self, key: &str) -> BTreeSet<NodeId> {
        self.nodes
            .iter()
            .zip(&self.hosts)
            .filter(|(_, host)| host.taken.lock().expect("a live lock").contains_key(key))
            .map(|(node, _)| node.node.id().clone())
            .collect()
    }

    async fn stop(self) {
        for node in self.nodes {
            node.crash().await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_is_reached_from_every_node_and_lives_on_one_of_them() {
    let mut world = World::start(3).await;
    world.converged(3).await;
    let keys: Vec<String> = (0..12).map(|index| format!("acc-{index}")).collect();

    for key in &keys {
        for at in 0..world.nodes.len() {
            let outcome = world.ask(at, key).await;
            assert!(
                matches!(outcome, Outcome::Value(_)),
                "{key} from {at}: {outcome:?}"
            );
        }
    }

    let taken = world.taken();
    for key in &keys {
        assert_eq!(taken.get(key), Some(&3), "{key} was not taken once per ask");
        assert_eq!(
            world.holders(key).len(),
            1,
            "{key} ran on more than one node"
        );
    }
    // The keys are spread, not all on the sender.
    let owners: BTreeSet<NodeId> = keys.iter().flat_map(|key| world.holders(key)).collect();
    assert!(owners.len() > 1, "every key landed on the same node");
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ask_to_a_node_that_died_fails_instead_of_waiting_for_the_deadline() {
    let mut world = World::start(3).await;
    world.converged(3).await;
    // Find a key the last node owns, so that losing it is what the ask runs into.
    let owned = {
        let mut found = None;
        for index in 0..200 {
            let key = format!("acc-{index}");
            world.ask(0, &key).await;
            let holders = world.holders(&key);
            if holders.contains(world.nodes[2].node.id()) {
                found = Some(key);
                break;
            }
        }
        found.expect("no key of the two hundred landed on the third node")
    };

    let gone = world.nodes.pop().expect("a node to lose");
    world.hosts.pop();
    gone.crash().await;
    world.converged(2).await;

    let node = &world.nodes[0].node;
    let id = node.take();
    let waiting = world.hosts[0].expect(id);
    node.deliver(ACTOR, &owned, b"hello".to_vec(), Some(node.waiting(id)));
    let outcome = tokio::time::timeout(WITHIN, waiting)
        .await
        .expect("the ask never ended")
        .expect("the answer was dropped");

    // Either the key moved and another node took it, or the ask failed; it never waits on a machine that is gone.
    assert!(
        matches!(outcome, Outcome::Value(_) | Outcome::Unreached { .. }),
        "{outcome:?}"
    );
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_alone_takes_its_own_messages() {
    let mut world = World::start(1).await;
    world.converged(1).await;

    let first = world.ask(0, "only").await;
    let second = world.ask(0, "only").await;

    assert_eq!(first, Outcome::Value(1_u64.to_be_bytes().to_vec()));
    assert_eq!(second, Outcome::Value(2_u64.to_be_bytes().to_vec()));
    world.stop().await;
}
