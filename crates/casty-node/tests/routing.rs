//! A message finding the owner of its key across a cluster of nodes of this core.
//!
//! The host here is a stub: it keeps a counter per key instead of running a body, which is enough to say that the
//! message reached exactly one node and that the answer came back to the one that asked.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use casty_core::chain::Chain;
use casty_core::mailbox::{Command, Deliver};
use casty_core::node::{NodeId, Target};
use casty_core::outcome::Outcome;
use casty_core::replication::messages::Write;
use casty_core::wire::Writer;
use casty_net::endpoint::{Config, Endpoint, TooLarge};
use casty_net::pool::Target as Address;
use casty_node::membership::runner::Cluster;
use casty_node::node::{Host, Node, Running};
use casty_node::routing::wire::{Answer, Message, Routed, decode_answer, encode};
use tokio::sync::{Notify, oneshot};

use common::{ACTOR, WITHIN, cluster, kind};

const REPLICAS: usize = 3;

/// A host that answers every message with the number of messages that key has taken here.
#[derive(Debug, Default)]
struct Counting {
    taken: Mutex<BTreeMap<String, u64>>,
    waiting: Mutex<BTreeMap<i64, oneshot::Sender<Outcome>>>,
    /// The cancellations that reached this host, as the key and the request they name.
    cancelled: Mutex<Vec<(String, Target)>>,
    cancelling: Notify,
}

impl Counting {
    /// Wait for the answer of the request `id`, which the node settles when it arrives.
    fn expect(&self, id: i64) -> oneshot::Receiver<Outcome> {
        let (answer, waiting) = oneshot::channel();
        self.waiting.lock().expect("a live lock").insert(id, answer);
        waiting
    }

    /// Wait until a cancellation has reached this host, and give back every one that has.
    async fn cancellations(&self) -> Vec<(String, Target)> {
        loop {
            let seen = self.cancelled.lock().expect("a live lock").clone();
            if !seen.is_empty() {
                return seen;
            }
            self.cancelling.notified().await;
        }
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

    fn cancel(&self, _: &str, key: &str, request: &Target) {
        self.cancelled
            .lock()
            .expect("a live lock")
            .push((key.to_owned(), request.clone()));
        // A permit is kept when nobody waits yet, so a cancellation that lands before the wait is not missed.
        self.cancelling.notify_one();
    }
}

struct World {
    nodes: Vec<Running>,
    hosts: Vec<Arc<Counting>>,
}

impl World {
    async fn start(size: usize) -> Self {
        let types = vec![kind(REPLICAS, Write::Majority)];
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

    async fn converged(&self, count: usize) {
        common::converged(&self.nodes, count, REPLICAS).await;
    }

    /// Ask `key` from the node `at`, and wait for the answer.
    async fn ask(&self, at: usize, key: &str) -> Outcome {
        let node = &self.nodes[at].node;
        let id = node.take();
        let waiting = self.hosts[at].expect(id);
        node.deliver(
            ACTOR,
            key,
            b"hello".to_vec(),
            Some(node.waiting(id)),
            Chain::default(),
        )
        .expect("it fits");
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
        common::crash(self.nodes).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_is_reached_from_every_node_and_lives_on_one_of_them() {
    let world = World::start(3).await;
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
    node.deliver(
        ACTOR,
        &owned,
        b"hello".to_vec(),
        Some(node.waiting(id)),
        Chain::default(),
    )
    .expect("it fits");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancellation_reaches_the_node_that_took_the_request_and_no_other() {
    let world = World::start(3).await;
    world.converged(3).await;
    // A key another node took, so that the cancellation crosses the network the way the request did.
    let asker = world.nodes[0].node.id().clone();
    let mut far = None;
    for index in 0..200 {
        let key = format!("acc-{index}");
        world.ask(0, &key).await;
        let holders = world.holders(&key);
        if !holders.contains(&asker) {
            far = Some((key, holders));
            break;
        }
    }
    let (key, holders) = far.expect("every key of the two hundred landed on the node that asked");
    let holder = world
        .nodes
        .iter()
        .position(|node| holders.contains(node.node.id()))
        .expect("a node took the key");

    let node = &world.nodes[0].node;
    let request = node.waiting(node.take());
    node.cancel(ACTOR, &key, request.clone());
    let seen = tokio::time::timeout(WITHIN, world.hosts[holder].cancellations())
        .await
        .expect("the cancellation never arrived");

    assert_eq!(seen, vec![(key, request)]);
    for (at, host) in world.hosts.iter().enumerate() {
        if at != holder {
            assert!(
                host.cancelled.lock().expect("a live lock").is_empty(),
                "the cancellation also reached node {at}"
            );
        }
    }
    world.stop().await;
}

/// What a peer that knows no cancellation meets in one that sends them: a message of a kind it does not know, which it
/// drops, and the connection it came on goes on carrying the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_of_a_kind_the_node_does_not_know_is_dropped_and_what_follows_it_is_answered() {
    let world = World::start(1).await;
    world.converged(1).await;
    let mut peer = Endpoint::start(Config::default())
        .await
        .expect("it started");
    let node = Address::Node(world.nodes[0].node.id().clone());

    let mut unknown = Writer::new();
    unknown.tagged("Unheard", 1);
    unknown.name("actor");
    unknown.text(ACTOR);
    peer.send(&node, "actors", &unknown.finish())
        .expect("it fits");
    let request = Routed {
        command: Command::Deliver(Deliver {
            actor: ACTOR.to_owned(),
            key: "only".to_owned(),
            message: b"hello".to_vec(),
            reply: Some(Target::Reply {
                node: peer.node().clone(),
                id: 1,
            }),
            chain: Chain::default(),
        }),
        origin: peer.node().clone(),
        attempt: 1,
    };
    peer.send(&node, "actors", &encode(&Message::Routed(request)))
        .expect("it fits");
    let received = tokio::time::timeout(WITHIN, peer.recv())
        .await
        .expect("the answer never came")
        .expect("the endpoint closed")
        .expect("the node refused the peer");

    assert_eq!(received.name, "replies");
    assert_eq!(
        decode_answer(&received.payload).expect("an answer"),
        Answer {
            id: 1,
            outcome: Outcome::Value(1_u64.to_be_bytes().to_vec()),
        }
    );
    peer.close(true).await;
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_alone_takes_its_own_messages() {
    let world = World::start(1).await;
    world.converged(1).await;

    let first = world.ask(0, "only").await;
    let second = world.ask(0, "only").await;

    assert_eq!(first, Outcome::Value(1_u64.to_be_bytes().to_vec()));
    assert_eq!(second, Outcome::Value(2_u64.to_be_bytes().to_vec()));
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_does_not_fit_in_one_message_is_refused_even_when_the_key_is_here() {
    let world = World::start(1).await;
    world.converged(1).await;
    let node = &world.nodes[0].node;
    let limit = Cluster::at("127.0.0.1:0").limits.message;

    let id = node.take();
    let refused = node.deliver(
        ACTOR,
        "only",
        vec![0; limit],
        Some(node.waiting(id)),
        Chain::default(),
    );
    assert!(
        refused.is_err_and(|TooLarge(why)| why.contains("limits.message")),
        "a message of the limit itself went out"
    );
    assert!(node.start(ACTOR, "only", Some(vec![0; limit])).is_err());
    // Nothing went out: the next message is the first the key takes.
    assert_eq!(
        world.ask(0, "only").await,
        Outcome::Value(1_u64.to_be_bytes().to_vec())
    );

    let id = node.take();
    let waiting = world.hosts[0].expect(id);
    node.answer(node.waiting(id), Outcome::Value(vec![0; limit]));
    let outcome = tokio::time::timeout(WITHIN, waiting)
        .await
        .expect("the answer never came")
        .expect("the answer was dropped");
    assert!(
        matches!(&outcome, Outcome::TooLarge(why) if why.contains("limits.message")),
        "{outcome:?}"
    );
    world.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_node_places_a_key_where_its_messages_land() {
    let world = World::start(3).await;
    world.converged(3).await;

    for index in 0..12 {
        let key = format!("acc-{index}");
        world.ask(0, &key).await;
        let holders = world.holders(&key);
        for running in &world.nodes {
            let placed = running.node.placed(ACTOR, &key).await;
            assert_eq!(placed.replicas.len(), REPLICAS, "{key}: {placed:?}");
            assert_eq!(
                placed.owner.as_ref(),
                placed.replicas.first(),
                "{key}: {placed:?}"
            );
            assert_eq!(
                placed.owner.into_iter().collect::<BTreeSet<_>>(),
                holders,
                "{key} is placed away from where it ran"
            );
        }
    }
    // A pinned key is placed on the node its address names, and on no other.
    for pinned in &world.nodes {
        let address = pinned
            .node
            .id()
            .address
            .clone()
            .expect("a member has an address");
        let key = casty_core::placement::pin(&address, "worker");
        for running in &world.nodes {
            let placed = running.node.placed(ACTOR, &key).await;
            assert_eq!(placed.owner.as_ref(), Some(pinned.node.id()), "{key}");
            assert_eq!(placed.replicas, vec![pinned.node.id().clone()], "{key}");
        }
    }
    world.stop().await;
}
