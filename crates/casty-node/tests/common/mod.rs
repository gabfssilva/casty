//! What the tests over real sockets share: the settings of a node on localhost, the type they place keys of, and
//! waiting for what the nodes see to settle.
#![allow(dead_code, reason = "each test binary uses a part of it")]

use core::time::Duration;

use casty_core::mailbox::Command;
use casty_core::outcome::Outcome;
use casty_core::replication::messages::Write;
use casty_node::membership::service::{Member, Timings};
use casty_node::node::{Cluster, Host, Kind, Node, Running};

pub const ACTOR: &str = "tests.app:account";
pub const WITHIN: Duration = Duration::from_secs(30);
/// How long a wait sleeps before it looks at the nodes again.
pub const POLL: Duration = Duration::from_millis(20);

pub fn quick() -> Timings {
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

/// A node on a free port of localhost that joins through `seeds`, on the quick timings.
pub fn cluster(seeds: &[String]) -> Cluster {
    Cluster {
        seeds: seeds.to_vec(),
        timings: quick(),
        ..Cluster::at("127.0.0.1:0")
    }
}

/// `ACTOR`, with `replicas` copies of each key and `write` confirming a write.
pub fn kind(replicas: usize, write: Write) -> Kind {
    Kind {
        actor: ACTOR.to_owned(),
        replicas,
        write,
        write_timeout: None,
        pinned: false,
        durable: None,
    }
}

/// A host with no bodies, for tests that route no message.
#[derive(Debug)]
pub struct Idle;

impl Host for Idle {
    fn hand(&self, _: &Node, _: Command) {}
    fn settle(&self, _: i64, _: Outcome) {}
}

/// Wait until what every node sees satisfies `settled`.
pub async fn until(nodes: &[Running], settled: impl Fn(&[Member]) -> bool) {
    for running in nodes {
        while !settled(&running.node.members()) {
            tokio::time::sleep(POLL).await;
        }
    }
}

/// Wait until every node sees `count` members, and places a sample of keys on the same nodes: `replicas` of them, or
/// every member when there are fewer.
///
/// A node that joined holds the keys of the ranges it gained on the ring before it until they have arrived, so
/// agreeing on the members is not yet agreeing on the owners.
pub async fn converged(nodes: &[Running], count: usize, replicas: usize) {
    let members =
        tokio::time::timeout(WITHIN, until(nodes, |members| members.len() == count)).await;
    assert!(members.is_ok(), "they never agreed on {count} members");
    let placed = tokio::time::timeout(WITHIN, placed(nodes, count.min(replicas))).await;
    assert!(placed.is_ok(), "they never agreed on where the keys are");
}

async fn placed(nodes: &[Running], replicas: usize) {
    loop {
        let mut agreed = true;
        for index in 0..200 {
            let key = format!("acc-{index}");
            let mut seen = Vec::new();
            for running in nodes {
                seen.push(running.node.placed(ACTOR, &key).await);
            }
            agreed &= seen.windows(2).all(|pair| pair[0] == pair[1])
                && seen.iter().all(|placed| placed.replicas.len() == replicas);
        }
        if agreed {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Stop every node without a word, which is what machines going away look like to the others.
pub async fn crash(nodes: Vec<Running>) {
    for node in nodes {
        node.crash().await;
    }
}
