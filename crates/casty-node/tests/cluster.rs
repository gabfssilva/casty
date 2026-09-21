//! Nodes of this core forming one cluster over real sockets.

use core::time::Duration;
use std::collections::BTreeSet;

use casty_core::membership::table::Status;
use casty_node::membership::runner::{Cluster, Joined};
use casty_node::membership::service::{Member, Timings};

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

fn types() -> BTreeSet<String> {
    BTreeSet::from(["tests.app:account".to_owned()])
}

fn cluster(seeds: &[String]) -> Cluster {
    Cluster {
        seeds: seeds.to_vec(),
        timings: quick(),
        ..Cluster::at("127.0.0.1:0")
    }
}

fn alive(members: &[Member]) -> usize {
    members
        .iter()
        .filter(|member| member.status == Status::Alive)
        .count()
}

/// Wait until every node sees `count` members alive.
async fn converged(nodes: &mut [Joined], count: usize) {
    let waiting = tokio::time::timeout(WITHIN, async {
        for node in nodes.iter_mut() {
            node.until(|members| alive(members) == count).await;
        }
    })
    .await;
    assert!(
        waiting.is_ok(),
        "they never agreed on {count}: {:?}",
        nodes
            .iter()
            .map(|node| alive(&node.members()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_that_join_through_a_seed_all_see_each_other() {
    let first = Joined::start(cluster(&[]), types())
        .await
        .expect("a free port");
    let seed = first
        .node()
        .address
        .clone()
        .expect("a bound node has an address");
    let mut nodes = vec![first];
    for _ in 0..4 {
        nodes.push(
            Joined::start(cluster(std::slice::from_ref(&seed)), types())
                .await
                .expect("it joined"),
        );
    }

    converged(&mut nodes, 5).await;

    for node in &nodes {
        let members = node.members();
        assert_eq!(members.len(), 5);
        for member in members {
            assert_eq!(
                member.types,
                types(),
                "a member arrived without the types it hosts"
            );
        }
    }
    for node in nodes {
        node.crash().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_machine_that_disappears_is_suspected_then_declared_dead() {
    let first = Joined::start(cluster(&[]), types())
        .await
        .expect("a free port");
    let seed = first.node().address.clone().expect("an address");
    let mut nodes = vec![first];
    for _ in 0..2 {
        nodes.push(
            Joined::start(cluster(std::slice::from_ref(&seed)), types())
                .await
                .expect("it joined"),
        );
    }
    converged(&mut nodes, 3).await;

    let gone = nodes.pop().expect("a node to lose");
    let lost = gone.node().clone();
    gone.crash().await;

    let noticed = tokio::time::timeout(WITHIN, async {
        for node in &mut nodes {
            node.until(|members| {
                members
                    .iter()
                    .any(|member| member.node == lost && member.status != Status::Alive)
            })
            .await;
        }
    })
    .await;
    assert!(noticed.is_ok(), "nobody noticed the machine was gone");

    // Two of three alive is a majority, so the dead one is taken out of the cluster.
    let removed = tokio::time::timeout(WITHIN, async {
        for node in &mut nodes {
            node.until(|members| !members.iter().any(|member| member.node == lost))
                .await;
        }
    })
    .await;
    assert!(
        removed.is_ok(),
        "the majority never removed the node it buried"
    );

    for node in nodes {
        node.crash().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seed_of_another_cluster_refuses_the_node() {
    let other = Joined::start(
        Cluster {
            name: "another".to_owned(),
            ..cluster(&[])
        },
        types(),
    )
    .await
    .expect("a free port");
    let seed = other.node().address.clone().expect("an address");

    let refused = Joined::start(cluster(std::slice::from_ref(&seed)), types()).await;

    let Err(why) = refused else {
        panic!("a node of another cluster was let in");
    };
    assert!(why.to_string().contains("another"), "{why}");
    other.crash().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_says_goodbye_leaves_the_cluster_at_once() {
    let first = Joined::start(cluster(&[]), types())
        .await
        .expect("a free port");
    let seed = first.node().address.clone().expect("an address");
    let mut nodes = vec![first];
    for _ in 0..2 {
        nodes.push(
            Joined::start(cluster(std::slice::from_ref(&seed)), types())
                .await
                .expect("it joined"),
        );
    }
    converged(&mut nodes, 3).await;

    let going = nodes.pop().expect("a node to lose");
    let left = going.node().clone();
    going.leave().await;

    let noticed = tokio::time::timeout(WITHIN, async {
        for node in &mut nodes {
            node.until(|members| !members.iter().any(|member| member.node == left))
                .await;
        }
    })
    .await;
    // An orderly exit does not wait for the failure detector: the node says it is gone.
    assert!(noticed.is_ok(), "the goodbye was not heard");

    for node in nodes {
        node.crash().await;
    }
}
