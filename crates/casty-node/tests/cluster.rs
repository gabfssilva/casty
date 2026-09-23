//! Nodes of this core forming one cluster over real sockets.

mod common;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;

use casty_core::membership::table::Status;
use casty_core::replication::messages::Write;
use casty_node::membership::service::Member;
use casty_node::node::{Cluster, Running};

use common::{ACTOR, Idle, WITHIN, cluster, kind, until};

/// A node that hosts the type of the tests and runs no body.
async fn start(cluster: Cluster) -> io::Result<Running> {
    Running::start(cluster, Arc::new(Idle), vec![kind(3, Write::Majority)]).await
}

fn alive(members: &[Member]) -> usize {
    members
        .iter()
        .filter(|member| member.status == Status::Alive)
        .count()
}

/// Wait until every node sees `count` members alive.
async fn converged(nodes: &[Running], count: usize) {
    let waiting =
        tokio::time::timeout(WITHIN, until(nodes, |members| alive(members) == count)).await;
    assert!(
        waiting.is_ok(),
        "they never agreed on {count}: {:?}",
        nodes
            .iter()
            .map(|running| alive(&running.node.members()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_that_join_through_a_seed_all_see_each_other() {
    let first = start(cluster(&[])).await.expect("a free port");
    let seed = first
        .node
        .id()
        .address
        .clone()
        .expect("a bound node has an address");
    let mut nodes = vec![first];
    for _ in 0..4 {
        nodes.push(
            start(cluster(std::slice::from_ref(&seed)))
                .await
                .expect("it joined"),
        );
    }

    converged(&nodes, 5).await;

    for running in &nodes {
        let members = running.node.members();
        assert_eq!(members.len(), 5);
        for member in members {
            assert_eq!(
                member.types,
                BTreeSet::from([ACTOR.to_owned()]),
                "a member arrived without the types it hosts"
            );
        }
    }
    common::crash(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_machine_that_disappears_is_suspected_then_declared_dead() {
    let first = start(cluster(&[])).await.expect("a free port");
    let seed = first.node.id().address.clone().expect("an address");
    let mut nodes = vec![first];
    for _ in 0..2 {
        nodes.push(
            start(cluster(std::slice::from_ref(&seed)))
                .await
                .expect("it joined"),
        );
    }
    converged(&nodes, 3).await;

    let gone = nodes.pop().expect("a node to lose");
    let lost = gone.node.id().clone();
    gone.crash().await;

    let noticed = tokio::time::timeout(
        WITHIN,
        until(&nodes, |members| {
            members
                .iter()
                .any(|member| member.node == lost && member.status != Status::Alive)
        }),
    )
    .await;
    assert!(noticed.is_ok(), "nobody noticed the machine was gone");

    // Two of three alive is a majority, so the dead one is taken out of the cluster.
    let removed = tokio::time::timeout(
        WITHIN,
        until(&nodes, |members| {
            !members.iter().any(|member| member.node == lost)
        }),
    )
    .await;
    assert!(
        removed.is_ok(),
        "the majority never removed the node it buried"
    );

    common::crash(nodes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seed_of_another_cluster_refuses_the_node() {
    let other = start(Cluster {
        name: "another".to_owned(),
        ..cluster(&[])
    })
    .await
    .expect("a free port");
    let seed = other.node.id().address.clone().expect("an address");

    let refused = start(cluster(std::slice::from_ref(&seed))).await;

    let Err(why) = refused else {
        panic!("a node of another cluster was let in");
    };
    assert!(why.to_string().contains("another"), "{why}");
    other.crash().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_says_goodbye_leaves_the_cluster_at_once() {
    let first = start(cluster(&[])).await.expect("a free port");
    let seed = first.node.id().address.clone().expect("an address");
    let mut nodes = vec![first];
    for _ in 0..2 {
        nodes.push(
            start(cluster(std::slice::from_ref(&seed)))
                .await
                .expect("it joined"),
        );
    }
    converged(&nodes, 3).await;

    let going = nodes.pop().expect("a node to lose");
    let left = going.node.id().clone();
    going.leave().await;

    let noticed = tokio::time::timeout(
        WITHIN,
        until(&nodes, |members| {
            !members.iter().any(|member| member.node == left)
        }),
    )
    .await;
    // An orderly exit does not wait for the failure detector: the node says it is gone.
    assert!(noticed.is_ok(), "the goodbye was not heard");

    common::crash(nodes).await;
}
