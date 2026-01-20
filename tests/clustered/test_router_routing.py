from __future__ import annotations

import pytest

from casty.cluster.router import select_target
from casty.cluster.replication import ReplicationConfig, Routing


def test_select_target_leader_routing():
    replicas = ["node-1", "node-2", "node-3"]
    config = ReplicationConfig(factor=3, routing=Routing.LEADER)

    target = select_target(
        replicas=replicas,
        config=config,
        local_node="node-2",
        is_write=True,
    )
    assert target == "node-1"


def test_select_target_any_routing_write_local():
    replicas = ["node-1", "node-2", "node-3"]
    config = ReplicationConfig(factor=3, routing=Routing.ANY)

    target = select_target(
        replicas=replicas,
        config=config,
        local_node="node-2",
        is_write=True,
    )
    assert target == "node-2"


def test_select_target_local_first_read():
    replicas = ["node-1", "node-2", "node-3"]
    config = ReplicationConfig(factor=3, routing=Routing.LOCAL_FIRST)

    target = select_target(
        replicas=replicas,
        config=config,
        local_node="node-3",
        is_write=False,
    )
    assert target == "node-3"


def test_select_target_local_first_not_replica():
    replicas = ["node-1", "node-2", "node-3"]
    config = ReplicationConfig(factor=3, routing=Routing.LOCAL_FIRST)

    target = select_target(
        replicas=replicas,
        config=config,
        local_node="node-99",
        is_write=False,
    )
    assert target == "node-1"
