from __future__ import annotations

import pytest
from casty.cluster.replica_manager import ReplicaManager
from casty.cluster.hash_ring import HashRing


class TestRebalancing:
    def test_get_actors_needing_rebalance(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2"], "node-1")  # needs 1 more
        manager.register("actor2", ["node-1", "node-2", "node-3"], "node-1")  # full

        needing = manager.get_actors_needing_rebalance(target_replicas=3)

        assert needing == ["actor1"]

    def test_suggest_new_replica_node(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2"], "node-1")

        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")
        ring.add_node("node-4")

        suggestion = manager.suggest_new_replica("actor1", ring)

        # Should suggest a node not already in the replica set
        assert suggestion is not None
        assert suggestion not in ["node-1", "node-2"]

    def test_suggest_new_replica_no_available_nodes(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2"], "node-1")

        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        # all nodes already in use

        suggestion = manager.suggest_new_replica("actor1", ring)

        assert suggestion is None

    def test_rebalance_adds_node(self):
        manager = ReplicaManager()
        manager.register("actor1", ["node-1", "node-2"], "node-1")

        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        new_node = manager.suggest_new_replica("actor1", ring)
        if new_node:
            manager.add_node("actor1", new_node)

        assert len(manager.get_replicas("actor1")) == 3
