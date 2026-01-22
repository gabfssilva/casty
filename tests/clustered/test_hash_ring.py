from __future__ import annotations

import pytest

from casty.cluster.hash_ring import HashRing


class TestHashRingBasic:
    def test_add_and_get_node(self):
        ring = HashRing()
        ring.add_node("node-1")

        node = ring.get_node("test-key")
        assert node == "node-1"

    def test_consistent_hashing(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        node1 = ring.get_node("user:123")
        node2 = ring.get_node("user:123")
        assert node1 == node2

    def test_get_node_empty_ring_raises(self):
        ring = HashRing()

        with pytest.raises(RuntimeError, match="HashRing is empty"):
            ring.get_node("test-key")

    def test_remove_node(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        ring.remove_node("node-1")

        assert ring.nodes == {"node-2"}
        assert ring.get_node("test-key") == "node-2"

    def test_nodes_property(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        assert ring.nodes == {"node-1", "node-2", "node-3"}


class TestHashRingMultipleNodes:
    def test_get_n_nodes_returns_n_nodes(self):
        ring = HashRing()
        for i in range(5):
            ring.add_node(f"node-{i}")

        nodes = ring.get_n_nodes("test-key", n=3)

        assert len(nodes) == 3
        assert len(set(nodes)) == 3  # all unique

    def test_get_n_nodes_with_n_greater_than_available(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        nodes = ring.get_n_nodes("test-key", n=5)

        assert len(nodes) == 2  # returns all available
        assert set(nodes) == {"node-1", "node-2"}

    def test_get_n_nodes_consistent_for_same_key(self):
        ring = HashRing()
        for i in range(4):
            ring.add_node(f"node-{i}")

        nodes1 = ring.get_n_nodes("user:123", n=3)
        nodes2 = ring.get_n_nodes("user:123", n=3)

        assert nodes1 == nodes2

    def test_get_n_nodes_first_is_same_as_get_node(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        single = ring.get_node("test-key")
        multiple = ring.get_n_nodes("test-key", n=3)

        assert multiple[0] == single

    def test_get_n_nodes_returns_clockwise_order(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        nodes_a = ring.get_n_nodes("key-a", n=3)
        nodes_b = ring.get_n_nodes("key-b", n=3)

        assert set(nodes_a) == {"node-1", "node-2", "node-3"}
        assert set(nodes_b) == {"node-1", "node-2", "node-3"}

    def test_get_n_nodes_empty_ring(self):
        ring = HashRing()

        nodes = ring.get_n_nodes("test", n=3)

        assert nodes == []

    def test_get_n_nodes_single_node(self):
        ring = HashRing()
        ring.add_node("node-1")

        nodes = ring.get_n_nodes("test-key", n=3)

        assert nodes == ["node-1"]

    def test_get_n_nodes_n_zero(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        nodes = ring.get_n_nodes("test-key", n=0)

        assert nodes == []


class TestHashRingDistribution:
    def test_keys_distributed_across_nodes(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        node_counts: dict[str, int] = {}
        for i in range(1000):
            node = ring.get_node(f"key-{i}")
            node_counts[node] = node_counts.get(node, 0) + 1

        assert len(node_counts) == 3
        for count in node_counts.values():
            assert count > 100  # each node gets reasonable share

    def test_virtual_nodes_parameter(self):
        ring = HashRing(virtual_nodes=10)
        ring.add_node("node-1")
        ring.add_node("node-2")

        assert len(ring._sorted_keys) == 20  # 2 nodes * 10 virtual nodes
