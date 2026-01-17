import pytest

from casty.cluster import HashRing


class TestHashRing:
    def test_empty_ring_returns_none(self):
        ring = HashRing()
        assert ring.get_node("key") is None

    def test_single_node(self):
        ring = HashRing()
        ring.add_node("node-1")
        assert ring.get_node("any-key") == "node-1"

    def test_add_remove_node(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        assert ring.node_count == 2

        ring.remove_node("node-1")
        assert ring.node_count == 1
        assert ring.get_node("key") == "node-2"

    def test_consistent_assignment(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        assignments = {f"key-{i}": ring.get_node(f"key-{i}") for i in range(100)}

        for key, expected in assignments.items():
            assert ring.get_node(key) == expected

    def test_minimal_rebalancing(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        before = {f"key-{i}": ring.get_node(f"key-{i}") for i in range(100)}

        ring.add_node("node-3")

        after = {f"key-{i}": ring.get_node(f"key-{i}") for i in range(100)}

        unchanged = sum(1 for k in before if before[k] == after[k])
        assert unchanged >= 50

    def test_preference_list(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        pref = ring.get_preference_list("key", n=3)
        assert len(pref) == 3
        assert len(set(pref)) == 3

    def test_is_responsible(self):
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        key = "test-key"
        responsible = ring.get_node(key)
        assert ring.is_responsible(responsible, key)

    def test_contains(self):
        ring = HashRing()
        ring.add_node("node-1")
        assert "node-1" in ring
        assert "node-2" not in ring
