"""Tests for HashRing consistent hashing."""

import pytest

from casty.cluster.hash_ring import HashRing, VirtualNode


class TestHashRingBasic:
    """Basic HashRing operations."""

    def test_empty_ring_returns_none(self):
        """Empty ring should return None for any key."""
        ring = HashRing()
        assert ring.get_node("any-key") is None
        assert ring.get_preference_list("any-key") == []

    def test_add_single_node(self):
        """Single node should handle all keys."""
        ring = HashRing()
        ring.add_node("node-1")

        assert ring.node_count == 1
        assert ring.vnode_count == 150  # default virtual nodes
        assert "node-1" in ring
        assert ring.get_node("any-key") == "node-1"
        assert ring.get_node("another-key") == "node-1"

    def test_add_duplicate_node_is_idempotent(self):
        """Adding same node twice should be no-op."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-1")

        assert ring.node_count == 1
        assert ring.vnode_count == 150

    def test_remove_node(self):
        """Removing a node should remove all its virtual nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        assert ring.node_count == 2
        assert ring.vnode_count == 300

        ring.remove_node("node-1")

        assert ring.node_count == 1
        assert ring.vnode_count == 150
        assert "node-1" not in ring
        assert "node-2" in ring

    def test_remove_nonexistent_node_is_noop(self):
        """Removing non-existent node should be no-op."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.remove_node("node-2")  # doesn't exist

        assert ring.node_count == 1

    def test_nodes_property(self):
        """nodes property should return frozenset of all nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        nodes = ring.nodes
        assert isinstance(nodes, frozenset)
        assert nodes == {"node-1", "node-2"}


class TestHashRingDeterminism:
    """Hash ring should be deterministic."""

    def test_same_key_same_node(self):
        """Same key should always map to same node."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        key = "users:user-123"
        node1 = ring.get_node(key)
        node2 = ring.get_node(key)
        node3 = ring.get_node(key)

        assert node1 == node2 == node3

    def test_deterministic_across_instances(self):
        """Different ring instances with same nodes should give same results."""
        ring1 = HashRing()
        ring1.add_node("node-1")
        ring1.add_node("node-2")
        ring1.add_node("node-3")

        ring2 = HashRing()
        ring2.add_node("node-1")
        ring2.add_node("node-2")
        ring2.add_node("node-3")

        for i in range(100):
            key = f"entity:{i}"
            assert ring1.get_node(key) == ring2.get_node(key)

    def test_add_order_independent(self):
        """Node addition order shouldn't affect key mapping."""
        ring1 = HashRing()
        ring1.add_node("node-1")
        ring1.add_node("node-2")
        ring1.add_node("node-3")

        ring2 = HashRing()
        ring2.add_node("node-3")
        ring2.add_node("node-1")
        ring2.add_node("node-2")

        for i in range(100):
            key = f"entity:{i}"
            assert ring1.get_node(key) == ring2.get_node(key)


class TestHashRingDistribution:
    """Hash ring should distribute keys evenly."""

    def test_distribution_across_nodes(self):
        """Keys should be roughly evenly distributed."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        counts = {"node-1": 0, "node-2": 0, "node-3": 0}
        num_keys = 10000

        for i in range(num_keys):
            key = f"entity:{i}"
            node = ring.get_node(key)
            counts[node] += 1

        # Each node should get roughly 1/3 of keys
        # Allow 20% deviation
        expected = num_keys / 3
        for node, count in counts.items():
            deviation = abs(count - expected) / expected
            assert deviation < 0.20, f"{node} got {count}, expected ~{expected}"

    def test_minimal_rebalancing_on_add(self):
        """Adding a node should only move ~1/n keys."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        # Record initial assignments
        keys = [f"entity:{i}" for i in range(1000)]
        initial = {key: ring.get_node(key) for key in keys}

        # Add third node
        ring.add_node("node-3")

        # Count how many keys moved
        moved = 0
        for key in keys:
            if ring.get_node(key) != initial[key]:
                moved += 1

        # Roughly 1/3 should move to new node
        move_ratio = moved / len(keys)
        assert 0.20 < move_ratio < 0.45, f"Moved {move_ratio:.1%}, expected ~33%"

    def test_minimal_rebalancing_on_remove(self):
        """Removing a node should only move its keys."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        # Record initial assignments
        keys = [f"entity:{i}" for i in range(1000)]
        initial = {key: ring.get_node(key) for key in keys}

        # Remove one node
        ring.remove_node("node-2")

        # Only keys that were on node-2 should move
        moved = 0
        for key in keys:
            old_node = initial[key]
            new_node = ring.get_node(key)
            if old_node != new_node:
                moved += 1
                assert old_node == "node-2", "Only node-2's keys should move"

        # Roughly 1/3 should have moved
        move_ratio = moved / len(keys)
        assert 0.20 < move_ratio < 0.45, f"Moved {move_ratio:.1%}"


class TestPreferenceList:
    """Tests for preference list (replication)."""

    def test_preference_list_single_node(self):
        """Single node cluster returns one-element list."""
        ring = HashRing()
        ring.add_node("node-1")

        prefs = ring.get_preference_list("any-key", n=3)
        assert prefs == ["node-1"]

    def test_preference_list_returns_unique_nodes(self):
        """Preference list should not have duplicates."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        prefs = ring.get_preference_list("any-key", n=3)
        assert len(prefs) == 3
        assert len(set(prefs)) == 3  # all unique

    def test_preference_list_first_is_primary(self):
        """First node in preference list should be primary."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        key = "users:user-456"
        prefs = ring.get_preference_list(key, n=3)
        primary = ring.get_node(key)

        assert prefs[0] == primary

    def test_preference_list_respects_n(self):
        """Should return at most n nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")
        ring.add_node("node-4")
        ring.add_node("node-5")

        prefs = ring.get_preference_list("key", n=2)
        assert len(prefs) == 2

    def test_preference_list_deterministic(self):
        """Preference list should be deterministic."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        key = "orders:order-789"
        prefs1 = ring.get_preference_list(key, n=3)
        prefs2 = ring.get_preference_list(key, n=3)

        assert prefs1 == prefs2


class TestIsResponsible:
    """Tests for is_responsible method."""

    def test_is_responsible_primary(self):
        """Primary node should be responsible."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        key = "entity:123"
        primary = ring.get_node(key)

        assert ring.is_responsible(primary, key) is True

    def test_is_responsible_non_primary(self):
        """Non-primary nodes should not be responsible."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        key = "entity:123"
        primary = ring.get_node(key)
        others = [n for n in ring.nodes if n != primary]

        for other in others:
            assert ring.is_responsible(other, key) is False


class TestVirtualNodes:
    """Tests for virtual node configuration."""

    def test_custom_virtual_nodes(self):
        """Should respect custom virtual node count."""
        ring = HashRing(virtual_nodes=50)
        ring.add_node("node-1")

        assert ring.vnode_count == 50

    def test_more_vnodes_better_distribution(self):
        """More virtual nodes should give better distribution."""
        # Low vnode count
        ring_low = HashRing(virtual_nodes=10)
        ring_low.add_node("node-1")
        ring_low.add_node("node-2")

        # High vnode count
        ring_high = HashRing(virtual_nodes=200)
        ring_high.add_node("node-1")
        ring_high.add_node("node-2")

        def measure_deviation(ring):
            counts = {"node-1": 0, "node-2": 0}
            for i in range(1000):
                counts[ring.get_node(f"key:{i}")] += 1
            return abs(counts["node-1"] - counts["node-2"]) / 1000

        low_deviation = measure_deviation(ring_low)
        high_deviation = measure_deviation(ring_high)

        # Higher vnodes should have lower deviation
        assert high_deviation <= low_deviation


class TestRepr:
    """Tests for string representation."""

    def test_repr(self):
        """repr should show node and vnode counts."""
        ring = HashRing(virtual_nodes=100)
        ring.add_node("node-1")
        ring.add_node("node-2")

        assert repr(ring) == "HashRing(nodes=2, vnodes=200)"

    def test_len(self):
        """len should return physical node count."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        assert len(ring) == 2
