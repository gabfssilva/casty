"""Tests for consistent hashing hash ring."""

import pytest

from casty.cluster.data_plane import HashRing, HashRingConfig, VirtualNode


class TestHashRingConfig:
    """Tests for HashRingConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = HashRingConfig()
        assert config.virtual_nodes == 150
        assert config.hash_function == "md5"

    def test_custom_config(self):
        """Test custom configuration."""
        config = HashRingConfig(virtual_nodes=100, hash_function="sha1")
        assert config.virtual_nodes == 100
        assert config.hash_function == "sha1"


class TestVirtualNode:
    """Tests for VirtualNode."""

    def test_virtual_node_creation(self):
        """Test creating a virtual node."""
        vnode = VirtualNode(node_id="node-1", virtual_id=5, position=12345)
        assert vnode.node_id == "node-1"
        assert vnode.virtual_id == 5
        assert vnode.position == 12345

    def test_virtual_node_comparison(self):
        """Test virtual node comparison by position."""
        v1 = VirtualNode(node_id="node-1", virtual_id=0, position=100)
        v2 = VirtualNode(node_id="node-2", virtual_id=0, position=200)
        v3 = VirtualNode(node_id="node-1", virtual_id=1, position=50)

        assert v3 < v1 < v2
        assert not v2 < v1

    def test_virtual_node_immutable(self):
        """Test that virtual nodes are frozen."""
        vnode = VirtualNode(node_id="node-1", virtual_id=0, position=100)
        with pytest.raises(AttributeError):
            vnode.position = 200  # type: ignore


class TestHashRing:
    """Tests for HashRing basic operations."""

    def test_empty_ring(self):
        """Test empty hash ring."""
        ring = HashRing()
        assert len(ring) == 0
        assert ring.node_count == 0
        assert ring.vnode_count == 0
        assert ring.nodes == frozenset()

    def test_add_single_node(self):
        """Test adding a single node."""
        ring = HashRing()
        ring.add_node("node-1")

        assert len(ring) == 1
        assert ring.node_count == 1
        assert "node-1" in ring
        assert ring.vnode_count == 150  # Default virtual nodes

    def test_add_multiple_nodes(self):
        """Test adding multiple nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        assert len(ring) == 3
        assert ring.nodes == frozenset({"node-1", "node-2", "node-3"})
        assert ring.vnode_count == 450

    def test_add_duplicate_node(self):
        """Test that adding duplicate node is idempotent."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-1")

        assert len(ring) == 1
        assert ring.vnode_count == 150

    def test_remove_node(self):
        """Test removing a node."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        ring.remove_node("node-1")

        assert len(ring) == 1
        assert "node-1" not in ring
        assert "node-2" in ring
        assert ring.vnode_count == 150

    def test_remove_nonexistent_node(self):
        """Test removing a node that doesn't exist."""
        ring = HashRing()
        ring.add_node("node-1")

        moves = ring.remove_node("node-999")
        assert moves == []
        assert len(ring) == 1

    def test_custom_virtual_nodes(self):
        """Test with custom number of virtual nodes."""
        config = HashRingConfig(virtual_nodes=50)
        ring = HashRing(config)
        ring.add_node("node-1")

        assert ring.vnode_count == 50


class TestHashRingLookup:
    """Tests for hash ring key lookup."""

    def test_get_node_empty_ring(self):
        """Test lookup on empty ring."""
        ring = HashRing()
        assert ring.get_node("key") is None

    def test_get_node_single_node(self):
        """Test lookup with single node."""
        ring = HashRing()
        ring.add_node("node-1")

        # All keys should map to the only node
        assert ring.get_node("key1") == "node-1"
        assert ring.get_node("key2") == "node-1"
        assert ring.get_node("key3") == "node-1"

    def test_get_node_deterministic(self):
        """Test that lookup is deterministic."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        # Same key should always return same node
        node = ring.get_node("user:123")
        for _ in range(100):
            assert ring.get_node("user:123") == node

    def test_get_node_distribution(self):
        """Test that keys are distributed across nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        # Check distribution of many keys
        distribution: dict[str, int] = {"node-1": 0, "node-2": 0, "node-3": 0}
        for i in range(1000):
            node = ring.get_node(f"key-{i}")
            assert node is not None
            distribution[node] += 1

        # Each node should get a reasonable share (roughly 1/3 each)
        for node, count in distribution.items():
            assert 200 < count < 500, f"{node} got {count} keys, expected ~333"

    def test_preference_list_empty_ring(self):
        """Test preference list on empty ring."""
        ring = HashRing()
        assert ring.get_preference_list("key") == []

    def test_preference_list_single_node(self):
        """Test preference list with single node."""
        ring = HashRing()
        ring.add_node("node-1")

        prefs = ring.get_preference_list("key", n=3)
        assert prefs == ["node-1"]

    def test_preference_list_multiple_nodes(self):
        """Test preference list returns unique physical nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        prefs = ring.get_preference_list("user:123", n=3)
        assert len(prefs) == 3
        assert len(set(prefs)) == 3  # All unique
        # Primary is first
        assert prefs[0] == ring.get_node("user:123")

    def test_preference_list_limited(self):
        """Test preference list respects n limit."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")
        ring.add_node("node-4")
        ring.add_node("node-5")

        prefs = ring.get_preference_list("key", n=2)
        assert len(prefs) == 2

    def test_preference_list_more_than_nodes(self):
        """Test preference list when n > node count."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        prefs = ring.get_preference_list("key", n=5)
        assert len(prefs) == 2


class TestHashRingRebalancing:
    """Tests for minimal rebalancing on node changes."""

    def test_add_node_minimal_movement(self):
        """Test that adding a node moves minimal keys."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        # Record initial placements
        initial: dict[str, str] = {}
        for i in range(1000):
            key = f"key-{i}"
            node = ring.get_node(key)
            assert node is not None
            initial[key] = node

        # Add a third node
        ring.add_node("node-3")

        # Count how many keys moved
        moved = 0
        for key, old_node in initial.items():
            new_node = ring.get_node(key)
            if new_node != old_node:
                moved += 1

        # Should be roughly 1/3 of keys (moving to new node)
        assert 200 < moved < 500, f"Moved {moved} keys, expected ~333"

    def test_remove_node_minimal_movement(self):
        """Test that removing a node moves minimal keys."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        # Record initial placements
        initial: dict[str, str] = {}
        for i in range(1000):
            key = f"key-{i}"
            node = ring.get_node(key)
            assert node is not None
            initial[key] = node

        # Remove one node
        ring.remove_node("node-2")

        # Count how many keys moved
        moved = 0
        for key, old_node in initial.items():
            new_node = ring.get_node(key)
            if old_node == "node-2":
                # Keys from removed node must move
                assert new_node in ("node-1", "node-3")
                moved += 1
            else:
                # Keys from other nodes should stay
                assert new_node == old_node

        # Should be roughly 1/3 of keys (from removed node)
        assert 200 < moved < 500, f"Moved {moved} keys, expected ~333"


class TestHashRingRanges:
    """Tests for range-based operations."""

    def test_get_ranges_for_node_empty(self):
        """Test ranges for non-existent node."""
        ring = HashRing()
        ring.add_node("node-1")

        ranges = ring.get_ranges_for_node("node-999")
        assert ranges == []

    def test_get_ranges_for_node_single(self):
        """Test ranges for single node (should cover entire ring)."""
        ring = HashRing()
        ring.add_node("node-1")

        ranges = ring.get_ranges_for_node("node-1")
        # Should have as many ranges as virtual nodes
        assert len(ranges) == 150

    def test_get_ranges_for_node_multiple(self):
        """Test ranges are split among nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        ranges1 = ring.get_ranges_for_node("node-1")
        ranges2 = ring.get_ranges_for_node("node-2")

        # Each should have their virtual nodes' ranges
        assert len(ranges1) == 150
        assert len(ranges2) == 150

    def test_get_nodes_for_range_empty(self):
        """Test nodes for range on empty ring."""
        ring = HashRing()
        nodes = ring.get_nodes_for_range(0, 1000)
        assert nodes == []

    def test_get_nodes_for_range(self):
        """Test getting nodes responsible for a range."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")

        nodes = ring.get_nodes_for_range(0, 1000000, n=3)
        assert len(nodes) == 3
        assert len(set(nodes)) == 3  # All unique


class TestHashRingRepr:
    """Tests for string representation."""

    def test_repr_empty(self):
        """Test repr of empty ring."""
        ring = HashRing()
        assert "HashRing" in repr(ring)
        assert "nodes=0" in repr(ring)
        assert "vnodes=0" in repr(ring)

    def test_repr_with_nodes(self):
        """Test repr with nodes."""
        ring = HashRing()
        ring.add_node("node-1")
        ring.add_node("node-2")

        r = repr(ring)
        assert "HashRing" in r
        assert "nodes=2" in r
        assert "vnodes=300" in r
