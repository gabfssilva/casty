# tests/test_hash_ring.py
import pytest


def test_hash_ring_single_node():
    from casty.cluster.hash_ring import HashRing

    ring = HashRing()
    ring.add_node("node-1")

    assert ring.get_node("counter/c1") == "node-1"
    assert ring.get_node("other/x") == "node-1"


def test_hash_ring_multiple_nodes():
    from casty.cluster.hash_ring import HashRing

    ring = HashRing()
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    nodes = set()
    for i in range(100):
        node = ring.get_node(f"actor/{i}")
        nodes.add(node)

    assert len(nodes) > 1


def test_hash_ring_get_nodes():
    from casty.cluster.hash_ring import HashRing

    ring = HashRing()
    ring.add_node("node-1")
    ring.add_node("node-2")
    ring.add_node("node-3")

    nodes = ring.get_nodes("counter/c1", n=2)
    assert len(nodes) == 2
    assert len(set(nodes)) == 2


def test_hash_ring_remove_node():
    from casty.cluster.hash_ring import HashRing

    ring = HashRing()
    ring.add_node("node-1")
    ring.add_node("node-2")

    ring.remove_node("node-1")

    assert ring.get_node("counter/c1") == "node-2"


def test_hash_ring_consistency():
    from casty.cluster.hash_ring import HashRing

    ring = HashRing()
    ring.add_node("node-1")
    ring.add_node("node-2")

    key = "counter/c1"
    node_before = ring.get_node(key)

    ring.add_node("node-3")

    node_after = ring.get_node(key)
    assert ring.get_node(key) == node_after
