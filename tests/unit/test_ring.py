from __future__ import annotations

import uuid

from casty.placement.ring import TOKEN_SPACE, Ring, TokenRange, key_token, node_token


def nid(i: int) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_DNS, f"node-{i}")


def test_hash_stability() -> None:
    """These values are the wire protocol. If this test breaks, the change is a
    cluster-wide breaking change — do not just update the constants."""
    fixed = uuid.UUID("00000000-0000-0000-0000-000000000001")
    assert node_token(fixed, 0) == 15258524739324641029
    assert node_token(fixed, 1) == 3611124066911778106
    assert key_token("user:42") == 15647646390214308482
    assert key_token("") == 16476032584258269876


def test_owner_is_deterministic_and_within_members() -> None:
    ring_a = Ring.build([nid(1), nid(2), nid(3)])
    ring_b = Ring.build([nid(3), nid(2), nid(1)])  # order must not matter
    for i in range(200):
        key = f"key-{i}"
        assert ring_a.owner(key) == ring_b.owner(key)
        assert ring_a.owner(key) in {nid(1), nid(2), nid(3)}


def test_uniformity_with_128_vnodes() -> None:
    nodes = [nid(i) for i in range(8)]
    ring = Ring.build(nodes, vnodes=128)
    ideal = TOKEN_SPACE / 8
    for node in nodes:
        share = sum(r.size() for r in ring.ranges_of(node))
        assert 0.7 * ideal < share < 1.3 * ideal, f"{node}: {share / ideal:.2f}x ideal"


def test_ranges_partition_the_space() -> None:
    nodes = [nid(i) for i in range(4)]
    ring = Ring.build(nodes, vnodes=32)
    total = sum(r.size() for node in nodes for r in ring.ranges_of(node))
    assert total == TOKEN_SPACE


def test_join_moves_keys_only_to_the_new_node() -> None:
    old = Ring.build([nid(i) for i in range(5)])
    new = Ring.build([nid(i) for i in range(6)])
    keys = [f"key-{i}" for i in range(2000)]
    moved = 0
    for key in keys:
        before, after = old.owner(key), new.owner(key)
        if before != after:
            assert after == nid(5), f"{key} moved to an old node"
            moved += 1
    assert 0 < moved < 2 * len(keys) / 6  # ~1/6 expected


def test_leave_moves_only_the_removed_nodes_keys() -> None:
    old = Ring.build([nid(i) for i in range(5)])
    new = Ring.build([nid(i) for i in range(4)])  # nid(4) left
    for i in range(2000):
        key = f"key-{i}"
        before = old.owner(key)
        if before == nid(4):
            assert new.owner(key) != nid(4)
        else:
            assert new.owner(key) == before


def test_replicas_are_distinct_physical_nodes() -> None:
    ring = Ring.build([nid(i) for i in range(5)], vnodes=128)
    for i in range(500):
        replicas = ring.replicas(f"key-{i}", 3)
        assert len(replicas) == 3
        assert len(set(replicas)) == 3
        assert replicas[0] == ring.owner(f"key-{i}")


def test_replicas_capped_at_cluster_size() -> None:
    ring = Ring.build([nid(1), nid(2)])
    replicas = ring.replicas("some-key", 5)
    assert set(replicas) == {nid(1), nid(2)}
    assert len(replicas) == 2


def test_single_node_owns_everything() -> None:
    ring = Ring.build([nid(1)], vnodes=8)
    assert ring.owner("anything") == nid(1)
    assert ring.replicas("anything", 3) == (nid(1),)
    assert sum(r.size() for r in ring.ranges_of(nid(1))) == TOKEN_SPACE


def test_token_range_membership() -> None:
    r = TokenRange(start=10, end=20)
    assert 15 in r and 20 in r
    assert 10 not in r and 25 not in r
    wrap = TokenRange(start=TOKEN_SPACE - 5, end=5)
    assert TOKEN_SPACE - 1 in wrap and 3 in wrap
    assert 100 not in wrap
