from __future__ import annotations

from casty.cluster_state import (
    VectorClock,
    NodeAddress,
    Member,
    MemberStatus,
    ClusterState,
    ServiceEntry,
)


def test_vector_clock_increment() -> None:
    vc = VectorClock()
    node = NodeAddress(host="10.0.0.1", port=25520)
    vc2 = vc.increment(node)
    assert vc2.version_of(node) == 1
    vc3 = vc2.increment(node)
    assert vc3.version_of(node) == 2


def test_vector_clock_merge() -> None:
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    vc_a = VectorClock().increment(node_a).increment(node_a)  # A=2
    vc_b = VectorClock().increment(node_b)  # B=1

    merged = vc_a.merge(vc_b)
    assert merged.version_of(node_a) == 2
    assert merged.version_of(node_b) == 1


def test_vector_clock_ordering() -> None:
    node = NodeAddress(host="10.0.0.1", port=25520)
    vc1 = VectorClock().increment(node)
    vc2 = vc1.increment(node)

    assert vc1.is_before(vc2)
    assert not vc2.is_before(vc1)
    assert not vc1.is_concurrent_with(vc2)


def test_vector_clock_concurrent() -> None:
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    vc_a = VectorClock().increment(node_a)
    vc_b = VectorClock().increment(node_b)

    assert vc_a.is_concurrent_with(vc_b)


def test_member_creation() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.up, roles=frozenset(), id="node-1")
    assert member.status == MemberStatus.up


def test_cluster_state_add_member() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.joining, roles=frozenset(), id="node-1")
    state = ClusterState()
    new_state = state.add_member(member)
    assert member in new_state.members


def test_cluster_state_update_member_status() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.joining, roles=frozenset(), id="node-1")
    state = ClusterState().add_member(member)
    new_state = state.update_status(addr, MemberStatus.up)
    updated = next(m for m in new_state.members if m.address == addr)
    assert updated.status == MemberStatus.up


def test_cluster_state_mark_unreachable() -> None:
    addr = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=addr, status=MemberStatus.up, roles=frozenset(), id="node-1")
    state = ClusterState().add_member(member)
    new_state = state.mark_unreachable(addr)
    assert addr in new_state.unreachable


def test_cluster_state_leader_is_lowest_up_address() -> None:
    addr_a = NodeAddress(host="10.0.0.1", port=25520)
    addr_b = NodeAddress(host="10.0.0.2", port=25520)
    state = (
        ClusterState()
        .add_member(Member(address=addr_a, status=MemberStatus.up, roles=frozenset(), id="node-a"))
        .add_member(Member(address=addr_b, status=MemberStatus.up, roles=frozenset(), id="node-b"))
    )
    assert state.leader == addr_a


def test_node_address_ordering() -> None:
    a = NodeAddress(host="10.0.0.1", port=25520)
    b = NodeAddress(host="10.0.0.2", port=25520)
    assert a < b


def test_cluster_state_has_registry_field() -> None:
    state = ClusterState()
    assert state.registry == frozenset()


def test_cluster_state_registry_preserved_on_add_member() -> None:
    node = NodeAddress(host="10.0.0.1", port=2551)
    entry = ServiceEntry(key="payment", node=node, path="/payment")
    state = ClusterState(registry=frozenset({entry}))
    member = Member(address=node, status=MemberStatus.up, roles=frozenset(), id="n1")
    new_state = state.add_member(member)
    assert new_state.registry == frozenset({entry})


def test_cluster_state_registry_preserved_on_update_status() -> None:
    node = NodeAddress(host="10.0.0.1", port=2551)
    entry = ServiceEntry(key="payment", node=node, path="/payment")
    member = Member(address=node, status=MemberStatus.up, roles=frozenset(), id="n1")
    state = ClusterState(
        members=frozenset({member}), registry=frozenset({entry})
    )
    new_state = state.update_status(node, MemberStatus.leaving)
    assert new_state.registry == frozenset({entry})


def test_cluster_state_registry_preserved_on_mark_unreachable() -> None:
    node = NodeAddress(host="10.0.0.1", port=2551)
    entry = ServiceEntry(key="payment", node=node, path="/payment")
    state = ClusterState(registry=frozenset({entry}))
    new_state = state.mark_unreachable(node)
    assert new_state.registry == frozenset({entry})


def test_cluster_state_registry_preserved_on_mark_reachable() -> None:
    node = NodeAddress(host="10.0.0.1", port=2551)
    entry = ServiceEntry(key="payment", node=node, path="/payment")
    state = ClusterState(
        unreachable=frozenset({node}), registry=frozenset({entry})
    )
    new_state = state.mark_reachable(node)
    assert new_state.registry == frozenset({entry})
