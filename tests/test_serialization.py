from __future__ import annotations

from dataclasses import dataclass

import pytest

from casty.address import ActorAddress
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    VectorClock,
)
from casty.ref import ActorRef
from casty.serialization import JsonSerializer, Serializer, TypeRegistry
from casty.sharding import ShardEnvelope
from casty.transport import LocalTransport
from casty.shard_coordinator_actor import GetShardLocation, ShardLocation


@dataclass(frozen=True)
class Greet:
    name: str


@dataclass(frozen=True)
class Count:
    value: int


@dataclass(frozen=True)
class UserGreet:
    name: str
    count: int


def _make_registry() -> TypeRegistry:
    registry = TypeRegistry()
    return registry


def _ref_factory(addr: ActorAddress) -> ActorRef[object]:
    transport = LocalTransport()
    return ActorRef(address=addr, _transport=transport)


# --- Original tests ---


def test_type_registry_register_and_resolve() -> None:
    registry = TypeRegistry()
    registry.register(Greet)
    resolved = registry.resolve(registry.type_name(Greet))
    assert resolved is Greet


def test_type_registry_unknown_raises() -> None:
    registry = TypeRegistry()
    with pytest.raises(KeyError):
        registry.resolve("nonexistent.Type")


def test_json_serializer_roundtrip_simple() -> None:
    registry = TypeRegistry()
    registry.register(Greet)
    serializer = JsonSerializer(registry)
    original = Greet(name="Alice")
    data = serializer.serialize(original)
    restored = serializer.deserialize(data)
    assert restored == original


def test_json_serializer_roundtrip_int_field() -> None:
    registry = TypeRegistry()
    registry.register(Count)
    serializer = JsonSerializer(registry)
    original = Count(value=42)
    data = serializer.serialize(original)
    restored = serializer.deserialize(data)
    assert restored == original


def test_json_serializer_implements_protocol() -> None:
    registry = TypeRegistry()
    serializer = JsonSerializer(registry)
    assert isinstance(serializer, Serializer)


def test_json_serializer_address_roundtrip() -> None:
    registry = TypeRegistry()
    registry.register(ActorAddress)
    serializer = JsonSerializer(registry)
    addr = ActorAddress(system="sys", host="10.0.0.1", port=9000, path="/a/b")
    data = serializer.serialize(addr)
    restored = serializer.deserialize(data)
    assert restored == addr


# --- Phase 1: Enhanced serialization tests ---


def test_nested_dataclass_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry, ref_factory=_ref_factory)

    inner = UserGreet(name="Alice", count=42)
    envelope = ShardEnvelope(entity_id="user-1", message=inner)

    data = serializer.serialize(envelope)
    restored = serializer.deserialize(data)

    assert isinstance(restored, ShardEnvelope)
    assert isinstance(restored.message, UserGreet)
    assert restored.entity_id == "user-1"
    assert restored.message.name == "Alice"
    assert restored.message.count == 42


def test_actor_ref_serialization() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry, ref_factory=_ref_factory)

    reply_ref: ActorRef[ShardLocation] = ActorRef(
        address=ActorAddress(
            system="test", path="/reply", host="127.0.0.1", port=25521
        ),
        _transport=LocalTransport(),
    )
    msg = GetShardLocation(shard_id=7, reply_to=reply_ref)

    data = serializer.serialize(msg)
    restored = serializer.deserialize(data)

    assert isinstance(restored, GetShardLocation)
    assert restored.shard_id == 7
    assert isinstance(restored.reply_to, ActorRef)
    assert restored.reply_to.address.host == "127.0.0.1"
    assert restored.reply_to.address.port == 25521
    assert restored.reply_to.address.path == "/reply"


def test_frozenset_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry, ref_factory=_ref_factory)

    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    member1 = Member(address=node1, status=MemberStatus.up, roles=frozenset({"web"}))
    member2 = Member(
        address=node2, status=MemberStatus.joining, roles=frozenset({"worker"})
    )
    state = ClusterState(members=frozenset({member1, member2}))

    data = serializer.serialize(state)
    restored = serializer.deserialize(data)

    assert isinstance(restored, ClusterState)
    assert len(restored.members) == 2
    addresses = {m.address for m in restored.members}
    assert node1 in addresses
    assert node2 in addresses


def test_enum_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry, ref_factory=_ref_factory)

    node = NodeAddress(host="127.0.0.1", port=25520)
    member = Member(address=node, status=MemberStatus.leaving, roles=frozenset())

    data = serializer.serialize(member)
    restored = serializer.deserialize(data)

    assert isinstance(restored, Member)
    assert restored.status == MemberStatus.leaving
    assert restored.address == node
    assert restored.roles == frozenset()


def test_vector_clock_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry, ref_factory=_ref_factory)

    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    clock = VectorClock().increment(node1).increment(node1).increment(node2)

    data = serializer.serialize(clock)
    restored = serializer.deserialize(data)

    assert isinstance(restored, VectorClock)
    assert restored.version_of(node1) == 2
    assert restored.version_of(node2) == 1


def test_full_cluster_state_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry, ref_factory=_ref_factory)

    node1 = NodeAddress(host="10.0.0.1", port=25520)
    node2 = NodeAddress(host="10.0.0.2", port=25521)
    member1 = Member(
        address=node1, status=MemberStatus.up, roles=frozenset({"seed", "web"})
    )
    member2 = Member(
        address=node2, status=MemberStatus.up, roles=frozenset({"worker"})
    )
    clock = VectorClock().increment(node1).increment(node2)
    state = ClusterState(
        members=frozenset({member1, member2}),
        unreachable=frozenset({node2}),
        version=clock,
    )

    data = serializer.serialize(state)
    restored = serializer.deserialize(data)

    assert isinstance(restored, ClusterState)
    assert len(restored.members) == 2
    assert len(restored.unreachable) == 1
    assert node2 in restored.unreachable
    assert restored.version.version_of(node1) == 1
    assert restored.version.version_of(node2) == 1


def test_auto_register_on_serialize() -> None:
    registry = TypeRegistry()
    serializer = JsonSerializer(registry)

    @dataclass(frozen=True)
    class Unknown:
        value: int

    data = serializer.serialize(Unknown(value=42))
    result = serializer.deserialize(data)
    assert result == Unknown(value=42)
    assert registry.is_registered(Unknown)


def test_shard_location_with_replicas_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry, ref_factory=_ref_factory)

    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    location = ShardLocation(shard_id=3, node=node1, replicas=[node2])

    data = serializer.serialize(location)
    restored = serializer.deserialize(data)

    assert isinstance(restored, ShardLocation)
    assert restored.shard_id == 3
    assert restored.node == node1
    assert len(restored.replicas) == 1
    assert restored.replicas[0] == node2
