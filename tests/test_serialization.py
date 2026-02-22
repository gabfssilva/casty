from __future__ import annotations

from dataclasses import dataclass

import pytest

from casty.core.address import ActorAddress
from casty.cluster.state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    VectorClock,
)
from casty.ref import ActorRef
from casty.remote.ref import RemoteActorRef
from casty.config import CompressionConfig, SerializationConfig
from casty.remote.serialization import (
    CompressedSerializer,
    JsonSerializer,
    PickleSerializer,
    Serializer,
    TypeRegistry,
    build_serializer,
)
from casty.cluster import ShardEnvelope
from casty.core.transport import LocalTransport
from casty.cluster.coordinator import GetShardLocation, ShardLocation


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
    return RemoteActorRef(address=addr, _transport=transport)


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
    serializer = JsonSerializer(registry)

    inner = UserGreet(name="Alice", count=42)
    envelope = ShardEnvelope(entity_id="user-1", message=inner)

    data = serializer.serialize(envelope)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

    assert isinstance(restored, ShardEnvelope)
    assert isinstance(restored.message, UserGreet)
    assert restored.entity_id == "user-1"
    assert restored.message.name == "Alice"
    assert restored.message.count == 42


def test_actor_ref_serialization() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry)

    reply_ref: ActorRef[ShardLocation] = RemoteActorRef(
        address=ActorAddress(
            system="test", path="/reply", host="127.0.0.1", port=25521
        ),
        _transport=LocalTransport(),
    )
    msg = GetShardLocation(shard_id=7, reply_to=reply_ref)

    data = serializer.serialize(msg)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

    assert isinstance(restored, GetShardLocation)
    assert restored.shard_id == 7
    assert isinstance(restored.reply_to, RemoteActorRef)
    assert restored.reply_to.address.host == "127.0.0.1"
    assert restored.reply_to.address.port == 25521
    assert restored.reply_to.address.path == "/reply"


def test_frozenset_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry)

    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    member1 = Member(
        id="node-1", address=node1, status=MemberStatus.up, roles=frozenset({"web"})
    )
    member2 = Member(
        id="node-2",
        address=node2,
        status=MemberStatus.joining,
        roles=frozenset({"worker"}),
    )
    state = ClusterState(members=frozenset({member1, member2}))

    data = serializer.serialize(state)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

    assert isinstance(restored, ClusterState)
    assert len(restored.members) == 2
    addresses = {m.address for m in restored.members}
    assert node1 in addresses
    assert node2 in addresses


def test_enum_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry)

    node = NodeAddress(host="127.0.0.1", port=25520)
    member = Member(
        id="node-1", address=node, status=MemberStatus.leaving, roles=frozenset()
    )

    data = serializer.serialize(member)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

    assert isinstance(restored, Member)
    assert restored.status == MemberStatus.leaving
    assert restored.address == node
    assert restored.roles == frozenset()


def test_vector_clock_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry)

    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    clock = VectorClock().increment(node1).increment(node1).increment(node2)

    data = serializer.serialize(clock)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

    assert isinstance(restored, VectorClock)
    assert restored.version_of(node1) == 2
    assert restored.version_of(node2) == 1


def test_full_cluster_state_roundtrip() -> None:
    registry = _make_registry()
    serializer = JsonSerializer(registry)

    node1 = NodeAddress(host="10.0.0.1", port=25520)
    node2 = NodeAddress(host="10.0.0.2", port=25521)
    member1 = Member(
        id="node-1",
        address=node1,
        status=MemberStatus.up,
        roles=frozenset({"seed", "web"}),
    )
    member2 = Member(
        id="node-2", address=node2, status=MemberStatus.up, roles=frozenset({"worker"})
    )
    clock = VectorClock().increment(node1).increment(node2)
    state = ClusterState(
        members=frozenset({member1, member2}),
        unreachable=frozenset({node2}),
        version=clock,
    )

    data = serializer.serialize(state)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

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
    serializer = JsonSerializer(registry)

    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    location = ShardLocation(shard_id=3, node=node1, replicas=[node2])

    data = serializer.serialize(location)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

    assert isinstance(restored, ShardLocation)
    assert restored.shard_id == 3
    assert restored.node == node1
    assert len(restored.replicas) == 1
    assert restored.replicas[0] == node2


# --- PickleSerializer tests ---


def test_pickle_serializer_implements_protocol() -> None:
    serializer = PickleSerializer()
    assert isinstance(serializer, Serializer)


def test_pickle_roundtrip_simple() -> None:
    serializer = PickleSerializer()
    original = Greet(name="Alice")
    data = serializer.serialize(original)
    restored = serializer.deserialize(data)
    assert restored == original


def test_pickle_roundtrip_int_field() -> None:
    serializer = PickleSerializer()
    original = Count(value=42)
    data = serializer.serialize(original)
    restored = serializer.deserialize(data)
    assert restored == original


def test_pickle_nested_dataclass_roundtrip() -> None:
    serializer = PickleSerializer()
    inner = UserGreet(name="Alice", count=42)
    envelope = ShardEnvelope(entity_id="user-1", message=inner)
    data = serializer.serialize(envelope)
    restored = serializer.deserialize(data)

    assert isinstance(restored, ShardEnvelope)
    assert isinstance(restored.message, UserGreet)
    assert restored.entity_id == "user-1"
    assert restored.message.name == "Alice"
    assert restored.message.count == 42


def test_pickle_actor_ref_serialization() -> None:
    serializer = PickleSerializer()
    reply_ref: ActorRef[ShardLocation] = RemoteActorRef(
        address=ActorAddress(
            system="test", path="/reply", host="127.0.0.1", port=25521
        ),
        _transport=LocalTransport(),
    )
    msg = GetShardLocation(shard_id=7, reply_to=reply_ref)

    data = serializer.serialize(msg)
    restored = serializer.deserialize(data, ref_factory=_ref_factory)

    assert isinstance(restored, GetShardLocation)
    assert restored.shard_id == 7
    assert isinstance(restored.reply_to, RemoteActorRef)
    assert restored.reply_to.address.host == "127.0.0.1"
    assert restored.reply_to.address.port == 25521
    assert restored.reply_to.address.path == "/reply"


def test_pickle_frozenset_roundtrip() -> None:
    serializer = PickleSerializer()
    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    member1 = Member(
        id="node-1", address=node1, status=MemberStatus.up, roles=frozenset({"web"})
    )
    member2 = Member(
        id="node-2",
        address=node2,
        status=MemberStatus.joining,
        roles=frozenset({"worker"}),
    )
    state = ClusterState(members=frozenset({member1, member2}))

    data = serializer.serialize(state)
    restored = serializer.deserialize(data)

    assert isinstance(restored, ClusterState)
    assert len(restored.members) == 2
    addresses = {m.address for m in restored.members}
    assert node1 in addresses
    assert node2 in addresses


def test_pickle_enum_roundtrip() -> None:
    serializer = PickleSerializer()
    node = NodeAddress(host="127.0.0.1", port=25520)
    member = Member(
        id="node-1", address=node, status=MemberStatus.leaving, roles=frozenset()
    )

    data = serializer.serialize(member)
    restored = serializer.deserialize(data)

    assert isinstance(restored, Member)
    assert restored.status == MemberStatus.leaving
    assert restored.address == node
    assert restored.roles == frozenset()


def test_pickle_vector_clock_roundtrip() -> None:
    serializer = PickleSerializer()
    node1 = NodeAddress(host="127.0.0.1", port=25520)
    node2 = NodeAddress(host="127.0.0.1", port=25521)
    clock = VectorClock().increment(node1).increment(node1).increment(node2)

    data = serializer.serialize(clock)
    restored = serializer.deserialize(data)

    assert isinstance(restored, VectorClock)
    assert restored.version_of(node1) == 2
    assert restored.version_of(node2) == 1


def test_pickle_full_cluster_state_roundtrip() -> None:
    serializer = PickleSerializer()
    node1 = NodeAddress(host="10.0.0.1", port=25520)
    node2 = NodeAddress(host="10.0.0.2", port=25521)
    member1 = Member(
        id="node-1",
        address=node1,
        status=MemberStatus.up,
        roles=frozenset({"seed", "web"}),
    )
    member2 = Member(
        id="node-2", address=node2, status=MemberStatus.up, roles=frozenset({"worker"})
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


# --- CompressedSerializer and build_serializer tests ---


class TestCompressedSerializer:
    @pytest.mark.parametrize("algorithm", ["zlib", "gzip", "bz2", "lzma"])
    def test_roundtrip_pickle_all_algorithms(self, algorithm: str) -> None:
        inner = PickleSerializer()
        config = CompressionConfig(algorithm=algorithm, level=6)  # type: ignore[arg-type]
        serializer = CompressedSerializer(inner, config)
        original = Greet(name="Alice")
        data = serializer.serialize(original)
        restored = serializer.deserialize(data)
        assert restored == original

    @pytest.mark.parametrize("algorithm", ["zlib", "gzip", "bz2", "lzma"])
    def test_roundtrip_json_all_algorithms(self, algorithm: str) -> None:
        registry = TypeRegistry()
        registry.register(Greet)
        inner = JsonSerializer(registry)
        config = CompressionConfig(algorithm=algorithm, level=6)  # type: ignore[arg-type]
        serializer = CompressedSerializer(inner, config)
        original = Greet(name="Alice")
        data = serializer.serialize(original)
        restored = serializer.deserialize(data)
        assert restored == original

    def test_compression_reduces_size(self) -> None:
        inner = PickleSerializer()
        config = CompressionConfig(algorithm="zlib", level=9)
        serializer = CompressedSerializer(inner, config)
        large_payload = Greet(name="A" * 10_000)
        raw = inner.serialize(large_payload)
        compressed = serializer.serialize(large_payload)
        assert len(compressed) < len(raw)

    def test_level_1_vs_level_9(self) -> None:
        inner = PickleSerializer()
        large_payload = Greet(name="B" * 10_000)
        low = CompressedSerializer(inner, CompressionConfig(level=1))
        high = CompressedSerializer(inner, CompressionConfig(level=9))
        data_low = low.serialize(large_payload)
        data_high = high.serialize(large_payload)
        assert low.deserialize(data_low) == large_payload
        assert high.deserialize(data_high) == large_payload
        assert len(data_high) <= len(data_low)

    def test_implements_protocol(self) -> None:
        inner = PickleSerializer()
        config = CompressionConfig()
        serializer = CompressedSerializer(inner, config)
        assert isinstance(serializer, Serializer)

    def test_ref_factory_passes_through(self) -> None:
        inner = PickleSerializer()
        config = CompressionConfig()
        serializer = CompressedSerializer(inner, config)
        reply_ref: ActorRef[ShardLocation] = RemoteActorRef(
            address=ActorAddress(
                system="test", path="/reply", host="127.0.0.1", port=25521
            ),
            _transport=LocalTransport(),
        )
        msg = GetShardLocation(shard_id=7, reply_to=reply_ref)
        data = serializer.serialize(msg)
        restored = serializer.deserialize(data, ref_factory=_ref_factory)
        assert isinstance(restored, GetShardLocation)
        assert restored.shard_id == 7
        assert isinstance(restored.reply_to, RemoteActorRef)


class TestBuildSerializer:
    def test_pickle_no_compression(self) -> None:
        config = SerializationConfig(serializer="pickle")
        ser = build_serializer(config)
        assert isinstance(ser, PickleSerializer)

    def test_json_no_compression(self) -> None:
        registry = TypeRegistry()
        config = SerializationConfig(serializer="json")
        ser = build_serializer(config, registry=registry)
        assert isinstance(ser, JsonSerializer)

    def test_pickle_with_compression(self) -> None:
        config = SerializationConfig(
            serializer="pickle",
            compression=CompressionConfig(algorithm="zlib", level=9),
        )
        ser = build_serializer(config)
        assert isinstance(ser, CompressedSerializer)

    def test_json_with_compression(self) -> None:
        registry = TypeRegistry()
        registry.register(Greet)
        config = SerializationConfig(
            serializer="json",
            compression=CompressionConfig(algorithm="gzip", level=3),
        )
        ser = build_serializer(config, registry=registry)
        assert isinstance(ser, CompressedSerializer)
        original = Greet(name="test")
        assert ser.deserialize(ser.serialize(original)) == original


# --- MsgpackSerializer tests ---

msgpack = pytest.importorskip("msgpack")

from casty.remote.extras import (  # noqa: E402
    CloudPickleSerializer,
    Lz4CompressedSerializer,
    MsgpackSerializer,
)


class TestMsgpackSerializer:
    def test_implements_protocol(self) -> None:
        ser = MsgpackSerializer(TypeRegistry())
        assert isinstance(ser, Serializer)

    def test_roundtrip_simple(self) -> None:
        registry = TypeRegistry()
        registry.register(Greet)
        ser = MsgpackSerializer(registry)
        original = Greet(name="Alice")
        assert ser.deserialize(ser.serialize(original)) == original

    def test_nested_dataclass_roundtrip(self) -> None:
        ser = MsgpackSerializer(_make_registry())
        inner = UserGreet(name="Alice", count=42)
        envelope = ShardEnvelope(entity_id="user-1", message=inner)
        restored = ser.deserialize(ser.serialize(envelope), ref_factory=_ref_factory)
        assert isinstance(restored, ShardEnvelope)
        assert restored.message == inner

    def test_actor_ref_roundtrip(self) -> None:
        ser = MsgpackSerializer(_make_registry())
        reply_ref: ActorRef[ShardLocation] = RemoteActorRef(
            address=ActorAddress(
                system="test", path="/reply", host="127.0.0.1", port=25521
            ),
            _transport=LocalTransport(),
        )
        msg = GetShardLocation(shard_id=7, reply_to=reply_ref)
        restored = ser.deserialize(ser.serialize(msg), ref_factory=_ref_factory)
        assert isinstance(restored, GetShardLocation)
        assert restored.shard_id == 7
        assert isinstance(restored.reply_to, RemoteActorRef)

    def test_frozenset_roundtrip(self) -> None:
        ser = MsgpackSerializer(_make_registry())
        node1 = NodeAddress(host="127.0.0.1", port=25520)
        node2 = NodeAddress(host="127.0.0.1", port=25521)
        member1 = Member(
            id="node-1", address=node1, status=MemberStatus.up, roles=frozenset({"web"})
        )
        member2 = Member(
            id="node-2",
            address=node2,
            status=MemberStatus.joining,
            roles=frozenset({"worker"}),
        )
        state = ClusterState(members=frozenset({member1, member2}))
        restored = ser.deserialize(ser.serialize(state), ref_factory=_ref_factory)
        assert isinstance(restored, ClusterState)
        assert len(restored.members) == 2

    def test_output_is_smaller_than_json(self) -> None:
        registry = TypeRegistry()
        json_ser = JsonSerializer(registry)
        msgpack_ser = MsgpackSerializer(registry)
        large = Greet(name="A" * 1_000)
        assert len(msgpack_ser.serialize(large)) < len(json_ser.serialize(large))


# --- CloudPickleSerializer tests ---

cloudpickle = pytest.importorskip("cloudpickle")


class TestCloudPickleSerializer:
    def test_implements_protocol(self) -> None:
        ser = CloudPickleSerializer()
        assert isinstance(ser, Serializer)

    def test_roundtrip_simple(self) -> None:
        ser = CloudPickleSerializer()
        original = Greet(name="Alice")
        assert ser.deserialize(ser.serialize(original)) == original

    def test_roundtrip_lambda(self) -> None:
        ser = CloudPickleSerializer()
        fn = lambda x: x + 1  # noqa: E731
        restored = ser.deserialize(ser.serialize(fn))
        assert restored(41) == 42

    def test_nested_dataclass_roundtrip(self) -> None:
        ser = CloudPickleSerializer()
        inner = UserGreet(name="Alice", count=42)
        envelope = ShardEnvelope(entity_id="user-1", message=inner)
        restored = ser.deserialize(ser.serialize(envelope))
        assert isinstance(restored, ShardEnvelope)
        assert restored.message == inner

    def test_actor_ref_roundtrip(self) -> None:
        ser = CloudPickleSerializer()
        reply_ref: ActorRef[ShardLocation] = RemoteActorRef(
            address=ActorAddress(
                system="test", path="/reply", host="127.0.0.1", port=25521
            ),
            _transport=LocalTransport(),
        )
        msg = GetShardLocation(shard_id=7, reply_to=reply_ref)
        restored = ser.deserialize(ser.serialize(msg), ref_factory=_ref_factory)
        assert isinstance(restored, GetShardLocation)
        assert restored.shard_id == 7
        assert isinstance(restored.reply_to, RemoteActorRef)


# --- Lz4CompressedSerializer tests ---

lz4 = pytest.importorskip("lz4")


class TestLz4CompressedSerializer:
    def test_implements_protocol(self) -> None:
        ser = Lz4CompressedSerializer(PickleSerializer())
        assert isinstance(ser, Serializer)

    def test_roundtrip_with_pickle(self) -> None:
        ser = Lz4CompressedSerializer(PickleSerializer())
        original = Greet(name="Alice")
        assert ser.deserialize(ser.serialize(original)) == original

    def test_roundtrip_with_json(self) -> None:
        registry = TypeRegistry()
        registry.register(Greet)
        ser = Lz4CompressedSerializer(JsonSerializer(registry))
        original = Greet(name="Alice")
        assert ser.deserialize(ser.serialize(original)) == original

    def test_roundtrip_with_msgpack(self) -> None:
        registry = TypeRegistry()
        registry.register(Greet)
        ser = Lz4CompressedSerializer(MsgpackSerializer(registry))
        original = Greet(name="Alice")
        assert ser.deserialize(ser.serialize(original)) == original

    def test_compression_reduces_size(self) -> None:
        inner = PickleSerializer()
        ser = Lz4CompressedSerializer(inner)
        large = Greet(name="A" * 10_000)
        raw = inner.serialize(large)
        compressed = ser.serialize(large)
        assert len(compressed) < len(raw)

    def test_ref_factory_passes_through(self) -> None:
        ser = Lz4CompressedSerializer(PickleSerializer())
        reply_ref: ActorRef[ShardLocation] = RemoteActorRef(
            address=ActorAddress(
                system="test", path="/reply", host="127.0.0.1", port=25521
            ),
            _transport=LocalTransport(),
        )
        msg = GetShardLocation(shard_id=7, reply_to=reply_ref)
        restored = ser.deserialize(ser.serialize(msg), ref_factory=_ref_factory)
        assert isinstance(restored, GetShardLocation)
        assert restored.shard_id == 7
        assert isinstance(restored.reply_to, RemoteActorRef)
