"""Tests for serialization module."""

from dataclasses import dataclass

import pytest

from casty.cluster.serialize import (
    serialize,
    deserialize,
    register_type,
    get_registered_type,
    serialize_to_dict,
    deserialize_from_dict,
)


class TestBasicSerialization:
    """Tests for basic type serialization."""

    def test_serialize_none(self):
        """Test serializing None."""
        data = serialize(None)
        assert deserialize(data) is None

    def test_serialize_bool(self):
        """Test serializing booleans."""
        assert deserialize(serialize(True)) is True
        assert deserialize(serialize(False)) is False

    def test_serialize_int(self):
        """Test serializing integers."""
        assert deserialize(serialize(42)) == 42
        assert deserialize(serialize(-123)) == -123
        assert deserialize(serialize(0)) == 0

    def test_serialize_float(self):
        """Test serializing floats."""
        assert deserialize(serialize(3.14)) == pytest.approx(3.14)
        assert deserialize(serialize(-0.5)) == pytest.approx(-0.5)

    def test_serialize_string(self):
        """Test serializing strings."""
        assert deserialize(serialize("hello")) == "hello"
        assert deserialize(serialize("")) == ""
        assert deserialize(serialize("unicode: \u00e9\u00e8")) == "unicode: \u00e9\u00e8"

    def test_serialize_bytes(self):
        """Test serializing bytes."""
        data = b"\x00\x01\x02\x03"
        assert deserialize(serialize(data)) == data

    def test_serialize_list(self):
        """Test serializing lists."""
        lst = [1, "two", 3.0, None, True]
        assert deserialize(serialize(lst)) == lst

    def test_serialize_nested_list(self):
        """Test serializing nested lists."""
        lst = [[1, 2], [3, [4, 5]]]
        assert deserialize(serialize(lst)) == lst

    def test_serialize_dict(self):
        """Test serializing dictionaries."""
        d = {"key": "value", "number": 42, "nested": {"a": 1}}
        result = deserialize(serialize(d))
        assert result == d

    def test_serialize_tuple_as_list(self):
        """Test that tuples are serialized as lists."""
        t = (1, 2, 3)
        result = deserialize(serialize(t))
        assert result == [1, 2, 3]

    def test_serialize_set(self):
        """Test serializing sets."""
        s = {1, 2, 3}
        result = deserialize(serialize(s))
        assert result == s


class TestDataclassSerialization:
    """Tests for dataclass serialization."""

    def test_serialize_simple_dataclass(self):
        """Test serializing a simple dataclass."""
        @register_type
        @dataclass
        class Point:
            x: int
            y: int

        p = Point(10, 20)
        result = deserialize(serialize(p))
        assert isinstance(result, Point)
        assert result.x == 10
        assert result.y == 20

    def test_serialize_nested_dataclass(self):
        """Test serializing nested dataclasses."""
        @register_type
        @dataclass
        class Inner:
            value: str

        @register_type
        @dataclass
        class Outer:
            inner: Inner
            count: int

        obj = Outer(inner=Inner("hello"), count=5)
        result = deserialize(serialize(obj))
        assert isinstance(result, Outer)
        assert isinstance(result.inner, Inner)
        assert result.inner.value == "hello"
        assert result.count == 5

    def test_serialize_dataclass_with_list(self):
        """Test serializing dataclass with list field."""
        @register_type
        @dataclass
        class Container:
            items: list[int]

        obj = Container(items=[1, 2, 3])
        result = deserialize(serialize(obj))
        assert result.items == [1, 2, 3]

    def test_serialize_dataclass_with_optional(self):
        """Test serializing dataclass with optional field."""
        @register_type
        @dataclass
        class WithOptional:
            required: str
            optional: int | None = None

        obj1 = WithOptional(required="test")
        result1 = deserialize(serialize(obj1))
        assert result1.required == "test"
        assert result1.optional is None

        obj2 = WithOptional(required="test", optional=42)
        result2 = deserialize(serialize(obj2))
        assert result2.optional == 42


class TestTypeRegistry:
    """Tests for type registry."""

    def test_register_type(self):
        """Test registering a type."""
        @register_type
        @dataclass
        class MyType:
            value: int

        full_name = f"{MyType.__module__}.{MyType.__qualname__}"
        assert get_registered_type(full_name) is MyType

    def test_get_unregistered_type(self):
        """Test getting an unregistered type."""
        assert get_registered_type("nonexistent.Type") is None


class TestDictSerialization:
    """Tests for dictionary serialization helpers."""

    def test_serialize_to_dict(self):
        """Test serializing to dictionary."""
        @register_type
        @dataclass
        class Sample:
            name: str
            value: int

        obj = Sample(name="test", value=42)
        d = serialize_to_dict(obj)

        assert "__type__" in d
        assert "__data__" in d
        assert d["__data__"]["name"] == "test"
        assert d["__data__"]["value"] == 42

    def test_deserialize_from_dict(self):
        """Test deserializing from dictionary."""
        @register_type
        @dataclass
        class Sample2:
            name: str
            value: int

        full_name = f"{Sample2.__module__}.{Sample2.__qualname__}"
        d = {
            "__type__": full_name,
            "__data__": {"name": "test", "value": 42},
        }

        result = deserialize_from_dict(d)
        assert isinstance(result, Sample2)
        assert result.name == "test"
        assert result.value == 42


class TestBuiltinTypes:
    """Tests for built-in type serialization."""

    def test_serialize_control_plane_command(self):
        """Test serializing control plane commands."""
        from casty.cluster.control_plane import JoinCluster

        cmd = JoinCluster(
            node_id="node-1",
            address=("localhost", 8001),
            timestamp=123.45,
        )

        result = deserialize(serialize(cmd))
        assert isinstance(result, JoinCluster)
        assert result.node_id == "node-1"
        # Tuple becomes list in serialization
        assert tuple(result.address) == ("localhost", 8001)
        assert result.timestamp == 123.45

    def test_serialize_raft_message(self):
        """Test serializing Raft messages."""
        from casty.cluster.control_plane import VoteRequest, VoteResponse

        req = VoteRequest(
            term=5,
            candidate_id="node-1",
            last_log_index=10,
            last_log_term=4,
        )

        result = deserialize(serialize(req))
        assert isinstance(result, VoteRequest)
        assert result.term == 5
        assert result.candidate_id == "node-1"

        resp = VoteResponse(term=5, vote_granted=True)
        result = deserialize(serialize(resp))
        assert isinstance(result, VoteResponse)
        assert result.vote_granted is True

    def test_serialize_gossip_message(self):
        """Test serializing gossip messages."""
        from casty.cluster.data_plane import Ping, MembershipUpdate, MemberStatus

        ping = Ping(sender_id="node-1", incarnation=3)
        result = deserialize(serialize(ping))
        assert isinstance(result, Ping)
        assert result.sender_id == "node-1"
        assert result.incarnation == 3

        update = MembershipUpdate(
            node_id="node-2",
            status=MemberStatus.ALIVE,
            incarnation=1,
            address=("localhost", 8002),
        )
        result = deserialize(serialize(update))
        assert isinstance(result, MembershipUpdate)
        assert result.node_id == "node-2"


class TestEdgeCases:
    """Tests for edge cases."""

    def test_serialize_empty_list(self):
        """Test serializing empty list."""
        assert deserialize(serialize([])) == []

    def test_serialize_empty_dict(self):
        """Test serializing empty dict."""
        assert deserialize(serialize({})) == {}

    def test_serialize_empty_set(self):
        """Test serializing empty set."""
        assert deserialize(serialize(set())) == set()

    def test_serialize_large_data(self):
        """Test serializing large data."""
        data = {"items": list(range(10000))}
        result = deserialize(serialize(data))
        assert result == data

    def test_serialize_deep_nesting(self):
        """Test serializing deeply nested data."""
        data: dict = {"level": 0}
        current = data
        for i in range(1, 50):
            current["nested"] = {"level": i}
            current = current["nested"]

        result = deserialize(serialize(data))
        assert result["level"] == 0
        assert result["nested"]["level"] == 1
