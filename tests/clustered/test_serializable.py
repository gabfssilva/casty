import pytest
from dataclasses import dataclass

from casty.cluster import serializable, deserialize


@serializable
@dataclass(frozen=True, slots=True)
class SimpleMessage:
    value: int
    name: str


@serializable
@dataclass(frozen=True, slots=True)
class NestedMessage:
    inner: SimpleMessage
    count: int


@serializable
@dataclass(frozen=True, slots=True)
class WithTuple:
    address: tuple[str, int]


@serializable
@dataclass(frozen=True, slots=True)
class WithOptional:
    value: int
    name: str | None = None


class TestSerializable:
    def test_simple_roundtrip(self):
        msg = SimpleMessage(value=42, name="test")
        data = SimpleMessage.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg

    def test_nested_roundtrip(self):
        inner = SimpleMessage(value=1, name="inner")
        msg = NestedMessage(inner=inner, count=5)
        data = NestedMessage.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg
        assert result.inner == inner

    def test_tuple_roundtrip(self):
        msg = WithTuple(address=("127.0.0.1", 8080))
        data = WithTuple.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg
        assert result.address == ("127.0.0.1", 8080)

    def test_optional_none(self):
        msg = WithOptional(value=10)
        data = WithOptional.Codec.serialize(msg)
        result = deserialize(data)
        assert result.value == 10
        assert result.name is None

    def test_optional_with_value(self):
        msg = WithOptional(value=10, name="hello")
        data = WithOptional.Codec.serialize(msg)
        result = deserialize(data)
        assert result == msg

    def test_type_id_preserved(self):
        msg = SimpleMessage(value=1, name="x")
        data = SimpleMessage.Codec.serialize(msg)
        assert len(data) > 4
        result = deserialize(data)
        assert isinstance(result, SimpleMessage)

    def test_unknown_type_id_raises(self):
        with pytest.raises(ValueError, match="unknown type_id"):
            deserialize(b"\x00\x00\x00\x00data")

    def test_data_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            deserialize(b"\x00\x01")
