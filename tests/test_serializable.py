import pytest
from dataclasses import dataclass


def test_serializable_decorator():
    from casty.serializable import serializable, serialize, deserialize

    @serializable
    @dataclass
    class Ping:
        value: int

    msg = Ping(42)
    data = serialize(msg)

    assert isinstance(data, bytes)

    restored = deserialize(data)
    assert restored == msg


def test_serializable_nested():
    from casty.serializable import serializable, serialize, deserialize

    @serializable
    @dataclass
    class Inner:
        x: int

    @serializable
    @dataclass
    class Outer:
        inner: Inner
        name: str

    msg = Outer(inner=Inner(10), name="test")
    data = serialize(msg)
    restored = deserialize(data)

    assert restored == msg


def test_unregistered_type_raises():
    from casty.serializable import serialize

    @dataclass
    class NotRegistered:
        x: int

    with pytest.raises(TypeError):
        serialize(NotRegistered(1))
