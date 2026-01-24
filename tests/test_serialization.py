import pytest
from dataclasses import dataclass, field

from casty.serializable import serializable, serialize, deserialize


@serializable
@dataclass
class Simple:
    value: int
    name: str


@serializable
@dataclass
class WithDefaults:
    value: int = 0
    name: str = "default"


@serializable
@dataclass
class Nested:
    inner: Simple
    count: int


@serializable
@dataclass
class WithList:
    items: list[int] = field(default_factory=list)


@serializable
@dataclass
class WithDict:
    data: dict[str, int] = field(default_factory=dict)


def test_serialize_simple_dataclass():
    """@serializable dataclass can be serialized to bytes"""
    obj = Simple(value=42, name="test")

    data = serialize(obj)

    assert isinstance(data, bytes)
    assert len(data) > 0


def test_deserialize_simple_dataclass():
    """Serialized bytes can be deserialized back to dataclass"""
    original = Simple(value=42, name="test")

    data = serialize(original)
    restored = deserialize(data)

    assert restored == original
    assert isinstance(restored, Simple)


def test_serialize_with_defaults():
    """Dataclass with default values serializes correctly"""
    obj = WithDefaults()

    data = serialize(obj)
    restored = deserialize(data)

    assert restored.value == 0
    assert restored.name == "default"


def test_serialize_nested_dataclass():
    """Nested dataclasses serialize correctly"""
    inner = Simple(value=1, name="inner")
    outer = Nested(inner=inner, count=5)

    data = serialize(outer)
    restored = deserialize(data)

    assert restored.inner.value == 1
    assert restored.inner.name == "inner"
    assert restored.count == 5


def test_serialize_with_list():
    """Dataclass with list field serializes correctly"""
    obj = WithList(items=[1, 2, 3, 4, 5])

    data = serialize(obj)
    restored = deserialize(data)

    assert restored.items == [1, 2, 3, 4, 5]


def test_serialize_with_dict():
    """Dataclass with dict field serializes correctly"""
    obj = WithDict(data={"a": 1, "b": 2, "c": 3})

    data = serialize(obj)
    restored = deserialize(data)

    assert restored.data == {"a": 1, "b": 2, "c": 3}
