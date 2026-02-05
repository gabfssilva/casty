from __future__ import annotations

import pytest
from dataclasses import dataclass

from casty.serialization import TypeRegistry, JsonSerializer, Serializer
from casty.address import ActorAddress


@dataclass(frozen=True)
class Greet:
    name: str


@dataclass(frozen=True)
class Count:
    value: int


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
