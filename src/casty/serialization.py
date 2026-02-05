from __future__ import annotations

import dataclasses
import json
from typing import Any, Protocol, runtime_checkable


class TypeRegistry:
    def __init__(self) -> None:
        self._name_to_type: dict[str, type] = {}
        self._type_to_name: dict[type, str] = {}

    def register(self, cls: type) -> None:
        name = f"{cls.__module__}.{cls.__qualname__}"
        self._name_to_type[name] = cls
        self._type_to_name[cls] = name

    def resolve(self, name: str) -> type:
        if name not in self._name_to_type:
            msg = f"Unknown type: {name}"
            raise KeyError(msg)
        return self._name_to_type[name]

    def type_name(self, cls: type) -> str:
        return self._type_to_name[cls]


@runtime_checkable
class Serializer(Protocol):
    def serialize(self, obj: Any) -> bytes: ...
    def deserialize(self, data: bytes) -> Any: ...


class JsonSerializer:
    def __init__(self, registry: TypeRegistry) -> None:
        self._registry = registry

    def serialize(self, obj: Any) -> bytes:
        if not dataclasses.is_dataclass(obj) or isinstance(obj, type):
            msg = f"Can only serialize dataclass instances, got {type(obj)}"
            raise TypeError(msg)
        type_name = self._registry.type_name(type(obj))
        payload = dataclasses.asdict(obj)
        envelope = {"_type": type_name, **payload}
        return json.dumps(envelope).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        envelope = json.loads(data.decode("utf-8"))
        type_name = envelope.pop("_type")
        cls = self._registry.resolve(type_name)
        return cls(**envelope)
