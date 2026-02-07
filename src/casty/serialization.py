from __future__ import annotations

import dataclasses
import enum
import importlib
import json
import logging
from collections.abc import Callable
from typing import Any, Protocol, cast, runtime_checkable

from casty.address import ActorAddress

logger = logging.getLogger("casty.serialization")


class TypeRegistry:
    def __init__(self) -> None:
        self._name_to_type: dict[str, type] = {}
        self._type_to_name: dict[type, str] = {}

    def register(self, cls: type) -> None:
        name = f"{cls.__module__}.{cls.__qualname__}"
        self._name_to_type[name] = cls
        self._type_to_name[cls] = name

    def register_all(self, *types: type) -> None:
        for cls in types:
            self.register(cls)

    def resolve(self, name: str) -> type:
        if name in self._name_to_type:
            return self._name_to_type[name]
        cls = self._auto_resolve(name)
        self.register(cls)
        return cls

    def type_name(self, cls: type) -> str:
        if cls not in self._type_to_name:
            self.register(cls)
        return self._type_to_name[cls]

    def _auto_resolve(self, name: str) -> type:
        parts = name.split(".")
        for i in range(len(parts) - 1, 0, -1):
            module_path = ".".join(parts[:i])
            attr_path = parts[i:]
            try:
                mod = importlib.import_module(module_path)
                obj: Any = mod
                for attr in attr_path:
                    obj = getattr(obj, attr)
                if isinstance(obj, type):
                    return obj
            except (ImportError, AttributeError):
                continue
        logger.warning("Failed to resolve type: %s", name)
        msg = f"Cannot resolve type: {name}"
        raise KeyError(msg)

    def is_registered(self, cls: type) -> bool:
        return cls in self._type_to_name


@runtime_checkable
class Serializer(Protocol):
    def serialize(self, obj: Any) -> bytes: ...
    def deserialize(self, data: bytes) -> Any: ...


class JsonSerializer:
    def __init__(
        self,
        registry: TypeRegistry,
        *,
        ref_factory: Callable[[ActorAddress], Any] | None = None,
    ) -> None:
        self._registry = registry
        self._ref_factory = ref_factory

    @property
    def registry(self) -> TypeRegistry:
        return self._registry

    def set_ref_factory(self, factory: Callable[[ActorAddress], Any]) -> None:
        self._ref_factory = factory

    def serialize(self, obj: Any) -> bytes:
        payload = self._to_dict(obj)
        return json.dumps(payload).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        payload: object = json.loads(data.decode("utf-8"))
        return self._from_dict(payload)

    def _to_dict(self, value: Any) -> object:
        from casty.ref import ActorRef

        match value:
            case ActorRef():
                return {"__ref__": value.address.to_uri()}
            case enum.Enum():
                enum_cls = type(value)
                return {
                    "__enum__": f"{enum_cls.__module__}.{enum_cls.__qualname__}",
                    "value": value.name,
                }
            case frozenset():
                return {"__frozenset__": [self._to_dict(item) for item in cast(frozenset[object], value)]}
            case _ if dataclasses.is_dataclass(value) and not isinstance(value, type):
                type_name = self._registry.type_name(type(value))
                result: dict[str, object] = {"_type": type_name}
                for f in dataclasses.fields(value):
                    field_value: object = getattr(value, f.name)
                    result[f.name] = self._to_dict(field_value)
                return result
            case dict():
                return {
                    "__dict__": [
                        [self._to_dict(k), self._to_dict(v)]
                        for k, v in cast(dict[object, object], value).items()
                    ]
                }
            case list():
                return [self._to_dict(item) for item in cast(list[object], value)]
            case tuple():
                return {"__tuple__": [self._to_dict(item) for item in cast(tuple[object, ...], value)]}
            case _:
                return value

    def _from_dict(self, value: object) -> Any:
        match value:
            case {"__ref__": str() as uri}:
                addr = ActorAddress.from_uri(uri)
                if self._ref_factory is not None:
                    return self._ref_factory(addr)
                msg = "Cannot deserialize ActorRef without ref_factory"
                raise ValueError(msg)
            case {"__enum__": str() as enum_type_name, "value": str() as member_name}:
                enum_cls = self._registry.resolve(enum_type_name)
                return enum_cls[member_name]  # type: ignore[index]
            case {"__frozenset__": list() as _fs}:  # pyright: ignore[reportUnknownVariableType]
                items = cast(list[object], _fs)
                return frozenset(self._from_dict(item) for item in items)
            case {"__dict__": list() as _pairs}:  # pyright: ignore[reportUnknownVariableType]
                pairs = cast(list[list[object]], _pairs)
                return {
                    self._from_dict(pair[0]): self._from_dict(pair[1])
                    for pair in pairs
                }
            case {"__tuple__": list() as _tup}:  # pyright: ignore[reportUnknownVariableType]
                items = cast(list[object], _tup)
                return tuple(self._from_dict(item) for item in items)
            case {"_type": str() as type_name}:
                cls = self._registry.resolve(type_name)
                fields = dataclasses.fields(cls)
                str_dict = cast(dict[str, object], value)
                kwargs: dict[str, Any] = {}
                for f in fields:
                    if f.name in str_dict:
                        kwargs[f.name] = self._from_dict(str_dict[f.name])
                return cls(**kwargs)
            case dict():
                return cast(dict[str, object], value)
            case list():
                return [self._from_dict(item) for item in cast(list[object], value)]
            case _:
                return value


