from __future__ import annotations

import asyncio
import msgpack
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any, TypeVar

T = TypeVar("T")

_registry: dict[str, type] = {}


def serializable(cls: type[T]) -> type[T]:
    if not is_dataclass(cls):
        raise TypeError(f"{cls.__name__} must be a dataclass")

    type_name = f"{cls.__module__}.{cls.__qualname__}"
    _registry[type_name] = cls
    cls.__serializable_type__ = type_name

    return cls


def serialize(obj: Any) -> bytes:
    return msgpack.packb(_to_dict(obj), use_bin_type=True)


def deserialize(data: bytes) -> Any:
    return _from_dict(msgpack.unpackb(data, raw=False))


def _is_serializable_value(value: Any) -> bool:
    if isinstance(value, asyncio.Future):
        return False
    return True


def _to_dict(obj: Any) -> Any:
    from .ref import ActorRef, UnresolvedActorRef

    match obj:
        case None | bool() | int() | float() | str() | bytes():
            return obj

        case Enum():
            return {
                "__enum__": f"{obj.__class__.__module__}.{obj.__class__.__qualname__}",
                "value": obj.value,
            }

        case list() | tuple():
            return [_to_dict(item) for item in obj]

        case dict():
            return {k: _to_dict(v) for k, v in obj.items()}

        case ActorRef():
            system = getattr(obj, "_system", None)
            if system is not None:
                node_id = system.node_id
            else:
                node_id = getattr(obj, "node_id", "local")
            unresolved = UnresolvedActorRef(actor_id=obj.actor_id, node_id=node_id)
            return _to_dict(unresolved)

        case _ if is_dataclass(obj) and not isinstance(obj, type):
            type_name = getattr(obj.__class__, "__serializable_type__", None)
            if type_name is None:
                raise TypeError(f"{obj.__class__.__name__} is not @serializable")

            return {
                "__type__": type_name,
                **{
                    f.name: _to_dict(getattr(obj, f.name))
                    for f in fields(obj)
                    if _is_serializable_value(getattr(obj, f.name))
                },
            }

        case _:
            raise TypeError(f"Cannot serialize {type(obj)}")


def _from_dict(data: Any) -> Any:
    import importlib

    match data:
        case None | bool() | int() | float() | str() | bytes():
            return data

        case list():
            return [_from_dict(item) for item in data]

        case {"__type__": type_name, **rest}:
            cls = _registry.get(type_name)
            if cls is None:
                raise TypeError(f"Unknown type: {type_name}")
            field_values = {k: _from_dict(v) for k, v in rest.items()}
            return cls(**field_values)

        case {"__enum__": enum_path, "value": value}:
            module_name, enum_name = enum_path.rsplit(".", 1)
            module = importlib.import_module(module_name)
            enum_cls = getattr(module, enum_name)
            return enum_cls(value)

        case dict():
            return {k: _from_dict(v) for k, v in data.items()}

        case _:
            return data
